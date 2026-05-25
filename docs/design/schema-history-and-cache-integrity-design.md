# Schema History + 缓冲文件完整性设计

> 创建时间：2026-05-24
> 状态：**待设计（设计讨论进行中，部分决策未完成）**
> 版本：v0.1（草案）

---

## 1. 背景

表级独立生命周期特性（`table-lifecycle-design.md`）引入了 binlog 缓冲层（`internal/cache/LocalBackend`），在 snapshotting 阶段将增量事件缓冲到本地文件。当前实现存在两类核心问题：

1. **缓冲文件完整性**：进程中断/崩溃后，缓冲文件可能不完整或包含重复事件
2. **Schema History**：DML 事件解析依赖表结构定义，需要自己维护一份完整的源端表结构历史链，不能依赖源库或目标库实时查询

这两个问题相互关联：缓冲文件存的是序列化的 ChangeEvent，ChangeEvent 的解析需要正确的表结构；表结构的变更（DDL）本身也需要记录在缓冲中并保证完整性。

---

## 2. 缓冲文件完整性

### 2.1 当前问题

通过对 `internal/cache/local_backend.go` 的代码审计，发现以下问题：

| 问题 | 位置 | 严重性 | 说明 |
|------|------|--------|------|
| Write 无 fsync | `local_backend.go:Write()` | P0 | 数据只在内核页面缓存，进程崩溃后可能丢失最后 N 条事件 |
| 部分写入无法检测 | `local_backend.go:Read()` | P0 | 长度前缀写入后载荷截断，Read 静默停止并报告 `CaughtUp=true`（假阳性） |
| Read 错误不传播 | `local_backend.go:Read()` goroutine | P1 | channel 关闭不区分"正常读完"和"遇到损坏记录"，调用方无法区分 |
| 无记录级校验和 | 文件格式 | P1 | 静默数据损坏（位翻转）无法检测，protobuf unmarshal 可能静默返回错误数据 |
| 无幂等写入 | `local_backend.go:Write()` | P2 | 重复写入产生两条相同记录，Read 会重复投递 |
| Route 错误静默丢弃 | `pipeline_integration.go:routeEvents()` | P1 | `_ = p.consumer.Route(...)` 忽略所有错误 |

### 2.2 大事务跨多表文件的核心问题

**这是最严重的一致性问题。**

一个 GTID 事务的事件分散到多张表的缓冲文件中。如果写了一半进程崩溃，重启后从该 GTID 重新拉取完整事务，会导致部分表文件里有重复事件：

```
GTID:500 的事务包含:
  INSERT INTO table_A (id=1)  → 写入 table_A.binlog ✅
  INSERT INTO table_A (id=2)  → 写入 table_A.binlog ✅
  INSERT INTO table_B (id=1)  → 写入 table_B.binlog ✅
  INSERT INTO table_B (id=2)  → 崩溃，未写入 ❌
  INSERT INTO table_C (id=1)  → 崩溃，未写入 ❌

重启后从 GTID:500 重新拉取完整事务:
  INSERT INTO table_A (id=1)  → 又写入 table_A.binlog ← 重复！
  INSERT INTO table_A (id=2)  → 又写入 table_A.binlog ← 重复！
  INSERT INTO table_B (id=1)  → 又写入 table_B.binlog ← 重复！
  INSERT INTO table_B (id=2)  → 写入 table_B.binlog ✅
  INSERT INTO table_C (id=1)  → 写入 table_C.binlog ✅

结果: table_A 有 4 条（2 条重复），table_B 有 3 条（1 条重复），table_C 正常
```

### 2.3 候选方案（待决策）

#### 方案 A：全局 WAL + 按表索引

不直接按表分文件写，先写一个全局 WAL（所有表的事件顺序追加到同一个文件），以 COMMIT 标记事务完整性。按表 Read 时通过索引跳读。

```
WAL 文件格式:
  [GTID:499 BEGIN]
  [event: table_A, row1]
  [event: table_A, row2]
  [event: table_B, row1]
  [GTID:499 COMMIT]        ← 这个事务完整
  [GTID:500 BEGIN]
  [event: table_A, row1]
  [event: table_B, row1]
  --- 崩溃，没有 COMMIT ---  ← 这个事务不完整

重启后:
  1. 找到最后一个 COMMIT 标记 → GTID:499
  2. truncate 掉 GTID:500 的所有不完整数据
  3. 从 GTID:500 开始重新拉取
```

- 优点：写入原子性以 GTID 事务为单位，天然解决跨表问题
- 缺点：按表回放需要索引跳读；单文件写入可能成为瓶颈

#### 方案 B：保持按表分文件 + committed_gtids 元数据文件

维护一个额外的元数据文件记录已完整提交的 GTID。

```
meta/committed_gtids.log:
  GTID:498 ✅
  GTID:499 ✅
  （GTID:500 不在这里 — 未完成）

重启后:
  1. 读 committed_gtids，最后完整 GTID = 499
  2. 对所有表文件，truncate 掉 GTID:500 的事件
  3. 从 GTID:500 开始重新拉取
```

- 优点：保持按表文件的 IO 模式，回放效率高
- 缺点：truncate 需要知道各表文件中 GTID:500 的起始偏移（需要维护偏移索引）

### 2.4 方案选定后的影响

**选 A 或 B 后，事务级完整性保证了，`table-lifecycle-design.md` 中的 catching_up UPSERT 安全窗口（§6.2）可以删掉或降级为可选防御性配置（默认关闭）。**

因为：
- 重启后精确知道最后一个完整 COMMIT 的 GTID
- 从下一个 GTID 开始拉取，不存在"重叠区"
- 不需要"前 1 分钟 UPSERT"来覆盖重叠

### 2.5 其他完整性修复

无论选哪个方案，以下修复都需要做：

| 修复项 | 说明 |
|--------|------|
| **Write 后 fsync** | 可配置 `sync-mode`: `every`（每条同步）/ `batch`（每 N 条或每事务同步）/ `none`（性能优先） |
| **记录格式加 CRC32** | `[4B length][4B CRC32][N bytes protobuf]`，Read 时校验 |
| **Read 错误传播** | 截断/损坏记录需要通知调用方，不能静默 CaughtUp=true |
| **部分写入检测 + truncate** | 重启时检查最后一条记录是否完整，不完整则 truncate |
| **Route 错误处理** | `pipeline_integration.go` 中记录 Route 错误而非静默丢弃 |

---

## 3. Schema History

### 3.1 为什么需要

CDC 增量同步中，DML 事件（INSERT/UPDATE/DELETE）的行数据是按列位置排列的原始值。要正确解析这些值，需要知道**当时那个时刻的表结构**（列名、类型、位置顺序）。

当前实现的做法是从源库的 `INFORMATION_SCHEMA` 实时查询表结构（`internal/source/mysql/schema_cache.go`），但这有两个根本性问题：

**问题 1：时序不匹配**

DML 事件对应的是历史时刻的表结构。如果 T1 时刻表有 3 列，T2 时刻 ALTER 加了第 4 列，T3 时刻查 `INFORMATION_SCHEMA` 拿到 4 列——但 T1 的 DML 只有 3 个值。列数对不上。

```
T1: INSERT INTO t (a,b,c) VALUES (1,2,3)     ← 3 列
T2: ALTER TABLE t ADD COLUMN d INT            ← 表变成 4 列
T3: 查 INFORMATION_SCHEMA → 拿到 4 列        ← 用 4 列 schema 解析 3 列数据 → 错误
```

**问题 2：跨数据库不可用**

跨库场景（MySQL→Oracle、PostgreSQL→MySQL），查目标库的 `INFORMATION_SCHEMA` 拿到的是目标端类型（如 Oracle 的 `VARCHAR2`），不是源端 binlog 需要的类型（MySQL 的 `VARCHAR`）。

### 3.2 Debezium 的做法（参考实现）

Debezium 维护一条 **Schema History 链**，每个 DDL 事件点都存一份完整的表结构快照：

```
Debezium Schema History:
  Position:BinlogFile=mysql-bin.000001,Pos=4
    → CREATE TABLE t (a INT, b VARCHAR(100))       → 存完整 Table{Columns:[a,b]}
  
  Position:BinlogFile=mysql-bin.000001,Pos=1234
    → ALTER TABLE t ADD COLUMN c TIMESTAMP          → 存完整 Table{Columns:[a,b,c]}
  
  Position:BinlogFile=mysql-bin.000002,Pos=567
    → ALTER TABLE t DROP COLUMN b                   → 存完整 Table{Columns:[a,c]}
```

关键代码路径（证据来源 `~/Codes/dts/debezium/`）：

- `HistoryRecord`（`debezium-core/.../history/HistoryRecord.java`）：每条记录包含 source position + DDL 文本 + 序列化的完整 TableChanges
- `JsonTableChangeSerializer`：序列化每列的 name、jdbcType、typeName、length、scale、position、nullable、autoIncremented、defaultValueExpression 等
- `AbstractSchemaHistory.recover()`：重启时按顺序读取所有 HistoryRecord，position ≤ connector offset 的 apply 到内存 Tables
- `RelationalChangeRecordEmitter`：DML 处理时列名/类型/位置**全部来自内存中的 TableSchema**，不查源库

存储后端默认是 Kafka topic（`KafkaSchemaHistory`），retention = 无限。

### 3.3 DataStream 的设计

#### 核心流程

```
CREATE TABLE:
  DDL → parser.Parse() → 完整 TableInfo（含所有 Column）
  → Tables.Put(tableInfo)
  → history.Record(position, 完整 TableInfo)

ALTER TABLE:
  DDL → parser.Parse() → delta（AddedColumns/DroppedColumns/ModifiedColumns）
  → Tables.Apply(delta) → 演算出新的完整 TableInfo
  → history.Record(position, 新的完整 TableInfo)

DROP TABLE:
  → Tables.Remove(tableID)
  → history.Record(position, drop)

重启恢复:
  → history.Recover(offsets)
  → 按位点顺序加载所有 Record
  → position ≤ connector offset 的 apply 到内存 Tables
  → 内存 Tables 精确反映该 offset 时刻的表结构

DML 事件处理:
  → 从内存 Tables 取 schema（不查源库 INFORMATION_SCHEMA）
  → 用 schema 解析 binlog 行数据
```

**注意：history 存的是 Apply 后的完整 TableInfo，不是 parser 的原始 delta。** 这样 Recover 时只需要反序列化完整 TableInfo 直接覆盖，不需要重放 delta 演算。

#### SchemaHistoryRecord

```go
type SchemaHistoryRecord struct {
    Position     event.Position
    Database     string
    Schema       string              // PostgreSQL/Oracle 的 schema
    Table        string
    DDL          string              // 原始 DDL 语句
    TableInfo    *event.TableInfo    // 变更后的完整表结构（CREATE/ALTER 时非 nil）
    ChangeType   string              // "CREATE" / "ALTER" / "DROP"
    Timestamp    time.Time
}
```

#### SchemaHistory 接口

```go
type SchemaHistory interface {
    Record(ctx context.Context, record *SchemaHistoryRecord) error
    Recover(ctx context.Context, tables *Tables, offset *event.Position) error
    Exists(ctx context.Context) (bool, error)
    Close() error
}
```

#### Tables（内存表定义集合）

```go
type Tables struct {
    tables map[string]*event.TableInfo  // "database.table" → TableInfo
    mu     sync.RWMutex
}

func (t *Tables) Put(info *event.TableInfo)
func (t *Tables) Get(database, table string) *event.TableInfo
func (t *Tables) Remove(database, table string)
func (t *Tables) Apply(delta *parser.TableChanges) *event.TableInfo  // 关键：delta → 新完整 TableInfo
```

#### 初始 Schema 来源

- **全量同步阶段**：从源库 `INFORMATION_SCHEMA` 查询（snapshot 时刻精准快照），作为第一批 CREATE 记录写入 history
- **增量阶段遇到 Tables 里没有的表**：从源库查询（安全的，因为还没有该表的 DDL 在 history 中）
- 类比 Debezium 的 `schema_only_recovery` 模式

### 3.4 DML 事件处理改造

当前（`internal/source/mysql/binlog_syncer.go`）：

```go
tableInfo, err := s.schemaCache.Get(s.ctx, database, table)  // 查 INFORMATION_SCHEMA
```

改为：

```go
tableInfo := s.tables.Get(database, table)  // 从 schema history 恢复的内存 Tables
```

---

## 4. Parser ALTER 解析增强

### 4.1 当前状态

通过代码审计（`pkg/parser/*/visitor.go`），4 个 parser 的 ALTER TABLE 产出质量如下：

| Parser | 列名 | 类型 | Nullable | 位置变更 | Default | 解析方式 |
|--------|------|------|----------|---------|---------|---------|
| MySQL | ✅ | 部分（`GetText()` 原始文本） | ❌ | ❌ 不处理 `AFTER`/`FIRST` | ❌ | ANTLR AST |
| Oracle | ✅ | ❌ | ❌ | ❌ | ❌ | **文本匹配**（不可靠） |
| PostgreSQL | ✅ | ❌ | ❌ | N/A（PG 不支持列位置） | ❌ | ANTLR AST |
| SQL Server | ✅ | ❌ | ❌ | N/A | ❌ | ANTLR AST |

### 4.2 ALTER delta 需要产出的完整信息

```go
type AlterColumnChange struct {
    Operation   AlterColumnOp  // ADD / MODIFY / CHANGE / DROP / RENAME
    OldName     string         // CHANGE/RENAME 时的旧列名
    NewName     string         // 列名（CHANGE/RENAME 时为新列名）
    Type        string         // 完整类型定义: "VARCHAR(255)", "INT UNSIGNED", "DECIMAL(10,2)"
    Nullable    *bool          // nil = 未指定
    Position    *ColumnPosition // MySQL: AFTER col_name / FIRST; 其他 DB: nil
    Default     *string        // DEFAULT 值; nil = 未指定
}

type ColumnPosition struct {
    First bool   // FIRST 关键字
    After string // AFTER column_name
}
```

### 4.3 各 Parser 需要增强的具体内容

#### MySQL

| 待修复 | 说明 | 当前代码位置 |
|--------|------|------------|
| `VisitAlterByChangeColumn` 未实现 | `CHANGE old_col new_col type` 同时改名+改类型 | `mysql/visitor.go` |
| `AFTER`/`FIRST` 位置不处理 | `MODIFY col INT AFTER another_col` 的位置指令被忽略 | `mysql/visitor.go:VisitAlterByModifyColumn` |
| 类型提取不完整 | `GetText()` 拿的是原始文本，需要解析为结构化类型 | `mysql/visitor.go:305-341` |
| Nullable 不提取 | `NOT NULL` / `NULL` 约束未解析 | — |
| Default 不提取 | `DEFAULT value` 未解析 | — |

MySQL 特有的 `CHANGE` 语法示例：
```sql
ALTER TABLE t CHANGE old_col new_col INT NOT NULL DEFAULT 0 AFTER another_col;
-- 一条语句同时：改名 + 改类型 + 改 nullable + 改默认值 + 改位置
```

#### Oracle

| 待修复 | 说明 |
|--------|------|
| 整个 ALTER 解析用文本匹配 | `strings.Contains(upperText, "ADD")` 不可靠，需要重写为 ANTLR visitor |
| 列类型不提取 | 只提取列名 |
| 不支持 `MODIFY` 语法完整解析 | Oracle 的 `MODIFY column datatype` |

#### PostgreSQL

| 待修复 | 说明 |
|--------|------|
| 列类型不提取 | `ADD COLUMN col type` 只提取了 col 名 |
| `ALTER COLUMN ... TYPE` 不处理 | 类型变更未解析 |
| `SET NOT NULL` / `DROP NOT NULL` 不处理 | — |
| `SET DEFAULT` / `DROP DEFAULT` 不处理 | — |

#### SQL Server

| 待修复 | 说明 |
|--------|------|
| `ALTER COLUMN` 不处理 | 类型变更未解析 |
| 列类型不提取 | 同 PostgreSQL |

### 4.4 Tables.Apply() 的列位置处理

MySQL 支持通过 ALTER TABLE 改变列的物理位置，这影响 binlog 中行数据的列顺序。

```sql
CREATE TABLE t (a INT, b INT, c INT);
-- binlog 行数据: [a_val, b_val, c_val]

ALTER TABLE t MODIFY b INT FIRST;
-- 表变为: b INT, a INT, c INT
-- binlog 行数据: [b_val, a_val, c_val]  ← 列顺序变了！
```

`Tables.Apply()` 必须正确处理位置变更：

```go
func (t *Tables) Apply(delta *parser.TableChanges) *event.TableInfo {
    existing := t.Get(delta.Database, delta.Table)
    
    for _, change := range delta.ModifiedColumns {
        if change.Position != nil {
            if change.Position.First {
                // 移到第一个位置
                removeColumn(existing, change.OldName)
                insertColumnAt(existing, 0, newColumnInfo)
            } else if change.Position.After != "" {
                // 移到指定列之后
                removeColumn(existing, change.OldName)
                idx := findColumnIndex(existing, change.Position.After)
                insertColumnAt(existing, idx+1, newColumnInfo)
            }
        }
    }
    
    return existing
}
```

---

## 5. 文件影响清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `pkg/event/schema_history.go` | 新增 | SchemaHistoryRecord + SchemaHistory 接口 |
| `internal/schema/tables.go` | 新增 | Tables 内存表集合 + Apply() 列位置处理 |
| `internal/schema/local_history.go` | 新增 | LocalSchemaHistory 存储实现 |
| `internal/schema/tables_test.go` | 新增 | Apply delta 测试（含列位置变更） |
| `internal/schema/local_history_test.go` | 新增 | Record + Recover 测试 |
| `pkg/parser/mysql/visitor.go` | 修改 | ALTER 增强：CHANGE、AFTER/FIRST、完整类型、Nullable |
| `pkg/parser/oracle/visitor.go` | 修改 | ALTER 重写为 ANTLR AST |
| `pkg/parser/postgres/visitor.go` | 修改 | ALTER 增强：类型、SET NOT NULL、SET DEFAULT |
| `pkg/parser/sqlserver/visitor.go` | 修改 | ALTER 增强：ALTER COLUMN 类型变更 |
| `pkg/parser/types.go` (或类似) | 修改 | AlterColumnChange + ColumnPosition 类型定义 |
| `internal/source/mysql/binlog_syncer.go` | 修改 | handleRowsEvent 改从内存 Tables 取 schema |
| `internal/source/mysql/connector.go` | 修改 | 启动时 Recover schema history |
| `internal/source/oracle/connector.go` | 修改 | 同上 |
| `internal/source/postgres/connector.go` | 修改 | 同上 |
| `internal/source/sqlserver/connector.go` | 修改 | 同上 |
| `internal/source/mariadb/connector.go` | 修改 | 同上 |
| `internal/source/*/schema_cache.go` | 废弃 | 不再从 INFORMATION_SCHEMA 查询（逐步迁移后删除） |
| `internal/cache/local_backend.go` | 修改 | fsync + CRC32 + 事务完整性 |
| `internal/lifecycle/pipeline_integration.go` | 修改 | Route 错误处理 |

---

## 6. 优先级

| 优先级 | 工作项 | 说明 |
|--------|--------|------|
| **P0** | 缓冲文件事务完整性 | 全局 WAL（方案 A）或 committed_gtids（方案 B）待决策后实现 |
| **P0** | Schema History 接口 + 存储 + Recover | 核心机制，所有 connector 依赖 |
| **P0** | Tables.Apply() 含列位置处理 | ALTER 后列顺序影响 binlog 行数据解析 |
| **P1** | MySQL parser ALTER 增强 | CHANGE 语法 + AFTER/FIRST + 完整类型 + Nullable |
| **P1** | Oracle parser ALTER 重写 | 文本匹配 → ANTLR AST |
| **P1** | PostgreSQL/SQL Server parser ALTER 增强 | 类型提取 + ALTER COLUMN |
| **P2** | 废弃 schema_cache.go | 迁移完成后删除 INFORMATION_SCHEMA 查询路径 |
| **P2** | fsync 模式配置化 | every / batch / none 可选 |
| **P2** | CRC32 记录校验 | 检测静默数据损坏 |

---

## 7. 待决策事项

| 编号 | 事项 | 选项 | 状态 |
|------|------|------|------|
| D1 | 缓冲事务完整性方案 | A. 全局 WAL + 按表索引 / B. 分文件 + committed_gtids | **未决** |
| D2 | Schema History 存储后端 | 单文件（全量记录顺序追加）/ 按表分文件 / 复用 LocalBackend | **未决** |
| D3 | History 序列化格式 | Protobuf / JSON | **未决** |
| D4 | catching_up UPSERT 安全窗口 | 删除 / 降级为可选（默认关闭）| 取决于 D1 |

---

*返回 [设计文档总览](./Design.md)*
