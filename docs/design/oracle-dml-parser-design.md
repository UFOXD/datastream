# Oracle LogMiner DML Parser 重构设计

> 创建时间：2026-05-23
> 版本：v1.0

---

## 1. 背景与问题

### 1.1 现状

当前 Oracle Source Connector 使用正则表达式解析 LogMiner `V$LOGMNR_CONTENTS.SQL_REDO` 输出的 DML 语句（`internal/source/oracle/logminer.go:257-401`）。

Oracle LogMiner 的 `SQL_REDO` 列输出的是**重建的 SQL 文本**，不是 MySQL binlog 那样的 row-image 二进制格式。这是 Oracle Redo Log 架构决定的——LogMiner 将 Redo 条目反向翻译为 SQL 语句。

### 1.2 问题分析

对照 Debezium 的实现（`LogMinerDmlParser.java`，736 行，手写状态机），当前正则方案存在以下缺陷：

| 问题 | 当前实现（正则） | Debezium（状态机） | 影响 |
|------|-----------------|-------------------|------|
| **引号内逗号** | `colAssignRe` 的 `[^,]+?` 在 `'hello, world'` 处断裂 | `inSingleQuote` 状态下所有字符直接 append，逗号不触发分割 | 含逗号的字符串值被截断，数据丢失 |
| **嵌套函数** | 无嵌套处理，`TO_TIMESTAMP('2020-01-01', 'YYYY-MM-DD')` 内部逗号被错切 | `nested` 计数器追踪括号深度 | DATE/TIMESTAMP 类型字段解析失败 |
| **转义引号** | 无 `''` 转义处理 | `lookAhead == '\''` 时 append 单引号并跳过 | 含单引号的字符串值（如 `O'Brien`）解析失败 |
| **UPDATE before-image** | `parseUpdate` 只解析 SET 子句，丢失 WHERE 子句 | `parseSetClause` + `parseWhereClause` 都解析 | Update 事件无 Before 数据，下游无法做冲突检测 |
| **NULL 处理** | 无 NULL / Unsupported Type 识别 | 区分 `NULL`、`NULL_SENTINEL`、`Unsupported Type` | NULL 值被当作普通字符串 |
| **IS NULL** | 不支持 | WHERE 子句中检测 `IS NULL` 关键字 | WHERE 含 IS NULL 的行解析失败 |

### 1.3 证据来源

- Debezium 源码：`~/Codes/dts/debezium/debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/logminer/parser/LogMinerDmlParser.java`
- Debezium 补充日志检查：`~/Codes/dts/debezium/debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/logminer/AbstractLogMinerStreamingChangeEventSource.java:1757-1773`
- DataStream 当前实现：`~/Codes/dts/datastream/internal/source/oracle/logminer.go:257-401`

---

## 2. 设计目标

1. 使用手写逐字符状态机替换正则表达式，正确处理所有已知边界 case
2. 解析器 Schema-free：只负责 SQL 文本 → 列名+原始字符串值，不依赖表结构
3. UPDATE 同时输出 NewValues（SET 子句）和 OldValues（WHERE 子句）
4. 与现有 `event.RowData` 接口兼容，Sink 侧不需要改动
5. 零外部依赖

---

## 3. Oracle LogMiner UPDATE 行为说明

Oracle LogMiner UPDATE 的 SET 子句**只输出变更的列，不是全部列**。WHERE 子句的内容取决于 supplemental logging 配置：

| 配置 | SET 子句 | WHERE 子句 |
|------|---------|-----------|
| 最小补充日志 `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA` | 仅变更列 | 仅主键/唯一键列 |
| 表级全列补充日志 `ALTER TABLE t ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` | 仅变更列 | 所有列（完整 before-image） |
| 数据库级全补充日志 `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` | 仅变更列 | 所有列 |

**DataStream 要求用户启用表级或数据库级 `(ALL) COLUMNS` 补充日志**，否则 WHERE 子句只有键列，before-image 不完整。

来源：Debezium `AbstractLogMinerStreamingChangeEventSource.java:1771`：
```
"Database table '{}' not configured with supplemental logging \"(ALL) COLUMNS\";
 only explicitly changed columns will be captured."
```

---

## 4. 接口与数据结构

### 4.1 解析结果

```go
// DmlType DML 操作类型
type DmlType int

const (
    DmlInsert DmlType = iota
    DmlUpdate
    DmlDelete
)

// DmlEntry 一条 DML 解析结果
type DmlEntry struct {
    Type      DmlType
    NewValues map[string]string  // INSERT: VALUES 提取; UPDATE: SET 子句提取
    OldValues map[string]string  // DELETE: WHERE 提取; UPDATE: WHERE 子句提取
}
```

值约定：
- key 存在且 value == `"NULL"` → 显式 NULL
- key 不存在 → 该列未出现在 SQL 中
- value == `"Unsupported Type"` → 跳过该列，不写入 map
- 函数调用（如 `TO_TIMESTAMP(...)`）→ 完整保留为字符串值

### 4.2 解析器接口

```go
// DmlParser LogMiner SQL_REDO 解析器
type DmlParser interface {
    Parse(sql string) (*DmlEntry, error)
}
```

不接收 Table 参数。类型推断和 Schema 校验由上层负责。

---

## 5. 状态机解析器设计

### 5.1 整体架构

```
输入: insert into "HR"."EMP"("ID","NAME") values (1,'O''Brien');
                   │
                   ▼
            ┌──────────────┐
            │ 识别 DML 类型 │  ← 首字符 i/u/d 分流
            └──────┬───────┘
                   ▼
            ┌──────────────┐
            │ 跳过表名      │  ← 扫描 "OWNER"."TABLE" 到边界
            └──────┬───────┘
                   ▼
       ┌───────────┼───────────┐
       ▼           ▼           ▼
  INSERT:      UPDATE:     DELETE:
  列名列表     SET子句      WHERE子句
  VALUES子句   WHERE子句
       │           │           │
       ▼           ▼           ▼
            ┌──────────────┐
            │  DmlEntry    │
            │  NewValues   │
            │  OldValues   │
            └──────────────┘
```

### 5.2 内部结构

```go
type dmlParser struct {
    sql    string
    pos    int
    length int
}

func NewDmlParser() DmlParser

// 公开入口
func (p *dmlParser) Parse(sql string) (*DmlEntry, error)

// 内部方法（非导出）
func (p *dmlParser) parseInsert() (*DmlEntry, error)
func (p *dmlParser) parseUpdate() (*DmlEntry, error)
func (p *dmlParser) parseDelete() (*DmlEntry, error)

// 子句解析
func (p *dmlParser) skipTableName()
func (p *dmlParser) parseColumnList() ([]string, error)
func (p *dmlParser) parseValueList(numCols int) ([]string, error)
func (p *dmlParser) parseSetClause() (map[string]string, error)
func (p *dmlParser) parseWhereClause() (map[string]string, error)

// 原子操作
func (p *dmlParser) readQuotedName() string    // 读 "COL_NAME"
func (p *dmlParser) readValue() string         // 读一个值（处理引号/嵌套/NULL）
func (p *dmlParser) skipSpaces()
func (p *dmlParser) peek() byte
func (p *dmlParser) advance() byte
func (p *dmlParser) expect(s string) error     // 断言并跳过固定前缀
```

### 5.3 核心状态变量（方法局部）

每个 `parseXxxClause` 方法内部使用局部变量管理状态，不在结构体上持久化：

- `inSingleQuote bool` — 是否在 `'...'` 内
- `inDoubleQuote bool` — 是否在 `"..."` 内
- `nested int` — 括号嵌套深度

### 5.4 边界 case 处理

| Case | LogMiner 输出示例 | 处理方式 |
|------|------------------|---------|
| 单引号转义 | `'O''Brien'` | `inSingleQuote` 时遇到 `''` → append 一个 `'`，继续 |
| 函数调用 | `TO_TIMESTAMP('2020-01-01','YYYY-MM-DD HH24:MI:SS')` | `nested++` 跟踪括号深度，内部逗号不分割，完整保留函数文本 |
| NULL 值 | `NULL` | 不在引号内的 `NULL` → 值为字符串 `"NULL"` |
| IS NULL | `"COL" IS NULL` | WHERE 子句中检测 `IS NULL` 关键字，该列值设为 `"NULL"` |
| Unsupported Type | `Unsupported Type` | 跳过该列，不写入 map |
| 值内逗号 | `'hello, world'` | `inSingleQuote == true` 时逗号不触发分割 |
| 字符串拼接 | `'abc' \|\| 'def'` | 检测 `\|\|`，跳过空白继续收集值 |
| 空 WHERE | UPDATE 无 WHERE | 返回空 OldValues，不报错（参考 Debezium DBZ-3235） |

---

## 6. 上层集成

### 6.1 UPDATE 合并逻辑

解析器输出 `NewValues`（SET，仅变更列）和 `OldValues`（WHERE，全列）后，上层执行合并：

```go
// mergeUpdateValues 将 WHERE 中未被 SET 覆盖的列复制到 NewValues
func mergeUpdateValues(entry *DmlEntry) {
    for col, oldVal := range entry.OldValues {
        if _, exists := entry.NewValues[col]; !exists {
            entry.NewValues[col] = oldVal
        }
    }
}
```

合并后：
- `NewValues` = 完整 after-image（SET 变更列 + WHERE 未变更列）
- `OldValues` = 完整 before-image（WHERE 原样）

### 6.2 类型转换

```go
// entryToRowData 将 map[string]string 转为 event.RowData
func entryToRowData(vals map[string]string) event.RowData {
    rd := event.NewRowData()
    for col, raw := range vals {
        rd.Set(col, parseValue(raw), "")  // 复用现有 parseValue 做类型推断
    }
    return *rd
}
```

`parseValue` 函数保留在 `logminer.go` 中，处理 `NULL` / 单引号字符串 / 数值字面量 / 函数调用原样透传。

### 6.3 `logminer.go` 改动

`parseRows` 方法中替换调用方式：

```go
parser := NewDmlParser()

case 1: // Insert
    entry, err := parser.Parse(sqlRedo.String)
    // ...
    ev.After = entryToRowData(entry.NewValues)

case 2: // Delete
    entry, err := parser.Parse(sqlRedo.String)
    // ...
    ev.Before = entryToRowData(entry.OldValues)

case 3: // Update
    entry, err := parser.Parse(sqlRedo.String)
    mergeUpdateValues(entry)
    // ...
    ev.After = entryToRowData(entry.NewValues)
    ev.Before = entryToRowData(entry.OldValues)  // ← 新增
```

### 6.4 删除的代码

`logminer.go:257-401` 以下内容删除：
- 4 个正则变量：`insertColsRe` / `updateSetRe` / `deleteWhereRe` / `colAssignRe`
- 3 个正则解析函数：`parseInsert` / `parseUpdate` / `parseDelete`
- 2 个辅助函数：`splitQuotedList` / `splitValueList`
- `import "regexp"` 删除

保留 `parseValue` 函数（上层类型推断仍需使用）。

---

## 7. 文件结构

```
internal/source/oracle/
├── connector.go           # 不动
├── logminer.go            # 修改：删除正则代码，集成新解析器
├── dml_parser.go          # 新增：状态机解析器
├── dml_parser_test.go     # 新增：解析器测试（TDD）
├── schema_cache.go        # 不动
└── stats.go               # 不动
```

---

## 8. 测试矩阵

开发以 TDD 模式进行：先写测试用例，再实现解析器代码。

### 8.1 解析器单元测试 (`dml_parser_test.go`)

| 编号 | 场景 | 输入 SQL | 验证点 |
|------|------|---------|--------|
| T01 | 基本 INSERT 多列 | `insert into "HR"."EMP"("ID","NAME","SALARY") values (1,'John',5000);` | NewValues: ID=1, NAME='John', SALARY=5000 |
| T02 | 基本 INSERT 单列 | `insert into "HR"."T"("ID") values (42);` | NewValues: ID=42 |
| T03 | 基本 UPDATE | `update "HR"."EMP" set "NAME" = 'Jane' where "ID" = 1 and "NAME" = 'John';` | NewValues: NAME='Jane'; OldValues: ID=1, NAME='John' |
| T04 | 基本 DELETE | `delete from "HR"."EMP" where "ID" = 1 and "NAME" = 'John';` | OldValues: ID=1, NAME='John' |
| T05 | 单引号转义 | `insert into "S"."T"("C") values ('O''Brien');` | NewValues: C=O'Brien |
| T06 | 值内逗号 | `insert into "S"."T"("C") values ('hello, world');` | NewValues: C=hello, world |
| T07 | TO_TIMESTAMP | `update "S"."T" set "D" = TO_TIMESTAMP('2020-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS') where "ID" = 1;` | NewValues.D = 完整 TO_TIMESTAMP 字符串 |
| T08 | TO_DATE 嵌套 | `insert into "S"."T"("ID","D") values (1,TO_DATE('2020-01-01','YYYY-MM-DD'));` | NewValues.D = 完整 TO_DATE 字符串 |
| T09 | NULL 值 INSERT | `insert into "S"."T"("A","B") values (1,NULL);` | NewValues: A=1, B=NULL |
| T10 | IS NULL in WHERE | `delete from "S"."T" where "A" = 1 and "B" IS NULL;` | OldValues: A=1, B=NULL |
| T11 | Unsupported Type | `insert into "S"."T"("A","B") values (1,Unsupported Type);` | NewValues 只含 A=1，B 被跳过 |
| T12 | 字符串拼接 | `update "S"."T" set "C" = 'abc' \|\| 'def' where "ID" = 1;` | NewValues.C = 拼接后完整值 |
| T13 | 空 WHERE | `update "S"."T" set "C" = 'v';` | NewValues: C='v', OldValues: 空 map |
| T14 | UPDATE 多列 SET | `update "S"."T" set "A" = 1, "B" = 'x', "C" = NULL where "A" = 0 and "B" = 'y' and "C" = 'z';` | NewValues: A=1,B='x',C=NULL; OldValues: A=0,B='y',C='z' |
| T15 | DML 类型识别错误 | `select * from "T"` | 返回 error |
| T16 | TO_TIMESTAMP_TZ | `update "S"."T" set "D" = TO_TIMESTAMP_TZ('2024-02-14 10:58:02.202590 +01:00') where "ID" = 1;` | NewValues.D = 完整 `TO_TIMESTAMP_TZ(...)` 字符串 |
| T17 | TO_DATE 在 WHERE | `delete from "S"."T" where "ID" = 1 and "D" = TO_DATE('15-MAY-21', 'DD-MON-RR');` | OldValues.D = 完整 `TO_DATE(...)` 字符串 |
| T18 | 多时间列 UPDATE | `update "S"."T" set "TS" = TO_TIMESTAMP('2020-02-02 00:00:00.'), "TZ" = TO_TIMESTAMP_TZ('2020-02-02 00:00:00.000000 +08:00') where "TS" = TO_TIMESTAMP('2020-02-01 00:00:00.') and "TZ" = TO_TIMESTAMP_TZ('2020-02-01 00:00:00.000000 +08:00');` | NewValues/OldValues 分别完整保留两个函数调用 |

### 8.2 集成测试（`logminer.go` 层）

| 编号 | 场景 | 验证点 |
|------|------|--------|
| I01 | UPDATE 合并逻辑 | mergeUpdateValues 后 NewValues 包含 WHERE 中未变更列 |
| I02 | entryToRowData 类型转换 | parseValue 正确处理 NULL/字符串/数值/函数调用 |
| I03 | ChangeEvent.Before 填充 | Update 事件同时有 After 和 Before |

---

## 9. 前置要求

用户必须在 Oracle 数据库上启用补充日志：

```sql
-- 最低要求：数据库级最小补充日志
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- 推荐：每个同步表启用全列补充日志（获取完整 before-image）
ALTER TABLE schema.table_name ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- 或：数据库级全列补充日志
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

如未启用 `(ALL) COLUMNS`，UPDATE 的 WHERE 子句仅包含主键/唯一键列，before-image 不完整。DataStream 应在连接初始化时检查并发出警告日志。

---

## 10. 与 Debezium 的关键差异

| 维度 | Debezium | DataStream |
|------|----------|-----------|
| 语言 | Java，736 行 | Go，预计 400-500 行 |
| Schema 依赖 | 解析时传入 Table，按列索引填充 `Object[]` | Schema-free，返回 `map[string]string` |
| 类型转换 | 解析器内部处理 | 两阶段：解析器输出原始字符串，上层用 `parseValue` 转换 |
| NULL 哨兵 | `NULL_SENTINEL` 对象引用 | 字符串值 `"NULL"` + map key 存在性判断 |
| LOB/XML | `ParserUtils.setColumnUnavailableValues`、`SelectLobParser`、`XmlWriteParser` | 暂不支持，后续按需扩展 |
| 解析器复用 | 有状态（`rowArchivalColumnIndex`） | 无状态，每次 `Parse` 调用重置 `pos` |

---

## 11. 时区处理策略

### 11.1 问题背景

Oracle 有 5 种时间/日期类型，LogMiner 的 `SQL_REDO` 对每种类型输出不同格式的函数调用：

| Oracle 类型 | LogMiner SQL_REDO 输出 | 时区信息 |
|-------------|----------------------|---------|
| `DATE` | `TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')` | 无 |
| `TIMESTAMP` | `TO_TIMESTAMP('2020-02-01 00:00:00.')` | 无 |
| `TIMESTAMP WITH TIME ZONE` | `TO_TIMESTAMP_TZ('2024-02-14 10:58:02.202590 +01:00')` | 有，显式偏移量 |
| `TIMESTAMP WITH LOCAL TIME ZONE` | `TO_TIMESTAMP_TZ('2024-02-14 10:58:02.202590 +01:00')` | 有，但是源库 session 时区 |

证据来源：
- Debezium `XmlBeginParserTest.java:143` — `TO_TIMESTAMP_TZ('2024-02-14 10:58:02.202590 +01:00')`
- Debezium `LogMinerDmlParserTest.java:66-67` — `TO_TIMESTAMP('2020-02-01 00:00:00.')` + `TO_DATE(...)`
- Debezium `OracleValueConverters.java:79-92` — TZ 解析格式定义

### 11.2 风险场景

直接透传函数调用字符串到 Sink 侧会导致以下问题：

| 场景 | 风险 |
|------|------|
| Oracle `TIMESTAMP WITH TIME ZONE` → MySQL `DATETIME` | MySQL 无时区类型，写入时丢失时区信息。源端 `10:58:02 +01:00` 到目标变成 `10:58:02`（实际应为 UTC `09:58:02`） |
| Oracle `TIMESTAMP WITH LOCAL TIME ZONE` → 任意目标 | LogMiner 输出的是源库 session 时区的值。源库 `Asia/Shanghai` 下 `10:00:00 +08:00`，目标库 session 为 `UTC`，直接透传差 8 小时 |
| Oracle `DATE`/`TIMESTAMP`（无时区）→ 不同时区目标 | 隐含时区依赖源库 session。跨时区部署时"同一个值"在目标库含义不同 |

这是生产环境中验证过的高频踩坑点。

### 11.3 设计方案：Source 侧 UTC 归一化

参考 Debezium 的生产验证方案（`OracleValueConverters.java:700-726`）：

**原则：所有时间值在 Source 侧统一转为 UTC，Sink 拿到的永远是 UTC 时间。**

```
LogMiner SQL_REDO
       │
       ▼
  DmlParser (本次实现)
  输出原始字符串: "TO_TIMESTAMP_TZ('2024-02-14 10:58:02.202590 +01:00')"
       │
       ▼
  TemporalConverter (后续迭代)
  识别函数 → 提取内部值 → 解析为 time.Time → 转 UTC
  输出: time.Time{2024-02-14 09:58:02.202590 UTC}
       │
       ▼
  event.RowData
  Sink 侧按目标库类型写入
```

#### 转换规则

| 函数 | 提取正则 | 解析格式 | UTC 转换 |
|------|---------|---------|---------|
| `TO_DATE('...', 'fmt')` | `TO_DATE\('([^']+)'` | `2006-01-02 15:04:05` | 按配置的 `source.timezone` 偏移转 UTC |
| `TO_TIMESTAMP('...')` | `TO_TIMESTAMP\('([^']+)'` | `2006-01-02 15:04:05.999999999` | 按配置的 `source.timezone` 偏移转 UTC |
| `TO_TIMESTAMP_TZ('...')` | `TO_TIMESTAMP_TZ\('([^']+)'` | `2006-01-02 15:04:05.999999999 -07:00` | 值自带偏移量，直接转 UTC |

#### 配置项

```toml
[source.connection]
# Oracle session 时区，用于 DATE/TIMESTAMP（无时区类型）的 UTC 转换
# 必须与源库 session 时区一致
# 默认: UTC
timezone = "Asia/Shanghai"
```

### 11.4 实现分层

| 层 | 职责 | 本次迭代范围 |
|----|------|------------|
| DmlParser | SQL 文本 → 列名+原始字符串值 | **是** |
| TemporalConverter | 识别时间函数 → 解析 → UTC 归一化 | 否（后续迭代） |
| parseValue | 通用类型推断（NULL/字符串/数值/透传） | **是**（保留现有逻辑） |

本次迭代中，`parseValue` 对 `TO_TIMESTAMP`/`TO_DATE`/`TO_TIMESTAMP_TZ` 仍原样透传字符串。DmlParser 的设计已确保这些函数调用被完整保留（嵌套括号处理），为后续 TemporalConverter 接入预留了干净的接口。

### 11.5 跨数据库时区统一

此问题不限于 Oracle。其他 Source Connector 同样存在：

| Source | 无时区类型 | 带时区类型 | 处理策略 |
|--------|----------|-----------|---------|
| Oracle | `DATE`, `TIMESTAMP` | `TIMESTAMP WITH [LOCAL] TIME ZONE` | 本节方案 |
| MySQL | `DATETIME`, `TIMESTAMP`（隐含 session TZ） | 无原生 TZ 类型 | `source.timezone` 配置 + UTC 转换 |
| PostgreSQL | `timestamp` | `timestamptz` | PG 内部存 UTC，输出带偏移量 |
| SQL Server | `datetime`, `datetime2` | `datetimeoffset` | 类似 Oracle 方案 |

**建议后续抽象为 `pkg/temporal` 包**，提供统一的时间值转换接口，各 Connector 共用：

```go
package temporal

// Converter 时间值转换器
type Converter interface {
    // ToUTC 将数据库特定格式的时间字符串转为 UTC time.Time
    ToUTC(raw string, sourceTimezone *time.Location) (time.Time, error)
}
```

各数据库的 TemporalConverter 实现该接口，注册到 Converter 中。

---

*返回 [设计文档总览](./Design.md)*
