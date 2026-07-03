# DataStream 端到端集成设计

> 创建时间：2026-06-26
> 状态：**已确认**
> 版本：v1.0

---

## 1. 背景

B1-B4 组件已全部实现（缓冲完整性、Schema History 存储、Parser ApplyDDL、DDLRecordManager），但尚未串通端到端链路。本文档定义集成方案，覆盖：

1. 统一存储层（目标库 ds_{task_id}）
2. DDL 同步阻塞执行
3. Connector 集成模式（公共接口 + 独立实现）
4. 功能完善优先级

---

## 2. 统一存储层

所有任务元数据存储在目标端数据库 `ds_{task_id}` 中，不依赖本地文件。

### 2.1 表结构

```sql
CREATE DATABASE IF NOT EXISTS ds_{task_id};

-- 1. 任务位点
CREATE TABLE ds_{task_id}.task_position (
    id                INT PRIMARY KEY DEFAULT 1,
    flushed_position  JSON NOT NULL,     -- 已成功写入目标的点位（恢复起点）
    current_position  JSON NOT NULL,     -- 当前正在执行的点位（失败/崩溃时卡在这里）
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 2. 表生命周期状态
CREATE TABLE ds_{task_id}.table_lifecycle (
    db_name           VARCHAR(255) NOT NULL,
    tbl_name          VARCHAR(255) NOT NULL,
    state             VARCHAR(32) NOT NULL,   -- pending/snapshotting/catching_up/streaming/error/paused
    snapshot_position JSON,
    error_msg         TEXT,
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (db_name, tbl_name)
);

-- 3. Schema History（DDL 历史，只追加）
CREATE TABLE ds_{task_id}.schema_history (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    position    JSON NOT NULL,
    db_name     VARCHAR(255) NOT NULL,
    tbl_name    VARCHAR(255) NOT NULL,
    ddl         TEXT NOT NULL,
    table_info  JSON NOT NULL,            -- parser 计算的自维护表结构
    change_type VARCHAR(32) NOT NULL,     -- CREATE/ALTER/DROP
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_table (db_name, tbl_name)
);

-- 4. DDL 执行状态
CREATE TABLE ds_{task_id}.ddl_state (
    db_name           VARCHAR(255) NOT NULL,
    tbl_name          VARCHAR(255) NOT NULL,
    ddl               TEXT NOT NULL,
    last_success_info JSON,               -- 上次成功的 table_info（回退用）
    status            VARCHAR(32) NOT NULL, -- applying/failed
    error_msg         TEXT,
    retry_count       INT DEFAULT 0,
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (db_name, tbl_name)
);

-- 5. Committed Position（MySQL GTID 专用）
CREATE TABLE ds_{task_id}.committed_position (
    id          INT PRIMARY KEY DEFAULT 1,
    gtid_set    TEXT NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

### 2.2 Position 更新时机

```
收到事件 at position P
→ UPDATE current_position = P        ← 每条事件都更新
→ sink 写入成功
→ UPDATE flushed_position = P        ← 只在 sink 确认后更新
```

### 2.3 启动 Recover 流程

```
1. SELECT * FROM task_position → 恢复 flushed_position 作为 connector 起点
2. SELECT * FROM table_lifecycle → 恢复每张表的状态机
3. SELECT * FROM schema_history ORDER BY id → 回放 Tables
4. SELECT * FROM ddl_state WHERE status='applying' → 重试未完成 DDL
5. SELECT * FROM committed_position → MySQL GTID 恢复
```

### 2.4 设计原则

- **SchemaHistory 是 DataStream 自己的 schema 真相**，不依赖源端也不依赖目标端
- 源端 schema 可能有延迟（刻舟求剑），目标端可能不支持某些 DDL
- NewTableInfo 由 parser 基于旧 schema + DDL 计算得出
- 只有目标端 DDL 执行成功后才写入 SchemaHistory + 更新 Tables

---

## 3. DDL 同步阻塞执行

### 3.1 核心规则

- DDL 操作均为同步阻塞操作
- DDL 到达时必须先 Flush 所有待处理的 DML 事件
- DDL 执行成功后才更新 Tables 和 SchemaHistory
- DDL 失败后阻塞重试，超过阈值报错人工介入

### 3.2 执行流程

```
DDL 事件到达（position P）
→ 1. Flush：等待所有 pending DML 事件写入目标端完成
→ 2. 记录：INSERT/UPDATE ddl_state (status=applying, last_success_info=当前 table_info)
→ 3. 执行：sink.ApplyDDL(ddl)
→ 4a. 成功：
    → INSERT schema_history (position=P, table_info=NewTableInfo)
    → DELETE ddl_state
    → Tables.Put(NewTableInfo)
    → 继续处理后续事件
→ 4b. 失败：
    → UPDATE ddl_state (status=failed, error_msg, retry_count+1)
    → 该表暂停，其他表继续
    → 下次事件到达时自动重试
    → retry_count > 阈值 → 不再重试，等待人工介入
```

### 3.3 人工介入操作

| 操作 | 效果 |
|------|------|
| **skip** | 跳过该 DDL，用 last_success_info 更新 Tables，继续 |
| **manual** | 运维在目标端手动执行 DDL，标记完成，继续 |
| **retry** | 重置 retry_count，重新自动重试 |

### 3.4 崩溃恢复

```
进程崩溃 at position P
→ 重启 → flushed_position = P'（P' < P）
→ connector 从 P' 开始重放
→ DDL 事件 at P 到达
→ ddl_state 中有记录 → 检查目标端该 DDL 是否已执行
  → 已执行: 标记 completed，更新 Tables
  → 未执行: 重新执行
```

---

## 4. Connector 集成模式

### 4.1 设计原则

- 每个 connector 独立实现（各源特殊逻辑多）
- 定义公共接口契约，pipeline 层面向接口编程
- 各 connector 根据自身特点实现接口

### 4.2 公共接口

```go
// SchemaProvider 提供表结构信息
type SchemaProvider interface {
    // GetSchema 从内存 Tables 获取表结构
    GetSchema(database, table string) *event.TableInfo
}

// DDLHandler 处理 DDL 事件
type DDLHandler interface {
    // HandleDDL 解析 DDL 并返回结果
    HandleDDL(ctx context.Context, oldTable *event.TableInfo, ddl string) (*parser.DDLResult, error)
}

// PositionTracker 追踪位点
type PositionTracker interface {
    GetPosition() *event.Position
    SetPosition(pos *event.Position)
}

// ConnectorState 连接器状态查询
type ConnectorState interface {
    // GetSourceType 返回源数据库类型
    GetSourceType() cache.SourceType
}
```

### 4.3 各 Connector 集成方案

#### MySQL（已完成）

- SchemaProvider: `tables.Get()` 优先，fallback `schemaCache`
- DDLHandler: `parser.ApplyDDL()` + Tables 更新
- PositionTracker: BinlogSyncer.position
- 特殊: GTID 事务标记 + committed.position

#### MariaDB

- 基于 MySQL 实现，复用 BinlogSyncer
- 差异: GTID 格式不同（domain:server_id:sequence）
- 集成方式: 复用 MySQL 的 Tables + DDL 路径

#### PostgreSQL

- SchemaProvider: `tables.Get()` 优先，fallback 查询 `information_schema`
- DDLHandler: PG parser `ApplyDDL()`
- PositionTracker: LSN（uint64，操作级精度）
- 特殊: Logical Replication 消息自带列信息，DML 对 schema 依赖较低

#### Oracle

- SchemaProvider: `tables.Get()`（Oracle DML schema-free，Tables 主要用于 DDL 跟踪）
- DDLHandler: Oracle parser `ApplyDDL()`
- PositionTracker: SCN（uint64，操作级精度）
- 特殊: LogMiner SQL_REDO 自描述，DML 不依赖 schema

#### SQL Server

- SchemaProvider: `tables.Get()` 优先，fallback 查询 `INFORMATION_SCHEMA`
- DDLHandler: SQLServer parser `ApplyDDL()`
- PositionTracker: (ChangeLsn, SeqVal) 组合
- 特殊: CDC 需要 seqval 才能精确恢复

#### MongoDB

- SchemaProvider: 不需要（文档数据库天然自描述）
- DDLHandler: 不需要（无 DDL 概念，schema 变更是隐式的）
- PositionTracker: resume_token（opaque BSON）
- 特殊: schema-free，完全不走 Schema History 路径

### 4.4 统一存储层接入

每个 connector 启动时：

```go
func (c *Connector) Initialize(ctx context.Context, config source.Config) error {
    // 1. 连接目标库（通过 sink 配置获取目标库连接信息）
    targetDB, err := connectTargetDB(config)

    // 2. 创建统一存储层
    store, err := NewTargetStore(targetDB, taskID)

    // 3. Recover
    flushedPos, err := store.LoadFlushedPosition(ctx)
    tableStates, err := store.LoadTableLifecycle(ctx)
    err = store.RecoverSchemaHistory(ctx, tables)

    // 4. 初始化 Tables
    c.tables = schema.NewTables()
    // ... 从 schema_history 回放 Tables
}
```

---

## 5. 功能完善优先级

| 优先级 | 事项 | 说明 | 依赖 |
|--------|------|------|------|
| **P0** | 统一存储层实现 | TargetStore 接口 + MySQL 实现 | 无 |
| **P0** | MySQL connector 接入统一存储 | 替换本地文件存储 | P0 统一存储 |
| **P0** | 6 个 connector 集成 | Tables + DDL + 统一存储 | P0 统一存储 |
| **P0** | DDL 同步阻塞 + Flush | pipeline 层改造 | P0 connector 集成 |
| **P1** | S3 全量中转路径 | 大表 snapshot 需要 | 无 |
| **P1** | 时区 UTC 归一化 | 跨时区部署必须 | 无 |
| **P2** | 测试覆盖率提升 | 持续改进 | P0 全部完成 |
| **P2** | 废弃 schema_cache.go | 所有 connector 走 Tables 后删除 | P0 全部完成 |

---

## 6. 测试矩阵

6 源 × 8 目标 = 48 种组合，全部需要测试。

| 源 \ 目标 | MySQL | PG | MongoDB | Kafka | ES | Redis | Oracle | SQLServer |
|-----------|-------|----|---------|-------|----|----|--------|-----------|
| MySQL | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| MariaDB | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| PostgreSQL | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Oracle | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| SQL Server | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| MongoDB | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

每种组合测试内容：
1. 全量同步（snapshot）
2. 增量同步（DML）
3. DDL 同步（ADD/DROP/MODIFY/RENAME COLUMN）
4. 进程崩溃恢复
5. DDL 失败重试

---

*返回 [设计文档总览](../Design.md)*
