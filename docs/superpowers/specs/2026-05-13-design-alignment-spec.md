# 设计文档对齐与功能补充规格

> **Created:** 2026-05-13
> **Status:** Draft for approval

---

## 背景

对比设计文档与实际实现，发现以下差距需要处理：
1. 目录结构设计 vs 实际不一致
2. 接口签名有差异
3. DatabaseDiscovery 和 TableManager 未实现

---

## 1. 目录结构决策

### 设计文档要求
```
pkg/
├── pipeline/
├── filter/
├── transform/
└── router/
```

### 实际实现
```
internal/
├── pipeline/
├── filter/
├── transform/
└── router/
```

### 决策：保持 `internal/`

**理由：**
- Filter/Transform/Router 是业务特定逻辑，不是通用库
- Go 社区最佳实践：`internal/` 存放项目私有代码
- 这些模块依赖 `event.ChangeEvent`，不是独立可复用的

**行动：** 更新 `docs/design/pipeline-design.md` 模块结构章节

---

## 2. Source Connector 接口对齐

### 当前差异

| 设计文档 | 实际实现 | 决策 |
|---------|---------|------|
| `Stop()` | `Stop(ctx context.Context)` | 保留实际（更好） |
| `Position()` | `GetPosition()` | 保留实际（语义更清晰） |
| `Seek(position)` | `SetPosition(pos)` | 保留实际（语义更清晰） |
| `Schemas()` | ❌ 未实现 | **需要添加** |

### 需要添加的方法

```go
// Schemas 返回所有已知的表 Schema
func (c *Connector) Schemas() map[string]*event.TableInfo
```

**实现位置：** `internal/source/mysql/connector.go`

---

## 3. DatabaseDiscovery 设计

### 功能说明

通配符模式 `["*"]` 下，自动发现新创建的数据库和表，并触发同步。

### 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                   DatabaseDiscovery                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐                                           │
│  │ SyncScope    │  ["*"] 通配符模式                          │
│  │ Level:Database│                                          │
│  └──────┬───────┘                                           │
│         │                                                   │
│         ▼                                                   │
│  ┌──────────────────┐                                       │
│  │ DatabaseDiscovery│                                       │
│  │ - WatchDDL()     │◄────── DDL 事件流                     │
│  │ - HandleCreateDB │                                       │
│  │ - HandleCreateTable                                      │
│  │ - HandleDropDB   │                                       │
│  └──────┬───────────┘                                       │
│         │                                                   │
│         ▼                                                   │
│  ┌──────────────────┐                                       │
│  │ SourceConnector  │                                       │
│  │ - AddTables()    │◄────── 自动调用                        │
│  │ - RemoveTables() │                                       │
│  └──────────────────┘                                       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 接口定义

```go
// DatabaseDiscovery 数据库/表自动发现器
type DatabaseDiscovery struct {
    scope       *source.SyncScope
    connector   source.Connector
    ddlParser   parser.DDLParser
    
    // 已知数据库和表
    knownDBs    map[string]struct{}
    knownTables map[string]struct{}
    
    mu          sync.RWMutex
}

// DiscoveryEvent 发现事件
type DiscoveryEvent struct {
    Type      DiscoveryType
    Database  string
    Table     string
    Timestamp time.Time
}

// DiscoveryType 发现事件类型
type DiscoveryType string

const (
    DiscoveryTypeDatabaseCreated DiscoveryType = "database-created"
    DiscoveryTypeDatabaseDropped DiscoveryType = "database-dropped"
    DiscoveryTypeTableCreated    DiscoveryType = "table-created"
    DiscoveryTypeTableDropped    DiscoveryType = "table-dropped"
    DiscoveryTypeTableAltered    DiscoveryType = "table-altered"
)

// NewDatabaseDiscovery 创建发现器
func NewDatabaseDiscovery(scope *source.SyncScope, connector source.Connector, ddlParser parser.DDLParser) *DatabaseDiscovery

// Start 启动发现器（监听 DDL 事件）
func (d *DatabaseDiscovery) Start(ctx context.Context) error

// Stop 停止发现器
func (d *DatabaseDiscovery) Stop() error

// OnDDLEvent 处理 DDL 事件
func (d *DatabaseDiscovery) OnDDLEvent(ddl *event.DDLEvent) error

// ShouldSyncDatabase 判断是否应该同步该数据库
func (d *DatabaseDiscovery) ShouldSyncDatabase(dbName string) bool

// ShouldSyncTable 判断是否应该同步该表
func (d *DatabaseDiscovery) ShouldSyncTable(dbName, tableName string) bool
```

### 文件位置

`internal/source/database_discovery.go`

### 与 Connector 的集成

```go
// 在 MySQL Connector 的 DDL 处理中
func (c *Connector) handleDDL(event *event.ChangeEvent) {
    // 如果是通配符模式，通知 DatabaseDiscovery
    if c.discovery != nil {
        c.discovery.OnDDLEvent(event.DDL)
    }
}
```

---

## 4. TableManager 设计

### 功能说明

提供 API 接口，让用户通过 REST API 或 CLI 手动添加/删除要同步的表。

### 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                      TableManager                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐                                           │
│  │ REST API     │  POST /api/v1/tables                      │
│  │ CLI          │  datastream-ctl tables add db.table       │
│  └──────┬───────┘                                           │
│         │                                                   │
│         ▼                                                   │
│  ┌──────────────────┐                                       │
│  │ TableManager     │                                       │
│  │ - AddTables()    │                                       │
│  │ - RemoveTables() │                                       │
│  │ - ListTables()   │                                       │
│  │ - GetTableStatus │                                       │
│  └──────┬───────────┘                                       │
│         │                                                   │
│         ▼                                                   │
│  ┌──────────────────┐                                       │
│  │ SourceConnector  │                                       │
│  │ - AddTables()    │◄────── 委托调用                        │
│  │ - RemoveTables() │                                       │
│  └──────────────────┘                                       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 接口定义

```go
// TableManager 表管理器
type TableManager struct {
    connector   source.Connector
    eventCh     chan *TableOperationEvent
    
    // 同步表状态
    tables      map[string]*TableSyncStatus
    mu          sync.RWMutex
}

// TableSyncStatus 表同步状态
type TableSyncStatus struct {
    Database    string
    Table       string
    Status      TableStatus    // pending, snapshotting, streaming, paused, error
    AddedAt     time.Time
    SyncStarted time.Time
    Position    *event.Position
    Error       error
}

// TableStatus 表状态
type TableStatus string

const (
    TableStatusPending     TableStatus = "pending"
    TableStatusSnapshotting TableStatus = "snapshotting"
    TableStatusStreaming   TableStatus = "streaming"
    TableStatusPaused      TableStatus = "paused"
    TableStatusError       TableStatus = "error"
)

// TableOperationEvent 表操作事件
type TableOperationEvent struct {
    Type      TableOperationType
    Database  string
    Table     string
    Timestamp time.Time
    Error     error
}

// TableOperationType 操作类型
type TableOperationType string

const (
    TableOpAdded    TableOperationType = "added"
    TableOpRemoved  TableOperationType = "removed"
    TableOpStarted  TableOperationType = "started"
    TableOpPaused   TableOperationType = "paused"
    TableOpResumed  TableOperationType = "resumed"
    TableOpError    TableOperationType = "error"
)

// NewTableManager 创建表管理器
func NewTableManager(connector source.Connector) *TableManager

// AddTables 添加表到同步列表
// 返回每个表的添加结果
func (tm *TableManager) AddTables(ctx context.Context, tables []string) []TableOperationResult

// RemoveTables 从同步列表移除表
func (tm *TableManager) RemoveTables(ctx context.Context, tables []string) []TableOperationResult

// ListTables 列出所有同步表及其状态
func (tm *TableManager) ListTables() []*TableSyncStatus

// GetTableStatus 获取单个表的状态
func (tm *TableManager) GetTableStatus(database, table string) (*TableSyncStatus, error)

// PauseTable 暂停表的同步
func (tm *TableManager) PauseTable(ctx context.Context, database, table string) error

// ResumeTable 恢复表的同步
func (tm *TableManager) ResumeTable(ctx context.Context, database, table string) error

// Events 返回表操作事件通道
func (tm *TableManager) Events() <-chan *TableOperationEvent
```

### 文件位置

`internal/source/table_manager.go`

### API 端点设计

```
POST   /api/v1/tables              # 添加表
DELETE /api/v1/tables              # 移除表
GET    /api/v1/tables              # 列出所有表
GET    /api/v1/tables/{db}/{table} # 获取表状态
POST   /api/v1/tables/{db}/{table}/pause   # 暂停表
POST   /api/v1/tables/{db}/{table}/resume  # 恢复表
```

---

## 5. 实现优先级

| 优先级 | 任务 | 文件 | 预计工作量 |
|--------|------|------|-----------|
| P0 | 更新设计文档 | `docs/design/pipeline-design.md` | 0.5天 |
| P0 | 更新设计文档 | `docs/design/connector-design.md` | 0.5天 |
| P1 | 添加 `Schemas()` 方法 | `internal/source/mysql/connector.go` | 0.5天 |
| P1 | 实现 DatabaseDiscovery | `internal/source/database_discovery.go` | 2天 |
| P1 | 实现 TableManager | `internal/source/table_manager.go` | 2天 |
| P2 | API 端点 | `internal/api/tables.go` | 1天 |
| P2 | CLI 命令 | `internal/cli/tables.go` | 1天 |

---

## 6. 测试要求

- DatabaseDiscovery 单元测试：覆盖 DDL 事件处理
- TableManager 单元测试：覆盖增删改查操作
- 集成测试：API 端点功能测试

---

*文档版本：v1.0*
*创建时间：2026-05-13*
