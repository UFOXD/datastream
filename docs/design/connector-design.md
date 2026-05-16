# Connector Layer 设计

Connector Layer 负责 Source 和 Sink 的连接与管理，支持多数据源和多目标。

---

## 1. 同步范围配置（SyncScope）

### 1.1 配置定义

```go
// SyncScope 同步范围定义
type SyncScope struct {
    // 同步级别：Database / Table
    Level       SyncLevel `json:"level" toml:"level"`
    
    // Database 级别配置
    Databases   DatabaseScope `json:"databases" toml:"databases"`
    
    // Table 级别配置
    Tables      TableScope `json:"tables" toml:"tables"`
}

// SyncLevel 同步级别
type SyncLevel string
const (
    SyncLevelDatabase SyncLevel = "database"  // Database 级别
    SyncLevelTable    SyncLevel = "table"     // Table 级别
)

// DatabaseScope Database 级别同步配置
type DatabaseScope struct {
    // 数据库列表，支持三种模式：
    // 1. ["db1"] - 单库同步
    // 2. ["db1", "db2", "db3"] - 多库同步
    // 3. ["*"] - 全库同步，源库新建 Database 自动发现并同步
    Names       []string `json:"names" toml:"names"`
    
    // DDL 同步开关
    EnableDDL   bool `json:"enable-ddl" toml:"enable-ddl"`
    
    // 表过滤规则（正则表达式，为空则同步所有表）
    TableFilter []string `json:"table-filter" toml:"table-filter"`
    
    // 忽略的表（正则表达式）
    IgnoreTables []string `json:"ignore-tables" toml:"ignore-tables"`
}

// TableScope Table 级别同步配置
type TableScope struct {
    // 表列表，格式：database.table
    // 例如：["db1.users", "db1.orders", "db2.products"]
    Names       []string `json:"names" toml:"names"`
    
    // DDL 同步开关
    EnableDDL   bool `json:"enable-ddl" toml:"enable-ddl"`
}

// IsWildcardDatabase 是否为通配符模式（全库同步）
func (d *DatabaseScope) IsWildcardDatabase() bool {
    return len(d.Names) == 1 && d.Names[0] == "*"
}
```

---

## 2. Source Connector 接口

### 2.1 接口定义

```go
// SourceConnector Source 连接器接口
type SourceConnector interface {
    // 基础生命周期
    Initialize(ctx context.Context, config *SourceConfig) error
    Start(ctx context.Context) error
    Stop(ctx context.Context) error  // 实际实现：接受 ctx 参数，优于设计阶段的无参版本
    
    // 事件流
    Events() <-chan *ChangeEvent
    Errors() <-chan error
    
    // 位点管理
    GetPosition() Position  // 实际实现：语义更清晰
    SetPosition(pos Position) error  // 实际实现：语义更清晰
    Seek(position Position) error
    
    // Schema 管理
    Schema(tableID TableID) (*TableSchema, error)
    Schemas() map[TableID]*TableSchema  // 已实现：返回所有缓存的表 Schema
    
    // 同步范围管理
    SyncScope() *SyncScope
    
    // 表管理（Table 级别专用）
    AddTables(ctx context.Context, tables []string) error
    RemoveTables(ctx context.Context, tables []string) error
    ListTables() []string
}
```

> **实现说明**：
> - `Stop(ctx context.Context)` — 实际实现接受 context 参数，支持带超时的优雅关闭，优于设计阶段的 `Stop() error`
> - `GetPosition()` / `SetPosition(pos)` — 实际实现采用 getter/setter 命名，语义比 `Position()` / `Seek()` 更清晰
> - `Schemas()` — 已在 MySQL Connector 中实现，返回 SchemaCache 中所有已缓存的表 Schema

### 2.2 配置定义

```go
// SourceConfig Source 配置
type SourceConfig struct {
    // 连接信息
    Connection ConnectionConfig `json:"connection" toml:"connection"`
    
    // 同步范围
    SyncScope  SyncScope `json:"sync-scope" toml:"sync-scope"`
    
    // 快照配置
    Snapshot   SnapshotConfig `json:"snapshot" toml:"snapshot"`
    
    // 增量配置
    Streaming  StreamingConfig `json:"streaming" toml:"streaming"`
}

// ConnectionConfig 数据库连接配置
type ConnectionConfig struct {
    Host        string `json:"host" toml:"host"`
    Port        int    `json:"port" toml:"port"`
    User        string `json:"user" toml:"user"`
    Password    string `json:"password" toml:"password"`
    
    // SSL/TLS 配置
    SSLMode     string `json:"ssl-mode" toml:"ssl-mode"`
    SSLCA       string `json:"ssl-ca" toml:"ssl-ca"`
    SSLCert     string `json:"ssl-cert" toml:"ssl-cert"`
    SSLKey      string `json:"ssl-key" toml:"ssl-key"`
    
    // 时区
    Timezone    string `json:"timezone" toml:"timezone"`
}

// SnapshotConfig 快照配置
type SnapshotConfig struct {
    // 是否执行快照
    Enable      bool `json:"enable" toml:"enable"`
    
    // 快照模式：initial（首次）, when_needed（需要时）, never（从不）, always（总是）
    Mode        SnapshotMode `json:"mode" toml:"mode"`
    
    // 并发线程数
    ParallelThreads int `json:"parallel-threads" toml:"parallel-threads"`
    
    // 单表分片大小（行数）
    ChunkSize   int `json:"chunk-size" toml:"chunk-size"`
    
    // 锁模式：none, shared, exclusive
    LockMode    LockMode `json:"lock-mode" toml:"lock-mode"`
}

// StreamingConfig 增量同步配置
type StreamingConfig struct {
    // 是否启用增量
    Enable      bool `json:"enable" toml:"enable"`
    
    // GTID 模式（MySQL）
    GTIDMode    bool `json:"gtid-mode" toml:"gtid-mode"`
    
    // 心跳间隔
    HeartbeatInterval time.Duration `json:"heartbeat-interval" toml:"heartbeat-interval"`
    
    // 重连配置
    ReconnectRetry    int           `json:"reconnect-retry" toml:"reconnect-retry"`
    ReconnectInterval time.Duration `json:"reconnect-interval" toml:"reconnect-interval"`
}
```

---

## 3. Database 级别自动发现机制

### 3.1 DatabaseDiscovery

> **实现说明**：`DatabaseDiscovery` 实现于 `internal/source/database_discovery.go`，在通配符模式（`*`）下监听 DDL 事件，自动发现并纳入新建的数据库和表。

```go
// DatabaseDiscovery Database 级别的数据库/表自动发现器
type DatabaseDiscovery struct {
    scope       *DatabaseScope
    eventCh     chan<- *DiscoveryEvent
    
    // 已知的数据库和表
    knownDBs    map[string]struct{}
    knownTables map[TableID]struct{}
    
    // DDL 监听器
    ddlListener DDLListener
}

// DiscoveryEvent 发现事件
type DiscoveryEvent struct {
    Type        DiscoveryType `json:"type"`
    Database    string        `json:"database"`
    Table       string        `json:"table"`
    Schema      *TableSchema  `json:"schema,omitempty"`
    Timestamp   time.Time     `json:"timestamp"`
}

// DiscoveryType 发现事件类型
type DiscoveryType string
const (
    DiscoveryTypeDatabaseCreated DiscoveryType = "database-created"  // 新数据库创建
    DiscoveryTypeDatabaseDropped DiscoveryType = "database-dropped"  // 数据库删除
    DiscoveryTypeTableCreated    DiscoveryType = "table-created"     // 新表创建
    DiscoveryTypeTableDropped    DiscoveryType = "table-dropped"     // 表删除
    DiscoveryTypeTableAltered    DiscoveryType = "table-altered"     // 表结构变更
)

// ShouldSyncDatabase 判断是否应该同步该数据库
func (d *DatabaseDiscovery) ShouldSyncDatabase(dbName string) bool {
    // 通配符模式：同步所有数据库
    if d.scope.IsWildcardDatabase() {
        return true
    }
    
    // 指定数据库列表
    for _, name := range d.scope.Names {
        if name == dbName {
            return true
        }
    }
    return false
}

// ShouldSyncTable 判断是否应该同步该表
func (d *DatabaseDiscovery) ShouldSyncTable(dbName, tableName string) bool {
    if !d.ShouldSyncDatabase(dbName) {
        return false
    }
    
    // 没有过滤规则，同步所有表
    if len(d.scope.TableFilter) == 0 {
        return !d.isIgnoredTable(tableName)
    }
    
    // 匹配过滤规则
    for _, pattern := range d.scope.TableFilter {
        if matched, _ := regexp.MatchString(pattern, tableName); matched {
            return !d.isIgnoredTable(tableName)
        }
    }
    return false
}

// Watch 监听数据库变更（通配符模式）
func (d *DatabaseDiscovery) Watch(ctx context.Context) error {
    if !d.scope.IsWildcardDatabase() {
        return nil  // 非通配符模式不需要监听新数据库
    }
    
    // 监听 DDL 事件
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case ddlEvent := <-d.ddlListener.Events():
            d.handleDDLEvent(ddlEvent)
        }
    }
}
```

---

## 4. Table 级别动态表管理

### 4.1 TableManager

> **实现说明**：`TableManager` 实现于 `internal/source/table_manager.go`，提供 API 驱动的表管理能力，支持运行时动态添加和移除同步表。

```go
// TableManager Table 级别的表管理器
type TableManager struct {
    scope       *TableScope
    source      SourceConnector
    eventCh     chan<- *TableOperationEvent
    
    // 当前同步的表
    syncTables  map[TableID]*TableSyncState
    mu          sync.RWMutex
}

// TableSyncState 表同步状态
type TableSyncState struct {
    TableID     TableID
    Status      TableSyncStatus
    SnapshotPos Position      // 快照位点
    StreamPos   Position      // 增量位点
    Schema      *TableSchema
    AddedAt     time.Time
    SyncStarted time.Time
}

// TableSyncStatus 表同步状态
type TableSyncStatus string
const (
    TableStatusPending   TableSyncStatus = "pending"    // 等待同步
    TableStatusSnapshot  TableSyncStatus = "snapshot"   // 快照中
    TableStatusStreaming TableSyncStatus = "streaming"  // 增量中
    TableStatusPaused    TableSyncStatus = "paused"     // 已暂停
    TableStatusError     TableSyncStatus = "error"      // 错误
)

// AddTables 添加表到同步任务
func (tm *TableManager) AddTables(ctx context.Context, tables []string) error {
    tm.mu.Lock()
    defer tm.mu.Unlock()
    
    for _, table := range tables {
        // 解析 database.table 格式
        db, tbl, err := parseTableName(table)
        if err != nil {
            return err
        }
        
        tableID := TableID{Database: db, Table: tbl}
        
        // 检查是否已存在
        if _, exists := tm.syncTables[tableID]; exists {
            continue
        }
        
        // 1. 获取表结构
        schema, err := tm.source.Schema(tableID)
        if err != nil {
            return WrapError(ErrSchemaFetchFailed, err, table)
        }
        
        // 2. 创建同步状态
        state := &TableSyncState{
            TableID:  tableID,
            Status:   TableStatusPending,
            Schema:   schema,
            AddedAt:  time.Now(),
        }
        tm.syncTables[tableID] = state
        
        // 3. 发送添加事件，触发快照同步
        tm.eventCh <- &TableOperationEvent{
            Operation: TableOpAdd,
            TableID:   tableID,
            Schema:    schema,
            Timestamp: time.Now(),
        }
    }
    
    return nil
}

// RemoveTables 从同步任务移除表
func (tm *TableManager) RemoveTables(ctx context.Context, tables []string) error {
    tm.mu.Lock()
    defer tm.mu.Unlock()
    
    for _, table := range tables {
        db, tbl, err := parseTableName(table)
        if err != nil {
            return err
        }
        
        tableID := TableID{Database: db, Table: tbl}
        
        // 检查是否存在
        if _, exists := tm.syncTables[tableID]; !exists {
            continue
        }
        
        // 删除同步状态
        delete(tm.syncTables, tableID)
        
        // 发送移除事件
        tm.eventCh <- &TableOperationEvent{
            Operation: TableOpRemove,
            TableID:   tableID,
            Timestamp: time.Now(),
        }
    }
    
    return nil
}
```

---

## 5. 快照阶段并发设计

### 5.1 并发层级

快照阶段有三个并发层级：

```
┌─────────────────────────────────────────────────────────────────┐
│                    快照并发层级                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Level 1: 表级并发（MaxTableThreads）                            │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐              │
│  │  表 A   │ │  表 B   │ │  表 C   │ │  表 D   │              │
│  │ Worker  │ │ Worker  │ │ Worker  │ │ Worker  │              │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘              │
│                                                                   │
│  Level 2: 分片并发（MaxChunkThreads）                            │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    表 A（大表）                          │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐      │   │
│  │  │ Chunk 1 │ │ Chunk 2 │ │ Chunk 3 │ │ Chunk 4 │      │   │
│  │  │ Worker  │ │ Worker  │ │ Worker  │ │ Worker  │      │   │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
│  Level 3: 批次大小（BatchSize）                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              单次读取行数                                │   │
│  │  [Row 1] [Row 2] [Row 3] ... [Row N]                   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 并发配置定义

```go
// SnapshotConcurrencyConfig 快照并发配置
type SnapshotConcurrencyConfig struct {
    // ===== 表级并发 =====

    // MaxTableThreads 最大表级并发线程数
    // 同时处理多少个表的快照
    // 默认: 4，范围: 1-16
    MaxTableThreads int `json:"max-table-threads" toml:"max-table-threads"`

    // ===== 分片并发（大表优化） =====

    // EnableChunkParallel 是否启用分片并发
    // 大表自动分片，多线程并行读取
    // 默认: true
    EnableChunkParallel bool `json:"enable-chunk-parallel" toml:"enable-chunk-parallel"`

    // MaxChunkThreads 单表最大分片并发线程数
    // 单个大表最多使用多少个线程并行读取
    // 默认: 4，范围: 1-8
    MaxChunkThreads int `json:"max-chunk-threads" toml:"max-chunk-threads"`

    // ChunkThreshold 触发分片并行的阈值
    // 表行数超过此值才启用分片并发
    // 默认: 100000 行
    ChunkThreshold int64 `json:"chunk-threshold" toml:"chunk-threshold"`

    // ===== 批次大小 =====

    // BatchSize 单次读取的行数
    // 每次从源表读取多少行
    // 默认: 1000，范围: 100-10000
    BatchSize int `json:"batch-size" toml:"batch-size"`

    // ChunkSize 分片大小（行数）
    // 大表按此大小分片
    // 默认: 10000
    ChunkSize int `json:"chunk-size" toml:"chunk-size"`

    // ===== 队列配置 =====

    // TaskQueueSize 任务队列大小
    // 默认: 1000
    TaskQueueSize int `json:"task-queue-size" toml:"task-queue-size"`

    // EventBufferSize 事件缓冲区大小
    // 默认: 10000
    EventBufferSize int `json:"event-buffer-size" toml:"event-buffer-size"`
}

// DefaultSnapshotConcurrencyConfig 默认配置
func DefaultSnapshotConcurrencyConfig() *SnapshotConcurrencyConfig {
    return &SnapshotConcurrencyConfig{
        MaxTableThreads:     4,
        EnableChunkParallel: true,
        MaxChunkThreads:     4,
        ChunkThreshold:      100000,
        BatchSize:           1000,
        ChunkSize:           10000,
        TaskQueueSize:       1000,
        EventBufferSize:     10000,
    }
}

// Validate 验证配置
func (c *SnapshotConcurrencyConfig) Validate() error {
    if c.MaxTableThreads < 1 || c.MaxTableThreads > 16 {
        return errors.New("max-table-threads must be between 1 and 16")
    }
    if c.MaxChunkThreads < 1 || c.MaxChunkThreads > 8 {
        return errors.New("max-chunk-threads must be between 1 and 8")
    }
    if c.BatchSize < 100 || c.BatchSize > 10000 {
        return errors.New("batch-size must be between 100 and 10000")
    }
    return nil
}
```

### 5.3 并发策略

```go
// SnapshotConcurrencyStrategy 并发策略
type SnapshotConcurrencyStrategy struct {
    config *SnapshotConcurrencyConfig
}

// PlanConcurrency 规划并发策略
// 根据表的大小决定使用哪种并发模式
func (s *SnapshotConcurrencyStrategy) PlanConcurrency(tables []*TableInfo) *ConcurrencyPlan {
    plan := &ConcurrencyPlan{
        TablePlans: make(map[TableID]*TableConcurrencyPlan),
    }

    // 计算总可用线程数
    totalThreads := s.config.MaxTableThreads
    usedThreads := 0

    // 按表大小排序，大表优先分配资源
    sort.Slice(tables, func(i, j int) bool {
        return tables[i].EstimatedRows > tables[j].EstimatedRows
    })

    for _, table := range tables {
        tablePlan := &TableConcurrencyPlan{
            TableID: table.TableID,
        }

        // 判断是否需要分片并发
        if s.config.EnableChunkParallel &&
           table.EstimatedRows >= s.config.ChunkThreshold &&
           usedThreads < totalThreads {

            // 大表：使用分片并发
            availableThreads := min(s.config.MaxChunkThreads, totalThreads-usedThreads)
            tablePlan.Mode = ConcurrencyModeChunkParallel
            tablePlan.ChunkThreads = availableThreads
            tablePlan.ChunkSize = s.config.ChunkSize
            usedThreads += availableThreads

        } else {
            // 小表：单线程
            tablePlan.Mode = ConcurrencyModeSingle
            tablePlan.ChunkThreads = 1
            tablePlan.BatchSize = s.config.BatchSize
        }

        plan.TablePlans[table.TableID] = tablePlan
    }

    return plan
}

// ConcurrencyMode 并发模式
type ConcurrencyMode string

const (
    ConcurrencyModeSingle        ConcurrencyMode = "single"         // 单线程
    ConcurrencyModeChunkParallel ConcurrencyMode = "chunk-parallel" // 分片并发
)

// TableConcurrencyPlan 表并发计划
type TableConcurrencyPlan struct {
    TableID      TableID
    Mode         ConcurrencyMode
    ChunkThreads int
    ChunkSize    int
    BatchSize    int
}
```

### 5.4 并发配置示例

```toml
# 小规模同步（少量小表）
[snapshot.concurrency]
max-table-threads = 2
enable-chunk-parallel = false
batch-size = 500

# 中等规模同步（多表混合）
[snapshot.concurrency]
max-table-threads = 4
enable-chunk-parallel = true
max-chunk-threads = 4
chunk-threshold = 100000
batch-size = 1000
chunk-size = 10000

# 大规模同步（大量大表）
[snapshot.concurrency]
max-table-threads = 8
enable-chunk-parallel = true
max-chunk-threads = 8
chunk-threshold = 50000
batch-size = 2000
chunk-size = 20000
```

---

## 6. 限速设计

### 6.1 限速架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        限速架构                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────┐      ┌─────────────────┐                  │
│  │  Source 端限速   │      │  Sink 端限速    │                  │
│  │                 │      │                 │                  │
│  │  ✓ 行数/秒      │      │  ✓ 行数/秒      │                  │
│  │  ✓ 字节/秒      │      │  ✓ 字节/秒      │                  │
│  │  ✓ 事务/秒      │      │  ✓ 事务/秒      │                  │
│  │                 │      │                 │                  │
│  │  推荐使用       │      │  备用（架构限制）│                  │
│  └─────────────────┘      └─────────────────┘                  │
│                                                                   │
│  说明：                                                          │
│  - 当前架构下，Source 端限速更有效（控制数据生产速率）           │
│  - Sink 端限速通过背压实现，但可能导致 Source 端积压            │
│  - 建议优先使用 Source 端限速                                    │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### 6.2 限速配置

```go
// RateLimitConfig 限速配置
type RateLimitConfig struct {
    // ===== Source 端限速（推荐） =====

    // SourceEnabled 是否启用 Source 端限速
    // 默认: true
    SourceEnabled bool `json:"source-enabled" toml:"source-enabled"`

    // SourceRowsPerSecond Source 端行数限速（行/秒）
    // 0 表示不限速
    // 默认: 0（不限速）
    SourceRowsPerSecond int `json:"source-rows-per-second" toml:"source-rows-per-second"`

    // SourceBytesPerSecond Source 端字节限速（字节/秒）
    // 0 表示不限速
    // 默认: 0（不限速）
    SourceBytesPerSecond int64 `json:"source-bytes-per-second" toml:"source-bytes-per-second"`

    // ===== Sink 端限速（备用） =====

    // SinkEnabled 是否启用 Sink 端限速
    // 注意：当前架构下 Sink 限速通过背压实现，可能导致 Source 积压
    // 默认: false
    SinkEnabled bool `json:"sink-enabled" toml:"sink-enabled"`

    // SinkRowsPerSecond Sink 端行数限速（行/秒）
    // 0 表示不限速
    // 默认: 0（不限速）
    SinkRowsPerSecond int `json:"sink-rows-per-second" toml:"sink-rows-per-second"`

    // SinkBytesPerSecond Sink 端字节限速（字节/秒）
    // 0 表示不限速
    // 默认: 0（不限速）
    SinkBytesPerSecond int64 `json:"sink-bytes-per-second" toml:"sink-bytes-per-second"`

    // ===== 通用配置 =====

    // BurstSize 突发大小
    // 允许短时间内的突发流量
    // 默认: 1000
    BurstSize int `json:"burst-size" toml:"burst-size"`

    // EnableAdaptive 是否启用自适应限速
    // 根据系统负载自动调整限速
    // 默认: false
    EnableAdaptive bool `json:"enable-adaptive" toml:"enable-adaptive"`

    // AdaptiveConfig 自适应限速配置
    AdaptiveConfig *AdaptiveRateLimitConfig `json:"adaptive-config" toml:"adaptive-config"`
}

// DefaultRateLimitConfig 默认限速配置
func DefaultRateLimitConfig() *RateLimitConfig {
    return &RateLimitConfig{
        SourceEnabled:        true,
        SourceRowsPerSecond:  0,      // 不限速
        SourceBytesPerSecond: 0,      // 不限速
        SinkEnabled:          false,  // 默认不启用
        SinkRowsPerSecond:    0,
        SinkBytesPerSecond:   0,
        BurstSize:            1000,
        EnableAdaptive:       false,
    }
}
```

### 6.3 限速器实现

```go
package ratelimit

import (
    "context"
    "time"

    "golang.org/x/time/rate"
)

// RateLimiter 限速器接口
type RateLimiter interface {
    // Wait 等待直到允许通过
    Wait(ctx context.Context) error

    // WaitN 等待 n 个令牌
    WaitN(ctx context.Context, n int) error

    // Allow 检查是否允许通过（非阻塞）
    Allow() bool

    // AllowN 检查是否允许 n 个令牌通过
    AllowN(n int) bool

    // SetLimit 设置限速值
    SetLimit(limit int)
}

// RateLimiterImpl 限速器实现
type RateLimiterImpl struct {
    rowsLimiter  *rate.Limiter  // 行数限速
    bytesLimiter *rate.Limiter  // 字节限速

    config       *RateLimitConfig
}

// NewRateLimiter 创建限速器
func NewRateLimiter(config *RateLimitConfig) *RateLimiterImpl {
    rl := &RateLimiterImpl{
        config: config,
    }

    // 行数限速
    if config.SourceRowsPerSecond > 0 {
        rl.rowsLimiter = rate.NewLimiter(
            rate.Limit(config.SourceRowsPerSecond),
            config.BurstSize,
        )
    }

    // 字节限速
    if config.SourceBytesPerSecond > 0 {
        rl.bytesLimiter = rate.NewLimiter(
            rate.Limit(config.SourceBytesPerSecond),
            config.BurstSize*1000, // 字节突发更大
        )
    }

    return rl
}

// WaitAndWaitForBytes 等待行数和字节令牌
func (rl *RateLimiterImpl) WaitAndWaitForBytes(ctx context.Context, rows int, bytes int64) error {
    // 行数限速
    if rl.rowsLimiter != nil && rows > 0 {
        if err := rl.rowsLimiter.WaitN(ctx, rows); err != nil {
            return err
        }
    }

    // 字节限速
    if rl.bytesLimiter != nil && bytes > 0 {
        if err := rl.bytesLimiter.WaitN(ctx, int(bytes)); err != nil {
            return err
        }
    }

    return nil
}

// SetLimit 动态调整限速
func (rl *RateLimiterImpl) SetLimit(rowsPerSecond int) {
    if rl.rowsLimiter != nil {
        rl.rowsLimiter.SetLimit(rate.Limit(rowsPerSecond))
    }
}
```

### 6.4 Source 端限速集成

```go
// RateLimitedSourceConnector 带限速的 Source Connector
type RateLimitedSourceConnector struct {
    inner      SourceConnector
    limiter    *RateLimiterImpl
    config     *RateLimitConfig
}

// ReadBatch 读取一批事件（带限速）
func (s *RateLimitedSourceConnector) ReadBatch(ctx context.Context, batchSize int) ([]*ChangeEvent, error) {
    events, err := s.inner.ReadBatch(ctx, batchSize)
    if err != nil {
        return nil, err
    }

    // 应用限速
    if s.config.SourceEnabled && s.limiter != nil {
        totalBytes := int64(0)
        for _, event := range events {
            totalBytes += event.EstimatedSize()
        }

        if err := s.limiter.WaitAndWaitForBytes(ctx, len(events), totalBytes); err != nil {
            return nil, err
        }
    }

    return events, nil
}
```

### 6.5 Sink 端背压限速

```go
// BackpressureController 背压控制器
type BackpressureController struct {
    config       *RateLimitConfig

    // 监控指标
    queueSize    int
    maxQueueSize int
    latency      time.Duration

    // 控制信号
    pauseCh      chan struct{}
    resumeCh     chan struct{}
}

// CheckBackpressure 检查是否需要背压
func (b *BackpressureController) CheckBackpressure() error {
    // 队列使用率过高，触发背压
    if b.queueSize > b.maxQueueSize*8/10 {
        return ErrBackpressureApplied
    }

    // 延迟过高，触发背压
    if b.latency > 5*time.Second {
        return ErrBackpressureApplied
    }

    return nil
}

// ApplyBackpressure 应用背压
// 通过阻塞读取来间接限速 Source
func (s *SourceConnector) ApplyBackpressure(controller *BackpressureController) {
    for {
        if err := controller.CheckBackpressure(); err != nil {
            // 等待恢复信号
            <-controller.resumeCh
        }

        // 正常读取
        // ...
    }
}
```

### 6.6 限速配置示例

```toml
# 不限速（默认）
[rate-limit]
source-enabled = true
source-rows-per-second = 0
source-bytes-per-second = 0
sink-enabled = false

# Source 端限速：10000 行/秒
[rate-limit]
source-enabled = true
source-rows-per-second = 10000
source-bytes-per-second = 0
burst-size = 2000

# Source 端限速：10 MB/秒
[rate-limit]
source-enabled = true
source-rows-per-second = 0
source-bytes-per-second = 10485760  # 10 MB
burst-size = 1048576  # 1 MB

# Source + Sink 双端限速（不推荐）
[rate-limit]
source-enabled = true
source-rows-per-second = 10000
sink-enabled = true
sink-rows-per-second = 8000
```

### 6.7 限速设计说明

| 场景 | 推荐配置 | 说明 |
|------|---------|------|
| 生产环境默认 | Source 限速 | 控制数据生产速率，避免影响源库 |
| 源库压力大 | Source 行数限速 | 限制每秒读取行数 |
| 网络带宽有限 | Source 字节限速 | 限制每秒传输字节数 |
| 目标库压力大 | Source 限速 | 通过降低源端速率保护目标库 |

**架构限制说明**：

当前 Pipeline 架构下，数据从 Source → Pipeline → Sink 单向流动：
- Source 端限速：直接控制数据生产速率，效果最好
- Sink 端限速：通过背压机制实现，会导致 Source 端队列积压，间接起到限速效果

**建议**：优先使用 Source 端限速，Sink 端限速仅在特殊场景下启用。

---

## 7. 增量阶段并发设计

### 7.1 数据顺序性保证

增量同步阶段需要保证数据一致性，核心原则：**同一行的变更事件必须有序处理**。

```
┌─────────────────────────────────────────────────────────────────┐
│                  增量阶段并发模型                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  问题：多线程并发可能导致顺序错乱                               │
│                                                                   │
│  时间线:  T1 ─────────────────────────────────────►             │
│                                                                   │
│  事件流:  INSERT(id=1) ── UPDATE(id=1) ── DELETE(id=1)          │
│              │              │              │                     │
│              ▼              ▼              ▼                     │
│           Worker 1      Worker 2      Worker 3                  │
│              │              │              │                     │
│              ▼              ▼              ▼                     │
│           处理顺序可能错乱：DELETE → UPDATE → INSERT ❌          │
│                                                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  解决方案：按主键 Hash 分发，同行事件进入同一 Worker             │
│                                                                   │
│  事件流:  INSERT(id=1) ── UPDATE(id=1) ── DELETE(id=1)          │
│              │              │              │                     │
│              └──────────────┼──────────────┘                     │
│                             ▼                                    │
│                    Hash(schema+table+pk) = Worker N              │
│                             │                                    │
│                             ▼                                    │
│                         Worker N                                 │
│                             │                                    │
│                             ▼                                    │
│           处理顺序保证：INSERT → UPDATE → DELETE ✓               │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### 7.2 行唯一标识策略

```go
// RowIdentifier 行唯一标识
type RowIdentifier struct {
    // Schema 名（PostgreSQL/Oracle）
    Schema string

    // 数据库名
    Database string

    // 表名
    Table string

    // 主键值（JSON 编码）
    PrimaryKeyValues string

    // 标识类型
    KeyType RowKeyType
}

// RowKeyType 主键类型
type RowKeyType int

const (
    KeyTypePrimaryKey   RowKeyType = iota  // 主键
    KeyTypeUniqueIndex                      // 唯一索引
    KeyTypeFullRow                          // 全行（无主键无唯一索引）
)

// BuildRowIdentifier 构建行唯一标识
func BuildRowIdentifier(event *ChangeEvent, tableSchema *TableSchema) *RowIdentifier {
    identifier := &RowIdentifier{
        Schema:   event.Table.Schema,
        Database: event.Table.Database,
        Table:    event.Table.Table,
    }

    // 1. 优先使用主键
    if len(tableSchema.PrimaryKeyColumns) > 0 {
        identifier.KeyType = KeyTypePrimaryKey
        identifier.PrimaryKeyValues = extractKeyValues(event, tableSchema.PrimaryKeyColumns)
        return identifier
    }

    // 2. 无主键，使用第一个唯一索引
    if len(tableSchema.UniqueIndexColumns) > 0 {
        identifier.KeyType = KeyTypeUniqueIndex
        identifier.PrimaryKeyValues = extractKeyValues(event, tableSchema.UniqueIndexColumns[0])
        return identifier
    }

    // 3. 无主键无唯一索引，使用全行数据
    identifier.KeyType = KeyTypeFullRow
    identifier.PrimaryKeyValues = extractAllColumnValues(event)
    return identifier
}

// extractKeyValues 提取键值
func extractKeyValues(event *ChangeEvent, columns []string) string {
    values := make([]string, len(columns))
    for i, col := range columns {
        if event.After != nil {
            values[i] = fmt.Sprintf("%v", event.After.Fields[col].Value)
        } else if event.Before != nil {
            values[i] = fmt.Sprintf("%v", event.Before.Fields[col].Value)
        }
    }
    return strings.Join(values, "|")
}

// HashKey 生成 Hash Key
func (r *RowIdentifier) HashKey() string {
    // 格式: schema:database:table:values
    // 示例: :inventory:users:101
    // 示例: public:inventory:orders:order-001
    return fmt.Sprintf("%s:%s:%s:%s",
        r.Schema,
        r.Database,
        r.Table,
        r.PrimaryKeyValues,
    )
}
```

### 7.3 Hash 分发器实现

```go
// HashDispatcher 基于 Hash 的事件分发器
type HashDispatcher struct {
    // Worker 数量
    workerCount int

    // Worker 通道
    workerChans []chan *ChangeEvent

    // 表 Schema 缓存
    schemaCache map[TableID]*TableSchema

    // 表级 Worker 映射（无主键表专用）
    tableWorkerMap sync.Map // tableID -> workerID

    // 配置
    config *DispatcherConfig
}

// DispatcherConfig 分发器配置
type DispatcherConfig struct {
    // Worker 数量
    WorkerCount int `json:"worker-count" toml:"worker-count"`

    // 每个 Worker 的缓冲区大小
    BufferSize int `json:"buffer-size" toml:"buffer-size"`

    // 无主键表的处理策略
    // "single": 单线程（所有无主键表共享一个 Worker）
    // "table": 按表分发（每表一个固定 Worker）
    NoPKTableStrategy string `json:"no-pk-table-strategy" toml:"no-pk-table-strategy"`
}

// NewHashDispatcher 创建 Hash 分发器
func NewHashDispatcher(config *DispatcherConfig) *HashDispatcher {
    d := &HashDispatcher{
        workerCount: config.WorkerCount,
        workerChans: make([]chan *ChangeEvent, config.WorkerCount),
        schemaCache: make(map[TableID]*TableSchema),
        config:      config,
    }

    // 初始化 Worker 通道
    for i := 0; i < config.WorkerCount; i++ {
        d.workerChans[i] = make(chan *ChangeEvent, config.BufferSize)
    }

    return d
}

// Dispatch 分发事件到对应的 Worker
func (d *HashDispatcher) Dispatch(ctx context.Context, event *ChangeEvent) error {
    // 获取表 Schema
    tableID := TableID{
        Database: event.Table.Database,
        Schema:   event.Table.Schema,
        Table:    event.Table.Table,
    }

    schema, ok := d.schemaCache[tableID]
    if !ok {
        return fmt.Errorf("schema not found for table %s", tableID)
    }

    // 计算目标 Worker ID
    workerID := d.calculateWorkerID(event, schema, tableID)

    // 发送到对应 Worker
    select {
    case d.workerChans[workerID] <- event:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}

// calculateWorkerID 计算目标 Worker ID
func (d *HashDispatcher) calculateWorkerID(event *ChangeEvent, schema *TableSchema, tableID TableID) int {
    // 有主键或有唯一索引：按主键 Hash 分发
    if len(schema.PrimaryKeyColumns) > 0 || len(schema.UniqueIndexColumns) > 0 {
        identifier := BuildRowIdentifier(event, schema)
        hashKey := identifier.HashKey()
        hash := fnv32(hashKey)
        return int(hash % uint32(d.workerCount))
    }

    // 无主键无唯一索引：按表分发（保证同表事件有序）
    switch d.config.NoPKTableStrategy {
    case "single":
        // 所有无主键表使用 Worker 0
        return 0

    case "table":
        // 每表固定一个 Worker
        if workerID, ok := d.tableWorkerMap.Load(tableID); ok {
            return workerID.(int)
        }

        // 新表，分配一个 Worker
        hash := fnv32(tableID.String())
        workerID := int(hash % uint32(d.workerCount))
        d.tableWorkerMap.Store(tableID, workerID)
        return workerID

    default:
        return 0
    }
}

// fnv32 FNV-32 Hash 算法
func fnv32(key string) uint32 {
    hash := uint32(2166136261)
    const prime32 = uint32(16777619)
    for i := 0; i < len(key); i++ {
        hash *= prime32
        hash ^= uint32(key[i])
    }
    return hash
}

// WorkerChannels 获取 Worker 通道（供 Worker 消费）
func (d *HashDispatcher) WorkerChannels() []chan *ChangeEvent {
    return d.workerChans
}
```

### 7.4 表 Schema 缓存与主键识别

```go
// TableSchemaCache 表 Schema 缓存
type TableSchemaCache struct {
    cache   map[TableID]*TableSchema
    mu      sync.RWMutex
    source  SchemaFetcher
}

// TableSchema 表 Schema 信息
type TableSchema struct {
    // 表标识
    TableID TableID

    // 列信息
    Columns []ColumnInfo

    // 主键列
    PrimaryKeyColumns []string

    // 唯一索引列（可能有多个唯一索引）
    UniqueIndexColumns [][]string

    // 是否有主键
    HasPrimaryKey bool

    // 是否有唯一索引
    HasUniqueIndex bool
}

// GetOrFetch 获取或拉取 Schema
func (c *TableSchemaCache) GetOrFetch(ctx context.Context, tableID TableID) (*TableSchema, error) {
    // 先读缓存
    c.mu.RLock()
    schema, ok := c.cache[tableID]
    c.mu.RUnlock()

    if ok {
        return schema, nil
    }

    // 缓存未命中，从源库拉取
    schema, err := c.source.FetchSchema(ctx, tableID)
    if err != nil {
        return nil, err
    }

    // 识别主键和唯一索引
    c.identifyKeys(schema)

    // 写入缓存
    c.mu.Lock()
    c.cache[tableID] = schema
    c.mu.Unlock()

    return schema, nil
}

// identifyKeys 识别主键和唯一索引
func (c *TableSchemaCache) identifyKeys(schema *TableSchema) {
    // 从列信息中提取主键
    for _, col := range schema.Columns {
        if col.IsPrimaryKey {
            schema.PrimaryKeyColumns = append(schema.PrimaryKeyColumns, col.Name)
            schema.HasPrimaryKey = true
        }
    }

    // 从索引信息中提取唯一索引
    for _, idx := range schema.Indexes {
        if idx.IsUnique && !idx.IsPrimaryKey {
            schema.UniqueIndexColumns = append(schema.UniqueIndexColumns, idx.Columns)
            schema.HasUniqueIndex = true
        }
    }

    // 排序：选择第一个唯一索引
    if len(schema.UniqueIndexColumns) > 0 {
        sort.Slice(schema.UniqueIndexColumns, func(i, j int) bool {
            return len(schema.UniqueIndexColumns[i]) < len(schema.UniqueIndexColumns[j])
        })
    }
}

// UpdateSchema 更新 Schema（DDL 变更时调用）
func (c *TableSchemaCache) UpdateSchema(tableID TableID, schema *TableSchema) {
    c.identifyKeys(schema)

    c.mu.Lock()
    defer c.mu.Unlock()
    c.cache[tableID] = schema
}
```

### 7.5 并发写入器

```go
// ConcurrentSinkWriter 并发写入器
type ConcurrentSinkWriter struct {
    dispatcher  *HashDispatcher
    workers     []*SinkWorker
    sink        SinkConnector
    config      *ConcurrentSinkConfig

    wg          sync.WaitGroup
    ctx         context.Context
    cancel      context.CancelFunc
}

// ConcurrentSinkConfig 并发写入配置
type ConcurrentSinkConfig struct {
    // Worker 数量
    WorkerCount int `json:"worker-count" toml:"worker-count"`

    // 每个 Worker 的批次大小
    BatchSize int `json:"batch-size" toml:"batch-size"`

    // 刷新间隔
    FlushInterval time.Duration `json:"flush-interval" toml:"flush-interval"`

    // 重试配置
    MaxRetry     int           `json:"max-retry" toml:"max-retry"`
    RetryBackoff time.Duration `json:"retry-backoff" toml:"retry-backoff"`
}

// NewConcurrentSinkWriter 创建并发写入器
func NewConcurrentSinkWriter(sink SinkConnector, config *ConcurrentSinkConfig) *ConcurrentSinkWriter {
    ctx, cancel := context.WithCancel(context.Background())

    w := &ConcurrentSinkWriter{
        sink:    sink,
        config:  config,
        ctx:     ctx,
        cancel:  cancel,
        workers: make([]*SinkWorker, config.WorkerCount),
    }

    // 创建分发器
    w.dispatcher = NewHashDispatcher(&DispatcherConfig{
        WorkerCount:       config.WorkerCount,
        BufferSize:        config.BatchSize * 2,
        NoPKTableStrategy: "table",
    })

    // 创建并启动 Worker
    workerChans := w.dispatcher.WorkerChannels()
    for i := 0; i < config.WorkerCount; i++ {
        worker := &SinkWorker{
            id:           i,
            eventCh:      workerChans[i],
            sink:         sink,
            batchSize:    config.BatchSize,
            flushInterval: config.FlushInterval,
        }
        w.workers[i] = worker
        w.wg.Add(1)
        go worker.Run(ctx, &w.wg)
    }

    return w
}

// Write 写入事件
func (w *ConcurrentSinkWriter) Write(ctx context.Context, event *ChangeEvent) error {
    return w.dispatcher.Dispatch(ctx, event)
}

// WriteBatch 批量写入
func (w *ConcurrentSinkWriter) WriteBatch(ctx context.Context, events []*ChangeEvent) error {
    for _, event := range events {
        if err := w.dispatcher.Dispatch(ctx, event); err != nil {
            return err
        }
    }
    return nil
}

// Close 关闭写入器
func (w *ConcurrentSinkWriter) Close() error {
    w.cancel()
    w.wg.Wait()
    return nil
}

// SinkWorker Sink Worker
type SinkWorker struct {
    id            int
    eventCh       <-chan *ChangeEvent
    sink          SinkConnector
    batchSize     int
    flushInterval time.Duration

    // 单一缓冲区（高效批量）
    buffer        []*ChangeEvent

    // 统计
    eventsWritten  int64
    batchesFlushed int64
}

// Run 运行 Worker
func (w *SinkWorker) Run(ctx context.Context, wg *sync.WaitGroup) {
    defer wg.Done()

    ticker := time.NewTicker(w.flushInterval)
    defer ticker.Stop()

    w.buffer = make([]*ChangeEvent, 0, w.batchSize)

    for {
        select {
        case <-ctx.Done():
            if len(w.buffer) > 0 {
                w.flush()
            }
            return

        case event := <-w.eventCh:
            w.buffer = append(w.buffer, event)
            if len(w.buffer) >= w.batchSize {
                w.flush()
            }

        case <-ticker.C:
            if len(w.buffer) > 0 {
                w.flush()
            }
        }
    }
}

// flush 刷新缓冲区
// Sink 实现负责按事务类型分组处理
func (w *SinkWorker) flush() error {
    if len(w.buffer) == 0 {
        return nil
    }

    // 交给 Sink 处理（Sink 内部按事务类型分组）
    if err := w.sink.WriteBatch(context.Background(), &EventBatch{
        Events: w.buffer,
    }); err != nil {
        log.Error("failed to flush batch",
            zap.Int("worker", w.id),
            zap.Int("count", len(w.buffer)),
            zap.Error(err),
        )
        return err
    }

    w.eventsWritten += int64(len(w.buffer))
    w.batchesFlushed++
    w.buffer = w.buffer[:0]
    return nil
}
```

### 7.6 配置示例

```toml
# 增量同步并发配置
[streaming.concurrency]
# Worker 数量（并发线程数）
worker-count = 8

# 批次大小
batch-size = 1000

# 刷新间隔
flush-interval = "100ms"

# 无主键表策略
# "single": 所有无主键表使用同一个 Worker（单线程）
# "table": 按表分发，每表一个固定 Worker
no-pk-table-strategy = "table"

# 重试配置
max-retry = 3
retry-backoff = "100ms"
```

### 7.7 设计总结

| 场景 | 主键/唯一索引 | 分发策略 | 并发性 | 顺序性 |
|------|--------------|---------|--------|--------|
| 有主键表 | 有主键 | 按 (表+主键) Hash | 高（同行并发，不同行并行） | 保证同行顺序 |
| 有唯一索引表 | 无主键有唯一索引 | 按 (表+唯一索引列) Hash | 高（同行并发，不同行并行） | 保证同行顺序 |
| 无主键无唯一索引 | 无 | 按表 Hash 或单线程 | 低（同表串行） | 保证同表顺序 |

**核心保证**：
1. 同一行的 INSERT/UPDATE/DELETE 必然进入同一个 Worker
2. 同一个 Worker 内串行处理，保证顺序
3. 不同行的事件可以并行处理，提高吞吐

---

## 8. Sink Connector 接口

### 8.1 接口定义

```go
// SinkConnector Sink 连接器接口
type SinkConnector interface {
    // 基础生命周期
    Initialize(ctx context.Context, config *SinkConfig) error
    Start(ctx context.Context) error
    Stop() error

    // 写入事件
    Write(ctx context.Context, events []*ChangeEvent) error

    // 批量写入
    // 实现需要根据数据库特性处理事务：
    // - MySQL: 按表引擎分组（事务表 vs 非事务表）
    // - PostgreSQL/Oracle/SQL Server: 单事务即可
    WriteBatch(ctx context.Context, batch *EventBatch) error

    // 位点持久化
    FlushPosition(ctx context.Context, position Position) error
    LastPosition() Position

    // DDL 处理
    ApplyDDL(ctx context.Context, ddl *DDLEvent) error
}
```

### 8.2 MySQL 事务表与非事务表处理

**问题**：MySQL GTID 模式下，事务表（InnoDB）和非事务表（MyISAM/MEMORY）不能在同一事务中操作，否则报错。

**解决方案**：WriteBatch 内部按表引擎分组，分别提交。

```go
// MySQLSinkConnector MySQL Sink 实现
type MySQLSinkConnector struct {
    db          *sql.DB
    config      *MySQLSinkConfig
    schemaCache *TableSchemaCache

    // 预编译语句缓存
    stmtCache sync.Map
}

// WriteBatch 批量写入
func (s *MySQLSinkConnector) WriteBatch(ctx context.Context, batch *EventBatch) error {
    if len(batch.Events) == 0 {
        return nil
    }

    // 按表引擎分组
    txnTables := make(map[TableID][]*ChangeEvent)    // 事务表（InnoDB）
    nonTxnTables := make(map[TableID][]*ChangeEvent) // 非事务表（MyISAM/MEMORY）

    for _, event := range batch.Events {
        tableID := TableID{
            Database: event.Table.Database,
            Table:    event.Table.Table,
        }

        // 获取表引擎类型
        engine := s.getTableEngine(tableID)

        if s.isTransactionalEngine(engine) {
            txnTables[tableID] = append(txnTables[tableID], event)
        } else {
            nonTxnTables[tableID] = append(nonTxnTables[tableID], event)
        }
    }

    // 1. 先写入非事务表（无事务，立即生效）
    if err := s.writeNonTransactionalTables(ctx, nonTxnTables); err != nil {
        return err
    }

    // 2. 再写入事务表（单事务）
    if err := s.writeTransactionalTables(ctx, txnTables); err != nil {
        return err
    }

    return nil
}

// isTransactionalEngine 判断是否为事务引擎
func (s *MySQLSinkConnector) isTransactionalEngine(engine string) bool {
    switch strings.ToUpper(engine) {
    case "INNODB", "NDB", "NDBCLUSTER":
        return true
    case "MYISAM", "MEMORY", "CSV", "ARCHIVE", "BLACKHOLE", "MERGE":
        return false
    default:
        // 默认视为事务表（更安全）
        return true
    }
}

// writeTransactionalTables 写入事务表（单事务）
func (s *MySQLSinkConnector) writeTransactionalTables(ctx context.Context, tables map[TableID][]*ChangeEvent) error {
    if len(tables) == 0 {
        return nil
    }

    // 开启事务
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // 按表处理
    for tableID, events := range tables {
        for _, event := range events {
            if err := s.executeEvent(ctx, tx, event); err != nil {
                return fmt.Errorf("table %s: %w", tableID, err)
            }
        }
    }

    // 提交事务
    return tx.Commit()
}

// writeNonTransactionalTables 写入非事务表（无事务）
func (s *MySQLSinkConnector) writeNonTransactionalTables(ctx context.Context, tables map[TableID][]*ChangeEvent) error {
    if len(tables) == 0 {
        return nil
    }

    // 非事务表：直接执行，无法回滚
    for tableID, events := range tables {
        for _, event := range events {
            // 直接使用连接执行，不开启事务
            if err := s.executeEvent(ctx, s.db, event); err != nil {
                return fmt.Errorf("table %s: %w", tableID, err)
            }
        }
    }

    return nil
}

// getTableEngine 获取表引擎类型
func (s *MySQLSinkConnector) getTableEngine(tableID TableID) string {
    // 从缓存获取
    if schema, ok := s.schemaCache.Get(tableID); ok {
        return schema.Engine
    }

    // 查询数据库
    var engine string
    query := `
        SELECT ENGINE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    `
    s.db.QueryRow(query, tableID.Database, tableID.Table).Scan(&engine)

    // 缓存结果
    s.schemaCache.SetEngine(tableID, engine)

    return engine
}

// executeEvent 执行单个事件
func (s *MySQLSinkConnector) executeEvent(ctx context.Context, executor Executor, event *ChangeEvent) error {
    switch event.Type {
    case EventTypeInsert:
        return s.executeInsert(ctx, executor, event)
    case EventTypeUpdate:
        return s.executeUpdate(ctx, executor, event)
    case EventTypeDelete:
        return s.executeDelete(ctx, executor, event)
    }
    return nil
}

// Executor 执行器接口（兼容 *sql.DB 和 *sql.Tx）
type Executor interface {
    ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
    QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
    QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}
```

### 8.3 PostgreSQL/Oracle/SQL Server 实现

**这些数据库所有表都支持事务，无需分组**：

```go
// PostgreSQLSinkConnector PostgreSQL Sink 实现
func (s *PostgreSQLSinkConnector) WriteBatch(ctx context.Context, batch *EventBatch) error {
    if len(batch.Events) == 0 {
        return nil
    }

    // PostgreSQL 所有表都支持事务，直接单事务写入
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    for _, event := range batch.Events {
        if err := s.executeEvent(ctx, tx, event); err != nil {
            return err
        }
    }

    return tx.Commit()
}

// OracleSinkConnector Oracle Sink 实现
// 同上，单事务写入所有表

// SQLServerSinkConnector SQL Server Sink 实现
// 同上，单事务写入所有表
```

### 8.4 表引擎信息缓存

```go
// TableSchema 表 Schema 信息（扩展）
type TableSchema struct {
    TableID  TableID
    Columns  []ColumnInfo

    // 主键列
    PrimaryKeyColumns []string

    // 唯一索引列
    UniqueIndexColumns [][]string

    // ===== MySQL 特有 =====
    // 表引擎
    Engine string

    // 是否为事务表
    IsTransactional bool
}

// TableSchemaCache Schema 缓存
type TableSchemaCache struct {
    mu      sync.RWMutex
    schemas map[TableID]*TableSchema
    source  SchemaFetcher
}

// Get 获取表 Schema
func (c *TableSchemaCache) Get(tableID TableID) (*TableSchema, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    schema, ok := c.schemas[tableID]
    return schema, ok
}

// SetEngine 设置表引擎（MySQL）
func (c *TableSchemaCache) SetEngine(tableID TableID, engine string) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if schema, ok := c.schemas[tableID]; ok {
        schema.Engine = engine
        schema.IsTransactional = isTransactionalEngine(engine)
    }
}
```

### 8.5 数据库事务特性总结

| 数据库 | 事务表 | 非事务表 | WriteBatch 策略 |
|--------|--------|---------|-----------------|
| MySQL/MariaDB | InnoDB, NDB | MyISAM, MEMORY, CSV | 按引擎分组，分别提交 |
| PostgreSQL | 所有表 | 无 | 单事务 |
| Oracle | 所有表 | 无 | 单事务 |
| SQL Server | 所有表 | 无 | 单事务 |
| MongoDB | - | - | 无事务概念 |

### 8.6 并发配置

```go
// ParallelSinkConfig 并发写入配置
type ParallelSinkConfig struct {
    // Worker 数量
    WorkerCount int `json:"worker-count" toml:"worker-count"`
    
    // 每个 Worker 的缓冲区大小
    BufferSize  int `json:"buffer-size" toml:"buffer-size"`
    
    // 分发策略：table, row-key, database, round-robin
    DispatchStrategy DispatchStrategy `json:"dispatch-strategy" toml:"dispatch-strategy"`
    
    // 事务模式：none, serial, parallel
    TxnMode     TxnMode `json:"txn-mode" toml:"txn-mode"`
}

// DispatchStrategy 分发策略
type DispatchStrategy string
const (
    DispatchTable    DispatchStrategy = "table"      // 按表分发，同表事件有序
    DispatchRowKey   DispatchStrategy = "row-key"    // 按主键分发，同行事件有序
    DispatchDatabase DispatchStrategy = "database"   // 按库分发，同库事件有序
    DispatchRoundRobin DispatchStrategy = "round-robin" // 轮询分发
)

// TxnMode 事务模式
type TxnMode string
const (
    TxnModeNone     TxnMode = "none"     // 无事务
    TxnModeSerial   TxnMode = "serial"   // 串行事务
    TxnModeParallel TxnMode = "parallel" // 并行事务
)
```

---

## 6. SnapshotCoordinator 快照协调器

### 6.1 多线程快照

```go
// SnapshotCoordinator 快照协调器 - 管理多线程快照
type SnapshotCoordinator struct {
    config      *SnapshotConfig
    source      SourceConnector
    sink        SinkConnector
    
    // 快照任务队列
    taskCh      chan *SnapshotTask
    
    // Worker 池
    workers     []*SnapshotWorker
    
    // 进度跟踪
    progress    *SnapshotProgress
    
    ctx         context.Context
    cancel      context.CancelFunc
    wg          sync.WaitGroup
}

// SnapshotTask 快照任务
type SnapshotTask struct {
    TableID     TableID
    Schema      *TableSchema
    ChunkID     int
    ChunkRange  *ChunkRange  // 分片范围（主键范围）
    Priority    int          // 优先级
}

// Run 启动快照协调器
func (sc *SnapshotCoordinator) Run() error {
    // 1. 扫描需要快照的表
    tables, err := sc.scanTables()
    if err != nil {
        return err
    }
    
    // 2. 为每个表生成快照任务（分片）
    for _, table := range tables {
        tasks := sc.generateTasks(table)
        for _, task := range tasks {
            sc.taskCh <- task
        }
    }
    
    // 3. 启动 Worker 池
    for i := 0; i < sc.config.ParallelThreads; i++ {
        worker := &SnapshotWorker{
            id:       i,
            taskCh:   sc.taskCh,
            resultCh: sc.resultCh,
            source:   sc.source,
        }
        sc.workers = append(sc.workers, worker)
        sc.wg.Add(1)
        go worker.Run(sc.ctx, &sc.wg)
    }
    
    return nil
}

// generateTasks 为表生成快照任务（分片）
func (sc *SnapshotCoordinator) generateTasks(table *TableInfo) []*SnapshotTask {
    var tasks []*SnapshotTask
    
    // 获取表的主键范围
    minKey, maxKey, err := sc.source.GetKeyRange(table)
    if err != nil {
        // 无法分片，整表作为一个任务
        return []*SnapshotTask{{
            TableID:  table.TableID,
            Schema:   table.Schema,
            ChunkID:  0,
            Priority: 0,
        }}
    }
    
    // 按 ChunkSize 分片
    chunkID := 0
    for start := minKey; start < maxKey; start += sc.config.ChunkSize {
        end := start + sc.config.ChunkSize
        if end > maxKey {
            end = maxKey
        }
        tasks = append(tasks, &SnapshotTask{
            TableID: table.TableID,
            Schema:  table.Schema,
            ChunkID: chunkID,
            ChunkRange: &ChunkRange{
                StartKey: start,
                EndKey:   end,
            },
            Priority: chunkID,
        })
        chunkID++
    }
    
    return tasks
}
```

---

## 7. SinkCoordinator Sink 协调器

### 7.1 多线程并发写入

```go
// SinkCoordinator Sink 协调器 - 管理多线程并发写入
type SinkCoordinator struct {
    config      *ParallelSinkConfig
    sink        SinkConnector
    
    // 分发器
    dispatcher  EventDispatcher
    
    // Worker 池
    workers     []*SinkWorker
    
    ctx         context.Context
    cancel      context.CancelFunc
    wg          sync.WaitGroup
}

// EventDispatcher 事件分发器
type EventDispatcher interface {
    // 分发事件到对应的 Worker
    Dispatch(event *ChangeEvent) int  // 返回 Worker ID
}

// TableDispatcher 按表分发
type TableDispatcher struct {
    workerCount int
    tableMap    sync.Map  // table -> workerID
}

func (d *TableDispatcher) Dispatch(event *ChangeEvent) int {
    key := event.Source.Database + "." + event.Source.Table
    if workerID, ok := d.tableMap.Load(key); ok {
        return workerID.(int)
    }
    
    // 新表，分配到负载最低的 Worker
    workerID := fnv32(key) % uint32(d.workerCount)
    d.tableMap.Store(key, int(workerID))
    return int(workerID)
}

// RowKeyDispatcher 按主键分发（同行事件有序）
type RowKeyDispatcher struct {
    workerCount int
}

func (d *RowKeyDispatcher) Dispatch(event *ChangeEvent) int {
    // 使用主键值计算 hash
    key := event.Source.Database + "." + event.Source.Table
    for _, pk := range event.After.Primary {
        key += fmt.Sprintf("%v", event.After.Fields[pk])
    }
    return int(fnv32(key) % uint32(d.workerCount))
}

// SinkWorker Sink Worker
type SinkWorker struct {
    id          int
    eventCh     chan *ChangeEvent
    sink        SinkConnector
    
    buffer      []*ChangeEvent
    bufferSize  int
}

// Run 运行 Worker
func (w *SinkWorker) Run(ctx context.Context) {
    ticker := time.NewTicker(100 * time.Millisecond)  // 批量刷新间隔
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            w.flush()  // 关闭前刷新缓冲区
            return
            
        case event := <-w.eventCh:
            w.buffer = append(w.buffer, event)
            if len(w.buffer) >= w.bufferSize {
                w.flush()
            }
            
        case <-ticker.C:
            if len(w.buffer) > 0 {
                w.flush()
            }
        }
    }
}

// flush 刷新缓冲区到 Sink
func (w *SinkWorker) flush() {
    if len(w.buffer) == 0 {
        return
    }
    
    batch := &EventBatch{Events: w.buffer}
    w.sink.WriteBatch(context.Background(), batch)
    w.buffer = w.buffer[:0]
}
```

---

## 8. Connector 层架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Source Connector                               │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    SyncScope Manager                             │    │
│  │  ┌──────────────────────┐  ┌──────────────────────┐             │    │
│  │  │  Database Level      │  │   Table Level        │             │    │
│  │  │  ┌───────────────┐   │  │  ┌───────────────┐   │             │    │
│  │  │  │ Single DB     │   │  │  │ Table Manager │   │             │    │
│  │  │  │ Multi DB      │   │  │  │ - AddTables() │   │             │    │
│  │  │  │ Wildcard (*)  │   │  │  │ - RemoveTables│   │             │    │
│  │  │  └───────────────┘   │  │  └───────────────┘   │             │    │
│  │  │        │             │  │         │            │             │    │
│  │  │        ▼             │  │         ▼            │             │    │
│  │  │  ┌───────────────┐   │  │  ┌───────────────┐   │             │    │
│  │  │  │ Auto-Discovery│   │  │  │ Schema Sync   │   │             │    │
│  │  │  │ - DDL Watch   │   │  │  │ + Snapshot    │   │             │    │
│  │  │  │ - New DB/Table│   │  │  └───────────────┘   │             │    │
│  │  │  └───────────────┘   │  └──────────────────────┘             │    │
│  │  └──────────────────────┘                                       │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                │                                        │
│                                ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    Snapshot Coordinator                         │    │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐              │    │
│  │  │ Worker 1│ │ Worker 2│ │ Worker 3│ │ Worker N│              │    │
│  │  │  (表1)  │ │  (表2)  │ │  (表3)  │ │  (表N)  │              │    │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘              │    │
│  │            Parallel Snapshot Processing                        │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                │                                        │
│                                ▼ ChangeEvent                            │
├─────────────────────────────────────────────────────────────────────────┤
│                           Pipeline Layer                                 │
│                  Filter → Transform → Router                            │
└─────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼ ChangeEvent
┌─────────────────────────────────────────────────────────────────────────┐
│                           Sink Coordinator                               │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    Event Dispatcher                             │    │
│  │         Table / RowKey / Database / Round-Robin                 │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                │                                        │
│                                ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    Sink Worker Pool                             │    │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐              │    │
│  │  │ Worker 1│ │ Worker 2│ │ Worker 3│ │ Worker N│              │    │
│  │  │ Buffer  │ │ Buffer  │ │ Buffer  │ │ Buffer  │              │    │
│  │  │ Flush   │ │ Flush   │ │ Flush   │ │ Flush   │              │    │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘              │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                │                                        │
│                                ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    Sink Connector                               │    │
│  │        MySQL / PostgreSQL / MongoDB / Kafka / HTTP              │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 9. 表结构存储与更新机制

### 9.1 概述

不同类型的连接器对表结构信息有不同的存储和管理策略：

| 连接器类型 | Schema 来源 | 存储方式 | DDL 后更新策略 |
|-----------|-------------|---------|---------------|
| MySQL/MariaDB | INFORMATION_SCHEMA | 内存缓存 | Invalidate + 懒加载 |
| PostgreSQL | pg_catalog | 内存缓存 | Invalidate + 懒加载 |
| SQL Server | sys.columns/INFORMATION_SCHEMA | 内存缓存 | Invalidate + 懒加载 |
| Oracle | ALL_TAB_COLUMNS | 内存缓存 | Invalidate + 懒加载 |
| MongoDB | 集合文档结构 | 无需缓存 | Change Stream 自动处理 |
| Kafka | 无 Schema | 无 | N/A (Schema Registry 可选) |
| Elasticsearch | Index Mapping | 无需缓存 | 动态映射或预定义 |
| Redis | 无 Schema | 无 | N/A |

---

### 9.2 关系型数据库 Schema 缓存机制

以 MySQL 为例说明关系型数据库的 Schema 缓存机制：

#### 9.2.1 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                    MySQL Schema 缓存架构                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  BinlogSyncer                                               │
│       │                                                     │
│       ▼                                                     │
│  ┌─────────────────┐                                        │
│  │  QueryEvent     │  (DDL语句: CREATE/ALTER/DROP)         │
│  └────────┬────────┘                                        │
│           │                                                 │
│           ▼                                                 │
│  ┌─────────────────┐      ┌─────────────────┐              │
│  │  DDL Parser     │──────▶ 解析DDL语句      │              │
│  │  (ANTLR)        │      │ 提取表名/库名    │              │
│  └────────┬────────┘      └─────────────────┘              │
│           │                                                 │
│           ▼                                                 │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              TableSchemaCache                        │   │
│  │  ┌─────────────────────────────────────────────────┐│   │
│  │  │  map[string]*event.TableInfo                    ││   │
│  │  │  key: "database.table"                          ││   │
│  │  │                                                 ││   │
│  │  │  • Invalidate(db, table) → 删除缓存             ││   │
│  │  │  • Get(db, table) → 查询DB + 缓存               ││   │
│  │  │  • Update(db, table, schema) → 更新缓存         ││   │
│  │  └─────────────────────────────────────────────────┘│   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

#### 9.2.2 核心数据结构

```go
// TableSchemaCache 表结构缓存
type TableSchemaCache struct {
    mu      sync.RWMutex
    schemas map[string]*event.TableInfo  // key: "database.table"
    db      *sql.DB
}

// TableInfo 表结构信息
type TableInfo struct {
    Database          string        // 数据库名
    Schema            string        // Schema 名（PostgreSQL/Oracle）
    Table             string        // 表名
    Columns           []ColumnInfo  // 列信息
    PrimaryKeyColumns []string      // 主键列
}

// ColumnInfo 列信息
type ColumnInfo struct {
    Name     string  // 列名
    Type     string  // 数据类型 (e.g., "varchar(255)", "int(11)")
    Nullable bool    // 是否允许 NULL
}
```

#### 9.2.3 DDL 处理流程

```go
// handleQueryEvent 处理 DDL 事件
func (s *BinlogSyncer) handleQueryEvent(ev *replication.BinlogEvent) error {
    query := string(queryEvent.Query)
    
    // 1. 检查是否是 DDL 语句
    if !isDDL(query) {
        return nil
    }
    
    // 2. 使用 DDL Parser 解析 DDL 语句
    var ddlResult *parser.DDLResult
    if s.parser != nil {
        results, err := s.parser.Parse(s.ctx, query)
        if err == nil && len(results) > 0 {
            ddlResult = results[0]
        }
    }
    
    // 3. 使缓存失效（关键步骤）
    if ddlResult != nil && ddlResult.Table != "" {
        // 已知表：只删除该表的缓存
        s.schemaCache.Invalidate(ddlResult.Database, ddlResult.Table)
    } else {
        // 未知表：清空所有缓存
        s.schemaCache.InvalidateAll()
    }
    
    // 4. 发送 DDL 事件到下游
    changeEvent := &event.ChangeEvent{
        Type: event.EventTypeDDL,
        Metadata: map[string]string{
            "ddl":     query,
            "ddlType": string(ddlResult.Type),
        },
    }
    s.events <- changeEvent
    
    return nil
}
```

#### 9.2.4 缓存更新策略

| DDL 操作 | 缓存处理 | 说明 |
|---------|---------|------|
| CREATE TABLE | `Invalidate(db, table)` | 下次访问时从DB查询新表结构 |
| ALTER TABLE | `Invalidate(db, table)` | 删除旧缓存，下次查询获取新结构 |
| DROP TABLE | `Invalidate(db, table)` | 删除缓存，表已不存在 |
| TRUNCATE TABLE | `Invalidate(db, table)` | 不改变结构，但使缓存失效 |
| RENAME TABLE | `InvalidateAll()` | 可能影响多表，清空全部 |

#### 9.2.5 懒加载查询

```go
// Get 获取表结构（懒加载）
func (c *TableSchemaCache) Get(ctx context.Context, database, table string) (*event.TableInfo, error) {
    key := database + "." + table
    
    // 1. 先查缓存
    c.mu.RLock()
    if schema, ok := c.schemas[key]; ok {
        c.mu.RUnlock()
        return schema.Clone(), nil
    }
    c.mu.RUnlock()
    
    // 2. 缓存未命中，从 INFORMATION_SCHEMA 查询
    schema, err := c.querySchema(ctx, database, table)
    if err != nil {
        return nil, err
    }
    
    // 3. 写入缓存
    c.mu.Lock()
    c.schemas[key] = schema
    c.mu.Unlock()
    
    return schema.Clone(), nil
}

// querySchema 从数据库查询表结构
func (c *TableSchemaCache) querySchema(ctx context.Context, database, table string) (*event.TableInfo, error) {
    rows, err := c.db.QueryContext(ctx, `
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    `, database, table)
    // ... 解析结果
}
```

#### 9.2.6 其他关系型数据库

**PostgreSQL:**
```go
// 查询 pg_catalog
SELECT column_name, data_type, is_nullable
FROM pg_catalog.pg_columns
WHERE table_schema = $1 AND table_name = $2
ORDER BY ordinal_position
```

**SQL Server:**
```go
// 查询 INFORMATION_SCHEMA
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
ORDER BY ORDINAL_POSITION
```

**Oracle:**
```go
// 查询 ALL_TAB_COLUMNS
SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
FROM ALL_TAB_COLUMNS
WHERE OWNER = :owner AND TABLE_NAME = :table
ORDER BY COLUMN_ID
```

---

### 9.3 NoSQL 数据库 Schema 机制

#### 9.3.1 MongoDB (无 Schema)

MongoDB 是 Schema-less 数据库，文档结构可以动态变化：

```
┌─────────────────────────────────────────────────────────────┐
│                    MongoDB Schema 处理                      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Change Stream                                              │
│       │                                                     │
│       ▼                                                     │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  {                                                   │   │
│  │    "operationType": "insert",                        │   │
│  │    "fullDocument": {                                 │   │
│  │      "_id": ObjectId("..."),                         │   │
│  │      "name": "John",         // 字段可能存在          │   │
│  │      "age": 30,              // 字段可能不存在        │   │
│  │      "email": "john@example.com"  // 动态添加        │   │
│  │    }                                                 │   │
│  │  }                                                   │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  特点：                                                     │
│  • 无需缓存表结构                                           │
│  • Change Stream 自动包含完整文档                           │
│  • DDL 概念不同 (createCollection/dropCollection)          │
│  • Sink 端直接使用文档字段                                  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**MongoDB Source 处理逻辑：**
```go
// 无需 Schema Cache
func (c *Connector) handleChangeEvent(changeDoc bson.M) *event.ChangeEvent {
    // Change Stream 已包含完整文档
    // 无需额外查询 Schema
    return &event.ChangeEvent{
        Type: mapOperationType(changeDoc["operationType"]),
        After: extractRowData(changeDoc["fullDocument"]),
    }
}
```

---

### 9.4 消息队列 Schema 机制

#### 9.4.1 Kafka (无内置 Schema)

Kafka 本身不存储 Schema，事件以字节流形式传输：

```
┌─────────────────────────────────────────────────────────────┐
│                    Kafka Schema 处理                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Producer (Source)                                          │
│       │                                                     │
│       ▼                                                     │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  序列化选项：                                         │   │
│  │                                                       │   │
│  │  1. JSON (默认)                                       │   │
│  │     {"id": 1, "name": "John", "op": "INSERT"}        │   │
│  │                                                       │   │
│  │  2. Avro + Schema Registry (推荐)                     │   │
│  │     - Schema 存储在 Registry                          │   │
│  │     - 消息只包含 schema_id + 数据                     │   │
│  │                                                       │   │
│  │  3. Protobuf                                          │   │
│  │     - 需要预定义 .proto 文件                          │   │
│  └─────────────────────────────────────────────────────┘   │
│       │                                                     │
│       ▼                                                     │
│  Kafka Topic                                                │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Topic: datastream.events                            │   │
│  │  Partition 0: [Message1, Message2, ...]              │   │
│  │  Partition 1: [Message3, Message4, ...]              │   │
│  │                                                       │   │
│  │  消息格式: Key + Value (字节数组)                     │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Schema Registry (可选):                                    │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  subjects:                                            │   │
│  │    datastream.events-value:                          │   │
│  │      1: {"type": "record", "fields": [...]}          │   │
│  │      2: {"type": "record", "fields": [...]}  // v2   │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Kafka Sink 配置示例：**
```toml
[sink.properties]
# 序列化格式
value_serializer = "json"  # json, avro, protobuf

# Schema Registry (Avro/Protobuf 需要)
schema_registry_url = "http://localhost:8081"

# DDL 处理
# Kafka 不执行 DDL，只传递 Schema 变更事件
# 下游消费者需要自行处理 Schema 演进
```

---

### 9.5 搜索引擎 Schema 机制

#### 9.5.1 Elasticsearch (动态映射)

Elasticsearch 支持 Schema-less 和显式 Mapping 两种模式：

```
┌─────────────────────────────────────────────────────────────┐
│                  Elasticsearch Schema 处理                  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  模式一：动态映射 (Dynamic Mapping)                          │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  PUT /datastream-users/_doc/1                       │   │
│  │  {                                                   │   │
│  │    "name": "John",      // 自动映射为 text           │   │
│  │    "age": 30,           // 自动映射为 integer        │   │
│  │    "created_at": "2024-01-01"  // 自动映射为 date    │   │
│  │  }                                                   │   │
│  │                                                       │   │
│  │  ES 自动推断类型并创建 Mapping                        │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  模式二：显式映射 (Explicit Mapping)                         │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  PUT /datastream-users                              │   │
│  │  {                                                   │   │
│  │    "mappings": {                                     │   │
│  │      "properties": {                                 │   │
│  │        "name": {"type": "keyword"},                  │   │
│  │        "age": {"type": "integer"},                   │   │
│  │        "created_at": {"type": "date"}                │   │
│  │      }                                               │   │
│  │    }                                                 │   │
│  │  }                                                   │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  DDL 处理：                                                 │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  ALTER TABLE 添加新列 →                              │   │
│  │    • 动态映射: 自动添加新字段                         │   │
│  │    • 显式映射: 需要手动更新 Mapping (PUT mapping)    │   │
│  │                                                       │   │
│  │  ALTER TABLE 修改列类型 →                            │   │
│  │    • ES 不支持修改已存在字段的类型                    │   │
│  │    • 需要 reindex 到新 Index                         │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Elasticsearch Sink 配置示例：**
```toml
[sink.properties]
# 索引命名模式
index_pattern = "{database}_{table}"

# 文档 ID 策略
doc_id_strategy = "primary_key"  # 使用主键作为文档 ID

# Mapping 策略
mapping_mode = "dynamic"  # dynamic 或 explicit
```

---

### 9.6 缓存数据库 Schema 机制

#### 9.6.1 Redis (无 Schema)

Redis 是 Key-Value 存储，无 Schema 概念：

```
┌─────────────────────────────────────────────────────────────┐
│                    Redis Schema 处理                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  数据格式由 Sink 配置决定：                                  │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  格式一：Hash (推荐)                                  │   │
│  │  HSET ds:users:123 name "John" age "30"             │   │
│  │                                                       │   │
│  │  格式二：JSON String                                  │   │
│  │  SET ds:users:123 '{"name":"John","age":30}'        │   │
│  │                                                       │   │
│  │  格式三：String (简单值)                              │   │
│  │  SET ds:users:123:name "John"                        │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  DDL 处理：                                                 │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  • Redis 无 DDL 概念                                  │   │
│  │  • 新字段直接写入 Key                                 │   │
│  │  • 字段删除：不写入该字段                             │   │
│  │  • 类型变更：覆盖写入新值                             │   │
│  │                                                       │   │
│  │  注意事项：                                            │   │
│  │  • Hash 格式可支持字段级更新                          │   │
│  │  • JSON 格式需要整体覆盖                              │   │
│  │  • 旧数据可能存在已删除字段                           │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Redis Sink 配置示例：**
```toml
[sink.properties]
# 存储格式
format = "hash"  # hash, json, string

# Key 命名模式
key_pattern = "{database}:{table}:{id}"

# TTL 设置
ttl = 0  # 0 表示永不过期
```

---

### 9.7 Schema 演进策略总结

| 场景 | 关系型数据库 | MongoDB | Kafka | Elasticsearch | Redis |
|------|-------------|---------|-------|---------------|-------|
| 添加列 | ALTER TABLE → Invalidate Cache | 自动支持 | 新字段写入 | 动态映射自动添加 | 直接写入 |
| 删除列 | ALTER TABLE → Invalidate Cache | 文档中不存在 | 不写入该字段 | 字段仍存在(需reindex) | 不写入 |
| 修改列类型 | ALTER TABLE → Invalidate Cache | 类型可变 | Schema Registry演进 | 需reindex新Index | 覆盖写入 |
| 重命名列 | ALTER TABLE → Invalidate Cache | 需要迁移 | 新字段名 | 需reindex | 写入新Key |
| DDL传播 | 执行DDL | createCollection事件 | 发送DDL消息 | 更新Mapping(可选) | N/A |

---

### 9.8 设计原则

1. **缓存失效优先**：DDL 时先使缓存失效，避免使用过时结构
2. **懒加载**：只在需要时查询 Schema，减少数据库压力
3. **并发安全**：使用读写锁保护 Schema 缓存
4. **优雅降级**：DDL 解析失败时清空全部缓存，保证一致性
5. **Sink 端自治**：Kafka/ES/Redis 等 Sink 根据自身特性处理 Schema 变更

---

*返回 [设计文档总览](./Design.md)*

---

## 2026-05-16 StatsProvider 可选接口

为支持 Prometheus pull-mode gauge 监控，新增 `internal/connector/stats.go`：

```go
type StatsProvider interface {
    Stats(ctx context.Context) Stats
}

type Stats struct {
    QueueSize, QueueCapacity int64
    Position string  // opaque; MySQL/MariaDB MUST honor binlog mode (file-pos OR GTID)
    LagSeconds float64  // NaN if unknown; negative clamped to 0
    LastEventTime time.Time
    SnapshotRunning bool
    SnapshotProgress float64  // 0-100
    SnapshotTotalTables, SnapshotRemainingTables int64
    Connected bool
}
```

约束：
- `Stats(ctx)` 必须线程安全、非阻塞、ctx-aware
- 不能 panic；StatsCollector 会 recover 并跳过本次采样
- 零值即"不适用"

12 个连接器已实现最小版本（`Connected` + `Position`）；snapshot/lag 等字段
随连接器内部跟踪增强逐步填充。
