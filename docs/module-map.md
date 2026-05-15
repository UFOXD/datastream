# DataStream 项目模块图谱

> Last Updated: 2026-05-13

## 架构层次

### 核心层 (pkg/)

| 模块 | 路径 | 状态 | 依赖 | 被依赖 |
|------|------|------|------|--------|
| event | pkg/event | ✅ 完成 | - | source, sink, parser, filter, transform |
| parser | pkg/parser | ✅ 完成 | event | source |
| config | pkg/config | ✅ 完成 | - | 所有模块 |
| errors | pkg/errors | ✅ 完成 | - | 所有模块 |
| logutil | pkg/logutil | ✅ 完成 | go.uber.org/zap | 所有模块 |
| utils | pkg/utils | ✅ 完成 | - | source, sink |
| metrics | pkg/metrics | ✅ 完成 | prometheus | app |
| version | pkg/version | ✅ 完成 | - | cli |

### 业务层 (internal/)

| 模块 | 路径 | 状态 | 依赖 | 被依赖 |
|------|------|------|------|--------|
| source | internal/source | ✅ 完成 | event, parser, logutil, offset | coordinator |
| sink | internal/sink | ✅ 完成 | event, logutil | coordinator |
| pipeline | internal/pipeline | ✅ 完成 | event, logutil, source, sink | app |
| filter | internal/filter | ✅ 完成 | event | pipeline |
| transform | internal/transform | ✅ 完成 | event | pipeline |
| ratelimit | internal/ratelimit | ✅ 完成 | golang.org/x/time | source, sink |
| router | internal/router | ✅ 完成 | event | pipeline |
| coordinator | internal/coordinator | ✅ 完成 | etcd | app |
| offset | internal/offset | ✅ 完成 | - | source, coordinator |
| api | internal/api | ✅ 完成 | gorilla/mux | app |
| cli | internal/cli | ✅ 完成 | spf13/cobra | cmd |
| app | internal/app | ✅ 完成 | - | cmd |

### 状态说明

- ✅ 完成: 功能完整，测试覆盖
- 🔄 进行中: 正在开发
- 📋 计划中: 已规划，未开始
- ⚠️ 有问题: 存在已知问题

---

## 模块详细说明

### internal/filter

**功能**: 事件过滤模块

| 组件 | 文件 | 说明 |
|------|------|------|
| ExpressionFilter | expression.go | 表达式过滤器，支持 table/database/field 匹配 |

**支持的操作符**:
- 比较: `==`, `!=`, `>`, `<`, `>=`, `<=`
- 逻辑: `&&`, `||`
- 正则: `=~`, `!~`
- 字段访问: `table`, `database`, `after.*`, `before.*`

### internal/transform

**功能**: 事件转换模块

| 组件 | 文件 | 说明 |
|------|------|------|
| CustomTransformer | custom.go | 自定义转换器 |
| ScriptTransformerRegistry | custom.go | 转换器注册表 |

**内置转换器**:
- `NewAddFieldTransformer` - 添加字段
- `NewRemoveFieldTransformer` - 删除字段
- `NewRenameFieldTransformer` - 重命名字段
- `NewTimestampTransformer` - 添加时间戳

### internal/pipeline

**功能**: 管道核心逻辑

| 组件 | 文件 | 说明 |
|------|------|------|
| BackpressureController | backpressure.go | 背压控制器 |
| PersistentBuffer | persistent_buffer.go | 持久化缓冲器（Badger后端） |
| MemoryBuffer | buffer.go | 内存缓冲器 |
| BatchBuffer | buffer.go | 批量缓冲器 |

**BackpressureController 配置**:
- `HighWatermark`: 触发暂停的队列使用率阈值 (默认80%)
- `LowWatermark`: 触发恢复的队列使用率阈值 (默认50%)
- `MaxLatency`: 最大可接受延迟
- `CheckInterval`: 检查间隔

### internal/ratelimit

**功能**: 限流模块

| 组件 | 文件 | 说明 |
|------|------|------|
| Limiter | ratelimit.go | 双限流器（行数+字节数） |

**配置项**:
- `SourceRowsPerSecond`: 源端每秒行数限制
- `SourceBytesPerSecond`: 源端每秒字节数限制
- `BurstSize`: 突发大小

### internal/sink

**功能**: Sink连接器及并发写入

| 组件 | 文件 | 说明 |
|------|------|------|
| HashDispatcher | hash_dispatcher.go | 哈希分发器，保证同Row顺序 |
| ConcurrentSinkWriter | concurrent_writer.go | 并发写入器 |
| RowIdentifier | row_identifier.go | 行标识符提取 |

### internal/source

**功能**: Source连接器及快照并发

| 组件 | 文件 | 说明 |
|------|------|------|
| SnapshotCoordinator | snapshot_coordinator.go | 快照协调器 |
| SnapshotConcurrencyConfig | snapshot_config.go | 快照并发配置 |
| matchPattern | binlog_syncer.go | 通配符模式匹配（支持 * 和 ?） |

**matchPattern 通配符支持**:
- `*` - 匹配任意字符序列（包括空）
- `?` - 匹配单个字符
- 示例: `table*` 匹配 `table1`, `table_name`; `*_suffix` 匹配 `abc_suffix`

---

## 模块依赖图

```
                    ┌─────────────┐
                    │    cmd/     │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  internal/  │
                    │     app     │
                    └──────┬──────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
    ┌────▼────┐      ┌─────▼─────┐     ┌────▼────┐
    │ source  │      │ pipeline  │     │  sink   │
    └────┬────┘      └─────┬─────┘     └────┬────┘
         │                 │                 │
    ┌────┴────┐      ┌─────┴─────┐     ┌────┴────┐
    │ratelimit│      │   filter  │     │   hash  │
    │         │      │ transform │     │dispatcher│
    └────┬────┘      │backpressure│    └────┬────┘
         │           └─────┬─────┘          │
         └─────────────────┼────────────────┘
                           │
                    ┌──────▼──────┐
                    │   pkg/event │
                    └─────────────┘
```

---

## 数据流

```
Source Connector
       │
       ▼
  RateLimiter ──────────────────┐
       │                        │
       ▼                        │
    Filter ──► ExpressionFilter │
       │                        │
       ▼                        │
  Transform ──► CustomTransformer
       │                        │
       ▼                        │
    Router                      │
       │                        │
       ▼                        │
BackpressureController ◄────────┘
       │
       ▼
HashDispatcher
       │
       ▼
ConcurrentSinkWriter
       │
       ▼
Sink Connector
```

---

## 公开 API 列表

### internal/filter

```go
// ExpressionFilter 过滤器
func NewExpressionFilter(cfg *ExpressionConfig) (*ExpressionFilter, error)
func (ef *ExpressionFilter) Filter(e *event.ChangeEvent) (bool, error)
func (ef *ExpressionFilter) Expression() string

// 配置
type ExpressionConfig struct {
    Expression string `json:"expression" toml:"expression"`
}
```

### internal/transform

```go
// CustomTransformer 转换器
func NewCustomTransformer(config *CustomTransformerConfig) (*CustomTransformer, error)
func (t *CustomTransformer) Transform(e *event.ChangeEvent) (*event.ChangeEvent, error)
func (t *CustomTransformer) Name() string

// 内置转换器
func NewAddFieldTransformer(fieldName string, value interface{}) *CustomTransformer
func NewRemoveFieldTransformer(fieldName string) *CustomTransformer
func NewRenameFieldTransformer(oldName, newName string) *CustomTransformer
func NewTimestampTransformer(fieldName string) *CustomTransformer

// 注册表
func NewScriptTransformerRegistry() *ScriptTransformerRegistry
func (r *ScriptTransformerRegistry) Register(name string, t *CustomTransformer)
func (r *ScriptTransformerRegistry) Get(name string) (*CustomTransformer, bool)
func (r *ScriptTransformerRegistry) Remove(name string)
func (r *ScriptTransformerRegistry) List() []string
```

### internal/pipeline

```go
// BackpressureController 背压控制器
func NewBackpressureController(config *BackpressureConfig) *BackpressureController
func (b *BackpressureController) Start()
func (b *BackpressureController) Stop()
func (b *BackpressureController) UpdateMetrics(queueSize, maxQueueSize int64, latency time.Duration)
func (b *BackpressureController) State() BackpressureState
func (b *BackpressureController) ShouldPause() bool
func (b *BackpressureController) WaitWhilePaused(ctx context.Context) error
func (b *BackpressureController) OnPause(fn func())
func (b *BackpressureController) OnResume(fn func())

// 配置
func DefaultBackpressureConfig() *BackpressureConfig

// PersistentBuffer 持久化缓冲器
func NewPersistentBuffer(config *PersistentBufferConfig) (*PersistentBuffer, error)
func (b *PersistentBuffer) Put(ctx context.Context, e *event.ChangeEvent) error
func (b *PersistentBuffer) Get(ctx context.Context, batchSize int) ([]*event.ChangeEvent, error)
func (b *PersistentBuffer) Flush(ctx context.Context) error
func (b *PersistentBuffer) Close() error
func (b *PersistentBuffer) Replay(ctx context.Context) (int, error)
func (b *PersistentBuffer) Clear() error
func (b *PersistentBuffer) Stats() (*PersistentBufferStats, error)

// 配置
type PersistentBufferConfig struct {
    Capacity      int           // 内存缓冲容量
    Path          string        // Badger数据库路径
    SyncWrites    bool          // 同步写入
    FlushInterval time.Duration // 刷新间隔
}
```

### internal/ratelimit

```go
// Limiter 限流器
func NewLimiter(config *Config) *Limiter
func (rl *Limiter) Wait(ctx context.Context) error
func (rl *Limiter) WaitN(ctx context.Context, n int) error
func (rl *Limiter) WaitRowsAndBytes(ctx context.Context, rows int, bytes int64) error
func (rl *Limiter) Allow() bool
func (rl *Limiter) AllowN(n int) bool
func (rl *Limiter) SetLimit(limit int)
func (rl *Limiter) SetBurst(burst int)
func (rl *Limiter) Delay(n int) time.Duration

// 配置
func DefaultConfig() *Config
```

---

*文档版本：v1.0*
*创建时间：2026-05-13*
