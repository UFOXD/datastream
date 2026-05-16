# DataStream Prometheus 监控集成设计

> **状态**：Draft v3（评审 2 轮后）
> **作者**：DataStream Team
> **日期**：2026-05-16
> **关联设计文档**：`docs/design/core-design.md` §4、`docs/design/connector-design.md`、`docs/design/pipeline-design.md`、`docs/design/api-cli-design.md`

---

## 1. 背景

`pkg/metrics` 已有指标定义齐全（task / source / sink / pipeline / cluster），但实际有 4 个问题：

| 问题 | 现状 |
|------|------|
| `/metrics` endpoint 是假的 | `internal/api/server.go:369-377` 只返回 `# DataStream Metrics\n` 字符串 |
| 现有 4 处调用 label 数量不符 | `internal/pipeline/pipeline.go:198/234/342/345` 只传 2 个 label，定义要 3 个，运行时 panic |
| Source/Sink 连接器完全未接入 | 12 个连接器内部无任何 metric 调用 |
| 命名冲突 | `pkg/metrics.Namespace = "datastream"` 是 Prometheus namespace（指标前缀），与原计划的"集群标识 label"重名 |

同时对照 Debezium，缺失：`source_lag_seconds`、队列容量、连接健康度、快照表数。

**本任务范围**：把现有 metric 真正接入。

### 明确不在本任务范围（scope drift 别出）

| 别出项 | 推迟到 |
|--------|--------|
| 重试架构统一（pipeline 层统一） | 独立任务：「重试架构重构」 |
| `pkg/utils.IsRetryableError` 与 `pkg/errors.IsRetryableError` 合并 | 同上 |
| `connector_retries_total` 指标 | 同上（依赖重试统一） |
| `events_filtered_total` 指标 | 独立任务：「filter pipeline 集成」 |
| `last_error_message` API 字段 | 独立任务：「task status API 增强」 |
| `TaskState.LastError` 字段扩展 | 同上 |

`error_type` label 仍保留（取值 `retriable/non_retriable`），独立于重试架构合并任务——它只是分类已经发生的错误，不依赖重试逻辑统一。

---

## 2. 决策汇总

| 决策项 | 选择 |
|--------|------|
| label 名 | **`cluster`**（不是 `namespace`），CLI flag `--cluster` |
| Source 埋点 | Pipeline 消费点直接埋点（`source.Connector` 是 channel 风格无 `Read()`） |
| Sink 埋点 | 装饰器，**同时实现** `Connector` + `BatchConnector` + `AsyncConnector` 透传 |
| 多 sink 处理 | `Pipeline.sinks` 是切片，每个 sink 各包装一层 MetricsSink |
| StatsProvider 包归属 | `internal/connector/stats.go`（新建 connector 包） |
| `error_type` label | `retriable` / `non_retriable`，用 `pkg/errors.IsRetryableError` |
| TaskEvents 拆分 | 加 `result=success/failed` label；`type` 严格事件类型 |
| EventType 取值 | `insert/update/delete/truncate/ddl/heartbeat/tombstone`（7 种，对齐 `pkg/event.EventType`） |
| `result=failed` 时机 | 仅 `utils.Retry` 最终返回失败时计；不在每次重试 |
| 监控开关 | 仅全局 `cfg.Metrics.Enabled`（默认 true） |
| `Stats(ctx)` 签名 | 接受 ctx，collector 传 1s 超时 |
| `Stats(ctx)` 并发 | 每 provider 独立 goroutine + WaitGroup |
| Unregister label 清理 | 不主动清理，接受 Prometheus 3min staleness |
| `pipeline_queue_size` 写入 | pull-only |
| Prometheus Registry | 重构 `pkg/metrics/metrics.go` 为 `MustRegisterAll(r)` 显式注册 + `ResetForTest()` |
| Pipeline 状态机 | 修复 panic：使用 `SetTaskTotal(cluster, state, 1)` 显式 Set |
| `task_state` 新增指标 | `datastream_task_state{cluster, task, state}` (0/1) 补单 task 维度观测 |
| `ChangeEvent.Size()` | **本任务同时新增**，返回字段累计估算字节数 |
| label vec 缓存 | 装饰器初始化时预先 `WithLabelValues(...)` 缓存，避免热路径 hashmap 查找 |
| 砍掉 | `queue_bytes`、`position_numeric`、`events_filtered_total`、`connector_retries_total`、`last_error_message`、任务级开关 |
| Grafana 面板 | `deployments/grafana/datastream-dashboard.json` |

---

## 3. 总体架构

```
通路 1：Pipeline 消费点（push）— success 计数 + bytes + lag
  for select { case e := <-p.source.Events():
      p.eventCounters[e.Type].Inc()        // 预缓存的 WithLabelValues
      p.eventBytesAdder.Add(float64(e.Size()))
      if !e.Timestamp.IsZero() { lag = now-e.Timestamp; SetSourceLag(... lag) }
      p.processEvent(ctx, e)               // 内部调用 sink.Write 经装饰器
  }

通路 2：Sink 装饰器（push）— sink_write_latency + errors + result=failed
  装饰器同时实现 Connector / BatchConnector / AsyncConnector：
      Write / WriteBatch / WriteAsync 都加 Timer + ClassifyError 分类
      失败：IncSinkWriteErrors{error_type}; 估算 result=failed 计数

通路 3：StatsCollector（pull）— gauge
  并发 goroutine + 1s timeout per provider
  Stats(ctx) → 字段映射 → Gauge.Set
```

| 通路 | 采集内容 | 实现位置 |
|------|---------|---------|
| 1（Pipeline 消费点） | `task_events_total{result=success}`、`task_events_bytes`、`source_lag_seconds`、`source_last_event_seconds` | `internal/pipeline/pipeline.go` |
| 2（Sink 装饰器） | `sink_write_latency_seconds`、`sink_write_errors_total{error_type}`、`task_events_total{result=failed}` | `internal/sink/decorator/` 新建 |
| 3（StatsCollector） | `pipeline_queue_size/capacity`、`source_snapshot_progress`、`snapshot_tables_total/remaining`、`connector_connected`、`task_state` | `pkg/metrics/collector_runtime.go` 新建 |

---

## 4. 接口与数据结构

### 4.1 ChangeEvent.Size() 新增（前置条件）

```go
// pkg/event/change_event.go
// Size returns an estimated byte size of the event for metrics accounting.
// This is a rough estimate (not exact serialized size) sufficient for byte-rate
// observability. The estimate counts: source database/table names, all field
// name+value byte lengths in Before and After, and a fixed overhead for metadata.
func (e *ChangeEvent) Size() int {
    if e == nil {
        return 0
    }
    n := len(e.Source.Database) + len(e.Table.Database) + len(e.Table.Name) + 64
    for name, f := range e.Before.Fields {
        n += len(name) + estimateValueSize(f.Value)
    }
    for name, f := range e.After.Fields {
        n += len(name) + estimateValueSize(f.Value)
    }
    return n
}

func estimateValueSize(v interface{}) int {
    switch x := v.(type) {
    case nil:
        return 0
    case string:
        return len(x)
    case []byte:
        return len(x)
    default:
        return 16 // numeric/bool fixed estimate
    }
}
```

实现注意：本估算用于 metric 的趋势观测，**不要求精确**。如果未来需要精确大小，可改为序列化后计算。

### 4.2 StatsProvider 接口（`internal/connector/stats.go`）

```go
// internal/connector/stats.go
package connector

import (
    "context"
    "time"
)

// StatsProvider is an optional interface that source/sink connectors MAY
// implement to expose runtime state for Prometheus gauge metrics.
// Connectors that don't implement this interface are skipped by the stats
// collector. App startup logs each connector's stats_provider=true/false.
type StatsProvider interface {
    // Stats returns a snapshot of the connector's runtime state.
    // Implementations MUST:
    //   - be safe for concurrent calls
    //   - return promptly; honor ctx
    //   - return zero values for fields that are "not applicable"
    //   - NOT panic; collector recovers and skips this sample on panic
    Stats(ctx context.Context) Stats
}

// Stats holds a snapshot of a connector's runtime state.
// See §4.2.1 for the field-to-metric mapping table.
type Stats struct {
    // --- Queue ---
    QueueSize     int64
    QueueCapacity int64

    // --- Position --- opaque string. For MySQL/MariaDB, format depends on the
    // connector's binlog mode (file-pos or GTID); implementations MUST NOT
    // hard-code one format.
    Position string

    // --- Lag & progress ---
    LagSeconds       float64   // now - event_time; NaN if unknown; clamp to 0 if negative
    LastEventTime    time.Time // zero if no event observed yet
    SnapshotRunning  bool
    SnapshotProgress float64   // 0-100
    SnapshotTotalTables     int64
    SnapshotRemainingTables int64

    // --- Connection ---
    Connected bool
}
```

**注意**：v2 中的 `LastErrorMessage` / `LastErrorTime` / `RetriesSoFar` 字段移除——这些都依赖未在本任务范围内的功能（last_error API、重试合并）。

#### 4.2.1 Stats 字段到 metric 的映射

| Stats 字段 | 对应 metric | 通路 | 备注 |
|-----------|-------------|------|------|
| `QueueSize` | `pipeline_queue_size` (Gauge) | pull | `Set` |
| `QueueCapacity` | `pipeline_queue_capacity` (Gauge) | pull | `Set` |
| `Position` | — | — | 仅日志/诊断 |
| `LagSeconds` | `source_lag_seconds` (Gauge) | pull | NaN 跳过；负值 clamp 到 0 |
| `LastEventTime` | `source_last_event_seconds` (Gauge) | pull | zero 跳过；写入 Unix 秒 |
| `SnapshotRunning` | — | — | 通过 `SnapshotProgress > 0` 隐式表达 |
| `SnapshotProgress` | `source_snapshot_progress` (Gauge，现有) | pull | `Set` |
| `SnapshotTotalTables` | `snapshot_tables_total` (Gauge) | pull | `Set` |
| `SnapshotRemainingTables` | `snapshot_tables_remaining` (Gauge) | pull | `Set` |
| `Connected` | `connector_connected` (Gauge) | pull | true=1, false=0 |

### 4.3 Sink 装饰器（`internal/sink/decorator/sink.go`）

需要透明实现 `Connector` + `BatchConnector` + `AsyncConnector` 三个接口：

```go
// internal/sink/decorator/sink.go
package decorator

import (
    "context"

    "github.com/UFOXD/datastream/internal/connector"
    "github.com/UFOXD/datastream/internal/sink"
    "github.com/UFOXD/datastream/pkg/event"
    "github.com/UFOXD/datastream/pkg/metrics"
    "github.com/prometheus/client_golang/prometheus"
)

// MetricsSink wraps a sink.Connector and emits metrics on Write/WriteBatch/WriteAsync.
type MetricsSink struct {
    inner    sink.Connector
    cluster  string
    taskID   string
    sinkType string

    // Pre-cached label vectors to avoid hashmap lookup in hot path.
    successCounters map[event.EventType]prometheus.Counter
    failedCounters  map[event.EventType]prometheus.Counter
    bytesAdder      prometheus.Counter
    latencyObserver prometheus.Observer
    errorCounterRetriable    prometheus.Counter
    errorCounterNonRetriable prometheus.Counter
}

func WrapSink(s sink.Connector, cluster, taskID, sinkType string) sink.Connector {
    m := &MetricsSink{
        inner: s, cluster: cluster, taskID: taskID, sinkType: sinkType,
    }
    m.precacheLabels()
    return m
}

func (m *MetricsSink) precacheLabels() {
    types := []event.EventType{
        event.EventTypeInsert, event.EventTypeUpdate, event.EventTypeDelete,
        event.EventTypeTruncate, event.EventTypeDDL,
        event.EventTypeHeartbeat, event.EventTypeTombstone,
    }
    m.successCounters = make(map[event.EventType]prometheus.Counter, len(types))
    m.failedCounters  = make(map[event.EventType]prometheus.Counter, len(types))
    for _, t := range types {
        m.successCounters[t] = metrics.TaskEventsTotal.WithLabelValues(m.cluster, m.taskID, string(t), "success")
        m.failedCounters[t]  = metrics.TaskEventsTotal.WithLabelValues(m.cluster, m.taskID, string(t), "failed")
    }
    m.bytesAdder = metrics.TaskEventsBytes.WithLabelValues(m.cluster, m.taskID)
    m.latencyObserver = metrics.SinkWriteLatency.WithLabelValues(m.cluster, m.taskID, m.sinkType)
    m.errorCounterRetriable    = metrics.SinkWriteErrors.WithLabelValues(m.cluster, m.taskID, m.sinkType, string(metrics.ErrorTypeRetriable))
    m.errorCounterNonRetriable = metrics.SinkWriteErrors.WithLabelValues(m.cluster, m.taskID, m.sinkType, string(metrics.ErrorTypeNonRetriable))
}

// --- sink.Connector ---
func (m *MetricsSink) Write(ctx context.Context, events []*event.ChangeEvent) error {
    start := time.Now()
    err := m.inner.Write(ctx, events)
    m.latencyObserver.Observe(time.Since(start).Seconds())
    m.recordResult(events, err)
    return err
}

// --- sink.BatchConnector (optional) ---
func (m *MetricsSink) WriteBatch(ctx context.Context, events []*event.ChangeEvent, batchSize int) error {
    bc, ok := m.inner.(sink.BatchConnector)
    if !ok {
        return m.Write(ctx, events)  // fallback to Connector.Write
    }
    start := time.Now()
    err := bc.WriteBatch(ctx, events, batchSize)
    m.latencyObserver.Observe(time.Since(start).Seconds())
    m.recordResult(events, err)
    return err
}

// --- sink.AsyncConnector (optional) ---
func (m *MetricsSink) WriteAsync(ctx context.Context, events []*event.ChangeEvent) error {
    ac, ok := m.inner.(sink.AsyncConnector)
    if !ok {
        return m.Write(ctx, events)
    }
    // Async: timer measures only enqueue time, not actual write time.
    // Final success/failure should be tracked by inner via a callback; current
    // approach only records enqueue latency and synchronous enqueue errors.
    start := time.Now()
    err := ac.WriteAsync(ctx, events)
    m.latencyObserver.Observe(time.Since(start).Seconds())
    if err != nil {
        m.recordResult(events, err)
    }
    // Note: success path doesn't increment task_events_total here for async; that
    // happens when callback fires. AsyncConnector callback wiring is out of scope.
    return err
}

func (m *MetricsSink) recordResult(events []*event.ChangeEvent, err error) {
    if err != nil {
        errType := metrics.ClassifyError(err)
        if errType == metrics.ErrorTypeRetriable {
            m.errorCounterRetriable.Inc()
        } else {
            m.errorCounterNonRetriable.Inc()
        }
        // result=failed counts only at the final-failure call site (pipeline's
        // utils.Retry wrapper). The decorator should NOT inc failed counter
        // here — it might be a retriable error and pipeline will retry.
        //
        // However, since retry architecture unification is out of scope, and
        // current sinks may have internal retries already, we ACCEPT that
        // result=failed under-counts (only counts errors that escape sink
        // entirely). This is documented in §13 risks.
        return
    }
    for _, e := range events {
        if c, ok := m.successCounters[e.Type]; ok {
            c.Inc()
        }
        // Unknown event types are silently dropped — defensive against future enum additions.
    }
    var totalBytes int
    for _, e := range events {
        totalBytes += e.Size()
    }
    m.bytesAdder.Add(float64(totalBytes))
}

// StatsProvider 透传
func (m *MetricsSink) Stats(ctx context.Context) connector.Stats {
    if sp, ok := m.inner.(connector.StatsProvider); ok {
        return sp.Stats(ctx)
    }
    return connector.Stats{}
}

func (m *MetricsSink) SupportsStats() bool {
    _, ok := m.inner.(connector.StatsProvider)
    return ok
}

// 转发其余 sink.Connector 方法到 inner（Name/Initialize/Start/Stop/Status/...）
```

**关键设计点**：
1. **Connector + BatchConnector + AsyncConnector 三接口同时实现**，调用方做类型断言时仍能发现可选能力
2. **label vec 预缓存**：构造期一次性 `WithLabelValues`，热路径直接 `Counter.Inc()`，避免 hashmap 查找
3. **未知事件类型容忍**：未来 `EventType` 枚举扩展时不需要立即改装饰器；通过测试覆盖确保已知 7 种类型在 precache 中
4. **result=failed 取舍**：当前 sink 内部可能有重试，装饰器层无法判断是否"最终失败"。本任务**不解决这个问题**，文档明确接受 `result=failed` 在某些场景下欠计——精准化等"重试统一"任务

### 4.4 错误分类工具（`pkg/metrics/classify.go`）

```go
// pkg/metrics/classify.go
package metrics

import "github.com/UFOXD/datastream/pkg/errors"

type ErrorType string

const (
    ErrorTypeRetriable    ErrorType = "retriable"
    ErrorTypeNonRetriable ErrorType = "non_retriable"
)

// ClassifyError maps an error to a metric label value.
// Uses pkg/errors.IsRetryableError (NOT pkg/utils.IsRetryableError;
// the two have different semantics and will be merged in a follow-up task).
func ClassifyError(err error) ErrorType {
    if err == nil {
        return ErrorTypeRetriable // defensive; shouldn't be called with nil
    }
    if errors.IsRetryableError(err) {
        return ErrorTypeRetriable
    }
    return ErrorTypeNonRetriable
}
```

### 4.5 StatsCollector（`pkg/metrics/collector_runtime.go`）

```go
// pkg/metrics/collector_runtime.go
package metrics

import (
    "context"
    "math"
    "sync"
    "time"

    "github.com/UFOXD/datastream/internal/connector"
    "github.com/pingcap/log"
    "go.uber.org/zap"
)

type StatsCollector struct {
    cluster   string
    interval  time.Duration
    timeout   time.Duration
    mu        sync.RWMutex
    providers map[string]providerEntry
}

type providerEntry struct {
    provider connector.StatsProvider
    role     string // "source" | "sink"
    cType    string
    taskID   string
}

func NewStatsCollector(cluster string, interval, timeout time.Duration) *StatsCollector

func (c *StatsCollector) Register(key, role, cType, taskID string, p connector.StatsProvider) {
    if p == nil { return }
    c.mu.Lock()
    c.providers[key] = providerEntry{p, role, cType, taskID}
    c.mu.Unlock()
}

func (c *StatsCollector) Unregister(key string) {
    c.mu.Lock()
    delete(c.providers, key)
    c.mu.Unlock()
    // NOTE: do NOT call DeleteLabelValues; rely on Prometheus 3min staleness.
}

func (c *StatsCollector) Run(ctx context.Context) {
    ticker := time.NewTicker(c.interval)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            c.tick(ctx)
        }
    }
}

func (c *StatsCollector) tick(parentCtx context.Context) {
    c.mu.RLock()
    snapshot := make([]providerEntry, 0, len(c.providers))
    keys := make([]string, 0, len(c.providers))
    for k, e := range c.providers {
        snapshot = append(snapshot, e)
        keys = append(keys, k)
    }
    c.mu.RUnlock()

    var wg sync.WaitGroup
    for i := range snapshot {
        wg.Add(1)
        go func(key string, e providerEntry) {
            defer wg.Done()
            defer func() {
                if r := recover(); r != nil {
                    log.Error("stats provider panicked",
                        zap.String("key", key), zap.Any("panic", r))
                }
            }()
            ctx, cancel := context.WithTimeout(parentCtx, c.timeout)
            defer cancel()
            stats := e.provider.Stats(ctx)
            c.emit(e, stats)
        }(keys[i], snapshot[i])
    }
    wg.Wait()
}

func (c *StatsCollector) emit(e providerEntry, s connector.Stats) {
    // Queue
    if e.role == "sink" { // queue lives at sink batch buffer typically
        PipelineQueueSize.WithLabelValues(c.cluster, e.taskID, "sink").Set(float64(s.QueueSize))
        PipelineQueueCapacity.WithLabelValues(c.cluster, e.taskID, "sink").Set(float64(s.QueueCapacity))
    } else {
        PipelineQueueSize.WithLabelValues(c.cluster, e.taskID, "source").Set(float64(s.QueueSize))
        PipelineQueueCapacity.WithLabelValues(c.cluster, e.taskID, "source").Set(float64(s.QueueCapacity))
    }
    // Lag — only for source role
    if e.role == "source" {
        if !math.IsNaN(s.LagSeconds) {
            lag := s.LagSeconds
            if lag < 0 {
                lag = 0
            }
            SourceLagSeconds.WithLabelValues(c.cluster, e.taskID, e.cType).Set(lag)
        }
        if !s.LastEventTime.IsZero() {
            SourceLastEventSeconds.WithLabelValues(c.cluster, e.taskID, e.cType).Set(float64(s.LastEventTime.Unix()))
        }
        SourceSnapshotProgress.WithLabelValues(c.cluster, e.taskID).Set(s.SnapshotProgress)
        SnapshotTablesTotal.WithLabelValues(c.cluster, e.taskID).Set(float64(s.SnapshotTotalTables))
        SnapshotTablesRemaining.WithLabelValues(c.cluster, e.taskID).Set(float64(s.SnapshotRemainingTables))
    }
    // Connection
    connected := 0.0
    if s.Connected {
        connected = 1.0
    }
    ConnectorConnected.WithLabelValues(c.cluster, e.taskID, e.role, e.cType).Set(connected)
}
```

**关键不变量**：
- 每 tick 内 N 个 provider 在 N 个 goroutine 中并发调用 `Stats(ctx)`，单次 1s 超时
- panic recover 在 per-goroutine 层级，单 provider 失败不影响其他
- `Stats(ctx)` 实现仍应非阻塞——ctx 是兜底
- snapshot copy 在锁外执行，避免长时间持锁

### 4.6 Registry 注入（`pkg/metrics/metrics.go` 重构）

```go
// pkg/metrics/metrics.go
package metrics

import "github.com/prometheus/client_golang/prometheus"

const Namespace = "datastream" // Prometheus metric name prefix

// All metrics are declared as package-level vars but NOT created at init.
// Use MustRegisterAll(r) to create and register them with a Registerer.
var (
    TaskTotal        *prometheus.GaugeVec
    TaskState        *prometheus.GaugeVec   // new in this task
    TaskEventsTotal  *prometheus.CounterVec
    TaskEventsBytes  *prometheus.CounterVec
    TaskLatencySeconds *prometheus.HistogramVec
    SourcePosition   *prometheus.GaugeVec
    SourceSnapshotProgress *prometheus.GaugeVec
    SourceLagSeconds       *prometheus.GaugeVec  // new
    SourceLastEventSeconds *prometheus.GaugeVec  // new
    SnapshotTablesTotal     *prometheus.GaugeVec // new
    SnapshotTablesRemaining *prometheus.GaugeVec // new
    SinkWriteLatency *prometheus.HistogramVec
    SinkWriteErrors  *prometheus.CounterVec
    PipelineQueueSize     *prometheus.GaugeVec
    PipelineQueueCapacity *prometheus.GaugeVec  // new
    PipelineProcessTime   *prometheus.HistogramVec
    ConnectorConnected    *prometheus.GaugeVec  // new
    NodeStatus     *prometheus.GaugeVec
    LeaderStatus   prometheus.Gauge
    LeaderChanges  prometheus.Counter

    currentRegistry prometheus.Registerer
)

// MustRegisterAll creates and registers all DataStream metrics with r.
// Must be called once at startup (e.g., in init() with DefaultRegisterer, or
// in tests with a fresh Registry). Calling twice on the same Registerer will panic.
func MustRegisterAll(r prometheus.Registerer) {
    currentRegistry = r
    TaskTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
        Namespace: Namespace, Name: "task_total", Help: "..."},
        []string{"cluster", "status"})
    TaskState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
        Namespace: Namespace, Name: "task_state", Help: "Per-task current state (0/1)"},
        []string{"cluster", "task", "state"})
    TaskEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
        Namespace: Namespace, Name: "task_events_total", Help: "..."},
        []string{"cluster", "task", "type", "result"})
    // ... rest
    r.MustRegister(TaskTotal, TaskState, TaskEventsTotal, /* ... */)
}

// ResetForTest unregisters all metrics from the current Registry and clears
// package vars. Intended for tests that want to rebuild with a new Registry.
func ResetForTest() {
    if currentRegistry == nil {
        return
    }
    if u, ok := currentRegistry.(prometheus.Unregisterer); ok {
        u.Unregister(TaskTotal)
        u.Unregister(TaskState)
        u.Unregister(TaskEventsTotal)
        // ... rest
    }
    TaskTotal = nil
    TaskState = nil
    TaskEventsTotal = nil
    // ... rest
    currentRegistry = nil
}

func init() {
    MustRegisterAll(prometheus.DefaultRegisterer)
}
```

测试模式：
```go
func TestXxx(t *testing.T) {
    metrics.ResetForTest()
    r := prometheus.NewRegistry()
    metrics.MustRegisterAll(r)
    t.Cleanup(func() {
        metrics.ResetForTest()
        metrics.MustRegisterAll(prometheus.DefaultRegisterer)
    })
    // ...
}
```

`/metrics` endpoint：
```go
// internal/api/server.go
func (s *Server) handleMetrics() http.Handler {
    return promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{})
}
```

注：endpoint 用 `DefaultGatherer`（生产场景始终是 `DefaultRegisterer`）；测试自有独立 Registry 不通过 endpoint 验证而通过 `r.Gather()`。

---

## 5. 指标定义增量

### 5.1 现有指标修改

| 指标 | 修改 |
|------|------|
| 所有指标的 `namespace` label | **重命名为 `cluster`**（避免与 Prometheus namespace 冲突） |
| `TaskEventsTotal` | label 由 `[cluster, task, type]` 改为 `[cluster, task, type, result]`；`type` 取值 `insert/update/delete/truncate/ddl/heartbeat/tombstone`；`result` 取值 `success/failed` |
| `SinkWriteErrors` | `error_type` 取值规范为 `retriable/non_retriable` |
| `TaskTotal` (Gauge) | 由 `Inc` 改为 `SetTaskTotal(cluster, state, 1.0)` 状态机表达 |

### 5.2 新增指标（7 个）

| 指标 | 类型 | label | 用途 |
|------|------|-------|------|
| `datastream_task_state` | GaugeVec | `cluster, task, state` | 单 task 当前状态 (0/1)，state ∈ {running, stopped, paused, error} |
| `datastream_source_lag_seconds` | GaugeVec | `cluster, task, source` | CDC 延迟核心指标 |
| `datastream_source_last_event_seconds` | GaugeVec | `cluster, task, source` | 最近事件 Unix 秒 |
| `datastream_pipeline_queue_capacity` | GaugeVec | `cluster, task, stage` | 队列容量 |
| `datastream_connector_connected` | GaugeVec | `cluster, task, role, type` | 0/1 连接健康 |
| `datastream_snapshot_tables_total` | GaugeVec | `cluster, task` | 待快照表总数 |
| `datastream_snapshot_tables_remaining` | GaugeVec | `cluster, task` | 剩余未快照表数 |

注：相比 v2，**砍掉了 3 个指标**：
- `source_read_errors_total` — 移除，由 Pipeline 消费 `source.Errors()` channel 时直接记录已存在的 metric 即可（不新增）
- `events_filtered_total` — 推迟到 filter 集成任务
- `connector_retries_total` — 推迟到重试统一任务

### 5.3 修复 Pipeline 状态机的 metric 写法

```go
// internal/pipeline/pipeline.go
const (
    stateRunning = "running"
    stateStopped = "stopped"
    statePaused  = "paused"
    stateError   = "error"
)

// updateState 集中处理状态切换，同时写两个指标：
//   1. task_state{cluster,task,state}: 当前 state 置 1，其他 state 置 0
//   2. task_total{cluster,status}: 集群级状态分布（gauge 增减）
func (p *Pipeline) updateState(newState string) {
    p.mu.Lock()
    oldState := p.status.State
    p.status.State = State(newState)
    p.mu.Unlock()

    // task-level state gauge
    for _, s := range []string{stateRunning, stateStopped, statePaused, stateError} {
        v := 0.0
        if s == newState {
            v = 1.0
        }
        metrics.TaskState.WithLabelValues(p.cluster, p.id, s).Set(v)
    }

    // cluster-level distribution
    if string(oldState) != "" {
        metrics.TaskTotal.WithLabelValues(p.cluster, string(oldState)).Dec()
    }
    metrics.TaskTotal.WithLabelValues(p.cluster, newState).Inc()
}
```

替换 `pipeline.go:198/234/342/345` 的 4 处 panic 调用。注意：
- `342/345` 原本是 `metrics.TaskEventsTotal.WithLabelValues(p.id, "failed"/"written").Inc()`——这是错把 task ID 当 namespace label，且把 status 当 type label。**清理掉**，事件计数由本任务的 Pipeline 消费点埋点和 Sink 装饰器统一覆盖

### 5.4 Pipeline 消费点埋点

```go
// internal/pipeline/pipeline.go (in Pipeline struct, new fields)
type Pipeline struct {
    // ... existing
    cluster string

    // pre-cached label vectors for hot path
    successCounters map[event.EventType]prometheus.Counter
    bytesAdder      prometheus.Counter
    lagGauge        prometheus.Gauge // per-source lag; nil if multi-source
    lastEventGauge  prometheus.Gauge
}

func (p *Pipeline) precacheLabels() {
    sourceType := p.config.Source.Type
    p.successCounters = make(map[event.EventType]prometheus.Counter, 7)
    for _, t := range []event.EventType{
        event.EventTypeInsert, event.EventTypeUpdate, event.EventTypeDelete,
        event.EventTypeTruncate, event.EventTypeDDL,
        event.EventTypeHeartbeat, event.EventTypeTombstone,
    } {
        p.successCounters[t] = metrics.TaskEventsTotal.WithLabelValues(p.cluster, p.id, string(t), "success")
    }
    p.bytesAdder = metrics.TaskEventsBytes.WithLabelValues(p.cluster, p.id)
    p.lagGauge = metrics.SourceLagSeconds.WithLabelValues(p.cluster, p.id, sourceType)
    p.lastEventGauge = metrics.SourceLastEventSeconds.WithLabelValues(p.cluster, p.id, sourceType)
}

// In Pipeline.run loop:
case e, ok := <-p.source.Events():
    if !ok { return }
    if c, ok := p.successCounters[e.Type]; ok {
        c.Inc()
    }
    p.bytesAdder.Add(float64(e.Size()))
    if !e.Timestamp.IsZero() {
        lag := time.Since(e.Timestamp).Seconds()
        if lag < 0 {
            lag = 0
        }
        p.lagGauge.Set(lag)
        p.lastEventGauge.Set(float64(e.Timestamp.Unix()))
    }
    p.processEvent(ctx, e)
```

注意：`success` 计数实际在两处都可以打：Pipeline 消费点（事件流入）或 Sink 装饰器（写成功后）。这里采用 **消费点打**——更接近"已处理"的语义，且 Sink 失败时由装饰器另计 `failed`，**两条路径不重叠**（消费点不打 failed）。

---

## 6. 装配与生命周期

### 6.1 `/metrics` Endpoint 修复

```go
// internal/api/server.go
import "github.com/prometheus/client_golang/prometheus/promhttp"

func (s *Server) handleMetrics() http.Handler {
    return promhttp.Handler()
}
```

### 6.2 配置入口

实际文件路径是 `pkg/config/config.go`（不是 `internal/app/config.go`）。修改：

```go
// pkg/config/config.go
type Config struct {
    // 现有字段...
    Cluster string        `toml:"cluster"`
    Metrics MetricsConfig `toml:"metrics"`
}

type MetricsConfig struct {
    Enabled        bool          `toml:"enabled"`         // default true
    ScrapeInterval time.Duration `toml:"scrape_interval"` // default 5s
    StatsTimeout   time.Duration `toml:"stats_timeout"`   // default 1s
}

// cmd/datastream/main.go
rootCmd.PersistentFlags().StringVar(&cfg.Cluster, "cluster",
    getEnv("DATASTREAM_CLUSTER", "default"),
    "Metric cluster label value")
```

### 6.3 任务装配链路

实际 API：`TaskManager.Create(ctx, id, name, *Config)` 返回 `*Task`。`Task` 持有 `*Pipeline`。

```go
// internal/pipeline/task.go (modifications)
type Task struct {
    // existing...
    cluster string
    pipeline *Pipeline
}

// internal/pipeline/task_manager.go (modifications)
type TaskManager struct {
    // existing...
    cluster        string
    statsCollector *metrics.StatsCollector  // nil if metrics disabled
}

func (m *TaskManager) Create(ctx context.Context, id, name string, cfg *Config) (*Task, error) {
    cfg.ID = id
    cfg.Name = name

    src, err := source.Create(cfg.Source.Type, cfg.Source)
    if err != nil { return nil, err }

    wrappedSinks := make([]sink.Connector, 0, len(cfg.Sinks))
    for _, sCfg := range cfg.Sinks {
        raw, err := sink.Create(sCfg.Type, sCfg)
        if err != nil { return nil, err }
        if m.statsCollector != nil {
            raw = sinkdec.WrapSink(raw, m.cluster, id, sCfg.Type)
        }
        wrappedSinks = append(wrappedSinks, raw)
    }

    pipe := pipeline.New(cfg)
    pipe.SetSource(src)
    pipe.SetCluster(m.cluster) // new method
    for _, s := range wrappedSinks {
        pipe.AddSink(s)
    }
    pipe.precacheLabels()

    task := &Task{ID: id, Name: name, pipeline: pipe, cluster: m.cluster}
    m.tasks[id] = task

    if m.statsCollector != nil {
        // Register source if it implements StatsProvider
        if sp, ok := src.(connector.StatsProvider); ok {
            m.statsCollector.Register(id+":source", "source", cfg.Source.Type, id, sp)
        }
        // Register each wrapped sink — wrapper transparently forwards StatsProvider
        for i, s := range wrappedSinks {
            if sp, ok := s.(connector.StatsProvider); ok {
                m.statsCollector.Register(
                    id+":sink:"+strconv.Itoa(i),
                    "sink", cfg.Sinks[i].Type, id, sp)
            }
        }
        // Startup log: which connectors implement StatsProvider
        log.Info("task metrics registered",
            zap.String("task", id),
            zap.Bool("source_stats", asProviderOK(src)),
            zap.Int("sink_count", len(wrappedSinks)),
            zap.Ints("sinks_with_stats", sinksWithStats(wrappedSinks)),
        )
    }

    return task, nil
}

func (m *TaskManager) Delete(ctx context.Context, id string) error {
    if m.statsCollector != nil {
        m.statsCollector.Unregister(id + ":source")
        // unregister all sink slots (best-effort: try indices 0..N)
        for i := 0; i < 16; i++ { // safe upper bound
            m.statsCollector.Unregister(id + ":sink:" + strconv.Itoa(i))
        }
    }
    // existing delete logic...
}
```

### 6.4 Application 生命周期

实际 struct 是 `Application`（不是 `App`）：

```go
// internal/app/app.go (modifications)
type Application struct {
    config         *config.Config
    apiServer      *api.Server
    coordinator    pipeline.Coordinator
    taskManager    *pipeline.TaskManager
    statsCollector *metrics.StatsCollector  // new
    // existing...
}

func (a *Application) Start(ctx context.Context) error {
    // existing init...

    if a.config.Metrics.Enabled {
        interval := a.config.Metrics.ScrapeInterval
        if interval == 0 { interval = 5 * time.Second }
        timeout := a.config.Metrics.StatsTimeout
        if timeout == 0 { timeout = time.Second }

        a.statsCollector = metrics.NewStatsCollector(a.config.Cluster, interval, timeout)
        a.taskManager.SetStatsCollector(a.statsCollector) // new method
        go a.statsCollector.Run(ctx)
    }

    // ... rest
}
```

启动/退出顺序：`Stop` 先停 TaskManager（触发每个任务 Delete → Unregister），再 cancel ctx 让 collector 自然退出。

---

## 7. Grafana Dashboard

`deployments/grafana/datastream-dashboard.json` 4 个 row：

| Row | 关键 panel | 主要查询 |
|-----|-----------|---------|
| Throughput | Events/s by task & result | `sum by (task, result) (rate(datastream_task_events_total[1m]))` |
| Latency | p50/p95/p99 sink write | `histogram_quantile(0.99, sum by (le, task) (rate(datastream_sink_write_latency_seconds_bucket[1m])))` |
| Lag & Health | Source lag, last event age, connected, task state | `datastream_source_lag_seconds`、`time() - datastream_source_last_event_seconds`、`datastream_connector_connected`、`datastream_task_state` |
| Errors & Queue | Error rate by error_type, queue usage | `sum by (error_type) (rate(datastream_sink_write_errors_total[1m]))`、`datastream_pipeline_queue_size / datastream_pipeline_queue_capacity` |

---

## 8. 测试策略

| 层级 | case | 文件 |
|------|------|------|
| 单元 | `ChangeEvent.Size()` 在各种字段下返回合理估算 | `pkg/event/change_event_test.go`（补 case） |
| 单元 | `ClassifyError` 对各种错误分类 | `pkg/metrics/classify_test.go` |
| 单元 | `MustRegisterAll` + `ResetForTest` 配合，重复注册不 panic | `pkg/metrics/registry_test.go` |
| 单元 | `MetricsSink.Write` 成功/失败下指标 +1；预缓存 label 命中 | `internal/sink/decorator/sink_test.go` |
| 单元 | `MetricsSink.WriteBatch` 在 inner 实现 BatchConnector 时调用 inner.WriteBatch | 同上 |
| 单元 | `MetricsSink.WriteBatch` 在 inner 不实现时回退到 Write | 同上 |
| 单元 | `MetricsSink.WriteAsync` 同理 | 同上 |
| 单元 | `MetricsSink.Stats(ctx)` 透传；`SupportsStats()` 反映 inner 能力 | 同上 |
| 单元 | `MetricsSink` 处理未知 EventType 时不 panic | 同上 |
| 单元 | `StatsCollector.Register/Unregister` 并发安全（`-race`） | `pkg/metrics/collector_runtime_test.go` |
| 单元 | `StatsCollector` panic 恢复：mock provider panic 不影响其他 | 同上 |
| 单元 | `StatsCollector` 超时：mock provider sleep 2s，1s 超时后单次 tick 继续 | 同上 |
| 单元 | `StatsCollector` 同 key 重新 Register 覆盖旧 provider | 同上 |
| 单元 | `StatsCollector` 多 provider 并发：N 个 provider，单次 tick 总时间 ≈ max(各 Stats 耗时) | 同上 |
| 单元 | `LagSeconds = NaN` 不写 gauge；`LastEventTime = zero` 不写；负值 clamp 到 0 | 同上 |
| 单元 | Pipeline 状态切换：`task_state` 和 `task_total` 同时更新且一致 | `internal/pipeline/pipeline_test.go` |
| 单元 | Pipeline 消费点不重复打 success（与 Sink 装饰器仅 Sink 打 success/failed 互斥） | 同上 |
| 集成 | `/metrics` endpoint 返回 Prometheus 文本且包含 `datastream_*` | `internal/api/server_test.go` |
| 集成 | 完整 MySQL→PostgreSQL 任务后 metric 值符合预期 | `tests/integration/metrics_test.go`（新增） |
| 集成 | 使用独立 Registry 隔离，多次 setup 可重复运行 | 同上 |
| 集成 | 任务删除后 5min 内 gauge 序列消失 | 同上 |
| 回归 | 修复后 pipeline.go 状态切换不再 panic | 已有 test 补 case |

---

## 9. 落地计划（4 个 commit）

| Stage | 内容 | 验收 |
|-------|------|------|
| 1 | **基础修复 + Registry 重构**：`pkg/metrics/metrics.go` 重构为 `MustRegisterAll` + `ResetForTest`；label `namespace` → `cluster` 全文替换；`/metrics` 接 `promhttp.Handler`；新增 `ChangeEvent.Size()`；Pipeline 状态机 4 处 panic 修复（updateState） | 现有测试通过；curl /metrics 看到 `datastream_*` 真实指标 |
| 2 | **公共组件**：新建 `internal/connector/stats.go`、`pkg/metrics/classify.go`、`pkg/metrics/collector_runtime.go`、7 个新指标定义（含 task_state） | 单测 + race 通过 |
| 3 | **Sink 装饰器 + Pipeline 消费点埋点**：新建 `internal/sink/decorator/`（支持 Connector + BatchConnector + AsyncConnector）；TaskManager 装配时 wrap sink；Pipeline 消费点 precacheLabels + 埋点 | 单测 + race + 集成测试通过 |
| 4 | **连接器 Stats 实现 + 文档**：12 个 connector 实现 `StatsProvider`；CLI `--cluster` flag；TaskManager 启动日志报告 stats_provider；同步 4 份设计文档；新增 Grafana dashboard JSON；新增 `docs/operations/metrics.md` 运维文档 | docker-compose 跑通后 /metrics 看到全套指标；文档评审 |

---

## 10. 回滚

| Stage | 回滚方式 |
|-------|---------|
| 1 | endpoint 可还原；panic 修复 / label 重命名 / Registry 重构是基础设施改造，回滚意味着回到坏状态，不建议回滚 |
| 2 | 删除 `internal/connector/`、`pkg/metrics/classify.go`、`pkg/metrics/collector_runtime.go`、`pkg/event/change_event.go` 中的 `Size()` |
| 3 | 删除 `internal/sink/decorator/`；TaskManager 不 wrap sink；Pipeline 移除 precacheLabels 调用 |
| 4 | 连接器 `Stats()` 删除；文档 revert |

`cfg.Metrics.Enabled = false` 是运行时兜底：关闭后 collector 不启动、TaskManager 不 wrap sink。

---

## 11. 设计文档同步

| 文件 | 修改内容 |
|------|---------|
| `docs/design/core-design.md` §4 | label 名 `namespace` → `cluster`；追加新增 7 个指标定义；`error_type` 取值约束；`TaskEventsTotal` 加 `result` label；Pipeline 状态机 metric 写法；`MustRegisterAll` 模式说明 |
| `docs/design/connector-design.md` | 新增「StatsProvider 可选接口」小节，定义 `internal/connector/stats.go` 的 Stats 结构体；约定 Position 字段在 MySQL/MariaDB 下需支持 GTID 与 File-Pos 两种格式；约定 `Stats(ctx)` 必须非阻塞 |
| `docs/design/pipeline-design.md` | 追加 §装配流程：Pipeline 消费点埋点 + Sink 装饰器装配；说明 success/failed 取数路径互斥 |
| `docs/design/event-model-design.md` | 追加 `ChangeEvent.Size()` 方法说明（估算字节数用途） |
| `docs/design/api-cli-design.md` | `--cluster` CLI flag 说明 |

---

## 12. 非目标（YAGNI）

明确**不在本任务范围**：
- 多 metric 后端（OpenTelemetry / StatsD）
- Push gateway
- 自定义指标动态注册
- 历史指标存储
- AlertManager 规则
- `queue_bytes` 系列
- `position_numeric` 数值化位点
- 任务级 Metrics 开关
- Unregister 时主动 `DeleteLabelValues`
- **重试架构统一**（别出独立任务）
- **`pkg/utils.IsRetryableError` 与 `pkg/errors.IsRetryableError` 合并**（别出）
- **`connector_retries_total` 指标**（依赖重试统一）
- **`events_filtered_total` 指标**（依赖 filter pipeline 集成）
- **`last_error_message` API 字段 / `TaskState.LastError` 扩展**（别出）
- **`source_read_errors_total` 指标**（用已有 SourcePosition / Errors channel 已足够）

---

## 13. 风险与权衡

| 风险 | 影响 | 缓解 |
|------|------|------|
| `result=failed` 在 Sink 内部重试场景下欠计 | 失败率指标偏低 | 文档明示；精确计数等"重试统一"任务做完 |
| `result=success` 在 Pipeline 消费点打，事件未实际写入 sink 时已计成功 | "处理成功" ≠ "写入成功"，监控含义需要文档明示 | docs/operations/metrics.md 中明确两个指标的语义边界 |
| `WriteAsync` 路径只能记录 enqueue 时间不能记录最终结果 | Async sink 监控不全 | 文档明示；AsyncConnector 回调集成等独立任务 |
| 未知 EventType 在装饰器/Pipeline 中被静默丢弃 | 新增枚举值时计数缺失 | 测试覆盖 7 种已知 type；新增 enum 时同步更新 precache 列表 |
| `Stats(ctx)` 实现非阻塞约束依赖连接器开发自律 | 慢实现拖慢 collector tick | ctx 1s timeout 兜底；并发 goroutine 隔离单 provider 影响 |
| `Stats(ctx)` 并发调用对数据库的轻量查询累加可能扰动 | 极端情况 24 个连接同时探活 | 实现规约要求 Stats 用进程内缓存，不直接发数据库探活 |
| LagSeconds 依赖 NTP 同步 | 时钟漂移时 lag 失真 | 负值 clamp 0；文档明示依赖 NTP |
| `MustRegisterAll` 重复调用 panic | 测试 setup 失误 | `ResetForTest()` 配合；test helper 函数封装 |
| Pipeline 消费点埋点与 Sink 装饰器埋点 success/failed 责任划分 | 重复或漏计 | 严格约定：消费点只打 success（事件流入）；装饰器只打 failed（写入失败）；测试断言无双计 |
| ChangeEvent.Size() 估算与实际序列化大小偏差 | bytes 指标趋势准但绝对值不准 | 文档明示估算性质；如未来需要精确改为序列化后计算 |
| 多 sink 任务下 task_events_total 计数对单 sink 失败语义模糊 | 1 个 sink 失败、其他成功时 result=failed 计数取决于装饰器位置 | Pipeline 调用每个 sink.Write 独立，装饰器各自统计；事件层面 result=failed 实际意义为"至少一个 sink 失败" |
