# Core Layer 设计

Core Layer 是基础层，为其他所有层提供通用能力。

---

## 1. 模块结构

```
pkg/
├── config/          # 配置管理
│   ├── config.go    # 配置定义与加载
│   ├── validator.go # 配置验证
│   └── loader.go    # 配置加载器（文件/环境变量）
├── logutil/         # 日志工具
│   ├── log.go       # 日志初始化
│   └── field.go     # 日志字段辅助函数
├── metrics/         # 指标收集
│   ├── metrics.go   # 指标定义
│   └── collector.go # 指标收集器
├── errors/          # 错误定义
│   ├── errors.go    # 错误定义
│   └── helper.go    # 错误辅助函数
├── version/         # 版本信息
│   └── version.go
└── utils/           # 通用工具
    ├── retry.go     # 重试工具
    ├── pool.go      # 对象池
    ├── hash.go      # Hash 工具
    └── string.go    # 字符串工具
```

---

## 2. 配置管理

### 2.1 配置定义

```go
package config

import (
    "fmt"
    "os"
    "reflect"
    "strings"
    
    "github.com/pelletier/go-toml"
    "github.com/pingcap/errors"
)

// Config 全局配置
type Config struct {
    // 服务配置
    Server   ServerConfig   `toml:"server" json:"server"`
    
    // 日志配置
    Log      LogConfig      `toml:"log" json:"log"`
    
    // 协调器配置
    Coordinator CoordinatorConfig `toml:"coordinator" json:"coordinator"`
    
    // 安全配置
    Security SecurityConfig `toml:"security" json:"security"`
}

// ServerConfig 服务配置
type ServerConfig struct {
    // 服务地址
    Addr        string `toml:"addr" json:"addr"`
    
    // API 地址
    APIAddr     string `toml:"api-addr" json:"api-addr"`
    
    // 广播地址（集群内通信）
    AdvertiseAddr string `toml:"advertise-addr" json:"advertise-addr"`
    
    // 数据目录
    DataDir     string `toml:"data-dir" json:"data-dir"`
    
    // GC TTL（任务进度保留时间）
    GCTTL       int64  `toml:"gc-ttl" json:"gc-ttl"`
}

// LogConfig 日志配置
type LogConfig struct {
    // 日志级别：debug, info, warn, error
    Level       string `toml:"level" json:"level"`
    
    // 日志文件路径
    File        string `toml:"file" json:"file"`
    
    // 单文件最大 MB
    MaxSize     int    `toml:"max-size" json:"max-size"`
    
    // 最大保留天数
    MaxDays     int    `toml:"max-days" json:"max-days"`
    
    // 最大备份文件数
    MaxBackups  int    `toml:"max-backups" json:"max-backups"`
}

// CoordinatorConfig 协调器配置
type CoordinatorConfig struct {
    // 后端类型：etcd, consul, memory（单机测试用）
    Backend     string `toml:"backend" json:"backend"`
    
    // 后端地址
    Endpoints   []string `toml:"endpoints" json:"endpoints"`
    
    // 会话 TTL（秒）
    SessionTTL  int    `toml:"session-ttl" json:"session-ttl"`
    
    // 竞选超时（毫秒）
    ElectionTimeout int `toml:"election-timeout" json:"election-timeout"`
}

// SecurityConfig 安全配置
type SecurityConfig struct {
    // SSL/TLS 配置
    SSLCa       string `toml:"ssl-ca" json:"ssl-ca"`
    SSLCert     string `toml:"ssl-cert" json:"ssl-cert"`
    SSLKey      string `toml:"ssl-key" json:"ssl-key"`
    
    // 是否允许不安全连接
    Insecure    bool   `toml:"insecure" json:"insecure"`
}
```

### 2.2 配置加载

```go
// 默认值
const (
    defaultAddr          = ":8300"
    defaultAPIAddr       = ":8301"
    defaultLogLevel      = "info"
    defaultLogMaxSize    = 512  // MB
    defaultLogMaxDays    = 7
    defaultDataDir       = "./data"
    defaultGCTTL         = 86400 // 24 小时
    defaultCoordinatorBackend = "etcd"
    defaultSessionTTL    = 10
    defaultElectionTimeout = 5000
)

// Adjust 调整默认值
func (c *Config) Adjust() {
    if c.Server.Addr == "" {
        c.Server.Addr = defaultAddr
    }
    if c.Server.APIAddr == "" {
        c.Server.APIAddr = defaultAPIAddr
    }
    if c.Server.DataDir == "" {
        c.Server.DataDir = defaultDataDir
    }
    if c.Server.GCTTL == 0 {
        c.Server.GCTTL = defaultGCTTL
    }
    
    if c.Log.Level == "" {
        c.Log.Level = defaultLogLevel
    }
    if c.Log.Level == "warning" {
        c.Log.Level = "warn"
    }
    if c.Log.MaxSize == 0 {
        c.Log.MaxSize = defaultLogMaxSize
    }
    if c.Log.MaxDays == 0 {
        c.Log.MaxDays = defaultLogMaxDays
    }
    
    if c.Coordinator.Backend == "" {
        c.Coordinator.Backend = defaultCoordinatorBackend
    }
    if c.Coordinator.SessionTTL == 0 {
        c.Coordinator.SessionTTL = defaultSessionTTL
    }
    if c.Coordinator.ElectionTimeout == 0 {
        c.Coordinator.ElectionTimeout = defaultElectionTimeout
    }
}

// Validate 验证配置
func (c *Config) Validate() error {
    if c.Server.Addr == "" {
        return errors.New("server addr is required")
    }
    if c.Server.DataDir == "" {
        return errors.New("data-dir is required")
    }
    if c.Coordinator.Backend != "memory" && len(c.Coordinator.Endpoints) == 0 {
        return errors.New("coordinator endpoints is required")
    }
    return nil
}

// LoadFromFile 从文件加载配置
func LoadFromFile(path string) (*Config, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, errors.Trace(err)
    }
    
    cfg := &Config{}
    if err := toml.Unmarshal(data, cfg); err != nil {
        return nil, errors.Trace(err)
    }
    
    cfg.Adjust()
    if err := cfg.Validate(); err != nil {
        return nil, err
    }
    
    return cfg, nil
}

// LoadFromEnv 从环境变量加载配置（覆盖文件配置）
func (c *Config) LoadFromEnv() error {
    // 支持环境变量覆盖，格式：DATASTREAM_<SECTION>_<KEY>
    // 例如：DATASTREAM_SERVER_ADDR=:8302
    envPrefix := "DATASTREAM_"
    
    envMap := make(map[string]string)
    for _, env := range os.Environ() {
        if !strings.HasPrefix(env, envPrefix) {
            continue
        }
        parts := strings.SplitN(env, "=", 2)
        if len(parts) != 2 {
            continue
        }
        key := strings.TrimPrefix(parts[0], envPrefix)
        envMap[strings.ToLower(key)] = parts[1]
    }
    
    // 使用反射设置值
    return setConfigFromEnv(c, envMap)
}
```

---

## 3. 日志工具

```go
package logutil

import (
    "context"
    "os"
    "strings"
    
    "github.com/pingcap/log"
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
)

// InitLogger 初始化日志
func InitLogger(cfg *LogConfig) error {
    pclogConfig := &log.Config{
        Level: cfg.Level,
        File: log.FileLogConfig{
            Filename:   cfg.File,
            MaxSize:    cfg.MaxSize,
            MaxDays:    cfg.MaxDays,
            MaxBackups: cfg.MaxBackups,
        },
    }
    
    lg, properties, err := log.InitLogger(pclogConfig)
    if err != nil {
        return err
    }
    
    // 不记录 DPanic 级别以下的堆栈
    lg = lg.WithOptions(zap.AddStacktrace(zap.DPanicLevel))
    log.ReplaceGlobals(lg, properties)
    return nil
}

// WithComponent 返回带组件标识的 logger
func WithComponent(component string) *zap.Logger {
    return log.L().With(zap.String("component", component))
}

// ShortError 只记录错误消息，不含堆栈
func ShortError(err error) zap.Field {
    if err == nil {
        return zap.Skip()
    }
    return zap.String("error", err.Error())
}

// ErrorFilterContextCanceled 过滤 context.Canceled 错误
func ErrorFilterContextCanceled(logger *zap.Logger, msg string, fields ...zap.Field) {
    for _, field := range fields {
        if field.Type == zapcore.ErrorType {
            if err, ok := field.Interface.(error); ok {
                if errors.Cause(err) == context.Canceled {
                    return
                }
            }
        }
    }
    logger.Error(msg, fields...)
}

// 常用日志字段辅助函数
func StringField(key, val string) zap.Field { return zap.String(key, val) }
func IntField(key string, val int) zap.Field { return zap.Int(key, val) }
func Int64Field(key string, val int64) zap.Field { return zap.Int64(key, val) }
func Uint64Field(key string, val uint64) zap.Field { return zap.Uint64(key, val) }
func DurationField(key string, val time.Duration) zap.Field { return zap.Duration(key, val) }
func TimeField(key string, val time.Time) zap.Field { return zap.Timep(key, &val) }
func ErrorField(err error) zap.Field { return zap.Error(err) }
func ObjectField(key string, val interface{}) zap.Field { return zap.Reflect(key, val) }
```

---

## 4. 指标收集

```go
package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

// 命名空间
const namespace = "datastream"

// 指标定义
var (
    // === 任务指标 ===
    
    // 任务总数
    TaskTotal = promauto.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: namespace,
            Name:      "task_total",
            Help:      "Total number of tasks",
        },
        []string{"namespace", "status"},  // status: running, stopped, error
    )
    
    // 任务事件数
    TaskEventsTotal = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: namespace,
            Name:      "task_events_total",
            Help:      "Total number of events processed by task",
        },
        []string{"namespace", "task", "type"},  // type: insert, update, delete
    )
    
    // 任务事件大小
    TaskEventsBytes = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: namespace,
            Name:      "task_events_bytes",
            Help:      "Total bytes of events processed by task",
        },
        []string{"namespace", "task"},
    )
    
    // 任务延迟（秒）
    TaskLatencySeconds = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Namespace: namespace,
            Name:      "task_latency_seconds",
            Help:      "Latency of event processing in seconds",
            Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),  // 1ms ~ 16s
        },
        []string{"namespace", "task"},
    )
    
    // === Source 指标 ===
    
    // Source 位点
    SourcePosition = promauto.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: namespace,
            Name:      "source_position",
            Help:      "Current position of source",
        },
        []string{"namespace", "task", "source"},
    )
    
    // Source 快照进度
    SourceSnapshotProgress = promauto.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: namespace,
            Name:      "source_snapshot_progress",
            Help:      "Snapshot progress percentage (0-100)",
        },
        []string{"namespace", "task"},
    )
    
    // === Sink 指标 ===
    
    // Sink 写入延迟
    SinkWriteLatency = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Namespace: namespace,
            Name:      "sink_write_latency_seconds",
            Help:      "Latency of sink write operations",
            Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
        },
        []string{"namespace", "task", "sink"},
    )
    
    // Sink 写入错误
    SinkWriteErrors = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: namespace,
            Name:      "sink_write_errors_total",
            Help:      "Total number of sink write errors",
        },
        []string{"namespace", "task", "sink", "error_type"},
    )
    
    // === Pipeline 指标 ===
    
    // Pipeline 队列大小
    PipelineQueueSize = promauto.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: namespace,
            Name:      "pipeline_queue_size",
            Help:      "Current size of pipeline queue",
        },
        []string{"namespace", "task", "stage"},  // stage: filter, transform, router
    )
    
    // Pipeline 处理时间
    PipelineProcessTime = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Namespace: namespace,
            Name:      "pipeline_process_time_seconds",
            Help:      "Time spent in pipeline stage",
            Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),  // 0.1ms ~ 1.6s
        },
        []string{"namespace", "task", "stage"},
    )
    
    // === 集群指标 ===
    
    // 节点状态
    NodeStatus = promauto.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: namespace,
            Name:      "node_status",
            Help:      "Status of cluster node (1=online, 0=offline)",
        },
        []string{"node"},
    )
    
    // Leader 状态
    LeaderStatus = promauto.NewGauge(
        prometheus.GaugeOpts{
            Namespace: namespace,
            Name:      "leader_status",
            Help:      "Whether current node is leader (1=yes, 0=no)",
        },
    )
    
    // Leader 切换次数
    LeaderChanges = promauto.NewCounter(
        prometheus.CounterOpts{
            Namespace: namespace,
            Name:      "leader_changes_total",
            Help:      "Total number of leader changes",
        },
    )
)
```

---

## 5. 错误定义

```go
package errors

import (
    "context"
    "net/http"
    "strings"
    
    "github.com/pingcap/errors"
)

// === 通用错误 ===
var (
    ErrUnknown           = errors.Normalize("unknown error", errors.RFCCodeText("DS:ErrUnknown"))
    ErrInvalidArgument   = errors.Normalize("invalid argument: %s", errors.RFCCodeText("DS:ErrInvalidArgument"))
    ErrInternal          = errors.Normalize("internal error: %s", errors.RFCCodeText("DS:ErrInternal"))
    ErrNotImplemented    = errors.Normalize("not implemented", errors.RFCCodeText("DS:ErrNotImplemented"))
)

// === 配置错误 ===
var (
    ErrInvalidConfig     = errors.Normalize("invalid config: %s", errors.RFCCodeText("DS:ErrInvalidConfig"))
    ErrConfigNotFound    = errors.Normalize("config not found: %s", errors.RFCCodeText("DS:ErrConfigNotFound"))
)

// === 任务错误 ===
var (
    ErrTaskNotExists       = errors.Normalize("task not exists: %s", errors.RFCCodeText("DS:ErrTaskNotExists"))
    ErrTaskAlreadyExists   = errors.Normalize("task already exists: %s", errors.RFCCodeText("DS:ErrTaskAlreadyExists"))
    ErrTaskRunning         = errors.Normalize("task is running: %s", errors.RFCCodeText("DS:ErrTaskRunning"))
    ErrTaskNotRunning      = errors.Normalize("task is not running: %s", errors.RFCCodeText("DS:ErrTaskNotRunning"))
    ErrTaskPaused          = errors.Normalize("task is paused: %s", errors.RFCCodeText("DS:ErrTaskPaused"))
    ErrTaskFailed          = errors.Normalize("task failed: %s", errors.RFCCodeText("DS:ErrTaskFailed"))
)

// === Source 错误 ===
var (
    ErrSourceNotExists          = errors.Normalize("source not exists: %s", errors.RFCCodeText("DS:ErrSourceNotExists"))
    ErrSourceConnectionFailed   = errors.Normalize("source connection failed: %s", errors.RFCCodeText("DS:ErrSourceConnectionFailed"))
    ErrSourceReadFailed         = errors.Normalize("source read failed: %s", errors.RFCCodeText("DS:ErrSourceReadFailed"))
    ErrSourcePositionLost       = errors.Normalize("source position lost", errors.RFCCodeText("DS:ErrSourcePositionLost"))
    ErrSourceGCTTLExceeded      = errors.Normalize("source GC TTL exceeded", errors.RFCCodeText("DS:ErrSourceGCTTLExceeded"))
    ErrSourceSnapshotFailed     = errors.Normalize("source snapshot failed: %s", errors.RFCCodeText("DS:ErrSourceSnapshotFailed"))
)

// === Sink 错误 ===
var (
    ErrSinkNotExists            = errors.Normalize("sink not exists: %s", errors.RFCCodeText("DS:ErrSinkNotExists"))
    ErrSinkConnectionFailed     = errors.Normalize("sink connection failed: %s", errors.RFCCodeText("DS:ErrSinkConnectionFailed"))
    ErrSinkWriteFailed          = errors.Normalize("sink write failed: %s", errors.RFCCodeText("DS:ErrSinkWriteFailed"))
    ErrSinkDDLFailed            = errors.Normalize("sink ddl apply failed: %s", errors.RFCCodeText("DS:ErrSinkDDLFailed"))
    ErrSinkPositionSaveFailed   = errors.Normalize("sink position save failed: %s", errors.RFCCodeText("DS:ErrSinkPositionSaveFailed"))
)

// === Schema 错误 ===
var (
    ErrSchemaNotExists          = errors.Normalize("schema not exists: %s", errors.RFCCodeText("DS:ErrSchemaNotExists"))
    ErrSchemaIncompatible       = errors.Normalize("schema incompatible: %s", errors.RFCCodeText("DS:ErrSchemaIncompatible"))
    ErrSchemaFetchFailed        = errors.Normalize("schema fetch failed: %s", errors.RFCCodeText("DS:ErrSchemaFetchFailed"))
)

// === Pipeline 错误 ===
var (
    ErrPipelineStopped      = errors.Normalize("pipeline stopped", errors.RFCCodeText("DS:ErrPipelineStopped"))
    ErrPipelineTimeout      = errors.Normalize("pipeline timeout", errors.RFCCodeText("DS:ErrPipelineTimeout"))
    ErrFilterFailed         = errors.Normalize("filter failed: %s", errors.RFCCodeText("DS:ErrFilterFailed"))
    ErrTransformFailed      = errors.Normalize("transform failed: %s", errors.RFCCodeText("DS:ErrTransformFailed"))
)

// === 协调器错误 ===
var (
    ErrNotLeader                = errors.Normalize("%s is not leader", errors.RFCCodeText("DS:ErrNotLeader"))
    ErrLeaderElectionFailed     = errors.Normalize("leader election failed: %s", errors.RFCCodeText("DS:ErrLeaderElectionFailed"))
    ErrCoordinatorTimeout       = errors.Normalize("coordinator timeout", errors.RFCCodeText("DS:ErrCoordinatorTimeout"))
    ErrCoordinatorUnreachable   = errors.Normalize("coordinator unreachable", errors.RFCCodeText("DS:ErrCoordinatorUnreachable"))
)

// === 表管理错误 ===
var (
    ErrTableNotExists       = errors.Normalize("table not exists: %s", errors.RFCCodeText("DS:ErrTableNotExists"))
    ErrTableAlreadyExists   = errors.Normalize("table already exists: %s", errors.RFCCodeText("DS:ErrTableAlreadyExists"))
)
```

### 5.1 错误辅助函数

```go
// WrapError 包装错误并附加上下文
func WrapError(rfcError *errors.Error, err error, args ...interface{}) error {
    if err == nil {
        return nil
    }
    return rfcError.Wrap(err).GenWithStackByArgs(args...)
}

// RFCCode 从错误中提取 RFC 错误码
func RFCCode(err error) (errors.RFCErrorCode, bool) {
    type rfcCoder interface {
        RFCCode() errors.RFCErrorCode
    }
    if terr, ok := err.(rfcCoder); ok {
        return terr.RFCCode(), true
    }
    cause := errors.Unwrap(err)
    if cause == nil {
        return "", false
    }
    return RFCCode(cause)
}

// IsRetryableError 判断错误是否可重试
func IsRetryableError(err error) bool {
    if err == nil {
        return false
    }
    
    // context 错误不可重试
    switch errors.Cause(err) {
    case context.Canceled, context.DeadlineExceeded:
        return false
    }
    
    // 检查不可重试错误列表
    for _, e := range unretryableErrors {
        if e.Equal(err) {
            return false
        }
    }
    return true
}

// 不可重试错误列表
var unretryableErrors = []*errors.Error{
    ErrInvalidConfig,
    ErrTaskNotExists,
    ErrSourceNotExists,
    ErrSinkNotExists,
    ErrSchemaIncompatible,
    ErrSourceGCTTLExceeded,
    ErrSourcePositionLost,
}

// HTTPStatusCode HTTP 状态码映射
var httpStatusCodeMapping = map[errors.RFCErrorCode]int{
    ErrUnknown.RFCCode():           http.StatusInternalServerError,
    ErrInvalidArgument.RFCCode():   http.StatusBadRequest,
    ErrTaskNotExists.RFCCode():     http.StatusNotFound,
    ErrTaskAlreadyExists.RFCCode(): http.StatusConflict,
    ErrNotLeader.RFCCode():         http.StatusServiceUnavailable,
}

func HTTPStatusCode(err error) int {
    if err == nil {
        return http.StatusOK
    }
    rfcCode, ok := RFCCode(err)
    if !ok {
        return http.StatusInternalServerError
    }
    if code, ok := httpStatusCodeMapping[rfcCode]; ok {
        return code
    }
    return http.StatusInternalServerError
}
```

---

## 6. 工具函数

### 6.1 重试工具

```go
package utils

// RetryConfig 重试配置
type RetryConfig struct {
    MaxRetries   int           // 最大重试次数
    InitialDelay time.Duration // 初始延迟
    MaxDelay     time.Duration // 最大延迟
    Multiplier   float64       // 延迟倍数
}

// DefaultRetryConfig 默认重试配置
var DefaultRetryConfig = RetryConfig{
    MaxRetries:   3,
    InitialDelay: 100 * time.Millisecond,
    MaxDelay:     10 * time.Second,
    Multiplier:   2.0,
}

// Retry 重试执行函数
func Retry(ctx context.Context, cfg RetryConfig, fn func() error) error {
    var err error
    delay := cfg.InitialDelay
    
    for i := 0; i <= cfg.MaxRetries; i++ {
        err = fn()
        if err == nil {
            return nil
        }
        
        // 检查是否可重试
        if !errors.IsRetryableError(err) {
            return err
        }
        
        // 检查 context
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }
        
        // 等待后重试
        if i < cfg.MaxRetries {
            time.Sleep(delay)
            delay = time.Duration(float64(delay) * cfg.Multiplier)
            if delay > cfg.MaxDelay {
                delay = cfg.MaxDelay
            }
        }
    }
    
    return err
}
```

### 6.2 对象池

```go
// Pool 通用对象池
type Pool[T any] struct {
    pool chan T
    new  func() T
}

// NewPool 创建对象池
func NewPool[T any](size int, newFn func() T) *Pool[T] {
    return &Pool[T]{
        pool: make(chan T, size),
        new:  newFn,
    }
}

// Get 从池中获取对象
func (p *Pool[T]) Get() T {
    select {
    case obj := <-p.pool:
        return obj
    default:
        return p.new()
    }
}

// Put 放回池中
func (p *Pool[T]) Put(obj T) {
    select {
    case p.pool <- obj:
    default:
    }
}
```

### 6.3 Hash 工具

```go
// FNV32 FNV-1a 32-bit hash
func FNV32(s string) uint32 {
    h := uint32(2166136261)
    for i := 0; i < len(s); i++ {
        h ^= uint32(s[i])
        h *= 16777619
    }
    return h
}

// FNV64 FNV-1a 64-bit hash
func FNV64(s string) uint64 {
    h := uint64(14695981039346656037)
    for i := 0; i < len(s); i++ {
        h ^= uint64(s[i])
        h *= 1099511628211
    }
    return h
}
```

---

## 7. Core Layer 架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Application Layers                             │
│              API/CLI  │  Coordinator  │  Pipeline  │  Connector          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              Core Layer                                  │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │
│  │   Config    │  │   Logutil   │  │   Metrics   │  │   Errors    │   │
│  │             │  │             │  │             │  │             │   │
│  │ - Load      │  │ - Init      │  │ - Counter   │  │ - Define    │   │
│  │ - Validate  │  │ - Component │  │ - Gauge     │  │ - Wrap      │   │
│  │ - Adjust    │  │ - Fields    │  │ - Histogram │  │ - RFCCode   │   │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                           Utils                                  │   │
│  │  Retry  │  Pool  │  String  │  Hash  │  Crypto  │  Sync        │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          External Dependencies                           │
│     pingcap/log  │  pingcap/errors  │  prometheus  │  zap  │  toml     │
└─────────────────────────────────────────────────────────────────────────┘
```

---

*返回 [设计文档总览](./Design.md)*

---

## 2026-05-16 监控指标集成更新

完整设计见 `docs/superpowers/specs/2026-05-16-metrics-integration-design.md`。

关键变更：

- 所有指标的 label `namespace` **重命名为 `cluster`**（避免与 Prometheus
  `Namespace` 概念冲突）。
- 注册方式从 `promauto` 隐式 init 改为显式 `metrics.MustRegisterAll(r)` +
  `metrics.ResetForTest()` 支持测试隔离。
- `TaskEventsTotal` 增加 `result` label，取值 `success` / `failed`；`type`
  严格取值 `insert/update/delete/truncate/ddl/heartbeat/tombstone`。
- `error_type` label 取值规范为 `retriable` / `non_retriable`，由
  `metrics.ClassifyError(err)` 基于 `pkg/errors.IsRetryableError` 统一映射。
- Pipeline 状态机由 `metrics.TaskState` 显式 Set 表达（每状态 0/1）+
  `task_total` Inc/Dec 维持集群级分布。

新增 7 个指标：

| 指标 | 用途 |
|------|------|
| `datastream_task_state` | 单 task 当前状态 (0/1) |
| `datastream_source_lag_seconds` | CDC 延迟 |
| `datastream_source_last_event_seconds` | 最近事件 Unix 时间戳 |
| `datastream_pipeline_queue_capacity` | 队列容量 |
| `datastream_connector_connected` | 连接健康 |
| `datastream_snapshot_tables_total` | 待快照表数 |
| `datastream_snapshot_tables_remaining` | 剩余快照表数 |
