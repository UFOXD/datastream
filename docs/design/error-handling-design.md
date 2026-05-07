# 错误处理与容错设计

## 概述

本文档定义 DataStream 的错误处理和容错机制，确保系统在各种异常情况下能够正确处理、恢复和告警。

---

## 错误分类

### 错误层级

```
┌─────────────────────────────────────────────────────────────────┐
│                        错误分类体系                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ 可恢复错误 (Recoverable)                                    │ │
│  │                                                               │ │
│  │  - 网络超时        → 自动重试                               │ │
│  │  - 连接断开        → 自动重连                               │ │
│  │  - 临时不可用      → 等待后重试                             │ │
│  │  - 锁冲突          → 等待后重试                             │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ 需干预错误 (Intervention Required)                          │ │
│  │                                                               │ │
│  │  - Schema 不兼容   → 暂停任务，告警                         │ │
│  │  - 权限不足        → 暂停任务，告警                         │ │
│  │  - 配置错误        → 暂停任务，告警                         │ │
│  │  - 数据冲突        → 暂停任务，告警                         │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ 致命错误 (Fatal)                                            │ │
│  │                                                               │ │
│  │  - 数据损坏        → 停止任务，告警                         │ │
│  │  - 存储故障        → 停止任务，告警                         │ │
│  │  - 内部错误        → 停止任务，告警                         │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### 错误类型定义

```go
package errors

import (
    "github.com/pingcap/errors"
)

// ErrorSeverity 错误严重级别
type ErrorSeverity string

const (
    SeverityWarning    ErrorSeverity = "warning"     // 警告
    SeverityRecoverable ErrorSeverity = "recoverable" // 可恢复
    SeverityIntervention ErrorSeverity = "intervention" // 需干预
    SeverityFatal      ErrorSeverity = "fatal"       // 致命
)

// ErrorCategory 错误类别
type ErrorCategory string

const (
    CategoryConnection  ErrorCategory = "connection"  // 连接错误
    CategoryNetwork     ErrorCategory = "network"     // 网络错误
    CategoryPermission  ErrorCategory = "permission"  // 权限错误
    CategorySchema      ErrorCategory = "schema"      // Schema 错误
    CategoryData        ErrorCategory = "data"        // 数据错误
    CategoryConfig      ErrorCategory = "config"      // 配置错误
    CategoryInternal    ErrorCategory = "internal"    // 内部错误
    CategoryResource    ErrorCategory = "resource"    // 资源错误
)

// DataStreamError DataStream 错误
type DataStreamError struct {
    // 错误码
    Code string `json:"code"`

    // 错误消息
    Message string `json:"message"`

    // 错误类别
    Category ErrorCategory `json:"category"`

    // 严重级别
    Severity ErrorSeverity `json:"severity"`

    // 是否可重试
    Retryable bool `json:"retryable"`

    // 原始错误
    Cause error `json:"cause,omitempty"`

    // 上下文信息
    Context map[string]interface{} `json:"context,omitempty"`
}

// Error 实现 error 接口
func (e *DataStreamError) Error() string {
    if e.Cause != nil {
        return e.Message + ": " + e.Cause.Error()
    }
    return e.Message
}

// Unwrap 实现 errors.Unwrap
func (e *DataStreamError) Unwrap() error {
    return e.Cause
}
```

### 错误码定义

```go
// 错误码规范：{模块}{类别}{序号}
// 模块: SRC=Source, SNK=Sink, PIP=Pipeline, CORD=Coordinator, CFG=Config
// 类别: CONN=连接, AUTH=认证, DATA=数据, SCHEMA=Schema, INT=内部

const (
    // Source Connector 错误 (SRC)
    ErrSrcConnFailed     = "SRC-CONN-001" // 连接失败
    ErrSrcConnTimeout    = "SRC-CONN-002" // 连接超时
    ErrSrcAuthFailed     = "SRC-AUTH-001" // 认证失败
    ErrSrcPermDenied     = "SRC-AUTH-002" // 权限不足
    ErrSrcBinlogLost     = "SRC-DATA-001" // Binlog 丢失
    ErrSrcSchemaMismatch = "SRC-SCHEMA-001" // Schema 不匹配

    // Sink Connector 错误 (SNK)
    ErrSnkConnFailed     = "SNK-CONN-001" // 连接失败
    ErrSnkWriteFailed    = "SNK-DATA-001" // 写入失败
    ErrSnkConflict       = "SNK-DATA-002" // 数据冲突
    ErrSnkSchemaInvalid  = "SNK-SCHEMA-001" // Schema 无效

    // Pipeline 错误 (PIP)
    ErrPipTransformFail  = "PIP-DATA-001" // 转换失败
    ErrPipFilterError    = "PIP-DATA-002" // 过滤错误
    ErrPipQueueFull      = "PIP-INT-001" // 队列满

    // Coordinator 错误 (CORD)
    ErrCordNoLeader      = "CORD-INT-001" // 无 Leader
    ErrCordLockFailed    = "CORD-INT-002" // 锁获取失败
    ErrCordNodeDown      = "CORD-INT-003" // 节点故障

    // Config 错误 (CFG)
    ErrCfgInvalid        = "CFG-INT-001" // 配置无效
    ErrCfgMissing        = "CFG-INT-002" // 配置缺失
)

// 错误定义
var (
    // Source 错误
    ErrSourceConnectionFailed = &DataStreamError{
        Code:      ErrSrcConnFailed,
        Message:   "failed to connect to source database",
        Category:  CategoryConnection,
        Severity:  SeverityRecoverable,
        Retryable: true,
    }

    ErrSourceAuthFailed = &DataStreamError{
        Code:      ErrSrcAuthFailed,
        Message:   "authentication failed",
        Category:  CategoryPermission,
        Severity:  SeverityIntervention,
        Retryable: false,
    }

    ErrSourceSchemaMismatch = &DataStreamError{
        Code:      ErrSrcSchemaMismatch,
        Message:   "schema mismatch detected",
        Category:  CategorySchema,
        Severity:  SeverityIntervention,
        Retryable: false,
    }

    // Sink 错误
    ErrSinkWriteFailed = &DataStreamError{
        Code:      ErrSnkWriteFailed,
        Message:   "failed to write to sink",
        Category:  CategoryData,
        Severity:  SeverityRecoverable,
        Retryable: true,
    }

    ErrSinkConflict = &DataStreamError{
        Code:      ErrSnkConflict,
        Message:   "data conflict detected",
        Category:  CategoryData,
        Severity:  SeverityIntervention,
        Retryable: false,
    }
)

// NewError 创建新错误
func NewError(template *DataStreamError, cause error, context map[string]interface{}) *DataStreamError {
    return &DataStreamError{
        Code:      template.Code,
        Message:   template.Message,
        Category:  template.Category,
        Severity:  template.Severity,
        Retryable: template.Retryable,
        Cause:     cause,
        Context:   context,
    }
}
```

---

## 重试机制

### 重试策略

```go
package retry

import (
    "context"
    "math/rand"
    "time"
)

// Strategy 重试策略
type Strategy struct {
    // 最大重试次数
    MaxAttempts int

    // 初始等待时间
    InitialDelay time.Duration

    // 最大等待时间
    MaxDelay time.Duration

    // 退避倍数
    Multiplier float64

    // 是否添加抖动
    Jitter bool
}

// DefaultStrategy 默认重试策略
var DefaultStrategy = &Strategy{
    MaxAttempts:  3,
    InitialDelay: 100 * time.Millisecond,
    MaxDelay:     30 * time.Second,
    Multiplier:   2.0,
    Jitter:       true,
}

// NetworkStrategy 网络错误重试策略
var NetworkStrategy = &Strategy{
    MaxAttempts:  5,
    InitialDelay: 1 * time.Second,
    MaxDelay:     60 * time.Second,
    Multiplier:   2.0,
    Jitter:       true,
}

// Retry 执行重试
func Retry(ctx context.Context, strategy *Strategy, fn func() error) error {
    var lastErr error
    delay := strategy.InitialDelay

    for attempt := 1; attempt <= strategy.MaxAttempts; attempt++ {
        err := fn()
        if err == nil {
            return nil
        }

        lastErr = err

        // 检查是否可重试
        if !isRetryable(err) {
            return err
        }

        // 最后一次尝试不等待
        if attempt == strategy.MaxAttempts {
            break
        }

        // 计算等待时间
        waitTime := calculateDelay(delay, strategy)

        log.Warn("retry attempt failed, waiting before retry",
            zap.Int("attempt", attempt),
            zap.Duration("wait", waitTime),
            zap.Error(err),
        )

        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(waitTime):
        }

        // 增加延迟
        delay = time.Duration(float64(delay) * strategy.Multiplier)
        if delay > strategy.MaxDelay {
            delay = strategy.MaxDelay
        }
    }

    return lastErr
}

// calculateDelay 计算延迟时间
func calculateDelay(base time.Duration, strategy *Strategy) time.Duration {
    if !strategy.Jitter {
        return base
    }

    // 添加 0-50% 的随机抖动
    jitter := time.Duration(rand.Float64() * 0.5 * float64(base))
    return base + jitter
}

// isRetryable 检查错误是否可重试
func isRetryable(err error) bool {
    var dsErr *DataStreamError
    if errors.As(err, &dsErr) {
        return dsErr.Retryable
    }

    // 默认可重试
    return true
}
```

### 使用示例

```go
// 连接数据库（带重试）
func (s *MySQLSourceConnector) connect(ctx context.Context) error {
    return retry.Retry(ctx, retry.NetworkStrategy, func() error {
        conn, err := mysql.Connect(s.cfg)
        if err != nil {
            return errors.NewError(ErrSourceConnectionFailed, err, map[string]interface{}{
                "host": s.cfg.Host,
                "port": s.cfg.Port,
            })
        }
        s.conn = conn
        return nil
    })
}
```

---

## 断路器模式

```go
package circuitbreaker

import (
    "context"
    "sync"
    "time"
)

// State 断路器状态
type State string

const (
    StateClosed   State = "closed"   // 关闭（正常）
    StateOpen     State = "open"     // 打开（熔断）
    StateHalfOpen State = "half-open" // 半开（探测）
)

// CircuitBreaker 断路器
type CircuitBreaker struct {
    mu sync.RWMutex

    // 状态
    state State

    // 失败计数
    failureCount int

    // 成功计数（半开状态）
    successCount int

    // 配置
    config *Config

    // 上次失败时间
    lastFailureTime time.Time
}

// Config 断路器配置
type Config struct {
    // 触发熔断的失败次数
    FailureThreshold int

    // 熔断持续时间
    OpenDuration time.Duration

    // 半开状态下成功的次数（恢复到关闭状态）
    SuccessThreshold int

    // 统计时间窗口
    Window time.Duration
}

// DefaultConfig 默认配置
var DefaultConfig = &Config{
    FailureThreshold: 5,
    OpenDuration:     30 * time.Second,
    SuccessThreshold: 3,
    Window:           60 * time.Second,
}

// NewCircuitBreaker 创建断路器
func NewCircuitBreaker(cfg *Config) *CircuitBreaker {
    return &CircuitBreaker{
        state:  StateClosed,
        config: cfg,
    }
}

// Call 执行调用
func (cb *CircuitBreaker) Call(ctx context.Context, fn func() error) error {
    if !cb.allowRequest() {
        return ErrCircuitOpen
    }

    err := fn()

    if err != nil {
        cb.recordFailure()
        return err
    }

    cb.recordSuccess()
    return nil
}

// allowRequest 检查是否允许请求
func (cb *CircuitBreaker) allowRequest() bool {
    cb.mu.Lock()
    defer cb.mu.Unlock()

    switch cb.state {
    case StateClosed:
        return true

    case StateOpen:
        // 检查是否超过熔断时间
        if time.Since(cb.lastFailureTime) > cb.config.OpenDuration {
            cb.state = StateHalfOpen
            cb.successCount = 0
            return true
        }
        return false

    case StateHalfOpen:
        return true
    }

    return false
}

// recordFailure 记录失败
func (cb *CircuitBreaker) recordFailure() {
    cb.mu.Lock()
    defer cb.mu.Unlock()

    cb.failureCount++
    cb.lastFailureTime = time.Now()

    if cb.state == StateHalfOpen {
        // 半开状态失败，立即熔断
        cb.state = StateOpen
        cb.failureCount = 0
        return
    }

    if cb.failureCount >= cb.config.FailureThreshold {
        cb.state = StateOpen
    }
}

// recordSuccess 记录成功
func (cb *CircuitBreaker) recordSuccess() {
    cb.mu.Lock()
    defer cb.mu.Unlock()

    cb.failureCount = 0

    if cb.state == StateHalfOpen {
        cb.successCount++
        if cb.successCount >= cb.config.SuccessThreshold {
            cb.state = StateClosed
            cb.successCount = 0
        }
    }
}
```

---

## 故障恢复机制

### 任务状态机

```
┌─────────────────────────────────────────────────────────────────┐
│                      任务状态转换图                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│    ┌─────────┐                                                   │
│    │ Pending │ ←────────────────────────────┐                   │
│    └────┬────┘                             │                   │
│         │ start                            │                   │
│         ▼                                  │                   │
│    ┌─────────┐      error (recoverable)    │                   │
│    │ Running │ ───────────────────────────►│                   │
│    └────┬────┘                             │                   │
│         │ pause / error (intervention)     │                   │
│         ▼                                  │                   │
│    ┌─────────┐                             │                   │
│    │ Paused  │ ────── resume ──────────────┘                   │
│    └────┬────┘                                                 │
│         │ stop / error (fatal)                                 │
│         ▼                                                      │
│    ┌─────────┐                                                 │
│    │ Failed  │                                                 │
│    └─────────┘                                                 │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### 故障恢复处理器

```go
package recovery

import (
    "context"
    "time"

    "datastream/errors"
)

// Handler 故障恢复处理器
type Handler struct {
    // 任务管理器
    taskMgr TaskManager

    // 告警器
    alerter Alerter

    // 恢复策略
    strategies map[errors.ErrorSeverity]RecoveryStrategy
}

// RecoveryStrategy 恢复策略
type RecoveryStrategy interface {
    Handle(ctx context.Context, task *Task, err error) error
}

// NewHandler 创建故障恢复处理器
func NewHandler(taskMgr TaskManager, alerter Alerter) *Handler {
    h := &Handler{
        taskMgr:    taskMgr,
        alerter:    alerter,
        strategies: make(map[errors.ErrorSeverity]RecoveryStrategy),
    }

    // 注册默认策略
    h.strategies[errors.SeverityRecoverable] = &RecoverableStrategy{}
    h.strategies[errors.SeverityIntervention] = &InterventionStrategy{alerter: alerter}
    h.strategies[errors.SeverityFatal] = &FatalStrategy{alerter: alerter}

    return h
}

// Handle 处理错误
func (h *Handler) Handle(ctx context.Context, task *Task, err error) error {
    // 提取错误信息
    var dsErr *DataStreamError
    if !errors.As(err, &dsErr) {
        dsErr = errors.NewError(errors.ErrInternal, err, nil)
    }

    // 获取恢复策略
    strategy, ok := h.strategies[dsErr.Severity]
    if !ok {
        strategy = h.strategies[errors.SeverityFatal]
    }

    // 执行恢复策略
    return strategy.Handle(ctx, task, dsErr)
}

// RecoverableStrategy 可恢复错误策略
type RecoverableStrategy struct{}

func (s *RecoverableStrategy) Handle(ctx context.Context, task *Task, err error) error {
    log.Warn("recoverable error, will retry",
        zap.String("task", task.ID),
        zap.Error(err),
    )

    // 记录错误，继续运行
    task.RecordError(err)

    return nil
}

// InterventionStrategy 需干预错误策略
type InterventionStrategy struct {
    alerter Alerter
}

func (s *InterventionStrategy) Handle(ctx context.Context, task *Task, err error) error {
    log.Error("intervention required, pausing task",
        zap.String("task", task.ID),
        zap.Error(err),
    )

    // 暂停任务
    if err := task.Pause(ctx); err != nil {
        return err
    }

    // 发送告警
    s.alerter.Alert(ctx, &Alert{
        Level:   AlertLevelWarning,
        TaskID:  task.ID,
        Message: "Task paused due to error: " + err.Error(),
        Error:   err,
    })

    return nil
}

// FatalStrategy 致命错误策略
type FatalStrategy struct {
    alerter Alerter
}

func (s *FatalStrategy) Handle(ctx context.Context, task *Task, err error) error {
    log.Error("fatal error, stopping task",
        zap.String("task", task.ID),
        zap.Error(err),
    )

    // 停止任务
    if err := task.Stop(ctx); err != nil {
        return err
    }

    // 发送告警
    s.alerter.Alert(ctx, &Alert{
        Level:   AlertLevelCritical,
        TaskID:  task.ID,
        Message: "Task stopped due to fatal error: " + err.Error(),
        Error:   err,
    })

    return nil
}
```

---

## 告警机制

### 告警接口

```go
package alert

import (
    "context"
    "time"
)

// Level 告警级别
type Level string

const (
    AlertLevelInfo     Level = "info"
    AlertLevelWarning  Level = "warning"
    AlertLevelCritical Level = "critical"
)

// Alert 告警信息
type Alert struct {
    // 告警级别
    Level Level `json:"level"`

    // 任务 ID
    TaskID string `json:"taskId"`

    // 告警消息
    Message string `json:"message"`

    // 错误信息
    Error error `json:"error,omitempty"`

    // 时间
    Time time.Time `json:"time"`

    // 上下文
    Context map[string]interface{} `json:"context,omitempty"`
}

// Alerter 告警器接口
type Alerter interface {
    // 发送告警
    Alert(ctx context.Context, alert *Alert) error
}

// MultiAlerter 多告警器组合
type MultiAlerter struct {
    alerters []Alerter
}

func NewMultiAlerter(alerters ...Alerter) *MultiAlerter {
    return &MultiAlerter{alerters: alerters}
}

func (a *MultiAlerter) Alert(ctx context.Context, alert *Alert) error {
    for _, alerter := range a.alerters {
        if err := alerter.Alert(ctx, alert); err != nil {
            log.Error("failed to send alert",
                zap.String("level", string(alert.Level)),
                zap.Error(err),
            )
        }
    }
    return nil
}
```

### 告警渠道实现

```go
// LogAlerter 日志告警器
type LogAlerter struct{}

func (a *LogAlerter) Alert(ctx context.Context, alert *Alert) error {
    fields := []zap.Field{
        zap.String("level", string(alert.Level)),
        zap.String("task", alert.TaskID),
        zap.String("message", alert.Message),
    }

    switch alert.Level {
    case AlertLevelCritical:
        log.Error("alert", fields...)
    case AlertLevelWarning:
        log.Warn("alert", fields...)
    default:
        log.Info("alert", fields...)
    }

    return nil
}

// WebhookAlerter Webhook 告警器
type WebhookAlerter struct {
    url    string
    client *http.Client
}

func (a *WebhookAlerter) Alert(ctx context.Context, alert *Alert) error {
    body, err := json.Marshal(alert)
    if err != nil {
        return err
    }

    req, err := http.NewRequestWithContext(ctx, "POST", a.url, bytes.NewReader(body))
    if err != nil {
        return err
    }

    req.Header.Set("Content-Type", "application/json")

    resp, err := a.client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    if resp.StatusCode >= 400 {
        return fmt.Errorf("webhook returned status %d", resp.StatusCode)
    }

    return nil
}

// SlackAlerter Slack 告警器
type SlackAlerter struct {
    webhookURL string
    client     *http.Client
}

func (a *SlackAlerter) Alert(ctx context.Context, alert *Alert) error {
    color := "good"
    if alert.Level == AlertLevelWarning {
        color = "warning"
    } else if alert.Level == AlertLevelCritical {
        color = "danger"
    }

    payload := map[string]interface{}{
        "attachments": []map[string]interface{}{
            {
                "color": color,
                "title": "DataStream Alert",
                "fields": []map[string]interface{}{
                    {"title": "Level", "value": alert.Level, "short": true},
                    {"title": "Task", "value": alert.TaskID, "short": true},
                    {"title": "Message", "value": alert.Message, "short": false},
                },
                "ts": alert.Time.Unix(),
            },
        },
    }

    body, err := json.Marshal(payload)
    if err != nil {
        return err
    }

    req, err := http.NewRequestWithContext(ctx, "POST", a.webhookURL, bytes.NewReader(body))
    if err != nil {
        return err
    }

    resp, err := a.client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    return nil
}
```

---

## 进度恢复

### 进度存储

```go
// ProgressStore 进度存储接口
type ProgressStore interface {
    // 保存进度
    Save(ctx context.Context, taskID string, position []byte) error

    // 加载进度
    Load(ctx context.Context, taskID string) ([]byte, error)

    // 删除进度
    Delete(ctx context.Context, taskID string) error
}

// DatabaseProgressStore 数据库进度存储
type DatabaseProgressStore struct {
    db     *sql.DB
    table  string
}

func (s *DatabaseProgressStore) Save(ctx context.Context, taskID string, position []byte) error {
    query := fmt.Sprintf(`
        INSERT INTO %s (task_id, position, update_time)
        VALUES (?, ?, NOW())
        ON DUPLICATE KEY UPDATE position = ?, update_time = NOW()
    `, s.table)

    _, err := s.db.ExecContext(ctx, query, taskID, position, position)
    return err
}

func (s *DatabaseProgressStore) Load(ctx context.Context, taskID string) ([]byte, error) {
    query := fmt.Sprintf(`SELECT position FROM %s WHERE task_id = ?`, s.table)

    var position []byte
    err := s.db.QueryRowContext(ctx, query, taskID).Scan(&position)
    if err == sql.ErrNoRows {
        return nil, nil
    }
    return position, err
}
```

### 进度恢复流程

```go
// ResumeFromProgress 从进度恢复
func (s *MySQLSourceConnector) ResumeFromProgress(ctx context.Context, position []byte) error {
    if position == nil {
        // 无进度，从最新位置开始
        return s.StartFromLatest(ctx)
    }

    // 解析进度
    pos := &Position{}
    if err := pos.UnmarshalBinary(position); err != nil {
        return err
    }

    // 从指定位置恢复
    log.Info("resuming from position",
        zap.String("task", s.taskID),
        zap.String("binlog", pos.BinlogFile),
        zap.Uint32("position", pos.BinlogPos),
    )

    return s.StartFromPosition(ctx, pos)
}
```

---

## 配置示例

```toml
[error-handling]
# 重试配置
[error-handling.retry]
max-attempts = 3
initial-delay = "100ms"
max-delay = "30s"
multiplier = 2.0
jitter = true

# 断路器配置
[error-handling.circuit-breaker]
failure-threshold = 5
open-duration = "30s"
success-threshold = 3

# 告警配置
[alerting]
enabled = true

[[alerting.channels]]
type = "log"

[[alerting.channels]]
type = "webhook"
url = "http://alert-server/alerts"

[[alerting.channels]]
type = "slack"
webhook-url = "${SLACK_WEBHOOK_URL}"
```

---

## 设计决策

| 决策项 | 选择 | 说明 |
|--------|------|------|
| 错误库 | pingcap/errors | RFC 错误码，链式追踪 |
| 错误分类 | 三级分类 | 可恢复、需干预、致命 |
| 重试策略 | 指数退避 + 抖动 | 避免惊群效应 |
| 断路器 | 三态断路器 | 关闭、打开、半开 |
| 进度存储 | 目标数据库 | 每个 Sink 独立存储 |
| 告警渠道 | 多渠道组合 | 日志、Webhook、Slack |

---

*文档版本：v1.0*
*创建时间：2026-05-07*
*更新时间：2026-05-07*
