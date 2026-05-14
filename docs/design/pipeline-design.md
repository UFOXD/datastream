# Pipeline Layer 设计

Pipeline Layer 负责事件的处理管道，实现 Source → Filter → Transform → Router → Sink 的数据流。

---

## 1. 模块结构

```
internal/
├── pipeline/         # Pipeline 核心
│   ├── pipeline.go   # Pipeline 定义与生命周期
│   ├── runner.go     # Pipeline 运行器
│   └── context.go    # Pipeline 上下文
├── filter/           # 过滤器
│   ├── filter.go     # Filter 接口
│   ├── rule.go       # 规则过滤
│   └── expression.go # 表达式过滤
├── transform/        # 转换器
│   ├── transform.go  # Transform 接口
│   ├── mapper.go     # 字段映射
│   └── custom.go     # 自定义转换
└── router/           # 路由器
    ├── router.go     # Router 接口
    └── dispatcher.go # 分发器
```

> **说明**：pipeline、filter、transform、router 均属于业务特定逻辑，不作为可复用库对外提供，因此遵循 Go 最佳实践放置于 `internal/` 目录，确保仅本项目可导入。

---

## 2. Pipeline 接口

### 2.1 核心结构

```go
package pipeline

import (
    "context"
    "sync"
    "time"
    
    "github.com/pingcap/errors"
    "go.uber.org/zap"
    
    "datastream/pkg/logutil"
    "datastream/pkg/metrics"
)

// Pipeline 事件处理管道
type Pipeline struct {
    // 配置
    config      *PipelineConfig
    
    // 组件
    source      <-chan *ChangeEvent
    sink        SinkConnector
    filter      Filter
    transform   Transformer
    router      Router
    
    // 内部通道
    filterCh    chan *ChangeEvent
    transformCh chan *ChangeEvent
    routerCh    chan *ChangeEvent
    
    // 状态管理
    status      PipelineStatus
    stats       *PipelineStats
    
    // 并发控制
    ctx         context.Context
    cancel      context.CancelFunc
    wg          sync.WaitGroup
    
    logger      *zap.Logger
}

// PipelineConfig Pipeline 配置
type PipelineConfig struct {
    // 缓冲区大小
    BufferSize  int `json:"buffer-size" toml:"buffer-size"`
    
    // 批处理大小
    BatchSize   int `json:"batch-size" toml:"batch-size"`
    
    // 批处理超时
    BatchTimeout time.Duration `json:"batch-timeout" toml:"batch-timeout"`
    
    // 并发数
    Concurrency int `json:"concurrency" toml:"concurrency"`
}

// PipelineStatus Pipeline 状态
type PipelineStatus string
const (
    PipelineStatusStopped   PipelineStatus = "stopped"
    PipelineStatusRunning   PipelineStatus = "running"
    PipelineStatusPausing   PipelineStatus = "pausing"
    PipelineStatusPaused    PipelineStatus = "paused"
    PipelineStatusError     PipelineStatus = "error"
)

// PipelineStats Pipeline 统计
type PipelineStats struct {
    mu              sync.RWMutex
    
    // 事件计数
    TotalReceived   int64
    TotalFiltered   int64
    TotalTransformed int64
    TotalSent       int64
    
    // 错误计数
    FilterErrors    int64
    TransformErrors int64
    SinkErrors      int64
    
    // 延迟统计
    AvgLatency      time.Duration
    MaxLatency      time.Duration
    
    // 最后更新时间
    LastUpdateTime  time.Time
}
```

### 2.2 生命周期管理

```go
// Run 启动 Pipeline
func (p *Pipeline) Run() error {
    p.logger.Info("pipeline starting",
        zap.Int("buffer-size", p.config.BufferSize),
        zap.Int("batch-size", p.config.BatchSize),
    )
    
    p.status = PipelineStatusRunning
    
    // 启动处理协程
    p.wg.Add(4)
    go p.runFilter()
    go p.runTransform()
    go p.runRouter()
    go p.runMetrics()
    
    return nil
}

// Stop 停止 Pipeline
func (p *Pipeline) Stop() error {
    p.logger.Info("pipeline stopping")
    p.status = PipelineStatusPausing
    
    p.cancel()
    
    // 等待所有协程完成，最多等待 30 秒
    done := make(chan struct{})
    go func() {
        p.wg.Wait()
        close(done)
    }()
    
    select {
    case <-done:
        p.status = PipelineStatusStopped
        p.logger.Info("pipeline stopped")
        return nil
    case <-time.After(30 * time.Second):
        p.status = PipelineStatusError
        return errors.New("pipeline stop timeout")
    }
}

// Pause 暂停 Pipeline
func (p *Pipeline) Pause() error {
    if p.status != PipelineStatusRunning {
        return errors.New("pipeline is not running")
    }
    
    p.status = PipelineStatusPaused
    p.cancel()
    
    p.wg.Wait()
    p.logger.Info("pipeline paused")
    return nil
}

// Resume 恢复 Pipeline
func (p *Pipeline) Resume() error {
    if p.status != PipelineStatusPaused {
        return errors.New("pipeline is not paused")
    }
    
    p.ctx, p.cancel = context.WithCancel(context.Background())
    return p.Run()
}
```

### 2.3 处理阶段

```go
// runFilter 运行过滤阶段
func (p *Pipeline) runFilter() {
    defer p.wg.Done()
    
    for {
        select {
        case <-p.ctx.Done():
            return
            
        case event, ok := <-p.source:
            if !ok {
                p.logger.Info("source channel closed")
                return
            }
            
            start := time.Now()
            p.stats.mu.Lock()
            p.stats.TotalReceived++
            p.stats.mu.Unlock()
            
            // 应用过滤器
            if p.filter != nil {
                pass, err := p.filter.Filter(event)
                if err != nil {
                    p.logger.Error("filter error",
                        zap.Error(err),
                        zap.String("table", event.Source.Table),
                    )
                    p.stats.mu.Lock()
                    p.stats.FilterErrors++
                    p.stats.mu.Unlock()
                    continue
                }
                if !pass {
                    p.stats.mu.Lock()
                    p.stats.TotalFiltered++
                    p.stats.mu.Unlock()
                    continue
                }
            }
            
            // 发送到转换阶段
            select {
            case <-p.ctx.Done():
                return
            case p.filterCh <- event:
            }
            
            metrics.PipelineProcessTime.WithLabelValues(
                "", "", "filter",
            ).Observe(time.Since(start).Seconds())
        }
    }
}

// runTransform 运行转换阶段
func (p *Pipeline) runTransform() {
    defer p.wg.Done()
    
    for {
        select {
        case <-p.ctx.Done():
            return
            
        case event := <-p.filterCh:
            start := time.Now()
            
            // 应用转换器
            if p.transform != nil {
                transformed, err := p.transform.Transform(event)
                if err != nil {
                    p.logger.Error("transform error",
                        zap.Error(err),
                        zap.String("table", event.Source.Table),
                    )
                    p.stats.mu.Lock()
                    p.stats.TransformErrors++
                    p.stats.mu.Unlock()
                    continue
                }
                event = transformed
            }
            
            p.stats.mu.Lock()
            p.stats.TotalTransformed++
            p.stats.mu.Unlock()
            
            // 发送到路由阶段
            select {
            case <-p.ctx.Done():
                return
            case p.transformCh <- event:
            }
            
            metrics.PipelineProcessTime.WithLabelValues(
                "", "", "transform",
            ).Observe(time.Since(start).Seconds())
        }
    }
}

// runRouter 运行路由阶段
func (p *Pipeline) runRouter() {
    defer p.wg.Done()
    
    batch := make([]*ChangeEvent, 0, p.config.BatchSize)
    timer := time.NewTimer(p.config.BatchTimeout)
    defer timer.Stop()
    
    for {
        select {
        case <-p.ctx.Done():
            // 发送剩余事件
            if len(batch) > 0 {
                p.sendBatch(batch)
            }
            return
            
        case event := <-p.transformCh:
            batch = append(batch, event)
            
            if len(batch) >= p.config.BatchSize {
                p.sendBatch(batch)
                batch = batch[:0]
                timer.Reset(p.config.BatchTimeout)
            }
            
        case <-timer.C:
            if len(batch) > 0 {
                p.sendBatch(batch)
                batch = batch[:0]
            }
            timer.Reset(p.config.BatchTimeout)
        }
    }
}

// sendBatch 发送批次到 Sink
func (p *Pipeline) sendBatch(events []*ChangeEvent) {
    if len(events) == 0 {
        return
    }
    
    ctx, cancel := context.WithTimeout(p.ctx, 30*time.Second)
    defer cancel()
    
    if err := p.sink.WriteBatch(ctx, &EventBatch{Events: events}); err != nil {
        p.logger.Error("sink write error", zap.Error(err))
        p.stats.mu.Lock()
        p.stats.SinkErrors += int64(len(events))
        p.stats.mu.Unlock()
        return
    }
    
    p.stats.mu.Lock()
    p.stats.TotalSent += int64(len(events))
    p.stats.LastUpdateTime = time.Now()
    p.stats.mu.Unlock()
}
```

---

## 3. Filter 过滤器

### 3.1 接口定义

```go
package filter

// Filter 过滤器接口
type Filter interface {
    // Filter 判断事件是否通过过滤
    // 返回 true 表示通过，false 表示过滤掉
    Filter(event *ChangeEvent) (bool, error)
}

// FilterChain 过滤器链
type FilterChain struct {
    filters []Filter
}

func NewFilterChain(filters ...Filter) *FilterChain {
    return &FilterChain{filters: filters}
}

func (fc *FilterChain) Filter(event *ChangeEvent) (bool, error) {
    for _, f := range fc.filters {
        pass, err := f.Filter(event)
        if err != nil {
            return false, err
        }
        if !pass {
            return false, nil
        }
    }
    return true, nil
}
```

### 3.2 RuleFilter 规则过滤器

```go
// RuleFilter 规则过滤器
type RuleFilter struct {
    // 包含规则（正则）
    includeTables []*regexp.Regexp
    
    // 排除规则（正则）
    excludeTables []*regexp.Regexp
    
    // 包含操作类型
    includeOps map[event.Operation]bool
    
    // 排除操作类型
    excludeOps map[event.Operation]bool
}

// NewRuleFilter 创建规则过滤器
func NewRuleFilter(cfg *FilterConfig) *RuleFilter {
    rf := &RuleFilter{
        includeOps: make(map[event.Operation]bool),
        excludeOps: make(map[event.Operation]bool),
    }
    
    // 编译包含规则
    for _, pattern := range cfg.IncludeTables {
        rf.includeTables = append(rf.includeTables, regexp.MustCompile(pattern))
    }
    
    // 编译排除规则
    for _, pattern := range cfg.ExcludeTables {
        rf.excludeTables = append(rf.excludeTables, regexp.MustCompile(pattern))
    }
    
    // 设置操作类型
    for _, op := range cfg.IncludeOperations {
        rf.includeOps[op] = true
    }
    for _, op := range cfg.ExcludeOperations {
        rf.excludeOps[op] = true
    }
    
    return rf
}

func (rf *RuleFilter) Filter(event *ChangeEvent) (bool, error) {
    // 检查操作类型
    if len(rf.excludeOps) > 0 && rf.excludeOps[event.Operation] {
        return false, nil
    }
    if len(rf.includeOps) > 0 && !rf.includeOps[event.Operation] {
        return false, nil
    }
    
    // 构建表名
    tableName := event.Source.Database + "." + event.Source.Table
    
    // 检查排除规则
    for _, re := range rf.excludeTables {
        if re.MatchString(tableName) {
            return false, nil
        }
    }
    
    // 检查包含规则
    if len(rf.includeTables) > 0 {
        matched := false
        for _, re := range rf.includeTables {
            if re.MatchString(tableName) {
                matched = true
                break
            }
        }
        if !matched {
            return false, nil
        }
    }
    
    return true, nil
}

// FilterConfig 过滤器配置
type FilterConfig struct {
    // 包含的表（正则表达式）
    IncludeTables []string `json:"include-tables" toml:"include-tables"`
    
    // 排除的表（正则表达式）
    ExcludeTables []string `json:"exclude-tables" toml:"exclude-tables"`
    
    // 包含的操作类型
    IncludeOperations []event.Operation `json:"include-operations" toml:"include-operations"`
    
    // 排除的操作类型
    ExcludeOperations []event.Operation `json:"exclude-operations" toml:"exclude-operations"`
}
```

---

## 4. Transform 转换器

### 4.1 接口定义

```go
package transform

// Transformer 转换器接口
type Transformer interface {
    // Transform 转换事件
    Transform(event *ChangeEvent) (*ChangeEvent, error)
}

// TransformChain 转换器链
type TransformChain struct {
    transformers []Transformer
}

func NewTransformChain(transformers ...Transformer) *TransformChain {
    return &TransformChain{transformers: transformers}
}

func (tc *TransformChain) Transform(event *ChangeEvent) (*ChangeEvent, error) {
    var err error
    for _, t := range tc.transformers {
        event, err = t.Transform(event)
        if err != nil {
            return nil, err
        }
    }
    return event, nil
}
```

### 4.2 MappingTransformer 字段映射转换器

```go
// MappingTransformer 字段映射转换器
type MappingTransformer struct {
    // 字段映射：源字段名 → 目标字段名
    fieldMapping map[string]string
    
    // 字段转换：字段名 → 转换函数
    fieldConverters map[string]FieldConverter
    
    // 静态字段：添加到所有事件的静态字段
    staticFields map[string]interface{}
}

// FieldConverter 字段转换函数
type FieldConverter func(value interface{}) (interface{}, error)

// NewMappingTransformer 创建映射转换器
func NewMappingTransformer(cfg *TransformConfig) *MappingTransformer {
    mt := &MappingTransformer{
        fieldMapping:     make(map[string]string),
        fieldConverters:  make(map[string]FieldConverter),
        staticFields:     make(map[string]interface{}),
    }
    
    // 设置字段映射
    for src, dst := range cfg.FieldMapping {
        mt.fieldMapping[src] = dst
    }
    
    // 设置静态字段
    for k, v := range cfg.StaticFields {
        mt.staticFields[k] = v
    }
    
    return mt
}

func (mt *MappingTransformer) Transform(event *ChangeEvent) (*ChangeEvent, error) {
    // 1. 应用字段映射
    if len(mt.fieldMapping) > 0 || len(mt.fieldConverters) > 0 {
        event.After.Fields = mt.transformFields(event.After.Fields)
        if event.Before != nil {
            event.Before.Fields = mt.transformFields(event.Before.Fields)
        }
    }
    
    // 2. 添加静态字段
    for k, v := range mt.staticFields {
        event.After.Fields[k] = v
    }
    
    return event, nil
}

func (mt *MappingTransformer) transformFields(fields map[string]interface{}) map[string]interface{} {
    result := make(map[string]interface{})
    
    for name, value := range fields {
        // 应用字段转换
        if converter, ok := mt.fieldConverters[name]; ok {
            converted, err := converter(value)
            if err != nil {
                // 转换失败，保留原值
                result[name] = value
                continue
            }
            value = converted
        }
        
        // 应用字段映射
        if newName, ok := mt.fieldMapping[name]; ok {
            result[newName] = value
        } else {
            result[name] = value
        }
    }
    
    return result
}

// TransformConfig 转换器配置
type TransformConfig struct {
    // 字段映射：源字段名 → 目标字段名
    FieldMapping map[string]string `json:"field-mapping" toml:"field-mapping"`
    
    // 静态字段：添加到所有事件
    StaticFields map[string]interface{} `json:"static-fields" toml:"static-fields"`
    
    // 自定义转换器（预留）
    CustomTransformers []string `json:"custom-transformers" toml:"custom-transformers"`
}
```

---

## 5. Router 路由器

### 5.1 接口定义

```go
package router

// Router 路由器接口
type Router interface {
    // Route 计算事件的路由目标
    // 返回目标 Sink ID 或分区 ID
    Route(event *ChangeEvent) (string, error)
}
```

### 5.2 TableRouter 表名路由器

```go
// TableRouter 表名路由器
type TableRouter struct {
    // 表名 → Sink ID 映射
    tableMapping map[string]string
    
    // 默认 Sink ID
    defaultSink string
}

func NewTableRouter(cfg *RouterConfig) *TableRouter {
    return &TableRouter{
        tableMapping: cfg.TableMapping,
        defaultSink:  cfg.DefaultSink,
    }
}

func (tr *TableRouter) Route(event *ChangeEvent) (string, error) {
    tableName := event.Source.Database + "." + event.Source.Table
    
    if sinkID, ok := tr.tableMapping[tableName]; ok {
        return sinkID, nil
    }
    
    return tr.defaultSink, nil
}
```

### 5.3 PartitionRouter 分区路由器

```go
// PartitionRouter 分区路由器（用于 Kafka 等分区目标）
type PartitionRouter struct {
    // 分区策略
    strategy PartitionStrategy
    
    // 分区数量
    partitionCount int
    
    // 分区键字段
    partitionKey []string
}

// PartitionStrategy 分区策略
type PartitionStrategy string
const (
    PartitionByTable   PartitionStrategy = "table"    // 按表分区
    PartitionByPK      PartitionStrategy = "pk"       // 按主键分区
    PartitionByField   PartitionStrategy = "field"    // 按指定字段分区
    PartitionRandom    PartitionStrategy = "random"   // 随机分区
)

func NewPartitionRouter(cfg *RouterConfig) *PartitionRouter {
    return &PartitionRouter{
        strategy:       cfg.PartitionStrategy,
        partitionCount: cfg.PartitionCount,
        partitionKey:   cfg.PartitionKey,
    }
}

func (pr *PartitionRouter) Route(event *ChangeEvent) (string, error) {
    var key string
    
    switch pr.strategy {
    case PartitionByTable:
        key = event.Source.Database + "." + event.Source.Table
        
    case PartitionByPK:
        // 使用主键值
        for _, pk := range event.After.Primary {
            key += fmt.Sprintf("%v|", event.After.Fields[pk])
        }
        
    case PartitionByField:
        // 使用指定字段
        for _, field := range pr.partitionKey {
            key += fmt.Sprintf("%v|", event.After.Fields[field])
        }
        
    case PartitionRandom:
        return fmt.Sprintf("%d", rand.Intn(pr.partitionCount)), nil
    }
    
    // Hash 到分区
    partition := utils.FNV32(key) % uint32(pr.partitionCount)
    return fmt.Sprintf("%d", partition), nil
}

// RouterConfig 路由器配置
type RouterConfig struct {
    // 表名映射（仅 TableRouter）
    TableMapping map[string]string `json:"table-mapping" toml:"table-mapping"`
    
    // 默认 Sink（仅 TableRouter）
    DefaultSink string `json:"default-sink" toml:"default-sink"`
    
    // 分区策略（仅 PartitionRouter）
    PartitionStrategy PartitionStrategy `json:"partition-strategy" toml:"partition-strategy"`
    
    // 分区数量（仅 PartitionRouter）
    PartitionCount int `json:"partition-count" toml:"partition-count"`
    
    // 分区键字段（仅 PartitionRouter，PartitionByField 策略）
    PartitionKey []string `json:"partition-key" toml:"partition-key"`
}
```

---

## 6. Pipeline Layer 架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Source Connector                               │
│                         ChangeEvent Stream                               │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              Pipeline                                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                        Filter Stage                               │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐                 │   │
│  │  │ RuleFilter │  │ ExprFilter │  │   ...      │  FilterChain    │   │
│  │  └────────────┘  └────────────┘  └────────────┘                 │   │
│  │                         │                                        │   │
│  │                         ▼                                        │   │
│  │                    filterCh (buffer)                             │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                    │                                     │
│                                    ▼                                     │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                      Transform Stage                              │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐                 │   │
│  │  │ MapTransform│  │CustomTrans│  │   ...      │  TransformChain │   │
│  │  └────────────┘  └────────────┘  └────────────┘                 │   │
│  │                         │                                        │   │
│  │                         ▼                                        │   │
│  │                  transformCh (buffer)                            │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                    │                                     │
│                                    ▼                                     │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                        Router Stage                               │   │
│  │  ┌────────────────┐  ┌────────────────┐                         │   │
│  │  │  TableRouter   │  │ PartitionRouter│                         │   │
│  │  └────────────────┘  └────────────────┘                         │   │
│  │                         │                                        │   │
│  │                         ▼                                        │   │
│  │                    Batch Accumulator                             │   │
│  │              (batch-size / batch-timeout)                        │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ EventBatch
┌─────────────────────────────────────────────────────────────────────────┐
│                           Sink Coordinator                               │
│                     Multi-Worker Concurrent Write                        │
└─────────────────────────────────────────────────────────────────────────┘
```

---

*返回 [设计文档总览](./Design.md)*
