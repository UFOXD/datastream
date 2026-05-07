# Go 实现标准

## 概述

本文档定义 DataStream 项目的 Go 实现标准，参考 pingcap/tiflow 项目的最佳实践。

---

## 项目结构

### 目录结构

```
datastream/
├── cmd/                    # 主程序入口
│   ├── datastream/         # 主服务
│   │   └── main.go
│   └── datastream-ctl/     # 命令行工具
│       └── main.go
├── pkg/                    # 公共库（可被外部引用）
│   ├── config/
│   ├── logutil/
│   ├── metrics/
│   ├── errors/
│   └── utils/
├── internal/               # 内部实现（不可被外部引用）
│   ├── source/             # Source Connector 实现
│   ├── sink/               # Sink Connector 实现
│   ├── pipeline/           # Pipeline 实现
│   ├── coordinator/        # Coordinator 实现
│   └── parser/             # DDL Parser 实现
├── proto/                  # Protobuf 定义
├── configs/                # 配置文件模板
│   └── datastream.toml
├── scripts/                # 脚本工具
├── tests/                  # 集成测试
├── docs/                   # 文档
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

### 模块划分原则

1. **pkg/** - 可被外部项目引用的公共库
2. **internal/** - 项目内部实现，使用 `go` 的 internal 机制限制访问
3. **cmd/** - 主程序入口，尽量精简

---

## 代码规范

### 命名规范

```go
// 包名：小写单词，不使用下划线
package config

// 接口：动词或名词 + er 后缀
type Reader interface {}
type ConfigParser interface {}

// 结构体：大驼峰命名
type SourceConnector struct {}

// 常量：大驼峰或全大写下划线分隔
const (
    DefaultPort = 3306
    MAX_RETRIES = 3
)

// 私有字段：小驼峰
type Config struct {
    host string
    port int
}
```

### 注释规范

```go
// Package config provides configuration management for DataStream.
// It supports TOML and JSON formats, with environment variable overrides.
package config

// Config represents the application configuration.
// It is loaded from file and can be updated at runtime.
type Config struct {
    // Name is the name of the DataStream instance.
    Name string `toml:"name" json:"name"`

    // Port is the HTTP server port for metrics and health checks.
    Port int `toml:"port" json:"port"`
}

// Load reads configuration from the specified file path.
// Environment variables override file values if set.
// Returns an error if the file cannot be read or parsed.
func Load(path string) (*Config, error) {
    // ...
}
```

### 错误处理

使用 `pingcap/errors` 库：

```go
import (
    "github.com/pingcap/errors"
)

// 定义错误码
const (
    ErrConfigNotFound  errors.ErrorCode = "Config.NotFound"
    ErrConfigInvalid   errors.ErrorCode = "Config.Invalid"
    ErrConnectionLost  errors.ErrorCode = "Connection.Lost"
)

// 定义错误
var (
    ErrConfigNotFound = errors.Normalize(
        "config file not found",
        errors.RFCCodeText("Config.NotFound"),
    )

    ErrConfigInvalid = errors.Normalize(
        "config is invalid: %s",
        errors.RFCCodeText("Config.Invalid"),
    )
)

// 使用错误
func Load(path string) (*Config, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, errors.Wrap(err, "failed to read config file")
    }

    if len(data) == 0 {
        return nil, ErrConfigNotFound.GenWithStackByArgs()
    }

    // ...
}
```

---

## 并发模式

### Context 传递

```go
// 所有长时间运行的函数都应该接收 context
func (s *SourceConnector) Start(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
            // 处理事件
        }
    }
}
```

### Goroutine 管理

```go
// 使用 errgroup 管理 goroutine
import "golang.org/x/sync/errgroup"

func (p *Pipeline) Run(ctx context.Context) error {
    g, ctx := errgroup.WithContext(ctx)

    // 启动 source
    g.Go(func() error {
        return p.source.Run(ctx)
    })

    // 启动 sink
    g.Go(func() error {
        return p.sink.Run(ctx)
    })

    // 等待所有 goroutine 完成
    return g.Wait()
}
```

### Channel 使用

```go
// 使用 channel 进行 goroutine 间通信
type Pipeline struct {
    eventCh chan *ChangeEvent
}

func (p *Pipeline) Run(ctx context.Context) error {
    for {
        select {
        case event := <-p.eventCh:
            if err := p.processEvent(event); err != nil {
                return err
            }
        case <-ctx.Done():
            return p.drainEvents()
        }
    }
}

// 优雅关闭：排空 channel
func (p *Pipeline) drainEvents() error {
    for {
        select {
        case event := <-p.eventCh:
            if err := p.processEvent(event); err != nil {
                return err
            }
        default:
            return nil
        }
    }
}
```

### 并发安全

```go
// 使用 sync.RWMutex 保护共享状态
type TableManager struct {
    mu     sync.RWMutex
    tables map[string]*TableInfo
}

func (m *TableManager) Get(name string) (*TableInfo, bool) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    t, ok := m.tables[name]
    return t, ok
}

func (m *TableManager) Add(name string, table *TableInfo) {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.tables[name] = table
}
```

---

## 日志规范

### 使用 pingcap/log

```go
import (
    "github.com/pingcap/log"
    "go.uber.org/zap"
)

// 初始化日志
func InitLogger(cfg *log.Config) error {
    logger, props, err := log.InitLogger(cfg)
    if err != nil {
        return err
    }
    log.ReplaceGlobals(logger, props)
    return nil
}

// 日志级别
// Debug: 开发调试信息
// Info:  正常运行信息
// Warn:  警告信息，不影响运行
// Error: 错误信息，需要关注
// Fatal: 致命错误，程序退出

// 结构化日志
func (s *SourceConnector) Connect() error {
    log.Info("connecting to database",
        zap.String("host", s.cfg.Host),
        zap.Int("port", s.cfg.Port),
        zap.String("database", s.cfg.Database),
    )

    if err := s.doConnect(); err != nil {
        log.Error("failed to connect to database",
            zap.String("host", s.cfg.Host),
            zap.Error(err),
        )
        return err
    }

    log.Info("connected to database successfully",
        zap.String("host", s.cfg.Host),
    )
    return nil
}
```

---

## 指标规范

### Prometheus 指标

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

// 定义指标
var (
    // 计数器
    eventsProcessed = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: "datastream",
            Subsystem: "pipeline",
            Name:      "events_processed_total",
            Help:      "Total number of events processed",
        },
        []string{"pipeline", "type"}, // 标签
    )

    // 直方图
    eventLatency = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Namespace: "datastream",
            Subsystem: "pipeline",
            Name:      "event_latency_seconds",
            Help:      "Latency of event processing in seconds",
            Buckets:   prometheus.ExponentialBuckets(0.001, 2, 10),
        },
        []string{"pipeline"},
    )

    // 仪表盘
    queueSize = promauto.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: "datastream",
            Subsystem: "pipeline",
            Name:      "queue_size",
            Help:      "Current size of the event queue",
        },
        []string{"pipeline"},
    )
)

// 使用指标
func (p *Pipeline) processEvent(event *ChangeEvent) error {
    start := time.Now()
    defer func() {
        eventLatency.WithLabelValues(p.name).Observe(time.Since(start).Seconds())
    }()

    // 处理事件...

    eventsProcessed.WithLabelValues(p.name, string(event.Type)).Inc()
    return nil
}
```

### 指标命名规范

```
<namespace>_<subsystem>_<name>_<suffix>

namespace: datastream
subsystem: 模块名 (source, sink, pipeline, coordinator)
name: 指标名称
suffix: _total (计数器), _seconds (时间), _bytes (大小)

示例:
- datastream_source_events_read_total
- datastream_sink_events_written_total
- datastream_pipeline_event_latency_seconds
- datastream_pipeline_queue_size
```

---

## 配置规范

### 配置结构

```go
// 配置使用结构体 + tag 定义
type Config struct {
    Name     string `toml:"name" json:"name"`
    LogLevel string `toml:"log-level" json:"log-level"`

    Source SourceConfig `toml:"source" json:"source"`
    Sink   SinkConfig   `toml:"sink" json:"sink"`
}

type SourceConfig struct {
    Type     string `toml:"type" json:"type"`
    Host     string `toml:"host" json:"host"`
    Port     int    `toml:"port" json:"port"`
    User     string `toml:"user" json:"user"`
    Password string `toml:"password" json:"password"`

    // 支持环境变量覆盖
    // DATASTREAM_SOURCE_PASSWORD
}

// 加载配置
func Load(path string) (*Config, error) {
    cfg := &Config{}

    // 1. 从文件加载
    if _, err := toml.DecodeFile(path, cfg); err != nil {
        return nil, err
    }

    // 2. 环境变量覆盖
    if pwd := os.Getenv("DATASTREAM_SOURCE_PASSWORD"); pwd != "" {
        cfg.Source.Password = pwd
    }

    // 3. 验证配置
    if err := cfg.Validate(); err != nil {
        return nil, err
    }

    return cfg, nil
}
```

### 配置文件示例

```toml
# datastream.toml

name = "datastream-1"
log-level = "info"

[source]
type = "mysql"
host = "localhost"
port = 3306
user = "root"
password = ""

[sink]
type = "mysql"
host = "localhost"
port = 3307
user = "root"
password = ""

[pipeline]
name = "pipeline-1"
batch-size = 1000
```

---

## 测试规范

### 单元测试

```go
// 文件名: config_test.go
package config

import (
    "testing"
    "github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
    t.Run("valid config", func(t *testing.T) {
        cfg, err := Load("testdata/valid.toml")
        require.NoError(t, err)
        require.Equal(t, "datastream-1", cfg.Name)
    })

    t.Run("invalid config", func(t *testing.T) {
        _, err := Load("testdata/nonexistent.toml")
        require.Error(t, err)
    })
}
```

### 表格驱动测试

```go
func TestParseDDL(t *testing.T) {
    tests := []struct {
        name     string
        ddl      string
        expected DDLType
    }{
        {
            name:     "create table",
            ddl:      "CREATE TABLE t (id INT)",
            expected: DDLTypeCreateTable,
        },
        {
            name:     "drop table",
            ddl:      "DROP TABLE t",
            expected: DDLTypeDropTable,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := Parse(tt.ddl)
            require.NoError(t, err)
            require.Equal(t, tt.expected, result.Type)
        })
    }
}
```

### 集成测试

```go
// 文件放在 tests/integration/ 目录
// +build integration

package integration

import (
    "testing"
)

func TestMySQLToMySQLSync(t *testing.T) {
    // 启动测试数据库
    // 执行同步
    // 验证结果
}
```

---

## 依赖管理

### go.mod

```go
module github.com/your-org/datastream

go 1.21

require (
    github.com/pingcap/log v1.1.0
    github.com/pingcap/errors v0.11.5
    github.com/prometheus/client_golang v1.17.0
    go.uber.org/zap v1.26.0
    golang.org/x/sync v0.5.0
    github.com/BurntSushi/toml v1.3.2
)
```

### 依赖原则

1. 最小化依赖
2. 优先使用稳定版本
3. 定期更新安全补丁
4. 避免引入重复功能的库

---

## Makefile

```makefile
.PHONY: build test clean lint

APP_NAME = datastream
VERSION = $(shell git describe --tags --always)
BUILD_TIME = $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS = -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)"

build:
	go build $(LDFLAGS) -o bin/$(APP_NAME) ./cmd/$(APP_NAME)

test:
	go test -v -race -coverprofile=coverage.out ./...

clean:
	rm -rf bin/
	rm -f coverage.out

lint:
	golangci-lint run

proto:
	protoc --go_out=. --go-grpc_out=. proto/*.proto

docker:
	docker build -t $(APP_NAME):$(VERSION) .
```

---

## 代码审查清单

- [ ] 代码符合命名规范
- [ ] 错误处理正确，使用 pingcap/errors
- [ ] 日志使用结构化格式
- [ ] 关键路径添加指标
- [ ] Context 正确传递
- [ ] Goroutine 有退出机制
- [ ] 共享状态有并发保护
- [ ] 单元测试覆盖核心逻辑
- [ ] 无硬编码配置

---

*文档版本：v1.0*
*创建时间：2026-05-07*
*更新时间：2026-05-07*
