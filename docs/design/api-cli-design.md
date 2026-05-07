# API/CLI Layer 设计

## 概述

API/CLI Layer 是 DataStream 的用户交互层，提供 REST API、命令行工具和 gRPC 接口，用于管理和监控数据同步任务。

---

## 架构设计

```
┌─────────────────────────────────────────────────────────────────┐
│                      API / CLI Layer                             │
│                                                                   │
│  ┌──────────────────┐  ┌──────────────────┐  ┌────────────────┐ │
│  │   REST API       │  │   CLI Tool       │  │   gRPC API     │ │
│  │                  │  │                  │  │                │ │
│  │  - 任务管理      │  │  - datastream-ctl│  │  - 内部通信    │ │
│  │  - 节点管理      │  │  - 交互式操作    │  │  - 性能优化    │ │
│  │  - 监控指标      │  │  - 脚本支持      │  │                │ │
│  └──────────────────┘  └──────────────────┘  └────────────────┘ │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                    HTTP Server                               │ │
│  │                                                               │ │
│  │  - Gin/Echo 框架                                             │ │
│  │  - 中间件：认证、日志、限流、CORS                            │ │
│  │  - Prometheus metrics endpoint                               │ │
│  │  - Health check endpoint                                     │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## REST API 设计

### API 规范

- 遵循 RESTful 设计原则
- 使用 JSON 作为请求/响应格式
- 使用标准 HTTP 状态码
- 支持分页、过滤、排序

### API 端点

#### 任务管理

```
# 任务 CRUD
POST   /api/v1/tasks              # 创建任务
GET    /api/v1/tasks              # 获取任务列表
GET    /api/v1/tasks/:id          # 获取任务详情
PUT    /api/v1/tasks/:id          # 更新任务
DELETE /api/v1/tasks/:id          # 删除任务

# 任务操作
POST   /api/v1/tasks/:id/start    # 启动任务
POST   /api/v1/tasks/:id/stop     # 停止任务
POST   /api/v1/tasks/:id/pause    # 暂停任务
POST   /api/v1/tasks/:id/resume   # 恢复任务
POST   /api/v1/tasks/:id/restart  # 重启任务

# 任务进度
GET    /api/v1/tasks/:id/progress # 获取任务进度
GET    /api/v1/tasks/:id/status   # 获取任务状态

# 表级同步（动态添加/删除表）
POST   /api/v1/tasks/:id/tables   # 添加表到任务
DELETE /api/v1/tasks/:id/tables   # 从任务移除表
```

#### 节点管理

```
GET    /api/v1/nodes              # 获取节点列表
GET    /api/v1/nodes/:id          # 获取节点详情
DELETE /api/v1/nodes/:id          # 移除节点（手动下线）
POST   /api/v1/nodes/:id/drain    # 排空节点上的任务
```

#### 集群管理

```
GET    /api/v1/cluster/status     # 获取集群状态
GET    /api/v1/cluster/leader     # 获取 Leader 信息
POST   /api/v1/cluster/rebalance  # 触发任务重新均衡
```

#### 监控与诊断

```
GET    /api/v1/metrics            # Prometheus 指标
GET    /api/v1/health             # 健康检查
GET    /api/v1/ready              # 就绪检查
GET    /api/v1/diagnose           # 诊断信息
```

### 请求/响应示例

#### 创建任务

**Request:**
```json
POST /api/v1/tasks
Content-Type: application/json

{
    "name": "mysql-to-mysql-sync",
    "type": "sync",
    "config": {
        "source": {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": "***",
            "database": "inventory"
        },
        "sink": {
            "type": "mysql",
            "host": "localhost",
            "port": 3307,
            "user": "root",
            "password": "***"
        },
        "pipeline": {
            "batchSize": 1000,
            "parallelism": 4
        }
    }
}
```

**Response:**
```json
{
    "code": 0,
    "message": "success",
    "data": {
        "id": "task-001",
        "name": "mysql-to-mysql-sync",
        "type": "sync",
        "status": "pending",
        "createTime": "2026-05-07T10:00:00Z",
        "assignedNode": ""
    }
}
```

#### 获取任务列表

**Request:**
```
GET /api/v1/tasks?page=1&pageSize=10&status=running
```

**Response:**
```json
{
    "code": 0,
    "message": "success",
    "data": {
        "items": [
            {
                "id": "task-001",
                "name": "mysql-to-mysql-sync",
                "status": "running",
                "assignedNode": "node-1",
                "progress": {
                    "eventsProcessed": 100000,
                    "currentPosition": "mysql-bin.000001:12345"
                }
            }
        ],
        "total": 1,
        "page": 1,
        "pageSize": 10
    }
}
```

#### 动态添加表

**Request:**
```json
POST /api/v1/tasks/task-001/tables
Content-Type: application/json

{
    "tables": [
        {
            "database": "inventory",
            "table": "orders"
        },
        {
            "database": "inventory",
            "table": "customers"
        }
    ],
    "triggerSnapshot": true
}
```

**Response:**
```json
{
    "code": 0,
    "message": "success",
    "data": {
        "addedTables": 2,
        "snapshotStarted": true
    }
}
```

### 错误响应

```json
{
    "code": 10001,
    "message": "task not found",
    "data": null
}
```

### 错误码定义

| 错误码 | 说明 |
|--------|------|
| 0 | 成功 |
| 10001 | 任务不存在 |
| 10002 | 任务配置无效 |
| 10003 | 任务已存在 |
| 20001 | 节点不存在 |
| 20002 | 节点不可用 |
| 30001 | 连接失败 |
| 30002 | 权限不足 |
| 50001 | 内部错误 |

---

## HTTP Server 实现

### Server 结构

```go
package api

import (
    "context"
    "net/http"

    "github.com/gin-gonic/gin"
)

// Server HTTP API 服务器
type Server struct {
    engine      *gin.Engine
    httpServer  *http.Server

    // 依赖
    coordinator coordinator.Coordinator
    taskManager task.Manager

    // 配置
    cfg *Config
}

// Config API 服务器配置
type Config struct {
    // 监听地址
    Addr string `toml:"addr" json:"addr"`

    // 是否启用认证
    EnableAuth bool `toml:"enableAuth" json:"enableAuth"`

    // 认证配置
    Auth *AuthConfig `toml:"auth" json:"auth"`

    // TLS 配置
    TLS *TLSConfig `toml:"tls" json:"tls"`
}

// NewServer 创建 API 服务器
func NewServer(cfg *Config, coord coordinator.Coordinator, mgr task.Manager) *Server {
    gin.SetMode(gin.ReleaseMode)

    s := &Server{
        engine:      gin.New(),
        cfg:         cfg,
        coordinator: coord,
        taskManager: mgr,
    }

    s.setupMiddlewares()
    s.setupRoutes()

    return s
}

// setupMiddlewares 设置中间件
func (s *Server) setupMiddlewares() {
    // Recovery 中间件
    s.engine.Use(gin.Recovery())

    // 日志中间件
    s.engine.Use(LoggerMiddleware())

    // CORS 中间件
    s.engine.Use(CORSMiddleware())

    // 认证中间件
    if s.cfg.EnableAuth {
        s.engine.Use(AuthMiddleware(s.cfg.Auth))
    }

    // 限流中间件
    s.engine.Use(RateLimitMiddleware())
}

// setupRoutes 设置路由
func (s *Server) setupRoutes() {
    // 健康检查
    s.engine.GET("/health", s.healthCheck)
    s.engine.GET("/ready", s.readyCheck)

    // Prometheus 指标
    s.engine.GET("/metrics", gin.WrapH(promhttp.Handler()))

    // API v1
    v1 := s.engine.Group("/api/v1")
    {
        // 任务管理
        tasks := v1.Group("/tasks")
        {
            tasks.GET("", s.listTasks)
            tasks.POST("", s.createTask)
            tasks.GET("/:id", s.getTask)
            tasks.PUT("/:id", s.updateTask)
            tasks.DELETE("/:id", s.deleteTask)

            tasks.POST("/:id/start", s.startTask)
            tasks.POST("/:id/stop", s.stopTask)
            tasks.POST("/:id/pause", s.pauseTask)
            tasks.POST("/:id/resume", s.resumeTask)
            tasks.POST("/:id/restart", s.restartTask)

            tasks.GET("/:id/progress", s.getTaskProgress)
            tasks.GET("/:id/status", s.getTaskStatus)

            // 表级同步
            tasks.POST("/:id/tables", s.addTables)
            tasks.DELETE("/:id/tables", s.removeTables)
        }

        // 节点管理
        nodes := v1.Group("/nodes")
        {
            nodes.GET("", s.listNodes)
            nodes.GET("/:id", s.getNode)
            nodes.DELETE("/:id", s.removeNode)
            nodes.POST("/:id/drain", s.drainNode)
        }

        // 集群管理
        cluster := v1.Group("/cluster")
        {
            cluster.GET("/status", s.getClusterStatus)
            cluster.GET("/leader", s.getLeader)
            cluster.POST("/rebalance", s.rebalance)
        }
    }
}

// Run 启动服务器
func (s *Server) Run(ctx context.Context) error {
    s.httpServer = &http.Server{
        Addr:    s.cfg.Addr,
        Handler: s.engine,
    }

    // 启动 HTTP 服务器
    errCh := make(chan error, 1)
    go func() {
        if s.cfg.TLS != nil {
            errCh <- s.httpServer.ListenAndServeTLS(
                s.cfg.TLS.CertFile,
                s.cfg.TLS.KeyFile,
            )
        } else {
            errCh <- s.httpServer.ListenAndServe()
        }
    }()

    select {
    case <-ctx.Done():
        return s.Shutdown(context.Background())
    case err := <-errCh:
        return err
    }
}

// Shutdown 关闭服务器
func (s *Server) Shutdown(ctx context.Context) error {
    return s.httpServer.Shutdown(ctx)
}
```

### Handler 实现

```go
package api

import (
    "net/http"

    "github.com/gin-gonic/gin"
)

// Response 统一响应格式
type Response struct {
    Code    int         `json:"code"`
    Message string      `json:"message"`
    Data    interface{} `json:"data,omitempty"`
}

// Success 成功响应
func Success(c *gin.Context, data interface{}) {
    c.JSON(http.StatusOK, Response{
        Code:    0,
        Message: "success",
        Data:    data,
    })
}

// Error 错误响应
func Error(c *gin.Context, code int, message string) {
    c.JSON(http.StatusOK, Response{
        Code:    code,
        Message: message,
    })
}

// createTask 创建任务
func (s *Server) createTask(c *gin.Context) {
    var req CreateTaskRequest
    if err := c.ShouldBindJSON(&req); err != nil {
        Error(c, 10002, "invalid request: "+err.Error())
        return
    }

    task, err := s.taskManager.CreateTask(c.Request.Context(), &req)
    if err != nil {
        Error(c, 50001, "failed to create task: "+err.Error())
        return
    }

    Success(c, task)
}

// listTasks 获取任务列表
func (s *Server) listTasks(c *gin.Context) {
    var req ListTasksRequest
    if err := c.ShouldBindQuery(&req); err != nil {
        Error(c, 10002, "invalid request: "+err.Error())
        return
    }

    tasks, total, err := s.taskManager.ListTasks(c.Request.Context(), &req)
    if err != nil {
        Error(c, 50001, "failed to list tasks: "+err.Error())
        return
    }

    Success(c, gin.H{
        "items":    tasks,
        "total":    total,
        "page":     req.Page,
        "pageSize": req.PageSize,
    })
}

// addTables 添加表到任务
func (s *Server) addTables(c *gin.Context) {
    taskID := c.Param("id")

    var req AddTablesRequest
    if err := c.ShouldBindJSON(&req); err != nil {
        Error(c, 10002, "invalid request: "+err.Error())
        return
    }

    result, err := s.taskManager.AddTables(c.Request.Context(), taskID, &req)
    if err != nil {
        Error(c, 50001, "failed to add tables: "+err.Error())
        return
    }

    Success(c, result)
}
```

---

## CLI 工具设计

### 命令结构

```
datastream-ctl <command> [subcommand] [flags]

命令:
  task      任务管理
  node      节点管理
  cluster   集群管理
  config    配置管理
  version   版本信息
```

### 任务管理命令

```bash
# 创建任务
datastream-ctl task create --config task.yaml

# 查看任务列表
datastream-ctl task list
datastream-ctl task list --status running

# 查看任务详情
datastream-ctl task show task-001

# 启动/停止/暂停/恢复任务
datastream-ctl task start task-001
datastream-ctl task stop task-001
datastream-ctl task pause task-001
datastream-ctl task resume task-001

# 删除任务
datastream-ctl task delete task-001

# 查看任务进度
datastream-ctl task progress task-001

# 动态添加表
datastream-ctl task add-tables task-001 --tables inventory.orders,inventory.customers --snapshot

# 动态移除表
datastream-ctl task remove-tables task-001 --tables inventory.orders
```

### 节点管理命令

```bash
# 查看节点列表
datastream-ctl node list

# 查看节点详情
datastream-ctl node show node-1

# 下线节点
datastream-ctl node drain node-1

# 移除节点
datastream-ctl node delete node-1
```

### 集群管理命令

```bash
# 查看集群状态
datastream-ctl cluster status

# 查看集群 Leader
datastream-ctl cluster leader

# 触发任务重新均衡
datastream-ctl cluster rebalance
```

### 配置管理命令

```bash
# 验证配置文件
datastream-ctl config validate --file config.toml

# 查看当前配置
datastream-ctl config show
```

### CLI 实现

```go
package cmd

import (
    "fmt"
    "os"

    "github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
    Use:   "datastream-ctl",
    Short: "DataStream CLI tool",
    Long:  "DataStream command line tool for managing sync tasks",
}

var taskCmd = &cobra.Command{
    Use:   "task",
    Short: "Task management",
}

var taskListCmd = &cobra.Command{
    Use:   "list",
    Short: "List tasks",
    Run:   runTaskList,
}

var taskShowCmd = &cobra.Command{
    Use:   "show <task-id>",
    Short: "Show task details",
    Args:  cobra.ExactArgs(1),
    Run:   runTaskShow,
}

var taskCreateCmd = &cobra.Command{
    Use:   "create",
    Short: "Create a new task",
    Run:   runTaskCreate,
}

func init() {
    // task list flags
    taskListCmd.Flags().StringP("status", "s", "", "Filter by status")
    taskListCmd.Flags().IntP("page", "p", 1, "Page number")
    taskListCmd.Flags().IntP("page-size", "n", 10, "Page size")

    // task create flags
    taskCreateCmd.Flags().StringP("config", "c", "", "Task config file")
    taskCreateCmd.MarkFlagRequired("config")

    // task 命令组
    taskCmd.AddCommand(taskListCmd, taskShowCmd, taskCreateCmd)

    // 根命令
    rootCmd.AddCommand(taskCmd, nodeCmd, clusterCmd, configCmd, versionCmd)
}

func Execute() {
    if err := rootCmd.Execute(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}

func runTaskList(cmd *cobra.Command, args []string) {
    client := NewAPIClient()

    status, _ := cmd.Flags().GetString("status")
    page, _ := cmd.Flags().GetInt("page")
    pageSize, _ := cmd.Flags().GetInt("page-size")

    tasks, err := client.ListTasks(status, page, pageSize)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error: %v\n", err)
        os.Exit(1)
    }

    // 打印表格
    printTaskTable(tasks)
}
```

### 任务配置文件格式

```yaml
# task.yaml
name: mysql-to-mysql-sync
type: sync

source:
  type: mysql
  host: localhost
  port: 3306
  user: root
  password: "${MYSQL_SOURCE_PASSWORD}"  # 环境变量
  database: inventory
  # 同步范围
  include:
    - inventory.*  # 所有表
  exclude:
    - inventory.log_*  # 排除日志表

sink:
  type: mysql
  host: localhost
  port: 3307
  user: root
  password: "${MYSQL_SINK_PASSWORD}"

pipeline:
  batchSize: 1000
  parallelism: 4
  filters:
    - type: rule
      rules:
        - "inventory.orders.status != 'deleted'"
  transforms:
    - type: field-mapping
      mappings:
        - source: inventory.users.password
          target: inventory.users.password_hash
```

---

## gRPC 接口（可选）

### Protobuf 定义

```protobuf
syntax = "proto3";

package datastream.v1;

option go_package = "github.com/your-org/datastream/api/grpc/v1;v1";

// TaskService 任务服务
service TaskService {
    rpc CreateTask(CreateTaskRequest) returns (CreateTaskResponse);
    rpc GetTask(GetTaskRequest) returns (GetTaskResponse);
    rpc ListTasks(ListTasksRequest) returns (ListTasksResponse);
    rpc DeleteTask(DeleteTaskRequest) returns (DeleteTaskResponse);

    rpc StartTask(StartTaskRequest) returns (StartTaskResponse);
    rpc StopTask(StopTaskRequest) returns (StopTaskResponse);
    rpc PauseTask(PauseTaskRequest) returns (PauseTaskResponse);
    rpc ResumeTask(ResumeTaskRequest) returns (ResumeTaskResponse);

    rpc WatchTask(WatchTaskRequest) returns (stream TaskEvent);
}

// CreateTaskRequest
message CreateTaskRequest {
    string name = 1;
    string type = 2;
    bytes config = 3;
}

message CreateTaskResponse {
    Task task = 1;
}

// Task 任务信息
message Task {
    string id = 1;
    string name = 2;
    string type = 3;
    string status = 4;
    bytes config = 5;
    string assigned_node = 6;
    int64 create_time = 7;
    int64 update_time = 8;
}

// TaskEvent 任务事件
message TaskEvent {
    string type = 1;
    Task task = 2;
}
```

---

## 中间件设计

### 认证中间件

```go
// AuthMiddleware 认证中间件
func AuthMiddleware(cfg *AuthConfig) gin.HandlerFunc {
    return func(c *gin.Context) {
        // 获取 Token
        token := c.GetHeader("Authorization")
        if token == "" {
            c.AbortWithStatusJSON(http.StatusUnauthorized, Response{
                Code:    401,
                Message: "unauthorized",
            })
            return
        }

        // 验证 Token
        claims, err := ValidateToken(token, cfg.Secret)
        if err != nil {
            c.AbortWithStatusJSON(http.StatusUnauthorized, Response{
                Code:    401,
                Message: "invalid token",
            })
            return
        }

        // 设置用户信息
        c.Set("user", claims)
        c.Next()
    }
}
```

### 限流中间件

```go
// RateLimitMiddleware 限流中间件
func RateLimitMiddleware() gin.HandlerFunc {
    limiter := rate.NewLimiter(rate.Limit(100), 200) // 100 QPS, burst 200

    return func(c *gin.Context) {
        if !limiter.Allow() {
            c.AbortWithStatusJSON(http.StatusTooManyRequests, Response{
                Code:    429,
                Message: "rate limit exceeded",
            })
            return
        }
        c.Next()
    }
}
```

### 日志中间件

```go
// LoggerMiddleware 日志中间件
func LoggerMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        start := time.Now()
        path := c.Request.URL.Path
        query := c.Request.URL.RawQuery

        c.Next()

        latency := time.Since(start)
        status := c.Writer.Status()

        log.Info("HTTP request",
            zap.Int("status", status),
            zap.String("method", c.Request.Method),
            zap.String("path", path),
            zap.String("query", query),
            zap.Duration("latency", latency),
            zap.String("client", c.ClientIP()),
        )
    }
}
```

---

## 配置示例

```toml
[api]
addr = ":8080"
enableAuth = false

[api.auth]
type = "jwt"
secret = "your-secret-key"
expire = "24h"

[api.tls]
enable = false
certFile = "/path/to/cert.pem"
keyFile = "/path/to/key.pem"

[api.rateLimit]
enabled = true
qps = 100
burst = 200

[cli]
server = "http://localhost:8080"
output = "table"  # table, json, yaml
```

---

## 设计决策

| 决策项 | 选择 | 说明 |
|--------|------|------|
| HTTP 框架 | Gin | 高性能，生态丰富 |
| CLI 框架 | Cobra | 标准 Go CLI 框架 |
| API 版本 | v1 | URL 路径版本控制 |
| 认证方式 | JWT | 可选，默认关闭 |
| 响应格式 | JSON | 统一格式，包含 code/message/data |
| gRPC | 可选 | 内部通信可选，公开 API 用 REST |

---

*文档版本：v1.0*
*创建时间：2026-05-07*
*更新时间：2026-05-07*
