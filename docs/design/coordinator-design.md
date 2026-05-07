# Coordinator Layer 设计

## 概述

Coordinator Layer 是 DataStream 的集群协调层，负责节点管理、任务调度、故障转移和协调后端交互。该层确保在单节点或多节点模式下，任务能够正确分配、执行和恢复。

---

## 核心职责

| 职责 | 说明 |
|------|------|
| 节点管理 | 节点注册、心跳检测、节点状态维护 |
| 任务调度 | 任务分配、负载均衡、任务迁移 |
| 故障转移 | 故障检测、任务重新分配、进度恢复 |
| 协调后端 | 与 etcd/Consul 等协调系统交互 |
| 锁服务 | 分布式锁，防止任务重复执行 |

---

## 架构设计

```
┌─────────────────────────────────────────────────────────────────┐
│                      Coordinator Layer                           │
│                                                                   │
│  ┌──────────────────┐  ┌──────────────────┐  ┌────────────────┐ │
│  │   Node Manager   │  │  Task Scheduler  │  │  Failover      │ │
│  │                  │  │                  │  │  Handler       │ │
│  │  - 注册/注销      │  │  - 任务分配      │  │  - 故障检测    │ │
│  │  - 心跳检测      │  │  - 负载均衡      │  │  - 任务迁移    │ │
│  │  - 状态维护      │  │  - 任务迁移      │  │  - 进度恢复    │ │
│  └──────────────────┘  └──────────────────┘  └────────────────┘ │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                   Coordination Backend                       │ │
│  │                                                               │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │ │
│  │  │    etcd     │  │   Consul    │  │   Memory (单节点)   │  │ │
│  │  │  (推荐)     │  │             │  │                     │  │ │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘  │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## 接口定义

### Coordinator 接口

```go
package coordinator

import (
    "context"
    "time"
)

// Coordinator 协调器主接口
type Coordinator interface {
    // 启动协调器
    Start(ctx context.Context) error

    // 停止协调器
    Stop(ctx context.Context) error

    // 节点管理
    NodeManager() NodeManager

    // 任务调度器
    TaskScheduler() TaskScheduler

    // 锁服务
    Locker() Locker

    // 获取协调器状态
    Status() *CoordinatorStatus
}

// CoordinatorStatus 协调器状态
type CoordinatorStatus struct {
    // 当前节点信息
    Node *NodeInfo

    // 集群节点列表
    Nodes []*NodeInfo

    // 任务列表
    Tasks []*TaskInfo

    // 是否为 Leader
    IsLeader bool
}
```

### NodeManager 接口

```go
// NodeManager 节点管理器
type NodeManager interface {
    // 注册当前节点
    Register(ctx context.Context, node *NodeInfo) error

    // 注销当前节点
    Deregister(ctx context.Context) error

    // 获取所有节点
    ListNodes(ctx context.Context) ([]*NodeInfo, error)

    // 获取指定节点
    GetNode(ctx context.Context, nodeID string) (*NodeInfo, error)

    // 更新心跳
    Heartbeat(ctx context.Context) error

    // 监听节点变化
    WatchNodes(ctx context.Context) (<-chan NodeEvent, error)
}

// NodeInfo 节点信息
type NodeInfo struct {
    // 节点 ID
    ID string `json:"id"`

    // 节点名称
    Name string `json:"name"`

    // 节点地址
    Address string `json:"address"`

    // 节点端口
    Port int `json:"port"`

    // 节点状态
    Status NodeStatus `json:"status"`

    // 节点标签
    Labels map[string]string `json:"labels,omitempty"`

    // 节点资源信息
    Resources *NodeResources `json:"resources,omitempty"`

    // 注册时间
    RegisterTime time.Time `json:"registerTime"`

    // 最后心跳时间
    LastHeartbeat time.Time `json:"lastHeartbeat"`
}

// NodeStatus 节点状态
type NodeStatus string

const (
    NodeStatusActive   NodeStatus = "active"   // 活跃
    NodeStatusInactive NodeStatus = "inactive" // 不活跃
    NodeStatusLeaving  NodeStatus = "leaving"  // 正在离开
    NodeStatusFailed   NodeStatus = "failed"   // 故障
)

// NodeResources 节点资源
type NodeResources struct {
    // CPU 核数
    CPU int `json:"cpu"`

    // 内存大小 (MB)
    Memory int `json:"memory"`

    // 当前任务数
    TaskCount int `json:"taskCount"`

    // 最大任务数
    MaxTasks int `json:"maxTasks"`
}

// NodeEvent 节点事件
type NodeEvent struct {
    Type   EventType `json:"type"`
    Node   *NodeInfo `json:"node"`
}
```

### TaskScheduler 接口

```go
// TaskScheduler 任务调度器
type TaskScheduler interface {
    // 创建任务
    CreateTask(ctx context.Context, task *TaskInfo) error

    // 删除任务
    DeleteTask(ctx context.Context, taskID string) error

    // 获取任务
    GetTask(ctx context.Context, taskID string) (*TaskInfo, error)

    // 获取所有任务
    ListTasks(ctx context.Context) ([]*TaskInfo, error)

    // 获取节点上的任务
    GetTasksByNode(ctx context.Context, nodeID string) ([]*TaskInfo, error)

    // 手动分配任务到节点
    AssignTask(ctx context.Context, taskID, nodeID string) error

    // 触发任务重新调度
    Rebalance(ctx context.Context) error

    // 监听任务变化
    WatchTasks(ctx context.Context) (<-chan TaskEvent, error)
}

// TaskInfo 任务信息
type TaskInfo struct {
    // 任务 ID
    ID string `json:"id"`

    // 任务名称
    Name string `json:"name"`

    // 任务类型
    Type TaskType `json:"type"`

    // 任务状态
    Status TaskStatus `json:"status"`

    // 任务配置
    Config []byte `json:"config"`

    // 分配的节点 ID
    AssignedNode string `json:"assignedNode,omitempty"`

    // 任务进度
    Position []byte `json:"position,omitempty"`

    // 创建时间
    CreateTime time.Time `json:"createTime"`

    // 更新时间
    UpdateTime time.Time `json:"updateTime"`

    // 错误信息
    Error string `json:"error,omitempty"`

    // 标签
    Labels map[string]string `json:"labels,omitempty"`
}

// TaskType 任务类型
type TaskType string

const (
    TaskTypeSync TaskType = "sync" // 数据同步任务
)

// TaskStatus 任务状态
type TaskStatus string

const (
    TaskStatusPending   TaskStatus = "pending"   // 待分配
    TaskStatusRunning   TaskStatus = "running"   // 运行中
    TaskStatusPaused    TaskStatus = "paused"    // 已暂停
    TaskStatusFailed    TaskStatus = "failed"    // 失败
    TaskStatusCompleted TaskStatus = "completed" // 已完成
)

// TaskEvent 任务事件
type TaskEvent struct {
    Type   EventType `json:"type"`
    Task   *TaskInfo `json:"task"`
}

// EventType 事件类型
type EventType string

const (
    EventTypeAdd    EventType = "add"
    EventTypeUpdate EventType = "update"
    EventTypeDelete EventType = "delete"
)
```

### Locker 接口

```go
// Locker 分布式锁接口
type Locker interface {
    // 获取锁
    Lock(ctx context.Context, key string, opts ...LockOption) (Lock, error)

    // 尝试获取锁（非阻塞）
    TryLock(ctx context.Context, key string, opts ...LockOption) (Lock, error)
}

// Lock 锁实例
type Lock interface {
    // 释放锁
    Unlock(ctx context.Context) error

    // 续约
    Renew(ctx context.Context) error

    // 获取锁信息
    Info() *LockInfo
}

// LockInfo 锁信息
type LockInfo struct {
    Key       string    `json:"key"`
    Holder    string    `json:"holder"`
    Acquired  time.Time `json:"acquired"`
    TTL       time.Duration `json:"ttl"`
}

// LockOption 锁选项
type LockOption func(*LockOptions)

type LockOptions struct {
    TTL     time.Duration // 锁过期时间
    Wait    time.Duration // 等待时间
    Holder  string        // 锁持有者标识
}
```

---

## 协调后端实现

### Backend 接口

```go
// Backend 协调后端接口
type Backend interface {
    // 初始化
    Init(ctx context.Context) error

    // 关闭
    Close() error

    // KV 操作
    Put(ctx context.Context, key, value string) error
    Get(ctx context.Context, key string) (string, error)
    Delete(ctx context.Context, key string) error
    List(ctx context.Context, prefix string) (map[string]string, error)

    // 监听
    Watch(ctx context.Context, key string) (<-chan WatchEvent, error)

    // 事务
    Txn(ctx context.Context, ops ...TxnOp) error

    // 锁
    Lock(ctx context.Context, key string, opts LockOptions) (Lock, error)

    // Leader 选举
    Campaign(ctx context.Context, key string, value string) (LeaderSession, error)

    // 健康检查
    HealthCheck(ctx context.Context) error
}

// WatchEvent 监听事件
type WatchEvent struct {
    Type   EventType `json:"type"`
    Key    string    `json:"key"`
    Value  string    `json:"value"`
}

// TxnOp 事务操作
type TxnOp interface {
    isTxnOp()
}

// LeaderSession Leader 会话
type LeaderSession interface {
    // 释放 Leader
    Resign() error

    // 是否仍是 Leader
    IsLeader() bool
}
```

### etcd Backend

```go
package etcd

import (
    "context"
    "time"

    "go.etcd.io/etcd/client/v3"
    "go.etcd.io/etcd/client/v3/concurrency"
)

// Backend etcd 后端实现
type Backend struct {
    client *clientv3.Client

    // 配置
    cfg *Config

    // 租约
    lease clientv3.Lease

    // 会话
    session *concurrency.Session
}

// Config etcd 配置
type Config struct {
    // etcd 端点
    Endpoints []string `toml:"endpoints" json:"endpoints"`

    // 连接超时
    DialTimeout time.Duration `toml:"dialTimeout" json:"dialTimeout"`

    // 认证信息
    Username string `toml:"username" json:"username"`
    Password string `toml:"password" json:"password"`

    // TLS 配置
    TLS *TLSConfig `toml:"tls" json:"tls"`

    // 键前缀
    Prefix string `toml:"prefix" json:"prefix"`
}

// NewBackend 创建 etcd 后端
func NewBackend(cfg *Config) (*Backend, error) {
    client, err := clientv3.New(clientv3.Config{
        Endpoints:   cfg.Endpoints,
        DialTimeout: cfg.DialTimeout,
        Username:    cfg.Username,
        Password:    cfg.Password,
    })
    if err != nil {
        return nil, err
    }

    return &Backend{
        client: client,
        cfg:    cfg,
    }, nil
}

// Put 写入键值
func (b *Backend) Put(ctx context.Context, key, value string) error {
    fullKey := b.cfg.Prefix + key
    _, err := b.client.Put(ctx, fullKey, value)
    return err
}

// Get 读取键值
func (b *Backend) Get(ctx context.Context, key string) (string, error) {
    fullKey := b.cfg.Prefix + key
    resp, err := b.client.Get(ctx, fullKey)
    if err != nil {
        return "", err
    }
    if len(resp.Kvs) == 0 {
        return "", ErrKeyNotFound
    }
    return string(resp.Kvs[0].Value), nil
}

// Lock 获取分布式锁
func (b *Backend) Lock(ctx context.Context, key string, opts LockOptions) (Lock, error) {
    mutex := concurrency.NewMutex(b.session, b.cfg.Prefix+key)

    if err := mutex.Lock(ctx); err != nil {
        return nil, err
    }

    return &etcdLock{
        mutex: mutex,
        key:   key,
    }, nil
}

// Campaign 参与 Leader 选举
func (b *Backend) Campaign(ctx context.Context, key string, value string) (LeaderSession, error) {
    election := concurrency.NewElection(b.session, b.cfg.Prefix+key)

    if err := election.Campaign(ctx, value); err != nil {
        return nil, err
    }

    return &etcdLeaderSession{
        election: election,
        session:  b.session,
    }, nil
}
```

### Memory Backend（单节点模式）

```go
package memory

import (
    "context"
    "sync"
    "time"
)

// Backend 内存后端实现（单节点模式）
type Backend struct {
    mu     sync.RWMutex
    data   map[string]string
    locks  map[string]*memLock
    watches map[string][]chan WatchEvent
}

// NewBackend 创建内存后端
func NewBackend() *Backend {
    return &Backend{
        data:    make(map[string]string),
        locks:   make(map[string]*memLock),
        watches: make(map[string][]chan WatchEvent),
    }
}

// Put 写入键值
func (b *Backend) Put(ctx context.Context, key, value string) error {
    b.mu.Lock()
    defer b.mu.Unlock()

    b.data[key] = value

    // 触发监听
    b.notifyWatchers(key, value)

    return nil
}

// Lock 获取锁（单节点模式下的简化实现）
func (b *Backend) Lock(ctx context.Context, key string, opts LockOptions) (Lock, error) {
    b.mu.Lock()
    defer b.mu.Unlock()

    if lock, exists := b.locks[key]; exists {
        // 检查锁是否过期
        if time.Now().Before(lock.expiresAt) {
            return nil, ErrLockConflict
        }
    }

    lock := &memLock{
        key:        key,
        holder:     opts.Holder,
        acquired:   time.Now(),
        expiresAt:  time.Now().Add(opts.TTL),
    }
    b.locks[key] = lock

    return lock, nil
}
```

---

## 故障转移设计

### FailoverHandler

```go
package failover

import (
    "context"
    "time"

    "datastream/coordinator"
)

// Handler 故障转移处理器
type Handler struct {
    coordinator coordinator.Coordinator

    // 故障检测间隔
    checkInterval time.Duration

    // 心跳超时时间
    heartbeatTimeout time.Duration
}

// NewHandler 创建故障转移处理器
func NewHandler(coord coordinator.Coordinator) *Handler {
    return &Handler{
        coordinator:      coord,
        checkInterval:    5 * time.Second,
        heartbeatTimeout: 30 * time.Second,
    }
}

// Run 运行故障检测
func (h *Handler) Run(ctx context.Context) error {
    ticker := time.NewTicker(h.checkInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-ticker.C:
            if err := h.checkNodes(ctx); err != nil {
                log.Error("failed to check nodes", zap.Error(err))
            }
        }
    }
}

// checkNodes 检查节点状态
func (h *Handler) checkNodes(ctx context.Context) error {
    nodes, err := h.coordinator.NodeManager().ListNodes(ctx)
    if err != nil {
        return err
    }

    now := time.Now()

    for _, node := range nodes {
        // 检查心跳超时
        if now.Sub(node.LastHeartbeat) > h.heartbeatTimeout {
            // 标记节点为故障
            if err := h.handleFailedNode(ctx, node); err != nil {
                log.Error("failed to handle failed node",
                    zap.String("node", node.ID),
                    zap.Error(err),
                )
            }
        }
    }

    return nil
}

// handleFailedNode 处理故障节点
func (h *Handler) handleFailedNode(ctx context.Context, node *coordinator.NodeInfo) error {
    log.Warn("node failed, rebalancing tasks",
        zap.String("node", node.ID),
    )

    // 1. 获取故障节点上的任务
    tasks, err := h.coordinator.TaskScheduler().GetTasksByNode(ctx, node.ID)
    if err != nil {
        return err
    }

    // 2. 重新分配任务
    for _, task := range tasks {
        if err := h.reassignTask(ctx, task); err != nil {
            log.Error("failed to reassign task",
                zap.String("task", task.ID),
                zap.Error(err),
            )
        }
    }

    // 3. 从节点列表中移除故障节点
    // 由心跳超时机制自动处理

    return nil
}

// reassignTask 重新分配任务
func (h *Handler) reassignTask(ctx context.Context, task *coordinator.TaskInfo) error {
    // 选择新节点
    newNode, err := h.selectNode(ctx, task)
    if err != nil {
        return err
    }

    // 分配任务到新节点
    return h.coordinator.TaskScheduler().AssignTask(ctx, task.ID, newNode.ID)
}

// selectNode 选择节点（负载均衡）
func (h *Handler) selectNode(ctx context.Context, task *coordinator.TaskInfo) (*coordinator.NodeInfo, error) {
    nodes, err := h.coordinator.NodeManager().ListNodes(ctx)
    if err != nil {
        return nil, err
    }

    // 过滤活跃节点
    var activeNodes []*coordinator.NodeInfo
    for _, node := range nodes {
        if node.Status == coordinator.NodeStatusActive {
            activeNodes = append(activeNodes, node)
        }
    }

    if len(activeNodes) == 0 {
        return nil, ErrNoAvailableNode
    }

    // 选择负载最低的节点
    var selected *coordinator.NodeInfo
    minTasks := int(^uint(0) >> 1) // Max int

    for _, node := range activeNodes {
        if node.Resources != nil && node.Resources.TaskCount < minTasks {
            minTasks = node.Resources.TaskCount
            selected = node
        }
    }

    return selected, nil
}
```

---

## Leader 选举

```go
package election

import (
    "context"

    "datastream/coordinator"
)

// LeaderElection Leader 选举
type LeaderElection struct {
    backend coordinator.Backend

    // 当前节点 ID
    nodeID string

    // 选举 Key
    key string

    // 当前 Leader 会话
    session coordinator.LeaderSession
}

// Run 参与 Leader 选举
func (e *LeaderElection) Run(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
            // 尝试成为 Leader
            session, err := e.backend.Campaign(ctx, e.key, e.nodeID)
            if err != nil {
                log.Error("failed to campaign for leader",
                    zap.Error(err),
                )
                time.Sleep(1 * time.Second)
                continue
            }

            e.session = session
            log.Info("became leader", zap.String("node", e.nodeID))

            // 保持 Leader 状态
            e.maintainLeadership(ctx)
        }
    }
}

// maintainLeadership 维持 Leader 状态
func (e *LeaderElection) maintainLeadership(ctx context.Context) {
    // 等待会话失效或上下文取消
    <-ctx.Done()

    if e.session != nil {
        e.session.Resign()
    }
}

// IsLeader 检查是否为 Leader
func (e *LeaderElection) IsLeader() bool {
    return e.session != nil && e.session.IsLeader()
}
```

---

## 运行模式

### 单节点模式

```go
// StandaloneCoordinator 单节点协调器
type StandaloneCoordinator struct {
    backend *memory.Backend
    tasks   map[string]*TaskInfo
    mu      sync.RWMutex
}

func NewStandaloneCoordinator() *StandaloneCoordinator {
    return &StandaloneCoordinator{
        backend: memory.NewBackend(),
        tasks:   make(map[string]*TaskInfo),
    }
}

// 单节点模式下不需要 Leader 选举和故障转移
// 所有任务都在本地执行
```

### 集群模式

```go
// ClusterCoordinator 集群协调器
type ClusterCoordinator struct {
    backend     *etcd.Backend
    nodeManager *NodeManager
    scheduler   *TaskScheduler
    failover    *failover.Handler
    election    *LeaderElection
}

func NewClusterCoordinator(cfg *etcd.Config, nodeID string) (*ClusterCoordinator, error) {
    backend, err := etcd.NewBackend(cfg)
    if err != nil {
        return nil, err
    }

    coord := &ClusterCoordinator{
        backend: backend,
    }

    coord.nodeManager = NewNodeManager(backend, nodeID)
    coord.scheduler = NewTaskScheduler(backend)
    coord.failover = failover.NewHandler(coord)
    coord.election = NewLeaderElection(backend, nodeID)

    return coord, nil
}

func (c *ClusterCoordinator) Start(ctx context.Context) error {
    // 1. 注册节点
    if err := c.nodeManager.Register(ctx); err != nil {
        return err
    }

    // 2. 启动心跳
    go c.heartbeatLoop(ctx)

    // 3. 启动 Leader 选举
    go c.election.Run(ctx)

    // 4. 启动故障检测（仅 Leader）
    go func() {
        if c.election.IsLeader() {
            c.failover.Run(ctx)
        }
    }()

    return nil
}
```

---

## 配置示例

```toml
[coordinator]
# 运行模式: standalone, cluster
mode = "cluster"

# 节点配置
[coordinator.node]
id = "node-1"
name = "datastream-node-1"
address = "192.168.1.100"
port = 8080
max-tasks = 10

# 心跳配置
[coordinator.heartbeat]
interval = "5s"
timeout = "30s"

# etcd 配置（集群模式）
[coordinator.etcd]
endpoints = ["http://etcd1:2379", "http://etcd2:2379", "http://etcd3:2379"]
dial-timeout = "5s"
username = ""
password = ""
prefix = "/datastream/"

# 故障转移配置
[coordinator.failover]
check-interval = "5s"
heartbeat-timeout = "30s"
```

---

## 关键设计决策

| 决策项 | 选择 | 说明 |
|--------|------|------|
| 协调后端 | etcd 优先 | 成熟的分布式协调系统 |
| 单节点模式 | 内存后端 | 简化部署，无需外部依赖 |
| Leader 选举 | etcd 选举 | 内置支持，可靠 |
| 心跳机制 | 定时心跳 | 5 秒间隔，30 秒超时 |
| 任务分配 | 负载均衡 | 选择负载最低的节点 |
| 故障恢复 | 自动迁移 | 检测故障后自动重新分配任务 |

---

*文档版本：v1.0*
*创建时间：2026-05-07*
*更新时间：2026-05-07*
