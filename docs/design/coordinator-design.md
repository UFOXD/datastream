# Coordinator Layer 设计

## 概述

Coordinator Layer 是 DataStream 的集群协调层，负责节点管理、任务位点持久化、Leader 选举和故障转移。该层确保在单节点或多节点模式下，任务能够正确分配、执行和恢复。

---

## 核心职责

| 职责 | 说明 |
|------|------|
| 任务持久化 | 任务/位点的增删查改，落地到协调后端 |
| 节点管理 | 节点注册/注销、心跳、存活列表 |
| Leader 选举 | 任务级 leader election（每个 task 独立选主，非集群单一 leader） |
| 故障转移 | 死节点探测、未分配任务重新分配（负载均衡） |

---

## 架构设计

```
┌───────────────────────────────────────────────────────────┐
│                  internal/pipeline                          │
│                                                               │
│  ┌───────────────────────┐    ┌───────────────────────────┐ │
│  │   Coordinator (接口)   │    │      ClusterManager        │ │
│  │                        │◄───│  - 心跳循环 (10s)           │ │
│  │  Save/Get/List Task    │    │  - leader 循环（抢集群锁）  │ │
│  │  Save/Get Position     │    │  - rebalanceCluster (30s)  │ │
│  │  Acquire/Release/IsLeader │  │  - pickLeastLoaded         │ │
│  │  Register/Unregister/  │    │  - acquireTask              │ │
│  │  ListNodes/Heartbeat   │    │  - WatchLeadership          │ │
│  └───────────┬────────────┘    └─────────────────────────────┘ │
│              │                                                  │
└──────────────┼──────────────────────────────────────────────────┘
               │ 实现
     ┌─────────┴──────────┐
     │                    │
┌────▼─────┐      ┌───────▼────────┐
│ Memory-  │      │ Etcd-          │
│Coordinator│      │Coordinator     │
│(单节点)  │      │(internal/       │
│          │      │coordinator/etcd)│
└──────────┘      └────────────────┘
```

`ClusterManager` 不是 Coordinator 的实现，而是 Coordinator 的**使用方**——它通过 Coordinator
接口驱动心跳、选主和 rebalance；具体的存储/选举机制由注入的 `MemoryCoordinator` 或
`EtcdCoordinator` 决定。

---

## 接口定义

### Coordinator 接口

单一扁平接口，没有 `NodeManager`/`TaskScheduler`/`Locker` 的子接口拆分（`internal/pipeline/coordinator.go`）：

```go
package pipeline

import (
    "context"

    "github.com/UFOXD/datastream/pkg/event"
)

// Coordinator coordinates tasks across multiple nodes.
type Coordinator interface {
    Initialize(ctx context.Context) error
    Close() error

    SaveTask(ctx context.Context, task *Task) error
    GetTask(ctx context.Context, id string) (*Task, error)
    DeleteTask(ctx context.Context, id string) error
    ListTasks(ctx context.Context) ([]*Task, error)

    SavePosition(ctx context.Context, taskID string, pos *event.Position) error
    GetPosition(ctx context.Context, taskID string) (*event.Position, error)

    // 任务级 leader election —— 每个 taskID 独立选主
    AcquireLeadership(ctx context.Context, taskID string) (bool, error)
    ReleaseLeadership(ctx context.Context, taskID string) error
    IsLeader(ctx context.Context, taskID string) (bool, error)
    WatchLeadership(ctx context.Context, taskID string) (<-chan LeadershipEvent, error)

    RegisterNode(ctx context.Context, nodeID string, info NodeInfo) error
    UnregisterNode(ctx context.Context, nodeID string) error
    ListNodes(ctx context.Context) ([]NodeInfo, error)
    Heartbeat(ctx context.Context, nodeID string) error
}

// LeadershipEvent represents a leadership change event.
type LeadershipEvent struct {
    TaskID    string
    IsLeader  bool
    LeaderID  string
    Timestamp int64
}

// NodeInfo holds information about a node.
type NodeInfo struct {
    ID        string            `json:"id"`
    Address   string            `json:"address"`
    Hostname  string            `json:"hostname"`
    StartedAt int64             `json:"startedAt"` // UnixNano
    LastSeen  int64             `json:"lastSeen"`  // UnixNano，非 time.Time
    Labels    map[string]string `json:"labels,omitempty"`
    Tasks     []string          `json:"tasks,omitempty"`
}
```

### Task / TaskStatus

任务模型定义在 `internal/pipeline/task.go`，没有独立的 `TaskInfo`/`TaskType` 类型：

```go
type Task struct {
    ID        string
    Name      string
    Status    TaskStatus
    Config    *Config
    Pipeline  *Pipeline       `json:"-"`
    Position  *event.Position
    CreatedAt time.Time
    UpdatedAt time.Time
    StartedAt *time.Time
    StoppedAt *time.Time
}

type TaskStatus string

const (
    TaskStatusCreated  TaskStatus = "created"
    TaskStatusStarting TaskStatus = "starting"
    TaskStatusRunning  TaskStatus = "running"
    TaskStatusPaused   TaskStatus = "paused"
    TaskStatusStopping TaskStatus = "stopping"
    TaskStatusStopped  TaskStatus = "stopped"
    TaskStatusError    TaskStatus = "error"
)
```

`TaskManager`（同文件）在 `Coordinator` 之上加了一层：任务生命周期管理 + 指标注册
（`RegisterSourceStats`/`RegisterSinkStats`），不属于 Coordinator Layer 职责，仅在此说明避免混淆。

---

## Coordinator 实现

### MemoryCoordinator（单节点模式）

`internal/pipeline/coordinator.go`。全部状态存在内存 map 里，无锁保护（单节点场景下由调用方保证串行访问）：

```go
type MemoryCoordinator struct {
    tasks     map[string]*Task
    positions map[string]*event.Position
    nodes     map[string]NodeInfo
    leaders   map[string]string // taskID -> nodeID
    thisNode  string
    watchers  map[string][]chan LeadershipEvent
}

func NewMemoryCoordinator(nodeID string) *MemoryCoordinator
```

- `AcquireLeadership`：taskID 未被占用则本节点直接拿锁；已被占用则返回 `leader == thisNode`。
- 单节点部署下永远只有一个节点，`AcquireLeadership` 总是成功。

### EtcdCoordinator（集群模式）

`internal/coordinator/etcd.go`，基于 `go.etcd.io/etcd/client/v3` 的 `concurrency.Election`：

```go
type EtcdCoordinator struct {
    client    *clientv3.Client
    session   *concurrency.Session
    prefix    string
    nodeID    string
    leases    map[string]clientv3.LeaseID
    elections map[string]*concurrency.Election // 每个 taskID 缓存一个 Election 实例
    watchers  map[string][]chan pipeline.LeadershipEvent
}

func NewEtcdCoordinator(cfg *EtcdConfig) (*EtcdCoordinator, error)
```

Key 布局（`prefix` 默认 `/datastream`）：

| 用途 | Key 格式 |
|---|---|
| 任务 | `{prefix}/tasks/{taskID}` |
| 位点 | `{prefix}/positions/{taskID}` |
| 任务 leader 选举 | `{prefix}/leadership/{taskID}` |
| 节点 | `{prefix}/nodes/{nodeID}` |

**关键实现细节**：
- `AcquireLeadership` 每次调用 `concurrency.NewElection(session, key)` 并 `Campaign`；赢得选举后把该
  `Election` 实例缓存进 `elections[taskID]`，因为 `Resign` 必须在赢得 `Campaign` 的**同一个** `Election`
  实例上调用，否则 `ReleaseLeadership` 会失败或释放错误的会话。
- 节点注册使用固定 30 秒 TTL 的 etcd lease（`RegisterNode` 内硬编码 `c.client.Grant(ctx, 30)`），
  不经过 `CoordinatorConfig.SessionTTL` 配置项，`Heartbeat` 通过 `KeepAliveOnce(leaseID)` 续约。
- `Initialize` 创建的 `concurrency.Session` TTL 硬编码为 15 秒，也未读取 `SessionTTL` 配置。

⚠️ 这两处硬编码意味着 `[coordinator] session-ttl` 配置项目前对 EtcdCoordinator **不生效**，是已知的配置与实现不一致点，需要在实现层修复或从配置文档中移除该项。

---

## 集群协调：ClusterManager

真实的心跳、选主、故障转移逻辑集中在 `internal/pipeline/cluster.go`，不是分散在
`NodeManager`/`Handler`/`LeaderElection` 三个类里：

```go
const (
    NodeHeartbeatInterval = 10 * time.Second // 节点心跳间隔
    NodeExpiryThreshold   = 30 * time.Second // 心跳超时判定节点死亡
    RebalanceInterval     = 30 * time.Second // leader 扫描未分配任务的间隔
    MaxTasksPerNode       = 10               // pickLeastLoaded 的软上限
)

type ClusterManager struct {
    coordinator Coordinator
    nodeID      string
    nodeInfo    NodeInfo
    localTasks  map[string]*Task // 本节点持有 leadership 的任务

    onTaskAssigned   func(ctx context.Context, task *Task) error
    onTaskRevoked    func(ctx context.Context, taskID string) error
    onLeadershipLost func(ctx context.Context, taskID string)

    isLeader bool // 是否为集群 leader（负责跑 rebalance 循环，非任务执行 leader）
}

func NewClusterManager(coordinator Coordinator, nodeID, address, hostname string) *ClusterManager
```

### 任务级 leader election（区别于"单一集群 leader"模型）

`ClusterManager` 里有两层 leadership，容易混淆：

1. **集群 leader**（`_cluster_leader` 固定 key）：只负责跑 `rebalanceCluster` 扫描逻辑，是单一节点。
2. **任务 leader**（每个 `task.ID` 一个 key）：真正执行某个同步任务的节点，是**按任务分散**的——
   不同任务可以由不同节点同时持有 leadership，集群 leader 不会把所有任务都抢到自己身上执行。

```go
// Start: 注册节点 + 启动 heartbeatLoop + leaderLoop
func (m *ClusterManager) Start(ctx context.Context) error

// heartbeatLoop：每 NodeHeartbeatInterval 发送心跳 + 重新注册（刷新 LastSeen）
func (m *ClusterManager) heartbeatLoop()

// leaderLoop：抢 "_cluster_leader" 集群锁；抢到后运行 leaderTasks（rebalance 循环）
func (m *ClusterManager) leaderLoop()

// rebalanceCluster：探活失败节点（超过 NodeExpiryThreshold 未心跳）
// → 找出未分配任务 → pickLeastLoaded 挑负载最低的存活节点 → acquireTask
func (m *ClusterManager) rebalanceCluster(ctx context.Context)

// acquireTask：AcquireLeadership(taskID) 成功后记入 localTasks，触发 onTaskAssigned 回调
func (m *ClusterManager) acquireTask(ctx context.Context, task *Task)

// WatchLeadership：监听某任务的 leadership 变更事件，失去 leadership 时触发 onLeadershipLost
func (m *ClusterManager) WatchLeadership(ctx context.Context, taskID string)
```

⚠️ **已知缺陷**（`cluster.go:260-276`，`rebalanceCluster` 内）：判断"任务是否已被分配给某个存活节点"
的分支目前只是把计算出的 `leaderKey` 丢弃（`_ = leaderKey`），并未真正查询该 key 判断任务当前归属，
导致已分配的任务可能被错误地当成"未分配"重新参与 rebalance。修复前不应依赖该路径做生产环境容量规划，
详见 `MEMORY.md`。

---

## 配置

`CoordinatorConfig`（`pkg/config/config.go`）：

```go
type CoordinatorConfig struct {
    Type            string     `toml:"type"`             // memory | etcd
    Backend         string     `toml:"backend"`
    Endpoints       []string   `toml:"endpoints"`
    SessionTTL      int        `toml:"session-ttl"`      // ⚠️ 当前对 EtcdCoordinator 不生效，见上文
    ElectionTimeout int        `toml:"election-timeout"`
    Etcd            EtcdConfig `toml:"etcd"`
}

type EtcdConfig struct {
    Endpoints   []string `toml:"endpoints"`
    DialTimeout int      `toml:"dial-timeout"`
    Username    string   `toml:"username"`
    Password    string   `toml:"password"`
    TLSCA       string   `toml:"tls-ca"`
    TLSCert     string   `toml:"tls-cert"`
    TLSKey      string   `toml:"tls-key"`
}
```

TOML 示例（`docs/user-guide.md` §3 已有等价示例，此处对齐字段名）：

```toml
[coordinator]
type             = "etcd"      # memory | etcd
session-ttl      = 15
election-timeout = 5000        # ms

[coordinator.etcd]
endpoints    = ["etcd1:2379", "etcd2:2379", "etcd3:2379"]
dial-timeout = 5
username     = ""
password     = ""
tls-ca       = "/etc/datastream/ca.pem"
tls-cert     = "/etc/datastream/cert.pem"
tls-key      = "/etc/datastream/key.pem"
```

---

## 关键设计决策

| 决策项 | 选择 | 说明 |
|--------|------|------|
| 协调后端 | etcd（集群）/ 内存（单节点） | 无 Consul 支持；`Type` 只接受 `memory`/`etcd` |
| Leader 选举粒度 | **按任务**，非按集群 | 任务分散到多个节点各自持有 leadership，负载均衡由 `pickLeastLoaded` 完成 |
| 集群 leader 职责 | 仅跑 rebalance 扫描 | 不代表集群 leader 会执行所有任务 |
| 心跳/过期 | 10s 心跳 / 30s 过期 | `NodeHeartbeatInterval` / `NodeExpiryThreshold`，代码常量，未走配置 |
| etcd session/lease TTL | 15s / 30s，硬编码 | 未读取 `CoordinatorConfig.SessionTTL`，是已知不一致点 |
| 故障恢复 | rebalance 自动重分配 | 但 `rebalanceCluster` 的已分配任务判断逻辑当前不完整（见上文已知缺陷） |

---

*文档版本：v2.0*
*创建时间：2026-05-07*
*重写时间：2026-07-03（删除与实现不符的 Handler/NodeManager/TaskScheduler/Locker/LeaderElection 伪代码，替换为 internal/pipeline/{coordinator,cluster,task}.go 与 internal/coordinator/etcd.go 的真实接口和已知缺陷）*
