# DataStream 设计文档

## 项目概述

**DataStream** 是一个基于 Go 语言实现的变更数据捕获（CDC）平台，从 Debezium 架构借鉴并重构，支持独立运行，无需强制依赖 Kafka。

### 核心目标

- **多数据源支持**：MySQL、PostgreSQL、MongoDB、Oracle、SQL Server、MariaDB
- **多目标支持**：Debezium 现有 Sink + Kafka（可选）
- **集群模式**：单节点/多节点，任务调度，故障转移
- **可插拔架构**：协调后端、Connector、转换器均可扩展

---

## 一、整体架构

```
┌────────────────────────────────────────────────────────────────┐
│                      API / CLI Layer                            │
│         REST API  │  CLI Tool  │  Web Dashboard (future)        │
├────────────────────────────────────────────────────────────────┤
│                     Coordinator Layer                           │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│   │   Cluster    │  │    Task      │  │   Failover   │         │
│   │   Manager    │  │  Scheduler   │  │   Handler    │         │
│   └──────────────┘  └──────────────┘  └──────────────┘         │
│                    Coordination Backend                          │
│                      (etcd / Consul / ...)                       │
├────────────────────────────────────────────────────────────────┤
│                      Pipeline Layer                             │
│   ┌────────────────────────────────────────────────────────┐    │
│   │  Pipeline: Source → Filter → Transform → Sink          │    │
│   │           └───── in-memory channel ─────┘              │    │
│   └────────────────────────────────────────────────────────┘    │
├────────────────────────────────────────────────────────────────┤
│                    Connector Layer                              │
│   ┌─────────────────────┐  ┌─────────────────────┐             │
│   │   Source Connector  │  │    Sink Connector   │             │
│   │ ┌─────┐ ┌─────┐ ... │  │ ┌─────┐ ┌─────┐ ... │             │
│   │ │MySQL│ │  PG │     │  │ │MySQL│ │  PG │     │             │
│   └─────────────────────┘  └─────────────────────┘             │
├────────────────────────────────────────────────────────────────┤
│                       Core Layer                                │
│   Config  │  Logger  │  Metrics  │  Errors  │  Utils           │
└────────────────────────────────────────────────────────────────┘
```

### 核心设计原则

- 每层独立，通过接口解耦
- 层内部使用 goroutine/channel 实现高并发
- 所有可变组件（Connector、Coordinator、Transform）可插拔

---

## 二、统一事件模型（Event Model）

详细设计参见 [事件模型设计](./event-model-design.md)。

---

## 三、需求决策记录

| 决策项 | 选择 | 说明 |
|--------|------|------|
| 数据源 | 全量支持 | MySQL、PostgreSQL、MongoDB、Oracle、SQL Server、MariaDB |
| 下游目标 | Debezium Sink + Kafka | 支持所有 Debezium Sink，Kafka 可选 |
| 运行模式 | 集群模式 | 单节点/多节点，任务调度，故障转移 |
| 协调机制 | 可插拔 | 优先支持 etcd/Consul |
| 数据传输 | 内存队列 | 同步进度持久化，重启可恢复 |
| 进度存储 | 目标数据库 | 每个 Sink 在目标库存进度表 |
| Schema 变更 | 兼容性检查 | 变更时检查，不兼容则暂停告警 |
| 数据转换 | 基础过滤 + 可插拔 | 内置过滤，支持自定义转换器，后续扩展脚本 |
| 推进方式 | 全量规划，分阶段实现 | 先完整设计，按优先级分期开发 |

---

## 四、Go 实现标准（参考 tiflow）

详细规范参见 [Go 实现标准](./go-standards-design.md)。

---

## 五、SQL Parser 设计

详细设计参见 [SQL Parser 设计](./parser-design.md)。

---

## 六、模块设计

### 5.1 Core Layer 设计

> [Core Layer 详细设计](./core-design.md)

Core Layer 是基础层，为其他所有层提供通用能力：
- 配置管理
- 日志工具
- 指标收集
- 错误定义
- 工具函数

### 5.2 Connector Layer 设计

> [Connector Layer 详细设计](./connector-design.md)

Connector Layer 负责：
- Source/Sink Connector 接口定义
- 同步范围配置（Database/Table 级别）
- Database 级别自动发现机制
- Table 级别动态表管理
- SnapshotCoordinator 多线程快照
- SinkCoordinator 多线程并发写入

### 5.3 Pipeline Layer 设计

> [Pipeline Layer 详细设计](./pipeline-design.md)

Pipeline Layer 负责：
- Filter 过滤器接口与实现
- Transform 转换器接口与实现
- Router 路由器接口与实现
- Pipeline 生命周期管理

### 5.4 Coordinator Layer 设计

> [Coordinator Layer 详细设计](./coordinator-design.md)

Coordinator Layer 负责：
- 节点管理（注册、心跳、状态维护）
- 任务调度（分配、负载均衡、迁移）
- 故障转移（检测、任务重新分配、进度恢复）
- 协调后端（etcd/Consul/内存模式）

### 5.5 API/CLI Layer 设计

> [API/CLI Layer 详细设计](./api-cli-design.md)

API/CLI Layer 负责：
- REST API 服务（任务管理、节点管理、监控）
- CLI 命令行工具（datastream-ctl）
- gRPC 接口（内部通信可选）

---

## 八、错误处理与容错设计

详细设计参见 [错误处理与容错设计](./error-handling-design.md)。

---

## 十、测试策略设计

详细设计参见 [测试策略设计](./test-strategy-design.md)。

---

## 十一、设计进度

- [x] 事件模型设计
- [x] Go 实现标准
- [x] SQL Parser 设计
- [x] Core Layer 设计
- [x] Connector Layer 设计
- [x] Pipeline Layer 设计
- [x] Coordinator Layer 设计
- [x] API/CLI Layer 设计
- [x] 错误处理与容错
- [x] 测试策略
- [x] 分阶段实现计划

---

## 十二、实现路线图

详细计划参见 [分阶段实现计划](./implementation-plan.md)。

| 阶段 | 内容 | 周期 |
|------|------|------|
| Phase 1 | 基础框架 | 4 周 |
| Phase 2 | MySQL 单节点同步 | 8 周 |
| Phase 3 | 多数据源支持 | 12 周 |
| Phase 4 | 集群模式 | 8 周 |

---

*文档版本：v1.0*
*创建时间：2026-05-07*
*更新时间：2026-05-07*
*状态：设计中*
