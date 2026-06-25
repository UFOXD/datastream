# DataStream 设计文档总览

> 最后更新：2026-06-02

## 项目概述

**DataStream** 是 Go 语言实现的 CDC（Change Data Capture）平台，从 Debezium 架构借鉴并重构，支持独立运行，无需强制依赖 Kafka。

---

## 文档索引

### 1. 项目总览

| 文档 | 说明 |
|------|------|
| [需求文档](./requirements.md) | 项目目标、功能范围、非功能需求 |
| [分阶段实现计划](./implementation-plan.md) | Phase 1-11 实现路线图与进度 |

### 2. 架构基础

| 文档 | 说明 |
|------|------|
| [事件模型设计](./event-model-design.md) | ChangeEvent / Position / RowData / TableInfo 核心数据结构 |
| [Core Layer 设计](./core-design.md) | 配置管理、日志、指标、错误码、工具函数 |
| [Go 实现标准](./go-standards-design.md) | 命名、注释、错误处理、并发安全编码规范 |
| [错误处理与容错设计](./error-handling-design.md) | DataStreamError 分类、CircuitBreaker、Alerter |

### 3. 解析层

| 文档 | 说明 |
|------|------|
| [SQL Parser 设计](./parser-design.md) | DDLParser 接口、ANTLR 语法、Registry、各数据库 Parser |
| [Oracle DML Parser 重构设计](./oracle-dml-parser-design.md) | Oracle LogMiner SQL_REDO 正则→状态机重写、时区策略 |

### 4. 管道层

| 文文档 | 说明 |
|------|------|
| [Pipeline Layer 设计](./pipeline-design.md) | Filter / Transform / Router 接口与实现、Buffer、Backpressure、RateLimiter |

### 5. 连接器层

| 文档 | 说明 |
|------|------|
| [Connector Layer 设计](./connector-design.md) | Source/Sink 接口定义、DatabaseDiscovery、TableManager、SnapshotCoordinator |
| [企业数据库连接器设计](./phase9-enterprise-connectors-design.md) | SQL Server CDC、Oracle LogMiner、Elasticsearch、Redis Sink 设计 |
| [企业连接器使用指南](./enterprise-connectors-guide.md) | 各连接器配置示例与使用说明 |

### 6. 协调与调度

| 文档 | 说明 |
|------|------|
| [Coordinator Layer 设计](./coordinator-design.md) | 节点管理、任务调度、故障转移、etcd 协调后端 |

### 7. API 与 CLI

| 文档 | 说明 |
|------|------|
| [API/CLI Layer 设计](./api-cli-design.md) | REST API 端点、CLI 命令（datastream-ctl）、表管理接口 |

### 8. 高级特性

| 文档 | 说明 |
|------|------|
| [表级独立生命周期设计](./table-lifecycle-design.md) | 全量/增量解耦、SnapshotScheduler、CatchingUpReplayer、BinlogConsumer、CacheEvent 缓冲 |
| [Schema History + 缓冲文件完整性设计](./schema-history-and-cache-integrity-design.md) | **B1 缓冲事务完整性**（按源分治）、**B2 Schema History**（自维护表结构链）、**B3 Parser ApplyDDL**、**B4 DDL 状态跟踪** |

### 9. 质量保障

| 文档 | 说明 |
|------|------|
| [测试策略设计](./test-strategy-design.md) | 单元测试、集成测试、覆盖率目标、Docker 测试环境 |

---

## 当前阻塞项

详见 [Schema History + 缓冲文件完整性设计](./schema-history-and-cache-integrity-design.md) §7 待决策事项。

| 编号 | 问题 | 状态 |
|------|------|------|
| **B1** | 缓冲文件事务完整性 | **已决策**：按源分治 + 事务标记 truncate |
| **B2** | Schema History | 待实现 |
| **B3** | Parser ApplyDDL() | MySQL CHANGE/AFTER/FIRST 未实现 |
| **B4** | DDL 应用状态跟踪 | 已设计，待实现 |

---

## 架构总览

```
┌────────────────────────────────────────────────────────────────┐
│                      API / CLI Layer                            │
│         REST API  │  CLI Tool  │  datastream-ctl               │
├────────────────────────────────────────────────────────────────┤
│                     Coordinator Layer                           │
│   Cluster Manager  │  Task Scheduler  │  Failover Handler      │
│                    Coordination Backend (etcd)                   │
├────────────────────────────────────────────────────────────────┤
│                      Pipeline Layer                             │
│   Source → Filter → Transform → Router → Sink                  │
│           └────── in-memory channel + Buffer ──────┘            │
├────────────────────────────────────────────────────────────────┤
│                    Connector Layer                              │
│   Source: MySQL | PostgreSQL | MongoDB | Oracle | SQLServer     │
│   Sink:   MySQL | PostgreSQL | MongoDB | ES | Redis | Kafka    │
├────────────────────────────────────────────────────────────────┤
│                    解析 & 生命周期                               │
│   Parser (ANTLR)  │  Schema History  │  Table Lifecycle        │
├────────────────────────────────────────────────────────────────┤
│                       Core Layer                                │
│   Config  │  Logger  │  Metrics  │  Errors  │  Utils           │
└────────────────────────────────────────────────────────────────┘
```

---

## 设计进度

| 模块 | 状态 | 文档 |
|------|------|------|
| 事件模型 | ✅ 完成 | [event-model-design.md](./event-model-design.md) |
| Go 标准 | ✅ 完成 | [go-standards-design.md](./go-standards-design.md) |
| SQL Parser | ✅ 完成 | [parser-design.md](./parser-design.md) |
| Core Layer | ✅ 完成 | [core-design.md](./core-design.md) |
| Connector Layer | ✅ 完成 | [connector-design.md](./connector-design.md) |
| Pipeline Layer | ✅ 完成 | [pipeline-design.md](./pipeline-design.md) |
| Coordinator Layer | ✅ 完成 | [coordinator-design.md](./coordinator-design.md) |
| API/CLI Layer | ✅ 完成 | [api-cli-design.md](./api-cli-design.md) |
| 错误处理 | ✅ 完成 | [error-handling-design.md](./error-handling-design.md) |
| 测试策略 | ✅ 完成 | [test-strategy-design.md](./test-strategy-design.md) |
| 企业连接器 | ✅ 完成 | [phase9-enterprise-connectors-design.md](./phase9-enterprise-connectors-design.md) |
| Oracle DML Parser | ✅ 完成 | [oracle-dml-parser-design.md](./oracle-dml-parser-design.md) |
| 表级生命周期 | ✅ 完成 | [table-lifecycle-design.md](./table-lifecycle-design.md) |
| Schema History + 缓冲完整性 | 🔄 设计中 | [schema-history-and-cache-integrity-design.md](./schema-history-and-cache-integrity-design.md) |

---

## 阅读建议

**新成员入门**：需求文档 → 事件模型 → Core Layer → Connector Layer → Pipeline Layer

**了解 CDC 核心流程**：事件模型 → Connector Layer → Pipeline Layer → 表级生命周期

**排查缓冲/恢复问题**：表级生命周期 → Schema History + 缓冲文件完整性设计

**开发新连接器**：Connector Layer 设计 → 企业连接器设计 → Go 实现标准
