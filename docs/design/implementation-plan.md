# 分阶段实现计划

## 概述

本文档定义 DataStream 的分阶段实现计划，按照优先级和依赖关系划分开发阶段，确保项目稳步推进。

---

## 实现原则

### 核心原则

1. **先核心后扩展** - 优先实现核心功能，后续扩展高级特性
2. **先单节点后集群** - 先支持单节点模式，再扩展集群模式
3. **先 MySQL 后其他** - 优先支持 MySQL，再扩展其他数据源
4. **增量迭代** - 每个阶段交付可用的增量版本

### 依赖关系

```
Phase 1: 基础框架
    │
    ├── Phase 2: 单节点 MySQL 同步
    │       │
    │       └── Phase 3: 多数据源支持
    │               │
    │               └── Phase 4: 集群模式
```

---

## Phase 1: 基础框架

**目标**：搭建项目骨架，实现核心基础设施

**周期**：4 周

### 里程碑

| 里程碑 | 内容 | 预计完成 |
|--------|------|---------|
| M1.1 | 项目初始化、目录结构 | Week 1 |
| M1.2 | Core Layer 实现 | Week 2 |
| M1.3 | 事件模型实现 | Week 3 |
| M1.4 | 单元测试框架 | Week 4 |

### 详细任务

#### Week 1: 项目初始化

```
datastream/
├── cmd/
│   └── datastream/
│       └── main.go
├── pkg/
│   ├── config/
│   ├── logutil/
│   ├── errors/
│   └── utils/
├── internal/
├── configs/
├── scripts/
├── tests/
├── docs/
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

**任务清单**：
- [ ] 创建项目目录结构
- [ ] 初始化 Go Module
- [ ] 配置 Makefile
- [ ] 设置 CI/CD (GitHub Actions)
- [ ] 编写 README.md

#### Week 2: Core Layer

**模块**：`pkg/config`, `pkg/logutil`, `pkg/errors`, `pkg/utils`

**任务清单**：
- [ ] Config 模块
  - [ ] 配置结构定义
  - [ ] TOML/JSON 加载
  - [ ] 环境变量覆盖
  - [ ] 配置验证
- [ ] Log 模块
  - [ ] zap 日志初始化
  - [ ] 日志级别配置
  - [ ] 结构化日志
- [ ] Errors 模块
  - [ ] 错误码定义
  - [ ] 错误包装
  - [ ] 错误追踪
- [ ] Utils 模块
  - [ ] Retry 工具
  - [ ] Pool 工具
  - [ ] Hash 工具

#### Week 3: 事件模型

**模块**：`pkg/event`

**任务清单**：
- [ ] ChangeEvent 结构
- [ ] SourceInfo 结构
- [ ] Position 结构
- [ ] TableInfo 结构
- [ ] RowData 结构
- [ ] 位置比较与序列化
- [ ] 事件 ID 生成

#### Week 4: 测试框架

**任务清单**：
- [ ] 单元测试模板
- [ ] Mock 工具集成
- [ ] 测试覆盖率报告
- [ ] CI 测试流水线

### 交付物

- 可编译的项目骨架
- Core Layer 完整实现
- 事件模型定义
- 测试框架搭建完成
- 单元测试覆盖率 > 80%

---

## Phase 2: 单节点 MySQL 同步

**目标**：实现 MySQL 到 MySQL 的单节点同步

**周期**：8 周

### 里程碑

| 里程碑 | 内容 | 预计完成 |
|--------|------|---------|
| M2.1 | MySQL Source Connector | Week 6 |
| M2.2 | MySQL Sink Connector | Week 8 |
| M2.3 | Pipeline 实现 | Week 10 |
| M2.4 | 端到端同步 | Week 12 |

### 详细任务

#### Week 5-6: MySQL Source Connector

**模块**：`internal/source/mysql`

**任务清单**：
- [ ] 连接管理
  - [ ] 连接池
  - [ ] 连接重试
  - [ ] 心跳检测
- [ ] Binlog 解析
  - [ ] Binlog 订阅
  - [ ] 事件解析
  - [ ] 位置记录
- [ ] 快照模式
  - [ ] 全量快照
  - [ ] 多线程快照
  - [ ] 快照进度
- [ ] DDL 处理
  - [ ] DDL 捕获
  - [ ] Schema 维护

#### Week 7-8: MySQL Sink Connector

**模块**：`internal/sink/mysql`

**任务清单**：
- [ ] 连接管理
- [ ] 批量写入
- [ ] 并发写入
- [ ] DDL 应用
- [ ] 进度存储
- [ ] 冲突处理

#### Week 9-10: Pipeline 实现

**模块**：`internal/pipeline`

**任务清单**：
- [ ] Pipeline 接口
- [ ] Filter 实现
  - [ ] 规则过滤器
  - [ ] 表名过滤器
- [ ] Transformer 实现
  - [ ] 字段映射
- [ ] Router 实现
  - [ ] 表路由
- [ ] 生命周期管理

#### Week 11-12: 端到端同步

**任务清单**：
- [ ] 任务配置
- [ ] 任务启动/停止
- [ ] 进度恢复
- [ ] 集成测试
- [ ] E2E 测试

### 交付物

- 完整的 MySQL Source Connector
- 完整的 MySQL Sink Connector
- Pipeline 框架
- MySQL → MySQL 单节点同步
- 集成测试套件
- E2E 测试套件

---

## Phase 3: 多数据源支持

**目标**：扩展支持 PostgreSQL、MongoDB、Oracle、SQL Server

**周期**：12 周

### 里程碑

| 里程碑 | 内容 | 预计完成 |
|--------|------|---------|
| M3.1 | PostgreSQL 支持 | Week 16 |
| M3.2 | MongoDB 支持 | Week 20 |
| M3.3 | Oracle/SQL Server 支持 | Week 24 |

### 详细任务

#### Week 13-16: PostgreSQL 支持

**模块**：`internal/source/postgresql`, `internal/sink/postgresql`

**任务清单**：
- [ ] PostgreSQL Source Connector
  - [ ] 逻辑解码配置
  - [ ] pgoutput 协议
  - [ ] 事件转换
  - [ ] 快照模式
- [ ] PostgreSQL Sink Connector
  - [ ] 批量写入
  - [ ] COPY 协议
  - [ ] DDL 应用

#### Week 17-20: MongoDB 支持

**模块**：`internal/source/mongodb`, `internal/sink/mongodb`

**任务清单**：
- [ ] MongoDB Source Connector
  - [ ] Change Stream 订阅
  - [ ] Oplog 解析（兼容旧版本）
  - [ ] 事件转换
- [ ] MongoDB Sink Connector
  - [ ] 批量写入
  - [ ] Upsert 操作
  - [ ] 嵌套文档处理

#### Week 21-24: Oracle/SQL Server 支持

**模块**：`internal/source/oracle`, `internal/sink/oracle`, `internal/source/sqlserver`, `internal/sink/sqlserver`

**任务清单**：
- [ ] Oracle Source Connector
  - [ ] LogMiner 集成
  - [ ] XStream 支持（可选）
- [ ] Oracle Sink Connector
  - [ ] 批量写入
  - [ ] 存储过程调用
- [ ] SQL Server Source Connector
  - [ ] CDC 表订阅
  - [ ] 事件转换
- [ ] SQL Server Sink Connector
  - [ ] 批量写入
  - [ ] Bulk Insert

### 交付物

- PostgreSQL 完整支持
- MongoDB 完整支持
- Oracle 基本支持
- SQL Server 基本支持
- 各数据源集成测试

---

## Phase 4: 集群模式

**目标**：实现多节点集群、任务调度、故障转移

**周期**：8 周

### 里程碑

| 里程碑 | 内容 | 预计完成 |
|--------|------|---------|
| M4.1 | Coordinator Layer | Week 28 |
| M4.2 | API Layer | Week 30 |
| M4.3 | 故障转移 | Week 32 |

### 详细任务

#### Week 25-28: Coordinator Layer

**模块**：`internal/coordinator`

**任务清单**：
- [ ] etcd Backend
  - [ ] KV 操作
  - [ ] Watch 机制
  - [ ] 分布式锁
  - [ ] Leader 选举
- [ ] Node Manager
  - [ ] 节点注册
  - [ ] 心跳检测
  - [ ] 节点状态维护
- [ ] Task Scheduler
  - [ ] 任务分配
  - [ ] 负载均衡
  - [ ] 任务迁移

#### Week 29-30: API Layer

**模块**：`internal/api`, `cmd/datastream-ctl`

**任务清单**：
- [ ] REST API Server
  - [ ] 任务管理 API
  - [ ] 节点管理 API
  - [ ] 集群管理 API
  - [ ] 认证中间件
- [ ] CLI Tool
  - [ ] 任务命令
  - [ ] 节点命令
  - [ ] 集群命令

#### Week 31-32: 故障转移

**任务清单**：
- [ ] 故障检测
- [ ] 任务重新分配
- [ ] 进度恢复
- [ ] Leader 切换
- [ ] 集群 E2E 测试

### 交付物

- 完整的 Coordinator Layer
- REST API 服务
- CLI 工具
- 故障转移机制
- 集群模式支持

---

## Phase 8.5: 技术债务修复（待办）

**背景**：Phase 8 实施过程中发现多处偏离设计文档的实现，需要重构以符合设计规范。

### 问题清单

| 问题 | 设计要求 | 当前实现 | 优先级 |
|------|---------|---------|--------|
| MySQL/MariaDB Source 使用 canal | 使用 `replication` + 自己的位点/schema 管理 | 使用 `canal.Canal` 封装 | **高** |
| DDL 未使用 Parser | Source 调用 `parser.Parse()` 解析 DDL | DDL 作为原始字符串传递 | **高** |
| Schema 管理未独立 | Connector 自己管理 `TableSchemaCache` | 依赖 canal 的 schema 包 | 中 |
| 位点管理未独立 | 使用 `internal/offset` 模块 | canal 内部位点管理 | 中 |

### 重构任务

#### 任务 1: 移除 canal 依赖，使用 replication 包

**目标**：MySQL/MariaDB Source 只使用 `go-mysql/replication` 底层包

**改动**：
```
internal/source/mysql/connector.go
internal/source/mariadb/connector.go

变更:
- 移除 canal.Canal
- 使用 replication.BinlogSyncer + replication.BinlogStreamer
- 自己实现连接管理和重连逻辑
```

**新增依赖**：
- `github.com/go-mysql-org/go-mysql/replication` (底层 binlog 协议)

#### 任务 2: 集成 DDL Parser

**目标**：Source Connector 收到 DDL 时调用 Parser 解析

**改动**：
```go
// OnDDL 收到 DDL 事件时
func (h *BinlogHandler) OnDDL(...) error {
    // 获取 Parser
    p := parser.DefaultRegistry.Get("mysql")

    // 解析 DDL
    results, err := p.Parse(ctx, query)
    if err != nil {
        return err
    }

    // 构建结构化 ChangeEvent
    for _, result := range results {
        event := &event.ChangeEvent{
            Type:         event.EventTypeDDL,
            DDLResult:    result,  // 新增字段
            // ...
        }
        h.connector.events <- event
    }
}
```

#### 任务 3: 独立 Schema 管理

**目标**：Connector 使用自己的 `TableSchemaCache`，查询 `INFORMATION_SCHEMA`

**改动**：
- 新增 `internal/source/schema_cache.go`
- 查询 `INFORMATION_SCHEMA.COLUMNS`, `INFORMATION_SCHEMA.TABLES`
- DDL 变更时更新缓存

#### 任务 4: 统一位点管理

**目标**：所有 Source 使用 `internal/offset` 模块

**现状**：已部分实现，需要确保与 replication 包集成

### 预计工期

| 任务 | 工期 |
|------|------|
| 移除 canal，使用 replication | 3 天 |
| 集成 DDL Parser | 2 天 |
| 独立 Schema 管理 | 2 天 |
| 统一位点管理 | 1 天 |
| 测试验证 | 2 天 |

**总计：约 2 周**

---

## 后续阶段（可选）

### Phase 5: 高级特性

- Schema 兼容性检查
- 脚本转换器 (Lua/WASM)
- Web Dashboard
- 监控告警增强

### Phase 6: 性能优化

- 高吞吐优化
- 内存优化
- 延迟优化
- 资源限制

---

## 版本规划

| 版本 | 阶段 | 功能 | 预计发布 |
|------|------|------|---------|
| v0.1.0 | Phase 1 | 基础框架 | Week 4 |
| v0.2.0 | Phase 2 | MySQL 单节点同步 | Week 12 |
| v0.3.0 | Phase 3 | 多数据源支持 | Week 24 |
| v1.0.0 | Phase 4 | 集群模式 | Week 32 |

---

## 风险与依赖

### 技术风险

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| ANTLR Parser 复杂度 | 高 | 复用 Debezium grammar，逐步完善 |
| Oracle/SQL Server 驱动 | 中 | 使用官方驱动，社区支持 |
| 集群一致性 | 高 | 充分测试，参考 etcd 最佳实践 |

### 外部依赖

| 依赖 | 用途 | 替代方案 |
|------|------|---------|
| pingcap/log | 日志 | zap |
| pingcap/errors | 错误 | 标准库 errors |
| etcd | 协调后端 | Consul |
| testify | 测试 | gomock |

---

## 人员配置建议

| 角色 | 数量 | 职责 |
|------|------|------|
| 架构师 | 1 | 架构设计、技术决策 |
| 后端开发 | 2-3 | 核心功能开发 |
| 测试工程师 | 1 | 测试用例、自动化 |
| DevOps | 1 | CI/CD、部署 |

---

## 总结

DataStream 项目采用分阶段增量交付策略：

1. **Phase 1 (4周)**：基础框架搭建
2. **Phase 2 (8周)**：MySQL 单节点同步（核心功能）
3. **Phase 3 (12周)**：多数据源扩展
4. **Phase 4 (8周)**：集群模式

总计约 **32 周**（8 个月）完成 v1.0.0 版本。

每个阶段结束时交付可用版本，确保项目持续推进和早期验证。

---

*文档版本：v1.0*
*创建时间：2026-05-07*
*更新时间：2026-05-07*
