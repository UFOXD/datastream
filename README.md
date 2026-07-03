# DataStream

DataStream 是一个基于 Go 语言实现的变更数据捕获（CDC）平台。

## 特性

- **Source 6 种**：MySQL、PostgreSQL、MongoDB、Oracle、SQL Server、MariaDB
- **Sink 8 种**：MySQL、PostgreSQL、MongoDB、Kafka、Elasticsearch、Redis、Oracle、SQL Server
- **运行模式**：单节点或基于 etcd 的多节点集群（leader 选举 + 任务漂移）
- **运行时管理**：动态增减同步表、暂停/恢复单表、通配符自动发现
- **可观测性**：Prometheus 指标 + Grafana 参考面板
- **可插拔架构**：协调后端、Connector、过滤/转换/路由模块均可扩展

## 快速开始

5 分钟跑通最小示例：[`docs/quickstart.md`](docs/quickstart.md)

```bash
# 构建
make build

# 运行
./bin/datastream -config configs/datastream.toml

# 运行测试
make test
```

## 项目结构

```
datastream/
├── cmd/                    # 主程序入口
├── pkg/                    # 公共库（可被外部 import）
├── internal/               # 内部实现
├── configs/                # 配置文件
├── tests/                  # 测试
├── docs/                   # 文档
├── deployments/            # Grafana / 部署模板
└── proto/                  # Protobuf 定义
```

## 文档

- [快速开始](docs/quickstart.md) — 5 分钟跑通示例
- [用户手册](docs/user-guide.md) — 端到端使用指南（配置、CLI、场景、监控、故障排查）
- [部署指南](docs/deployment/README.md) — Docker Compose / Kubernetes 生产部署
- [REST API](docs/api/openapi.yaml) — OpenAPI 3.0 规范
- [运维指标](docs/operations/metrics.md) — Prometheus 指标参考
- [设计文档](docs/design/) — 架构与模块设计
- [模块地图](docs/module-map.md) — 代码组织结构，各模块状态与依赖关系
- [项目状态记录](MEMORY.md) — 已完成组件、已知缺陷、待办事项的最新记录（比本节更新更频繁，出现冲突时以它为准）

## 当前状态与已知缺陷

B1-B4（缓冲文件完整性、Schema History、Parser ApplyDDL、DDL 状态跟踪）已全部完成，
所有 6 个 source connector 集成完成，DDL 同步链路已串通，无 P0 阻塞项。
详细清单见 [`MEMORY.md`](MEMORY.md)。

### 需要留意的已知缺陷

| 缺陷 | 影响 | 详情 |
|------|------|------|
| 集群 rebalance 任务归属判断不完整 | 死节点任务重分配路径未经验证 | `internal/pipeline/cluster.go:265-275`，`leaderKey` 计算后未被使用；见 [`coordinator-design.md`](docs/design/coordinator-design.md) |
| etcd session/lease TTL 硬编码 | `[coordinator] session-ttl` 配置项对 EtcdCoordinator 不生效 | 同上设计文档 |
| Oracle / MongoDB 的 `Schemas()` 仍是空存根 | 这两个 connector 无法通过 `Schemas()` 拿到已缓存的表结构；MySQL/SQL Server/MariaDB/PostgreSQL 已委托给各自的 schema 缓存 | `internal/source/{oracle,mongodb}/connector.go` |

### 其他待办

| 优先级 | 事项 | 设计文档 |
|--------|------|---------|
| P1 | 表级独立生命周期 S3 全量中转路径实现（大表 snapshot 本地磁盘不够用） | [`table-lifecycle-design.md`](docs/design/table-lifecycle-design.md) §5.3 |
| P1 | TemporalConverter 时区 UTC 归一化 | [`oracle-dml-parser-design.md`](docs/design/oracle-dml-parser-design.md) §11 |
| P2 | 测试覆盖率：15-24% → 目标 60%+ | [`test-strategy-design.md`](docs/design/test-strategy-design.md) |
| P2 | 废弃 `schema_cache.go`（确认所有路径切到 `internal/schema.Tables` 后删除） | — |

## 开发

```bash
# 安装依赖
make deps

# 格式化代码
make fmt

# 代码检查
make lint

# 运行测试
make test
```

## License

MIT
