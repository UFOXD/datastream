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

## 当前卡点与重要待办

### 卡点：Schema History + 缓冲文件完整性

表级独立生命周期的核心机制已实现（状态机、缓冲层、调度器、API），但在数据一致性上有两个阻塞问题尚未解决：

1. **缓冲文件事务完整性** — 大事务跨多张表的缓冲文件写了一半崩溃后，重启从 GTID 重新拉取会导致部分表文件出现重复事件。需要在事务粒度上保证写入原子性（全局 WAL 或 committed_gtids 方案待决策）。

2. **Schema History 机制缺失** — DML 事件解析依赖表结构定义，当前从源库 `INFORMATION_SCHEMA` 实时查询存在时序问题（DDL 后查到的是新结构，但 binlog 中的 DML 对应旧结构）。需要自己维护一条完整的表结构变更历史链，类似 Debezium 的 Schema History。

3. **Parser ALTER 解析不完整** — 4 个 DDL Parser 的 ALTER TABLE 产出缺少列类型、Nullable、Default 值等关键信息。MySQL 不处理 `CHANGE` 语法和 `AFTER/FIRST` 列位置变更。Oracle 的 ALTER 用文本匹配而非 ANTLR AST。

详细设计与待决策事项：[`docs/design/schema-history-and-cache-integrity-design.md`](docs/design/schema-history-and-cache-integrity-design.md)

### 其他待办

| 优先级 | 事项 | 设计文档 |
|--------|------|---------|
| P1 | 表级独立生命周期 S3 中转路径实现 | [`table-lifecycle-design.md`](docs/design/table-lifecycle-design.md) §5.3 |
| P1 | TemporalConverter 时区 UTC 归一化 | [`oracle-dml-parser-design.md`](docs/design/oracle-dml-parser-design.md) §11 |
| P2 | Source connector Schemas() 从 stub 改为委托 schemaCache | Reviewer 发现 S2 |
| P2 | 测试覆盖率：source connector 15-24%，目标 60%+ | [`test-strategy-design.md`](docs/design/test-strategy-design.md) |

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
