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
