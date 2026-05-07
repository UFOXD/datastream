# DataStream

DataStream 是一个基于 Go 语言实现的变更数据捕获（CDC）平台。

## 特性

- 多数据源支持：MySQL、PostgreSQL、MongoDB、Oracle、SQL Server、MariaDB
- 多目标支持：Debezium Sink + Kafka（可选）
- 集群模式：单节点/多节点，任务调度，故障转移
- 可插拔架构：协调后端、Connector、转换器均可扩展

## 快速开始

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
├── pkg/                    # 公共库
├── internal/               # 内部实现
├── configs/                # 配置文件
├── tests/                  # 测试
├── docs/                   # 文档
└── proto/                  # Protobuf 定义
```

## 文档

详细设计文档请参阅 [docs/design/](docs/design/)。

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
