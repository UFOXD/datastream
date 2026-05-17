# DataStream 快速开始

本文档帮助你在 5 分钟内完成 DataStream 的本地构建、配置和运行，搭建一条最小可用的 CDC 数据管道。

## 前置要求

- Go 1.21+
- Docker 20.10+（可选，用于快速启动 MySQL/Kafka）
- 一个可用的 Source 数据库（MySQL/PostgreSQL 等）
- 一个可用的 Sink（Kafka 或目标数据库）

## 1. 构建

```bash
# 克隆仓库
git clone https://github.com/UFOXD/datastream.git
cd datastream

# 安装依赖
make deps

# 构建主程序和控制工具
make build
make build-ctl
```

构建产物：
- `bin/datastream` — 主服务
- `bin/datastream-ctl` — 控制工具

查看版本：
```bash
./bin/datastream -version
```

## 2. 准备 Source 与 Sink

### MySQL Source（示例）

启动一个开启 binlog 的 MySQL：

```bash
docker run -d --name ds-mysql \
  -e MYSQL_ROOT_PASSWORD=datastream \
  -e MYSQL_DATABASE=demo \
  -p 3306:3306 \
  mysql:8.0 \
  --server-id=1 --log-bin=mysql-bin --binlog-format=ROW
```

创建测试表：
```sql
CREATE TABLE demo.users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(64),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Kafka Sink（示例）

```bash
docker run -d --name ds-kafka \
  -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=1 \
  -e KAFKA_CFG_PROCESS_ROLES=broker,controller \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  bitnami/kafka:3.6
```

## 3. 编写配置

编辑 `configs/datastream.toml`：

```toml
name = "datastream-quickstart"

[log]
level = "info"
format = "console"
output = "stdout"

[source]
type = "mysql"
host = "localhost"
port = 3306
user = "root"
password = "datastream"
server-id = 1001
include-databases = ["demo"]

[sink]
type = "kafka"
brokers = ["localhost:9092"]
topic-prefix = "ds."

[pipeline]
batch-size = 1000
parallelism = 4
```

> 完整配置项参见 [docs/design/](design/) 中的设计文档。

## 4. 运行

```bash
./bin/datastream -config configs/datastream.toml
```

启动后默认监听 `8300` 端口提供 REST API（详见 [openapi.yaml](api/openapi.yaml)）。

健康检查：
```bash
curl http://localhost:8300/health
```

## 5. 验证数据流

在 MySQL 中执行：
```sql
INSERT INTO demo.users(name) VALUES ('alice'), ('bob');
UPDATE demo.users SET name='alice2' WHERE id=1;
```

订阅 Kafka topic 查看变更事件：
```bash
docker exec -it ds-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ds.demo.users --from-beginning
```

## 6. 控制工具

```bash
# 列出任务
./bin/datastream-ctl task list

# 查看任务状态
./bin/datastream-ctl task status <task-id>
```

## 下一步

- [用户手册](user-guide.md) — 端到端使用指南（配置、CLI、场景、监控、故障排查）
- [部署指南](deployment/README.md) — Docker Compose / Kubernetes 生产部署
- [运维指标](operations/metrics.md) — Prometheus 指标参考
- [API 文档](api/openapi.yaml) — REST API 规范
- [设计文档](design/) — 架构与模块设计
- [模块地图](module-map.md) — 代码组织结构

## 常见问题

**Q: MySQL 报 `Access denied` 或 `replication slave` 权限不足？**
A: 确保使用的账号拥有 `REPLICATION SLAVE`、`REPLICATION CLIENT` 权限。

**Q: 启动后没有事件输出？**
A: 检查 `include-databases` / `include-tables` 是否匹配，以及 binlog 是否真正写入（`SHOW MASTER STATUS`）。

**Q: 如何切换到 PostgreSQL Source？**
A: 将 `[source].type` 改为 `postgres`，并确保数据库开启 `wal_level=logical` 并创建 publication/replication slot。
