# DataStream 用户手册

本文档面向最终用户和运维人员，提供从配置到运维的端到端指南。

> **快速上手**：5 分钟跑通最小示例请看 [快速开始](quickstart.md)。
> **生产部署**：Docker / K8s 部署请看 [部署指南](deployment/README.md)。
> **REST API**：请看 [`docs/api/openapi.yaml`](api/openapi.yaml)。

---

## 目录

1. [概念速览](#1-概念速览)
2. [支持的连接器](#2-支持的连接器)
3. [配置参考](#3-配置参考)
4. [任务管理](#4-任务管理)
5. [动态表管理](#5-动态表管理)
6. [典型场景](#6-典型场景)
7. [运维与监控](#7-运维与监控)
8. [故障排查](#8-故障排查)
9. [限制与已知问题](#9-限制与已知问题)

---

## 1. 概念速览

| 术语 | 含义 |
|------|------|
| Task | 一条独立的同步任务，包含一个 Source 和一个或多个 Sink |
| Source / Sink | 数据来源 / 目的地连接器（详见下一节） |
| Pipeline | 任务运行时的数据通路（消费 → 过滤/转换/路由 → 写入） |
| Position | 数据位点（MySQL binlog file:pos 或 GTID、PG LSN、Oracle SCN、Mongo resume token、SQL Server LSN） |
| Snapshot | 全量阶段，先存量导入再切到增量 |
| Cluster | 一组 DataStream 实例的逻辑分组，作为 Prometheus 指标的 `cluster` label |

DataStream 默认每个实例独立运行；启用 etcd coordinator 后多实例组成一个集群，支持 leader 选举与任务漂移。

---

## 2. 支持的连接器

### Source（6 种）

| 连接器 | 机制 | 位点格式 | 备注 |
|--------|------|---------|------|
| `mysql` | Binlog 流式订阅 | `binlog-file:pos`（GTID 模式待支持） | 需要 `REPLICATION SLAVE` / `REPLICATION CLIENT` 权限，binlog-format=ROW |
| `mariadb` | Binlog 流式订阅 | 同 MySQL | 基于 MySQL connector |
| `postgres` | 逻辑复制 | LSN | 需要 `wal_level=logical`、replication slot、publication |
| `mongodb` | Change Stream | Resume token | 需要副本集或分片集群 |
| `oracle` | LogMiner | SCN | 需要 `LOGMINING` 权限,redo log 可读 |
| `sqlserver` | CDC | LSN hex | 需要 `EXEC sys.sp_cdc_enable_db` 启用 CDC |

### Sink（6 种）

| 连接器 | 写入模式 | 备注 |
|--------|---------|------|
| `mysql` | 批量 INSERT/UPDATE/DELETE | 支持 DDL |
| `postgres` | COPY + UPSERT (`ON CONFLICT`) | 高吞吐 |
| `mongodb` | Bulk write + upsert | 支持多种 write strategy |
| `kafka` | Producer，可选压缩 | 可作为下游消费总线 |
| `elasticsearch` | Bulk API | 自定义索引名 + 文档映射 |
| `redis` | Pipeline write | hash / json / string 三种格式 |

---

## 3. 配置参考

DataStream 配置使用 TOML 格式，默认路径 `configs/datastream.toml`。所有字段
都可以通过环境变量 `DATASTREAM_<UPPERCASE_PATH>` 覆盖。

### 顶层

```toml
name    = "my-datastream"   # 实例名（仅用于日志）
cluster = "prod-east"       # 集群标识；作为所有 Prometheus 指标的 cluster label
```

### 服务

```toml
[server]
addr          = ":8300"     # API 监听地址
data-dir      = "/var/lib/datastream"
gc-ttl        = 86400       # 任务历史保留秒数
read-timeout  = 30          # HTTP 读取超时（秒）
write-timeout = 30
idle-timeout  = 120
```

### 协调器（多实例集群）

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

### 管道缓存（2026-06-29 新增）

```toml
[pipeline.cache]
max-size = "80%"      # binlog 缓存磁盘占用上限：百分比或固定值，如 "100GB", "500MB"
sync     = "batch"    # fsync 策略：none（交给 OS）/ batch（每批 fsync）/ every（每条事件 fsync）
```

⚠️ 注意与下方 `[log] max-size` 区分：此项控制 `internal/cache`（binlog 缓存）的**磁盘占用上限**，
单位可以是百分比或绝对大小；`[log] max-size` 控制的是**日志单文件轮转大小**，单位固定为 MB，
两者互不影响，命名相同纯属巧合。

环境变量覆盖：`DATASTREAM_PIPELINE_CACHE_MAX_SIZE` / `DATASTREAM_PIPELINE_CACHE_SYNC`

### 日志

```toml
[log]
level   = "info"      # debug | info | warn | error
format  = "console"   # console | json
output  = "stdout"    # 或文件路径
file    = ""          # 当 output != stdout 时使用
max-size = 512        # 单文件 MB 上限
max-days = 7
```

### 监控

```toml
[metrics]
enabled         = true       # false 时仍开放 /metrics 端点但停发 pull-mode gauge
scrape-interval = "5s"       # StatsCollector 轮询连接器 Stats() 的间隔
stats-timeout   = "1s"       # 单次 Stats() 调用超时
```

### 安全

```toml
[security]
insecure  = false
ssl-ca    = "/etc/datastream/ca.pem"
ssl-cert  = "/etc/datastream/cert.pem"
ssl-key   = "/etc/datastream/key.pem"
```

任务级配置（source / sink / pipeline 参数）通过 REST API 或 CLI 在运行时
创建任务时传入，不写在主配置里。

---

## 4. 任务管理

### CLI

```bash
# 列出全部任务
datastream-ctl task list

# 创建任务（任务定义文件描述 source / sink / pipeline）
datastream-ctl task create my-task "My Sync" --config my-task.toml

# 启动 / 停止 / 暂停 / 恢复
datastream-ctl task start  my-task
datastream-ctl task stop   my-task
datastream-ctl task pause  my-task
datastream-ctl task resume my-task

# 查看详情（含状态、位点、统计）
datastream-ctl task get my-task

# 删除任务
datastream-ctl task delete my-task
```

### REST API 等价物

| 操作 | 方法 + 路径 |
|------|------------|
| 列表 | `GET /api/v1/tasks` |
| 创建 | `POST /api/v1/tasks` |
| 详情 | `GET /api/v1/tasks/{id}` |
| 删除 | `DELETE /api/v1/tasks/{id}` |
| 启动 | `POST /api/v1/tasks/{id}/start` |
| 停止 | `POST /api/v1/tasks/{id}/stop` |
| 暂停 | `POST /api/v1/tasks/{id}/pause` |
| 恢复 | `POST /api/v1/tasks/{id}/resume` |
| 取位点 | `GET /api/v1/tasks/{id}/position` |
| 设位点 | `PUT /api/v1/tasks/{id}/position` |

完整 schema 见 [`docs/api/openapi.yaml`](api/openapi.yaml)。

### 任务生命周期

```
created → starting → running ↔ paused
                       ↓
                    stopping → stopped
                       ↓
                     error  (可重启)
```

- `pause` 不丢位点；`resume` 从原位点继续
- `stop` 后位点持久化到 coordinator（如果启用），下次 `start` 自动续传
- 修改位点：先 `stop` 再 `PUT /position`，最后 `start`

---

## 5. 动态表管理

DataStream 支持运行时增减同步表，**无需重启任务**。

### CLI

```bash
# 添加表
datastream-ctl tables add mydb.users mydb.orders

# 列出当前同步的表（可按数据库过滤）
datastream-ctl tables list
datastream-ctl tables list --database mydb

# 查看单表状态
datastream-ctl tables get mydb.users

# 暂停 / 恢复单表
datastream-ctl tables pause  mydb.users
datastream-ctl tables resume mydb.users

# 移除表
datastream-ctl tables remove mydb.orders
```

### 通配符模式（DatabaseDiscovery）

如果任务的 `SyncScope` 使用了通配符（如 `mydb.*` 或 `*.orders_*`），
DataStream 会**自动发现**新建的匹配表并加入同步。无需手动 `tables add`。

> 通配符匹配支持 `*`（零或多个字符）和 `?`（单字符），算法基于动态规划。

---

## 6. 典型场景

### 6.1 MySQL → Kafka 全量+增量

```toml
[source]
type       = "mysql"
host       = "mysql.prod"
port       = 3306
user       = "datastream"
password   = "..."
server-id  = 1001
include-databases = ["orders_db"]

[source.snapshot]
mode    = "initial"      # never | initial | always
threads = 4

[sink]
type         = "kafka"
brokers      = ["kafka1:9092", "kafka2:9092"]
topic-prefix = "ds."
compression  = "snappy"

[pipeline]
batch-size  = 1000
parallelism = 4
```

### 6.2 PostgreSQL → PostgreSQL（异构同步）

源端需要 `wal_level=logical`：

```sql
ALTER SYSTEM SET wal_level = 'logical';
SELECT pg_create_logical_replication_slot('datastream_slot', 'pgoutput');
CREATE PUBLICATION datastream_pub FOR ALL TABLES;
```

任务配置：

```toml
[source]
type           = "postgres"
host           = "pg-src.prod"
slot-name      = "datastream_slot"
publication    = "datastream_pub"

[sink]
type            = "postgres"
host            = "pg-dst.prod"
on-conflict     = "update"   # 等价于 ON CONFLICT DO UPDATE SET ...
```

### 6.3 MongoDB → Elasticsearch（搜索同步）

```toml
[source]
type           = "mongodb"
uri            = "mongodb://mongo-rs/?replicaSet=rs0"
include-databases = ["catalog"]

[sink]
type        = "elasticsearch"
endpoints   = ["https://es.prod:9200"]
index       = "catalog_{database}_{collection}"
doc-id      = "{_id}"
```

### 6.4 多 Sink 广播

一个 source 同时写多个 sink：

```toml
[source]
type = "mysql"
# ...

[[sinks]]
type = "kafka"
# ...

[[sinks]]
type = "elasticsearch"
# ...
```

Pipeline 的 dispatcher 默认 `broadcast`（每个 sink 收到全部事件）。
设置 `[pipeline.dispatcher].type = "hash"` 可按字段做分区路由。

---

## 7. 运维与监控

### Prometheus 指标

DataStream 在 `/metrics` 端点暴露 Prometheus 指标。完整指标目录请看
[`docs/operations/metrics.md`](operations/metrics.md)。

**最关键的 4 个告警信号**：

| 信号 | PromQL | 含义 |
|------|--------|------|
| 同步延迟过高 | `datastream_source_lag_seconds > 60` | CDC 落后实时太多 |
| 数据停止流动 | `time() - datastream_source_last_event_seconds > 300` | 5 分钟没新事件 |
| 写入错误攀升 | `rate(datastream_sink_write_errors_total[5m]) > 0.1` | 下游写错误率 > 0.1/s |
| 连接器掉线 | `datastream_connector_connected == 0` | source 或 sink 断连 |

### Grafana 面板

`deployments/grafana/datastream-dashboard.json` 提供 6 面板基础看板：吞吐
（按 task/result）、p99 延迟、source lag、连接健康、错误率（按 error_type）、
队列使用率。直接 Grafana → Import 即可。

### 日志位置

- 默认输出到 stdout
- `[log].output = "/var/log/datastream/datastream.log"` 切到文件，按
  `max-size`（MB）和 `max-days` 自动轮转

---

## 8. 故障排查

### 启动失败

| 症状 | 原因 | 解决 |
|------|------|------|
| `connection refused to etcd` | 协调器无法连接 | 检查 endpoints；`etcdctl endpoint health` 验证 |
| `binlog format not ROW` | MySQL 配置不对 | `SET GLOBAL binlog_format='ROW';` 永久改 my.cnf |
| `replication slot does not exist` | PG 复制槽未建 | 见 [6.2 场景](#62-postgresql--postgresql异构同步) |
| `CDC not enabled` | SQL Server 没开 CDC | `EXEC sys.sp_cdc_enable_db; sp_cdc_enable_table @source_schema='dbo', @source_name='users', ...` |

### 运行期问题

| 症状 | 排查路径 |
|------|---------|
| 启动后没有事件 | (1) `SHOW MASTER STATUS` 看 MySQL 是否真的有 binlog；(2) `include-databases/tables` 是否匹配；(3) `datastream_source_last_event_seconds` 是否在涨 |
| `source_lag_seconds` 持续上涨 | 下游 sink 慢了。看 `sink_write_latency_seconds` p99；调高 `[pipeline].batch-size` 或 `parallelism` |
| `result=failed` 计数飙升 | 看日志找具体错误；检查 `sink_write_errors_total{error_type}`，`non_retriable` 通常意味着 schema 不兼容或权限问题 |
| 任务 stuck 在 `stopping` 状态 | sink Flush 超时；查看日志，必要时 `task delete` 强制清理 |
| Pipeline 队列使用率持续 100% | 看 `pipeline_queue_size / pipeline_queue_capacity`，调大 `[pipeline.buffer].size` |

### 日志关键字

```bash
# 找错误事件
grep ERROR /var/log/datastream/datastream.log

# 看 sink 写入失败
grep "failed to write to sink" /var/log/datastream/datastream.log

# 看 metrics 注册情况（启动时打印）
grep "task metrics registered" /var/log/datastream/datastream.log
```

---

## 9. 限制与已知问题

- **`result=failed` 计数欠计**：当 sink 内部已有重试（如 MySQL driver 自动
  重连）时，只有逃出 sink 的最终失败才会计入。精确计数需要后续"重试架构
  统一"任务完成。
- **`result=success` ≠ "已写入 sink"**：消费点计数表示事件已流过 Pipeline,
  实际写入由 sink decorator 单独计 `result=failed`。
- **MySQL GTID 位点**：当前 Position 只渲染 `binlog-file:pos`。GTID 兼容
  实现在路上，届时 Position 字符串会自动切换格式。
- **AsyncConnector 监控不完整**：异步 sink 的成功事件计数依赖回调通路，
  当前只统计 enqueue 时间。
- **Stats() 是可选接口**：连接器实现 `StatsProvider` 接口才提供 pull-mode
  gauge。当前所有 12 个连接器都已实现最小版本（`Connected` + `Position`），
  snapshot/lag 字段随连接器内部跟踪逐步增强。
- **位点存储后端**：默认使用 memory，重启丢失。生产环境务必启用 etcd 协调器。

---

## 相关文档

- [快速开始](quickstart.md) — 5 分钟跑通示例
- [部署指南](deployment/README.md) — Docker / K8s 部署
- [REST API 规范](api/openapi.yaml) — OpenAPI 3.0
- [运维指标参考](operations/metrics.md) — Prometheus 指标完整目录
- [设计文档](design/) — 架构与模块设计
- [模块地图](module-map.md) — 代码组织结构
