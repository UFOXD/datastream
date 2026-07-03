# DataStream Deployment Guide

This guide covers deploying DataStream in various environments.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick Start with Docker](#quick-start-with-docker)
3. [Docker Compose Deployment](#docker-compose-deployment)
4. [Kubernetes Deployment](#kubernetes-deployment)
5. [Configuration](#configuration)
6. [Monitoring](#monitoring)
7. [High Availability](#high-availability)
8. [Operations](#operations)
9. [Troubleshooting](#troubleshooting)

## Prerequisites

- Go 1.21+ (for building from source)
- Docker 20.10+
- Kubernetes 1.20+ (for K8s deployment)
- etcd 3.5+ (for distributed mode)
- MySQL 8.0+ or PostgreSQL 15+ (as source)
- Kafka 2.8+ or MySQL 8.0+ (as sink)

## Quick Start with Docker

### Build the Image

```bash
# Build from source
docker build -t datastream:latest .

# Or use the pre-built image
docker pull ghcr.io/ufoxd/datastream:latest
```

### Run Single Instance

```bash
docker run -d \
  --name datastream \
  -p 8300:8300 \
  -v $(pwd)/configs:/app/configs \
  datastream:latest \
  --config /app/configs/datastream.toml
```

## Docker Compose Deployment

For production-like deployments with MySQL, Kafka, and etcd:

```yaml
# docker-compose.yml
version: '3.8'

services:
  datastream:
    image: datastream:latest
    container_name: datastream
    depends_on:
      - etcd
      - mysql
    ports:
      - "8300:8300"
    volumes:
      - ./configs:/app/configs
    command: --config /app/configs/datastream.toml
    environment:
      - DATASTREAM_COORDINATOR_ENDPOINTS=etcd:2379
    restart: unless-stopped

  etcd:
    image: bitnami/etcd:3.5
    container_name: datastream-etcd
    environment:
      - ALLOW_NONE_AUTHENTICATION=yes
    ports:
      - "2379:2379"
    volumes:
      - etcd_data:/bitnami/etcd

  mysql:
    image: mysql:8.0
    container_name: datastream-mysql
    environment:
      - MYSQL_ROOT_PASSWORD=datastream
      - MYSQL_DATABASE=datastream
    ports:
      - "3306:3306"
    command: --server-id=1 --log-bin=mysql-bin --binlog-format=ROW

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: datastream-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      - KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: datastream-zookeeper
    environment:
      - ZOOKEEPER_CLIENT_PORT=2181

volumes:
  etcd_data:
```

Start the stack:

```bash
docker-compose up -d
```

## Kubernetes Deployment

### Namespace and ConfigMap

```yaml
# k8s/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: datastream
```

```yaml
# k8s/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: datastream-config
  namespace: datastream
data:
  datastream.toml: |
    name = "datastream"
    
    [server]
    addr = ":8300"
    http-addr = ":8300"
    data-dir = "/data"
    
    [log]
    level = "info"
    file = "/var/log/datastream/datastream.log"
    
    [coordinator]
    type = "etcd"
    endpoints = ["etcd:2379"]
    
    [coordinator.etcd]
    endpoints = ["etcd:2379"]
    dial-timeout = 5
```

### Secret for Credentials

```yaml
# k8s/secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: datastream-secret
  namespace: datastream
type: Opaque
stringData:
  mysql-password: "your-mysql-password"
  kafka-password: "your-kafka-password"
```

### Deployment

```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: datastream
  namespace: datastream
spec:
  replicas: 3
  selector:
    matchLabels:
      app: datastream
  template:
    metadata:
      labels:
        app: datastream
    spec:
      containers:
      - name: datastream
        image: datastream:latest
        ports:
        - containerPort: 8300
        volumeMounts:
        - name: config
          mountPath: /app/configs
          readOnly: true
        - name: data
          mountPath: /data
        resources:
          requests:
            cpu: 500m
            memory: 512Mi
          limits:
            cpu: 2000m
            memory: 2Gi
        livenessProbe:
          httpGet:
            path: /health
            port: 8300
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 8300
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: config
        configMap:
          name: datastream-config
      - name: data
        emptyDir: {}
```

### Service

```yaml
# k8s/service.yaml
apiVersion: v1
kind: Service
metadata:
  name: datastream
  namespace: datastream
spec:
  selector:
    app: datastream
  ports:
  - port: 8300
    targetPort: 8300
  type: ClusterIP
```

### Deploy to Kubernetes

```bash
kubectl apply -f k8s/
```

## Configuration

### Minimal Configuration

```toml
# datastream.toml
name = "datastream"
cluster = "default"        # value of the 'cluster' label on all metrics

[server]
addr = ":8300"
data-dir = "./data"

[log]
level = "info"

[coordinator]
type = "memory"  # Use "etcd" for production

[metrics]
enabled = true              # set false to disable pull-mode gauges
scrape-interval = "5s"      # how often StatsCollector polls connectors
stats-timeout = "1s"        # per-connector Stats() timeout
```

### Production Configuration

```toml
name = "datastream"
cluster = "prod-east"        # appears in every metric's 'cluster' label

[server]
addr = ":8300"
http-addr = ":8300"
advertise-addr = "datastream-0.datastream:8300"
data-dir = "/data"
gc-ttl = 86400
read-timeout = 30
write-timeout = 30
idle-timeout = 120

[metrics]
enabled = true
scrape-interval = "5s"
stats-timeout = "1s"

[log]
level = "info"
file = "/var/log/datastream/datastream.log"
max-size = 512
max-days = 7
max-backups = 5

[coordinator]
type = "etcd"
session-ttl = 15
election-timeout = 5000

[coordinator.etcd]
endpoints = ["etcd-0.etcd:2379", "etcd-1.etcd:2379", "etcd-2.etcd:2379"]
dial-timeout = 5
username = ""
password = ""

[pipeline.cache]
max-size = "80%"        # percentage of disk, or fixed size e.g. "100GB"
sync = "batch"          # none | batch | every — see Configuration below

[security]
insecure = true
# For TLS:
# ssl-ca = "/etc/datastream/ca.pem"
# ssl-cert = "/etc/datastream/cert.pem"
# ssl-key = "/etc/datastream/key.pem"
```

### Environment Variables

Override any config value with environment variables:

```bash
export DATASTREAM_NAME=my-datastream
export DATASTREAM_CLUSTER=prod-east
export DATASTREAM_SERVER_ADDR=:8301
export DATASTREAM_LOG_LEVEL=debug
export DATASTREAM_COORDINATOR_TYPE=etcd
export DATASTREAM_COORDINATOR_ENDPOINTS=etcd1:2379,etcd2:2379
```

## Monitoring

### Prometheus Metrics

DataStream exposes Prometheus metrics at `/metrics`:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'datastream'
    static_configs:
      - targets: ['datastream:8300']
```

### Key Metrics

All metrics share the `datastream_` prefix and a `cluster` label whose value
comes from the `cluster` config field (see [Configuration](#configuration)).

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `datastream_task_total` | Gauge | cluster, status | Cluster-level task count by state |
| `datastream_task_state` | Gauge | cluster, task, state | Per-task current state (0/1) |
| `datastream_task_events_total` | Counter | cluster, task, type, result | Events processed; result=success/failed |
| `datastream_task_events_bytes` | Counter | cluster, task | Total bytes processed (estimate) |
| `datastream_task_latency_seconds` | Histogram | cluster, task | End-to-end event latency |
| `datastream_source_lag_seconds` | Gauge | cluster, task, source | CDC lag (now − event_time) |
| `datastream_source_last_event_seconds` | Gauge | cluster, task, source | Unix timestamp of last observed event |
| `datastream_source_snapshot_progress` | Gauge | cluster, task | Snapshot progress 0-100% |
| `datastream_snapshot_tables_total` | Gauge | cluster, task | Tables to snapshot |
| `datastream_snapshot_tables_remaining` | Gauge | cluster, task | Tables not yet snapshotted |
| `datastream_sink_write_latency_seconds` | Histogram | cluster, task, sink | Sink write latency |
| `datastream_sink_write_errors_total` | Counter | cluster, task, sink, error_type | Errors classified retriable/non_retriable |
| `datastream_pipeline_queue_size` | Gauge | cluster, task, stage | Current queue depth |
| `datastream_pipeline_queue_capacity` | Gauge | cluster, task, stage | Max queue depth |
| `datastream_connector_connected` | Gauge | cluster, task, role, type | Connection health 0/1 |

For the complete catalogue, label semantics, recommended PromQL queries, and
known caveats (e.g. `result=failed` undercounting under sink-internal retry),
see [`docs/operations/metrics.md`](../operations/metrics.md).

### Recommended Alerts

```yaml
- alert: DataStreamSourceLagHigh
  expr: datastream_source_lag_seconds > 60
  for: 5m
- alert: DataStreamSinkErrorsRising
  expr: sum by (task) (rate(datastream_sink_write_errors_total[5m])) > 0.1
  for: 10m
- alert: DataStreamConnectorDown
  expr: datastream_connector_connected == 0
  for: 2m
```

### Grafana Dashboard

Import the reference dashboard from
[`deployments/grafana/datastream-dashboard.json`](../../deployments/grafana/datastream-dashboard.json).
It includes 6 panels: throughput by task/result, sink-write p99 latency,
source lag, connector connectivity, error rate by retriable/non_retriable,
and pipeline queue usage.

## High Availability

### Multi-Node Cluster

For HA, deploy at least 3 nodes with etcd:

```bash
# Node 1
datastream --config node1.toml

# Node 2
datastream --config node2.toml

# Node 3
datastream --config node3.toml
```

Each node should have unique `advertise-addr` but same etcd endpoints.

### Task-Level Leader Election

Unlike a single-leader model, `ClusterManager` (`internal/pipeline/cluster.go`)
elects a **leader per task**, not one leader for the whole cluster. Any node
can be the leader for one task and a follower for another — task ownership
is distributed across all alive nodes via `pickLeastLoaded`, not
concentrated on a single elected node.

Timing parameters (fixed, not yet configurable):

| Parameter | Value | Purpose |
|---|---|---|
| `NodeHeartbeatInterval` | 10s | How often a node reports liveness |
| `NodeExpiryThreshold` | 30s | No heartbeat past this → node considered dead |
| `RebalanceInterval` | 30s | How often the elected cluster leader scans for unassigned tasks |
| `MaxTasksPerNode` | 10 | Soft cap used by `pickLeastLoaded` |

### Failover

If a node misses heartbeats past `NodeExpiryThreshold` (30s), the cluster
leader's `rebalanceCluster` loop (runs every `RebalanceInterval`) detects it
and reassigns its tasks to the least-loaded alive node.

⚠️ **Known limitation**: the logic that determines whether a task is
already owned by a live node (`cluster.go:265-275`) is incomplete — the
computed `leaderKey` is discarded (`_ = leaderKey`) instead of being used to
check current ownership. Treat automatic failover as unverified until this
is fixed; see `MEMORY.md` for tracking.

## Operations

### Create a Task

```bash
# Using CLI
datastream-ctl task create mysql-to-kafka "MySQL to Kafka Sync" \
  --source mysql://user:pass@mysql:3306/db \
  --sink kafka://kafka:9092/topic

# Using API
curl -X POST http://localhost:8300/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "id": "mysql-to-kafka",
    "name": "MySQL to Kafka Sync",
    "config": {
      "sourceType": "mysql",
      "sinkType": "kafka",
      "batchSize": 1000
    }
  }'
```

### Start a Task

```bash
datastream-ctl task start mysql-to-kafka
```

### Check Task Status

```bash
datastream-ctl task get mysql-to-kafka
```

### Manage Sync Tables

Tables can be added, paused, resumed, or removed at runtime without
restarting the task:

```bash
# Add tables to the sync scope
datastream-ctl tables add mydb.users mydb.orders

# List currently-synced tables
datastream-ctl tables list

# Pause / resume a single table
datastream-ctl tables pause mydb.users
datastream-ctl tables resume mydb.users

# Remove a table from sync
datastream-ctl tables remove mydb.orders
```

The HTTP equivalents are documented in [`docs/api/openapi.yaml`](../api/openapi.yaml).

### Graceful Shutdown

DataStream handles SIGINT and SIGTERM for graceful shutdown:

```bash
# Send shutdown signal
kill -SIGTERM <pid>

# Or via Docker
docker stop datastream
```

## Troubleshooting

### Common Issues

1. **Connection refused to etcd**
   - Check etcd is running: `etcdctl endpoint health`
   - Verify endpoints in config

2. **MySQL binlog not enabled**
   - Ensure `log-bin` and `binlog-format=ROW` in MySQL config

3. **Kafka connection timeout**
   - Verify `advertised.listeners` in Kafka config
   - Check network connectivity

### Logs

```bash
# View logs
kubectl logs -f deployment/datastream -n datastream

# Or with Docker
docker logs -f datastream
```

### Health Check

```bash
curl http://localhost:8300/health
```
