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

[server]
addr = ":8300"
data-dir = "./data"

[log]
level = "info"

[coordinator]
type = "memory"  # Use "etcd" for production
```

### Production Configuration

```toml
name = "datastream"

[server]
addr = ":8300"
http-addr = ":8300"
advertise-addr = "datastream-0.datastream:8300"
data-dir = "/data"
gc-ttl = 86400
read-timeout = 30
write-timeout = 30
idle-timeout = 120

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

| Metric | Description |
|--------|-------------|
| `datastream_events_total` | Total events processed |
| `datastream_events_duration_seconds` | Event processing latency |
| `datastream_tasks_running` | Number of running tasks |
| `datastream_buffer_size` | Current buffer size |
| `datastream_errors_total` | Total errors encountered |

### Grafana Dashboard

Import the provided dashboard from `docs/dashboards/datastream.json`.

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

### Leadership Election

The coordinator uses etcd for leader election. Only the leader processes tasks; followers standby.

### Failover

If the leader fails, etcd elects a new leader automatically within 5-15 seconds.

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
