# Enterprise Connectors Guide

This document provides detailed configuration and usage instructions for DataStream's enterprise-grade connectors.

---

## Source Connectors

### SQL Server CDC Source

SQL Server Source uses Change Data Capture (CDC) to stream data changes.

#### Configuration

```toml
[[source]]
type = "sqlserver"
name = "sqlserver-source"

[source.connection]
host = "localhost"
port = 1433
user = "sa"
password = "your-password"
database = "your-database"

[source.properties]
poll_interval = 1000        # Poll interval in milliseconds
batch_size = 1000           # Max changes per poll
```

#### Prerequisites

1. Enable CDC on the database:
```sql
USE your_database;
EXEC sys.sp_cdc_enable_db;
```

2. Enable CDC on tables:
```sql
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'your_table',
    @role_name = NULL;
```

#### Position Tracking

- Uses LSN (Log Sequence Number) for position tracking
- Position is automatically saved to offset storage
- Supports resume from last position

---

### Oracle LogMiner Source

Oracle Source uses LogMiner to mine redo logs for change events.

#### Configuration

```toml
[[source]]
type = "oracle"
name = "oracle-source"

[source.connection]
host = "localhost"
port = 1521
user = "system"
password = "your-password"

[source.properties]
service_name = "XE"
mining_strategy = "continuous"  # "continuous" or "online"
poll_interval = 1000            # milliseconds
batch_size = 1000
schemas = ["SCHEMA1", "SCHEMA2"]
```

#### Mining Strategies

| Strategy | Description | Performance |
|----------|-------------|-------------|
| `continuous` | Automatically mines new logs | Best for real-time |
| `online` | Manual log management | Lower resource usage |

#### Position Tracking

- Uses SCN (System Change Number) for position tracking
- Supports resume from specific SCN
- Position persisted to offset storage

---

## Sink Connectors

### Elasticsearch Sink

Elasticsearch Sink writes change events to Elasticsearch using the Bulk API.

#### Configuration

```toml
[[sink]]
type = "elasticsearch"
name = "es-sink"

[sink.connection]
host = "localhost"
port = 9200

[sink.properties]
index_prefix = "datastream"
index_pattern = "{table}"          # Supports {database}, {table}, {schema}
doc_id_strategy = "primary_key"    # "primary_key" or "generate"
batch_size = 1000
flush_interval = 1000              # milliseconds
```

#### Index Naming

Index patterns support template variables:
- `{database}` - Source database name
- `{table}` - Source table name
- `{schema}` - Source schema name

Example: `index_pattern = "{database}_{table}"` → `inventory_orders`

#### Document ID Strategy

| Strategy | Description |
|----------|-------------|
| `primary_key` | Use primary key columns as document ID |
| `generate` | Let Elasticsearch generate IDs |

---

### Redis Sink

Redis Sink writes change events to Redis with multiple format options.

#### Configuration

```toml
[[sink]]
type = "redis"
name = "redis-sink"

[sink.connection]
host = "localhost"
port = 6379
password = ""
database = 0

[sink.properties]
format = "hash"              # "hash", "json", or "string"
key_prefix = "ds:"
key_pattern = "{database}:{table}:{id}"
ttl = 0                      # TTL in seconds, 0 = no expiry
batch_size = 1000
```

#### Format Options

| Format | Description | Use Case |
|--------|-------------|----------|
| `hash` | Store as Redis Hash (HSET) | Field-level access |
| `json` | Store as JSON string | Document storage |
| `string` | Store as plain string | Simple values |

#### Key Pattern

Key patterns support template variables:
- `{database}` - Source database name
- `{table}` - Source table name
- `{id}` - Primary key value

Example: `key_pattern = "{database}:{table}:{id}"` → `inventory:users:123`

---

## MongoDB Source/Sink

### MongoDB Source (Change Stream)

MongoDB Source uses Change Streams to capture real-time changes.

#### Configuration

```toml
[[source]]
type = "mongodb"
name = "mongo-source"

[source.connection]
uri = "mongodb://user:password@localhost:27017/?authSource=admin"
database = "your-database"

[source.properties]
batch_size = 1000
full_document = true          # Include full document in events
resume_token = ""             # Optional: resume from token
```

### MongoDB Sink (Bulk Write)

MongoDB Sink uses Bulk Write API for efficient writes.

#### Configuration

```toml
[[sink]]
type = "mongodb"
name = "mongo-sink"

[sink.connection]
uri = "mongodb://user:password@localhost:27017/?authSource=admin"
database = "your-database"

[sink.properties]
write_strategy = "upsert"     # "insert", "update", "upsert"
ordered = false               # Ordered bulk writes
batch_size = 1000
```

---

## Integration Test Configuration

To run integration tests, use the following environment variables:

```bash
# SQL Server
export SQLSERVER_HOST=localhost
export SQLSERVER_PORT=1433
export SQLSERVER_USER=sa
export SQLSERVER_PASSWORD="YourPassword123!"
export SQLSERVER_DATABASE=datastream_test

# Oracle
export ORACLE_HOST=localhost
export ORACLE_PORT=1521
export ORACLE_USER=system
export ORACLE_PASSWORD=oracle
export ORACLE_SERVICE_NAME=XE

# MongoDB
export MONGODB_HOST=localhost
export MONGODB_PORT=27017
export MONGODB_USER=datastream
export MONGODB_PASSWORD=datastream
export MONGODB_DATABASE=datastream_test

# Elasticsearch
export ELASTICSEARCH_HOST=localhost
export ELASTICSEARCH_PORT=9200

# Redis
export REDIS_HOST=localhost
export REDIS_PORT=6379
```

Run integration tests:
```bash
go test -tags=integration ./tests/integration/...
```

---

## Troubleshooting

### SQL Server CDC Issues

**Problem:** CDC tables not found
**Solution:** Ensure CDC is enabled on both database and tables:
```sql
-- Check CDC status
SELECT name, is_cdc_enabled FROM sys.databases;
SELECT * FROM cdc.change_tables;
```

### Oracle LogMiner Issues

**Problem:** Insufficient privileges
**Solution:** Grant required privileges:
```sql
GRANT EXECUTE ON DBMS_LOGMNR TO your_user;
GRANT SELECT ON V$LOGMNR_CONTENTS TO your_user;
GRANT SELECT ON V$DATABASE TO your_user;
```

### Elasticsearch Connection Issues

**Problem:** Connection refused
**Solution:** Check Elasticsearch is running and accessible:
```bash
curl http://localhost:9200/_cluster/health
```

### Redis Connection Issues

**Problem:** Authentication failed
**Solution:** Verify credentials and Redis configuration:
```bash
redis-cli -h localhost -p 6379 -a your_password ping
```

---

*Last Updated: 2026-05-11*
