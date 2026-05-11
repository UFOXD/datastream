# Phase 9: Enterprise Database Connectors Design

> Version: 1.0
> Date: 2026-05-11
> Status: Design Complete

## Overview

Phase 9 adds support for enterprise databases and additional sinks:

| Connector | Type | Priority | Complexity |
|-----------|------|----------|------------|
| Elasticsearch | Sink | 1 (highest) | Low |
| Redis | Sink | 2 | Low |
| SQL Server | Source | 3 | Medium |
| Oracle | Source | 4 (lowest) | High |

Priority is ordered by increasing complexity - validate patterns with simpler connectors first.

## Design Principles

1. **Follow existing patterns**: Match MySQL/PostgreSQL/MongoDB connector architecture
2. **Interface compliance**: All connectors implement `source.Connector` or `sink.Connector`
3. **Independent schema management**: Use `TableSchemaCache` pattern from MySQL refactor
4. **Position tracking**: Integrate with `offset.Storage` for resume capability
5. **Error handling**: Retry with backoff, graceful degradation

---

## 1. Elasticsearch Sink

### 1.1 Architecture

```
ChangeEvent → ElasticsearchSink → Bulk API Request
                     ↓
              Document Mapping
              (table → index, row → document)
```

### 1.2 Package Structure

```
internal/sink/elasticsearch/
├── config.go        # Configuration struct
├── connector.go     # Connector implementation
├── indexer.go       # Bulk indexer with batching
├── mapper.go        # Event to document mapping
└── connector_test.go
```

### 1.3 Configuration

```go
package elasticsearch

type Config struct {
    // Connection
    URLs          []string        `toml:"urls"`
    Username      string          `toml:"username"`
    Password      string          `toml:"password"`
    APIKey        string          `toml:"api_key"`
    
    // Index settings
    IndexPrefix   string          `toml:"index_prefix"`
    IndexPattern  string          `toml:"index_pattern"`  // default: "{database}_{table}"
    
    // Bulk settings
    BatchSize     int             `toml:"batch_size"`     // default: 1000
    FlushInterval time.Duration   `toml:"flush_interval"` // default: 1s
    
    // Write settings
    RefreshPolicy string          `toml:"refresh_policy"` // "true", "wait_for", "false"
    RetryOnConflict int           `toml:"retry_on_conflict"` // default: 3
}

func DefaultConfig() *Config {
    return &Config{
        IndexPattern:    "{database}_{table}",
        BatchSize:       1000,
        FlushInterval:   time.Second,
        RefreshPolicy:   "false",
        RetryOnConflict: 3,
    }
}
```

### 1.4 Event to Document Mapping

| ChangeEvent.Type | ES Operation | ES Action |
|------------------|--------------|-----------|
| `EventTypeInsert` | `index` | Create or replace document |
| `EventTypeUpdate` | `update` | Update with `doc_as_upsert: true` |
| `EventTypeDelete` | `delete` | Remove document |

**Document ID Generation:**

```go
func (m *DocumentMapper) generateDocID(row event.RowData, pkColumns []string) string {
    var parts []string
    for _, col := range pkColumns {
        if val, ok := row.Fields[col]; ok {
            parts = append(parts, fmt.Sprintf("%v", val.Value))
        }
    }
    return strings.Join(parts, "_")
}
```

**Index Name Resolution:**

```go
func (m *DocumentMapper) resolveIndex(table event.TableInfo) string {
    indexName := m.config.IndexPattern
    indexName = strings.ReplaceAll(indexName, "{database}", strings.ToLower(table.Database))
    indexName = strings.ReplaceAll(indexName, "{table}", strings.ToLower(table.Table))
    if m.config.IndexPrefix != "" {
        indexName = m.config.IndexPrefix + indexName
    }
    return indexName
}
```

### 1.5 Bulk Indexer

```go
type BulkIndexer struct {
    client    *es.Client
    config    *Config
    buffer    []*BulkAction
    bufferMu  sync.Mutex
    stopCh    chan struct{}
    flushCh   chan struct{}
}

type BulkAction struct {
    Index string
    ID    string
    Op    string // "index", "update", "delete"
    Doc   map[string]interface{}
}

func (b *BulkIndexer) Add(action *BulkAction) error {
    b.bufferMu.Lock()
    b.buffer = append(b.buffer, action)
    shouldFlush := len(b.buffer) >= b.config.BatchSize
    b.bufferMu.Unlock()
    
    if shouldFlush {
        select {
        case b.flushCh <- struct{}{}:
        default:
        }
    }
    return nil
}

func (b *BulkIndexer) run() {
    ticker := time.NewTicker(b.config.FlushInterval)
    defer ticker.Stop()
    
    for {
        select {
        case <-b.stopCh:
            b.flush()
            return
        case <-ticker.C:
            b.flush()
        case <-b.flushCh:
            b.flush()
        }
    }
}
```

### 1.6 Error Handling

- **Connection errors**: Retry with exponential backoff (max 5 retries)
- **Bulk errors**: Log individual document failures, continue processing
- **Version conflicts**: Retry up to `RetryOnConflict` times

### 1.7 Dependencies

```go
import (
    "github.com/elastic/go-elasticsearch/v8"
)
```

---

## 2. Redis Sink

### 2.1 Architecture

```
ChangeEvent → RedisSink → Redis Pipeline
                   ↓
            Key Mapping
            (table → key pattern)
```

### 2.2 Package Structure

```
internal/sink/redis/
├── config.go        # Configuration struct
├── connector.go     # Connector implementation
├── writer.go        # Pipeline writer with batching
└── connector_test.go
```

### 2.3 Configuration

```go
package redis

type Config struct {
    // Connection
    Addr         string          `toml:"addr"`           // host:port
    Password     string          `toml:"password"`
    DB           int             `toml:"db"`
    
    // Write settings
    KeyPattern   string          `toml:"key_pattern"`    // default: "{database}:{table}:{pk}"
    TTL          time.Duration   `toml:"ttl"`            // 0 = no expiration
    BatchSize    int             `toml:"batch_size"`     // default: 1000
    FlushInterval time.Duration  `toml:"flush_interval"` // default: 1s
    
    // Data format
    Format       string          `toml:"format"`         // "hash", "json", "string"
}

func DefaultConfig() *Config {
    return &Config{
        Addr:          "localhost:6379",
        KeyPattern:    "{database}:{table}:{pk}",
        BatchSize:     1000,
        FlushInterval: time.Second,
        Format:        "hash",
    }
}
```

### 2.4 Event to Redis Command Mapping

| ChangeEvent.Type | Format | Redis Command |
|------------------|--------|---------------|
| `EventTypeInsert` | hash | `HSET key field1 val1 field2 val2 ...` |
| `EventTypeInsert` | json | `SET key json_value` |
| `EventTypeUpdate` | hash | `HSET key field1 val1 field2 val2 ...` |
| `EventTypeUpdate` | json | `SET key json_value` |
| `EventTypeDelete` | any | `DEL key` |

### 2.5 Key Generation

```go
func (w *PipelineWriter) generateKey(table event.TableInfo, row event.RowData, pkColumns []string) string {
    key := w.config.KeyPattern
    key = strings.ReplaceAll(key, "{database}", table.Database)
    key = strings.ReplaceAll(key, "{table}", table.Table)
    
    // Build primary key value
    var pkParts []string
    for _, col := range pkColumns {
        if val, ok := row.Fields[col]; ok {
            pkParts = append(pkParts, fmt.Sprintf("%v", val.Value))
        }
    }
    key = strings.ReplaceAll(key, "{pk}", strings.Join(pkParts, "_"))
    
    return key
}
```

### 2.6 Pipeline Writer

```go
type PipelineWriter struct {
    client    *redis.Client
    config    *Config
    pipe      redis.Pipeliner
    buffer    []*WriteCommand
    bufferMu  sync.Mutex
}

type WriteCommand struct {
    Key   string
    Op    string // "set", "hset", "del"
    Value interface{}
}

func (w *PipelineWriter) flush() error {
    w.bufferMu.Lock()
    cmds := w.buffer
    w.buffer = nil
    w.bufferMu.Unlock()
    
    if len(cmds) == 0 {
        return nil
    }
    
    pipe := w.client.Pipeline()
    
    for _, cmd := range cmds {
        switch cmd.Op {
        case "set":
            pipe.Set(context.Background(), cmd.Key, cmd.Value, w.config.TTL)
        case "hset":
            pipe.HSet(context.Background(), cmd.Key, cmd.Value)
            if w.config.TTL > 0 {
                pipe.Expire(context.Background(), cmd.Key, w.config.TTL)
            }
        case "del":
            pipe.Del(context.Background(), cmd.Key)
        }
    }
    
    _, err := pipe.Exec(context.Background())
    return err
}
```

### 2.7 Dependencies

```go
import (
    "github.com/redis/go-redis/v9"
)
```

---

## 3. SQL Server Source (CDC)

### 3.1 Architecture

```
SQL Server CDC Tables → CDCReader → ChangeEvent
         ↓
    Change Tables
    (cdc.dbo_table_CT)
```

### 3.2 Package Structure

```
internal/source/sqlserver/
├── config.go        # Configuration struct
├── connector.go     # Connector implementation
├── cdc_reader.go    # CDC table polling
├── schema_cache.go  # Table schema cache
└── connector_test.go
```

### 3.3 Configuration

```go
package sqlserver

type Config struct {
    // Connection
    Host         string          `toml:"host"`
    Port         int             `toml:"port"`          // default: 1433
    User         string          `toml:"user"`
    Password     string          `toml:"password"`
    Database     string          `toml:"database"`
    
    // CDC settings
    PollInterval time.Duration   `toml:"poll_interval"` // default: 1s
    BatchSize    int             `toml:"batch_size"`    // default: 1000
    
    // Tables
    Schemas      []string        `toml:"schemas"`       // default: ["dbo"]
    Tables       map[string]string `toml:"tables"`      // schema -> pattern
}

func DefaultConfig() *Config {
    return &Config{
        Port:         1433,
        PollInterval: time.Second,
        BatchSize:    1000,
        Schemas:      []string{"dbo"},
    }
}
```

### 3.4 CDC Enablement Requirements

```sql
-- Enable CDC at database level
EXEC sys.sp_cdc_enable_db;

-- Enable CDC for specific table
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'users',
    @role_name = NULL;
```

### 3.5 CDC Query Pattern

```go
func (r *CDCReader) queryChanges(ctx context.Context, fromLSN, toLSN string) ([]*event.ChangeEvent, error) {
    query := fmt.Sprintf(`
        SELECT __$start_lsn, __$operation, __$update_mask, *
        FROM cdc.fn_cdc_get_all_changes_%s(?, ?, 'all')
        ORDER BY __$start_lsn
    `, r.captureInstance)
    
    rows, err := r.db.QueryContext(ctx, query, fromLSN, toLSN)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    return r.parseRows(rows)
}
```

### 3.6 Operation Mapping

| `__$operation` | Meaning | ChangeEvent.Type |
|----------------|---------|------------------|
| 1 | Delete | `EventTypeDelete` |
| 2 | Insert | `EventTypeInsert` |
| 3 | Update (before image) | (used for Before field) |
| 4 | Update (after image) | `EventTypeUpdate` |

### 3.7 LSN Position Tracking

```go
type Position struct {
    StartLSN   string    `json:"start_lsn"`   // Binary(10) as hex string
    CommitTime time.Time `json:"commit_time"`
}

func (r *CDCReader) getCurrentLSN(ctx context.Context) (string, error) {
    var lsn []byte
    err := r.db.QueryRowContext(ctx, `
        SELECT sys.fn_cdc_get_max_lsn()
    `).Scan(&lsn)
    if err != nil {
        return "", err
    }
    return hex.EncodeToString(lsn), nil
}

func (r *CDCReader) incrementLSN(lsn string) string {
    // Increment LSN by 1 to avoid re-reading last record
    data, _ := hex.DecodeString(lsn)
    for i := len(data) - 1; i >= 0; i-- {
        data[i]++
        if data[i] != 0 {
            break
        }
    }
    return hex.EncodeToString(data)
}
```

### 3.8 Schema Cache

Uses same pattern as MySQL `TableSchemaCache`, querying:

```sql
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION

SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
AND CONSTRAINT_NAME LIKE 'PK_%'
ORDER BY ORDINAL_POSITION
```

### 3.9 Dependencies

```go
import (
    "github.com/microsoft/go-mssqldb"
)
```

---

## 4. Oracle Source (LogMiner)

### 4.1 Architecture

```
Oracle Redo Logs → LogMiner → ChangeEvent
         ↓
    V$LOGMNR_CONTENTS
```

### 4.2 Package Structure

```
internal/source/oracle/
├── config.go        # Configuration struct
├── connector.go     # Connector implementation
├── logminer.go      # LogMiner reader
├── sql_parser.go    # SQL parsing for DML/DDL
├── schema_cache.go  # Table schema cache
└── connector_test.go
```

### 4.3 Configuration

```go
package oracle

type Config struct {
    // Connection
    Host         string          `toml:"host"`
    Port         int             `toml:"port"`          // default: 1521
    User         string          `toml:"user"`
    Password     string          `toml:"password"`
    ServiceName  string          `toml:"service_name"`
    
    // LogMiner settings
    MiningStrategy string        `toml:"mining_strategy"` // "continuous", "online"
    BatchSize      int           `toml:"batch_size"`      // default: 1000
    
    // Tables
    Schemas      []string        `toml:"schemas"`
    Tables       map[string]string `toml:"tables"`
}

func DefaultConfig() *Config {
    return &Config{
        Port:           1521,
        MiningStrategy: "continuous",
        BatchSize:      1000,
    }
}
```

### 4.4 LogMiner Setup

```go
func (r *LogMinerReader) startMining(ctx context.Context, startSCN uint64) error {
    // Build dictionary
    _, err := r.db.ExecContext(ctx, `
        BEGIN
            DBMS_LOGMNR.START_LOGMNR(
                STARTSCN => :start_scn,
                OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +
                           DBMS_LOGMNR.CONTINUOUS_MINE
            );
        END;
    `, startSCN)
    return err
}
```

### 4.5 Change Query Pattern

```go
func (r *LogMinerReader) queryChanges(ctx context.Context, startSCN uint64) ([]*event.ChangeEvent, error) {
    query := `
        SELECT SCN, SQL_REDO, OPERATION_CODE, TABLE_NAME, SEG_OWNER
        FROM V$LOGMNR_CONTENTS
        WHERE SCN > :start_scn
        AND OPERATION_CODE IN (1, 2, 3, 5)  -- Insert, Update, Delete, DDL
        ORDER BY SCN
        FETCH FIRST :batch_size ROWS ONLY
    `
    
    rows, err := r.db.QueryContext(ctx, query, startSCN, r.config.BatchSize)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    return r.parseRows(rows)
}
```

### 4.6 Operation Mapping

| OPERATION_CODE | Operation | ChangeEvent.Type |
|----------------|-----------|------------------|
| 1 | Insert | `EventTypeInsert` |
| 2 | Delete | `EventTypeDelete` |
| 3 | Update | `EventTypeUpdate` |
| 5 | DDL | `EventTypeDDL` |

### 4.7 SQL Parsing Challenge

LogMiner returns SQL text in `SQL_REDO` column, not structured data:

```sql
-- Example SQL_REDO output:
INSERT INTO "SCOTT"."EMP"("EMPNO","ENAME","SAL") VALUES (7369,'SMITH',800);
UPDATE "SCOTT"."EMP" SET "SAL" = 900 WHERE "EMPNO" = 7369;
DELETE FROM "SCOTT"."EMP" WHERE "EMPNO" = 7369;
```

**Solution:** Implement a SQL parser or use the Oracle DDL parser from `pkg/parser/oracle`:

```go
func (r *LogMinerReader) parseDML(sql string) (*event.RowData, error) {
    // Simple regex-based parsing for common patterns
    // Or use ANTLR parser for full SQL support
    
    if strings.HasPrefix(sql, "INSERT INTO") {
        return r.parseInsert(sql)
    } else if strings.HasPrefix(sql, "UPDATE") {
        return r.parseUpdate(sql)
    } else if strings.HasPrefix(sql, "DELETE FROM") {
        return r.parseDelete(sql)
    }
    return nil, fmt.Errorf("unsupported DML type")
}
```

### 4.8 SCN Position Tracking

```go
type Position struct {
    SCN        uint64    `json:"scn"`
    CommitTime time.Time `json:"commit_time"`
}

func (r *LogMinerReader) getCurrentSCN(ctx context.Context) (uint64, error) {
    var scn uint64
    err := r.db.QueryRowContext(ctx, `
        SELECT CURRENT_SCN FROM V$DATABASE
    `).Scan(&scn)
    return scn, err
}
```

### 4.9 Schema Cache

Queries Oracle data dictionary:

```sql
SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
FROM ALL_TAB_COLUMNS
WHERE OWNER = ? AND TABLE_NAME = ?
ORDER BY COLUMN_ID

SELECT cols.COLUMN_NAME
FROM ALL_CONSTRAINTS cons
JOIN ALL_CONS_COLUMNS cols ON cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
WHERE cons.OWNER = ? AND cons.TABLE_NAME = ? AND cons.CONSTRAINT_TYPE = 'P'
ORDER BY cols.POSITION
```

### 4.10 Dependencies

```go
import (
    "github.com/sijms/go-ora/v2"
)
```

---

## 5. Implementation Order

| Week | Connector | Key Tasks |
|------|-----------|-----------|
| 1-2 | Elasticsearch Sink | Config, Connector, BulkIndexer, DocumentMapper |
| 3 | Redis Sink | Config, Connector, PipelineWriter |
| 4-5 | SQL Server Source | Config, Connector, CDCReader, SchemaCache |
| 6-8 | Oracle Source | Config, Connector, LogMinerReader, SQL Parser |

## 6. Testing Strategy

### Unit Tests

- Config validation
- Event to document/key mapping
- Position serialization

### Integration Tests

- Docker containers for each database
- Test fixtures with sample data
- CDC/streaming validation

### Test Configuration

```yaml
# docker-compose.test.yml
services:
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
    ports:
      - "9200:9200"
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
  
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
  
  sqlserver:
    image: mcr.microsoft.com/mssql/server:2022-latest
    ports:
      - "1433:1433"
    environment:
      - ACCEPT_EULA=Y
      - SA_PASSWORD=Test@123456
  
  oracle:
    image: gvenzl/oracle-xe:21-slim
    ports:
      - "1521:1521"
    environment:
      - ORACLE_PASSWORD=Test@123456
```

## 7. Configuration Examples

### TOML Configuration

```toml
# Elasticsearch Sink
[[sinks]]
type = "elasticsearch"
name = "es-main"

[sinks.config]
urls = ["http://localhost:9200"]
index_pattern = "{database}_{table}"
batch_size = 1000
refresh_policy = "false"

# Redis Sink
[[sinks]]
type = "redis"
name = "redis-cache"

[sinks.config]
addr = "localhost:6379"
key_pattern = "{database}:{table}:{pk}"
format = "hash"

# SQL Server Source
[[sources]]
type = "sqlserver"
name = "sqlserver-main"

[sources.config]
host = "localhost"
port = 1433
user = "sa"
password = "Test@123456"
database = "mydb"
schemas = ["dbo"]

# Oracle Source
[[sources]]
type = "oracle"
name = "oracle-main"

[sources.config]
host = "localhost"
port = 1521
user = "system"
password = "Test@123456"
service_name = "XE"
mining_strategy = "continuous"
```

## 8. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| LogMiner SQL parsing complexity | High | Start with simple regex, add ANTLR parser later |
| SQL Server CDC requires setup | Medium | Document setup requirements, add pre-flight checks |
| Elasticsearch version compatibility | Medium | Test with multiple ES versions |
| Redis cluster support | Low | Start with single-node, add cluster support later |

---

*Document Version: 1.0*
*Created: 2026-05-11*
*Author: DataStream Team*
