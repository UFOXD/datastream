# DataStream Project Memory

> Last Updated: 2026-05-09

## Project Overview

DataStream is a Go-based CDC (Change Data Capture) platform that refactors Debezium concepts from Java to Go. It supports independent operation without mandatory Kafka dependency, enabling direct synchronization from upstream databases to downstream targets.

## Current Status

**Phase:** Phase 6 Complete ✅  
**Branch:** `feature/phase6-benchmarks-deployment`  
**Build Status:** PASSING  
**Test Status:** ALL PASSING

## Git Commits

| Commit | Phase | Description |
|--------|-------|-------------|
| `12062bc` | 1-2 | Core infrastructure + Connector layer |
| `e82737a` | 3 | Pipeline layer (Task, Dispatcher, Buffer) |
| `1068daf` | 4 | Coordinator layer with etcd support |
| `353de9e` | 5 | API & CLI layer |
| `43ea879` | 6 | Update mod path to github.com/UFOXD/datastream |
| `99ac50a` | 6 | Add Phase 6 benchmarks and deployment docs |
| `62913e1` | 6 | Implement MySQL sink and Kafka sink connectors |
| `274b182` | 6 | Implement MySQL binlog streaming source connector |
| `8bfc699` | 6 | Implement PostgreSQL logical replication source connector |
| `64fc5c0` | 6 | Add unit tests for all connectors |

## Completed Phases

### Phase 1: Core Layer + Event Model ✅

**Modules:**
- `pkg/config` - TOML configuration with env override (DATASTREAM_*)
- `pkg/logutil` - zap logger wrapper with field helpers
- `pkg/errors` - RFC error codes (DS:Category:ErrorType)
- `pkg/metrics` - Prometheus metrics
- `pkg/utils` - retry, Pool[T], FNV hash
- `pkg/version` - version info

**Event Model:**
- `pkg/event/change_event.go` - ChangeEvent with ID, Source, Type, Position, RowData
- `pkg/event/position.go` - Position with Compare, MarshalBinary, Clone
- `pkg/event/row_data.go` - RowData with Get, Set, SetNull, ColumnNames
- `pkg/event/table_info.go` - TableInfo with GetKeyColumns
- `pkg/event/ddl.go` - DDL event support
- `pkg/event/heartbeat.go` - Heartbeat events
- `pkg/event/transaction.go` - Transaction info

### Phase 2: Connector Layer ✅

**Source Interface** (`pkg/source/connector.go`):
```go
type Connector interface {
    Name() string
    Initialize(ctx context.Context, config Config) error
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    Status() Status
    Events() <-chan *event.ChangeEvent
    Errors() <-chan error
    GetPosition() *event.Position
    SetPosition(pos *event.Position) error
    GetSchema(database, table string) (*event.TableInfo, error)
}
```

**Source Connectors:**
- `pkg/source/mysql/` - MySQL binlog replication ✅ IMPLEMENTED
- `pkg/source/postgres/` - PostgreSQL logical replication ✅ IMPLEMENTED

**Sink Interface** (`pkg/sink/connector.go`):
```go
type Connector interface {
    Name() string
    Initialize(ctx context.Context, config Config) error
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    Status() Status
    Write(ctx context.Context, events []*event.ChangeEvent) error
    Flush(ctx context.Context) error
    GetPosition() *event.Position
    SupportsDDL() bool
    SupportsTransaction() bool
}
```

**Sink Connectors:**
- `pkg/sink/kafka/` - Kafka producer ✅ IMPLEMENTED
- `pkg/sink/mysql/` - MySQL writer ✅ IMPLEMENTED

### Phase 3: Pipeline Layer ✅

**Components:**
- `pkg/pipeline/pipeline.go` - Pipeline lifecycle (Start, Stop, Pause, Resume)
- `pkg/pipeline/task.go` - Task and TaskManager with CRUD
- `pkg/pipeline/dispatcher.go` - Round-robin, hash, broadcast dispatchers
- `pkg/pipeline/buffer.go` - MemoryBuffer, BatchBuffer
- `pkg/pipeline/coordinator.go` - Coordinator interface + MemoryCoordinator

**Dispatcher Types:**
- RoundRobinDispatcher - Even distribution
- HashDispatcher - Key-based routing
- BroadcastDispatcher - Send to all sinks

### Phase 4: Coordinator Layer ✅

**EtcdCoordinator** (`pkg/coordinator/etcd.go`):
- Distributed task storage
- Leadership election via etcd concurrency
- Position persistence
- Node registration with TTL leases
- Heartbeat mechanism

**Key Paths:**
- Tasks: `/datastream/tasks/{id}`
- Positions: `/datastream/positions/{taskId}`
- Leadership: `/datastream/leadership/{taskId}`
- Nodes: `/datastream/nodes/{nodeId}`

### Phase 5: API & CLI Layer ✅

**HTTP API** (`pkg/api/server.go`):

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/api/v1/tasks` | List all tasks |
| POST | `/api/v1/tasks` | Create task |
| GET | `/api/v1/tasks/{id}` | Get task details |
| DELETE | `/api/v1/tasks/{id}` | Delete task |
| POST | `/api/v1/tasks/{id}/start` | Start task |
| POST | `/api/v1/tasks/{id}/stop` | Stop task |
| GET | `/api/v1/tasks/{id}/position` | Get position |
| PUT | `/api/v1/tasks/{id}/position` | Set position |
| GET | `/api/v1/nodes` | List nodes |
| GET | `/metrics` | Prometheus metrics |

**CLI** (`pkg/cli/commands.go`):
```bash
datastream-ctl task list
datastream-ctl task create <id> <name> --config task.toml
datastream-ctl task get <id>
datastream-ctl task delete <id>
datastream-ctl task start <id>
datastream-ctl task stop <id>
datastream-ctl node list
datastream-ctl version
```

### Phase 6: Integration & Implementation ✅

#### Completed:

1. **Integration Test Framework** ✅
   - `tests/integration/docker-compose.yml` - MySQL, PostgreSQL, Kafka, etcd
   - `tests/integration/fixtures.go` - Test helpers and utilities
   - `tests/integration/mysql_test.go` - MySQL integration tests
   - `tests/integration/postgres_test.go` - PostgreSQL integration tests
   - `tests/integration/etcd_test.go` - etcd integration tests
   - `tests/integration/pipeline_test.go` - Pipeline integration tests
   - `tests/integration/run.sh` - Test runner script

2. **Graceful Shutdown** ✅
   - `pkg/app/app.go` - Application lifecycle management
   - Signal handling (SIGINT, SIGTERM, SIGHUP)
   - Ordered component shutdown
   - 30-second shutdown timeout

3. **OpenAPI Documentation** ✅
   - `docs/api/openapi.yaml` - OpenAPI 3.0 specification
   - `docs/api/README.md` - API usage guide

4. **E2E Tests** ✅
   - `tests/e2e/e2e_test.go` - End-to-end test suite
   - `tests/e2e/run.sh` - E2E test runner

5. **Performance Benchmarks** ✅
   - `pkg/event/benchmark_test.go` - Event model benchmarks
   - `pkg/pipeline/benchmark_test.go` - Buffer/Coordinator benchmarks

6. **Deployment Guide** ✅
   - `Dockerfile` - Container image definition
   - `docs/deployment/README.md` - Docker/Kubernetes deployment

7. **MySQL Sink Implementation** ✅
   - Database connection with connection pool
   - INSERT/UPDATE/DELETE query execution
   - Transaction support
   - DDL execution
   - Upsert/Replace/Insert strategies

8. **Kafka Sink Implementation** ✅
   - Kafka producer using segmentio/kafka-go
   - Compression support (gzip, snappy, lz4, zstd)
   - Topic naming strategies
   - Partition key configuration

9. **MySQL Binlog Streaming** ✅
   - Integrated go-mysql-org/go-mysql canal
   - BinlogHandler for INSERT/UPDATE/DELETE/DDL events
   - Schema caching and table info extraction
   - Position tracking for recovery

10. **PostgreSQL Logical Replication** ✅
    - Integrated jackc/pglogrepl for pgoutput protocol
    - PGOutputHandler for logical replication messages
    - Replication slot and publication management
    - LSN position tracking for recovery

11. **Unit Tests for All Connectors** ✅
    - MySQL source connector tests (12 tests)
    - PostgreSQL source connector tests (12 tests)
    - Kafka sink connector tests (15 tests)
    - MySQL sink connector tests (17 tests)

## Dependencies

```go
require (
    github.com/pelletier/go-toml/v2 v2.1.0
    github.com/pingcap/errors v0.11.5-0.20260310054046-9c8b3586e4b2
    github.com/pingcap/log v1.1.1-0.20260227082333-572e590d08f1
    github.com/prometheus/client_golang v1.17.0
    go.uber.org/zap v1.28.0
    go.etcd.io/etcd/client/v3 v3.5.9
    github.com/gorilla/mux v1.8.1
    github.com/spf13/cobra v1.8.0
    github.com/go-sql-driver/mysql v1.10.0
    github.com/lib/pq v1.12.3
    github.com/segmentio/kafka-go v0.4.51
    github.com/go-mysql-org/go-mysql v1.15.0
    github.com/jackc/pglogrepl v0.0.0-20260401131349-e37c41485510
    github.com/jackc/pgx/v5 v5.9.2
)
```

## Project Structure

```
datastream/
├── cmd/
│   ├── datastream/main.go          # Main server
│   └── datastream-ctl/main.go      # CLI tool
├── pkg/
│   ├── api/                        # HTTP REST API
│   ├── app/                        # Application lifecycle
│   ├── cli/                        # Cobra CLI commands
│   ├── config/                     # Configuration
│   ├── coordinator/                # etcd coordinator
│   ├── errors/                     # RFC error codes
│   ├── event/                      # Event model
│   ├── logutil/                    # Logging
│   ├── metrics/                    # Prometheus metrics
│   ├── pipeline/                   # Pipeline, Task, Dispatcher, Buffer
│   ├── sink/                       # Sink connectors
│   │   ├── kafka/                  # ✅ Implemented
│   │   └── mysql/                  # ✅ Implemented
│   ├── source/                     # Source connectors
│   │   ├── mysql/                  # ✅ Implemented
│   │   └── postgres/               # ✅ Implemented
│   ├── utils/                      # Utilities
│   └── version/                    # Version info
├── tests/
│   ├── integration/                # Integration tests
│   └── e2e/                        # End-to-end tests
├── docs/
│   ├── api/                        # OpenAPI docs
│   ├── deployment/                 # Deployment guide
│   └── design/                     # Design documents
├── configs/datastream.toml         # Sample config
├── Dockerfile                      # Container definition
├── Makefile
├── go.mod
└── go.sum
```

## Statistics

- **Go Files:** 70+
- **Packages:** 12
- **Tests:** ALL PASSING (including connector unit tests)

## Benchmark Results (Apple M5)

| Benchmark | Latency | Allocs |
|-----------|---------|--------|
| MemoryBuffer Put | 12.76 ns/op | 0 |
| MemoryBuffer Get | 110 ns/op | 1 |
| Event Creation | 29 ns/op | 0 |
| Position Clone | 0.23 ns/op | 0 |

## Resume Instructions

To continue in a new session:

```bash
# 1. Read this file
cat MEMORY.md

# 2. Check git history and current branch
git log --oneline
git branch

# 3. Verify build and tests
go build ./... && go test ./...

# 4. Project is ready for PR to dev branch
```

## Key Patterns

- **Factory Pattern:** Connector creation via registry
- **Channel-based:** Event streaming via Go channels
- **Context-aware:** Cancellation and timeout support
- **Interface-based:** Pluggable components

## Next Steps

Phase 6 is now complete! All source and sink connectors are implemented and tested.

Recommended next actions:
1. ✅ Add unit tests for source connectors (MySQL binlog, PostgreSQL logical replication)
2. ✅ Add unit tests for sink connectors (Kafka, MySQL)
3. Create PR to merge `feature/phase6-benchmarks-deployment` into `dev` branch
4. Plan Phase 7: Production hardening and additional features
