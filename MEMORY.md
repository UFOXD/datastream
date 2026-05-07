# DataStream Project Memory

> Last Updated: 2026-05-07

## Project Overview

DataStream is a Go-based CDC (Change Data Capture) platform that refactors Debezium concepts from Java to Go. It supports independent operation without mandatory Kafka dependency, enabling direct synchronization from upstream databases to downstream targets.

## Current Status

**Phase:** Phase 5 Completed  
**Next Phase:** Phase 6 - Integration & Testing  
**Build Status:** PASSING  
**Test Status:** PASSING (72 tests)

## Git Commits

| Commit | Phase | Description |
|--------|-------|-------------|
| `12062bc` | 1-2 | Core infrastructure + Connector layer |
| `e82737a` | 3 | Pipeline layer (Task, Dispatcher, Buffer) |
| `1068daf` | 4 | Coordinator layer with etcd support |
| `353de9e` | 5 | API & CLI layer |

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
- `pkg/source/mysql/` - MySQL binlog replication skeleton
- `pkg/source/postgres/` - PostgreSQL logical replication skeleton

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
- `pkg/sink/kafka/` - Kafka producer skeleton
- `pkg/sink/mysql/` - MySQL writer skeleton

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

## Dependencies

```go
require (
    github.com/pelletier/go-toml/v2 v2.1.0
    github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
    github.com/pingcap/log v1.1.0
    github.com/prometheus/client_golang v1.17.0
    go.uber.org/zap v1.26.0
    go.etcd.io/etcd/client/v3 v3.5.9
    github.com/gorilla/mux v1.8.1
    github.com/spf13/cobra v1.8.0
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
│   ├── cli/                        # Cobra CLI commands
│   ├── config/                     # Configuration
│   ├── coordinator/                # etcd coordinator
│   ├── errors/                     # RFC error codes
│   ├── event/                      # Event model
│   ├── logutil/                    # Logging
│   ├── metrics/                    # Prometheus metrics
│   ├── pipeline/                   # Pipeline, Task, Dispatcher, Buffer
│   ├── sink/                       # Sink connectors
│   │   ├── kafka/
│   │   └── mysql/
│   ├── source/                     # Source connectors
│   │   ├── mysql/
│   │   └── postgres/
│   ├── utils/                      # Utilities
│   └── version/                    # Version info
├── configs/datastream.toml         # Sample config
├── docs/design/                    # Design documents
├── Makefile
├── go.mod
└── go.sum
```

## Statistics

- **Go Files:** 56
- **Packages:** 10
- **Tests:** 72 (all passing)

## Next Phase: Phase 6 - Integration & Testing

### TODO
1. **Integration Tests**
   - End-to-end pipeline tests
   - MySQL/PostgreSQL integration tests
   - Kafka sink tests

2. **Documentation**
   - API documentation (OpenAPI/Swagger)
   - User guide
   - Deployment guide

3. **Production Readiness**
   - Graceful shutdown
   - Error recovery
   - Monitoring dashboards
   - Performance optimization

## Resume Instructions

To continue in a new session:

```bash
# 1. Read this file
cat MEMORY.md

# 2. Check git history
git log --oneline

# 3. Verify build and tests
go build ./... && go test ./...

# 4. Continue with Phase 6
```

## Key Patterns

- **Factory Pattern:** Connector creation via registry
- **Channel-based:** Event streaming via Go channels
- **Context-aware:** Cancellation and timeout support
- **Interface-based:** Pluggable components
