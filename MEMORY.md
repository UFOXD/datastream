# DataStream Project Memory

> Last Updated: 2026-05-10

## Project Overview

DataStream is a Go-based CDC (Change Data Capture) platform that refactors Debezium concepts from Java to Go. It supports independent operation without mandatory Kafka dependency, enabling direct synchronization from upstream databases to downstream targets.

## Current Status

**Phase:** Phase 7 In Progress (Week 1 Complete ✅)
**Branch:** `feature/phase6-benchmarks-deployment`
**Build Status:** PASSING
**Test Status:** ALL PASSING
**Overall Completion:** ~75%

---

## 📊 Project Completion Matrix

### Core Modules

| Module | Status | Coverage | Notes |
|--------|--------|----------|-------|
| `pkg/config` | ✅ Complete | 61.0% | TOML configuration |
| `pkg/logutil` | ✅ Complete | 43.6% | Zap logger wrapper |
| `pkg/errors` | ✅ Complete | 0.0%* | RFC error codes |
| `pkg/utils` | ✅ Complete | 70.2% | Retry, Pool, Hash |
| `pkg/metrics` | ✅ Complete | 0.0%* | Prometheus metrics |
| `pkg/version` | ✅ Complete | 100.0% | Version info |

*Only variable declarations, no executable code

### Event Model

| Module | Status | Coverage | Notes |
|--------|--------|----------|-------|
| `pkg/event` | ✅ Complete | 27.4% | ChangeEvent, Position, RowData |

### Parser Layer

| Module | Status | Coverage | Notes |
|--------|--------|----------|-------|
| `pkg/parser` | ✅ Complete | 100% | DDLParser interface, types, registry |
| `pkg/parser/mysql` | ✅ Complete | 100% | MySQL DDL parser (ANTLR) |
| `pkg/parser/postgres` | ✅ Complete | 100% | PostgreSQL DDL parser (ANTLR) |
| `pkg/parser/oracle` | ✅ Complete | 100% | Oracle PL/SQL parser (ANTLR) |
| `pkg/parser/sqlserver` | ✅ Complete | 100% | SQL Server T-SQL parser (ANTLR) |
| `pkg/parser/noop` | ✅ Complete | 100% | Noop parser for MongoDB |
| `pkg/parser/grammars` | ✅ Complete | - | ANTLR grammar files |
| `scripts/generate-parsers.sh` | ✅ Complete | - | Parser generation script |

### Pipeline Layer

| Module | Status | Coverage | Notes |
|--------|--------|----------|-------|
| `pkg/pipeline` | ⚠️ Partial | 27.6% | Core pipeline structure |
| `pkg/filter` | ✅ Complete | 100% | Filter interface, FilterChain, RuleFilter |
| `pkg/transform` | ✅ Complete | 100% | Transformer interface, TransformChain, MappingTransformer |
| `pkg/router` | ❌ **MISSING** | 0% | Router interface, TableRouter, PartitionRouter |

### Connector Layer - Source

| Connector | Required | Status | Coverage | Notes |
|-----------|----------|--------|----------|-------|
| MySQL | ✅ | ✅ Complete | 23.6% | Binlog streaming |
| PostgreSQL | ✅ | ✅ Complete | 15.3% | Logical replication |
| MongoDB | ✅ | ❌ **MISSING** | 0% | Change Stream |
| Oracle | ✅ | ❌ **MISSING** | 0% | LogMiner |
| SQL Server | ✅ | ❌ **MISSING** | 0% | CDC |
| MariaDB | ✅ | ❌ **MISSING** | 0% | Binlog (similar to MySQL) |

**Source Completion: 2/6 = 33.3%**

### Connector Layer - Sink

| Connector | Required | Status | Coverage | Notes |
|-----------|----------|--------|----------|-------|
| MySQL | ✅ | ✅ Complete | 17.5% | Batch write, DDL |
| Kafka | ⚠️ Optional | ✅ Complete | 43.7% | Producer with compression |
| PostgreSQL | ✅ | ❌ **MISSING** | 0% | COPY protocol |
| MongoDB | ✅ | ❌ **MISSING** | 0% | Batch upsert |
| Elasticsearch | ✅ | ❌ **MISSING** | 0% | Bulk API |
| Redis | ✅ | ❌ **MISSING** | 0% | Pipeline write |

**Sink Completion: 2/6 = 33.3%**

### Coordinator Layer

| Module | Status | Coverage | Notes |
|--------|--------|----------|-------|
| `pkg/coordinator` | ✅ Complete | 2.4% | etcd coordinator |
| `pkg/offset` | ✅ Complete | 40.7% | Position storage |

### API/CLI Layer

| Module | Status | Coverage | Notes |
|--------|--------|----------|-------|
| `pkg/api` | ✅ Complete | 27.9% | REST API |
| `pkg/cli` | ✅ Complete | 37.4% | Cobra CLI |
| `pkg/app` | ✅ Complete | 59.0% | Application lifecycle |

---

## 🔴 Missing Modules Detail

### 1. Router Module (`pkg/router/`)

**Design Doc:** `docs/design/pipeline-design.md` §5

```
Required Files:
├── pkg/router/
│   ├── router.go              # Router interface
│   ├── table_router.go        # Table-based routing
│   └── partition_router.go    # Partition routing
```

**Priority:** P1 - Architecture optimization (partially in pipeline/dispatcher.go)

---

## 📋 Implementation Plan

### Phase 7: Core Module Completion (4 weeks)

#### Week 1: Parser Module ✅ COMPLETE
- [x] Create `pkg/parser/parser.go` - DDLParser interface
- [x] Create `pkg/parser/ddl_result.go` - DDLResult, DDLType
- [x] Create `pkg/parser/table_changes.go` - TableChanges
- [x] Create `pkg/parser/registry.go` - Parser registry
- [x] Create `pkg/parser/mysql/mysql_parser.go` - ANTLR parser (per design doc)
- [x] Create `pkg/parser/postgres/postgres_parser.go` - ANTLR parser
- [x] Create `pkg/parser/oracle/oracle_parser.go` - ANTLR parser
- [x] Create `pkg/parser/sqlserver/sqlserver_parser.go` - ANTLR parser
- [x] Create `pkg/parser/noop/noop_parser.go` - Noop implementation
- [x] Create `pkg/parser/grammars/` - ANTLR grammar files
- [x] Create `scripts/generate-parsers.sh` - Parser generation script
- [x] Update `Makefile` - Add generate-parsers target
- [x] Write unit tests

#### Week 2: Filter Module ✅ COMPLETE
- [x] Create `pkg/filter/filter.go` - Filter interface
- [x] Create `pkg/filter/chain.go` - FilterChain (in filter.go)
- [x] Create `pkg/filter/rule.go` - Rule-based filter
- [x] Write unit tests

#### Week 3: Transform Module ✅ COMPLETE
- [x] Create `pkg/transform/transform.go` - Transformer interface
- [x] Create `pkg/transform/chain.go` - TransformChain (in transform.go)
- [x] Create `pkg/transform/mapper.go` - Field mapping transformer
- [x] Write unit tests

#### Week 4: Router + Integration
- [ ] Create `pkg/router/router.go` - Router interface
- [ ] Create `pkg/router/table_router.go` - Table routing
- [ ] Create `pkg/router/partition_router.go` - Partition routing
- [ ] Refactor pipeline/dispatcher.go
- [ ] Full Pipeline integration tests
- [ ] Update documentation

---

### Phase 8: Connector Extension (6 weeks)

#### Week 5-6: PostgreSQL Sink
- [ ] Create `pkg/sink/postgres/connector.go`
- [ ] Create `pkg/sink/postgres/batch_writer.go`
- [ ] COPY protocol support
- [ ] Integration tests

#### Week 7-8: MongoDB Source
- [ ] Create `pkg/source/mongodb/connector.go`
- [ ] Create `pkg/source/mongodb/change_stream.go`
- [ ] Resume token handling
- [ ] Integration tests

#### Week 9: MongoDB Sink + MariaDB
- [ ] Create `pkg/sink/mongodb/connector.go`
- [ ] Create `pkg/source/mariadb/connector.go` (fork MySQL)
- [ ] Integration tests

#### Week 10: Integration + Docs
- [ ] Full integration test suite
- [ ] Documentation update
- [ ] Performance benchmarks

---

### Phase 9: Enterprise Database Support (Optional)

#### Week 11-13: Oracle Source
- [ ] LogMiner integration
- [ ] XStream support (optional)

#### Week 14-15: SQL Server Source
- [ ] CDC table subscription
- [ ] Event conversion

#### Week 16: Additional Sinks
- [ ] Elasticsearch Sink
- [ ] Redis Sink

---

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
| `5c37058` | 6 | Improve test coverage across multiple packages |
| `16a048a` | 7 | Transform module with MappingTransformer |

---

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

---

## Benchmark Results (Apple M5)

| Benchmark | Latency | Allocs |
|-----------|---------|--------|
| MemoryBuffer Put | 12.76 ns/op | 0 |
| MemoryBuffer Get | 110 ns/op | 1 |
| Event Creation | 29 ns/op | 0 |
| Position Clone | 0.23 ns/op | 0 |

---

## Key Patterns

- **Factory Pattern:** Connector creation via registry
- **Channel-based:** Event streaming via Go channels
- **Context-aware:** Cancellation and timeout support
- **Interface-based:** Pluggable components

---

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

# 4. Continue with Phase 7 implementation
# Priority: Parser → Filter → Transform → Router
```

---

*文档版本：v2.1*
*创建时间：2026-05-07*
*更新时间：2026-05-10*
