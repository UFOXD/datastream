# DataStream Project Memory

> Last Updated: 2026-05-26

## Project Overview

DataStream is a Go-based CDC (Change Data Capture) platform that refactors Debezium concepts from Java to Go. It supports independent operation without mandatory Kafka dependency, enabling direct synchronization from upstream databases to downstream targets.

## Current Status

**Phase:** Phase 11 — 表级独立生命周期 + Reviewer 修复
**Branch:** `main`
**Build Status:** PASSING
**Test Status:** ALL PASSING (43 packages)
**Total Commits:** 134
**Source Files:** 153 (不含生成代码和测试)
**Overall Completion:** 核心功能完成，表级生命周期代码已实现，Schema History + 缓冲完整性阻塞中

**Last Updated:** 2026-05-26

---

## 📋 当前卡点与待办

### ⛔ 阻塞项（必须解决才能上线）

详细设计文档：[`docs/design/schema-history-and-cache-integrity-design.md`](docs/design/schema-history-and-cache-integrity-design.md)

| 编号 | 问题 | 状态 |
|------|------|------|
| **B1** | 缓冲文件事务完整性 — 按源分治 + CRC32 + fsync + ReadResult | ✅ 已完成 |
| **B2** | Schema History — Tables 内存集合 + LocalSchemaHistory 单文件存储 + Recover | ✅ 已完成 |
| **B3** | Parser ApplyDDL — MySQL 实现完成 + ALTER 增强 (CHANGE/AFTER/FIRST/类型) | ✅ MySQL 完成，**Oracle/PG/SQLServer 存根** |
| **B4** | DDL 应用状态跟踪 — DDLRecordManager 状态机 | ✅ 已完成 |

### 📌 重要待办

| 优先级 | 事项 | 说明 |
|--------|------|------|
| **P0** | **Oracle parser ApplyDDL 实现** | 当前是存根。Oracle 支持 RENAME COLUMN / 改列顺序，DDL 事件会 failed 导致表停在旧 schema。同构/异构都是卡点。设计文档 §4.3 |
| **P0** | **PG parser ApplyDDL 实现** | 同上，PG 支持 ALTER COLUMN TYPE / RENAME COLUMN |
| **P0** | **SQLServer parser ApplyDDL 实现** | 同上，SQLServer 支持 sp_rename / ALTER COLUMN |
| P1 | S3 全量中转路径实现 | table-lifecycle-design.md §5.3 |
| P1 | TemporalConverter 时区 UTC 归一化 | oracle-dml-parser-design.md §11 |
| P2 | Source connector Schemas() 从 stub 改委托 | Reviewer 发现 S2 |
| P2 | Source connector 测试覆盖率 15-24% → 60%+ | test-strategy-design.md |
| P2 | 废弃 schema_cache.go | 跟随 B2 完成后迁移删除 |

### ✅ 2026-05-23~24 完成的工作

| 分类 | 工作 | Commits |
|------|------|---------|
| IPv6 修复 | 8 个连接器 `net.JoinHostPort` | 1 |
| Oracle DML Parser | 正则→状态机重写 + 大写/空白修复 + 设计文档（含时区策略） | 3 |
| Reviewer 修复 P0 | Pipeline Stop panic, Pause 假暂停 | 2 |
| Reviewer 修复 P1 | EventsWritten 计数, API setTaskPosition, etcd Election 缓存 | 3 |
| Reviewer 修复 P2 | Source Schemas(), Sink ApplyDDL, API 响应信封格式 | 3 |
| 功能补全 | when_needed 快照模式, 设计文档更新(Gin→mux, crypto/sync) | 2 |
| Error Handling | DataStreamError 分类 + CircuitBreaker + Alerter + Reviewer 修复 | 4 |
| API 端点 | 11 个新端点（task 管理 + 集群 + 诊断）| 2 |
| 新增 Sink | Oracle Sink + SQL Server Sink + Reviewer 修复 | 3 |
| 测试覆盖率 | event 100%, pipeline 92%, api 89% | 3 |
| 表级生命周期 SP1 | event.Position 扩展, TableLifecycle 状态机, Store, GlobalMinPosition | 4 |
| 表级生命周期 SP2 | CacheEvent Protobuf, LocalBackend, CacheSize, CLI decode | 6 |
| 表级生命周期 SP3 | HashChunker, BinlogConsumer, CatchingUpReplayer, SnapshotScheduler, Pipeline 集成 | 5 |
| 表级生命周期 SP4 | 8 个生命周期 API + 8 个 CLI 命令 + 6 个 Prometheus 指标 | 3 |
| 设计文档 | 表级生命周期设计 + Schema History 设计（草案）| 2+ |

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
| `pkg/router` | ✅ Complete | 100% | Router interface, TableRouter, PartitionRouter |

### Internal Pipeline Modules (2026-05-13 新增)

| Module | Status | Coverage | Notes |
|--------|--------|----------|-------|
| `internal/filter` | ✅ Complete | 100% | ExpressionFilter with table/field/regex matching |
| `internal/transform` | ✅ Complete | 100% | CustomTransformer, built-in transformers (AddField, RemoveField, RenameField, Timestamp) |
| `internal/pipeline` | ✅ Complete | 100% | BackpressureController, PersistentBuffer (Badger backend), MemoryBuffer, BatchBuffer |
| `internal/ratelimit` | ✅ Complete | 100% | Rate limiter using golang.org/x/time/rate |
| `internal/sink` | ✅ Complete | 100% | HashDispatcher, ConcurrentSinkWriter, RowIdentifier |
| `internal/source` | ✅ Complete | 100% | SnapshotCoordinator, SnapshotConcurrencyConfig, wildcard pattern matching, DatabaseDiscovery, TableManager |

### Connector Layer - Source

| Connector | Required | Status | Coverage | Notes |
|-----------|----------|--------|----------|-------|
| MySQL | ✅ | ✅ Complete | 23.6% | Binlog streaming, Schemas() method |
| PostgreSQL | ✅ | ✅ Complete | 15.3% | Logical replication |
| MongoDB | ✅ | ✅ Complete | 85.0% | Change Stream |
| Oracle | ✅ | ✅ Complete | ~70% | LogMiner, SCN tracking |
| SQL Server | ✅ | ✅ Complete | ~75% | CDC, LSN tracking |
| MariaDB | ✅ | ✅ Complete | 22.0% | Binlog (based on MySQL) |

**Source Completion: 6/6 = 100%** ✅

### Connector Layer - Sink

| Connector | Required | Status | Coverage | Notes |
|-----------|----------|--------|----------|-------|
| MySQL | ✅ | ✅ Complete | 17.5% | Batch write, DDL |
| Kafka | ⚠️ Optional | ✅ Complete | 43.7% | Producer with compression |
| PostgreSQL | ✅ | ✅ Complete | 19.2% | COPY protocol, Upsert |
| MongoDB | ✅ | ✅ Complete | 68.0% | Bulk write, Upsert |
| Elasticsearch | ✅ | ✅ Complete | ~80% | Bulk API, Document Mapper |
| Redis | ✅ | ✅ Complete | ~75% | Pipeline write, hash/json/string |

**Sink Completion: 6/6 = 100%** ✅

### Coordinator Layer

| Module | Status | Coverage | Notes |
|--------|--------|----------|-------|
| `pkg/coordinator` | ✅ Complete | 2.4% | etcd coordinator |
| `pkg/offset` | ✅ Complete | 40.7% | Position storage |

### API/CLI Layer

| Module | Status | Coverage | Notes |
|--------|--------|----------|-------|
| `pkg/api` | ✅ Complete | 27.9% | REST API |
| `internal/api` | ✅ Complete | - | Table management REST API endpoints |
| `pkg/cli` | ✅ Complete | 37.4% | Cobra CLI |
| `pkg/app` | ✅ Complete | 59.0% | Application lifecycle |

---

## ✅ Pipeline Layer Complete

All Pipeline Layer modules have been implemented:
- `pkg/filter` - Filter interface, FilterChain, RuleFilter
- `pkg/transform` - Transformer interface, TransformChain, MappingTransformer
- `pkg/router` - Router interface, TableRouter, PartitionRouter

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

#### Week 4: Router + Integration ✅ COMPLETE
- [x] Create `pkg/router/router.go` - Router interface
- [x] Create `pkg/router/table.go` - TableRouter
- [x] Create `pkg/router/partition.go` - PartitionRouter
- [x] Write unit tests

---

### Phase 8: Connector Extension (6 weeks)

#### Week 5-6: PostgreSQL Sink ✅ COMPLETE
- [x] Create `pkg/sink/postgres/config.go` - Configuration
- [x] Create `pkg/sink/postgres/connector.go` - Main connector
- [x] Create `pkg/sink/postgres/copy_writer.go` - COPY protocol writer
- [x] UPSERT support with ON CONFLICT
- [x] Schema-aware table quoting
- [x] Write unit tests

#### Week 7-8: MongoDB Source ✅ COMPLETE
- [x] Create `internal/source/mongodb/config.go` - Configuration
- [x] Create `internal/source/mongodb/connector.go` - Main connector
- [x] Create `internal/source/mongodb/change_stream.go` - Change Stream types
- [x] Resume token handling
- [x] Write unit tests

#### Week 9: MongoDB Sink + MariaDB ✅ COMPLETE
- [x] Create `internal/sink/mongodb/config.go` - Configuration with write strategies
- [x] Create `internal/sink/mongodb/connector.go` - Main connector with bulk write
- [x] Create `internal/source/mariadb/config.go` - Configuration with GTID support
- [x] Create `internal/source/mariadb/connector.go` - Binlog streaming (based on MySQL)
- [x] Write unit tests

#### Week 10: Integration + Docs ✅ COMPLETE
- [x] Integration tests for MongoDB, Elasticsearch, Redis, SQL Server, Oracle
- [x] Enterprise connectors documentation
- [x] Performance benchmarks (existing in pipeline/event packages)

#### Week 11-12: Technical Debt (Phase 8.5) - **COMPLETED** ✅
- [x] Remove canal dependency, use replication package directly
- [x] Integrate DDL Parser in Source Connectors
- [x] Independent Schema management (TableSchemaCache)
- [x] Unify position management with internal/offset

**Changes Made:**
- `internal/source/mysql/connector.go` - Refactored to use BinlogSyncer
- `internal/source/mysql/binlog_syncer.go` - New file using replication package
- `internal/source/mysql/schema_cache.go` - New file for independent schema management
- `internal/source/mariadb/connector.go` - Refactored similarly to MySQL

---

### Phase 9: Enterprise Database Support - **COMPLETE** ✅

#### Week 1-2: Elasticsearch Sink ✅ COMPLETE
- [x] `internal/sink/elasticsearch/config.go` - Configuration with validation
- [x] `internal/sink/elasticsearch/mapper.go` - Document mapping (GenerateDocID, ResolveIndex, BuildDocument)
- [x] `internal/sink/elasticsearch/indexer.go` - Bulk indexer with ND-JSON body builder
- [x] `internal/sink/elasticsearch/connector.go` - Full sink.Connector implementation
- [x] 58 unit tests passing

#### Week 3: Redis Sink ✅ COMPLETE
- [x] `internal/sink/redis/config.go` - Configuration with format validation
- [x] `internal/sink/redis/connector.go` - PipelineWriter + Connector implementation
- [x] Support for hash/json/string formats
- [x] TTL support, composite key generation
- [x] 15 unit tests passing

#### Week 4-5: SQL Server Source (CDC) ✅ COMPLETE
- [x] CDC-based change capture using cdc.fn_cdc_get_all_changes_*
- [x] LSN-based position tracking
- [x] Multi-capture instance support
- [x] Schema caching (TableSchemaCache)

#### Week 6-8: Oracle Source (LogMiner) ✅ COMPLETE
- [x] LogMiner integration (continuous/online mining)
- [x] SQL_REDO parsing for INSERT/UPDATE/DELETE
- [x] SCN position tracking
- [x] Schema caching (TableSchemaCache)

---

### Phase 10: Dynamic Table Management - **COMPLETE** ✅

#### DatabaseDiscovery ✅ COMPLETE
- [x] `internal/source/database_discovery.go` - Monitors DDL events for wildcard mode auto-discovery
- [x] Automatically adds matching tables to sync when new tables are created

#### TableManager ✅ COMPLETE
- [x] `internal/source/table_manager.go` - API-driven table management
- [x] AddTables, RemoveTables, PauseTable, ResumeTable operations

#### MySQL Connector Enhancement ✅ COMPLETE
- [x] `Schemas()` method added to MySQL Connector - returns all cached table schemas

#### Table API Endpoints ✅ COMPLETE
- [x] `internal/api/tables.go` - REST API for table management (add, remove, pause, resume)

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
| `ea99146` | 7 | Router module with TableRouter and PartitionRouter |
| `234c1b5` | 8 | PostgreSQL sink connector with COPY protocol |
| `3e78df3` | 8 | Update MEMORY.md with PostgreSQL sink completion |
| `a469559` | 8 | Refactor: reorganize directory structure (pkg→internal) |
| `98d4661` | 9 | feat(source): add SQL Server and Oracle CDC source connectors |
| `6f40e4e` | 9 | test: add integration tests for new connectors |
| `48b802c` | 9 | docs: add enterprise connectors guide |

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
    github.com/dgraph-io/badger/v4 v4.9.1  // PersistentBuffer后端
    golang.org/x/time v0.15.0              // RateLimiter
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

*文档版本：v2.3*
*创建时间：2026-05-07*
*更新时间：2026-05-13*

---

## 2026-05-13 阶段完成记录

### 完成的任务

| Task ID | 任务名称 | 状态 |
|---------|---------|------|
| #87 | ExpressionFilter Tests | ✅ 完成 |
| #86 | CustomTransformer Foundation | ✅ 完成 |
| #90 | BackpressureController | ✅ 完成 |
| #91 | RateLimiter Tests | ✅ 完成 |

### 新增/修改的文件

| 文件路径 | 操作类型 | 说明 |
|---------|---------|------|
| `internal/filter/expression.go` | 修改 | 修复regex pattern handling, RowData处理 |
| `internal/filter/expression_test.go` | 新增 | ExpressionFilter完整测试 |
| `internal/transform/custom.go` | 新增 | CustomTransformer实现，内置转换器 |
| `internal/transform/custom_test.go` | 新增 | CustomTransformer测试 |
| `internal/pipeline/backpressure.go` | 新增 | BackpressureController流控实现 |
| `internal/pipeline/backpressure_test.go` | 新增 | BackpressureController测试 |
| `internal/pipeline/persistent_buffer.go` | 新增 | PersistentBuffer实现（Badger后端） |
| `internal/pipeline/persistent_buffer_test.go` | 新增 | PersistentBuffer测试 |
| `internal/ratelimit/ratelimit.go` | 新增 | RateLimiter实现 |
| `internal/ratelimit/ratelimit_test.go` | 新增 | RateLimiter测试 |
| `internal/source/mysql/binlog_syncer.go` | 修改 | 添加通配符模式匹配（支持 * 和 ?） |
| `internal/source/mysql/pattern_test.go` | 新增 | 模式匹配测试 |
| `docs/module-map.md` | 修改 | 更新API文档 |
| `go.mod` | 修改 | 新增 golang.org/x/time, badger/v4 依赖 |

### 关键决策记录

1. **ExpressionFilter regex修复**: 使用parseValue后的值（无引号）作为regex pattern，而非原始字符串
2. **BackpressureController resume条件**: 同时检查queue usage和latency，任一超标都触发pause
3. **RateLimiter依赖**: 使用 golang.org/x/time/rate 包实现令牌桶限流
4. **PersistentBuffer后端**: 使用 badger/v4 作为KV存储后端，支持事件持久化和重启恢复
5. **通配符匹配**: 使用DP算法实现 `*` 和 `?` 通配符匹配，支持表名/数据库名模式过滤

### Git Commits

| Commit | Description |
|--------|-------------|
| `54a6af4` | test(filter): add comprehensive tests for ExpressionFilter |
| `5a854f7` | feat(transform): add CustomTransformer foundation with built-in transformers |
| `3812a82` | feat(pipeline): add BackpressureController for flow control |
| `42a13b5` | test(ratelimit): add comprehensive tests for RateLimiter |

### 技术债务

无新增技术债务。

### 下一阶段计划

- 继续Phase 6剩余任务
- 集成测试验证各模块协作

---

## 2026-05-13 Phase 10 阶段完成记录

### 完成的任务

| Task ID | 任务名称 | 状态 |
|---------|---------|------|
| #96 | DatabaseDiscovery | ✅ 完成 |
| #99 | TableManager | ✅ 完成 |
| #95 | Schemas() method (MySQL Connector) | ✅ 完成 |
| #98 | Table API endpoints | ✅ 完成 |
| #97 | Final integration test | ✅ 完成 |

### 新增/修改的文件

| 文件路径 | 操作类型 | 说明 |
|---------|---------|------|
| `internal/source/database_discovery.go` | 新增 | DDL事件监控，通配符模式自动发现新表 |
| `internal/source/table_manager.go` | 新增 | API驱动的表管理（AddTables, RemoveTables, PauseTable, ResumeTable） |
| `internal/source/mysql/connector.go` | 修改 | 添加Schemas()方法，返回所有缓存的表结构 |
| `internal/api/tables.go` | 新增 | 表管理REST API端点 |

### 关键决策记录

1. **DatabaseDiscovery**: 监听DDL事件流，对新建表名执行通配符匹配，自动加入同步列表
2. **TableManager**: 提供运行时动态增减同步表的能力，支持暂停/恢复单表同步
3. **Schemas()**: 暴露MySQL连接器内部的schema缓存，供上层组件（如DatabaseDiscovery）使用
4. **Table API**: RESTful端点与TableManager集成，支持HTTP方式管理同步表

---

## 📋 待完成工作 (Todo)

### 优先级 P0 - 核心功能完善

| 任务 | 说明 | 状态 |
|------|------|------|
| CLI表管理命令 | `datastream-ctl tables add/remove/list/get/pause/resume` 命令 | ✅ 已完成 |
| 设计文档同步 | 更新 `docs/design/` 下所有文档反映实际实现 | ✅ 已完成 |

### 优先级 P1 - 功能增强

| 任务 | 说明 | 状态 |
|------|------|------|
| 数据库兼容性测试 | MySQL/PostgreSQL/SQL Server/Oracle 集成测试 | ✅ 已完成 |
| 性能基准测试 | 各连接器的吞吐量和延迟基准 | ⏳ 待开始 |
| 监控指标集成 | Prometheus metrics 端点暴露 | ⏳ 待开始 |

### 优先级 P2 - 文档与部署

| 任务 | 说明 | 状态 |
|------|------|------|
| API文档 | OpenAPI/Swagger 文档生成 | ⏳ 待开始 |
| 部署指南 | Docker/K8s 部署文档 | ⏳ 待开始 |
| 用户手册 | 端到端使用指南 | ⏳ 待开始 |

### 技术债务

| 项目 | 说明 | 优先级 |
|------|------|--------|
| SchemaFetcher 优化 | TableManager.AddTables 在锁内调用FetchSchema，可能导致阻塞 | P2 |
| DDLDiscovery清理 | `DDLDiscovery` 的 DropDatabase 逻辑与 `DatabaseDiscovery` 同步 | P3 |

---

## 2026-05-14 会话记录

### 会话概要

- **目标**: 完成设计文档与实际实现对齐，实现 DatabaseDiscovery、TableManager 和 Table API
- **方法**: 使用 `superpowers:subagent-driven-development` 技能，每个任务两阶段审查（spec合规 + 代码质量）
- **结果**: 6个任务全部完成，36个测试包全部通过

### 实现质量亮点

1. **代码审查流程**: 每个任务经过 spec reviewer 和 code reviewer 双重审查
2. **问题修复**: 发现并修复了多个潜在问题（如通道死锁、错误信息泄露、JSON序列化问题）
3. **设计对齐**: 更新了 `pipeline-design.md` 和 `connector-design.md` 反映实际实现

### Git Commits (Phase 10)

| Commit | Description |
|--------|-------------|
| `d83ec7f` | feat(source): add Schemas() method to MySQL connector |
| `3e5b2fb` | feat(source): implement DatabaseDiscovery for wildcard mode |
| `7ac43ee` | feat(source): implement TableManager for API-driven table management |
| `2b7ac08` | docs: update design documents to reflect actual implementation |
| `d1e7e4a` | docs: update MEMORY.md with Phase 10 completion |

### 下一步建议

1. **集成测试**: 使用真实数据库测试 DatabaseDiscovery 的自动发现功能
2. ~~**CLI集成**: 添加 `datastream-ctl tables` 命令行支持~~ ✅ 已完成
3. **文档完善**: 编写 API 使用示例和最佳实践

---

## 2026-05-14 CLI Tables 命令实现

### 完成的任务

| 任务 | 状态 |
|------|------|
| CLI tables 命令 (add/remove/list/get/pause/resume) | ✅ 完成 |
| 重构 main.go 使用 Cobra CLI | ✅ 完成 |
| 添加 CLI 测试 | ✅ 完成 |

### 新增/修改的文件

| 文件路径 | 操作类型 | 说明 |
|---------|---------|------|
| `internal/cli/tables.go` | 新增 | tables 子命令实现 (6个命令) |
| `cmd/datastream-ctl/main.go` | 修改 | 重构为使用 Cobra CLI |
| `internal/cli/commands.go` | 修改 | 添加 tables 子命令 |
| `internal/cli/commands_test.go` | 修改 | 添加 tables 命令测试 |

### Git Commits

| Commit | Description |
|--------|-------------|
| `b0ea83a` | feat(cli): add tables command for sync table management |

### 命令用法

```bash
# 添加表到同步列表
datastream-ctl tables add mydb.users mydb.orders

# 从同步列表移除表
datastream-ctl tables remove mydb.users

# 列出所有同步表
datastream-ctl tables list
datastream-ctl tables list --database mydb

# 获取表同步状态
datastream-ctl tables get mydb.users

# 暂停/恢复表同步
datastream-ctl tables pause mydb.users
datastream-ctl tables resume mydb.users
```

---

## 2026-05-15 数据库兼容性集成测试

### 完成的任务

| 任务 | 状态 |
|------|------|
| Docker Compose 配置 | ✅ 完成 |
| 测试辅助函数 | ✅ 完成 |
| MySQL 集成测试 | ✅ 完成 |
| PostgreSQL 集成测试 | ✅ 完成 |
| SQL Server 集成测试 | ✅ 完成 |
| Oracle 集成测试 | ✅ 完成 |
| Makefile 集成测试命令 | ✅ 完成 |
| 测试依赖添加 | ✅ 完成 |

### 新增文件

| 文件路径 | 说明 |
|---------|------|
| `tests/docker/docker-compose.yml` | 数据库容器配置 |
| `tests/docker/mysql/init.sql` | MySQL 初始化脚本 |
| `tests/docker/postgres/init.sql` | PostgreSQL 初始化脚本 |
| `tests/docker/sqlserver/init.sql` | SQL Server 初始化脚本 |
| `tests/docker/oracle/init.sql` | Oracle 初始化脚本 |
| `tests/integration/integration_test.go` | 测试辅助函数 |
| `tests/integration/mysql_test.go` | MySQL 集成测试 |
| `tests/integration/postgres_test.go` | PostgreSQL 集成测试 |
| `tests/integration/sqlserver_test.go` | SQL Server 集成测试 |
| `tests/integration/oracle_test.go` | Oracle 集成测试 |

### Git Commits

| Commit | Description |
|--------|-------------|
| `eeb3f47` | feat(test): add docker-compose for integration test databases |
| `31183d5` | feat(test): add integration test helpers |
| `47ccf86` | feat(test): add MySQL integration tests |
| `b159412` | feat(test): add PostgreSQL integration tests |
| `ad00303` | feat(test): add SQL Server integration tests |
| `bf6419b` | feat(test): add Oracle integration tests |
| `664425a` | feat(test): add integration test Makefile targets |
| `c583432` | feat(test): add integration test dependencies |

### 运行方式

```bash
# 启动数据库容器并运行所有集成测试
make test-integration

# 运行单个数据库测试
make test-integration-mysql
make test-integration-postgres
make test-integration-sqlserver
make test-integration-oracle

# 手动管理容器
make test-integration-up    # 启动
make test-integration-down  # 停止
```

