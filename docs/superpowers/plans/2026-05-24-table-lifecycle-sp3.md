# Table-Level Independent Lifecycle — Sub-Plan 3: Core Engine

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire up the core runtime — BinlogConsumer routes events per-table, SnapshotScheduler orchestrates lifecycle, CatchingUpReplayer handles catch-up, and hash-based chunking enables parallel snapshots.

**Architecture:** BinlogConsumer reads from source.Events() channel and routes per-table based on TableLifecycle state. SnapshotScheduler manages state transitions. CatchingUpReplayer reads from BinlogCacheBackend and applies to sinks. All components use interfaces for testability.

**Tech Stack:** Go 1.22+, channels, sync primitives

**Design Doc:** `docs/design/table-lifecycle-design.md` §3-6, §10

**Depends on:** Sub-Plan 1 (TableLifecycle, Position) + Sub-Plan 2 (BinlogCacheBackend, LocalBackend)

---

## File Structure

```
internal/lifecycle/
  binlog_consumer.go          — CREATE: event router (source.Events → cache or sink)
  binlog_consumer_test.go     — CREATE: tests
  snapshot_scheduler.go       — CREATE: orchestrator for all table lifecycles
  snapshot_scheduler_test.go  — CREATE: tests
  catching_up_replayer.go     — CREATE: reads cache, applies to sink, detects caught-up
  catching_up_replayer_test.go — CREATE: tests
  hash_chunker.go             — CREATE: hash-based table splitting for parallel snapshot
  hash_chunker_test.go        — CREATE: tests
  config.go                   — CREATE: SchedulerConfig, merged from design doc §8
```

---

### Task 1: SchedulerConfig + HashChunker

**Files:**
- Create: `internal/lifecycle/config.go`
- Create: `internal/lifecycle/hash_chunker.go`
- Create: `internal/lifecycle/hash_chunker_test.go`

- [ ] **Step 1: Write failing tests for HashChunker**

```go
package lifecycle

import "testing"

func TestHashChunkerGeneratesCorrectSQL(t *testing.T) {
    chunker := NewHashChunker(4) // 4 workers

    tests := []struct {
        db       string
        table    string
        pkCols   []string
        workerID int
        dbType   string
        wantSQL  string
    }{
        {"db1", "users", []string{"id"}, 0, "mysql",
            "SELECT * FROM `db1`.`users` WHERE MOD(CRC32(CONCAT(`id`)), 4) = 0"},
        {"db1", "users", []string{"id"}, 3, "mysql",
            "SELECT * FROM `db1`.`users` WHERE MOD(CRC32(CONCAT(`id`)), 4) = 3"},
        {"db1", "orders", []string{"tenant_id", "order_id"}, 1, "mysql",
            "SELECT * FROM `db1`.`orders` WHERE MOD(CRC32(CONCAT(`tenant_id`,`order_id`)), 4) = 1"},
        {"public", "users", []string{"id"}, 0, "postgres",
            `SELECT * FROM "public"."users" WHERE MOD(hashtext("id"::text), 4) = 0`},
        {"HR", "EMP", []string{"ID"}, 0, "oracle",
            `SELECT * FROM "HR"."EMP" WHERE MOD(ORA_HASH("ID"), 4) = 0`},
        {"dbo", "users", []string{"id"}, 0, "sqlserver",
            `SELECT * FROM [dbo].[users] WHERE ABS(CHECKSUM([id])) % 4 = 0`},
    }
    for _, tt := range tests {
        t.Run(tt.dbType+"_"+tt.table, func(t *testing.T) {
            sql := chunker.BuildChunkSQL(tt.db, tt.table, tt.pkCols, tt.workerID, tt.dbType)
            if sql != tt.wantSQL {
                t.Errorf("got:\n  %s\nwant:\n  %s", sql, tt.wantSQL)
            }
        })
    }
}

func TestHashChunkerWorkerCount(t *testing.T) {
    chunker := NewHashChunker(8)
    if chunker.Workers() != 8 {
        t.Errorf("Workers() = %d, want 8", chunker.Workers())
    }
}
```

- [ ] **Step 2: Implement config.go + hash_chunker.go**

`config.go`:
```go
package lifecycle

import "time"

type SchedulerConfig struct {
    MaxTableThreads   int           `toml:"max-table-threads"`
    MaxChunkThreads   int           `toml:"max-chunk-threads"`
    ChunkThreshold    int64         `toml:"chunk-threshold"`
    SmartOrder        bool          `toml:"smart-order"`
    MaxRetries        int           `toml:"max-retries"`
    RetryInterval     time.Duration `toml:"retry-interval"`
    BatchSize         int           `toml:"batch-size"`
    UpsertDuration    time.Duration `toml:"upsert-duration"`
    TargetMode        string        `toml:"target-mode"` // "drop-create-insert"
}

func DefaultSchedulerConfig() *SchedulerConfig {
    return &SchedulerConfig{
        MaxTableThreads: 4,
        MaxChunkThreads: 4,
        ChunkThreshold:  1000000,
        SmartOrder:      true,
        MaxRetries:      3,
        RetryInterval:   5 * time.Minute,
        BatchSize:       1000,
        UpsertDuration:  time.Minute,
        TargetMode:      "drop-create-insert",
    }
}
```

`hash_chunker.go`:
```go
package lifecycle

import (
    "fmt"
    "strings"
)

type HashChunker struct {
    workers int
}

func NewHashChunker(workers int) *HashChunker {
    return &HashChunker{workers: workers}
}

func (c *HashChunker) Workers() int { return c.workers }

func (c *HashChunker) BuildChunkSQL(schema, table string, pkCols []string, workerID int, dbType string) string {
    switch dbType {
    case "mysql", "mariadb":
        return c.buildMySQL(schema, table, pkCols, workerID)
    case "postgres":
        return c.buildPostgres(schema, table, pkCols, workerID)
    case "oracle":
        return c.buildOracle(schema, table, pkCols, workerID)
    case "sqlserver":
        return c.buildSQLServer(schema, table, pkCols, workerID)
    default:
        return c.buildMySQL(schema, table, pkCols, workerID)
    }
}
```

Implement each `buildXxx` method generating the appropriate SQL per the test expectations.

- [ ] **Step 3: Run tests**

Run: `go test ./internal/lifecycle/ -count=1 -v`
Expected: All PASS

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(lifecycle): add SchedulerConfig and HashChunker for parallel snapshot"
```

---

### Task 2: BinlogConsumer — event router

**Files:**
- Create: `internal/lifecycle/binlog_consumer.go`
- Create: `internal/lifecycle/binlog_consumer_test.go`

- [ ] **Step 1: Write failing tests**

```go
func TestBinlogConsumerRoutesToCache(t *testing.T) {
    // Table in snapshotting → events go to cache backend
    store := source.NewMemoryLifecycleStore()
    cache := NewMockCacheBackend()
    sink := NewMockSink()
    
    lc := source.NewTableLifecycle(source.TableID{Database: "db1", Table: "users"})
    lc.TransitionTo(source.TableStateSnapshotting, &event.Position{GTID: "uuid:1"})
    store.Save(ctx, "task-1", lc)
    
    consumer := NewBinlogConsumer("task-1", store, cache, sink)
    
    ev := &event.ChangeEvent{Table: event.TableInfo{Database: "db1", Table: "users"}}
    consumer.Route(ctx, ev)
    
    assert.Equal(t, 1, cache.WriteCount("db1.users"))
    assert.Equal(t, 0, sink.WriteCount())
}

func TestBinlogConsumerRoutesToSink(t *testing.T) {
    // Table in streaming → events go to sink
    store := source.NewMemoryLifecycleStore()
    cache := NewMockCacheBackend()
    sink := NewMockSink()
    
    lc := source.NewTableLifecycle(source.TableID{Database: "db1", Table: "users"})
    lc.TransitionTo(source.TableStateSnapshotting, &event.Position{GTID: "uuid:1"})
    lc.TransitionTo(source.TableStateCatchingUp, nil)
    lc.TransitionTo(source.TableStateStreaming, nil)
    store.Save(ctx, "task-1", lc)
    
    consumer := NewBinlogConsumer("task-1", store, cache, sink)
    
    ev := &event.ChangeEvent{Table: event.TableInfo{Database: "db1", Table: "users"}}
    consumer.Route(ctx, ev)
    
    assert.Equal(t, 0, cache.WriteCount("db1.users"))
    assert.Equal(t, 1, sink.WriteCount())
}

func TestBinlogConsumerDiscardsPending(t *testing.T) {
    // Table in pending → events discarded
    store := source.NewMemoryLifecycleStore()
    cache := NewMockCacheBackend()
    sink := NewMockSink()
    
    lc := source.NewTableLifecycle(source.TableID{Database: "db1", Table: "users"})
    store.Save(ctx, "task-1", lc) // stays pending
    
    consumer := NewBinlogConsumer("task-1", store, cache, sink)
    
    ev := &event.ChangeEvent{Table: event.TableInfo{Database: "db1", Table: "users"}}
    consumer.Route(ctx, ev)
    
    assert.Equal(t, 0, cache.WriteCount("db1.users"))
    assert.Equal(t, 0, sink.WriteCount())
}
```

- [ ] **Step 2: Implement BinlogConsumer**

```go
type BinlogConsumer struct {
    taskID string
    store  source.TableLifecycleStore
    cache  cache.BinlogCacheBackend
    sink   EventSink // interface { Write(ctx, *event.ChangeEvent) error }
    mu     sync.RWMutex
    routes map[string]source.TableState // tableID -> cached state for fast lookup
}

// EventSink is a simplified sink interface for the consumer
type EventSink interface {
    Write(ctx context.Context, events []*event.ChangeEvent) error
}

func (c *BinlogConsumer) Route(ctx context.Context, ev *event.ChangeEvent) error {
    tableID := ev.Table.Database + "." + ev.Table.Table
    state := c.getTableState(tableID)
    
    switch state {
    case source.TableStateSnapshotting:
        return c.cache.Write(ctx, tableID, eventToCacheEvent(ev))
    case source.TableStateCatchingUp, source.TableStateStreaming:
        return c.sink.Write(ctx, []*event.ChangeEvent{ev})
    default: // pending, error, paused
        return nil // discard
    }
}
```

- [ ] **Step 3: Run tests, verify pass**
- [ ] **Step 4: Commit**

```bash
git commit -m "feat(lifecycle): add BinlogConsumer event router"
```

---

### Task 3: CatchingUpReplayer

**Files:**
- Create: `internal/lifecycle/catching_up_replayer.go`
- Create: `internal/lifecycle/catching_up_replayer_test.go`

- [ ] **Step 1: Write failing tests**

```go
func TestReplayerAppliesEventsFromCache(t *testing.T) {
    // Setup: cache has 3 events, replayer reads and applies them to sink
    cacheBackend := setupCacheWith3Events(t)
    sink := NewMockSink()
    
    replayer := NewCatchingUpReplayer(cacheBackend, sink, ReplayerConfig{
        BatchSize:      100,
        UpsertDuration: time.Minute,
    })
    
    result, err := replayer.Replay(ctx, "db1.users", "", 0)
    require.NoError(t, err)
    assert.Equal(t, 3, sink.WriteCount())
    assert.True(t, result.CaughtUp)
}

func TestReplayerResumeFromGTID(t *testing.T) {
    // Cache has 5 events, resume from GTID event_seq=3 → only apply 2 events
    cacheBackend := setupCacheWith5Events(t)
    sink := NewMockSink()
    
    replayer := NewCatchingUpReplayer(cacheBackend, sink, ReplayerConfig{BatchSize: 100})
    
    result, err := replayer.Replay(ctx, "db1.users", "uuid:100", 3)
    require.NoError(t, err)
    assert.Equal(t, 2, sink.WriteCount()) // events 3 and 4
}

func TestReplayerUpsertModeDuration(t *testing.T) {
    // Verify that replayer reports upsert mode for first N events
    cacheBackend := setupCacheWith3Events(t)
    sink := &writeModeSink{}
    
    replayer := NewCatchingUpReplayer(cacheBackend, sink, ReplayerConfig{
        UpsertDuration: 100 * time.Millisecond,
    })
    
    replayer.Replay(ctx, "db1.users", "", 0)
    // First events should be in upsert mode
    assert.True(t, sink.firstEventUpsert)
}
```

- [ ] **Step 2: Implement CatchingUpReplayer**

Key behavior:
- Reads from `BinlogCacheBackend.Read(ctx, tableID, fromGTID, fromEventSeq)`
- Applies each event to the sink
- Tracks progress (GTID + EventSeq)
- First `UpsertDuration` uses UPSERT mode
- After channel closes (no more events), returns `CaughtUp: true`
- Returns `ReplayResult{CaughtUp bool, LastGTID string, LastEventSeq int64}`

- [ ] **Step 3: Run tests**
- [ ] **Step 4: Commit**

```bash
git commit -m "feat(lifecycle): add CatchingUpReplayer with UPSERT window"
```

---

### Task 4: SnapshotScheduler — state orchestrator

**Files:**
- Create: `internal/lifecycle/snapshot_scheduler.go`
- Create: `internal/lifecycle/snapshot_scheduler_test.go`

- [ ] **Step 1: Write failing tests**

```go
func TestSchedulerTransitionsTableThrough Lifecycle(t *testing.T) {
    // Add a table, verify it goes pending → snapshotting → catching_up → streaming
    scheduler := newTestScheduler(t)
    
    scheduler.AddTable(source.TableID{Database: "db1", Table: "users"})
    
    // Simulate snapshot complete
    scheduler.OnSnapshotComplete("db1.users")
    lc, _ := scheduler.GetTableLifecycle("db1.users")
    assert.Equal(t, source.TableStateCatchingUp, lc.GetState())
    
    // Simulate caught-up
    scheduler.OnCaughtUp("db1.users")
    lc, _ = scheduler.GetTableLifecycle("db1.users")
    assert.Equal(t, source.TableStateStreaming, lc.GetState())
}

func TestSchedulerHandlesSnapshotFailure(t *testing.T) {
    scheduler := newTestScheduler(t)
    scheduler.AddTable(source.TableID{Database: "db1", Table: "users"})
    
    scheduler.OnSnapshotError("db1.users", "connection timeout")
    lc, _ := scheduler.GetTableLifecycle("db1.users")
    assert.Equal(t, source.TableStateError, lc.GetState())
}

func TestSchedulerRestartTable(t *testing.T) {
    scheduler := newTestScheduler(t)
    scheduler.AddTable(source.TableID{Database: "db1", Table: "users"})
    scheduler.OnSnapshotComplete("db1.users")
    scheduler.OnCaughtUp("db1.users")
    
    // Force restart from streaming
    pos := &event.Position{GTID: "uuid:500"}
    err := scheduler.RestartTable("db1.users", pos, true)
    require.NoError(t, err)
    
    lc, _ := scheduler.GetTableLifecycle("db1.users")
    assert.Equal(t, source.TableStatePending, lc.GetState())
}

func TestSchedulerGlobalMinPosition(t *testing.T) {
    scheduler := newTestScheduler(t)
    scheduler.AddTable(source.TableID{Database: "db1", Table: "t1"})
    scheduler.AddTable(source.TableID{Database: "db1", Table: "t2"})
    
    // Both start at same time but t1 moves ahead
    // GlobalMinPosition should be the slower one
    pos := scheduler.GetGlobalMinPosition()
    assert.NotNil(t, pos)
}
```

- [ ] **Step 2: Implement SnapshotScheduler**

```go
type SnapshotScheduler struct {
    config   *SchedulerConfig
    store    source.TableLifecycleStore
    taskID   string
    consumer *BinlogConsumer
    cache    cache.BinlogCacheBackend
    mu       sync.RWMutex
}

func NewSnapshotScheduler(config *SchedulerConfig, taskID string, store source.TableLifecycleStore, cache cache.BinlogCacheBackend) *SnapshotScheduler

func (s *SnapshotScheduler) AddTable(tableID source.TableID) error
func (s *SnapshotScheduler) OnSnapshotComplete(tableID string) error
func (s *SnapshotScheduler) OnSnapshotError(tableID string, errMsg string) error
func (s *SnapshotScheduler) OnCaughtUp(tableID string) error
func (s *SnapshotScheduler) RestartTable(tableID string, newPos *event.Position, force bool) error
func (s *SnapshotScheduler) RestartSchema(schema string, newPos *event.Position, force bool) ([]string, error)
func (s *SnapshotScheduler) GetTableLifecycle(tableID string) (*source.TableLifecycle, error)
func (s *SnapshotScheduler) GetGlobalMinPosition() *event.Position
func (s *SnapshotScheduler) ListErrors() ([]*source.TableLifecycle, error)
```

- [ ] **Step 3: Run tests**
- [ ] **Step 4: Commit**

```bash
git commit -m "feat(lifecycle): add SnapshotScheduler orchestrator"
```

---

### Task 5: Integration — wire into existing Pipeline

**Files:**
- Modify: `internal/pipeline/pipeline.go` — add optional lifecycle mode
- Create: `internal/lifecycle/pipeline_integration.go` — adapter layer
- Create: `internal/lifecycle/pipeline_integration_test.go`

- [ ] **Step 1: Write integration test**

```go
func TestLifecycleModePipeline(t *testing.T) {
    // End-to-end: source emits events, consumer routes,
    // table goes through full lifecycle
    src := newMockSource()
    snk := newMockSink()
    
    scheduler := setupSchedulerWithTable(t, "db1.users")
    pipeline := NewLifecyclePipeline(src, snk, scheduler)
    
    ctx := context.Background()
    pipeline.Start(ctx)
    
    // Emit event while snapshotting → goes to cache
    src.EmitEvent(&event.ChangeEvent{Table: event.TableInfo{Database: "db1", Table: "users"}})
    time.Sleep(100 * time.Millisecond)
    assert.Equal(t, 0, snk.WriteCount())
    
    // Transition to streaming
    scheduler.OnSnapshotComplete("db1.users")
    scheduler.OnCaughtUp("db1.users")
    
    // Emit event while streaming → goes to sink
    src.EmitEvent(&event.ChangeEvent{Table: event.TableInfo{Database: "db1", Table: "users"}})
    time.Sleep(100 * time.Millisecond)
    assert.Equal(t, 1, snk.WriteCount())
    
    pipeline.Stop(ctx)
}
```

- [ ] **Step 2: Implement LifecyclePipeline adapter**

This is a thin adapter that:
1. Starts the source connector
2. Reads from source.Events()
3. Calls BinlogConsumer.Route() for each event
4. Manages the overall start/stop lifecycle

It does NOT replace the existing Pipeline — it's an alternative mode that can be enabled via config.

- [ ] **Step 3: Run tests**
- [ ] **Step 4: Commit**

```bash
git commit -m "feat(lifecycle): add pipeline integration adapter"
```

---

### Task 6: Full verification

- [ ] **Step 1: Full build**

Run: `go build ./...`

- [ ] **Step 2: Full tests**

Run: `go test ./... -count=1`
Expected: All packages pass

---

## Summary

| Task | What | Key Component |
|------|------|--------------|
| 1 | SchedulerConfig + HashChunker | `hash_chunker.go` |
| 2 | BinlogConsumer (event router) | `binlog_consumer.go` |
| 3 | CatchingUpReplayer | `catching_up_replayer.go` |
| 4 | SnapshotScheduler (orchestrator) | `snapshot_scheduler.go` |
| 5 | Pipeline integration adapter | `pipeline_integration.go` |
| 6 | Full verification | — |
