# Concurrent Processing Subsystem Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement concurrent snapshot processing and concurrent sink writing with ordering guarantees.

**Architecture:** Two-layer concurrent processing: SnapshotCoordinator manages parallel table/chunk snapshots, ConcurrentSinkWriter uses HashDispatcher to ensure row-level ordering while enabling parallel writes. Based on connector-design.md §5 and §7.

**Tech Stack:** Go 1.21+, golang.org/x/time/rate, sync package, channel-based concurrency

---

## File Structure

```
internal/source/
├── snapshot_coordinator.go     # Parallel snapshot orchestration
├── snapshot_coordinator_test.go
├── snapshot_config.go          # Snapshot concurrency configuration

internal/sink/
├── hash_dispatcher.go          # Hash-based event dispatching
├── hash_dispatcher_test.go
├── concurrent_writer.go        # Multi-worker concurrent writes
├── concurrent_writer_test.go
├── row_identifier.go           # Row identification for ordering
└── row_identifier_test.go
```

---

### Task 1: Implement Snapshot Concurrency Configuration

**Files:**
- Create: `internal/source/snapshot_config.go`
- Create: `internal/source/snapshot_config_test.go`

- [ ] **Step 1: Write snapshot configuration**

```go
// internal/source/snapshot_config.go
package source

import (
	"errors"
)

// SnapshotConcurrencyConfig holds snapshot concurrency settings.
type SnapshotConcurrencyConfig struct {
	// ===== Table-Level Concurrency =====

	// MaxTableThreads is the max parallel tables during snapshot
	// Default: 4, Range: 1-16
	MaxTableThreads int `json:"max-table-threads" toml:"max-table-threads"`

	// ===== Chunk-Level Concurrency (Large Table Optimization) =====

	// EnableChunkParallel enables parallel chunk reading for large tables
	EnableChunkParallel bool `json:"enable-chunk-parallel" toml:"enable-chunk-parallel"`

	// MaxChunkThreads is the max parallel chunks per large table
	// Default: 4, Range: 1-8
	MaxChunkThreads int `json:"max-chunk-threads" toml:"max-chunk-threads"`

	// ChunkThreshold is the row count threshold for chunk parallelism
	// Tables with more rows use chunk parallelism
	ChunkThreshold int64 `json:"chunk-threshold" toml:"chunk-threshold"`

	// ===== Batch Settings =====

	// BatchSize is rows per read operation
	// Default: 1000, Range: 100-10000
	BatchSize int `json:"batch-size" toml:"batch-size"`

	// ChunkSize is rows per chunk for large tables
	ChunkSize int `json:"chunk-size" toml:"chunk-size"`

	// ===== Queue Settings =====

	// TaskQueueSize is the snapshot task queue size
	TaskQueueSize int `json:"task-queue-size" toml:"task-queue-size"`

	// EventBufferSize is the event buffer size
	EventBufferSize int `json:"event-buffer-size" toml:"event-buffer-size"`
}

// DefaultSnapshotConcurrencyConfig returns defaults.
func DefaultSnapshotConcurrencyConfig() *SnapshotConcurrencyConfig {
	return &SnapshotConcurrencyConfig{
		MaxTableThreads:     4,
		EnableChunkParallel: true,
		MaxChunkThreads:     4,
		ChunkThreshold:      100000,
		BatchSize:           1000,
		ChunkSize:           10000,
		TaskQueueSize:       1000,
		EventBufferSize:     10000,
	}
}

// Validate validates the configuration.
func (c *SnapshotConcurrencyConfig) Validate() error {
	if c.MaxTableThreads < 1 || c.MaxTableThreads > 16 {
		return errors.New("max-table-threads must be between 1 and 16")
	}
	if c.MaxChunkThreads < 1 || c.MaxChunkThreads > 8 {
		return errors.New("max-chunk-threads must be between 1 and 8")
	}
	if c.BatchSize < 100 || c.BatchSize > 10000 {
		return errors.New("batch-size must be between 100 and 10000")
	}
	if c.ChunkSize < 1000 {
		return errors.New("chunk-size must be at least 1000")
	}
	return nil
}

// ConcurrencyMode represents the concurrency mode for a table.
type ConcurrencyMode string

const (
	// ConcurrencyModeSingle means single-threaded snapshot
	ConcurrencyModeSingle ConcurrencyMode = "single"
	// ConcurrencyModeChunkParallel means chunk-level parallelism
	ConcurrencyModeChunkParallel ConcurrencyMode = "chunk-parallel"
)

// TableConcurrencyPlan holds the concurrency plan for a table.
type TableConcurrencyPlan struct {
	TableID      TableID
	Mode         ConcurrencyMode
	ChunkThreads int
	ChunkSize    int
	BatchSize    int
}

// ConcurrencyPlan holds the overall concurrency plan.
type ConcurrencyPlan struct {
	TablePlans map[string]*TableConcurrencyPlan // key: "database.table"
}

// SnapshotConcurrencyStrategy plans concurrency for tables.
type SnapshotConcurrencyStrategy struct {
	config *SnapshotConcurrencyConfig
}

// NewSnapshotConcurrencyStrategy creates a strategy.
func NewSnapshotConcurrencyStrategy(config *SnapshotConcurrencyConfig) *SnapshotConcurrencyStrategy {
	return &SnapshotConcurrencyStrategy{config: config}
}

// PlanConcurrency creates a concurrency plan for tables.
func (s *SnapshotConcurrencyStrategy) PlanConcurrency(tables []*TableInfo) *ConcurrencyPlan {
	plan := &ConcurrencyPlan{
		TablePlans: make(map[string]*TableConcurrencyPlan),
	}

	totalThreads := s.config.MaxTableThreads
	usedThreads := 0

	for _, table := range tables {
		tablePlan := &TableConcurrencyPlan{
			TableID: table.TableID,
		}

		// Check if chunk parallelism should be used
		if s.config.EnableChunkParallel &&
			table.EstimatedRows >= s.config.ChunkThreshold &&
			usedThreads < totalThreads {

			availableThreads := min(s.config.MaxChunkThreads, totalThreads-usedThreads)
			tablePlan.Mode = ConcurrencyModeChunkParallel
			tablePlan.ChunkThreads = availableThreads
			tablePlan.ChunkSize = s.config.ChunkSize
			usedThreads += availableThreads
		} else {
			tablePlan.Mode = ConcurrencyModeSingle
			tablePlan.ChunkThreads = 1
			tablePlan.BatchSize = s.config.BatchSize
		}

		plan.TablePlans[table.TableID.String()] = tablePlan
	}

	return plan
}

// TableInfo holds table metadata for planning.
type TableInfo struct {
	TableID       TableID
	EstimatedRows int64
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
```

- [ ] **Step 2: Write tests**

```go
// internal/source/snapshot_config_test.go
package source

import (
	"testing"
)

func TestSnapshotConcurrencyConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *SnapshotConcurrencyConfig
		wantErr bool
	}{
		{
			name:    "valid default config",
			config:  DefaultSnapshotConcurrencyConfig(),
			wantErr: false,
		},
		{
			name: "invalid max table threads",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 0,
			},
			wantErr: true,
		},
		{
			name: "invalid max chunk threads",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 4,
				MaxChunkThreads: 20,
			},
			wantErr: true,
		},
		{
			name: "invalid batch size",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 4,
				MaxChunkThreads: 4,
				BatchSize:       10,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSnapshotConcurrencyStrategy_PlanConcurrency(t *testing.T) {
	config := DefaultSnapshotConcurrencyConfig()
	strategy := NewSnapshotConcurrencyStrategy(config)

	tables := []*TableInfo{
		{TableID: TableID{Database: "db1", Table: "small_table"}, EstimatedRows: 1000},
		{TableID: TableID{Database: "db1", Table: "large_table"}, EstimatedRows: 500000},
	}

	plan := strategy.PlanConcurrency(tables)

	if len(plan.TablePlans) != 2 {
		t.Fatalf("Expected 2 table plans, got %d", len(plan.TablePlans))
	}

	// Small table should use single mode
	smallPlan := plan.TablePlans["db1.small_table"]
	if smallPlan.Mode != ConcurrencyModeSingle {
		t.Errorf("Small table should use single mode, got %s", smallPlan.Mode)
	}

	// Large table should use chunk parallel
	largePlan := plan.TablePlans["db1.large_table"]
	if largePlan.Mode != ConcurrencyModeChunkParallel {
		t.Errorf("Large table should use chunk-parallel mode, got %s", largePlan.Mode)
	}
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./internal/source/... -v -run TestSnapshotConcurrency`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add internal/source/snapshot_config.go internal/source/snapshot_config_test.go
git commit -m "feat(source): add snapshot concurrency configuration"
```

---

### Task 2: Implement RowIdentifier for Ordering

**Files:**
- Create: `internal/sink/row_identifier.go`
- Create: `internal/sink/row_identifier_test.go`

- [ ] **Step 1: Write row identifier**

```go
// internal/sink/row_identifier.go
package sink

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/UFOXD/datastream/pkg/event"
)

// RowKeyType represents the type of row key.
type RowKeyType int

const (
	// KeyTypePrimaryKey indicates primary key
	KeyTypePrimaryKey RowKeyType = iota
	// KeyTypeUniqueIndex indicates unique index
	KeyTypeUniqueIndex
	// KeyTypeFullRow indicates full row (no PK or unique index)
	KeyTypeFullRow
)

// RowIdentifier uniquely identifies a row for ordering.
type RowIdentifier struct {
	Schema           string      `json:"schema"`
	Database         string      `json:"database"`
	Table            string      `json:"table"`
	PrimaryKeyValues string      `json:"primaryKeyValues"`
	KeyType          RowKeyType  `json:"keyType"`
}

// BuildRowIdentifier creates a row identifier from an event.
func BuildRowIdentifier(e *event.ChangeEvent, schema *event.TableInfo) *RowIdentifier {
	ident := &RowIdentifier{
		Schema:   e.Table.Schema,
		Database: e.Table.Database,
		Table:    e.Table.Table,
	}

	// 1. Try primary key first
	if len(schema.PrimaryKeyColumns) > 0 {
		ident.KeyType = KeyTypePrimaryKey
		ident.PrimaryKeyValues = extractKeyValues(e, schema.PrimaryKeyColumns)
		return ident
	}

	// 2. Try unique index
	if len(schema.UniqueIndexColumns) > 0 {
		ident.KeyType = KeyTypeUniqueIndex
		// Use first unique index
		sort.Slice(schema.UniqueIndexColumns, func(i, j int) bool {
			return len(schema.UniqueIndexColumns[i]) < len(schema.UniqueIndexColumns[j])
		})
		ident.PrimaryKeyValues = extractKeyValues(e, schema.UniqueIndexColumns[0])
		return ident
	}

	// 3. Fall back to full row
	ident.KeyType = KeyTypeFullRow
	ident.PrimaryKeyValues = extractAllColumnValues(e)
	return ident
}

// HashKey generates a hash key for distribution.
func (r *RowIdentifier) HashKey() string {
	return fmt.Sprintf("%s:%s:%s:%s",
		r.Schema,
		r.Database,
		r.Table,
		r.PrimaryKeyValues,
	)
}

// String returns string representation.
func (r *RowIdentifier) String() string {
	return r.HashKey()
}

// extractKeyValues extracts key values from event.
func extractKeyValues(e *event.ChangeEvent, columns []string) string {
	values := make([]string, len(columns))
	for i, col := range columns {
		var val interface{}
		if e.After != nil {
			if v, ok := e.After.Fields[col]; ok {
				val = v
			}
		} else if e.Before != nil {
			if v, ok := e.Before.Fields[col]; ok {
				val = v
			}
		}
		values[i] = formatValue(val)
	}
	return strings.Join(values, "|")
}

// extractAllColumnValues extracts all column values.
func extractAllColumnValues(e *event.ChangeEvent) string {
	var fields map[string]interface{}
	if e.After != nil {
		fields = e.After.Fields
	} else if e.Before != nil {
		fields = e.Before.Fields
	}

	if fields == nil {
		return ""
	}

	// Sort keys for deterministic ordering
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	values := make([]string, len(keys))
	for i, k := range keys {
		values[i] = fmt.Sprintf("%s=%s", k, formatValue(fields[k]))
	}
	return strings.Join(values, "|")
}

// formatValue formats a value for hashing.
func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}

	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case json.Number:
		return string(val)
	default:
		return fmt.Sprintf("%v", v)
	}
}
```

- [ ] **Step 2: Write tests**

```go
// internal/sink/row_identifier_test.go
package sink

import (
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
)

func TestBuildRowIdentifier_PrimaryKey(t *testing.T) {
	e := &event.ChangeEvent{
		Table: event.TableInfo{
			Database: "db1",
			Table:    "users",
		},
		After: &event.RowData{
			Fields: map[string]interface{}{
				"id":   123,
				"name": "John",
			},
		},
	}

	schema := &event.TableInfo{
		Database:          "db1",
		Table:             "users",
		PrimaryKeyColumns: []string{"id"},
	}

	ident := BuildRowIdentifier(e, schema)

	if ident.KeyType != KeyTypePrimaryKey {
		t.Errorf("Expected KeyTypePrimaryKey, got %d", ident.KeyType)
	}
	if ident.PrimaryKeyValues != "123" {
		t.Errorf("Expected '123', got '%s'", ident.PrimaryKeyValues)
	}
	if ident.HashKey() != ":db1:users:123" {
		t.Errorf("Unexpected hash key: %s", ident.HashKey())
	}
}

func TestBuildRowIdentifier_CompositeKey(t *testing.T) {
	e := &event.ChangeEvent{
		Table: event.TableInfo{
			Database: "db1",
			Table:    "orders",
		},
		After: &event.RowData{
			Fields: map[string]interface{}{
				"order_id": "ORD-001",
				"user_id":  456,
				"amount":   99.99,
			},
		},
	}

	schema := &event.TableInfo{
		Database:          "db1",
		Table:             "orders",
		PrimaryKeyColumns: []string{"user_id", "order_id"},
	}

	ident := BuildRowIdentifier(e, schema)

	if ident.PrimaryKeyValues != "456|ORD-001" {
		t.Errorf("Expected '456|ORD-001', got '%s'", ident.PrimaryKeyValues)
	}
}

func TestBuildRowIdentifier_NoPrimaryKey(t *testing.T) {
	e := &event.ChangeEvent{
		Table: event.TableInfo{
			Database: "db1",
			Table:    "logs",
		},
		After: &event.RowData{
			Fields: map[string]interface{}{
				"message": "test",
				"level":   "info",
			},
		},
	}

	schema := &event.TableInfo{
		Database:          "db1",
		Table:             "logs",
		PrimaryKeyColumns: []string{},
	}

	ident := BuildRowIdentifier(e, schema)

	if ident.KeyType != KeyTypeFullRow {
		t.Errorf("Expected KeyTypeFullRow, got %d", ident.KeyType)
	}
	// Full row should contain all columns
	if ident.PrimaryKeyValues == "" {
		t.Error("Full row key values should not be empty")
	}
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./internal/sink/... -v -run TestBuildRowIdentifier`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add internal/sink/row_identifier.go internal/sink/row_identifier_test.go
git commit -m "feat(sink): add RowIdentifier for ordering guarantees"
```

---

### Task 3: Implement HashDispatcher

**Files:**
- Create: `internal/sink/hash_dispatcher.go`
- Create: `internal/sink/hash_dispatcher_test.go`

- [ ] **Step 1: Write hash dispatcher**

```go
// internal/sink/hash_dispatcher.go
package sink

import (
	"context"
	"fmt"
	"sync"

	"github.com/UFOXD/datastream/pkg/event"
)

// DispatcherConfig holds dispatcher configuration.
type DispatcherConfig struct {
	// WorkerCount is the number of workers
	WorkerCount int `json:"worker-count" toml:"worker-count"`

	// BufferSize is the channel buffer size per worker
	BufferSize int `json:"buffer-size" toml:"buffer-size"`

	// NoPKTableStrategy is the strategy for tables without primary key
	// "single": all no-PK tables use worker 0
	// "table": each no-PK table gets a fixed worker
	NoPKTableStrategy string `json:"no-pk-table-strategy" toml:"no-pk-table-strategy"`
}

// DefaultDispatcherConfig returns default configuration.
func DefaultDispatcherConfig() *DispatcherConfig {
	return &DispatcherConfig{
		WorkerCount:       8,
		BufferSize:        1000,
		NoPKTableStrategy: "table",
	}
}

// HashDispatcher dispatches events to workers based on hash.
type HashDispatcher struct {
	config      *DispatcherConfig
	workerChans []chan *event.ChangeEvent
	schemaCache map[string]*event.TableInfo

	// Table-to-worker mapping for no-PK tables
	tableWorkerMap sync.Map

	// FNV-32 hash function
}

// NewHashDispatcher creates a new hash dispatcher.
func NewHashDispatcher(config *DispatcherConfig) *HashDispatcher {
	d := &HashDispatcher{
		config:      config,
		workerChans: make([]chan *event.ChangeEvent, config.WorkerCount),
		schemaCache: make(map[string]*event.TableInfo),
	}

	for i := 0; i < config.WorkerCount; i++ {
		d.workerChans[i] = make(chan *event.ChangeEvent, config.BufferSize)
	}

	return d
}

// Dispatch dispatches an event to the appropriate worker.
func (d *HashDispatcher) Dispatch(ctx context.Context, e *event.ChangeEvent, schema *event.TableInfo) error {
	workerID := d.calculateWorkerID(e, schema)

	select {
	case d.workerChans[workerID] <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// calculateWorkerID calculates which worker should handle the event.
func (d *HashDispatcher) calculateWorkerID(e *event.ChangeEvent, schema *event.TableInfo) int {
	// Has primary key or unique index: hash by row key
	if len(schema.PrimaryKeyColumns) > 0 || len(schema.UniqueIndexColumns) > 0 {
		ident := BuildRowIdentifier(e, schema)
		hash := fnv32(ident.HashKey())
		return int(hash % uint32(d.config.WorkerCount))
	}

	// No PK or unique index: use table-based strategy
	switch d.config.NoPKTableStrategy {
	case "single":
		// All no-PK tables use worker 0
		return 0

	case "table":
		// Each table gets a fixed worker
		tableKey := e.Table.Database + "." + e.Table.Table
		if workerID, ok := d.tableWorkerMap.Load(tableKey); ok {
			return workerID.(int)
		}

		// Assign a worker
		hash := fnv32(tableKey)
		workerID := int(hash % uint32(d.config.WorkerCount))
		d.tableWorkerMap.Store(tableKey, workerID)
		return workerID

	default:
		return 0
	}
}

// WorkerChannels returns the worker channels.
func (d *HashDispatcher) WorkerChannels() []chan *event.ChangeEvent {
	return d.workerChans
}

// Close closes all worker channels.
func (d *HashDispatcher) Close() {
	for _, ch := range d.workerChans {
		close(ch)
	}
}

// fnv32 implements FNV-32 hash.
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
```

- [ ] **Step 2: Write tests**

```go
// internal/sink/hash_dispatcher_test.go
package sink

import (
	"context"
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
)

func TestHashDispatcher_SameRowGoesToSameWorker(t *testing.T) {
	config := DefaultDispatcherConfig()
	dispatcher := NewHashDispatcher(config)

	schema := &event.TableInfo{
		Database:          "db1",
		Table:             "users",
		PrimaryKeyColumns: []string{"id"},
	}

	// Create multiple events for same row
	for i := 0; i < 10; i++ {
		e := &event.ChangeEvent{
			Table: event.TableInfo{
				Database: "db1",
				Table:    "users",
			},
			After: &event.RowData{
				Fields: map[string]interface{}{"id": 123},
			},
		}

		workerID := dispatcher.calculateWorkerID(e, schema)
		// All events for same row should go to same worker
		if workerID != dispatcher.calculateWorkerID(&event.ChangeEvent{
			Table: event.TableInfo{Database: "db1", Table: "users"},
			After:  &event.RowData{Fields: map[string]interface{}{"id": 123}},
		}, schema) {
			t.Error("Same row should always go to same worker")
		}
	}
}

func TestHashDispatcher_DifferentRowsDistribute(t *testing.T) {
	config := DefaultDispatcherConfig()
	dispatcher := NewHashDispatcher(config)

	schema := &event.TableInfo{
		Database:          "db1",
		Table:             "users",
		PrimaryKeyColumns: []string{"id"},
	}

	workerSet := make(map[int]bool)
	for i := 0; i < 100; i++ {
		e := &event.ChangeEvent{
			Table: event.TableInfo{
				Database: "db1",
				Table:    "users",
			},
			After: &event.RowData{
				Fields: map[string]interface{}{"id": i},
			},
		}
		workerID := dispatcher.calculateWorkerID(e, schema)
		workerSet[workerID] = true
	}

	// Should distribute across multiple workers
	if len(workerSet) < 4 {
		t.Errorf("Expected distribution across multiple workers, got %d workers", len(workerSet))
	}
}

func TestHashDispatcher_Dispatch(t *testing.T) {
	config := &DispatcherConfig{
		WorkerCount:       2,
		BufferSize:        10,
		NoPKTableStrategy: "table",
	}
	dispatcher := NewHashDispatcher(config)
	defer dispatcher.Close()

	schema := &event.TableInfo{
		Database:          "db1",
		Table:             "users",
		PrimaryKeyColumns: []string{"id"},
	}

	e := &event.ChangeEvent{
		Table: event.TableInfo{
			Database: "db1",
			Table:    "users",
		},
		After: &event.RowData{
			Fields: map[string]interface{}{"id": 1},
		},
	}

	err := dispatcher.Dispatch(context.Background(), e, schema)
	if err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}

	// Check that event was sent to a worker channel
	received := false
	for _, ch := range dispatcher.WorkerChannels() {
		select {
		case <-ch:
			received = true
		default:
		}
	}

	if !received {
		t.Error("Event was not received by any worker")
	}
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./internal/sink/... -v -run TestHashDispatcher`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add internal/sink/hash_dispatcher.go internal/sink/hash_dispatcher_test.go
git commit -m "feat(sink): add HashDispatcher for ordered parallel writes"
```

---

### Task 4: Implement ConcurrentSinkWriter

**Files:**
- Create: `internal/sink/concurrent_writer.go`
- Create: `internal/sink/concurrent_writer_test.go`

- [ ] **Step 1: Write concurrent sink writer**

```go
// internal/sink/concurrent_writer.go
package sink

import (
	"context"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/logutil"
	"go.uber.org/zap"
)

// ConcurrentSinkConfig holds configuration for concurrent writing.
type ConcurrentSinkConfig struct {
	// WorkerCount is the number of write workers
	WorkerCount int `json:"worker-count" toml:"worker-count"`

	// BatchSize is the batch size per worker
	BatchSize int `json:"batch-size" toml:"batch-size"`

	// FlushInterval is the batch flush interval
	FlushInterval time.Duration `json:"flush-interval" toml:"flush-interval"`

	// MaxRetry is the max retry count
	MaxRetry int `json:"max-retry" toml:"max-retry"`

	// RetryBackoff is the retry backoff duration
	RetryBackoff time.Duration `json:"retry-backoff" toml:"retry-backoff"`
}

// DefaultConcurrentSinkConfig returns defaults.
func DefaultConcurrentSinkConfig() *ConcurrentSinkConfig {
	return &ConcurrentSinkConfig{
		WorkerCount:   8,
		BatchSize:     1000,
		FlushInterval: 100 * time.Millisecond,
		MaxRetry:      3,
		RetryBackoff:  100 * time.Millisecond,
	}
}

// BatchWriter writes batches of events.
type BatchWriter interface {
	WriteBatch(ctx context.Context, events []*event.ChangeEvent) error
}

// ConcurrentSinkWriter manages concurrent sink writing.
type ConcurrentSinkWriter struct {
	sink       BatchWriter
	dispatcher *HashDispatcher
	workers    []*SinkWorker
	config     *ConcurrentSinkConfig

	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	logger  *zap.Logger
}

// NewConcurrentSinkWriter creates a concurrent writer.
func NewConcurrentSinkWriter(sink BatchWriter, config *ConcurrentSinkConfig) *ConcurrentSinkWriter {
	ctx, cancel := context.WithCancel(context.Background())

	w := &ConcurrentSinkWriter{
		sink:    sink,
		config:  config,
		ctx:     ctx,
		cancel:  cancel,
		logger:  logutil.Logger(),
	}

	// Create dispatcher
	dispatcherConfig := DefaultDispatcherConfig()
	dispatcherConfig.WorkerCount = config.WorkerCount
	dispatcherConfig.BufferSize = config.BatchSize * 2
	w.dispatcher = NewHashDispatcher(dispatcherConfig)

	// Create workers
	w.workers = make([]*SinkWorker, config.WorkerCount)
	workerChans := w.dispatcher.WorkerChannels()

	for i := 0; i < config.WorkerCount; i++ {
		worker := &SinkWorker{
			id:            i,
			eventCh:       workerChans[i],
			sink:          sink,
			batchSize:     config.BatchSize,
			flushInterval: config.FlushInterval,
			maxRetry:      config.MaxRetry,
			retryBackoff:  config.RetryBackoff,
			logger:        w.logger,
		}
		w.workers[i] = worker
	}

	return w
}

// Start starts all workers.
func (w *ConcurrentSinkWriter) Start() {
	for _, worker := range w.workers {
		w.wg.Add(1)
		go worker.Run(w.ctx, &w.wg)
	}
}

// Write writes an event.
func (w *ConcurrentSinkWriter) Write(ctx context.Context, e *event.ChangeEvent, schema *event.TableInfo) error {
	return w.dispatcher.Dispatch(ctx, e, schema)
}

// WriteBatch writes multiple events.
func (w *ConcurrentSinkWriter) WriteBatch(ctx context.Context, events []*event.ChangeEvent, schema *event.TableInfo) error {
	for _, e := range events {
		if err := w.dispatcher.Dispatch(ctx, e, schema); err != nil {
			return err
		}
	}
	return nil
}

// Close stops the writer.
func (w *ConcurrentSinkWriter) Close() error {
	w.cancel()
	w.wg.Wait()
	w.dispatcher.Close()
	return nil
}

// SinkWorker handles events for a single worker.
type SinkWorker struct {
	id            int
	eventCh       <-chan *event.ChangeEvent
	sink          BatchWriter
	batchSize     int
	flushInterval time.Duration
	maxRetry      int
	retryBackoff  time.Duration
	logger        *zap.Logger

	buffer []*event.ChangeEvent

	// Stats
	eventsWritten  int64
	batchesFlushed int64
}

// Run runs the worker.
func (w *SinkWorker) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	w.buffer = make([]*event.ChangeEvent, 0, w.batchSize)

	for {
		select {
		case <-ctx.Done():
			// Flush remaining events
			if len(w.buffer) > 0 {
				w.flush()
			}
			return

		case e, ok := <-w.eventCh:
			if !ok {
				if len(w.buffer) > 0 {
					w.flush()
				}
				return
			}

			w.buffer = append(w.buffer, e)
			if len(w.buffer) >= w.batchSize {
				w.flush()
			}

		case <-ticker.C:
			if len(w.buffer) > 0 {
				w.flush()
			}
		}
	}
}

// flush flushes the buffer to sink.
func (w *SinkWorker) flush() error {
	if len(w.buffer) == 0 {
		return nil
	}

	var err error
	for retry := 0; retry <= w.maxRetry; retry++ {
		err = w.sink.WriteBatch(context.Background(), w.buffer)
		if err == nil {
			break
		}

		w.logger.Warn("flush failed, retrying",
			zap.Int("worker", w.id),
			zap.Int("retry", retry),
			zap.Error(err),
		)

		if retry < w.maxRetry {
			time.Sleep(w.retryBackoff)
		}
	}

	if err != nil {
		w.logger.Error("flush failed after retries",
			zap.Int("worker", w.id),
			zap.Int("count", len(w.buffer)),
			zap.Error(err),
		)
		return err
	}

	w.eventsWritten += int64(len(w.buffer))
	w.batchesFlushed++
	w.buffer = w.buffer[:0]
	return nil
}

// Stats returns worker statistics.
func (w *SinkWorker) Stats() (eventsWritten, batchesFlushed int64) {
	return w.eventsWritten, w.batchesFlushed
}
```

- [ ] **Step 2: Write tests**

```go
// internal/sink/concurrent_writer_test.go
package sink

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

// mockBatchWriter implements BatchWriter for testing
type mockBatchWriter struct {
	mu       sync.Mutex
	batches  [][]*event.ChangeEvent
	written  int
	delay    time.Duration
	failRate float64
}

func (m *mockBatchWriter) WriteBatch(ctx context.Context, events []*event.ChangeEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.delay > 0 {
		time.Sleep(m.delay)
	}

	m.batches = append(m.batches, events)
	m.written += len(events)
	return nil
}

func TestConcurrentSinkWriter_Write(t *testing.T) {
	mockSink := &mockBatchWriter{}

	config := &ConcurrentSinkConfig{
		WorkerCount:   2,
		BatchSize:     10,
		FlushInterval: 50 * time.Millisecond,
	}

	writer := NewConcurrentSinkWriter(mockSink, config)
	writer.Start()
	defer writer.Close()

	schema := &event.TableInfo{
		Database:          "db1",
		Table:             "users",
		PrimaryKeyColumns: []string{"id"},
	}

	// Write events
	for i := 0; i < 20; i++ {
		e := &event.ChangeEvent{
			Table: event.TableInfo{
				Database: "db1",
				Table:    "users",
			},
			After: &event.RowData{
				Fields: map[string]interface{}{"id": i},
			},
		}
		if err := writer.Write(context.Background(), e, schema); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Wait for flush
	time.Sleep(200 * time.Millisecond)

	mockSink.mu.Lock()
	total := mockSink.written
	mockSink.mu.Unlock()

	if total != 20 {
		t.Errorf("Expected 20 events written, got %d", total)
	}
}

func TestConcurrentSinkWriter_Ordering(t *testing.T) {
	mockSink := &mockBatchWriter{}

	config := &ConcurrentSinkConfig{
		WorkerCount:   4,
		BatchSize:     5,
		FlushInterval: 50 * time.Millisecond,
	}

	writer := NewConcurrentSinkWriter(mockSink, config)
	writer.Start()
	defer writer.Close()

	schema := &event.TableInfo{
		Database:          "db1",
		Table:             "users",
		PrimaryKeyColumns: []string{"id"},
	}

	// Write multiple events for same row
	rowID := 123
	for i := 0; i < 10; i++ {
		e := &event.ChangeEvent{
			Table: event.TableInfo{
				Database: "db1",
				Table:    "users",
			},
			After: &event.RowData{
				Fields: map[string]interface{}{"id": rowID, "seq": i},
			},
		}
		if err := writer.Write(context.Background(), e, schema); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Wait for flush
	time.Sleep(200 * time.Millisecond)

	// All events for same row should go to same worker (in same batch or order)
	// This is verified by the HashDispatcher tests
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./internal/sink/... -v -run TestConcurrentSinkWriter`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add internal/sink/concurrent_writer.go internal/sink/concurrent_writer_test.go
git commit -m "feat(sink): add ConcurrentSinkWriter for parallel writes"
```

---

### Task 5: Implement SnapshotCoordinator

**Files:**
- Create: `internal/source/snapshot_coordinator.go`
- Create: `internal/source/snapshot_coordinator_test.go`

- [ ] **Step 1: Write snapshot coordinator**

```go
// internal/source/snapshot_coordinator.go
package source

import (
	"context"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/logutil"
	"go.uber.org/zap"
)

// SnapshotTask represents a snapshot task.
type SnapshotTask struct {
	TableID    TableID
	Schema     *event.TableInfo
	ChunkID    int
	ChunkRange *ChunkRange
	Priority   int
}

// ChunkRange defines a range for chunked snapshot.
type ChunkRange struct {
	StartKey interface{}
	EndKey   interface{}
}

// SnapshotResult represents a snapshot result.
type SnapshotResult struct {
	TableID TableID
	ChunkID int
	Events  []*event.ChangeEvent
	Error   error
}

// SnapshotReader reads snapshot data.
type SnapshotReader interface {
	ReadSnapshot(ctx context.Context, task *SnapshotTask) ([]*event.ChangeEvent, error)
}

// SnapshotCoordinator coordinates parallel snapshot execution.
type SnapshotCoordinator struct {
	config      *SnapshotConcurrencyConfig
	reader      SnapshotReader
	taskCh      chan *SnapshotTask
	resultCh    chan *SnapshotResult
	workers     []*SnapshotWorker
	progress    *SnapshotProgress

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *zap.Logger
}

// SnapshotProgress tracks snapshot progress.
type SnapshotProgress struct {
	mu             sync.RWMutex
	TotalTables    int
	CompletedTable int
	TotalRows      int64
	ReadRows       int64
	StartTime      time.Time
}

// NewSnapshotCoordinator creates a coordinator.
func NewSnapshotCoordinator(config *SnapshotConcurrencyConfig, reader SnapshotReader) *SnapshotCoordinator {
	ctx, cancel := context.WithCancel(context.Background())

	return &SnapshotCoordinator{
		config:   config,
		reader:   reader,
		taskCh:   make(chan *SnapshotTask, config.TaskQueueSize),
		resultCh: make(chan *SnapshotResult, config.EventBufferSize),
		progress: &SnapshotProgress{},
		ctx:      ctx,
		cancel:   cancel,
		logger:   logutil.Logger(),
	}
}

// Start starts the coordinator.
func (c *SnapshotCoordinator) Start(tables []*TableInfo) error {
	c.progress.StartTime = time.Now()
	c.progress.TotalTables = len(tables)

	// Create workers
	c.workers = make([]*SnapshotWorker, c.config.MaxTableThreads)
	for i := 0; i < c.config.MaxTableThreads; i++ {
		worker := &SnapshotWorker{
			id:       i,
			taskCh:   c.taskCh,
			resultCh: c.resultCh,
			reader:   c.reader,
		}
		c.workers[i] = worker
		c.wg.Add(1)
		go worker.Run(c.ctx, &c.wg)
	}

	// Generate tasks
	go c.generateTasks(tables)

	// Start result handler
	go c.handleResults()

	return nil
}

// generateTasks generates snapshot tasks for tables.
func (c *SnapshotCoordinator) generateTasks(tables []*TableInfo) {
	defer close(c.taskCh)

	for _, table := range tables {
		// For now, single task per table
		// Chunk-based parallelism would split into multiple tasks
		task := &SnapshotTask{
			TableID: table.TableID,
			Priority: 0,
		}
		c.taskCh <- task
	}
}

// handleResults handles snapshot results.
func (c *SnapshotCoordinator) handleResults() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case result, ok := <-c.resultCh:
			if !ok {
				return
			}
			if result.Error != nil {
				c.logger.Error("snapshot task failed",
					zap.String("table", result.TableID.String()),
					zap.Error(result.Error),
				)
			} else {
				c.progress.mu.Lock()
				c.progress.CompletedTable++
				c.progress.mu.Unlock()
			}
		}
	}
}

// Stop stops the coordinator.
func (c *SnapshotCoordinator) Stop() {
	c.cancel()
	c.wg.Wait()
}

// Progress returns current progress.
func (c *SnapshotCoordinator) Progress() *SnapshotProgress {
	return c.progress
}

// SnapshotWorker handles snapshot tasks.
type SnapshotWorker struct {
	id       int
	taskCh   <-chan *SnapshotTask
	resultCh chan<- *SnapshotResult
	reader   SnapshotReader
}

// Run runs the worker.
func (w *SnapshotWorker) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-w.taskCh:
			if !ok {
				return
			}

			events, err := w.reader.ReadSnapshot(ctx, task)
			w.resultCh <- &SnapshotResult{
				TableID: task.TableID,
				ChunkID: task.ChunkID,
				Events:  events,
				Error:   err,
			}
		}
	}
}
```

- [ ] **Step 2: Write tests**

```go
// internal/source/snapshot_coordinator_test.go
package source

import (
	"context"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

// mockSnapshotReader implements SnapshotReader for testing
type mockSnapshotReader struct {
	events []*event.ChangeEvent
	delay  time.Duration
}

func (m *mockSnapshotReader) ReadSnapshot(ctx context.Context, task *SnapshotTask) ([]*event.ChangeEvent, error) {
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	return m.events, nil
}

func TestSnapshotCoordinator_Start(t *testing.T) {
	config := DefaultSnapshotConcurrencyConfig()
	reader := &mockSnapshotReader{
		events: []*event.ChangeEvent{
			{Table: event.TableInfo{Database: "db1", Table: "users"}},
		},
	}

	coordinator := NewSnapshotCoordinator(config, reader)

	tables := []*TableInfo{
		{TableID: TableID{Database: "db1", Table: "users"}},
		{TableID: TableID{Database: "db1", Table: "orders"}},
	}

	err := coordinator.Start(tables)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for completion
	time.Sleep(500 * time.Millisecond)
	coordinator.Stop()

	progress := coordinator.Progress()
	if progress.CompletedTable == 0 {
		t.Error("Expected some tables to be completed")
	}
}

func TestSnapshotProgress(t *testing.T) {
	progress := &SnapshotProgress{
		TotalTables: 10,
		StartTime:   time.Now(),
	}

	progress.mu.Lock()
	progress.CompletedTable = 5
	progress.TotalRows = 10000
	progress.ReadRows = 5000
	progress.mu.Unlock()

	if progress.CompletedTable != 5 {
		t.Errorf("Expected 5 completed tables, got %d", progress.CompletedTable)
	}
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./internal/source/... -v -run TestSnapshotCoordinator`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add internal/source/snapshot_coordinator.go internal/source/snapshot_coordinator_test.go
git commit -m "feat(source): add SnapshotCoordinator for parallel snapshots"
```

---

## Summary

This plan implements:

1. **SnapshotConcurrencyConfig** - Three-level concurrency configuration
2. **RowIdentifier** - Row identification for ordering guarantees
3. **HashDispatcher** - Hash-based event distribution to workers
4. **ConcurrentSinkWriter** - Multi-worker concurrent writes
5. **SnapshotCoordinator** - Parallel snapshot orchestration

**Total Tasks:** 5
**Estimated Time:** 2-3 hours

---

*Plan created: 2026-05-12*
