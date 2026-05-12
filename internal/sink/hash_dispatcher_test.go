package sink

import (
	"context"
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
)

// schemaWithPK returns a TableInfo that has a primary key.
func schemaWithPK(db, table string, pkCols ...string) *event.TableInfo {
	return &event.TableInfo{
		Database:          db,
		Schema:            "public",
		Table:             table,
		PrimaryKeyColumns: pkCols,
	}
}

// schemaWithUK returns a TableInfo that has a unique key but no PK.
func schemaWithUK(db, table string, ukCols ...string) *event.TableInfo {
	return &event.TableInfo{
		Database:         db,
		Schema:           "public",
		Table:            table,
		UniqueKeyColumns: ukCols,
	}
}

// schemaNoPK returns a TableInfo with no PK or unique index.
func schemaNoPK(db, table string) *event.TableInfo {
	return &event.TableInfo{
		Database: db,
		Schema:   "public",
		Table:    table,
	}
}

// eventWithPK creates a ChangeEvent whose After row contains the given fields.
func eventWithAfter(fields map[string]interface{}) *event.ChangeEvent {
	rd := event.NewRowData()
	for k, v := range fields {
		rd.Set(k, v, "string")
	}
	return &event.ChangeEvent{
		Type:  event.EventTypeInsert,
		After: *rd,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHashDispatcher_SameRowGoesToSameWorker
// Consistency: the same logical row must always be routed to the same worker.
// ─────────────────────────────────────────────────────────────────────────────

func TestHashDispatcher_SameRowGoesToSameWorker(t *testing.T) {
	cfg := &DispatcherConfig{
		WorkerCount:       8,
		BufferSize:        64,
		NoPKTableStrategy: NoPKStrategyTable,
	}
	d := NewHashDispatcher(cfg)

	schema := schemaWithPK("testdb", "users", "id")

	// Two events that represent different operations on the same PK value.
	e1 := eventWithAfter(map[string]interface{}{"id": int64(42), "name": "Alice"})
	e2 := eventWithAfter(map[string]interface{}{"id": int64(42), "name": "Alice-updated"})

	w1 := d.calculateWorkerID(e1, schema)
	w2 := d.calculateWorkerID(e2, schema)

	if w1 != w2 {
		t.Errorf("same PK row must go to the same worker: got %d and %d", w1, w2)
	}

	// Unique-key table should also be consistent.
	schemaUK := schemaWithUK("testdb", "emails", "email")
	eu1 := eventWithAfter(map[string]interface{}{"email": "alice@example.com", "name": "Alice"})
	eu2 := eventWithAfter(map[string]interface{}{"email": "alice@example.com", "name": "Alice-v2"})

	wu1 := d.calculateWorkerID(eu1, schemaUK)
	wu2 := d.calculateWorkerID(eu2, schemaUK)

	if wu1 != wu2 {
		t.Errorf("same UK row must go to the same worker: got %d and %d", wu1, wu2)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHashDispatcher_DifferentRowsDistribute
// Distribution: different rows should (with high probability) land on different
// workers when the worker count is large relative to the test set.
// ─────────────────────────────────────────────────────────────────────────────

func TestHashDispatcher_DifferentRowsDistribute(t *testing.T) {
	const workerCount = 8
	cfg := &DispatcherConfig{
		WorkerCount:       workerCount,
		BufferSize:        64,
		NoPKTableStrategy: NoPKStrategyTable,
	}
	d := NewHashDispatcher(cfg)
	schema := schemaWithPK("testdb", "orders", "id")

	seen := make(map[int]bool)
	for i := int64(0); i < 100; i++ {
		e := eventWithAfter(map[string]interface{}{"id": i})
		seen[d.calculateWorkerID(e, schema)] = true
	}

	// With 100 distinct rows and 8 workers, we expect all workers to be used.
	if len(seen) < workerCount {
		t.Errorf("expected all %d workers to be used, only %d were hit", workerCount, len(seen))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHashDispatcher_NoPKStrategySingle
// All rows from a no-PK table must go to worker 0 when strategy is "single".
// ─────────────────────────────────────────────────────────────────────────────

func TestHashDispatcher_NoPKStrategySingle(t *testing.T) {
	cfg := &DispatcherConfig{
		WorkerCount:       4,
		BufferSize:        64,
		NoPKTableStrategy: NoPKStrategySingle,
	}
	d := NewHashDispatcher(cfg)
	schema := schemaNoPK("testdb", "logs")

	for i := 0; i < 10; i++ {
		e := eventWithAfter(map[string]interface{}{"msg": i})
		if got := d.calculateWorkerID(e, schema); got != 0 {
			t.Errorf("NoPKStrategySingle: expected worker 0, got %d", got)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHashDispatcher_NoPKStrategyTable
// All rows from the same no-PK table must go to the same worker, but different
// tables may go to different workers.
// ─────────────────────────────────────────────────────────────────────────────

func TestHashDispatcher_NoPKStrategyTable(t *testing.T) {
	cfg := &DispatcherConfig{
		WorkerCount:       4,
		BufferSize:        64,
		NoPKTableStrategy: NoPKStrategyTable,
	}
	d := NewHashDispatcher(cfg)

	sA := schemaNoPK("testdb", "table_a")
	sB := schemaNoPK("testdb", "table_b")

	// All rows from table_a must land on the same worker.
	var wA int
	for i := 0; i < 5; i++ {
		e := eventWithAfter(map[string]interface{}{"x": i})
		w := d.calculateWorkerID(e, sA)
		if i == 0 {
			wA = w
		} else if w != wA {
			t.Errorf("NoPKStrategyTable: table_a rows should all go to worker %d, got %d", wA, w)
		}
	}

	// All rows from table_b must land on the same worker.
	var wB int
	for i := 0; i < 5; i++ {
		e := eventWithAfter(map[string]interface{}{"x": i})
		w := d.calculateWorkerID(e, sB)
		if i == 0 {
			wB = w
		} else if w != wB {
			t.Errorf("NoPKStrategyTable: table_b rows should all go to worker %d, got %d", wB, w)
		}
	}
	// Note: wA == wB is allowed (hash collision), so we don't assert inequality.
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHashDispatcher_Dispatch
// Actual channel dispatch: events land in the right worker channel.
// ─────────────────────────────────────────────────────────────────────────────

func TestHashDispatcher_Dispatch(t *testing.T) {
	cfg := &DispatcherConfig{
		WorkerCount:       4,
		BufferSize:        16,
		NoPKTableStrategy: NoPKStrategyTable,
	}
	d := NewHashDispatcher(cfg)
	schema := schemaWithPK("testdb", "users", "id")

	ctx := context.Background()

	// Dispatch 20 events.
	const total = 20
	for i := int64(0); i < total; i++ {
		e := eventWithAfter(map[string]interface{}{"id": i, "name": "user"})
		if err := d.Dispatch(ctx, e, schema); err != nil {
			t.Fatalf("Dispatch failed: %v", err)
		}
	}

	// Count how many events are in all channels.
	d.Close()
	got := 0
	for _, ch := range d.WorkerChannels() {
		got += len(ch)
	}
	if got != total {
		t.Errorf("expected %d events across workers, got %d", total, got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHashDispatcher_DispatchContextCancelled
// Dispatch must return ctx.Err() when the context is cancelled and the channel
// is full (blocking scenario).
// ─────────────────────────────────────────────────────────────────────────────

func TestHashDispatcher_DispatchContextCancelled(t *testing.T) {
	cfg := &DispatcherConfig{
		WorkerCount:       1,
		BufferSize:        1, // tiny buffer — will fill immediately
		NoPKTableStrategy: NoPKStrategySingle,
	}
	d := NewHashDispatcher(cfg)
	schema := schemaNoPK("testdb", "logs")

	// Fill the single channel completely.
	ctx := context.Background()
	e := eventWithAfter(map[string]interface{}{"msg": "fill"})
	if err := d.Dispatch(ctx, e, schema); err != nil {
		t.Fatalf("first dispatch failed unexpectedly: %v", err)
	}

	// Now use an already-cancelled context; dispatch must fail.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	e2 := eventWithAfter(map[string]interface{}{"msg": "should-fail"})
	err := d.Dispatch(cancelled, e2, schema)
	if err == nil {
		t.Error("expected an error from cancelled context, got nil")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHashDispatcher_WorkerChannelsCount
// WorkerChannels must return exactly WorkerCount channels.
// ─────────────────────────────────────────────────────────────────────────────

func TestHashDispatcher_WorkerChannelsCount(t *testing.T) {
	const count = 6
	d := NewHashDispatcher(&DispatcherConfig{
		WorkerCount:       count,
		BufferSize:        8,
		NoPKTableStrategy: NoPKStrategyTable,
	})
	if n := len(d.WorkerChannels()); n != count {
		t.Errorf("expected %d worker channels, got %d", count, n)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestHashDispatcher_DefaultConfig
// DefaultDispatcherConfig must return sensible non-zero values.
// ─────────────────────────────────────────────────────────────────────────────

func TestHashDispatcher_DefaultConfig(t *testing.T) {
	cfg := DefaultDispatcherConfig()
	if cfg.WorkerCount <= 0 {
		t.Errorf("WorkerCount should be positive, got %d", cfg.WorkerCount)
	}
	if cfg.BufferSize <= 0 {
		t.Errorf("BufferSize should be positive, got %d", cfg.BufferSize)
	}
	if cfg.NoPKTableStrategy == "" {
		t.Error("NoPKTableStrategy should not be empty")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestFnv32
// Basic sanity checks for the FNV-32 hash helper.
// ─────────────────────────────────────────────────────────────────────────────

func TestFnv32(t *testing.T) {
	// Same input → same output (determinism).
	if fnv32("hello") != fnv32("hello") {
		t.Error("fnv32 must be deterministic")
	}
	// Different inputs → (very likely) different outputs.
	if fnv32("hello") == fnv32("world") {
		t.Error("fnv32(\"hello\") == fnv32(\"world\") — unexpected collision in test")
	}
	// Empty string must not panic.
	_ = fnv32("")
}
