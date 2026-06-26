package lifecycle

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/UFOXD/datastream/internal/cache"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
)

// --- mock types ---

type mockCacheBackend struct {
	mu     sync.Mutex
	writes map[string][]*cache.CacheEvent
}

func newMockCacheBackend() *mockCacheBackend {
	return &mockCacheBackend{writes: make(map[string][]*cache.CacheEvent)}
}

func (m *mockCacheBackend) Write(_ context.Context, tableID string, ev *cache.CacheEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes[tableID] = append(m.writes[tableID], ev)
	return nil
}
func (m *mockCacheBackend) WriteBatch(_ context.Context, tableID string, events []*cache.CacheEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes[tableID] = append(m.writes[tableID], events...)
	return nil
}
func (m *mockCacheBackend) Read(_ context.Context, _ string, _ string, _ int64) cache.ReadResult {
	return cache.ReadResult{Events: make(chan *cache.CacheEvent), Err: make(chan error)}
}
func (m *mockCacheBackend) Delete(_ context.Context, _ string) error   { return nil }
func (m *mockCacheBackend) Size(_ context.Context, _ string) (int64, error) { return 0, nil }
func (m *mockCacheBackend) TotalSize(_ context.Context) (int64, error) { return 0, nil }
func (m *mockCacheBackend) Exists(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (m *mockCacheBackend) Sync(_ context.Context, _ string) error { return nil }
func (m *mockCacheBackend) TruncateToLastComplete(_ context.Context, _ string) (*event.Position, error) {
	return nil, nil
}
func (m *mockCacheBackend) Close() error { return nil }

func (m *mockCacheBackend) writesFor(tableID string) []*cache.CacheEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writes[tableID]
}

type mockEventSink struct {
	mu     sync.Mutex
	events []*event.ChangeEvent
}

func (m *mockEventSink) Write(_ context.Context, events []*event.ChangeEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, events...)
	return nil
}

func (m *mockEventSink) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

// --- helpers ---

func makeChangeEvent(db, table, txID string, seqNo int) *event.ChangeEvent {
	return &event.ChangeEvent{
		Type: event.EventTypeInsert,
		Table: event.TableInfo{
			Database: db,
			Table:    table,
		},
		Position: event.Position{
			TxID:  txID,
			SeqNo: seqNo,
		},
		Timestamp: time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC),
	}
}

func setupStore(t *testing.T, taskID string, tables map[source.TableID]source.TableState) source.TableLifecycleStore {
	t.Helper()
	store := source.NewMemoryLifecycleStore()
	ctx := context.Background()
	for tid, state := range tables {
		lc := source.NewTableLifecycle(tid)
		switch state {
		case source.TableStatePending:
		case source.TableStateSnapshotting:
			lc.TransitionTo(source.TableStateSnapshotting, nil)
		case source.TableStateCatchingUp:
			lc.TransitionTo(source.TableStateSnapshotting, nil)
			lc.TransitionTo(source.TableStateCatchingUp, nil)
		case source.TableStateStreaming:
			lc.TransitionTo(source.TableStateSnapshotting, nil)
			lc.TransitionTo(source.TableStateCatchingUp, nil)
			lc.TransitionTo(source.TableStateStreaming, nil)
		case source.TableStateError:
			lc.SetError("test error")
		case source.TableStatePaused:
			lc.TransitionTo(source.TableStateSnapshotting, nil)
			lc.TransitionTo(source.TableStateCatchingUp, nil)
			lc.Pause()
		}
		store.Save(ctx, taskID, lc)
	}
	return store
}

// --- tests ---

func TestBinlogConsumerRoutesToCache(t *testing.T) {
	taskID := "task-1"
	tid := source.TableID{Database: "mydb", Table: "users"}
	store := setupStore(t, taskID, map[source.TableID]source.TableState{
		tid: source.TableStateSnapshotting,
	})
	cb := newMockCacheBackend()
	sink := &mockEventSink{}

	consumer := NewBinlogConsumer(taskID, store, cb, sink, cache.SourceTypeMySQLGTID, nil, nil, nil, nil, nil)

	ev := makeChangeEvent("mydb", "users", "gtid-abc", 1)
	if err := consumer.Route(context.Background(), ev); err != nil {
		t.Fatalf("Route returned error: %v", err)
	}

	if got := cb.writesFor("mydb.users"); len(got) != 1 {
		t.Fatalf("expected 1 cache write, got %d", len(got))
	}
	if sink.count() != 0 {
		t.Fatalf("expected 0 sink writes, got %d", sink.count())
	}

	ce := cb.writesFor("mydb.users")[0]
	if ce.TxID != "gtid-abc" {
		t.Errorf("CacheEvent.TxID = %q, want %q", ce.TxID, "gtid-abc")
	}
	if ce.EventSeq != 1 {
		t.Errorf("CacheEvent.EventSeq = %d, want 1", ce.EventSeq)
	}
	if ce.TimestampMs != ev.Timestamp.UnixMilli() {
		t.Errorf("CacheEvent.TimestampMs = %d, want %d", ce.TimestampMs, ev.Timestamp.UnixMilli())
	}
	if len(ce.Payload) == 0 {
		t.Error("CacheEvent.Payload is empty")
	}
}

func TestBinlogConsumerRoutesToSink(t *testing.T) {
	taskID := "task-1"
	tid := source.TableID{Database: "mydb", Table: "orders"}
	store := setupStore(t, taskID, map[source.TableID]source.TableState{
		tid: source.TableStateStreaming,
	})
	cb := newMockCacheBackend()
	sink := &mockEventSink{}

	consumer := NewBinlogConsumer(taskID, store, cb, sink, cache.SourceTypeMySQLGTID, nil, nil, nil, nil, nil)

	ev := makeChangeEvent("mydb", "orders", "gtid-xyz", 2)
	if err := consumer.Route(context.Background(), ev); err != nil {
		t.Fatalf("Route returned error: %v", err)
	}

	if sink.count() != 1 {
		t.Fatalf("expected 1 sink write, got %d", sink.count())
	}
	if got := cb.writesFor("mydb.orders"); len(got) != 0 {
		t.Fatalf("expected 0 cache writes, got %d", len(got))
	}
}

func TestBinlogConsumerDiscardsPending(t *testing.T) {
	taskID := "task-1"
	tid := source.TableID{Database: "mydb", Table: "pending_tbl"}
	store := setupStore(t, taskID, map[source.TableID]source.TableState{
		tid: source.TableStatePending,
	})
	cb := newMockCacheBackend()
	sink := &mockEventSink{}

	consumer := NewBinlogConsumer(taskID, store, cb, sink, cache.SourceTypeMySQLGTID, nil, nil, nil, nil, nil)

	ev := makeChangeEvent("mydb", "pending_tbl", "gtid-111", 1)
	if err := consumer.Route(context.Background(), ev); err != nil {
		t.Fatalf("Route returned error: %v", err)
	}

	if sink.count() != 0 {
		t.Fatalf("expected 0 sink writes, got %d", sink.count())
	}
	if got := cb.writesFor("mydb.pending_tbl"); len(got) != 0 {
		t.Fatalf("expected 0 cache writes, got %d", len(got))
	}
}

func TestBinlogConsumerDiscardsUnknownTable(t *testing.T) {
	taskID := "task-1"
	store := setupStore(t, taskID, map[source.TableID]source.TableState{})
	cb := newMockCacheBackend()
	sink := &mockEventSink{}

	consumer := NewBinlogConsumer(taskID, store, cb, sink, cache.SourceTypeMySQLGTID, nil, nil, nil, nil, nil)

	ev := makeChangeEvent("mydb", "unknown_tbl", "gtid-222", 1)
	if err := consumer.Route(context.Background(), ev); err != nil {
		t.Fatalf("Route returned error: %v", err)
	}

	if sink.count() != 0 {
		t.Fatalf("expected 0 sink writes, got %d", sink.count())
	}
	if got := cb.writesFor("mydb.unknown_tbl"); len(got) != 0 {
		t.Fatalf("expected 0 cache writes, got %d", len(got))
	}
}

func TestBinlogConsumerCatchingUpRoutesToSink(t *testing.T) {
	taskID := "task-1"
	tid := source.TableID{Database: "mydb", Table: "catching_tbl"}
	store := setupStore(t, taskID, map[source.TableID]source.TableState{
		tid: source.TableStateCatchingUp,
	})
	cb := newMockCacheBackend()
	sink := &mockEventSink{}

	consumer := NewBinlogConsumer(taskID, store, cb, sink, cache.SourceTypeMySQLGTID, nil, nil, nil, nil, nil)

	ev := makeChangeEvent("mydb", "catching_tbl", "gtid-333", 3)
	if err := consumer.Route(context.Background(), ev); err != nil {
		t.Fatalf("Route returned error: %v", err)
	}

	if sink.count() != 1 {
		t.Fatalf("expected 1 sink write, got %d", sink.count())
	}
	if got := cb.writesFor("mydb.catching_tbl"); len(got) != 0 {
		t.Fatalf("expected 0 cache writes, got %d", len(got))
	}
}
