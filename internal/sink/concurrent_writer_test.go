package sink

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

// mockBatchWriter records every event written to it, thread-safely.
type mockBatchWriter struct {
	mu     sync.Mutex
	events []*event.ChangeEvent
	// If failUntil > 0 the writer fails that many times before succeeding.
	failUntil int
	calls     int
}

func (m *mockBatchWriter) WriteBatch(_ context.Context, events []*event.ChangeEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls <= m.failUntil {
		return ErrWriteFailed.GenWithStack("mock failure %d", m.calls)
	}
	m.events = append(m.events, events...)
	return nil
}

func (m *mockBatchWriter) Written() []*event.ChangeEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*event.ChangeEvent, len(m.events))
	copy(out, m.events)
	return out
}

func (m *mockBatchWriter) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

// orderedBatchWriter records the events in the exact order they arrive per-worker.
// Because a single worker processes events sequentially, we can verify ordering.
type orderedBatchWriter struct {
	mu     sync.Mutex
	events []*event.ChangeEvent
}

func (o *orderedBatchWriter) WriteBatch(_ context.Context, events []*event.ChangeEvent) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, events...)
	return nil
}

func (o *orderedBatchWriter) Written() []*event.ChangeEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]*event.ChangeEvent, len(o.events))
	copy(out, o.events)
	return out
}

// makeSinkEvent creates a ChangeEvent with a given sequence tag stored in Metadata.
func makeSinkEvent(seqTag string, fields map[string]interface{}) *event.ChangeEvent {
	rd := event.NewRowData()
	for k, v := range fields {
		rd.Set(k, v, "string")
	}
	return &event.ChangeEvent{
		Type:     event.EventTypeInsert,
		After:    *rd,
		Metadata: map[string]string{"seq": seqTag},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestConcurrentSinkWriter_Write
// Write N events and verify all of them are delivered to the sink.
// ─────────────────────────────────────────────────────────────────────────────

func TestConcurrentSinkWriter_Write(t *testing.T) {
	mock := &mockBatchWriter{}
	cfg := &ConcurrentSinkConfig{
		WorkerCount:   2,
		BatchSize:     10,
		FlushInterval: 50 * time.Millisecond,
		MaxRetry:      0,
		RetryBackoff:  10 * time.Millisecond,
	}
	w := NewConcurrentSinkWriter(mock, cfg)
	w.Start()

	schema := schemaWithPK("testdb", "users", "id")
	ctx := context.Background()

	const total = 50
	for i := int64(0); i < total; i++ {
		e := makeSinkEvent("", map[string]interface{}{"id": i, "name": "user"})
		if err := w.Write(ctx, e, schema); err != nil {
			t.Fatalf("Write(%d) failed: %v", i, err)
		}
	}

	// Close triggers a final flush and waits for workers to finish.
	if err := w.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	got := mock.Count()
	if got != total {
		t.Errorf("expected %d events written, got %d", total, got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestConcurrentSinkWriter_WriteBatch
// WriteBatch dispatches multiple events and all should arrive.
// ─────────────────────────────────────────────────────────────────────────────

func TestConcurrentSinkWriter_WriteBatch(t *testing.T) {
	mock := &mockBatchWriter{}
	cfg := &ConcurrentSinkConfig{
		WorkerCount:   4,
		BatchSize:     20,
		FlushInterval: 50 * time.Millisecond,
		MaxRetry:      0,
		RetryBackoff:  10 * time.Millisecond,
	}
	w := NewConcurrentSinkWriter(mock, cfg)
	w.Start()

	schema := schemaWithPK("testdb", "orders", "order_id")
	ctx := context.Background()

	events := make([]*event.ChangeEvent, 30)
	for i := int64(0); i < 30; i++ {
		events[i] = makeSinkEvent("", map[string]interface{}{"order_id": i})
	}

	if err := w.WriteBatch(ctx, events, schema); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	if got := mock.Count(); got != 30 {
		t.Errorf("expected 30 events, got %d", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestConcurrentSinkWriter_Ordering
// Events for the same row (same PK value) must arrive at the sink in the
// same order they were written, because they are always routed to the same
// worker which processes them sequentially.
// ─────────────────────────────────────────────────────────────────────────────

func TestConcurrentSinkWriter_Ordering(t *testing.T) {
	ord := &orderedBatchWriter{}
	cfg := &ConcurrentSinkConfig{
		WorkerCount:   4,
		BatchSize:     5,
		FlushInterval: 20 * time.Millisecond,
		MaxRetry:      0,
		RetryBackoff:  5 * time.Millisecond,
	}
	w := NewConcurrentSinkWriter(ord, cfg)
	w.Start()

	schema := schemaWithPK("testdb", "items", "id")
	ctx := context.Background()

	// Send 20 sequential events all with the same PK (id=1).
	// They must all go to the same worker and arrive in order.
	const seqLen = 20
	for i := 0; i < seqLen; i++ {
		e := makeSinkEvent(string(rune('A'+i)), map[string]interface{}{"id": int64(1), "seq": i})
		if err := w.Write(ctx, e, schema); err != nil {
			t.Fatalf("Write(%d) failed: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	written := ord.Written()
	// Filter only the events for id=1 by checking the seq metadata tag is set.
	// All events sent had id=1, so all written events belong to that row.
	var id1Events []*event.ChangeEvent
	for _, ev := range written {
		val, ok := ev.After.Get("id")
		if ok && val == int64(1) {
			id1Events = append(id1Events, ev)
		}
	}

	if len(id1Events) != seqLen {
		t.Fatalf("expected %d events for id=1, got %d", seqLen, len(id1Events))
	}

	// Verify ordering: seq field must be strictly increasing.
	for i, ev := range id1Events {
		seqVal, _ := ev.After.Get("seq")
		if seqVal != i {
			t.Errorf("ordering violation at position %d: expected seq=%d, got %v", i, i, seqVal)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestConcurrentSinkWriter_DefaultConfig
// DefaultConcurrentSinkConfig returns non-zero values.
// ─────────────────────────────────────────────────────────────────────────────

func TestConcurrentSinkWriter_DefaultConfig(t *testing.T) {
	cfg := DefaultConcurrentSinkConfig()
	if cfg.WorkerCount <= 0 {
		t.Errorf("WorkerCount should be positive, got %d", cfg.WorkerCount)
	}
	if cfg.BatchSize <= 0 {
		t.Errorf("BatchSize should be positive, got %d", cfg.BatchSize)
	}
	if cfg.FlushInterval <= 0 {
		t.Errorf("FlushInterval should be positive, got %v", cfg.FlushInterval)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestConcurrentSinkWriter_Stats
// Stats() returns the aggregate event/batch counts.
// ─────────────────────────────────────────────────────────────────────────────

func TestConcurrentSinkWriter_Stats(t *testing.T) {
	mock := &mockBatchWriter{}
	cfg := &ConcurrentSinkConfig{
		WorkerCount:   2,
		BatchSize:     5,
		FlushInterval: 20 * time.Millisecond,
		MaxRetry:      0,
		RetryBackoff:  5 * time.Millisecond,
	}
	w := NewConcurrentSinkWriter(mock, cfg)
	w.Start()

	schema := schemaWithPK("testdb", "stats_table", "id")
	ctx := context.Background()

	const total = 10
	for i := int64(0); i < total; i++ {
		e := makeSinkEvent("", map[string]interface{}{"id": i})
		if err := w.Write(ctx, e, schema); err != nil {
			t.Fatalf("Write(%d) failed: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	written, flushed := w.Stats()
	if written != total {
		t.Errorf("expected eventsWritten=%d, got %d", total, written)
	}
	if flushed == 0 {
		t.Error("expected at least 1 batch flushed, got 0")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestConcurrentSinkWriter_RetryOnFailure
// When the sink fails the first attempt, the worker retries and succeeds.
// ─────────────────────────────────────────────────────────────────────────────

func TestConcurrentSinkWriter_RetryOnFailure(t *testing.T) {
	// Fail exactly once; succeed on retry.
	mock := &mockBatchWriter{failUntil: 1}
	cfg := &ConcurrentSinkConfig{
		WorkerCount:   1,
		BatchSize:     10,
		FlushInterval: 20 * time.Millisecond,
		MaxRetry:      3,
		RetryBackoff:  5 * time.Millisecond,
	}
	w := NewConcurrentSinkWriter(mock, cfg)
	w.Start()

	schema := schemaWithPK("testdb", "retry_table", "id")
	ctx := context.Background()

	for i := int64(0); i < 5; i++ {
		e := makeSinkEvent("", map[string]interface{}{"id": i})
		if err := w.Write(ctx, e, schema); err != nil {
			t.Fatalf("Write(%d) failed: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	if got := mock.Count(); got != 5 {
		t.Errorf("expected 5 events after retry, got %d", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TestConcurrentSinkWriter_CancelledContext
// Write should return an error when the provided context is cancelled and
// the worker channels are full.
// ─────────────────────────────────────────────────────────────────────────────

func TestConcurrentSinkWriter_CancelledContext(t *testing.T) {
	// Use a stalling sink so workers never drain their channels.
	var stall atomic.Int32
	stall.Store(1)

	stallSink := &stallBatchWriter{stall: &stall}
	cfg := &ConcurrentSinkConfig{
		WorkerCount:   1,
		BatchSize:     1,
		FlushInterval: 10 * time.Millisecond,
		MaxRetry:      0,
		RetryBackoff:  5 * time.Millisecond,
	}
	w := NewConcurrentSinkWriter(stallSink, cfg)
	// Override dispatcher buffer to 1 so it fills immediately.
	dispCfg := &DispatcherConfig{WorkerCount: 1, BufferSize: 1, NoPKTableStrategy: NoPKStrategySingle}
	w.dispatcher = NewHashDispatcher(dispCfg)
	workerCh := w.dispatcher.WorkerChannels()[0]
	w.workers[0].eventCh = workerCh
	w.Start()

	schema := schemaNoPK("testdb", "stall_table")

	// Fill the buffer.
	bgCtx := context.Background()
	e1 := makeSinkEvent("", map[string]interface{}{"x": 1})
	_ = w.Write(bgCtx, e1, schema)

	// Now cancel and expect error.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	e2 := makeSinkEvent("", map[string]interface{}{"x": 2})
	err := w.Write(cancelled, e2, schema)
	if err == nil {
		t.Error("expected error from cancelled context, got nil")
	}

	// Unblock workers and close cleanly.
	stall.Store(0)
	w.Close() //nolint:errcheck
}

// stallBatchWriter blocks until stall is set to 0.
type stallBatchWriter struct {
	stall *atomic.Int32
}

func (s *stallBatchWriter) WriteBatch(ctx context.Context, _ []*event.ChangeEvent) error {
	for s.stall.Load() != 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	return nil
}
