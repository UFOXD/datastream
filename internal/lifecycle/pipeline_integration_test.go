package lifecycle

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UFOXD/datastream/internal/cache"
	"github.com/UFOXD/datastream/internal/sink"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
)

// --- mock source ---

type mockLifecycleSource struct {
	events  chan *event.ChangeEvent
	errors  chan error
	started bool
	stopped bool
}

func newMockLifecycleSource() *mockLifecycleSource {
	return &mockLifecycleSource{
		events: make(chan *event.ChangeEvent, 100),
		errors: make(chan error, 10),
	}
}

func (m *mockLifecycleSource) Name() string                       { return "mock-lifecycle-source" }
func (m *mockLifecycleSource) Initialize(_ context.Context, _ source.Config) error { return nil }
func (m *mockLifecycleSource) Start(_ context.Context) error      { m.started = true; return nil }
func (m *mockLifecycleSource) Stop(_ context.Context) error       { m.stopped = true; return nil }
func (m *mockLifecycleSource) Status() source.Status              { return source.Status{State: source.StateRunning} }
func (m *mockLifecycleSource) Events() <-chan *event.ChangeEvent  { return m.events }
func (m *mockLifecycleSource) Errors() <-chan error                { return m.errors }
func (m *mockLifecycleSource) GetPosition() *event.Position       { return nil }
func (m *mockLifecycleSource) SetPosition(_ *event.Position) error { return nil }
func (m *mockLifecycleSource) GetSchema(_, _ string) (*event.TableInfo, error) { return nil, nil }
func (m *mockLifecycleSource) Schemas() map[string]*event.TableInfo { return nil }
func (m *mockLifecycleSource) SyncScope() *source.SyncScope       { return nil }
func (m *mockLifecycleSource) AddTables(_ context.Context, _ []string) error    { return nil }
func (m *mockLifecycleSource) RemoveTables(_ context.Context, _ []string) error { return nil }
func (m *mockLifecycleSource) ListTables() []string                { return nil }

// Compile-time check.
var _ source.Connector = (*mockLifecycleSource)(nil)

// --- mock sink ---

type mockLifecycleSink struct {
	writes  int32
	started bool
	stopped bool
}

func newMockLifecycleSink() *mockLifecycleSink {
	return &mockLifecycleSink{}
}

func (m *mockLifecycleSink) Name() string                                   { return "mock-lifecycle-sink" }
func (m *mockLifecycleSink) Initialize(_ context.Context, _ sink.Config) error { return nil }
func (m *mockLifecycleSink) Start(_ context.Context) error                  { m.started = true; return nil }
func (m *mockLifecycleSink) Stop(_ context.Context) error                   { m.stopped = true; return nil }
func (m *mockLifecycleSink) Status() sink.Status                            { return sink.Status{State: sink.StateReady} }
func (m *mockLifecycleSink) Write(_ context.Context, events []*event.ChangeEvent) error {
	atomic.AddInt32(&m.writes, int32(len(events)))
	return nil
}
func (m *mockLifecycleSink) Flush(_ context.Context) error                  { return nil }
func (m *mockLifecycleSink) GetPosition() *event.Position                   { return nil }
func (m *mockLifecycleSink) SupportsDDL() bool                              { return false }
func (m *mockLifecycleSink) ApplyDDL(_ context.Context, _ *event.ChangeEvent) error { return nil }
func (m *mockLifecycleSink) SupportsTransaction() bool                      { return false }

func (m *mockLifecycleSink) writeCount() int {
	return int(atomic.LoadInt32(&m.writes))
}

// Compile-time check.
var _ sink.Connector = (*mockLifecycleSink)(nil)

// --- tests ---

func TestLifecyclePipelineStartStop(t *testing.T) {
	src := newMockLifecycleSource()
	snk := newMockLifecycleSink()
	store := source.NewMemoryLifecycleStore()
	dir := t.TempDir()
	cacheBackend, err := cache.NewLocalBackend(dir, cache.SyncModeNone)
	if err != nil {
		t.Fatal(err)
	}
	defer cacheBackend.Close()

	scheduler := NewSnapshotScheduler(DefaultSchedulerConfig(), "task-1", store, cacheBackend)

	pipeline := NewLifecyclePipeline(src, []sink.Connector{snk}, scheduler, cacheBackend, store, "task-1", cache.SourceTypeMySQLGTID, nil)

	ctx := context.Background()
	if err := pipeline.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if !src.started {
		t.Error("source not started")
	}

	if err := pipeline.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if !src.stopped {
		t.Error("source not stopped")
	}
}

func TestLifecyclePipelineRoutesEvents(t *testing.T) {
	src := newMockLifecycleSource()
	snk := newMockLifecycleSink()
	store := source.NewMemoryLifecycleStore()
	dir := t.TempDir()
	cacheBackend, err := cache.NewLocalBackend(dir, cache.SyncModeNone)
	if err != nil {
		t.Fatal(err)
	}
	defer cacheBackend.Close()

	scheduler := NewSnapshotScheduler(DefaultSchedulerConfig(), "task-1", store, cacheBackend)

	// Add table and transition to streaming state.
	tid := source.TableID{Database: "db1", Table: "users"}
	if err := scheduler.AddTable(tid, &event.Position{TxID: "uuid:1", CommitTime: time.Now()}); err != nil {
		t.Fatal(err)
	}

	lc, err := store.Get(context.Background(), "task-1", tid)
	if err != nil {
		t.Fatal(err)
	}
	if err := lc.TransitionTo(source.TableStateSnapshotting, &event.Position{TxID: "uuid:1"}); err != nil {
		t.Fatal(err)
	}
	if err := lc.TransitionTo(source.TableStateCatchingUp, nil); err != nil {
		t.Fatal(err)
	}
	if err := lc.TransitionTo(source.TableStateStreaming, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), "task-1", lc); err != nil {
		t.Fatal(err)
	}

	pipeline := NewLifecyclePipeline(src, []sink.Connector{snk}, scheduler, cacheBackend, store, "task-1", cache.SourceTypeMySQLGTID, nil)

	ctx := context.Background()
	if err := pipeline.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// Send an event for the streaming table.
	src.events <- &event.ChangeEvent{
		Table:     event.TableInfo{Database: "db1", Table: "users"},
		Timestamp: time.Now(),
	}
	time.Sleep(200 * time.Millisecond)

	// The event should arrive at the sink since the table is in streaming state.
	if snk.writeCount() < 1 {
		t.Errorf("expected at least 1 sink write, got %d", snk.writeCount())
	}

	if err := pipeline.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestBinlogConsumerFlushWaitsForPendingDML verifies that flushPending
// blocks until all in-flight DML writes complete.
func TestBinlogConsumerFlushWaitsForPendingDML(t *testing.T) {
	taskID := "task-1"
	tid := source.TableID{Database: "mydb", Table: "users"}
	lstore := setupStore(t, taskID, map[source.TableID]source.TableState{
		tid: source.TableStateStreaming,
	})
	cb := newMockCacheBackend()

	// Slow sink that blocks until signaled.
	writeStarted := make(chan struct{})
	unblockWrite := make(chan struct{})
	slowSink := &slowEventSink{
		writeStarted: writeStarted,
		unblockWrite: unblockWrite,
	}

	consumer := NewBinlogConsumer(taskID, lstore, cb, slowSink, cache.SourceTypeMySQLGTID, nil, nil, nil, nil, nil)

	ctx := context.Background()

	// Start a DML write in a goroutine (simulates async DML).
	go func() {
		ev := makeChangeEvent("mydb", "users", "gtid-1", 1)
		_ = consumer.Route(ctx, ev)
	}()

	// Wait for the write to start.
	<-writeStarted

	// flushPending should block until the DML write completes.
	done := make(chan struct{})
	go func() {
		consumer.flushPending()
		close(done)
	}()

	// Verify flushPending is still blocking.
	select {
	case <-done:
		t.Fatal("flushPending returned before DML write completed")
	case <-time.After(100 * time.Millisecond):
		// Expected: still blocking.
	}

	// Unblock the DML write.
	close(unblockWrite)

	// Now flushPending should return.
	select {
	case <-done:
		// Expected: flushPending returned after DML completed.
	case <-time.After(2 * time.Second):
		t.Fatal("flushPending did not return within timeout after unblock")
	}
}

// slowEventSink is a sink that signals when a write starts and blocks
// until unblockWrite is closed.
type slowEventSink struct {
	writeStarted chan struct{}
	unblockWrite chan struct{}
}

func (s *slowEventSink) Write(_ context.Context, _ []*event.ChangeEvent) error {
	select {
	case s.writeStarted <- struct{}{}:
	default:
	}
	<-s.unblockWrite
	return nil
}

// TestBinlogConsumerDDLEventDiscardedWhenNoDeps verifies that DDL events
// are silently discarded when DDL dependencies are not configured.
func TestBinlogConsumerDDLEventDiscardedWhenNoDeps(t *testing.T) {
	taskID := "task-1"
	lstore := source.NewMemoryLifecycleStore()
	cb := newMockCacheBackend()
	sink := &mockEventSink{}

	consumer := NewBinlogConsumer(taskID, lstore, cb, sink, cache.SourceTypeMySQLGTID, nil, nil, nil, nil, nil)

	ev := &event.ChangeEvent{
		Type: event.EventTypeDDL,
		Table: event.TableInfo{
			Database: "mydb",
			Table:    "users",
		},
		Metadata: map[string]string{
			"ddl": "ALTER TABLE users ADD COLUMN age INT",
		},
		Timestamp: time.Now(),
	}

	err := consumer.Route(context.Background(), ev)
	if err != nil {
		t.Fatalf("expected nil error for DDL without deps, got: %v", err)
	}
	if sink.count() != 0 {
		t.Fatalf("expected 0 sink writes, got %d", sink.count())
	}
}
