package lifecycle

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err)
	defer cacheBackend.Close()

	scheduler := NewSnapshotScheduler(DefaultSchedulerConfig(), "task-1", store, cacheBackend)

	pipeline := NewLifecyclePipeline(src, []sink.Connector{snk}, scheduler, cacheBackend, store, "task-1", cache.SourceTypeMySQLGTID)

	ctx := context.Background()
	require.NoError(t, pipeline.Start(ctx))
	assert.True(t, src.started)

	require.NoError(t, pipeline.Stop(ctx))
	assert.True(t, src.stopped)
}

func TestLifecyclePipelineRoutesEvents(t *testing.T) {
	src := newMockLifecycleSource()
	snk := newMockLifecycleSink()
	store := source.NewMemoryLifecycleStore()
	dir := t.TempDir()
	cacheBackend, err := cache.NewLocalBackend(dir, cache.SyncModeNone)
	require.NoError(t, err)
	defer cacheBackend.Close()

	scheduler := NewSnapshotScheduler(DefaultSchedulerConfig(), "task-1", store, cacheBackend)

	// Add table and transition to streaming state.
	tid := source.TableID{Database: "db1", Table: "users"}
	require.NoError(t, scheduler.AddTable(tid, &event.Position{TxID: "uuid:1", CommitTime: time.Now()}))

	lc, err := store.Get(context.Background(), "task-1", tid)
	require.NoError(t, err)
	require.NoError(t, lc.TransitionTo(source.TableStateSnapshotting, &event.Position{TxID: "uuid:1"}))
	require.NoError(t, lc.TransitionTo(source.TableStateCatchingUp, nil))
	require.NoError(t, lc.TransitionTo(source.TableStateStreaming, nil))
	require.NoError(t, store.Save(context.Background(), "task-1", lc))

	pipeline := NewLifecyclePipeline(src, []sink.Connector{snk}, scheduler, cacheBackend, store, "task-1", cache.SourceTypeMySQLGTID)

	ctx := context.Background()
	require.NoError(t, pipeline.Start(ctx))

	// Send an event for the streaming table.
	src.events <- &event.ChangeEvent{
		Table:     event.TableInfo{Database: "db1", Table: "users"},
		Timestamp: time.Now(),
	}
	time.Sleep(200 * time.Millisecond)

	// The event should arrive at the sink since the table is in streaming state.
	assert.GreaterOrEqual(t, snk.writeCount(), 1)

	require.NoError(t, pipeline.Stop(ctx))
}
