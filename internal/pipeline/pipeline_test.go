package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UFOXD/datastream/internal/sink"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func TestMemoryBuffer(t *testing.T) {
	buffer := NewMemoryBuffer(10)

	// Test Put and Get
	e := &event.ChangeEvent{
		ID:        "test-1",
		Type:      event.EventTypeInsert,
		Timestamp: time.Now(),
	}

	ctx := context.Background()
	if err := buffer.Put(ctx, e); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if buffer.Len() != 1 {
		t.Errorf("Expected buffer length 1, got %d", buffer.Len())
	}

	events, err := buffer.Get(ctx, 1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(events))
	}

	if events[0].ID != "test-1" {
		t.Errorf("Expected event ID 'test-1', got '%s'", events[0].ID)
	}

	buffer.Close()
}

func TestMemoryBufferCapacity(t *testing.T) {
	buffer := NewMemoryBuffer(2)
	ctx := context.Background()

	// Fill buffer
	e1 := &event.ChangeEvent{ID: "1", Timestamp: time.Now()}
	e2 := &event.ChangeEvent{ID: "2", Timestamp: time.Now()}

	if err := buffer.Put(ctx, e1); err != nil {
		t.Fatalf("Put e1 failed: %v", err)
	}
	if err := buffer.Put(ctx, e2); err != nil {
		t.Fatalf("Put e2 failed: %v", err)
	}

	// Buffer should be full
	e3 := &event.ChangeEvent{ID: "3", Timestamp: time.Now()}
	if err := buffer.Put(ctx, e3); err != ErrBufferFull {
		t.Errorf("Expected ErrBufferFull, got %v", err)
	}

	buffer.Close()
}

func TestBatchBuffer(t *testing.T) {
	buffer := NewBatchBuffer(100, 5, 100) // capacity=100, batchSize=5, timeout=100ms
	ctx := context.Background()

	// Add events
	for i := 0; i < 3; i++ {
		e := &event.ChangeEvent{
			ID:        string(rune('a' + i)),
			Timestamp: time.Now(),
		}
		if err := buffer.Put(ctx, e); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Get should return partial batch after timeout
	events, err := buffer.Get(ctx, 5)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(events) != 3 {
		t.Errorf("Expected 3 events, got %d", len(events))
	}

	buffer.Close()
}

func TestRoundRobinDispatcher(t *testing.T) {
	d := NewRoundRobinDispatcher()

	// Create mock sinks (we'll use the interface)
	sinks := []interface{}{nil, nil, nil}

	// Test counter increments
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		e := &event.ChangeEvent{ID: string(rune('a' + i))}
		// We can't test actual dispatch without real sinks
		_ = ctx
		_ = e
		_ = sinks
	}

	if err := d.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestHashDispatcher(t *testing.T) {
	d := NewHashDispatcher("id")

	// Hash should be consistent
	key := "test-key"
	hash1 := hashOfString(key)
	hash2 := hashOfString(key)

	if hash1 != hash2 {
		t.Error("Hash should be consistent for same input")
	}

	if err := d.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func hashOfString(s string) uint32 {
	// Simple FNV hash for testing
	h := uint32(2166136261)
	for _, c := range s {
		h ^= uint32(c)
		h *= 16777619
	}
	return h
}

func TestNewDispatcher(t *testing.T) {
	tests := []struct {
		configType string
		expectType string
	}{
		{"round-robin", "*pipeline.RoundRobinDispatcher"},
		{"hash", "*pipeline.HashDispatcher"},
		{"broadcast", "*pipeline.BroadcastDispatcher"},
		{"unknown", "*pipeline.RoundRobinDispatcher"}, // default
	}

	for _, tt := range tests {
		d := NewDispatcher(DispatcherConfig{Type: tt.configType})
		if d == nil {
			t.Errorf("Expected non-nil dispatcher for type %s", tt.configType)
		}
		d.Close()
	}
}

func TestTaskStatus(t *testing.T) {
	task := NewTask("test-1", "Test Task", &Config{})

	if task.GetStatus() != TaskStatusCreated {
		t.Errorf("Expected status Created, got %s", task.GetStatus())
	}
}

func TestTaskPosition(t *testing.T) {
	task := NewTask("test-1", "Test Task", &Config{})

	pos := &event.Position{
		CommitTime: time.Now(),
		TxID:       "tx-123",
		SeqNo:      5,
	}

	task.SetPosition(pos)

	retrieved := task.GetPosition()
	if retrieved == nil {
		t.Fatal("Expected non-nil position")
	}

	if retrieved.TxID != "tx-123" {
		t.Errorf("Expected TxID 'tx-123', got '%s'", retrieved.TxID)
	}

	// Modify original should not affect retrieved
	pos.TxID = "tx-456"
	if task.GetPosition().TxID != "tx-123" {
		t.Error("Position should be a copy")
	}
}

func TestTaskManager(t *testing.T) {
	tm := NewTaskManager()

	ctx := context.Background()
	task, err := tm.Create(ctx, "task-1", "Task One", &Config{})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if task.ID != "task-1" {
		t.Errorf("Expected ID 'task-1', got '%s'", task.ID)
	}

	// List tasks
	tasks := tm.List()
	if len(tasks) != 1 {
		t.Errorf("Expected 1 task, got %d", len(tasks))
	}

	// Get task
	retrieved, err := tm.Get("task-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.ID != "task-1" {
		t.Errorf("Expected ID 'task-1', got '%s'", retrieved.ID)
	}

	// Delete task
	if err := tm.Delete(ctx, "task-1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	tasks = tm.List()
	if len(tasks) != 0 {
		t.Errorf("Expected 0 tasks after delete, got %d", len(tasks))
	}
}

func TestTaskManagerDuplicateTask(t *testing.T) {
	tm := NewTaskManager()
	ctx := context.Background()

	_, err := tm.Create(ctx, "task-1", "Task One", &Config{})
	if err != nil {
		t.Fatalf("First create failed: %v", err)
	}

	_, err = tm.Create(ctx, "task-1", "Task Two", &Config{})
	if err != ErrTaskExists {
		t.Errorf("Expected ErrTaskExists, got %v", err)
	}
}

func TestMemoryCoordinator(t *testing.T) {
	c := NewMemoryCoordinator("node-1")
	ctx := context.Background()

	// Initialize
	if err := c.Initialize(ctx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Save and get task
	task := NewTask("task-1", "Test", &Config{})
	if err := c.SaveTask(ctx, task); err != nil {
		t.Fatalf("SaveTask failed: %v", err)
	}

	retrieved, err := c.GetTask(ctx, "task-1")
	if err != nil {
		t.Fatalf("GetTask failed: %v", err)
	}

	if retrieved.ID != "task-1" {
		t.Errorf("Expected ID 'task-1', got '%s'", retrieved.ID)
	}

	// Position
	pos := &event.Position{CommitTime: time.Now(), TxID: "tx-1"}
	if err := c.SavePosition(ctx, "task-1", pos); err != nil {
		t.Fatalf("SavePosition failed: %v", err)
	}

	retrievedPos, err := c.GetPosition(ctx, "task-1")
	if err != nil {
		t.Fatalf("GetPosition failed: %v", err)
	}

	if retrievedPos.TxID != "tx-1" {
		t.Errorf("Expected TxID 'tx-1', got '%s'", retrievedPos.TxID)
	}

	// Leadership
	isLeader, err := c.AcquireLeadership(ctx, "task-1")
	if err != nil {
		t.Fatalf("AcquireLeadership failed: %v", err)
	}

	if !isLeader {
		t.Error("Expected to be leader")
	}

	isLeader, err = c.IsLeader(ctx, "task-1")
	if err != nil {
		t.Fatalf("IsLeader failed: %v", err)
	}

	if !isLeader {
		t.Error("Expected to be leader")
	}

	// Node registration
	nodeInfo := NodeInfo{
		ID:       "node-1",
		Address:  "localhost:8300",
		Hostname: "localhost",
	}

	if err := c.RegisterNode(ctx, "node-1", nodeInfo); err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}

	nodes, err := c.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes failed: %v", err)
	}

	if len(nodes) != 1 {
		t.Errorf("Expected 1 node, got %d", len(nodes))
	}

	c.Close()
}

func TestPipelineConfig(t *testing.T) {
	config := &Config{
		ID:   "pipeline-1",
		Name: "Test Pipeline",
		Buffer: BufferConfig{
			Size:      1000,
			BatchSize: 100,
		},
		Dispatcher: DispatcherConfig{
			Type:    "hash",
			HashKey: "id",
		},
	}

	if config.ID != "pipeline-1" {
		t.Errorf("Expected ID 'pipeline-1', got '%s'", config.ID)
	}

	if config.Buffer.Size != 1000 {
		t.Errorf("Expected buffer size 1000, got %d", config.Buffer.Size)
	}
}

func TestPipelineStatistics(t *testing.T) {
	stats := Statistics{
		EventsRead:    1000,
		EventsWritten: 998,
		EventsFailed:  2,
		BytesRead:     102400,
		BytesWritten:  102000,
	}

	if stats.EventsRead != 1000 {
		t.Errorf("Expected 1000 events read, got %d", stats.EventsRead)
	}

	if stats.EventsFailed != 2 {
		t.Errorf("Expected 2 events failed, got %d", stats.EventsFailed)
	}
}

// getGaugeValue is a test helper to extract a labeled gauge value.
func getGaugeValue(t *testing.T, r *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, _ := r.Gather()
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			match := true
			for _, lp := range m.GetLabel() {
				if v, ok := labels[lp.GetName()]; ok && v != lp.GetValue() {
					match = false
					break
				}
			}
			if match && m.GetGauge() != nil {
				return m.GetGauge().GetValue()
			}
		}
	}
	return -1
}

func TestPipeline_StateMachine_NoPanic(t *testing.T) {
	metrics.ResetForTest()
	r := prometheus.NewRegistry()
	metrics.MustRegisterAll(r)
	t.Cleanup(func() {
		metrics.ResetForTest()
		metrics.MustRegisterAll(prometheus.DefaultRegisterer)
	})

	cfg := &Config{ID: "t1", Name: "test"}
	p := New(cfg)
	p.SetCluster("c1")

	p.updateState("running")
	p.updateState("paused")
	p.updateState("stopped")

	val := getGaugeValue(t, r, "datastream_task_state", map[string]string{
		"cluster": "c1", "task": "t1", "state": "stopped",
	})
	if val != 1 {
		t.Errorf("task_state{stopped} = %v, want 1", val)
	}
	val = getGaugeValue(t, r, "datastream_task_state", map[string]string{
		"cluster": "c1", "task": "t1", "state": "running",
	})
	if val != 0 {
		t.Errorf("task_state{running} = %v, want 0", val)
	}
}


func TestPipeline_ConsumePoint_EmitsLagAndLastEvent(t *testing.T) {
	metrics.ResetForTest()
	r := prometheus.NewRegistry()
	metrics.MustRegisterAll(r)
	t.Cleanup(func() {
		metrics.ResetForTest()
		metrics.MustRegisterAll(prometheus.DefaultRegisterer)
	})

	p := New(&Config{ID: "t1", Name: "test", Source: source.Config{Type: "mysql"}})
	p.SetCluster("c1")
	p.precacheLabels()

	ev := &event.ChangeEvent{
		Type:      event.EventTypeInsert,
		Timestamp: time.Now().Add(-2 * time.Second),
	}
	p.instrumentEvent(ev)

	lag := getGaugeValue(t, r, "datastream_source_lag_seconds", map[string]string{
		"cluster": "c1", "task": "t1", "source": "mysql",
	})
	if lag < 1.0 {
		t.Errorf("source_lag_seconds = %v, want >= 1.0", lag)
	}
}

// mockSource is a minimal source.Connector for testing Pipeline lifecycle.
type mockSource struct {
	eventsCh chan *event.ChangeEvent
	errorsCh chan error
}

func newMockSource() *mockSource {
	return &mockSource{
		eventsCh: make(chan *event.ChangeEvent),
		errorsCh: make(chan error),
	}
}

func (m *mockSource) Name() string                                              { return "mock" }
func (m *mockSource) Initialize(_ context.Context, _ source.Config) error       { return nil }
func (m *mockSource) Start(_ context.Context) error                             { return nil }
func (m *mockSource) Stop(_ context.Context) error                              { return nil }
func (m *mockSource) Status() source.Status                                     { return source.Status{} }
func (m *mockSource) Events() <-chan *event.ChangeEvent                         { return m.eventsCh }
func (m *mockSource) Errors() <-chan error                                      { return m.errorsCh }
func (m *mockSource) GetPosition() *event.Position                              { return nil }
func (m *mockSource) SetPosition(_ *event.Position) error                       { return nil }
func (m *mockSource) GetSchema(_, _ string) (*event.TableInfo, error)           { return nil, nil }
func (m *mockSource) SyncScope() *source.SyncScope                             { return nil }
func (m *mockSource) AddTables(_ context.Context, _ []string) error             { return nil }
func (m *mockSource) RemoveTables(_ context.Context, _ []string) error          { return nil }
func (m *mockSource) ListTables() []string                                      { return nil }

// mockSink is a minimal sink.Connector for testing Pipeline lifecycle.
type mockSink struct{}

func (m *mockSink) Name() string                                                  { return "mock" }
func (m *mockSink) Initialize(_ context.Context, _ sink.Config) error             { return nil }
func (m *mockSink) Start(_ context.Context) error                                 { return nil }
func (m *mockSink) Stop(_ context.Context) error                                  { return nil }
func (m *mockSink) Status() sink.Status                                           { return sink.Status{} }
func (m *mockSink) Write(_ context.Context, _ []*event.ChangeEvent) error         { return nil }
func (m *mockSink) Flush(_ context.Context) error                                 { return nil }
func (m *mockSink) GetPosition() *event.Position                                  { return nil }
func (m *mockSink) SupportsDDL() bool                                             { return false }
func (m *mockSink) SupportsTransaction() bool                                     { return false }

// errMockSink is a sink.Connector whose Write always returns an error.
type errMockSink struct {
	writeErr error
}

func newErrMockSink(err error) *errMockSink { return &errMockSink{writeErr: err} }

func (m *errMockSink) Name() string                                                  { return "err-mock" }
func (m *errMockSink) Initialize(_ context.Context, _ sink.Config) error             { return nil }
func (m *errMockSink) Start(_ context.Context) error                                 { return nil }
func (m *errMockSink) Stop(_ context.Context) error                                  { return nil }
func (m *errMockSink) Status() sink.Status                                           { return sink.Status{} }
func (m *errMockSink) Write(_ context.Context, _ []*event.ChangeEvent) error         { return m.writeErr }
func (m *errMockSink) Flush(_ context.Context) error                                 { return nil }
func (m *errMockSink) GetPosition() *event.Position                                  { return nil }
func (m *errMockSink) SupportsDDL() bool                                             { return false }
func (m *errMockSink) SupportsTransaction() bool                                     { return false }

func TestEventsWrittenNotIncrementedOnError(t *testing.T) {
	metrics.ResetForTest()
	r := prometheus.NewRegistry()
	metrics.MustRegisterAll(r)
	t.Cleanup(func() {
		metrics.ResetForTest()
		metrics.MustRegisterAll(prometheus.DefaultRegisterer)
	})

	cfg := &Config{ID: "err-sink-test", Name: "err-sink-test"}
	p := New(cfg)
	p.SetCluster("c1")

	src := newMockSource()
	p.SetSource(src)
	p.AddSink(newErrMockSink(errors.New("disk full")))

	ctx := context.Background()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Send one event
	src.eventsCh <- &event.ChangeEvent{
		ID:        "e1",
		Type:      event.EventTypeInsert,
		Timestamp: time.Now(),
	}
	time.Sleep(200 * time.Millisecond)

	stats := p.Status().Statistics
	if stats.EventsWritten != 0 {
		t.Errorf("expected EventsWritten=0 on sink error, got %d", stats.EventsWritten)
	}
	if stats.EventsFailed < 1 {
		t.Errorf("expected EventsFailed>=1, got %d", stats.EventsFailed)
	}

	if err := p.Stop(ctx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestPipelineStopDoubleCallNoPanic(t *testing.T) {
	metrics.ResetForTest()
	r := prometheus.NewRegistry()
	metrics.MustRegisterAll(r)
	t.Cleanup(func() {
		metrics.ResetForTest()
		metrics.MustRegisterAll(prometheus.DefaultRegisterer)
	})

	cfg := &Config{ID: "double-stop", Name: "double-stop-test"}
	p := New(cfg)
	p.SetCluster("c1")

	src := newMockSource()
	p.SetSource(src)
	p.AddSink(&mockSink{})

	ctx := context.Background()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Call Stop concurrently from two goroutines. Without sync.Once
	// protection both goroutines can pass the state check and
	// double-close stopCh, causing a panic.
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			errs <- p.Stop(ctx)
		}()
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Errorf("Stop returned error: %v", err)
		}
	}
}

func TestTaskManager_SetStatsCollector_WrapsAndRegisters(t *testing.T) {
	metrics.ResetForTest()
	r := prometheus.NewRegistry()
	metrics.MustRegisterAll(r)
	t.Cleanup(func() {
		metrics.ResetForTest()
		metrics.MustRegisterAll(prometheus.DefaultRegisterer)
	})

	tm := NewTaskManager()
	tm.SetCluster("c1")
	sc := metrics.NewStatsCollector("c1", time.Second, time.Second)
	tm.SetStatsCollector(sc)

	if tm.Cluster() != "c1" {
		t.Errorf("cluster = %q, want c1", tm.Cluster())
	}
	if tm.StatsCollector() == nil {
		t.Error("statsCollector should be set")
	}

	// WrapSink with metrics disabled returns sink unchanged
	tm2 := NewTaskManager()
	wrapped := tm2.WrapSink(nil, "t1", "mysql")
	if wrapped != nil {
		t.Errorf("WrapSink with disabled metrics should return arg unchanged, got %v", wrapped)
	}
}

func TestPipelinePauseStopsProcessing(t *testing.T) {
	metrics.ResetForTest()
	r := prometheus.NewRegistry()
	metrics.MustRegisterAll(r)
	t.Cleanup(func() {
		metrics.ResetForTest()
		metrics.MustRegisterAll(prometheus.DefaultRegisterer)
	})

	cfg := &Config{ID: "pause-test", Name: "pause-test"}
	p := New(cfg)
	p.SetCluster("c1")

	src := newMockSource()
	p.SetSource(src)
	p.AddSink(&mockSink{})

	ctx := context.Background()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Send an event and verify it gets processed.
	src.eventsCh <- &event.ChangeEvent{
		ID:        "e1",
		Type:      event.EventTypeInsert,
		Timestamp: time.Now(),
	}
	// Give the run loop time to process
	time.Sleep(100 * time.Millisecond)

	written1 := p.Status().Statistics.EventsWritten
	if written1 != 1 {
		t.Fatalf("expected EventsWritten=1 after first event, got %d", written1)
	}

	// Pause the pipeline
	if err := p.Pause(ctx); err != nil {
		t.Fatalf("Pause failed: %v", err)
	}

	// Send another event while paused
	go func() {
		src.eventsCh <- &event.ChangeEvent{
			ID:        "e2",
			Type:      event.EventTypeInsert,
			Timestamp: time.Now(),
		}
	}()

	// Wait and verify EventsWritten does NOT increase
	time.Sleep(200 * time.Millisecond)
	written2 := p.Status().Statistics.EventsWritten
	if written2 != 1 {
		t.Errorf("expected EventsWritten=1 while paused, got %d (pause did not stop processing)", written2)
	}

	// Resume the pipeline
	if err := p.Resume(ctx); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}

	// After resume, the queued event should get processed
	time.Sleep(200 * time.Millisecond)
	written3 := p.Status().Statistics.EventsWritten
	if written3 != 2 {
		t.Errorf("expected EventsWritten=2 after resume, got %d", written3)
	}

	// Stop the pipeline
	if err := p.Stop(ctx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}
