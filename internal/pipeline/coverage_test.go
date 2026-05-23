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

// ---------------------------------------------------------------------------
// errMockSource: a source whose Start always returns an error
// ---------------------------------------------------------------------------
type errMockSource struct {
	startErr error
	eventsCh chan *event.ChangeEvent
	errorsCh chan error
}

func newErrMockSource(err error) *errMockSource {
	return &errMockSource{
		startErr: err,
		eventsCh: make(chan *event.ChangeEvent),
		errorsCh: make(chan error),
	}
}

func (m *errMockSource) Name() string                                         { return "err-mock-src" }
func (m *errMockSource) Initialize(_ context.Context, _ source.Config) error  { return nil }
func (m *errMockSource) Start(_ context.Context) error                        { return m.startErr }
func (m *errMockSource) Stop(_ context.Context) error                         { return nil }
func (m *errMockSource) Status() source.Status                                { return source.Status{} }
func (m *errMockSource) Events() <-chan *event.ChangeEvent                    { return m.eventsCh }
func (m *errMockSource) Errors() <-chan error                                 { return m.errorsCh }
func (m *errMockSource) GetPosition() *event.Position                         { return nil }
func (m *errMockSource) SetPosition(_ *event.Position) error                  { return nil }
func (m *errMockSource) GetSchema(_, _ string) (*event.TableInfo, error)      { return nil, nil }
func (m *errMockSource) SyncScope() *source.SyncScope                        { return nil }
func (m *errMockSource) AddTables(_ context.Context, _ []string) error        { return nil }
func (m *errMockSource) RemoveTables(_ context.Context, _ []string) error     { return nil }
func (m *errMockSource) ListTables() []string                                 { return nil }
func (m *errMockSource) Schemas() map[string]*event.TableInfo                 { return nil }

// recordingSink records write calls for verification.
type recordingSink struct {
	written []*event.ChangeEvent
}

func (s *recordingSink) Name() string                                         { return "recording" }
func (s *recordingSink) Initialize(_ context.Context, _ sink.Config) error    { return nil }
func (s *recordingSink) Start(_ context.Context) error                        { return nil }
func (s *recordingSink) Stop(_ context.Context) error                         { return nil }
func (s *recordingSink) Status() sink.Status                                  { return sink.Status{} }
func (s *recordingSink) Write(_ context.Context, evts []*event.ChangeEvent) error {
	s.written = append(s.written, evts...)
	return nil
}
func (s *recordingSink) Flush(_ context.Context) error                        { return nil }
func (s *recordingSink) GetPosition() *event.Position                         { return nil }
func (s *recordingSink) SupportsDDL() bool                                    { return false }
func (s *recordingSink) ApplyDDL(_ context.Context, _ *event.ChangeEvent) error { return nil }
func (s *recordingSink) SupportsTransaction() bool                            { return false }

// ---------------------------------------------------------------------------
// helper: metricsTestSetup registers metrics on a fresh registry.
// ---------------------------------------------------------------------------
func metricsTestSetup(t *testing.T) {
	t.Helper()
	metrics.ResetForTest()
	r := prometheus.NewRegistry()
	metrics.MustRegisterAll(r)
	t.Cleanup(func() {
		metrics.ResetForTest()
		metrics.MustRegisterAll(prometheus.DefaultRegisterer)
	})
}

// ===========================================================================
// Pipeline basic accessors
// ===========================================================================

func TestPipeline_ID(t *testing.T) {
	p := New(&Config{ID: "p1", Name: "pipeline-one"})
	if p.ID() != "p1" {
		t.Errorf("ID() = %q, want p1", p.ID())
	}
}

func TestPipeline_Name(t *testing.T) {
	p := New(&Config{ID: "p1", Name: "pipeline-one"})
	if p.Name() != "pipeline-one" {
		t.Errorf("Name() = %q, want pipeline-one", p.Name())
	}
}

func TestPipeline_SetDispatcher(t *testing.T) {
	p := New(&Config{ID: "p1"})
	d := NewRoundRobinDispatcher()
	p.SetDispatcher(d)
	if p.dispatcher == nil {
		t.Error("dispatcher should be set")
	}
}

func TestPipeline_SetBuffer(t *testing.T) {
	p := New(&Config{ID: "p1"})
	b := NewMemoryBuffer(10)
	p.SetBuffer(b)
	if p.buffer == nil {
		t.Error("buffer should be set")
	}
}

// ===========================================================================
// Pipeline Start error paths
// ===========================================================================

func TestPipeline_StartAlreadyRunning(t *testing.T) {
	metricsTestSetup(t)

	p := New(&Config{ID: "sar"})
	p.SetCluster("c")
	src := newMockSource()
	p.SetSource(src)
	p.AddSink(&mockSink{})

	ctx := context.Background()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("first Start failed: %v", err)
	}

	// Second start should return ErrAlreadyRunning.
	err := p.Start(ctx)
	if err != ErrAlreadyRunning {
		t.Errorf("expected ErrAlreadyRunning, got %v", err)
	}

	p.Stop(ctx)
}

func TestPipeline_StartSourceError(t *testing.T) {
	metricsTestSetup(t)

	p := New(&Config{ID: "sserr"})
	p.SetCluster("c")
	srcErr := errors.New("source kaput")
	src := newErrMockSource(srcErr)
	p.SetSource(src)

	err := p.Start(context.Background())
	if err == nil {
		t.Fatal("expected error from Start when source fails")
	}
	if err.Error() != srcErr.Error() {
		t.Errorf("unexpected error: %v", err)
	}
	// Pipeline state should be error.
	if p.Status().State != StateError {
		t.Errorf("state = %s, want error", p.Status().State)
	}
}

// ===========================================================================
// Pipeline processEvent: heartbeat with no dispatcher
// ===========================================================================

func TestPipeline_processEvent_HeartbeatSkip(t *testing.T) {
	metricsTestSetup(t)

	p := New(&Config{ID: "hb"})
	p.SetCluster("c")
	p.precacheLabels()

	hb := &event.ChangeEvent{
		ID:        "hb1",
		Type:      event.EventTypeHeartbeat,
		Timestamp: time.Now(),
	}
	// No dispatcher set - heartbeat should be skipped (no write count).
	p.processEvent(context.Background(), hb)

	if p.Status().Statistics.EventsWritten != 0 {
		t.Errorf("heartbeat without dispatcher should not increment EventsWritten, got %d",
			p.Status().Statistics.EventsWritten)
	}
}

// ===========================================================================
// Pipeline processEvent: with dispatcher
// ===========================================================================

func TestPipeline_processEvent_WithDispatcher(t *testing.T) {
	metricsTestSetup(t)

	p := New(&Config{ID: "pd"})
	p.SetCluster("c")
	p.precacheLabels()
	p.SetDispatcher(NewBroadcastDispatcher())
	p.AddSink(&mockSink{})

	evt := &event.ChangeEvent{
		ID:        "e1",
		Type:      event.EventTypeInsert,
		Timestamp: time.Now(),
	}
	p.processEvent(context.Background(), evt)

	if p.Status().Statistics.EventsWritten != 1 {
		t.Errorf("EventsWritten = %d, want 1", p.Status().Statistics.EventsWritten)
	}
}

// ===========================================================================
// Pipeline processEvent: dispatcher returns error
// ===========================================================================

func TestPipeline_processEvent_DispatcherError(t *testing.T) {
	metricsTestSetup(t)

	p := New(&Config{ID: "de"})
	p.SetCluster("c")
	p.precacheLabels()
	p.SetDispatcher(NewBroadcastDispatcher())
	p.AddSink(newErrMockSink(errors.New("write fail")))

	evt := &event.ChangeEvent{
		ID:        "e1",
		Type:      event.EventTypeInsert,
		Timestamp: time.Now(),
	}
	p.processEvent(context.Background(), evt)

	if p.Status().Statistics.EventsFailed != 1 {
		t.Errorf("EventsFailed = %d, want 1", p.Status().Statistics.EventsFailed)
	}
}

// ===========================================================================
// Pipeline instrumentEvent: nil event
// ===========================================================================

func TestPipeline_instrumentEvent_Nil(t *testing.T) {
	metricsTestSetup(t)

	p := New(&Config{ID: "nil"})
	p.SetCluster("c")
	p.precacheLabels()

	// Should not panic.
	p.instrumentEvent(nil)
}

// ===========================================================================
// Pipeline run: source errors channel
// ===========================================================================

func TestPipeline_RunSourceErrors(t *testing.T) {
	metricsTestSetup(t)

	cfg := &Config{ID: "src-errs", Name: "test"}
	p := New(cfg)
	p.SetCluster("c")
	src := newMockSource()
	p.SetSource(src)
	p.AddSink(&mockSink{})

	ctx := context.Background()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Send an error through the source error channel.
	src.errorsCh <- errors.New("transient")
	time.Sleep(100 * time.Millisecond)

	if p.Status().Statistics.EventsFailed < 1 {
		t.Errorf("EventsFailed = %d, want >= 1 after source error", p.Status().Statistics.EventsFailed)
	}

	p.Stop(ctx)
}

// ===========================================================================
// Pipeline run: context cancellation
// ===========================================================================

func TestPipeline_RunContextCancel(t *testing.T) {
	metricsTestSetup(t)

	cfg := &Config{ID: "ctx-cancel", Name: "test"}
	p := New(cfg)
	p.SetCluster("c")
	src := newMockSource()
	p.SetSource(src)
	p.AddSink(&mockSink{})

	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Cancel the context - run loop should exit.
	cancel()
	time.Sleep(100 * time.Millisecond)

	// Stop should still succeed.
	if err := p.Stop(context.Background()); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

// ===========================================================================
// Pipeline run: source events channel closes
// ===========================================================================

func TestPipeline_RunSourceChannelClosed(t *testing.T) {
	metricsTestSetup(t)

	cfg := &Config{ID: "ch-close", Name: "test"}
	p := New(cfg)
	p.SetCluster("c")
	src := newMockSource()
	p.SetSource(src)
	p.AddSink(&mockSink{})

	ctx := context.Background()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Close the events channel - run loop should exit gracefully.
	close(src.eventsCh)
	time.Sleep(100 * time.Millisecond)

	p.Stop(ctx)
}

// ===========================================================================
// Pipeline Pause/Resume error paths
// ===========================================================================

func TestPipeline_Pause_InvalidState(t *testing.T) {
	metricsTestSetup(t)

	p := New(&Config{ID: "pi"})
	// State is "created", not "running".
	err := p.Pause(context.Background())
	if err != ErrInvalidState {
		t.Errorf("Pause non-running: got %v, want ErrInvalidState", err)
	}
}

func TestPipeline_Resume_InvalidState(t *testing.T) {
	metricsTestSetup(t)

	p := New(&Config{ID: "ri"})
	// State is "created", not "paused".
	err := p.Resume(context.Background())
	if err != ErrInvalidState {
		t.Errorf("Resume non-paused: got %v, want ErrInvalidState", err)
	}
}

// ===========================================================================
// Dispatcher: RoundRobin with actual sinks
// ===========================================================================

func TestRoundRobinDispatcher_Dispatch(t *testing.T) {
	d := NewRoundRobinDispatcher()
	defer d.Close()

	s1 := &recordingSink{}
	s2 := &recordingSink{}
	sinks := []sink.Connector{s1, s2}

	ctx := context.Background()
	for i := 0; i < 4; i++ {
		e := &event.ChangeEvent{ID: "rr-" + string(rune('0'+i))}
		if err := d.Dispatch(ctx, e, sinks); err != nil {
			t.Fatalf("Dispatch %d: %v", i, err)
		}
	}
	// 4 events round-robin over 2 sinks -> 2 each.
	if len(s1.written) != 2 {
		t.Errorf("sink1 got %d events, want 2", len(s1.written))
	}
	if len(s2.written) != 2 {
		t.Errorf("sink2 got %d events, want 2", len(s2.written))
	}
}

func TestRoundRobinDispatcher_EmptySinks(t *testing.T) {
	d := NewRoundRobinDispatcher()
	defer d.Close()

	err := d.Dispatch(context.Background(), &event.ChangeEvent{}, nil)
	if err != ErrNoSink {
		t.Errorf("expected ErrNoSink, got %v", err)
	}
}

// ===========================================================================
// Dispatcher: Hash with actual sinks
// ===========================================================================

func TestHashDispatcher_Dispatch(t *testing.T) {
	d := NewHashDispatcher("id")
	defer d.Close()

	s1 := &recordingSink{}
	s2 := &recordingSink{}
	sinks := []sink.Connector{s1, s2}

	// Build an event with After.Fields so the hash key lookup works.
	e := &event.ChangeEvent{
		ID:   "h1",
		Type: event.EventTypeInsert,
		Table: event.TableInfo{
			Database: "db",
			Table:    "users",
		},
		After: event.RowData{
			Fields: map[string]event.Field{
				"id": {Name: "id", Value: "user-42", Type: "string"},
			},
		},
	}

	ctx := context.Background()
	if err := d.Dispatch(ctx, e, sinks); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	// At least one sink should have received the event.
	total := len(s1.written) + len(s2.written)
	if total != 1 {
		t.Errorf("total events = %d, want 1", total)
	}
}

func TestHashDispatcher_FallbackToTable(t *testing.T) {
	d := NewHashDispatcher("missing_key")
	defer d.Close()

	s := &recordingSink{}
	sinks := []sink.Connector{s}

	// Event has no matching field -> falls back to table name.
	e := &event.ChangeEvent{
		ID:   "h2",
		Type: event.EventTypeInsert,
		Table: event.TableInfo{
			Database: "db",
			Table:    "orders",
		},
	}

	if err := d.Dispatch(context.Background(), e, sinks); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if len(s.written) != 1 {
		t.Errorf("expected 1 event, got %d", len(s.written))
	}
}

func TestHashDispatcher_EmptySinks(t *testing.T) {
	d := NewHashDispatcher("id")
	defer d.Close()

	err := d.Dispatch(context.Background(), &event.ChangeEvent{}, nil)
	if err != ErrNoSink {
		t.Errorf("expected ErrNoSink, got %v", err)
	}
}

// ===========================================================================
// Dispatcher: Broadcast with actual sinks
// ===========================================================================

func TestBroadcastDispatcher_Dispatch(t *testing.T) {
	d := NewBroadcastDispatcher()
	defer d.Close()

	s1 := &recordingSink{}
	s2 := &recordingSink{}
	sinks := []sink.Connector{s1, s2}

	e := &event.ChangeEvent{ID: "bc1"}
	if err := d.Dispatch(context.Background(), e, sinks); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if len(s1.written) != 1 || len(s2.written) != 1 {
		t.Errorf("broadcast: s1=%d s2=%d, want 1,1", len(s1.written), len(s2.written))
	}
}

func TestBroadcastDispatcher_EmptySinks(t *testing.T) {
	d := NewBroadcastDispatcher()
	defer d.Close()

	err := d.Dispatch(context.Background(), &event.ChangeEvent{}, nil)
	if err != ErrNoSink {
		t.Errorf("expected ErrNoSink, got %v", err)
	}
}

func TestBroadcastDispatcher_PartialError(t *testing.T) {
	d := NewBroadcastDispatcher()
	defer d.Close()

	s1 := &recordingSink{}
	s2 := newErrMockSink(errors.New("boom"))
	sinks := []sink.Connector{s1, s2}

	err := d.Dispatch(context.Background(), &event.ChangeEvent{ID: "bc2"}, sinks)
	if err == nil {
		t.Error("expected non-nil error from broadcast with failing sink")
	}
	// Good sink should still have received the event.
	if len(s1.written) != 1 {
		t.Errorf("s1 should have 1 event, got %d", len(s1.written))
	}
}

// ===========================================================================
// Buffer: MemoryBuffer edge cases
// ===========================================================================

func TestMemoryBuffer_Flush(t *testing.T) {
	buf := NewMemoryBuffer(10)
	defer buf.Close()

	if err := buf.Flush(context.Background()); err != nil {
		t.Errorf("Flush: %v", err)
	}
}

func TestMemoryBuffer_Cap(t *testing.T) {
	buf := NewMemoryBuffer(42)
	if buf.Cap() != 42 {
		t.Errorf("Cap() = %d, want 42", buf.Cap())
	}
	buf.Close()
}

func TestMemoryBuffer_PutClosed(t *testing.T) {
	buf := NewMemoryBuffer(10)
	buf.Close()

	err := buf.Put(context.Background(), &event.ChangeEvent{ID: "x"})
	if err != ErrBufferFull {
		t.Errorf("Put on closed buffer: %v, want ErrBufferFull", err)
	}
}

func TestMemoryBuffer_PutContextCancelled(t *testing.T) {
	buf := NewMemoryBuffer(1)
	defer buf.Close()

	// Fill it up.
	buf.Put(context.Background(), &event.ChangeEvent{ID: "fill"})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Buffer is full + ctx cancelled => should return ctx.Err() or ErrBufferFull.
	err := buf.Put(ctx, &event.ChangeEvent{ID: "overflow"})
	if err == nil {
		t.Error("expected error on full buffer with cancelled ctx")
	}
}

func TestMemoryBuffer_GetContextCancelled(t *testing.T) {
	buf := NewMemoryBuffer(10)
	defer buf.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	events, err := buf.Get(ctx, 5)
	// When buffer is empty and ctx is done, default branch fires first (empty result).
	// Accept either empty result or context error.
	if err != nil && err != context.Canceled {
		t.Errorf("unexpected error: %v", err)
	}
	_ = events
}

func TestMemoryBuffer_CloseIdempotent(t *testing.T) {
	buf := NewMemoryBuffer(10)
	if err := buf.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := buf.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestMemoryBuffer_GetFromClosedChannel(t *testing.T) {
	buf := NewMemoryBuffer(10)

	// Add events, then close.
	buf.Put(context.Background(), &event.ChangeEvent{ID: "e1"})
	buf.Close()

	// Get should still return the buffered event.
	events, err := buf.Get(context.Background(), 5)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(events) != 1 {
		t.Errorf("events = %d, want 1", len(events))
	}
}

// ===========================================================================
// Buffer: BatchBuffer edge cases
// ===========================================================================

func TestBatchBuffer_LenCap(t *testing.T) {
	bb := NewBatchBuffer(50, 10, 100)
	defer bb.Close()

	if bb.Cap() != 50 {
		t.Errorf("Cap() = %d, want 50", bb.Cap())
	}
	if bb.Len() != 0 {
		t.Errorf("Len() = %d, want 0", bb.Len())
	}
}

func TestBatchBuffer_Flush(t *testing.T) {
	bb := NewBatchBuffer(50, 10, 100)
	defer bb.Close()

	// Add some events.
	for i := 0; i < 3; i++ {
		bb.Put(context.Background(), &event.ChangeEvent{ID: "f" + string(rune('0'+i))})
	}

	if err := bb.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

// ===========================================================================
// Buffer: NewBuffer factory
// ===========================================================================

func TestNewBuffer_BatchMode(t *testing.T) {
	buf := NewBuffer(BufferConfig{Size: 100, BatchSize: 10, FlushTimeout: 50})
	if buf == nil {
		t.Fatal("expected non-nil buffer")
	}
	_, ok := buf.(*BatchBuffer)
	if !ok {
		t.Errorf("expected *BatchBuffer, got %T", buf)
	}
	buf.Close()
}

func TestNewBuffer_SimpleMode(t *testing.T) {
	buf := NewBuffer(BufferConfig{Size: 100})
	if buf == nil {
		t.Fatal("expected non-nil buffer")
	}
	_, ok := buf.(*MemoryBuffer)
	if !ok {
		t.Errorf("expected *MemoryBuffer, got %T", buf)
	}
	buf.Close()
}

// ===========================================================================
// Task lifecycle
// ===========================================================================

func TestTask_StartStop(t *testing.T) {
	metricsTestSetup(t)

	cfg := &Config{ID: "ts", Name: "task-start-stop"}
	p := New(cfg)
	p.SetCluster("c")
	src := newMockSource()
	p.SetSource(src)
	p.AddSink(&mockSink{})

	task := NewTask("t1", "test-task", cfg)
	task.Pipeline = p

	ctx := context.Background()
	if err := task.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if task.GetStatus() != TaskStatusRunning {
		t.Errorf("status = %s, want running", task.GetStatus())
	}
	if task.StartedAt == nil {
		t.Error("StartedAt should be set")
	}

	if err := task.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if task.GetStatus() != TaskStatusStopped {
		t.Errorf("status = %s, want stopped", task.GetStatus())
	}
	if task.StoppedAt == nil {
		t.Error("StoppedAt should be set")
	}
}

func TestTask_StartAlreadyRunning(t *testing.T) {
	metricsTestSetup(t)

	cfg := &Config{ID: "tar", Name: "test"}
	p := New(cfg)
	p.SetCluster("c")
	src := newMockSource()
	p.SetSource(src)
	p.AddSink(&mockSink{})

	task := NewTask("t2", "test", cfg)
	task.Pipeline = p

	ctx := context.Background()
	task.Start(ctx)
	defer task.Stop(ctx)

	err := task.Start(ctx)
	if err != ErrAlreadyRunning {
		t.Errorf("expected ErrAlreadyRunning, got %v", err)
	}
}

func TestTask_StartNoPipeline(t *testing.T) {
	task := NewTask("t3", "test", &Config{})
	// Pipeline is nil.
	err := task.Start(context.Background())
	if err != ErrNoSource {
		t.Errorf("expected ErrNoSource, got %v", err)
	}
	if task.GetStatus() != TaskStatusError {
		t.Errorf("status = %s, want error", task.GetStatus())
	}
}

func TestTask_StopAlreadyStopped(t *testing.T) {
	task := NewTask("t4", "test", &Config{})
	task.Status = TaskStatusStopped

	err := task.Stop(context.Background())
	if err != nil {
		t.Errorf("Stop already stopped: %v", err)
	}
}

func TestTask_StopNoPipeline(t *testing.T) {
	task := NewTask("t5", "test", &Config{})
	task.Status = TaskStatusRunning
	// Pipeline is nil - Stop should still work.
	err := task.Stop(context.Background())
	if err != nil {
		t.Errorf("Stop with nil pipeline: %v", err)
	}
	if task.GetStatus() != TaskStatusStopped {
		t.Errorf("status = %s, want stopped", task.GetStatus())
	}
}

func TestTask_PauseResume(t *testing.T) {
	task := NewTask("t6", "test", &Config{})
	ctx := context.Background()

	// Pause should fail when not running.
	if err := task.Pause(ctx); err != ErrInvalidState {
		t.Errorf("Pause non-running: %v, want ErrInvalidState", err)
	}

	task.Status = TaskStatusRunning
	if err := task.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if task.GetStatus() != TaskStatusPaused {
		t.Errorf("status after Pause = %s, want paused", task.GetStatus())
	}

	// Resume should fail when not paused.
	task2 := NewTask("t7", "test", &Config{})
	if err := task2.Resume(ctx); err != ErrInvalidState {
		t.Errorf("Resume non-paused: %v, want ErrInvalidState", err)
	}

	if err := task.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if task.GetStatus() != TaskStatusRunning {
		t.Errorf("status after Resume = %s, want running", task.GetStatus())
	}
}

func TestTask_Update(t *testing.T) {
	task := NewTask("t8", "old-name", &Config{ID: "old"})
	before := task.UpdatedAt

	time.Sleep(time.Millisecond)
	task.Update("new-name", &Config{ID: "new"})

	if task.Name != "new-name" {
		t.Errorf("Name = %s, want new-name", task.Name)
	}
	if task.Config.ID != "new" {
		t.Errorf("Config.ID = %s, want new", task.Config.ID)
	}
	if !task.UpdatedAt.After(before) {
		t.Error("UpdatedAt should advance")
	}

	// Update with empty name should keep old name.
	task.Update("", nil)
	if task.Name != "new-name" {
		t.Errorf("Name changed unexpectedly: %s", task.Name)
	}
}

func TestTask_GetPosition_NilPosition(t *testing.T) {
	task := NewTask("t9", "test", &Config{})
	if task.GetPosition() != nil {
		t.Error("GetPosition should return nil when no position set")
	}
}

// ===========================================================================
// TaskManager: Start / Stop / StopAll / error paths
// ===========================================================================

func TestTaskManager_StartNotFound(t *testing.T) {
	tm := NewTaskManager()
	err := tm.Start(context.Background(), "nonexistent")
	if err != ErrTaskNotFound {
		t.Errorf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestTaskManager_StopNotFound(t *testing.T) {
	tm := NewTaskManager()
	err := tm.Stop(context.Background(), "nonexistent")
	if err != ErrTaskNotFound {
		t.Errorf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestTaskManager_GetNotFound(t *testing.T) {
	tm := NewTaskManager()
	_, err := tm.Get("nonexistent")
	if err != ErrTaskNotFound {
		t.Errorf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestTaskManager_DeleteNotFound(t *testing.T) {
	tm := NewTaskManager()
	err := tm.Delete(context.Background(), "nonexistent")
	if err != ErrTaskNotFound {
		t.Errorf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestTaskManager_StopAll(t *testing.T) {
	metricsTestSetup(t)

	tm := NewTaskManager()
	ctx := context.Background()

	// Create tasks with pipelines.
	for _, id := range []string{"sa1", "sa2"} {
		cfg := &Config{ID: id, Name: id}
		task, _ := tm.Create(ctx, id, id, cfg)

		p := New(cfg)
		p.SetCluster("c")
		src := newMockSource()
		p.SetSource(src)
		p.AddSink(&mockSink{})
		task.Pipeline = p
		task.Start(ctx)
	}

	err := tm.StopAll(ctx)
	if err != nil {
		t.Errorf("StopAll: %v", err)
	}

	// All tasks should be stopped.
	for _, task := range tm.List() {
		if task.GetStatus() != TaskStatusStopped {
			t.Errorf("task %s status = %s, want stopped", task.ID, task.GetStatus())
		}
	}
}

func TestTaskManager_StopAllEmpty(t *testing.T) {
	tm := NewTaskManager()
	err := tm.StopAll(context.Background())
	if err != nil {
		t.Errorf("StopAll empty: %v", err)
	}
}

func TestTaskManager_DeleteRunningTask(t *testing.T) {
	metricsTestSetup(t)

	tm := NewTaskManager()
	ctx := context.Background()

	cfg := &Config{ID: "del-run", Name: "del-run"}
	task, _ := tm.Create(ctx, "del-run", "del-run", cfg)

	p := New(cfg)
	p.SetCluster("c")
	src := newMockSource()
	p.SetSource(src)
	p.AddSink(&mockSink{})
	task.Pipeline = p
	task.Start(ctx)

	// Delete should stop the task first.
	if err := tm.Delete(ctx, "del-run"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if len(tm.List()) != 0 {
		t.Errorf("task list should be empty, got %d", len(tm.List()))
	}
}

func TestTaskManager_CreateWithCoordinator(t *testing.T) {
	tm := NewTaskManager()
	coord := NewMemoryCoordinator("node-1")
	tm.SetCoordinator(coord)

	ctx := context.Background()
	task, err := tm.Create(ctx, "coord-task", "test", &Config{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Verify task is in coordinator.
	ct, err := coord.GetTask(ctx, "coord-task")
	if err != nil {
		t.Fatalf("GetTask from coordinator: %v", err)
	}
	if ct.ID != task.ID {
		t.Error("coordinator task mismatch")
	}
}

func TestTaskManager_DeleteWithCoordinator(t *testing.T) {
	tm := NewTaskManager()
	coord := NewMemoryCoordinator("node-1")
	tm.SetCoordinator(coord)

	ctx := context.Background()
	tm.Create(ctx, "del-coord", "test", &Config{})

	if err := tm.Delete(ctx, "del-coord"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

// ===========================================================================
// TaskManager: WrapSink, RegisterSourceStats, RegisterSinkStats
// ===========================================================================

func TestTaskManager_WrapSink_NoCollector(t *testing.T) {
	tm := NewTaskManager()
	s := &mockSink{}
	wrapped := tm.WrapSink(s, "t1", "mysql")
	if wrapped != s {
		t.Error("without collector, WrapSink should return original sink")
	}
}

func TestTaskManager_RegisterSourceStats_NoCollector(t *testing.T) {
	tm := NewTaskManager()
	// Should not panic even without collector.
	tm.RegisterSourceStats("t1", "mysql", newMockSource())
}

func TestTaskManager_RegisterSinkStats_NoCollector(t *testing.T) {
	tm := NewTaskManager()
	// Should not panic.
	tm.RegisterSinkStats("t1", "mysql", 0, &mockSink{})
}

func TestTaskManager_UnregisterTaskStats_NoCollector(t *testing.T) {
	tm := NewTaskManager()
	// Should not panic.
	tm.UnregisterTaskStats("t1")
}

// ===========================================================================
// intToStr helper
// ===========================================================================

func TestIntToStr(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, "0"},
		{1, "1"},
		{9, "9"},
		{10, "10"},
		{15, "15"},
		{123, "123"},
	}
	for _, tt := range tests {
		got := intToStr(tt.n)
		if got != tt.want {
			t.Errorf("intToStr(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

// ===========================================================================
// MemoryCoordinator: additional methods
// ===========================================================================

func TestMemoryCoordinator_DeleteTask(t *testing.T) {
	c := NewMemoryCoordinator("node-1")
	ctx := context.Background()

	task := NewTask("dt1", "test", &Config{})
	c.SaveTask(ctx, task)
	c.SavePosition(ctx, "dt1", &event.Position{TxID: "tx1"})
	c.AcquireLeadership(ctx, "dt1")

	if err := c.DeleteTask(ctx, "dt1"); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}

	// Task should be gone.
	_, err := c.GetTask(ctx, "dt1")
	if err != ErrTaskNotFound {
		t.Errorf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestMemoryCoordinator_GetTaskNotFound(t *testing.T) {
	c := NewMemoryCoordinator("node-1")
	_, err := c.GetTask(context.Background(), "nonexistent")
	if err != ErrTaskNotFound {
		t.Errorf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestMemoryCoordinator_ListTasks(t *testing.T) {
	c := NewMemoryCoordinator("node-1")
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		c.SaveTask(ctx, NewTask("lt"+string(rune('0'+i)), "test", &Config{}))
	}

	tasks, err := c.ListTasks(ctx)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 3 {
		t.Errorf("ListTasks = %d, want 3", len(tasks))
	}
}

func TestMemoryCoordinator_GetPosition_NotFound(t *testing.T) {
	c := NewMemoryCoordinator("node-1")
	pos, err := c.GetPosition(context.Background(), "no-pos")
	if err != nil {
		t.Fatalf("GetPosition: %v", err)
	}
	if pos != nil {
		t.Errorf("expected nil position, got %+v", pos)
	}
}

func TestMemoryCoordinator_ReleaseLeadership(t *testing.T) {
	c := NewMemoryCoordinator("node-1")
	ctx := context.Background()

	c.AcquireLeadership(ctx, "rl1")
	if err := c.ReleaseLeadership(ctx, "rl1"); err != nil {
		t.Fatalf("ReleaseLeadership: %v", err)
	}

	is, _ := c.IsLeader(ctx, "rl1")
	if is {
		t.Error("should not be leader after release")
	}
}

func TestMemoryCoordinator_AcquireLeadership_AlreadyLeader(t *testing.T) {
	c := NewMemoryCoordinator("node-1")
	ctx := context.Background()

	c.AcquireLeadership(ctx, "al1")

	// Same node re-acquires - should succeed.
	ok, err := c.AcquireLeadership(ctx, "al1")
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}
	if !ok {
		t.Error("same node should re-acquire leadership")
	}
}

func TestMemoryCoordinator_IsLeader_NoLeader(t *testing.T) {
	c := NewMemoryCoordinator("node-1")
	is, err := c.IsLeader(context.Background(), "no-leader")
	if err != nil {
		t.Fatalf("IsLeader: %v", err)
	}
	if is {
		t.Error("should not be leader when none acquired")
	}
}

func TestMemoryCoordinator_WatchLeadership(t *testing.T) {
	c := NewMemoryCoordinator("node-1")
	ch, err := c.WatchLeadership(context.Background(), "wl1")
	if err != nil {
		t.Fatalf("WatchLeadership: %v", err)
	}
	if ch == nil {
		t.Error("expected non-nil channel")
	}
}

func TestMemoryCoordinator_UnregisterNode(t *testing.T) {
	c := NewMemoryCoordinator("node-1")
	ctx := context.Background()

	c.RegisterNode(ctx, "n1", NodeInfo{ID: "n1", Address: "localhost"})
	if err := c.UnregisterNode(ctx, "n1"); err != nil {
		t.Fatalf("UnregisterNode: %v", err)
	}

	nodes, _ := c.ListNodes(ctx)
	if len(nodes) != 0 {
		t.Errorf("nodes = %d, want 0", len(nodes))
	}
}

func TestMemoryCoordinator_Heartbeat(t *testing.T) {
	c := NewMemoryCoordinator("node-1")
	ctx := context.Background()

	c.RegisterNode(ctx, "hb1", NodeInfo{ID: "hb1"})
	if err := c.Heartbeat(ctx, "hb1"); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// Heartbeat for unknown node should be no-op.
	if err := c.Heartbeat(ctx, "unknown"); err != nil {
		t.Fatalf("Heartbeat unknown: %v", err)
	}
}

// ===========================================================================
// BackpressureController: additional coverage
// ===========================================================================

func TestBackpressureController_PauseCh_ResumeCh(t *testing.T) {
	config := DefaultBackpressureConfig()
	ctrl := NewBackpressureController(config)

	// PauseCh and ResumeCh should return non-nil channels.
	if ctrl.PauseCh() == nil {
		t.Error("PauseCh() should not be nil")
	}
	if ctrl.ResumeCh() == nil {
		t.Error("ResumeCh() should not be nil")
	}
}

func TestBackpressureController_WaitForResume_ContextCancelled(t *testing.T) {
	config := DefaultBackpressureConfig()
	ctrl := NewBackpressureController(config)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ctrl.WaitForResume(ctx)
	if err != context.Canceled {
		t.Errorf("WaitForResume: %v, want context.Canceled", err)
	}
}

func TestBackpressureController_WaitForResume_Signal(t *testing.T) {
	config := DefaultBackpressureConfig()
	ctrl := NewBackpressureController(config)
	ctrl.Start()
	defer ctrl.Stop()

	// Trigger pause, then resume.
	ctrl.UpdateMetrics(90, 100, time.Second)
	time.Sleep(150 * time.Millisecond) // Wait for check to fire.

	go func() {
		time.Sleep(50 * time.Millisecond)
		ctrl.UpdateMetrics(10, 100, time.Millisecond)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := ctrl.WaitForResume(ctx)
	if err != nil {
		t.Errorf("WaitForResume: %v", err)
	}
}

func TestBackpressureController_WaitWhilePaused_NotPaused(t *testing.T) {
	config := DefaultBackpressureConfig()
	ctrl := NewBackpressureController(config)

	// Should return immediately since state is normal.
	err := ctrl.WaitWhilePaused(context.Background())
	if err != nil {
		t.Errorf("WaitWhilePaused: %v", err)
	}
}

func TestBackpressureController_WaitWhilePaused_ContextCancel(t *testing.T) {
	config := &BackpressureConfig{
		EnableBackpressure: true,
		HighWatermark:      80,
		LowWatermark:       50,
		MaxLatency:         5 * time.Second,
		CheckInterval:      20 * time.Millisecond,
	}

	ctrl := NewBackpressureController(config)
	ctrl.Start()
	defer ctrl.Stop()

	// Trigger pause.
	ctrl.UpdateMetrics(90, 100, time.Second)
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := ctrl.WaitWhilePaused(ctx)
	if err != context.DeadlineExceeded {
		t.Errorf("WaitWhilePaused: %v, want context.DeadlineExceeded", err)
	}
}

// ===========================================================================
// Pipeline: stop already-stopped is idempotent
// ===========================================================================

func TestPipeline_StopAlreadyStopped(t *testing.T) {
	metricsTestSetup(t)

	p := New(&Config{ID: "sas"})
	p.SetCluster("c")
	p.status.State = StateStopped

	err := p.Stop(context.Background())
	if err != nil {
		t.Errorf("Stop already stopped: %v", err)
	}
}

// ===========================================================================
// Pipeline: processEvent with no sinks (broadcast path, nil events skipped)
// ===========================================================================

func TestPipeline_processEvent_NoDispatcherNoSinks(t *testing.T) {
	metricsTestSetup(t)

	p := New(&Config{ID: "ns"})
	p.SetCluster("c")
	p.precacheLabels()
	// No dispatcher, no sinks.
	evt := &event.ChangeEvent{
		ID:        "e1",
		Type:      event.EventTypeInsert,
		Timestamp: time.Now(),
	}
	// Should not panic - will iterate empty sinks slice.
	p.processEvent(context.Background(), evt)
}

// ===========================================================================
// Pipeline: Source errors channel closed
// ===========================================================================

func TestPipeline_SourceErrorsChannelClosed(t *testing.T) {
	metricsTestSetup(t)

	p := New(&Config{ID: "err-close", Name: "test"})
	p.SetCluster("c")
	src := newMockSource()
	p.SetSource(src)
	p.AddSink(&mockSink{})

	ctx := context.Background()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Close errors channel - should not crash the run loop.
	close(src.errorsCh)
	time.Sleep(100 * time.Millisecond)

	p.Stop(ctx)
}

// ===========================================================================
// Pipeline: HashDispatcher with Before instead of After
// ===========================================================================

func TestHashDispatcher_UsesBefore(t *testing.T) {
	d := NewHashDispatcher("id")
	defer d.Close()

	s := &recordingSink{}
	sinks := []sink.Connector{s}

	// Event with Before set but After empty.
	e := &event.ChangeEvent{
		ID:   "hb1",
		Type: event.EventTypeDelete,
		Table: event.TableInfo{
			Database: "db",
			Table:    "users",
		},
		Before: event.RowData{
			Fields: map[string]event.Field{
				"id": {Name: "id", Value: "user-99", Type: "string"},
			},
		},
	}

	if err := d.Dispatch(context.Background(), e, sinks); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if len(s.written) != 1 {
		t.Errorf("expected 1 event, got %d", len(s.written))
	}
}

// ===========================================================================
// Pipeline: HashDispatcher with non-data event (DDL, heartbeat)
// ===========================================================================

func TestHashDispatcher_NonDataEvent(t *testing.T) {
	d := NewHashDispatcher("id")
	defer d.Close()

	s := &recordingSink{}
	sinks := []sink.Connector{s}

	e := &event.ChangeEvent{
		ID:   "ddl1",
		Type: event.EventTypeDDL,
		Table: event.TableInfo{
			Database: "db",
			Table:    "users",
		},
	}

	if err := d.Dispatch(context.Background(), e, sinks); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if len(s.written) != 1 {
		t.Errorf("expected 1 event, got %d", len(s.written))
	}
}

// ===========================================================================
// TaskManager: Start and Stop by ID (with pipeline)
// ===========================================================================

func TestTaskManager_StartAndStopByID(t *testing.T) {
	metricsTestSetup(t)

	tm := NewTaskManager()
	ctx := context.Background()

	cfg := &Config{ID: "tmss", Name: "test"}
	task, _ := tm.Create(ctx, "tmss", "test", cfg)

	p := New(cfg)
	p.SetCluster("c")
	src := newMockSource()
	p.SetSource(src)
	p.AddSink(&mockSink{})
	task.Pipeline = p

	if err := tm.Start(ctx, "tmss"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := tm.Stop(ctx, "tmss"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
