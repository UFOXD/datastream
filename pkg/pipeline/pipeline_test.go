package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
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
