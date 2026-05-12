package source

import (
	"context"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

// mockSnapshotReader implements SnapshotReader for testing
type mockSnapshotReader struct {
	events   []*event.ChangeEvent
	delay    time.Duration
	keyRange struct {
		min interface{}
		max interface{}
		err error
	}
}

func (m *mockSnapshotReader) ReadSnapshot(ctx context.Context, task *SnapshotTask) ([]*event.ChangeEvent, error) {
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	return m.events, nil
}

func (m *mockSnapshotReader) GetKeyRange(table *event.TableInfo) (minKey, maxKey interface{}, err error) {
	return m.keyRange.min, m.keyRange.max, m.keyRange.err
}

func TestSnapshotCoordinator_Start(t *testing.T) {
	config := DefaultSnapshotConcurrencyConfig()
	reader := &mockSnapshotReader{
		events: []*event.ChangeEvent{
			{Table: event.TableInfo{Database: "db1", Table: "users"}},
		},
	}

	coordinator := NewSnapshotCoordinator(config, reader)

	tables := []*event.TableInfo{
		{Database: "db1", Table: "users"},
		{Database: "db1", Table: "orders"},
	}

	err := coordinator.Start(tables)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for completion
	time.Sleep(200 * time.Millisecond)
	coordinator.Stop()

	progress := coordinator.Progress()
	if progress.CompletedTable == 0 {
		t.Error("Expected some tables to be completed")
	}
}

func TestSnapshotCoordinator_GenerateTableTasks_Single(t *testing.T) {
	config := DefaultSnapshotConcurrencyConfig()
	reader := &mockSnapshotReader{
		keyRange: struct {
			min interface{}
			max interface{}
			err error
		}{
			err: context.Canceled, // Simulate error - cannot chunk
		},
	}

	coordinator := NewSnapshotCoordinator(config, reader)

	table := &event.TableInfo{
		Database: "db1",
		Table:    "no_pk_table",
	}

	tasks := coordinator.generateTableTasks(table)

	if len(tasks) != 1 {
		t.Errorf("Expected 1 task for non-chunkable table, got %d", len(tasks))
	}
	if tasks[0].ChunkID != 0 {
		t.Errorf("Expected ChunkID 0, got %d", tasks[0].ChunkID)
	}
	if tasks[0].ChunkRange != nil {
		t.Error("Expected nil ChunkRange for non-chunkable table")
	}
}

func TestSnapshotCoordinator_GenerateTableTasks_Chunked(t *testing.T) {
	config := DefaultSnapshotConcurrencyConfig()
	config.ChunkSize = 100

	reader := &mockSnapshotReader{
		keyRange: struct {
			min interface{}
			max interface{}
			err error
		}{
			min: int(1),
			max: int(250),
			err: nil,
		},
	}

	coordinator := NewSnapshotCoordinator(config, reader)

	table := &event.TableInfo{
		Database: "db1",
		Table:    "large_table",
	}

	tasks := coordinator.generateTableTasks(table)

	// With range 1-250 and chunk size 100, we expect:
	// Chunk 1: 1-100, Chunk 2: 101-200, Chunk 3: 201-250
	if len(tasks) != 3 {
		t.Errorf("Expected 3 chunks for range 1-250 with size 100, got %d", len(tasks))
	}

	// Verify chunk ranges
	if tasks[0].ChunkRange.StartKey.(int) != 1 {
		t.Errorf("Expected start 1, got %v", tasks[0].ChunkRange.StartKey)
	}
	if tasks[0].ChunkRange.EndKey.(int) != 100 {
		t.Errorf("Expected end 100, got %v", tasks[0].ChunkRange.EndKey)
	}

	if tasks[2].ChunkRange.StartKey.(int) != 201 {
		t.Errorf("Expected start 201, got %v", tasks[2].ChunkRange.StartKey)
	}
	if tasks[2].ChunkRange.EndKey.(int) != 250 {
		t.Errorf("Expected end 250, got %v", tasks[2].ChunkRange.EndKey)
	}
}

func TestSnapshotCoordinator_GenerateTableTasks_ChunkedInt64(t *testing.T) {
	config := DefaultSnapshotConcurrencyConfig()
	config.ChunkSize = 1000

	reader := &mockSnapshotReader{
		keyRange: struct {
			min interface{}
			max interface{}
			err error
		}{
			min: int64(1),
			max: int64(2500),
			err: nil,
		},
	}

	coordinator := NewSnapshotCoordinator(config, reader)

	table := &event.TableInfo{
		Database: "db1",
		Table:    "big_table",
	}

	tasks := coordinator.generateTableTasks(table)

	// With range 1-2500 and chunk size 1000, we expect 3 chunks
	if len(tasks) != 3 {
		t.Errorf("Expected 3 chunks for range 1-2500 with size 1000, got %d", len(tasks))
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

	pct := progress.Progress()
	if pct != 50.0 {
		t.Errorf("Expected 50%% progress, got %f%%", pct)
	}
}

func TestSnapshotCoordinator_WorkerDistribution(t *testing.T) {
	config := DefaultSnapshotConcurrencyConfig()
	config.MaxTableThreads = 4

	callCount := 0
	reader := &mockSnapshotReader{
		events: []*event.ChangeEvent{},
	}

	coordinator := NewSnapshotCoordinator(config, reader)

	tables := []*event.TableInfo{
		{Database: "db1", Table: "t1"},
		{Database: "db1", Table: "t2"},
		{Database: "db1", Table: "t3"},
		{Database: "db1", Table: "t4"},
	}

	err := coordinator.Start(tables)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for tasks to be processed
	time.Sleep(100 * time.Millisecond)
	coordinator.Stop()

	// Verify that workers were created
	if len(coordinator.workers) != 4 {
		t.Errorf("Expected 4 workers, got %d", len(coordinator.workers))
	}

	_ = callCount // Used for tracking
}
