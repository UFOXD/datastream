//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/config"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/pipeline"
)

func TestPipelineMemoryIntegration(t *testing.T) {
	fixture := NewTestFixture(t)
	defer fixture.Cleanup()

	// Create pipeline config
	cfg := &config.PipelineConfig{
		Name:      "test-pipeline",
		Workers:   2,
		BatchSize: 100,
	}

	// Create memory coordinator
	coord := pipeline.NewMemoryCoordinator()

	// Create pipeline
	p, err := pipeline.NewPipeline(cfg, coord)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start pipeline
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}

	// Verify pipeline is running
	if status := p.Status(); status != pipeline.StatusRunning {
		t.Fatalf("Expected pipeline status running, got %s", status)
	}

	// Stop pipeline
	if err := p.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop pipeline: %v", err)
	}

	t.Log("Pipeline memory integration test successful")
}

func TestPipelineTaskManagement(t *testing.T) {
	fixture := NewTestFixture(t)
	defer fixture.Cleanup()

	// Create memory coordinator
	coord := pipeline.NewMemoryCoordinator()

	// Create task manager
	tm := pipeline.NewTaskManager(coord)

	ctx := context.Background()

	// Create task
	task := &pipeline.Task{
		ID:     "test-task-1",
		Name:   "Test Task",
		Source: "mysql://localhost:3306/test",
		Sink:   "kafka://localhost:9092/test",
		Status: pipeline.TaskStatusStopped,
		Config: map[string]string{"batch_size": "100"},
	}

	if err := tm.CreateTask(ctx, task); err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	// Get task
	retrieved, err := tm.GetTask(ctx, "test-task-1")
	if err != nil {
		t.Fatalf("Failed to get task: %v", err)
	}

	if retrieved.Name != task.Name {
		t.Fatalf("Expected task name %s, got %s", task.Name, retrieved.Name)
	}

	// List tasks
	tasks, err := tm.ListTasks(ctx)
	if err != nil {
		t.Fatalf("Failed to list tasks: %v", err)
	}

	if len(tasks) != 1 {
		t.Fatalf("Expected 1 task, got %d", len(tasks))
	}

	// Update task
	task.Status = pipeline.TaskStatusRunning
	if err := tm.UpdateTask(ctx, task); err != nil {
		t.Fatalf("Failed to update task: %v", err)
	}

	retrieved, _ = tm.GetTask(ctx, "test-task-1")
	if retrieved.Status != pipeline.TaskStatusRunning {
		t.Fatalf("Expected task status running, got %s", retrieved.Status)
	}

	// Delete task
	if err := tm.DeleteTask(ctx, "test-task-1"); err != nil {
		t.Fatalf("Failed to delete task: %v", err)
	}

	// Verify deleted
	tasks, _ = tm.ListTasks(ctx)
	if len(tasks) != 0 {
		t.Fatalf("Expected 0 tasks after deletion, got %d", len(tasks))
	}

	t.Log("Task management integration test successful")
}

func TestPipelineDispatcher(t *testing.T) {
	// Create test events
	events := []*event.ChangeEvent{
		{
			ID:     "1",
			Source: "test",
			Type:   event.EventTypeInsert,
			Table:  "users",
			RowData: &event.RowData{
				Before: nil,
				After: map[string]interface{}{
					"id":   1,
					"name": "user1",
				},
			},
		},
		{
			ID:     "2",
			Source: "test",
			Type:   event.EventTypeInsert,
			Table:  "users",
			RowData: &event.RowData{
				Before: nil,
				After: map[string]interface{}{
					"id":   2,
					"name": "user2",
				},
			},
		},
	}

	// Test RoundRobinDispatcher
	t.Run("RoundRobin", func(t *testing.T) {
		sinks := make([]chan *event.ChangeEvent, 3)
		for i := range sinks {
			sinks[i] = make(chan *event.ChangeEvent, 10)
		}

		dispatcher := pipeline.NewRoundRobinDispatcher(sinks)
		ctx := context.Background()

		for _, e := range events {
			if err := dispatcher.Dispatch(ctx, e); err != nil {
				t.Fatalf("Dispatch failed: %v", err)
			}
		}

		// Verify distribution
		counts := make([]int, 3)
		for i, ch := range sinks {
			counts[i] = len(ch)
		}

		// Events should be distributed evenly
		total := counts[0] + counts[1] + counts[2]
		if total != len(events) {
			t.Fatalf("Expected %d total events, got %d", len(events), total)
		}
	})

	// Test HashDispatcher
	t.Run("Hash", func(t *testing.T) {
		sinks := make([]chan *event.ChangeEvent, 3)
		for i := range sinks {
			sinks[i] = make(chan *event.ChangeEvent, 10)
		}

		dispatcher := pipeline.NewHashDispatcher(sinks, "id")
		ctx := context.Background()

		for _, e := range events {
			if err := dispatcher.Dispatch(ctx, e); err != nil {
				t.Fatalf("Dispatch failed: %v", err)
			}
		}

		// Same key should go to same sink
		total := 0
		for _, ch := range sinks {
			total += len(ch)
		}
		if total != len(events) {
			t.Fatalf("Expected %d total events, got %d", len(events), total)
		}
	})

	t.Log("Dispatcher integration test successful")
}

func TestPipelineBuffer(t *testing.T) {
	ctx := context.Background()

	// Test MemoryBuffer
	t.Run("MemoryBuffer", func(t *testing.T) {
		buffer := pipeline.NewMemoryBuffer(1000)

		events := []*event.ChangeEvent{
			{ID: "1", Type: event.EventTypeInsert},
			{ID: "2", Type: event.EventTypeUpdate},
			{ID: "3", Type: event.EventTypeDelete},
		}

		for _, e := range events {
			if err := buffer.Write(ctx, e); err != nil {
				t.Fatalf("Buffer write failed: %v", err)
			}
		}

		// Read events
		readEvents, err := buffer.Read(ctx, 10)
		if err != nil {
			t.Fatalf("Buffer read failed: %v", err)
		}

		if len(readEvents) != len(events) {
			t.Fatalf("Expected %d events, got %d", len(events), len(readEvents))
		}
	})

	// Test BatchBuffer
	t.Run("BatchBuffer", func(t *testing.T) {
		innerBuffer := pipeline.NewMemoryBuffer(1000)
		batchBuffer := pipeline.NewBatchBuffer(innerBuffer, 2, time.Second)

		events := []*event.ChangeEvent{
			{ID: "1", Type: event.EventTypeInsert},
			{ID: "2", Type: event.EventTypeUpdate},
			{ID: "3", Type: event.EventTypeDelete},
		}

		for _, e := range events {
			if err := batchBuffer.Write(ctx, e); err != nil {
				t.Fatalf("Batch buffer write failed: %v", err)
			}
		}

		// Flush to ensure all events are in inner buffer
		if err := batchBuffer.Flush(ctx); err != nil {
			t.Fatalf("Batch buffer flush failed: %v", err)
		}

		readEvents, err := innerBuffer.Read(ctx, 10)
		if err != nil {
			t.Fatalf("Buffer read failed: %v", err)
		}

		if len(readEvents) != len(events) {
			t.Fatalf("Expected %d events, got %d", len(events), len(readEvents))
		}
	})

	t.Log("Buffer integration test successful")
}
