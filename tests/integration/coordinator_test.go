// +build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/UFOXD/datastream/internal/coordinator"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/internal/pipeline"
)

func TestEtcdCoordinatorLifecycle(t *testing.T) {
	fixture := NewTestFixture(t)
	defer fixture.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create coordinator
	cfg := &coordinator.EtcdConfig{
		Endpoints: []string{fixture.Config.EtcdEndpoints},
		NodeID:    "test-node-1",
		Prefix:    "/datastream/test",
	}

	coord, err := coordinator.NewEtcdCoordinator(cfg)
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}
	defer coord.Close()

	// Initialize
	if err := coord.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize coordinator: %v", err)
	}

	t.Log("Coordinator lifecycle test passed")
}

func TestEtcdCoordinatorTaskCRUD(t *testing.T) {
	fixture := NewTestFixture(t)
	defer fixture.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := &coordinator.EtcdConfig{
		Endpoints: []string{fixture.Config.EtcdEndpoints},
		NodeID:    "test-node-1",
		Prefix:    "/datastream/test/crud",
	}

	coord, err := coordinator.NewEtcdCoordinator(cfg)
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}
	defer coord.Close()

	if err := coord.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize coordinator: %v", err)
	}

	// Create task
	task := pipeline.NewTask("task-1", "Test Task", &pipeline.Config{
		ID:   "pipeline-1",
		Name: "Test Pipeline",
	})

	// Save task
	if err := coord.SaveTask(ctx, task); err != nil {
		t.Fatalf("Failed to save task: %v", err)
	}

	// Get task
	retrieved, err := coord.GetTask(ctx, "task-1")
	if err != nil {
		t.Fatalf("Failed to get task: %v", err)
	}

	if retrieved.ID != task.ID {
		t.Errorf("Expected task ID '%s', got '%s'", task.ID, retrieved.ID)
	}
	if retrieved.Name != task.Name {
		t.Errorf("Expected task name '%s', got '%s'", task.Name, retrieved.Name)
	}

	// List tasks
	tasks, err := coord.ListTasks(ctx)
	if err != nil {
		t.Fatalf("Failed to list tasks: %v", err)
	}

	if len(tasks) != 1 {
		t.Errorf("Expected 1 task, got %d", len(tasks))
	}

	// Delete task
	if err := coord.DeleteTask(ctx, "task-1"); err != nil {
		t.Fatalf("Failed to delete task: %v", err)
	}

	// Verify task is deleted
	_, err = coord.GetTask(ctx, "task-1")
	if err == nil {
		t.Error("Expected error when getting deleted task")
	}

	t.Log("Task CRUD test passed")
}

func TestEtcdCoordinatorPosition(t *testing.T) {
	fixture := NewTestFixture(t)
	defer fixture.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := &coordinator.EtcdConfig{
		Endpoints: []string{fixture.Config.EtcdEndpoints},
		NodeID:    "test-node-1",
		Prefix:    "/datastream/test/position",
	}

	coord, err := coordinator.NewEtcdCoordinator(cfg)
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}
	defer coord.Close()

	if err := coord.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize coordinator: %v", err)
	}

	// Save position
	pos := &event.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
		LSN:        67890,
	}

	if err := coord.SavePosition(ctx, "task-1", pos); err != nil {
		t.Fatalf("Failed to save position: %v", err)
	}

	// Get position
	retrieved, err := coord.GetPosition(ctx, "task-1")
	if err != nil {
		t.Fatalf("Failed to get position: %v", err)
	}

	if retrieved == nil {
		t.Fatal("Expected non-nil position")
	}

	if retrieved.BinlogFile != pos.BinlogFile {
		t.Errorf("Expected binlog file '%s', got '%s'", pos.BinlogFile, retrieved.BinlogFile)
	}
	if retrieved.BinlogPos != pos.BinlogPos {
		t.Errorf("Expected binlog pos %d, got %d", pos.BinlogPos, retrieved.BinlogPos)
	}
	if retrieved.LSN != pos.LSN {
		t.Errorf("Expected LSN %d, got %d", pos.LSN, retrieved.LSN)
	}

	// Get non-existent position
	emptyPos, err := coord.GetPosition(ctx, "non-existent-task")
	if err != nil {
		t.Fatalf("Failed to get position: %v", err)
	}
	if emptyPos != nil {
		t.Error("Expected nil position for non-existent task")
	}

	t.Log("Position test passed")
}

func TestEtcdCoordinatorLeadership(t *testing.T) {
	fixture := NewTestFixture(t)
	defer fixture.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := &coordinator.EtcdConfig{
		Endpoints: []string{fixture.Config.EtcdEndpoints},
		NodeID:    "test-node-1",
		Prefix:    "/datastream/test/leadership",
	}

	coord, err := coordinator.NewEtcdCoordinator(cfg)
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}
	defer coord.Close()

	if err := coord.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize coordinator: %v", err)
	}

	// Acquire leadership
	acquired, err := coord.AcquireLeadership(ctx, "task-1")
	if err != nil {
		t.Fatalf("Failed to acquire leadership: %v", err)
	}

	if !acquired {
		t.Error("Expected to acquire leadership")
	}

	// Check if leader
	isLeader, err := coord.IsLeader(ctx, "task-1")
	if err != nil {
		t.Fatalf("Failed to check leadership: %v", err)
	}

	if !isLeader {
		t.Error("Expected to be leader")
	}

	// Release leadership
	if err := coord.ReleaseLeadership(ctx, "task-1"); err != nil {
		t.Fatalf("Failed to release leadership: %v", err)
	}

	t.Log("Leadership test passed")
}

func TestEtcdCoordinatorNodeManagement(t *testing.T) {
	fixture := NewTestFixture(t)
	defer fixture.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := &coordinator.EtcdConfig{
		Endpoints: []string{fixture.Config.EtcdEndpoints},
		NodeID:    "test-node-1",
		Prefix:    "/datastream/test/nodes",
	}

	coord, err := coordinator.NewEtcdCoordinator(cfg)
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}
	defer coord.Close()

	if err := coord.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize coordinator: %v", err)
	}

	// Register node
	info := pipeline.NodeInfo{
		ID:        "node-1",
		Address:   "localhost:8080",
		Hostname:  "test-host",
		StartedAt: time.Now().Unix(),
		LastSeen:  time.Now().Unix(),
		Labels: map[string]string{
			"zone": "us-west-1",
		},
	}

	if err := coord.RegisterNode(ctx, "node-1", info); err != nil {
		t.Fatalf("Failed to register node: %v", err)
	}

	// List nodes
	nodes, err := coord.ListNodes(ctx)
	if err != nil {
		t.Fatalf("Failed to list nodes: %v", err)
	}

	if len(nodes) != 1 {
		t.Errorf("Expected 1 node, got %d", len(nodes))
	}

	if nodes[0].ID != "node-1" {
		t.Errorf("Expected node ID 'node-1', got '%s'", nodes[0].ID)
	}

	// Heartbeat
	if err := coord.Heartbeat(ctx, "node-1"); err != nil {
		t.Fatalf("Failed to send heartbeat: %v", err)
	}

	// Unregister node
	if err := coord.UnregisterNode(ctx, "node-1"); err != nil {
		t.Fatalf("Failed to unregister node: %v", err)
	}

	// Verify node is unregistered
	nodes, err = coord.ListNodes(ctx)
	if err != nil {
		t.Fatalf("Failed to list nodes: %v", err)
	}

	if len(nodes) != 0 {
		t.Errorf("Expected 0 nodes after unregister, got %d", len(nodes))
	}

	t.Log("Node management test passed")
}

func TestEtcdCoordinatorMultipleTasks(t *testing.T) {
	fixture := NewTestFixture(t)
	defer fixture.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := &coordinator.EtcdConfig{
		Endpoints: []string{fixture.Config.EtcdEndpoints},
		NodeID:    "test-node-1",
		Prefix:    "/datastream/test/multi",
	}

	coord, err := coordinator.NewEtcdCoordinator(cfg)
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}
	defer coord.Close()

	if err := coord.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize coordinator: %v", err)
	}

	// Create multiple tasks
	for i := 1; i <= 5; i++ {
		taskID := string(rune('0' + i))
		task := pipeline.NewTask(taskID, "Task "+taskID, &pipeline.Config{
			ID:   "pipeline-" + taskID,
			Name: "Pipeline " + taskID,
		})

		if err := coord.SaveTask(ctx, task); err != nil {
			t.Fatalf("Failed to save task %s: %v", taskID, err)
		}
	}

	// List all tasks
	tasks, err := coord.ListTasks(ctx)
	if err != nil {
		t.Fatalf("Failed to list tasks: %v", err)
	}

	if len(tasks) != 5 {
		t.Errorf("Expected 5 tasks, got %d", len(tasks))
	}

	// Delete all tasks
	for i := 1; i <= 5; i++ {
		taskID := string(rune('0' + i))
		if err := coord.DeleteTask(ctx, taskID); err != nil {
			t.Fatalf("Failed to delete task %s: %v", taskID, err)
		}
	}

	// Verify all deleted
	tasks, err = coord.ListTasks(ctx)
	if err != nil {
		t.Fatalf("Failed to list tasks: %v", err)
	}

	if len(tasks) != 0 {
		t.Errorf("Expected 0 tasks after deletion, got %d", len(tasks))
	}

	t.Log("Multiple tasks test passed")
}
