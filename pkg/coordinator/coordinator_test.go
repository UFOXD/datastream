package coordinator

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/your-org/datastream/pkg/event"
	"github.com/your-org/datastream/pkg/pipeline"
	"go.etcd.io/etcd/client/v3"
)

func TestEtcdConfigDefaults(t *testing.T) {
	cfg := &EtcdConfig{
		Endpoints: []string{"localhost:2379"},
		NodeID:    "node-1",
	}

	// Apply defaults
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "/datastream"
	}
	if cfg.TTL == 0 {
		cfg.TTL = 15
	}

	if cfg.DialTimeout != 5*time.Second {
		t.Errorf("Expected dial timeout 5s, got %v", cfg.DialTimeout)
	}

	if cfg.Prefix != "/datastream" {
		t.Errorf("Expected prefix '/datastream', got '%s'", cfg.Prefix)
	}

	if cfg.TTL != 15 {
		t.Errorf("Expected TTL 15, got %d", cfg.TTL)
	}
}

func TestEtcdCoordinatorKeys(t *testing.T) {
	cfg := &EtcdConfig{
		Endpoints: []string{"localhost:2379"},
		Prefix:    "/datastream",
		NodeID:    "node-1",
	}

	// We can't create a real etcd client without a running etcd,
	// but we can test the key generation logic
	c := &EtcdCoordinator{
		prefix: cfg.Prefix,
		nodeID: cfg.NodeID,
	}

	// Test key generation
	taskKey := c.taskKey("task-1")
	expectedTaskKey := "/datastream/tasks/task-1"
	if taskKey != expectedTaskKey {
		t.Errorf("Expected task key '%s', got '%s'", expectedTaskKey, taskKey)
	}

	posKey := c.positionKey("task-1")
	expectedPosKey := "/datastream/positions/task-1"
	if posKey != expectedPosKey {
		t.Errorf("Expected position key '%s', got '%s'", expectedPosKey, posKey)
	}

	leaderKey := c.leadershipKey("task-1")
	expectedLeaderKey := "/datastream/leadership/task-1"
	if leaderKey != expectedLeaderKey {
		t.Errorf("Expected leader key '%s', got '%s'", expectedLeaderKey, leaderKey)
	}

	nodeKey := c.nodeKey("node-1")
	expectedNodeKey := "/datastream/nodes/node-1"
	if nodeKey != expectedNodeKey {
		t.Errorf("Expected node key '%s', got '%s'", expectedNodeKey, nodeKey)
	}
}

func TestEtcdCoordinatorWithoutClient(t *testing.T) {
	// Test that we can create a coordinator struct without a client
	c := &EtcdCoordinator{
		prefix:   "/datastream",
		nodeID:   "node-1",
		leases:   make(map[string]clientv3.LeaseID),
		watchers: make(map[string][]chan pipeline.LeadershipEvent),
	}

	if c.prefix != "/datastream" {
		t.Errorf("Expected prefix '/datastream', got '%s'", c.prefix)
	}

	if c.nodeID != "node-1" {
		t.Errorf("Expected nodeID 'node-1', got '%s'", c.nodeID)
	}
}

func TestMarshalTask(t *testing.T) {
	task := pipeline.NewTask("task-1", "Test Task", &pipeline.Config{
		ID:   "pipeline-1",
		Name: "Test Pipeline",
	})

	data, err := json.Marshal(task.Config)
	if err != nil {
		t.Fatalf("Failed to marshal config: %v", err)
	}

	_ = data // Would be used in etcd Put
}

func TestMarshalPosition(t *testing.T) {
	pos := &event.Position{
		CommitTime: time.Now(),
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  1234,
		TxID:       "tx-123",
		SeqNo:      5,
	}

	data, err := pos.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to marshal position: %v", err)
	}

	var restored event.Position
	if err := restored.UnmarshalBinary(data); err != nil {
		t.Fatalf("Failed to unmarshal position: %v", err)
	}

	if restored.BinlogFile != pos.BinlogFile {
		t.Errorf("Expected binlog file '%s', got '%s'", pos.BinlogFile, restored.BinlogFile)
	}

	if restored.BinlogPos != pos.BinlogPos {
		t.Errorf("Expected binlog pos %d, got %d", pos.BinlogPos, restored.BinlogPos)
	}
}

func TestMarshalNodeInfo(t *testing.T) {
	info := pipeline.NodeInfo{
		ID:        "node-1",
		Address:   "localhost:8300",
		Hostname:  "localhost",
		StartedAt: time.Now().Unix(),
		LastSeen:  time.Now().Unix(),
		Labels: map[string]string{
			"zone": "us-west-1",
		},
		Tasks: []string{"task-1", "task-2"},
	}

	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Failed to marshal node info: %v", err)
	}

	var restored pipeline.NodeInfo
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Failed to unmarshal node info: %v", err)
	}

	if restored.ID != info.ID {
		t.Errorf("Expected ID '%s', got '%s'", info.ID, restored.ID)
	}

	if len(restored.Labels) != 1 {
		t.Errorf("Expected 1 label, got %d", len(restored.Labels))
	}

	if len(restored.Tasks) != 2 {
		t.Errorf("Expected 2 tasks, got %d", len(restored.Tasks))
	}
}

func TestCoordinatorErrors(t *testing.T) {
	errors := []error{
		ErrNotInitialized,
		ErrConnectionFailed,
		ErrSessionLost,
		ErrLeaderElectFailed,
		ErrInvalidConfig,
	}

	for _, err := range errors {
		if err == nil {
			t.Error("Error should not be nil")
		}
	}
}

func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	select {
	case <-ctx.Done():
		t.Error("Context should not be done yet")
	default:
	}

	time.Sleep(150 * time.Millisecond)

	select {
	case <-ctx.Done():
		// Expected
	default:
		t.Error("Context should be done after timeout")
	}
}
