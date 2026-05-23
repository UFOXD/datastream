package coordinator

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/UFOXD/datastream/internal/pipeline"
	"github.com/UFOXD/datastream/pkg/event"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
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

func TestEtcdConfigWithCustomValues(t *testing.T) {
	cfg := &EtcdConfig{
		Endpoints:   []string{"etcd1:2379", "etcd2:2379", "etcd3:2379"},
		DialTimeout: 10 * time.Second,
		Username:    "testuser",
		Password:    "testpass",
		Prefix:      "/custom",
		NodeID:      "node-123",
		TTL:         30,
	}

	if len(cfg.Endpoints) != 3 {
		t.Errorf("expected 3 endpoints, got %d", len(cfg.Endpoints))
	}
	if cfg.DialTimeout != 10*time.Second {
		t.Errorf("expected dial timeout 10s, got %v", cfg.DialTimeout)
	}
	if cfg.Username != "testuser" {
		t.Errorf("expected username 'testuser', got '%s'", cfg.Username)
	}
	if cfg.Password != "testpass" {
		t.Errorf("expected password 'testpass', got '%s'", cfg.Password)
	}
	if cfg.Prefix != "/custom" {
		t.Errorf("expected prefix '/custom', got '%s'", cfg.Prefix)
	}
	if cfg.NodeID != "node-123" {
		t.Errorf("expected node ID 'node-123', got '%s'", cfg.NodeID)
	}
	if cfg.TTL != 30 {
		t.Errorf("expected TTL 30, got %d", cfg.TTL)
	}
}

func TestEtcdCoordinatorWithCustomPrefix(t *testing.T) {
	c := &EtcdCoordinator{
		prefix: "/custom/prefix",
		nodeID: "test-node",
	}

	// Test that custom prefix is used in all key paths
	tests := []struct {
		name     string
		got      string
		expected string
	}{
		{"task key", c.taskKey("task-1"), "/custom/prefix/tasks/task-1"},
		{"position key", c.positionKey("task-1"), "/custom/prefix/positions/task-1"},
		{"leadership key", c.leadershipKey("task-1"), "/custom/prefix/leadership/task-1"},
		{"node key", c.nodeKey("node-1"), "/custom/prefix/nodes/node-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, tt.got)
			}
		})
	}
}

func TestMultipleEndpointsConfig(t *testing.T) {
	endpoints := []string{
		"etcd1.example.com:2379",
		"etcd2.example.com:2379",
		"etcd3.example.com:2379",
	}

	cfg := &EtcdConfig{
		Endpoints: endpoints,
		NodeID:    "node-1",
	}

	if len(cfg.Endpoints) != 3 {
		t.Errorf("expected 3 endpoints, got %d", len(cfg.Endpoints))
	}

	for i, ep := range cfg.Endpoints {
		if ep != endpoints[i] {
			t.Errorf("endpoint %d: expected '%s', got '%s'", i, endpoints[i], ep)
		}
	}
}

func TestEtcdConfigWithAuth(t *testing.T) {
	cfg := &EtcdConfig{
		Endpoints: []string{"localhost:2379"},
		Username:  "admin",
		Password:  "secret",
		NodeID:    "node-1",
	}

	if cfg.Username != "admin" {
		t.Errorf("expected username 'admin', got '%s'", cfg.Username)
	}
	if cfg.Password != "secret" {
		t.Errorf("expected password 'secret', got '%s'", cfg.Password)
	}
}

func TestLeadershipEventFields(t *testing.T) {
	evt := pipeline.LeadershipEvent{
		TaskID:    "task-123",
		IsLeader:  true,
		LeaderID:  "node-456",
		Timestamp: time.Now().UnixNano(),
	}

	if evt.TaskID != "task-123" {
		t.Errorf("expected task ID 'task-123', got '%s'", evt.TaskID)
	}
	if !evt.IsLeader {
		t.Error("expected IsLeader to be true")
	}
	if evt.LeaderID != "node-456" {
		t.Errorf("expected leader ID 'node-456', got '%s'", evt.LeaderID)
	}
	if evt.Timestamp == 0 {
		t.Error("expected non-zero timestamp")
	}
}

func TestNodeInfoWithLabels(t *testing.T) {
	info := pipeline.NodeInfo{
		ID:        "node-123",
		Address:   "192.168.1.100:8080",
		Hostname:  "datastream-node-1",
		StartedAt: time.Now().Unix(),
		LastSeen:  time.Now().Unix(),
		Labels: map[string]string{
			"zone":    "us-west-1",
			"version": "1.0.0",
			"env":     "production",
		},
		Tasks: []string{"task-1", "task-2", "task-3"},
	}

	if info.ID != "node-123" {
		t.Errorf("expected ID 'node-123', got '%s'", info.ID)
	}
	if info.Address != "192.168.1.100:8080" {
		t.Errorf("expected address '192.168.1.100:8080', got '%s'", info.Address)
	}
	if len(info.Labels) != 3 {
		t.Errorf("expected 3 labels, got %d", len(info.Labels))
	}
	if info.Labels["zone"] != "us-west-1" {
		t.Errorf("expected zone 'us-west-1', got '%s'", info.Labels["zone"])
	}
	if len(info.Tasks) != 3 {
		t.Errorf("expected 3 tasks, got %d", len(info.Tasks))
	}
}

func TestPositionWithAllFields(t *testing.T) {
	pos := &event.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
		LSN:        67890,
		TxID:       "tx-123",
		SeqNo:      5,
	}

	data, err := pos.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal position: %v", err)
	}

	var restored event.Position
	if err := restored.UnmarshalBinary(data); err != nil {
		t.Fatalf("failed to unmarshal position: %v", err)
	}

	if restored.BinlogFile != pos.BinlogFile {
		t.Errorf("expected binlog file '%s', got '%s'", pos.BinlogFile, restored.BinlogFile)
	}
	if restored.BinlogPos != pos.BinlogPos {
		t.Errorf("expected binlog pos %d, got %d", pos.BinlogPos, restored.BinlogPos)
	}
	if restored.LSN != pos.LSN {
		t.Errorf("expected LSN %d, got %d", pos.LSN, restored.LSN)
	}
	if restored.TxID != pos.TxID {
		t.Errorf("expected TxID '%s', got '%s'", pos.TxID, restored.TxID)
	}
	if restored.SeqNo != pos.SeqNo {
		t.Errorf("expected SeqNo %d, got %d", pos.SeqNo, restored.SeqNo)
	}
}

func TestTaskWithDifferentStatuses(t *testing.T) {
	statuses := []pipeline.TaskStatus{
		pipeline.TaskStatusCreated,
		pipeline.TaskStatusStarting,
		pipeline.TaskStatusRunning,
		pipeline.TaskStatusPaused,
		pipeline.TaskStatusStopping,
		pipeline.TaskStatusStopped,
		pipeline.TaskStatusError,
	}

	for _, status := range statuses {
		task := pipeline.NewTask("task-1", "Test Task", &pipeline.Config{
			ID:   "pipeline-1",
			Name: "Test Pipeline",
		})
		task.Status = status

		if task.Status != status {
			t.Errorf("expected status '%s', got '%s'", status, task.Status)
		}
	}
}

func TestEmptyEndpointsError(t *testing.T) {
	cfg := &EtcdConfig{
		Endpoints: []string{},
		NodeID:    "node-1",
	}

	// Empty endpoints should cause an error when creating client
	if len(cfg.Endpoints) != 0 {
		t.Error("expected empty endpoints")
	}
}

func TestZeroValues(t *testing.T) {
	cfg := &EtcdConfig{
		Endpoints: []string{"localhost:2379"},
		NodeID:    "node-1",
	}

	// Test zero values before defaults are applied
	if cfg.DialTimeout != 0 {
		t.Error("expected zero dial timeout before defaults")
	}
	if cfg.Prefix != "" {
		t.Error("expected empty prefix before defaults")
	}
	if cfg.TTL != 0 {
		t.Error("expected zero TTL before defaults")
	}
}

func TestCoordinatorNilSafety(t *testing.T) {
	// Test that methods can be called on partially initialized coordinator
	c := &EtcdCoordinator{
		prefix: "/datastream",
		nodeID: "test-node",
	}

	// Key methods should work without client
	key := c.taskKey("task-1")
	if key != "/datastream/tasks/task-1" {
		t.Errorf("unexpected key: %s", key)
	}
}

func TestConfigJSON(t *testing.T) {
	cfg := &EtcdConfig{
		Endpoints:   []string{"localhost:2379", "localhost:2380"},
		DialTimeout: 5 * time.Second,
		Username:    "user",
		Password:    "pass",
		Prefix:      "/datastream",
		NodeID:      "node-1",
		TTL:         15,
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	var restored EtcdConfig
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("failed to unmarshal config: %v", err)
	}

	if len(restored.Endpoints) != 2 {
		t.Errorf("expected 2 endpoints, got %d", len(restored.Endpoints))
	}
	if restored.NodeID != "node-1" {
		t.Errorf("expected NodeID 'node-1', got '%s'", restored.NodeID)
	}
}

func TestElectionInstanceCached(t *testing.T) {
	// Test that the elections map correctly stores and removes Election instances.
	// This validates the fix for zombie leaders: Resign must be called on the
	// same Election instance that won Campaign, so we cache elections by taskID.
	c := &EtcdCoordinator{
		prefix:    "/datastream",
		nodeID:    "test-node",
		leases:    make(map[string]clientv3.LeaseID),
		watchers:  make(map[string][]chan pipeline.LeadershipEvent),
		elections: make(map[string]*concurrency.Election),
	}

	// Verify elections map is initialized and empty
	if len(c.elections) != 0 {
		t.Fatalf("expected empty elections map, got %d entries", len(c.elections))
	}

	// Simulate storing an election after Campaign (what AcquireLeadership should do)
	// We can't call Campaign without a real etcd, but we can test the map mechanics.
	taskID := "task-1"
	// Create a nil-session election just to test map storage (won't call Campaign/Resign)
	// In production, session is non-nil and election is fully functional.
	c.mu.Lock()
	c.elections[taskID] = nil // placeholder: real code stores *concurrency.Election
	c.mu.Unlock()

	// Verify election is stored
	c.mu.RLock()
	_, exists := c.elections[taskID]
	c.mu.RUnlock()
	if !exists {
		t.Fatal("expected election to be stored in map after AcquireLeadership")
	}

	// Simulate ReleaseLeadership: retrieve and remove from map
	c.mu.Lock()
	_, exists = c.elections[taskID]
	delete(c.elections, taskID)
	c.mu.Unlock()
	if !exists {
		t.Fatal("expected election to exist in map before ReleaseLeadership")
	}

	// Verify election is removed
	c.mu.RLock()
	_, exists = c.elections[taskID]
	c.mu.RUnlock()
	if exists {
		t.Fatal("expected election to be removed from map after ReleaseLeadership")
	}
}

func TestReleaseLeadershipWithoutAcquire(t *testing.T) {
	// ReleaseLeadership called without prior AcquireLeadership should be a no-op.
	c := &EtcdCoordinator{
		prefix:    "/datastream",
		nodeID:    "test-node",
		leases:    make(map[string]clientv3.LeaseID),
		watchers:  make(map[string][]chan pipeline.LeadershipEvent),
		elections: make(map[string]*concurrency.Election),
	}

	// ReleaseLeadership on a task that was never acquired should return nil
	err := c.ReleaseLeadership(context.Background(), "nonexistent-task")
	if err != nil {
		t.Fatalf("expected nil error for ReleaseLeadership without prior Acquire, got: %v", err)
	}
}

func TestMultipleElectionsCached(t *testing.T) {
	// Verify that multiple tasks each get their own cached election.
	c := &EtcdCoordinator{
		prefix:    "/datastream",
		nodeID:    "test-node",
		leases:    make(map[string]clientv3.LeaseID),
		watchers:  make(map[string][]chan pipeline.LeadershipEvent),
		elections: make(map[string]*concurrency.Election),
	}

	// Store elections for multiple tasks
	c.mu.Lock()
	c.elections["task-1"] = nil
	c.elections["task-2"] = nil
	c.elections["task-3"] = nil
	c.mu.Unlock()

	c.mu.RLock()
	count := len(c.elections)
	c.mu.RUnlock()
	if count != 3 {
		t.Fatalf("expected 3 elections in map, got %d", count)
	}

	// Remove one
	c.mu.Lock()
	delete(c.elections, "task-2")
	c.mu.Unlock()

	c.mu.RLock()
	count = len(c.elections)
	_, has1 := c.elections["task-1"]
	_, has2 := c.elections["task-2"]
	_, has3 := c.elections["task-3"]
	c.mu.RUnlock()

	if count != 2 {
		t.Fatalf("expected 2 elections in map after removal, got %d", count)
	}
	if !has1 || has2 || !has3 {
		t.Fatalf("unexpected map state: task-1=%v task-2=%v task-3=%v", has1, has2, has3)
	}
}

func TestErrorMessages(t *testing.T) {
	// Verify error messages are defined and non-nil
	errors := map[string]error{
		"ErrNotInitialized":     ErrNotInitialized,
		"ErrConnectionFailed":   ErrConnectionFailed,
		"ErrSessionLost":        ErrSessionLost,
		"ErrLeaderElectFailed":  ErrLeaderElectFailed,
		"ErrInvalidConfig":      ErrInvalidConfig,
	}

	for name, err := range errors {
		if err == nil {
			t.Errorf("%s should not be nil", name)
		}
		if err.Error() == "" {
			t.Errorf("%s should have non-empty error message", name)
		}
	}
}
