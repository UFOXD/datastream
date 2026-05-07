// Package coordinator provides distributed coordination for DataStream.
package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/your-org/datastream/pkg/event"
	"github.com/your-org/datastream/pkg/pipeline"
	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

// EtcdCoordinator implements Coordinator using etcd.
type EtcdCoordinator struct {
	client   *clientv3.Client
	session  *concurrency.Session
	prefix   string
	nodeID   string
	leases   map[string]clientv3.LeaseID
	mu       sync.RWMutex
	watchers map[string][]chan pipeline.LeadershipEvent
}

// EtcdConfig holds etcd coordinator configuration.
type EtcdConfig struct {
	Endpoints   []string      `json:"endpoints"`
	DialTimeout time.Duration `json:"dialTimeout"`
	Username    string        `json:"username,omitempty"`
	Password    string        `json:"password,omitempty"`
	Prefix      string        `json:"prefix"` // Key prefix for all data
	NodeID      string        `json:"nodeId"`
	TTL         int           `json:"ttl"` // Session TTL in seconds
}

// NewEtcdCoordinator creates a new etcd coordinator.
func NewEtcdCoordinator(cfg *EtcdConfig) (*EtcdCoordinator, error) {
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "/datastream"
	}
	if cfg.TTL == 0 {
		cfg.TTL = 15
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
		Username:    cfg.Username,
		Password:    cfg.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	return &EtcdCoordinator{
		client:   client,
		prefix:   cfg.Prefix,
		nodeID:   cfg.NodeID,
		leases:   make(map[string]clientv3.LeaseID),
		watchers: make(map[string][]chan pipeline.LeadershipEvent),
	}, nil
}

// Initialize initializes the coordinator.
func (c *EtcdCoordinator) Initialize(ctx context.Context) error {
	// Create session for leadership election
	session, err := concurrency.NewSession(c.client,
		concurrency.WithContext(ctx),
		concurrency.WithTTL(15))
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	c.session = session

	log.Info("etcd coordinator initialized",
		zap.String("prefix", c.prefix),
		zap.String("nodeId", c.nodeID))
	return nil
}

// Close closes the coordinator.
func (c *EtcdCoordinator) Close() error {
	if c.session != nil {
		c.session.Close()
	}
	return c.client.Close()
}

// taskKey returns the key for a task.
func (c *EtcdCoordinator) taskKey(id string) string {
	return fmt.Sprintf("%s/tasks/%s", c.prefix, id)
}

// positionKey returns the key for a task's position.
func (c *EtcdCoordinator) positionKey(taskID string) string {
	return fmt.Sprintf("%s/positions/%s", c.prefix, taskID)
}

// leadershipKey returns the key for task leadership.
func (c *EtcdCoordinator) leadershipKey(taskID string) string {
	return fmt.Sprintf("%s/leadership/%s", c.prefix, taskID)
}

// nodeKey returns the key for a node.
func (c *EtcdCoordinator) nodeKey(nodeID string) string {
	return fmt.Sprintf("%s/nodes/%s", c.prefix, nodeID)
}

// SaveTask saves a task.
func (c *EtcdCoordinator) SaveTask(ctx context.Context, task *pipeline.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	_, err = c.client.Put(ctx, c.taskKey(task.ID), string(data))
	if err != nil {
		return fmt.Errorf("failed to save task: %w", err)
	}

	log.Debug("task saved", zap.String("id", task.ID))
	return nil
}

// GetTask retrieves a task by ID.
func (c *EtcdCoordinator) GetTask(ctx context.Context, id string) (*pipeline.Task, error) {
	resp, err := c.client.Get(ctx, c.taskKey(id))
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, pipeline.ErrTaskNotFound
	}

	var task pipeline.Task
	if err := json.Unmarshal(resp.Kvs[0].Value, &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}

	return &task, nil
}

// DeleteTask deletes a task.
func (c *EtcdCoordinator) DeleteTask(ctx context.Context, id string) error {
	_, err := c.client.Delete(ctx, c.taskKey(id))
	if err != nil {
		return fmt.Errorf("failed to delete task: %w", err)
	}

	// Also delete position and leadership
	c.client.Delete(ctx, c.positionKey(id))
	c.client.Delete(ctx, c.leadershipKey(id))

	log.Debug("task deleted", zap.String("id", id))
	return nil
}

// ListTasks lists all tasks.
func (c *EtcdCoordinator) ListTasks(ctx context.Context) ([]*pipeline.Task, error) {
	resp, err := c.client.Get(ctx, c.taskKey(""), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list tasks: %w", err)
	}

	tasks := make([]*pipeline.Task, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var task pipeline.Task
		if err := json.Unmarshal(kv.Value, &task); err != nil {
			log.Warn("failed to unmarshal task", zap.String("key", string(kv.Key)))
			continue
		}
		tasks = append(tasks, &task)
	}

	return tasks, nil
}

// SavePosition saves the position for a task.
func (c *EtcdCoordinator) SavePosition(ctx context.Context, taskID string, pos *event.Position) error {
	data, err := pos.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal position: %w", err)
	}

	_, err = c.client.Put(ctx, c.positionKey(taskID), string(data))
	if err != nil {
		return fmt.Errorf("failed to save position: %w", err)
	}

	return nil
}

// GetPosition retrieves the position for a task.
func (c *EtcdCoordinator) GetPosition(ctx context.Context, taskID string) (*event.Position, error) {
	resp, err := c.client.Get(ctx, c.positionKey(taskID))
	if err != nil {
		return nil, fmt.Errorf("failed to get position: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	var pos event.Position
	if err := pos.UnmarshalBinary(resp.Kvs[0].Value); err != nil {
		return nil, fmt.Errorf("failed to unmarshal position: %w", err)
	}

	return &pos, nil
}

// AcquireLeadership acquires leadership for a task.
func (c *EtcdCoordinator) AcquireLeadership(ctx context.Context, taskID string) (bool, error) {
	if c.session == nil {
		return false, fmt.Errorf("session not initialized")
	}

	key := c.leadershipKey(taskID)
	election := concurrency.NewElection(c.session, key)

	// Try to campaign for leadership
	err := election.Campaign(ctx, c.nodeID)
	if err != nil {
		if err == context.DeadlineExceeded || err == context.Canceled {
			return false, nil
		}
		return false, fmt.Errorf("campaign failed: %w", err)
	}

	log.Info("leadership acquired",
		zap.String("taskId", taskID),
		zap.String("nodeId", c.nodeID))
	return true, nil
}

// ReleaseLeadership releases leadership for a task.
func (c *EtcdCoordinator) ReleaseLeadership(ctx context.Context, taskID string) error {
	if c.session == nil {
		return nil
	}

	key := c.leadershipKey(taskID)
	election := concurrency.NewElection(c.session, key)

	if err := election.Resign(ctx); err != nil {
		return fmt.Errorf("resign failed: %w", err)
	}

	log.Info("leadership released",
		zap.String("taskId", taskID),
		zap.String("nodeId", c.nodeID))
	return nil
}

// IsLeader checks if this node is the leader for a task.
func (c *EtcdCoordinator) IsLeader(ctx context.Context, taskID string) (bool, error) {
	if c.session == nil {
		return false, nil
	}

	key := c.leadershipKey(taskID)
	election := concurrency.NewElection(c.session, key)

	resp, err := election.Leader(ctx)
	if err != nil {
		if err == concurrency.ErrElectionNoLeader {
			return false, nil
		}
		return false, err
	}

	if len(resp.Kvs) == 0 {
		return false, nil
	}

	return string(resp.Kvs[0].Value) == c.nodeID, nil
}

// WatchLeadership watches for leadership changes.
func (c *EtcdCoordinator) WatchLeadership(ctx context.Context, taskID string) (<-chan pipeline.LeadershipEvent, error) {
	ch := make(chan pipeline.LeadershipEvent, 10)

	c.mu.Lock()
	c.watchers[taskID] = append(c.watchers[taskID], ch)
	c.mu.Unlock()

	// Start watching the election key
	key := c.leadershipKey(taskID)
	watchCh := c.client.Watch(ctx, key)

	go func() {
		defer close(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case resp, ok := <-watchCh:
				if !ok {
					return
				}
				for _, ev := range resp.Events {
					evt := pipeline.LeadershipEvent{
						TaskID:    taskID,
						Timestamp: time.Now().UnixNano(),
					}
					if ev.Type == clientv3.EventTypePut {
						evt.LeaderID = string(ev.Kv.Value)
						evt.IsLeader = evt.LeaderID == c.nodeID
					}
					select {
					case ch <- evt:
					default:
					}
				}
			}
		}
	}()

	return ch, nil
}

// RegisterNode registers this node.
func (c *EtcdCoordinator) RegisterNode(ctx context.Context, nodeID string, info pipeline.NodeInfo) error {
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal node info: %w", err)
	}

	// Create with lease for TTL
	resp, err := c.client.Grant(ctx, 30) // 30 second TTL
	if err != nil {
		return fmt.Errorf("failed to create lease: %w", err)
	}

	_, err = c.client.Put(ctx, c.nodeKey(nodeID), string(data), clientv3.WithLease(resp.ID))
	if err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}

	c.mu.Lock()
	c.leases[nodeID] = resp.ID
	c.mu.Unlock()

	log.Info("node registered", zap.String("nodeId", nodeID))
	return nil
}

// UnregisterNode unregisters this node.
func (c *EtcdCoordinator) UnregisterNode(ctx context.Context, nodeID string) error {
	_, err := c.client.Delete(ctx, c.nodeKey(nodeID))
	if err != nil {
		return fmt.Errorf("failed to unregister node: %w", err)
	}

	c.mu.Lock()
	delete(c.leases, nodeID)
	c.mu.Unlock()

	log.Info("node unregistered", zap.String("nodeId", nodeID))
	return nil
}

// ListNodes lists all registered nodes.
func (c *EtcdCoordinator) ListNodes(ctx context.Context) ([]pipeline.NodeInfo, error) {
	resp, err := c.client.Get(ctx, c.nodeKey(""), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}

	nodes := make([]pipeline.NodeInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var info pipeline.NodeInfo
		if err := json.Unmarshal(kv.Value, &info); err != nil {
			log.Warn("failed to unmarshal node info", zap.String("key", string(kv.Key)))
			continue
		}
		nodes = append(nodes, info)
	}

	return nodes, nil
}

// Heartbeat sends a heartbeat for this node.
func (c *EtcdCoordinator) Heartbeat(ctx context.Context, nodeID string) error {
	c.mu.RLock()
	leaseID, ok := c.leases[nodeID]
	c.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no lease for node %s", nodeID)
	}

	_, err := c.client.KeepAliveOnce(ctx, leaseID)
	if err != nil {
		return fmt.Errorf("heartbeat failed: %w", err)
	}

	return nil
}
