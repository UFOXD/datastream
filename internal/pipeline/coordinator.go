package pipeline

import (
	"context"

	"github.com/UFOXD/datastream/pkg/event"
)

// Coordinator coordinates tasks across multiple nodes.
type Coordinator interface {
	// Initialize initializes the coordinator.
	Initialize(ctx context.Context) error

	// Close closes the coordinator.
	Close() error

	// SaveTask saves a task.
	SaveTask(ctx context.Context, task *Task) error

	// GetTask retrieves a task by ID.
	GetTask(ctx context.Context, id string) (*Task, error)

	// DeleteTask deletes a task.
	DeleteTask(ctx context.Context, id string) error

	// ListTasks lists all tasks.
	ListTasks(ctx context.Context) ([]*Task, error)

	// SavePosition saves the position for a task.
	SavePosition(ctx context.Context, taskID string, pos *event.Position) error

	// GetPosition retrieves the position for a task.
	GetPosition(ctx context.Context, taskID string) (*event.Position, error)

	// AcquireLeadership acquires leadership for a task.
	AcquireLeadership(ctx context.Context, taskID string) (bool, error)

	// ReleaseLeadership releases leadership for a task.
	ReleaseLeadership(ctx context.Context, taskID string) error

	// IsLeader checks if this node is the leader for a task.
	IsLeader(ctx context.Context, taskID string) (bool, error)

	// WatchLeadership watches for leadership changes.
	WatchLeadership(ctx context.Context, taskID string) (<-chan LeadershipEvent, error)

	// RegisterNode registers this node.
	RegisterNode(ctx context.Context, nodeID string, info NodeInfo) error

	// UnregisterNode unregisters this node.
	UnregisterNode(ctx context.Context, nodeID string) error

	// ListNodes lists all registered nodes.
	ListNodes(ctx context.Context) ([]NodeInfo, error)

	// Heartbeat sends a heartbeat for this node.
	Heartbeat(ctx context.Context, nodeID string) error
}

// LeadershipEvent represents a leadership change event.
type LeadershipEvent struct {
	TaskID    string
	IsLeader  bool
	LeaderID  string
	Timestamp int64
}

// NodeInfo holds information about a node.
type NodeInfo struct {
	ID        string            `json:"id"`
	Address   string            `json:"address"`
	Hostname  string            `json:"hostname"`
	StartedAt int64             `json:"startedAt"`
	LastSeen  int64             `json:"lastSeen"`
	Labels    map[string]string `json:"labels,omitempty"`
	Tasks     []string          `json:"tasks,omitempty"`
}

// MemoryCoordinator is an in-memory coordinator for single-node deployment.
type MemoryCoordinator struct {
	tasks     map[string]*Task
	positions map[string]*event.Position
	nodes     map[string]NodeInfo
	leaders   map[string]string // taskID -> nodeID
	thisNode  string
	watchers  map[string][]chan LeadershipEvent
}

// NewMemoryCoordinator creates a new memory coordinator.
func NewMemoryCoordinator(nodeID string) *MemoryCoordinator {
	return &MemoryCoordinator{
		tasks:     make(map[string]*Task),
		positions: make(map[string]*event.Position),
		nodes:     make(map[string]NodeInfo),
		leaders:   make(map[string]string),
		thisNode:  nodeID,
		watchers:  make(map[string][]chan LeadershipEvent),
	}
}

// Initialize initializes the coordinator.
func (c *MemoryCoordinator) Initialize(ctx context.Context) error {
	return nil
}

// Close closes the coordinator.
func (c *MemoryCoordinator) Close() error {
	return nil
}

// SaveTask saves a task.
func (c *MemoryCoordinator) SaveTask(ctx context.Context, task *Task) error {
	c.tasks[task.ID] = task
	return nil
}

// GetTask retrieves a task by ID.
func (c *MemoryCoordinator) GetTask(ctx context.Context, id string) (*Task, error) {
	task, ok := c.tasks[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return task, nil
}

// DeleteTask deletes a task.
func (c *MemoryCoordinator) DeleteTask(ctx context.Context, id string) error {
	delete(c.tasks, id)
	delete(c.positions, id)
	delete(c.leaders, id)
	return nil
}

// ListTasks lists all tasks.
func (c *MemoryCoordinator) ListTasks(ctx context.Context) ([]*Task, error) {
	tasks := make([]*Task, 0, len(c.tasks))
	for _, t := range c.tasks {
		tasks = append(tasks, t)
	}
	return tasks, nil
}

// SavePosition saves the position for a task.
func (c *MemoryCoordinator) SavePosition(ctx context.Context, taskID string, pos *event.Position) error {
	c.positions[taskID] = pos.Clone()
	return nil
}

// GetPosition retrieves the position for a task.
func (c *MemoryCoordinator) GetPosition(ctx context.Context, taskID string) (*event.Position, error) {
	pos, ok := c.positions[taskID]
	if !ok {
		return nil, nil
	}
	return pos.Clone(), nil
}

// AcquireLeadership acquires leadership for a task.
func (c *MemoryCoordinator) AcquireLeadership(ctx context.Context, taskID string) (bool, error) {
	if leader, ok := c.leaders[taskID]; ok {
		return leader == c.thisNode, nil
	}
	c.leaders[taskID] = c.thisNode
	return true, nil
}

// ReleaseLeadership releases leadership for a task.
func (c *MemoryCoordinator) ReleaseLeadership(ctx context.Context, taskID string) error {
	delete(c.leaders, taskID)
	return nil
}

// IsLeader checks if this node is the leader for a task.
func (c *MemoryCoordinator) IsLeader(ctx context.Context, taskID string) (bool, error) {
	leader, ok := c.leaders[taskID]
	if !ok {
		return false, nil
	}
	return leader == c.thisNode, nil
}

// WatchLeadership watches for leadership changes.
func (c *MemoryCoordinator) WatchLeadership(ctx context.Context, taskID string) (<-chan LeadershipEvent, error) {
	ch := make(chan LeadershipEvent, 10)
	c.watchers[taskID] = append(c.watchers[taskID], ch)
	return ch, nil
}

// RegisterNode registers this node.
func (c *MemoryCoordinator) RegisterNode(ctx context.Context, nodeID string, info NodeInfo) error {
	c.nodes[nodeID] = info
	return nil
}

// UnregisterNode unregisters this node.
func (c *MemoryCoordinator) UnregisterNode(ctx context.Context, nodeID string) error {
	delete(c.nodes, nodeID)
	return nil
}

// ListNodes lists all registered nodes.
func (c *MemoryCoordinator) ListNodes(ctx context.Context) ([]NodeInfo, error) {
	nodes := make([]NodeInfo, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	return nodes, nil
}

// Heartbeat sends a heartbeat for this node.
func (c *MemoryCoordinator) Heartbeat(ctx context.Context, nodeID string) error {
	if node, ok := c.nodes[nodeID]; ok {
		node.LastSeen = node.LastSeen // Would update timestamp
		c.nodes[nodeID] = node
	}
	return nil
}
