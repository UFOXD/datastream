// Package pipeline provides the core pipeline management for DataStream.
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	// NodeHeartbeatInterval is how often a node sends heartbeats.
	NodeHeartbeatInterval = 10 * time.Second

	// NodeExpiryThreshold is how long without heartbeat before a node is considered dead.
	NodeExpiryThreshold = 30 * time.Second

	// RebalanceInterval is how often the leader checks for rebalance.
	RebalanceInterval = 30 * time.Second

	// MaxTasksPerNode is the default maximum tasks per node.
	MaxTasksPerNode = 10
)

// ClusterManager handles multi-node cluster coordination.
type ClusterManager struct {
	coordinator Coordinator
	nodeID      string
	nodeInfo    NodeInfo
	localTasks  map[string]*Task // tasks owned by this node
	mu          sync.RWMutex

	heartbeatCtx    context.Context
	heartbeatCancel context.CancelFunc
	rebalanceCtx    context.Context
	rebalanceCancel context.CancelFunc
	wg              sync.WaitGroup

	// Callbacks
	onTaskAssigned   func(ctx context.Context, task *Task) error
	onTaskRevoked    func(ctx context.Context, taskID string) error
	onLeadershipLost func(ctx context.Context, taskID string)

	isLeader bool
}

// NewClusterManager creates a new cluster manager.
func NewClusterManager(coordinator Coordinator, nodeID, address, hostname string) *ClusterManager {
	return &ClusterManager{
		coordinator: coordinator,
		nodeID:      nodeID,
		nodeInfo: NodeInfo{
			ID:        nodeID,
			Address:   address,
			Hostname:  hostname,
			StartedAt: time.Now().UnixNano(),
			LastSeen:  time.Now().UnixNano(),
		},
		localTasks: make(map[string]*Task),
	}
}

// SetTaskCallbacks sets the callbacks for task assignment and revocation.
func (m *ClusterManager) SetTaskCallbacks(
	onAssigned func(ctx context.Context, task *Task) error,
	onRevoked func(ctx context.Context, taskID string) error,
	onLost func(ctx context.Context, taskID string),
) {
	m.onTaskAssigned = onAssigned
	m.onTaskRevoked = onRevoked
	m.onLeadershipLost = onLost
}

// Start starts the cluster manager: heartbeat + leader loop + failover detection.
func (m *ClusterManager) Start(ctx context.Context) error {
	m.heartbeatCtx, m.heartbeatCancel = context.WithCancel(ctx)
	m.rebalanceCtx, m.rebalanceCancel = context.WithCancel(ctx)

	// Register this node
	if err := m.coordinator.RegisterNode(ctx, m.nodeID, m.nodeInfo); err != nil {
		return fmt.Errorf("register node: %w", err)
	}

	// Start heartbeat goroutine
	m.wg.Add(1)
	go m.heartbeatLoop()

	// Start leader loop (try to become cluster leader)
	m.wg.Add(1)
	go m.leaderLoop()

	log.Info("cluster manager started",
		zap.String("nodeId", m.nodeID))
	return nil
}

// Stop stops the cluster manager gracefully.
func (m *ClusterManager) Stop(ctx context.Context) error {
	m.heartbeatCancel()
	m.rebalanceCancel()
	m.wg.Wait()

	// Release all task leaderships
	m.mu.RLock()
	taskIDs := make([]string, 0, len(m.localTasks))
	for id := range m.localTasks {
		taskIDs = append(taskIDs, id)
	}
	m.mu.RUnlock()

	for _, taskID := range taskIDs {
		if err := m.coordinator.ReleaseLeadership(ctx, taskID); err != nil {
			log.Warn("failed to release task leadership",
				zap.String("taskId", taskID),
				zap.Error(err))
		}
	}

	// Unregister node
	if err := m.coordinator.UnregisterNode(ctx, m.nodeID); err != nil {
		log.Warn("failed to unregister node", zap.Error(err))
	}

	log.Info("cluster manager stopped",
		zap.String("nodeId", m.nodeID))
	return nil
}

// heartbeatLoop sends periodic heartbeats.
func (m *ClusterManager) heartbeatLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(NodeHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.heartbeatCtx.Done():
			return
		case <-ticker.C:
			if err := m.coordinator.Heartbeat(m.heartbeatCtx, m.nodeID); err != nil {
				log.Warn("heartbeat failed", zap.Error(err))
			}
			// Renew node registration with timestamp
			m.mu.RLock()
			info := m.nodeInfo
			m.mu.RUnlock()
			info.LastSeen = time.Now().UnixNano()
			if err := m.coordinator.RegisterNode(m.heartbeatCtx, m.nodeID, info); err != nil {
				log.Warn("re-register node failed", zap.Error(err))
			}
		}
	}
}

// leaderLoop tries to become the cluster leader and manages task scheduling.
func (m *ClusterManager) leaderLoop() {
	defer m.wg.Done()

	// Try to become cluster leader (use a fixed cluster leadership key)
	const clusterLeadershipKey = "_cluster_leader"

	for {
		select {
		case <-m.rebalanceCtx.Done():
			return
		default:
		}

		isLeader, err := m.coordinator.IsLeader(m.rebalanceCtx, clusterLeadershipKey)
		if err != nil {
			log.Warn("failed to check leadership", zap.Error(err))
			time.Sleep(time.Second)
			continue
		}

		if !isLeader {
			// Try to become leader
			acquired, err := m.coordinator.AcquireLeadership(m.rebalanceCtx, clusterLeadershipKey)
			if err != nil {
				log.Warn("failed to acquire cluster leadership", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}
			if !acquired {
				time.Sleep(time.Second)
				continue
			}
			log.Info("became cluster leader")
			m.isLeader = true
		}

		// We're the leader: run rebalance loop
		m.leaderTasks(m.rebalanceCtx)

		// If we lost leadership, retry
		m.isLeader = false
		time.Sleep(time.Second)
	}
}

// leaderTasks runs the leader's task management loop.
func (m *ClusterManager) leaderTasks(ctx context.Context) {
	ticker := time.NewTicker(RebalanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Verify we're still leader
			isLeader, err := m.coordinator.IsLeader(ctx, "_cluster_leader")
			if err != nil || !isLeader {
				return
			}
			m.rebalanceCluster(ctx)
		}
	}
}

// rebalanceCluster detects dead nodes and reassigns their tasks.
func (m *ClusterManager) rebalanceCluster(ctx context.Context) {
	nodes, err := m.coordinator.ListNodes(ctx)
	if err != nil {
		log.Warn("failed to list nodes", zap.Error(err))
		return
	}

	tasks, err := m.coordinator.ListTasks(ctx)
	if err != nil {
		log.Warn("failed to list tasks", zap.Error(err))
		return
	}

	now := time.Now()
	aliveNodes := make(map[string]bool)
	var unassignedTasks []*Task

	// Collect alive nodes and their task counts
	nodeTaskCount := make(map[string]int)
	for _, node := range nodes {
		lastSeen := time.Unix(0, node.LastSeen)
		if now.Sub(lastSeen) > NodeExpiryThreshold {
			log.Warn("node detected as dead",
				zap.String("nodeId", node.ID),
				zap.Time("lastSeen", lastSeen))
			continue // skip dead nodes
		}
		aliveNodes[node.ID] = true
	}

	if len(aliveNodes) == 0 {
		return
	}

	// Count tasks per node and find unassigned tasks
	for _, task := range tasks {
		if task.Status != TaskStatusRunning && task.Status != TaskStatusCreated {
			continue
		}

		// Check if task has a leader
		isLeader, _ := m.coordinator.IsLeader(ctx, task.ID)
		if isLeader {
			// Find which node is the leader
			// Use the WatchLeadership mechanism
			unassignedTasks = append(unassignedTasks, task)
		} else {
			// Task is assigned - find its node
			leaderKey := fmt.Sprintf("%s/leadership/%s", "_datastream_internal", task.ID)
			_ = leaderKey
		}
	}

	// Assign unassigned tasks to nodes with lowest load
	for _, task := range unassignedTasks {
		targetNode := m.pickLeastLoaded(aliveNodes, nodeTaskCount)
		if targetNode == "" {
			break
		}
		nodeTaskCount[targetNode]++

		if targetNode == m.nodeID {
			// This node needs to pick it up
			m.acquireTask(ctx, task)
		}
	}
}

// pickLeastLoaded picks the node with fewest tasks.
func (m *ClusterManager) pickLeastLoaded(aliveNodes map[string]bool, taskCount map[string]int) string {
	var best string
	minTasks := MaxTasksPerNode + 1
	for nodeID := range aliveNodes {
		count := taskCount[nodeID]
		if count < minTasks {
			minTasks = count
			best = nodeID
		}
	}
	return best
}

// acquireTask tries to acquire leadership for a task.
func (m *ClusterManager) acquireTask(ctx context.Context, task *Task) {
	acquired, err := m.coordinator.AcquireLeadership(ctx, task.ID)
	if err != nil {
		log.Warn("failed to acquire task leadership",
			zap.String("taskId", task.ID),
			zap.Error(err))
		return
	}
	if !acquired {
		return // someone else got it
	}

	m.mu.Lock()
	m.localTasks[task.ID] = task
	m.mu.Unlock()

	log.Info("task acquired",
		zap.String("taskId", task.ID))

	// Notify via callback
	if m.onTaskAssigned != nil {
		if err := m.onTaskAssigned(ctx, task); err != nil {
			log.Error("failed to start task", zap.String("taskId", task.ID), zap.Error(err))
		}
	}
}

// WatchLeadership watches for leadership changes and triggers callbacks.
func (m *ClusterManager) WatchLeadership(ctx context.Context, taskID string) {
	ch, err := m.coordinator.WatchLeadership(ctx, taskID)
	if err != nil {
		log.Warn("failed to watch leadership", zap.String("taskId", taskID), zap.Error(err))
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case evt, ok := <-ch:
				if !ok {
					return
				}
				if !evt.IsLeader && evt.LeaderID != m.nodeID && m.onLeadershipLost != nil {
					// We lost leadership (another node took over)
					m.mu.Lock()
					delete(m.localTasks, taskID)
					m.mu.Unlock()
					m.onLeadershipLost(ctx, taskID)
				}
			}
		}
	}()
}

// LocalTaskCount returns the number of tasks on this node.
func (m *ClusterManager) LocalTaskCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.localTasks)
}
