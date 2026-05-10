package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Task represents a data synchronization task.
type Task struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Status    TaskStatus      `json:"status"`
	Config    *Config         `json:"config"`
	Pipeline  *Pipeline       `json:"-"`
	Position  *event.Position `json:"position"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
	StartedAt *time.Time      `json:"startedAt,omitempty"`
	StoppedAt *time.Time      `json:"stoppedAt,omitempty"`
	mu        sync.RWMutex
}

// TaskStatus represents task status.
type TaskStatus string

const (
	TaskStatusCreated  TaskStatus = "created"
	TaskStatusStarting TaskStatus = "starting"
	TaskStatusRunning  TaskStatus = "running"
	TaskStatusPaused   TaskStatus = "paused"
	TaskStatusStopping TaskStatus = "stopping"
	TaskStatusStopped  TaskStatus = "stopped"
	TaskStatusError    TaskStatus = "error"
)

// NewTask creates a new task.
func NewTask(id, name string, config *Config) *Task {
	return &Task{
		ID:        id,
		Name:      name,
		Status:    TaskStatusCreated,
		Config:    config,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

// Start starts the task.
func (t *Task) Start(ctx context.Context) error {
	t.mu.Lock()
	if t.Status == TaskStatusRunning {
		t.mu.Unlock()
		return ErrAlreadyRunning
	}
	t.Status = TaskStatusStarting
	t.mu.Unlock()

	if t.Pipeline == nil {
		t.mu.Lock()
		t.Status = TaskStatusError
		t.mu.Unlock()
		return ErrNoSource
	}

	if err := t.Pipeline.Start(ctx); err != nil {
		t.mu.Lock()
		t.Status = TaskStatusError
		t.mu.Unlock()
		return err
	}

	now := time.Now()
	t.mu.Lock()
	t.Status = TaskStatusRunning
	t.StartedAt = &now
	t.UpdatedAt = now
	t.mu.Unlock()

	log.Info("task started", zap.String("id", t.ID))
	return nil
}

// Stop stops the task.
func (t *Task) Stop(ctx context.Context) error {
	t.mu.Lock()
	if t.Status == TaskStatusStopped {
		t.mu.Unlock()
		return nil
	}
	t.Status = TaskStatusStopping
	t.mu.Unlock()

	if t.Pipeline != nil {
		if err := t.Pipeline.Stop(ctx); err != nil {
			log.Error("failed to stop pipeline", zap.Error(err))
		}
	}

	now := time.Now()
	t.mu.Lock()
	t.Status = TaskStatusStopped
	t.StoppedAt = &now
	t.UpdatedAt = now
	t.mu.Unlock()

	log.Info("task stopped", zap.String("id", t.ID))
	return nil
}

// Pause pauses the task.
func (t *Task) Pause(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Status != TaskStatusRunning {
		return ErrInvalidState
	}

	t.Status = TaskStatusPaused
	t.UpdatedAt = time.Now()
	log.Info("task paused", zap.String("id", t.ID))
	return nil
}

// Resume resumes the task.
func (t *Task) Resume(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Status != TaskStatusPaused {
		return ErrInvalidState
	}

	t.Status = TaskStatusRunning
	t.UpdatedAt = time.Now()
	log.Info("task resumed", zap.String("id", t.ID))
	return nil
}

// GetPosition returns the current position.
func (t *Task) GetPosition() *event.Position {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.Position == nil {
		return nil
	}
	return t.Position.Clone()
}

// SetPosition sets the position.
func (t *Task) SetPosition(pos *event.Position) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Position = pos.Clone()
	t.UpdatedAt = time.Now()
}

// GetStatus returns the task status.
func (t *Task) GetStatus() TaskStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Status
}

// TaskManager manages multiple tasks.
type TaskManager struct {
	tasks       map[string]*Task
	mu          sync.RWMutex
	coordinator Coordinator
}

// NewTaskManager creates a new task manager.
func NewTaskManager() *TaskManager {
	return &TaskManager{
		tasks: make(map[string]*Task),
	}
}

// SetCoordinator sets the coordinator.
func (m *TaskManager) SetCoordinator(c Coordinator) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.coordinator = c
}

// Create creates a new task.
func (m *TaskManager) Create(ctx context.Context, id, name string, config *Config) (*Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.tasks[id]; exists {
		return nil, ErrTaskExists
	}

	task := NewTask(id, name, config)
	m.tasks[id] = task

	// Persist to coordinator
	if m.coordinator != nil {
		if err := m.coordinator.SaveTask(ctx, task); err != nil {
			delete(m.tasks, id)
			return nil, err
		}
	}

	log.Info("task created", zap.String("id", id))
	return task, nil
}

// Get retrieves a task by ID.
func (m *TaskManager) Get(id string) (*Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, ok := m.tasks[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return task, nil
}

// List lists all tasks.
func (m *TaskManager) List() []*Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tasks := make([]*Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		tasks = append(tasks, t)
	}
	return tasks
}

// Delete deletes a task.
func (m *TaskManager) Delete(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, ok := m.tasks[id]
	if !ok {
		return ErrTaskNotFound
	}

	// Stop task first
	if task.GetStatus() == TaskStatusRunning {
		if err := task.Stop(ctx); err != nil {
			return err
		}
	}

	// Delete from coordinator
	if m.coordinator != nil {
		if err := m.coordinator.DeleteTask(ctx, id); err != nil {
			return err
		}
	}

	delete(m.tasks, id)
	log.Info("task deleted", zap.String("id", id))
	return nil
}

// Start starts a task.
func (m *TaskManager) Start(ctx context.Context, id string) error {
	m.mu.RLock()
	task, ok := m.tasks[id]
	m.mu.RUnlock()

	if !ok {
		return ErrTaskNotFound
	}

	return task.Start(ctx)
}

// Stop stops a task.
func (m *TaskManager) Stop(ctx context.Context, id string) error {
	m.mu.RLock()
	task, ok := m.tasks[id]
	m.mu.RUnlock()

	if !ok {
		return ErrTaskNotFound
	}

	return task.Stop(ctx)
}

// StopAll stops all running tasks.
func (m *TaskManager) StopAll(ctx context.Context) error {
	m.mu.RLock()
	tasks := make([]*Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		if t.GetStatus() == TaskStatusRunning {
			tasks = append(tasks, t)
		}
	}
	m.mu.RUnlock()

	var lastErr error
	for _, t := range tasks {
		if err := t.Stop(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
