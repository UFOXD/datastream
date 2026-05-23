package source

import (
	"context"
	"fmt"
	"sync"
)

// TableLifecycleStore defines the interface for persisting table lifecycle states.
type TableLifecycleStore interface {
	Save(ctx context.Context, taskID string, lc *TableLifecycle) error
	Get(ctx context.Context, taskID string, tableID TableID) (*TableLifecycle, error)
	Delete(ctx context.Context, taskID string, tableID TableID) error
	List(ctx context.Context, taskID string) ([]*TableLifecycle, error)
	ListByState(ctx context.Context, taskID string, state TableState) ([]*TableLifecycle, error)
	ListBySchema(ctx context.Context, taskID string, schema string) ([]*TableLifecycle, error)
}

// MemoryLifecycleStore is an in-memory implementation of TableLifecycleStore.
type MemoryLifecycleStore struct {
	mu   sync.RWMutex
	data map[string]map[TableID]*TableLifecycle // taskID → tableID → lifecycle
}

// NewMemoryLifecycleStore creates a new MemoryLifecycleStore.
func NewMemoryLifecycleStore() *MemoryLifecycleStore {
	return &MemoryLifecycleStore{
		data: make(map[string]map[TableID]*TableLifecycle),
	}
}

func (s *MemoryLifecycleStore) Save(_ context.Context, taskID string, lc *TableLifecycle) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[taskID]; !ok {
		s.data[taskID] = make(map[TableID]*TableLifecycle)
	}
	s.data[taskID][lc.TableID] = lc
	return nil
}

func (s *MemoryLifecycleStore) Get(_ context.Context, taskID string, tableID TableID) (*TableLifecycle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tables, ok := s.data[taskID]
	if !ok {
		return nil, fmt.Errorf("task %q not found", taskID)
	}
	lc, ok := tables[tableID]
	if !ok {
		return nil, fmt.Errorf("table %v not found in task %q", tableID, taskID)
	}
	return lc, nil
}

func (s *MemoryLifecycleStore) Delete(_ context.Context, taskID string, tableID TableID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables, ok := s.data[taskID]
	if !ok {
		return nil
	}
	delete(tables, tableID)
	return nil
}

func (s *MemoryLifecycleStore) List(_ context.Context, taskID string) ([]*TableLifecycle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tables, ok := s.data[taskID]
	if !ok {
		return nil, nil
	}
	result := make([]*TableLifecycle, 0, len(tables))
	for _, lc := range tables {
		result = append(result, lc)
	}
	return result, nil
}

func (s *MemoryLifecycleStore) ListByState(_ context.Context, taskID string, state TableState) ([]*TableLifecycle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tables, ok := s.data[taskID]
	if !ok {
		return nil, nil
	}
	var result []*TableLifecycle
	for _, lc := range tables {
		if lc.GetState() == state {
			result = append(result, lc)
		}
	}
	return result, nil
}

func (s *MemoryLifecycleStore) ListBySchema(_ context.Context, taskID string, schema string) ([]*TableLifecycle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tables, ok := s.data[taskID]
	if !ok {
		return nil, nil
	}
	var result []*TableLifecycle
	for _, lc := range tables {
		if lc.TableID.Database == schema {
			result = append(result, lc)
		}
	}
	return result, nil
}
