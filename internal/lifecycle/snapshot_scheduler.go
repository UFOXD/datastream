package lifecycle

import (
	"context"
	"fmt"
	"sync"

	"github.com/UFOXD/datastream/internal/cache"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
)

// SnapshotScheduler is the central orchestrator for table lifecycle state
// transitions. It receives lifecycle events (snapshot complete, error, caught-up)
// and transitions tables between states accordingly.
type SnapshotScheduler struct {
	config *SchedulerConfig
	store  source.TableLifecycleStore
	cache  cache.BinlogCacheBackend
	taskID string
	mu     sync.RWMutex
}

// NewSnapshotScheduler creates a new SnapshotScheduler.
func NewSnapshotScheduler(config *SchedulerConfig, taskID string, store source.TableLifecycleStore, cacheBackend cache.BinlogCacheBackend) *SnapshotScheduler {
	return &SnapshotScheduler{
		config: config,
		store:  store,
		cache:  cacheBackend,
		taskID: taskID,
	}
}

// Store returns the underlying TableLifecycleStore.
func (s *SnapshotScheduler) Store() source.TableLifecycleStore {
	return s.store
}

// TaskID returns the task ID associated with this scheduler.
func (s *SnapshotScheduler) TaskID() string {
	return s.taskID
}

// AddTable registers a table and sets it to pending state.
func (s *SnapshotScheduler) AddTable(tableID source.TableID, snapshotPos *event.Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()
	lc := source.NewTableLifecycle(tableID)
	lc.SnapshotPosition = snapshotPos
	return s.store.Save(ctx, s.taskID, lc)
}

// OnSnapshotComplete transitions a table from snapshotting to catching_up.
func (s *SnapshotScheduler) OnSnapshotComplete(tableID source.TableID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()
	lc, err := s.store.Get(ctx, s.taskID, tableID)
	if err != nil {
		return fmt.Errorf("get table %v: %w", tableID, err)
	}

	if err := lc.TransitionTo(source.TableStateCatchingUp, nil); err != nil {
		return fmt.Errorf("transition %v to catching_up: %w", tableID, err)
	}

	return s.store.Save(ctx, s.taskID, lc)
}

// OnSnapshotError transitions a table to the error state.
func (s *SnapshotScheduler) OnSnapshotError(tableID source.TableID, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()
	lc, err := s.store.Get(ctx, s.taskID, tableID)
	if err != nil {
		return fmt.Errorf("get table %v: %w", tableID, err)
	}

	lc.SetError(errMsg)

	return s.store.Save(ctx, s.taskID, lc)
}

// OnCaughtUp transitions a table from catching_up to streaming and deletes
// the binlog cache for that table.
func (s *SnapshotScheduler) OnCaughtUp(tableID source.TableID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()
	lc, err := s.store.Get(ctx, s.taskID, tableID)
	if err != nil {
		return fmt.Errorf("get table %v: %w", tableID, err)
	}

	if err := lc.TransitionTo(source.TableStateStreaming, nil); err != nil {
		return fmt.Errorf("transition %v to streaming: %w", tableID, err)
	}

	if err := s.store.Save(ctx, s.taskID, lc); err != nil {
		return fmt.Errorf("save table %v: %w", tableID, err)
	}

	if err := s.cache.Delete(ctx, tableID.String()); err != nil {
		return fmt.Errorf("delete cache for %v: %w", tableID, err)
	}

	return nil
}

// RestartTable force-restarts a table: records a new position, transitions to
// pending, and clears the cache.
//
// If force is false and the table is currently streaming, an error is returned
// (conflict). If force is true or the table is in error state, the restart
// proceeds.
func (s *SnapshotScheduler) RestartTable(tableID source.TableID, newPos *event.Position, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()
	lc, err := s.store.Get(ctx, s.taskID, tableID)
	if err != nil {
		return fmt.Errorf("get table %v: %w", tableID, err)
	}

	state := lc.GetState()

	// If not forced and table is streaming, reject the restart.
	if !force && state == source.TableStateStreaming {
		return fmt.Errorf("table %v is streaming, use force=true to restart", tableID)
	}

	// If table is not already in error state, set error first so that
	// ResetToPending (which requires error state) can succeed.
	if state != source.TableStateError {
		lc.SetError("restart requested")
	}

	// ResetToPending: records new position, transitions error → pending, clears error.
	if err := lc.ResetToPending(newPos); err != nil {
		return fmt.Errorf("reset table %v to pending: %w", tableID, err)
	}

	if err := s.store.Save(ctx, s.taskID, lc); err != nil {
		return fmt.Errorf("save table %v: %w", tableID, err)
	}

	if err := s.cache.Delete(ctx, tableID.String()); err != nil {
		return fmt.Errorf("delete cache for %v: %w", tableID, err)
	}

	return nil
}

// RestartSchema restarts all tables belonging to a schema (database).
// Returns the list of table IDs that were successfully restarted.
func (s *SnapshotScheduler) RestartSchema(schema string, newPos *event.Position, force bool) ([]source.TableID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()
	tables, err := s.store.ListBySchema(ctx, s.taskID, schema)
	if err != nil {
		return nil, fmt.Errorf("list tables by schema %q: %w", schema, err)
	}

	var restarted []source.TableID
	for _, lc := range tables {
		tableID := lc.TableID
		state := lc.GetState()

		if !force && state == source.TableStateStreaming {
			continue
		}

		if state != source.TableStateError {
			lc.SetError("restart requested")
		}

		if err := lc.ResetToPending(newPos); err != nil {
			return restarted, fmt.Errorf("reset table %v to pending: %w", tableID, err)
		}

		if err := s.store.Save(ctx, s.taskID, lc); err != nil {
			return restarted, fmt.Errorf("save table %v: %w", tableID, err)
		}

		if err := s.cache.Delete(ctx, tableID.String()); err != nil {
			return restarted, fmt.Errorf("delete cache for %v: %w", tableID, err)
		}

		restarted = append(restarted, tableID)
	}

	return restarted, nil
}

// PauseTable pauses a table.
func (s *SnapshotScheduler) PauseTable(tableID source.TableID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()
	lc, err := s.store.Get(ctx, s.taskID, tableID)
	if err != nil {
		return fmt.Errorf("get table %v: %w", tableID, err)
	}

	if err := lc.Pause(); err != nil {
		return fmt.Errorf("pause table %v: %w", tableID, err)
	}

	return s.store.Save(ctx, s.taskID, lc)
}

// ResumeTable resumes a paused table.
func (s *SnapshotScheduler) ResumeTable(tableID source.TableID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()
	lc, err := s.store.Get(ctx, s.taskID, tableID)
	if err != nil {
		return fmt.Errorf("get table %v: %w", tableID, err)
	}

	if err := lc.Resume(); err != nil {
		return fmt.Errorf("resume table %v: %w", tableID, err)
	}

	return s.store.Save(ctx, s.taskID, lc)
}

// GetGlobalMinPosition returns the minimum position across all tables.
func (s *SnapshotScheduler) GetGlobalMinPosition() *event.Position {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ctx := context.Background()
	tables, err := s.store.List(ctx, s.taskID)
	if err != nil {
		return nil
	}

	return source.ComputeGlobalMinPosition(tables)
}

// ListErrors returns all tables in error state.
func (s *SnapshotScheduler) ListErrors() ([]*source.TableLifecycle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ctx := context.Background()
	return s.store.ListByState(ctx, s.taskID, source.TableStateError)
}
