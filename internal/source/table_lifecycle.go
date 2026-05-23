package source

import (
	"fmt"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

// TableState represents the lifecycle state of a table in the CDC pipeline.
type TableState string

const (
	TableStatePending      TableState = "pending"
	TableStateSnapshotting TableState = "snapshotting"
	TableStateCatchingUp   TableState = "catching_up"
	TableStateStreaming     TableState = "streaming"
	TableStateError        TableState = "error"
	TableStatePaused       TableState = "paused"
)

// validTransitions defines the allowed state transitions via TransitionTo.
// Transitions to error are handled by SetError (any state → error).
// Transitions from error are handled by ResetToPending (error → pending).
// Transitions to/from paused are handled by Pause/Resume.
var validTransitions = map[TableState][]TableState{
	TableStatePending:      {TableStateSnapshotting},
	TableStateSnapshotting: {TableStateCatchingUp, TableStateError},
	TableStateCatchingUp:   {TableStateStreaming, TableStateCatchingUp, TableStateError, TableStatePaused},
	TableStateStreaming:    {TableStateError, TableStatePaused},
}

// CatchingUpProgress tracks the progress of a table during the catching-up phase.
type CatchingUpProgress struct {
	CurrentGTID string    `json:"currentGtid"`
	EventSeq    int64     `json:"eventSeq"`
	FileOffset  int64     `json:"fileOffset"`
	UpsertUntil time.Time `json:"upsertUntil"`
}

// TableLifecycle manages the state machine for a single table in the CDC pipeline.
type TableLifecycle struct {
	TableID            TableID            `json:"tableId"`
	State              TableState         `json:"state"`
	PreviousState      TableState         `json:"previousState,omitempty"`
	SnapshotPosition   *event.Position    `json:"snapshotPosition,omitempty"`
	StreamPosition     *event.Position    `json:"streamPosition,omitempty"`
	CatchingUpProgress CatchingUpProgress `json:"catchingUpProgress,omitempty"`
	RetryCount         int                `json:"retryCount"`
	MaxRetries         int                `json:"maxRetries"`
	LastError          string             `json:"lastError,omitempty"`
	LastStateChange    time.Time          `json:"lastStateChange"`
	mu                 sync.RWMutex
}

// NewTableLifecycle creates a new TableLifecycle in the pending state.
func NewTableLifecycle(tableID TableID) *TableLifecycle {
	return &TableLifecycle{
		TableID:         tableID,
		State:           TableStatePending,
		MaxRetries:      3,
		LastStateChange: time.Now(),
	}
}

// TransitionTo validates and performs a state transition.
// When entering snapshotting, pos is recorded as SnapshotPosition.
func (tl *TableLifecycle) TransitionTo(newState TableState, pos *event.Position) error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	allowed, ok := validTransitions[tl.State]
	if !ok {
		return fmt.Errorf("no transitions defined from state %q", tl.State)
	}

	valid := false
	for _, s := range allowed {
		if s == newState {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("invalid transition from %q to %q", tl.State, newState)
	}

	tl.PreviousState = tl.State
	tl.State = newState
	tl.LastStateChange = time.Now()

	if newState == TableStateSnapshotting && pos != nil {
		tl.SnapshotPosition = pos
	}

	if pos != nil {
		tl.StreamPosition = pos
	}

	return nil
}

// SetError transitions the table to the error state from any state.
// It records PreviousState and the error message.
func (tl *TableLifecycle) SetError(msg string) {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	tl.PreviousState = tl.State
	tl.State = TableStateError
	tl.LastError = msg
	tl.LastStateChange = time.Now()
}

// ResetToPending resets the table from error state back to pending.
// Order: set position, change state, clear error. Increments RetryCount.
func (tl *TableLifecycle) ResetToPending(newSnapshotPos *event.Position) error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	if tl.State != TableStateError {
		return fmt.Errorf("ResetToPending only allowed from error state, current state: %q", tl.State)
	}

	// Step 1: set position
	tl.SnapshotPosition = newSnapshotPos

	// Step 2: change state
	tl.PreviousState = tl.State
	tl.State = TableStatePending
	tl.LastStateChange = time.Now()

	// Step 3: clear error
	tl.LastError = ""

	tl.RetryCount++

	return nil
}

// Pause transitions the table to the paused state.
// Only allowed from catching_up or streaming.
func (tl *TableLifecycle) Pause() error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	if tl.State != TableStateCatchingUp && tl.State != TableStateStreaming {
		return fmt.Errorf("Pause only allowed from catching_up or streaming, current state: %q", tl.State)
	}

	tl.PreviousState = tl.State
	tl.State = TableStatePaused
	tl.LastStateChange = time.Now()

	return nil
}

// Resume transitions the table from paused back to PreviousState.
func (tl *TableLifecycle) Resume() error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	if tl.State != TableStatePaused {
		return fmt.Errorf("Resume only allowed from paused state, current state: %q", tl.State)
	}

	tl.State = tl.PreviousState
	tl.PreviousState = TableStatePaused
	tl.LastStateChange = time.Now()

	return nil
}

// GetState returns the current state in a thread-safe manner.
func (tl *TableLifecycle) GetState() TableState {
	tl.mu.RLock()
	defer tl.mu.RUnlock()
	return tl.State
}

// UpdateStreamPosition updates the stream position in a thread-safe manner.
func (tl *TableLifecycle) UpdateStreamPosition(pos *event.Position) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.StreamPosition = pos
}

// UpdateCatchingUpProgress updates the catching-up progress in a thread-safe manner.
func (tl *TableLifecycle) UpdateCatchingUpProgress(progress CatchingUpProgress) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	tl.CatchingUpProgress = progress
}
