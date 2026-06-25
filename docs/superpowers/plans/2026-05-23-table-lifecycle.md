# Table-Level Independent Lifecycle — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable each table to independently progress through snapshot → catching_up → streaming states, so large tables don't block smaller ones from entering real-time sync.

**Architecture:** Decomposed into 4 sub-plans. This is **Sub-Plan 1: Foundation** — the types, state machine, persistence, and event.Position extension that all other sub-plans depend on. Sub-Plans 2-4 (BinlogCache, Core Engine, API) will be planned separately after this is implemented.

**Tech Stack:** Go 1.22+, Badger v4, Protobuf, gorilla/mux

**Design Doc:** `docs/design/table-lifecycle-design.md`

---

## Sub-Plan Decomposition

| Sub-Plan | Scope | Dependencies |
|----------|-------|-------------|
| **1. Foundation (this plan)** | TableLifecycle state machine, event.Position extension, TableLifecycleStore, GlobalMinPosition | None |
| 2. BinlogCache | BinlogCacheBackend interface, LocalBackend (Badger), CacheEvent Protobuf, CLI decode tool | Sub-Plan 1 |
| 3. Core Engine | BinlogConsumer, SnapshotScheduler, CatchingUpReplayer, hash-based chunking, S3 path | Sub-Plans 1+2 |
| 4. API & CLI | Lifecycle API endpoints (§12), CLI commands, monitoring metrics | Sub-Plans 1+2+3 |

---

## File Structure (Sub-Plan 1)

```
pkg/event/
  position.go           — MODIFY: add GTID + ResumeToken fields

internal/source/
  table_lifecycle.go    — CREATE: TableState, TableLifecycle, state machine logic
  table_lifecycle_test.go — CREATE: state machine tests
  lifecycle_store.go    — CREATE: TableLifecycleStore interface + MemoryStore
  lifecycle_store_test.go — CREATE: store tests
  global_min_position.go — CREATE: GlobalMinPosition calculator
  global_min_position_test.go — CREATE: min position tests
  table_manager.go      — MODIFY: replace TableSyncState with TableLifecycle
  connector.go          — MODIFY: add TableState type alias if needed
```

---

### Task 1: Extend event.Position with GTID and ResumeToken

**Files:**
- Modify: `pkg/event/position.go`
- Modify: `pkg/event/event_test.go`

- [ ] **Step 1: Write failing test**

```go
// In pkg/event/event_test.go, add:
func TestPositionGTIDField(t *testing.T) {
	pos := &Position{
		GTID:       "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
		CommitTime: time.Now(),
	}
	if pos.GTID == "" {
		t.Error("GTID should be set")
	}
	if pos.IsZero() {
		t.Error("Position with GTID should not be zero")
	}

	cloned := pos.Clone()
	if cloned.GTID != pos.GTID {
		t.Errorf("Clone GTID = %q, want %q", cloned.GTID, pos.GTID)
	}
}

func TestPositionResumeTokenField(t *testing.T) {
	token := []byte(`{"_data": "826470..."}`)
	pos := &Position{
		ResumeToken: token,
		CommitTime:  time.Now(),
	}
	if pos.ResumeToken == nil {
		t.Error("ResumeToken should be set")
	}

	cloned := pos.Clone()
	if !bytes.Equal(cloned.ResumeToken, pos.ResumeToken) {
		t.Error("Clone should copy ResumeToken")
	}

	// Mutate original should not affect clone
	pos.ResumeToken[0] = 0xFF
	if cloned.ResumeToken[0] == 0xFF {
		t.Error("Clone ResumeToken should be independent copy")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/event/ -run "TestPositionGTID|TestPositionResumeToken" -count=1 -v`
Expected: FAIL — `pos.GTID undefined`, `pos.ResumeToken undefined`

- [ ] **Step 3: Add GTID and ResumeToken fields to Position**

In `pkg/event/position.go`, add to the Position struct:

```go
// MySQL GTID Set (e.g., "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5")
GTID string `json:"gtid,omitempty"`

// MongoDB Change Stream resume token
ResumeToken []byte `json:"resumeToken,omitempty"`
```

Update `IsZero()` to check GTID:
```go
func (p *Position) IsZero() bool {
	return p.CommitTime.IsZero() && p.BinlogFile == "" && p.LSN == 0 &&
		p.SCN == 0 && p.Timestamp == 0 && p.ChangeLsn == "" && p.GTID == ""
}
```

Update `Clone()` to deep copy ResumeToken:
```go
func (p *Position) Clone() *Position {
	c := &Position{
		BinlogFile: p.BinlogFile,
		BinlogPos:  p.BinlogPos,
		LSN:        p.LSN,
		SCN:        p.SCN,
		Timestamp:  p.Timestamp,
		Order:      p.Order,
		ChangeLsn:  p.ChangeLsn,
		CommitTime: p.CommitTime,
		TxID:       p.TxID,
		SeqNo:      p.SeqNo,
		Total:      p.Total,
		GTID:       p.GTID,
	}
	if p.ResumeToken != nil {
		c.ResumeToken = make([]byte, len(p.ResumeToken))
		copy(c.ResumeToken, p.ResumeToken)
	}
	return c
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/event/ -run "TestPositionGTID|TestPositionResumeToken" -count=1 -v`
Expected: PASS

- [ ] **Step 5: Run all event tests**

Run: `go test ./pkg/event/ -count=1`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add pkg/event/position.go pkg/event/event_test.go
git commit -m "feat(event): extend Position with GTID and ResumeToken fields"
```

---

### Task 2: TableLifecycle state machine

**Files:**
- Create: `internal/source/table_lifecycle.go`
- Create: `internal/source/table_lifecycle_test.go`

- [ ] **Step 1: Write failing tests**

```go
package source

import (
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

func TestTableStateConstants(t *testing.T) {
	states := []TableState{
		TableStatePending,
		TableStateSnapshotting,
		TableStateCatchingUp,
		TableStateStreaming,
		TableStateError,
		TableStatePaused,
	}
	for _, s := range states {
		if s == "" {
			t.Error("state should not be empty")
		}
	}
}

func TestNewTableLifecycle(t *testing.T) {
	tid := TableID{Database: "db1", Table: "users"}
	lc := NewTableLifecycle(tid)

	if lc.State != TableStatePending {
		t.Errorf("initial state = %q, want %q", lc.State, TableStatePending)
	}
	if lc.TableID != tid {
		t.Errorf("TableID mismatch")
	}
	if lc.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0", lc.RetryCount)
	}
}

func TestTransitionPendingToSnapshotting(t *testing.T) {
	lc := NewTableLifecycle(TableID{Database: "db1", Table: "users"})
	pos := &event.Position{GTID: "uuid:100", CommitTime: time.Now()}

	err := lc.TransitionTo(TableStateSnapshotting, pos)
	if err != nil {
		t.Fatalf("TransitionTo failed: %v", err)
	}
	if lc.State != TableStateSnapshotting {
		t.Errorf("state = %q, want snapshotting", lc.State)
	}
	if lc.SnapshotPosition == nil || lc.SnapshotPosition.GTID != "uuid:100" {
		t.Error("SnapshotPosition should be set")
	}
}

func TestTransitionSnapshotToCatchingUp(t *testing.T) {
	lc := NewTableLifecycle(TableID{Database: "db1", Table: "users"})
	pos := &event.Position{GTID: "uuid:100"}
	lc.TransitionTo(TableStateSnapshotting, pos)

	err := lc.TransitionTo(TableStateCatchingUp, nil)
	if err != nil {
		t.Fatalf("TransitionTo failed: %v", err)
	}
	if lc.State != TableStateCatchingUp {
		t.Errorf("state = %q, want catching_up", lc.State)
	}
}

func TestTransitionCatchingUpToStreaming(t *testing.T) {
	lc := NewTableLifecycle(TableID{Database: "db1", Table: "users"})
	lc.TransitionTo(TableStateSnapshotting, &event.Position{GTID: "uuid:100"})
	lc.TransitionTo(TableStateCatchingUp, nil)

	err := lc.TransitionTo(TableStateStreaming, nil)
	if err != nil {
		t.Fatalf("TransitionTo failed: %v", err)
	}
	if lc.State != TableStateStreaming {
		t.Errorf("state = %q, want streaming", lc.State)
	}
}

func TestInvalidTransition(t *testing.T) {
	lc := NewTableLifecycle(TableID{Database: "db1", Table: "users"})

	// pending -> streaming is invalid (must go through snapshotting first)
	err := lc.TransitionTo(TableStateStreaming, nil)
	if err == nil {
		t.Error("expected error for invalid transition pending -> streaming")
	}
}

func TestTransitionToError(t *testing.T) {
	lc := NewTableLifecycle(TableID{Database: "db1", Table: "users"})
	lc.TransitionTo(TableStateSnapshotting, &event.Position{GTID: "uuid:100"})

	lc.SetError("connection timeout")
	if lc.State != TableStateError {
		t.Errorf("state = %q, want error", lc.State)
	}
	if lc.LastError != "connection timeout" {
		t.Errorf("LastError = %q, want 'connection timeout'", lc.LastError)
	}
	if lc.PreviousState != TableStateSnapshotting {
		t.Errorf("PreviousState = %q, want snapshotting", lc.PreviousState)
	}
}

func TestTransitionErrorToPending(t *testing.T) {
	lc := NewTableLifecycle(TableID{Database: "db1", Table: "users"})
	lc.TransitionTo(TableStateSnapshotting, &event.Position{GTID: "uuid:100"})
	lc.SetError("timeout")

	newPos := &event.Position{GTID: "uuid:200"}
	err := lc.ResetToPending(newPos)
	if err != nil {
		t.Fatalf("ResetToPending failed: %v", err)
	}
	if lc.State != TableStatePending {
		t.Errorf("state = %q, want pending", lc.State)
	}
	if lc.SnapshotPosition.GTID != "uuid:200" {
		t.Error("SnapshotPosition should be updated to new position")
	}
	if lc.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", lc.RetryCount)
	}
}

func TestPauseOnlyAllowedInCatchingUpAndStreaming(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*TableLifecycle)
		wantErr bool
	}{
		{"pause from pending", func(lc *TableLifecycle) {}, true},
		{"pause from snapshotting", func(lc *TableLifecycle) {
			lc.TransitionTo(TableStateSnapshotting, &event.Position{GTID: "uuid:1"})
		}, true},
		{"pause from catching_up", func(lc *TableLifecycle) {
			lc.TransitionTo(TableStateSnapshotting, &event.Position{GTID: "uuid:1"})
			lc.TransitionTo(TableStateCatchingUp, nil)
		}, false},
		{"pause from streaming", func(lc *TableLifecycle) {
			lc.TransitionTo(TableStateSnapshotting, &event.Position{GTID: "uuid:1"})
			lc.TransitionTo(TableStateCatchingUp, nil)
			lc.TransitionTo(TableStateStreaming, nil)
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc := NewTableLifecycle(TableID{Database: "db1", Table: "t"})
			tt.setup(lc)
			err := lc.Pause()
			if (err != nil) != tt.wantErr {
				t.Errorf("Pause() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestResumeRestoresPreviousState(t *testing.T) {
	lc := NewTableLifecycle(TableID{Database: "db1", Table: "t"})
	lc.TransitionTo(TableStateSnapshotting, &event.Position{GTID: "uuid:1"})
	lc.TransitionTo(TableStateCatchingUp, nil)
	lc.Pause()

	if lc.State != TableStatePaused {
		t.Fatalf("state = %q, want paused", lc.State)
	}

	err := lc.Resume()
	if err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
	if lc.State != TableStateCatchingUp {
		t.Errorf("state after resume = %q, want catching_up", lc.State)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/source/ -run "TestTableState|TestNewTableLifecycle|TestTransition|TestPause|TestResume" -count=1 -v`
Expected: FAIL — types not defined

- [ ] **Step 3: Implement TableLifecycle**

Create `internal/source/table_lifecycle.go`:

```go
package source

import (
	"fmt"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

type TableState string

const (
	TableStatePending      TableState = "pending"
	TableStateSnapshotting TableState = "snapshotting"
	TableStateCatchingUp   TableState = "catching_up"
	TableStateStreaming     TableState = "streaming"
	TableStateError        TableState = "error"
	TableStatePaused       TableState = "paused"
)

type CatchingUpProgress struct {
	CurrentGTID string    `json:"currentGtid"`
	EventSeq    int64     `json:"eventSeq"`
	FileOffset  int64     `json:"fileOffset"`
	UpsertUntil time.Time `json:"upsertUntil"`
}

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

func NewTableLifecycle(tableID TableID) *TableLifecycle {
	return &TableLifecycle{
		TableID:         tableID,
		State:           TableStatePending,
		MaxRetries:      3,
		LastStateChange: time.Now(),
	}
}

// validTransitions defines which state transitions are allowed.
var validTransitions = map[TableState][]TableState{
	TableStatePending:      {TableStateSnapshotting},
	TableStateSnapshotting: {TableStateCatchingUp, TableStateError},
	TableStateCatchingUp:   {TableStateStreaming, TableStateCatchingUp, TableStateError, TableStatePaused},
	TableStateStreaming:     {TableStateError, TableStatePaused},
	TableStateError:        {TableStatePending},
	TableStatePaused:       {}, // handled by Resume()
}

func (lc *TableLifecycle) TransitionTo(newState TableState, pos *event.Position) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	allowed := validTransitions[lc.State]
	valid := false
	for _, s := range allowed {
		if s == newState {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("invalid transition: %s -> %s", lc.State, newState)
	}

	lc.PreviousState = lc.State
	lc.State = newState
	lc.LastStateChange = time.Now()

	if newState == TableStateSnapshotting && pos != nil {
		lc.SnapshotPosition = pos.Clone()
	}
	if newState == TableStatePending {
		lc.LastError = ""
	}

	return nil
}

func (lc *TableLifecycle) SetError(msg string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	lc.PreviousState = lc.State
	lc.State = TableStateError
	lc.LastError = msg
	lc.LastStateChange = time.Now()
}

func (lc *TableLifecycle) ResetToPending(newSnapshotPos *event.Position) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	if lc.State != TableStateError {
		return fmt.Errorf("can only reset from error state, current: %s", lc.State)
	}

	// Order: record position first, then change state (M5)
	lc.SnapshotPosition = newSnapshotPos.Clone()
	lc.PreviousState = lc.State
	lc.State = TableStatePending
	lc.RetryCount++
	lc.LastError = ""
	lc.LastStateChange = time.Now()
	lc.CatchingUpProgress = CatchingUpProgress{}

	return nil
}

func (lc *TableLifecycle) Pause() error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	if lc.State != TableStateCatchingUp && lc.State != TableStateStreaming {
		return fmt.Errorf("pause only allowed in catching_up or streaming, current: %s", lc.State)
	}

	lc.PreviousState = lc.State
	lc.State = TableStatePaused
	lc.LastStateChange = time.Now()
	return nil
}

func (lc *TableLifecycle) Resume() error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	if lc.State != TableStatePaused {
		return fmt.Errorf("resume only allowed from paused, current: %s", lc.State)
	}

	lc.State = lc.PreviousState
	lc.PreviousState = TableStatePaused
	lc.LastStateChange = time.Now()
	return nil
}

func (lc *TableLifecycle) GetState() TableState {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	return lc.State
}

func (lc *TableLifecycle) UpdateStreamPosition(pos *event.Position) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.StreamPosition = pos.Clone()
}

func (lc *TableLifecycle) UpdateCatchingUpProgress(progress CatchingUpProgress) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.CatchingUpProgress = progress
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/source/ -run "TestTableState|TestNewTableLifecycle|TestTransition|TestPause|TestResume" -count=1 -v`
Expected: All PASS

- [ ] **Step 5: Run all source tests**

Run: `go test ./internal/source/ -count=1`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add internal/source/table_lifecycle.go internal/source/table_lifecycle_test.go
git commit -m "feat(source): add TableLifecycle state machine with 6 states"
```

---

### Task 3: TableLifecycleStore (persistence)

**Files:**
- Create: `internal/source/lifecycle_store.go`
- Create: `internal/source/lifecycle_store_test.go`

- [ ] **Step 1: Write failing tests**

```go
package source

import (
	"context"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

func TestMemoryLifecycleStoreBasicCRUD(t *testing.T) {
	store := NewMemoryLifecycleStore()
	ctx := context.Background()

	tid := TableID{Database: "db1", Table: "users"}
	lc := NewTableLifecycle(tid)
	lc.TransitionTo(TableStateSnapshotting, &event.Position{GTID: "uuid:100"})

	// Save
	if err := store.Save(ctx, "task-1", lc); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Get
	got, err := store.Get(ctx, "task-1", tid)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.State != TableStateSnapshotting {
		t.Errorf("state = %q, want snapshotting", got.State)
	}

	// List
	all, err := store.List(ctx, "task-1")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("List returned %d, want 1", len(all))
	}

	// Delete
	if err := store.Delete(ctx, "task-1", tid); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	all, _ = store.List(ctx, "task-1")
	if len(all) != 0 {
		t.Errorf("List after delete returned %d, want 0", len(all))
	}
}

func TestMemoryLifecycleStoreGetNotFound(t *testing.T) {
	store := NewMemoryLifecycleStore()
	ctx := context.Background()

	_, err := store.Get(ctx, "task-1", TableID{Database: "db1", Table: "nope"})
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestMemoryLifecycleStoreListByState(t *testing.T) {
	store := NewMemoryLifecycleStore()
	ctx := context.Background()

	lc1 := NewTableLifecycle(TableID{Database: "db1", Table: "t1"})
	lc1.TransitionTo(TableStateSnapshotting, &event.Position{GTID: "uuid:1"})

	lc2 := NewTableLifecycle(TableID{Database: "db1", Table: "t2"})
	// stays pending

	lc3 := NewTableLifecycle(TableID{Database: "db1", Table: "t3"})
	lc3.TransitionTo(TableStateSnapshotting, &event.Position{GTID: "uuid:3"})
	lc3.SetError("fail")

	store.Save(ctx, "task-1", lc1)
	store.Save(ctx, "task-1", lc2)
	store.Save(ctx, "task-1", lc3)

	errors, err := store.ListByState(ctx, "task-1", TableStateError)
	if err != nil {
		t.Fatalf("ListByState failed: %v", err)
	}
	if len(errors) != 1 {
		t.Errorf("ListByState(error) = %d, want 1", len(errors))
	}
	if errors[0].TableID.Table != "t3" {
		t.Errorf("error table = %q, want t3", errors[0].TableID.Table)
	}
}

func TestMemoryLifecycleStoreListBySchema(t *testing.T) {
	store := NewMemoryLifecycleStore()
	ctx := context.Background()

	store.Save(ctx, "task-1", NewTableLifecycle(TableID{Database: "db1", Table: "t1"}))
	store.Save(ctx, "task-1", NewTableLifecycle(TableID{Database: "db1", Table: "t2"}))
	store.Save(ctx, "task-1", NewTableLifecycle(TableID{Database: "db2", Table: "t3"}))

	db1Tables, err := store.ListBySchema(ctx, "task-1", "db1")
	if err != nil {
		t.Fatalf("ListBySchema failed: %v", err)
	}
	if len(db1Tables) != 2 {
		t.Errorf("ListBySchema(db1) = %d, want 2", len(db1Tables))
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/source/ -run "TestMemoryLifecycleStore" -count=1 -v`
Expected: FAIL — types not defined

- [ ] **Step 3: Implement**

Create `internal/source/lifecycle_store.go`:

```go
package source

import (
	"context"
	"fmt"
	"sync"
)

type TableLifecycleStore interface {
	Save(ctx context.Context, taskID string, lc *TableLifecycle) error
	Get(ctx context.Context, taskID string, tableID TableID) (*TableLifecycle, error)
	Delete(ctx context.Context, taskID string, tableID TableID) error
	List(ctx context.Context, taskID string) ([]*TableLifecycle, error)
	ListByState(ctx context.Context, taskID string, state TableState) ([]*TableLifecycle, error)
	ListBySchema(ctx context.Context, taskID string, schema string) ([]*TableLifecycle, error)
}

type MemoryLifecycleStore struct {
	data map[string]map[TableID]*TableLifecycle // taskID -> tableID -> lifecycle
	mu   sync.RWMutex
}

func NewMemoryLifecycleStore() *MemoryLifecycleStore {
	return &MemoryLifecycleStore{
		data: make(map[string]map[TableID]*TableLifecycle),
	}
}

func (s *MemoryLifecycleStore) Save(ctx context.Context, taskID string, lc *TableLifecycle) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data[taskID] == nil {
		s.data[taskID] = make(map[TableID]*TableLifecycle)
	}
	s.data[taskID][lc.TableID] = lc
	return nil
}

func (s *MemoryLifecycleStore) Get(ctx context.Context, taskID string, tableID TableID) (*TableLifecycle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tables := s.data[taskID]
	if tables == nil {
		return nil, fmt.Errorf("table %s not found", tableID.String())
	}
	lc, ok := tables[tableID]
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableID.String())
	}
	return lc, nil
}

func (s *MemoryLifecycleStore) Delete(ctx context.Context, taskID string, tableID TableID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tables := s.data[taskID]; tables != nil {
		delete(tables, tableID)
	}
	return nil
}

func (s *MemoryLifecycleStore) List(ctx context.Context, taskID string) ([]*TableLifecycle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*TableLifecycle
	for _, lc := range s.data[taskID] {
		result = append(result, lc)
	}
	return result, nil
}

func (s *MemoryLifecycleStore) ListByState(ctx context.Context, taskID string, state TableState) ([]*TableLifecycle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*TableLifecycle
	for _, lc := range s.data[taskID] {
		if lc.GetState() == state {
			result = append(result, lc)
		}
	}
	return result, nil
}

func (s *MemoryLifecycleStore) ListBySchema(ctx context.Context, taskID string, schema string) ([]*TableLifecycle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*TableLifecycle
	for _, lc := range s.data[taskID] {
		if lc.TableID.Database == schema {
			result = append(result, lc)
		}
	}
	return result, nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/source/ -run "TestMemoryLifecycleStore" -count=1 -v`
Expected: All PASS

- [ ] **Step 5: Run all source tests**

Run: `go test ./internal/source/ -count=1`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add internal/source/lifecycle_store.go internal/source/lifecycle_store_test.go
git commit -m "feat(source): add TableLifecycleStore interface with MemoryStore implementation"
```

---

### Task 4: GlobalMinPosition calculator

**Files:**
- Create: `internal/source/global_min_position.go`
- Create: `internal/source/global_min_position_test.go`

- [ ] **Step 1: Write failing tests**

```go
package source

import (
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

func TestGlobalMinPositionAllStreaming(t *testing.T) {
	tables := []*TableLifecycle{
		{State: TableStateStreaming, StreamPosition: &event.Position{CommitTime: time.Unix(1000, 0)}},
		{State: TableStateStreaming, StreamPosition: &event.Position{CommitTime: time.Unix(500, 0)}},
		{State: TableStateStreaming, StreamPosition: &event.Position{CommitTime: time.Unix(800, 0)}},
	}
	pos := ComputeGlobalMinPosition(tables)
	if pos == nil {
		t.Fatal("expected non-nil position")
	}
	if pos.CommitTime.Unix() != 500 {
		t.Errorf("min position = %d, want 500", pos.CommitTime.Unix())
	}
}

func TestGlobalMinPositionMixedStates(t *testing.T) {
	tables := []*TableLifecycle{
		{State: TableStateStreaming, StreamPosition: &event.Position{CommitTime: time.Unix(1000, 0)}},
		{State: TableStateCatchingUp, StreamPosition: &event.Position{CommitTime: time.Unix(500, 0)}},
		{State: TableStateSnapshotting, SnapshotPosition: &event.Position{CommitTime: time.Unix(300, 0)}},
	}
	pos := ComputeGlobalMinPosition(tables)
	if pos.CommitTime.Unix() != 300 {
		t.Errorf("min position = %d, want 300 (snapshotting table's snapshot position)", pos.CommitTime.Unix())
	}
}

func TestGlobalMinPositionEmpty(t *testing.T) {
	pos := ComputeGlobalMinPosition(nil)
	if pos != nil {
		t.Error("expected nil for empty tables")
	}
}

func TestGlobalMinPositionSkipsPendingAndError(t *testing.T) {
	tables := []*TableLifecycle{
		{State: TableStatePending},
		{State: TableStateError, SnapshotPosition: &event.Position{CommitTime: time.Unix(100, 0)}},
		{State: TableStateStreaming, StreamPosition: &event.Position{CommitTime: time.Unix(500, 0)}},
	}
	pos := ComputeGlobalMinPosition(tables)
	// pending has no position, error's position should still count (it needs recovery)
	if pos.CommitTime.Unix() != 100 {
		t.Errorf("min position = %d, want 100 (error table's snapshot position still counts for recovery)", pos.CommitTime.Unix())
	}
}
```

- [ ] **Step 2: Run tests, verify fail**

- [ ] **Step 3: Implement**

Create `internal/source/global_min_position.go`:

```go
package source

import "github.com/UFOXD/datastream/pkg/event"

func ComputeGlobalMinPosition(tables []*TableLifecycle) *event.Position {
	var minPos *event.Position

	for _, lc := range tables {
		var pos *event.Position

		switch lc.State {
		case TableStatePending:
			continue
		case TableStateSnapshotting, TableStateError:
			pos = lc.SnapshotPosition
		case TableStateCatchingUp, TableStateStreaming, TableStatePaused:
			pos = lc.StreamPosition
			if pos == nil {
				pos = lc.SnapshotPosition
			}
		}

		if pos == nil {
			continue
		}

		if minPos == nil || pos.CommitTime.Before(minPos.CommitTime) {
			minPos = pos
		}
	}

	return minPos
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/source/ -run "TestGlobalMinPosition" -count=1 -v`
Expected: All PASS

- [ ] **Step 5: Run all source tests**

Run: `go test ./internal/source/ -count=1`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add internal/source/global_min_position.go internal/source/global_min_position_test.go
git commit -m "feat(source): add GlobalMinPosition calculator using CommitTime comparison"
```

---

### Task 5: Full build verification + cleanup

- [ ] **Step 1: Run full build**

Run: `go build ./...`
Expected: Clean build

- [ ] **Step 2: Run full test suite**

Run: `go test ./... -count=1`
Expected: All packages pass

- [ ] **Step 3: Verify no regressions in existing table_manager.go**

Run: `go test ./internal/source/ -count=1 -v`
Expected: All source tests pass (existing + new)

Note: The existing `TableSyncState` in `table_manager.go` is NOT removed in this sub-plan. The replacement happens in Sub-Plan 3 (Core Engine) when SnapshotScheduler takes over TableManager's lifecycle responsibilities. This sub-plan only adds the new types alongside the old ones.

---

## Summary

| Task | What | Files |
|------|------|-------|
| 1 | event.Position GTID + ResumeToken | `pkg/event/position.go` |
| 2 | TableLifecycle state machine | `internal/source/table_lifecycle.go` |
| 3 | TableLifecycleStore + MemoryStore | `internal/source/lifecycle_store.go` |
| 4 | GlobalMinPosition calculator | `internal/source/global_min_position.go` |
| 5 | Build + test verification | — |

## Next Sub-Plans

After this sub-plan is complete:

- **Sub-Plan 2: BinlogCache** — `BinlogCacheBackend` interface, `LocalBackend` (Badger), CacheEvent Protobuf, `datastream-ctl binlog decode` CLI
- **Sub-Plan 3: Core Engine** — `BinlogConsumer`, `SnapshotScheduler`, `CatchingUpReplayer`, hash-based chunk splitting, S3 snapshot path, wire into Pipeline
- **Sub-Plan 4: API & CLI** — Lifecycle API endpoints (§12), CLI commands, monitoring metrics
