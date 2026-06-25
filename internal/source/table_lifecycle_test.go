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
			t.Errorf("TableState constant must not be empty")
		}
	}
}

func TestNewTableLifecycle(t *testing.T) {
	tid := TableID{Database: "mydb", Table: "users"}
	lc := NewTableLifecycle(tid)

	if lc.GetState() != TableStatePending {
		t.Errorf("initial state = %q, want %q", lc.GetState(), TableStatePending)
	}
	if lc.RetryCount != 0 {
		t.Errorf("initial RetryCount = %d, want 0", lc.RetryCount)
	}
	if lc.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", lc.MaxRetries)
	}
	if lc.TableID != tid {
		t.Errorf("TableID = %v, want %v", lc.TableID, tid)
	}
}

func TestTransitionPendingToSnapshotting(t *testing.T) {
	tid := TableID{Database: "mydb", Table: "users"}
	lc := NewTableLifecycle(tid)

	pos := &event.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  154,
		TxID:       "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
		CommitTime: time.Now(),
	}

	if err := lc.TransitionTo(TableStateSnapshotting, pos); err != nil {
		t.Fatalf("TransitionTo(snapshotting) failed: %v", err)
	}
	if lc.GetState() != TableStateSnapshotting {
		t.Errorf("state = %q, want %q", lc.GetState(), TableStateSnapshotting)
	}
	if lc.SnapshotPosition == nil {
		t.Fatal("SnapshotPosition should be set when entering snapshotting")
	}
	if lc.SnapshotPosition.TxID != pos.TxID {
		t.Errorf("SnapshotPosition.TxID = %q, want %q", lc.SnapshotPosition.TxID, pos.TxID)
	}
}

func TestTransitionSnapshotToCatchingUp(t *testing.T) {
	tid := TableID{Database: "mydb", Table: "orders"}
	lc := NewTableLifecycle(tid)

	snapPos := &event.Position{TxID: "gtid-1", CommitTime: time.Now()}
	if err := lc.TransitionTo(TableStateSnapshotting, snapPos); err != nil {
		t.Fatalf("pending→snapshotting failed: %v", err)
	}

	catchPos := &event.Position{TxID: "gtid-2", CommitTime: time.Now()}
	if err := lc.TransitionTo(TableStateCatchingUp, catchPos); err != nil {
		t.Fatalf("snapshotting→catching_up failed: %v", err)
	}
	if lc.GetState() != TableStateCatchingUp {
		t.Errorf("state = %q, want %q", lc.GetState(), TableStateCatchingUp)
	}
}

func TestTransitionCatchingUpToStreaming(t *testing.T) {
	tid := TableID{Database: "mydb", Table: "orders"}
	lc := NewTableLifecycle(tid)

	// pending → snapshotting → catching_up → streaming
	if err := lc.TransitionTo(TableStateSnapshotting, &event.Position{TxID: "g1"}); err != nil {
		t.Fatalf("pending→snapshotting failed: %v", err)
	}
	if err := lc.TransitionTo(TableStateCatchingUp, &event.Position{TxID: "g2"}); err != nil {
		t.Fatalf("snapshotting→catching_up failed: %v", err)
	}
	if err := lc.TransitionTo(TableStateStreaming, &event.Position{TxID: "g3"}); err != nil {
		t.Fatalf("catching_up→streaming failed: %v", err)
	}
	if lc.GetState() != TableStateStreaming {
		t.Errorf("state = %q, want %q", lc.GetState(), TableStateStreaming)
	}
}

func TestInvalidTransition(t *testing.T) {
	tid := TableID{Database: "mydb", Table: "users"}
	lc := NewTableLifecycle(tid)

	err := lc.TransitionTo(TableStateStreaming, nil)
	if err == nil {
		t.Fatal("pending→streaming should return an error, got nil")
	}
	// State must remain pending.
	if lc.GetState() != TableStatePending {
		t.Errorf("state after invalid transition = %q, want %q", lc.GetState(), TableStatePending)
	}
}

func TestTransitionToError(t *testing.T) {
	tid := TableID{Database: "mydb", Table: "orders"}
	lc := NewTableLifecycle(tid)

	// Move to snapshotting first.
	if err := lc.TransitionTo(TableStateSnapshotting, &event.Position{TxID: "g1"}); err != nil {
		t.Fatalf("pending→snapshotting failed: %v", err)
	}

	lc.SetError("disk full")

	if lc.GetState() != TableStateError {
		t.Errorf("state = %q, want %q", lc.GetState(), TableStateError)
	}
	if lc.PreviousState != TableStateSnapshotting {
		t.Errorf("PreviousState = %q, want %q", lc.PreviousState, TableStateSnapshotting)
	}
	if lc.LastError != "disk full" {
		t.Errorf("LastError = %q, want %q", lc.LastError, "disk full")
	}
}

func TestTransitionErrorToPending(t *testing.T) {
	tid := TableID{Database: "mydb", Table: "orders"}
	lc := NewTableLifecycle(tid)

	// pending → snapshotting → error → pending (via ResetToPending)
	if err := lc.TransitionTo(TableStateSnapshotting, &event.Position{TxID: "g1"}); err != nil {
		t.Fatalf("pending→snapshotting failed: %v", err)
	}
	lc.SetError("connection lost")

	newPos := &event.Position{TxID: "g-new", CommitTime: time.Now()}
	if err := lc.ResetToPending(newPos); err != nil {
		t.Fatalf("ResetToPending failed: %v", err)
	}

	if lc.GetState() != TableStatePending {
		t.Errorf("state = %q, want %q", lc.GetState(), TableStatePending)
	}
	if lc.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", lc.RetryCount)
	}
	if lc.SnapshotPosition == nil || lc.SnapshotPosition.TxID != "g-new" {
		t.Errorf("SnapshotPosition not updated to new position")
	}
	if lc.LastError != "" {
		t.Errorf("LastError = %q, want empty after ResetToPending", lc.LastError)
	}
}

func TestPauseOnlyAllowedInCatchingUpAndStreaming(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(*TableLifecycle) // drive lifecycle to target state
		wantErr   bool
	}{
		{
			name:    "pending",
			setup:   func(lc *TableLifecycle) { /* already pending */ },
			wantErr: true,
		},
		{
			name: "snapshotting",
			setup: func(lc *TableLifecycle) {
				_ = lc.TransitionTo(TableStateSnapshotting, &event.Position{TxID: "g1"})
			},
			wantErr: true,
		},
		{
			name: "catching_up",
			setup: func(lc *TableLifecycle) {
				_ = lc.TransitionTo(TableStateSnapshotting, &event.Position{TxID: "g1"})
				_ = lc.TransitionTo(TableStateCatchingUp, &event.Position{TxID: "g2"})
			},
			wantErr: false,
		},
		{
			name: "streaming",
			setup: func(lc *TableLifecycle) {
				_ = lc.TransitionTo(TableStateSnapshotting, &event.Position{TxID: "g1"})
				_ = lc.TransitionTo(TableStateCatchingUp, &event.Position{TxID: "g2"})
				_ = lc.TransitionTo(TableStateStreaming, &event.Position{TxID: "g3"})
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lc := NewTableLifecycle(TableID{Database: "db", Table: "t"})
			tt.setup(lc)

			err := lc.Pause()
			if (err != nil) != tt.wantErr {
				t.Errorf("Pause() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && lc.GetState() != TableStatePaused {
				t.Errorf("state after Pause() = %q, want %q", lc.GetState(), TableStatePaused)
			}
		})
	}
}

func TestResumeRestoresPreviousState(t *testing.T) {
	tid := TableID{Database: "mydb", Table: "orders"}
	lc := NewTableLifecycle(tid)

	// Drive to streaming, then pause, then resume.
	_ = lc.TransitionTo(TableStateSnapshotting, &event.Position{TxID: "g1"})
	_ = lc.TransitionTo(TableStateCatchingUp, &event.Position{TxID: "g2"})
	_ = lc.TransitionTo(TableStateStreaming, &event.Position{TxID: "g3"})

	if err := lc.Pause(); err != nil {
		t.Fatalf("Pause() failed: %v", err)
	}
	if lc.GetState() != TableStatePaused {
		t.Fatalf("state after Pause = %q, want %q", lc.GetState(), TableStatePaused)
	}

	if err := lc.Resume(); err != nil {
		t.Fatalf("Resume() failed: %v", err)
	}
	if lc.GetState() != TableStateStreaming {
		t.Errorf("state after Resume = %q, want %q (restored)", lc.GetState(), TableStateStreaming)
	}

	// Also test Resume from paused catching_up.
	lc2 := NewTableLifecycle(tid)
	_ = lc2.TransitionTo(TableStateSnapshotting, &event.Position{TxID: "g1"})
	_ = lc2.TransitionTo(TableStateCatchingUp, &event.Position{TxID: "g2"})
	_ = lc2.Pause()
	_ = lc2.Resume()
	if lc2.GetState() != TableStateCatchingUp {
		t.Errorf("state after Resume from paused catching_up = %q, want %q", lc2.GetState(), TableStateCatchingUp)
	}
}
