package source

import (
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

func TestGlobalMinPositionAllStreaming(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tables := []*TableLifecycle{
		{
			State:          TableStateStreaming,
			StreamPosition: &event.Position{CommitTime: now.Add(300 * time.Second)},
		},
		{
			State:          TableStateStreaming,
			StreamPosition: &event.Position{CommitTime: now.Add(100 * time.Second)},
		},
		{
			State:          TableStateStreaming,
			StreamPosition: &event.Position{CommitTime: now.Add(200 * time.Second)},
		},
	}

	got := ComputeGlobalMinPosition(tables)
	if got == nil {
		t.Fatal("expected non-nil position, got nil")
	}
	want := now.Add(100 * time.Second)
	if !got.CommitTime.Equal(want) {
		t.Errorf("CommitTime = %v, want %v", got.CommitTime, want)
	}
}

func TestGlobalMinPositionMixedStates(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tables := []*TableLifecycle{
		{
			State:          TableStateStreaming,
			StreamPosition: &event.Position{CommitTime: now.Add(1000 * time.Second)},
		},
		{
			State:          TableStateCatchingUp,
			StreamPosition: &event.Position{CommitTime: now.Add(500 * time.Second)},
		},
		{
			State:            TableStateSnapshotting,
			SnapshotPosition: &event.Position{CommitTime: now.Add(300 * time.Second)},
		},
	}

	got := ComputeGlobalMinPosition(tables)
	if got == nil {
		t.Fatal("expected non-nil position, got nil")
	}
	want := now.Add(300 * time.Second)
	if !got.CommitTime.Equal(want) {
		t.Errorf("CommitTime = %v, want %v", got.CommitTime, want)
	}
}

func TestGlobalMinPositionEmpty(t *testing.T) {
	got := ComputeGlobalMinPosition(nil)
	if got != nil {
		t.Errorf("expected nil for nil input, got %v", got)
	}

	got = ComputeGlobalMinPosition([]*TableLifecycle{})
	if got != nil {
		t.Errorf("expected nil for empty input, got %v", got)
	}
}

func TestGlobalMinPositionSkipsPendingButIncludesError(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tables := []*TableLifecycle{
		{
			State: TableStatePending,
			// pending has no position
		},
		{
			State:            TableStateError,
			SnapshotPosition: &event.Position{CommitTime: now.Add(100 * time.Second)},
		},
		{
			State:          TableStateStreaming,
			StreamPosition: &event.Position{CommitTime: now.Add(500 * time.Second)},
		},
	}

	got := ComputeGlobalMinPosition(tables)
	if got == nil {
		t.Fatal("expected non-nil position, got nil")
	}
	want := now.Add(100 * time.Second)
	if !got.CommitTime.Equal(want) {
		t.Errorf("CommitTime = %v, want %v", got.CommitTime, want)
	}
}
