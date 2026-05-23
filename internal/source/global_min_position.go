package source

import "github.com/UFOXD/datastream/pkg/event"

// ComputeGlobalMinPosition returns the earliest effective position across all
// tables, using CommitTime for cross-database comparison.
//
// Effective position per state:
//   - pending: skipped (no position yet)
//   - snapshotting, error: SnapshotPosition
//   - catching_up, streaming, paused: StreamPosition, fallback to SnapshotPosition
//
// Returns nil if no tables have an effective position.
func ComputeGlobalMinPosition(tables []*TableLifecycle) *event.Position {
	var minPos *event.Position

	for _, tl := range tables {
		pos := effectivePosition(tl)
		if pos == nil {
			continue
		}
		if minPos == nil || pos.CommitTime.Before(minPos.CommitTime) {
			minPos = pos
		}
	}

	return minPos
}

// effectivePosition returns the position that represents a table's current
// progress, based on its lifecycle state.
func effectivePosition(tl *TableLifecycle) *event.Position {
	switch tl.State {
	case TableStatePending:
		return nil

	case TableStateSnapshotting, TableStateError:
		return tl.SnapshotPosition

	case TableStateCatchingUp, TableStateStreaming, TableStatePaused:
		if tl.StreamPosition != nil {
			return tl.StreamPosition
		}
		return tl.SnapshotPosition

	default:
		return nil
	}
}
