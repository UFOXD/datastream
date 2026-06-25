package lifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/UFOXD/datastream/internal/cache"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
)

func newTestScheduler(t *testing.T) *SnapshotScheduler {
	t.Helper()
	store := source.NewMemoryLifecycleStore()
	dir := t.TempDir()
	cacheBackend, err := cache.NewLocalBackend(dir, cache.SyncModeNone)
	require.NoError(t, err)
	t.Cleanup(func() { cacheBackend.Close() })
	return NewSnapshotScheduler(DefaultSchedulerConfig(), "task-1", store, cacheBackend)
}

func TestSchedulerAddTable(t *testing.T) {
	s := newTestScheduler(t)
	ctx := context.Background()

	err := s.AddTable(source.TableID{Database: "db1", Table: "users"}, &event.Position{TxID: "uuid:100"})
	require.NoError(t, err)

	lc, err := s.store.Get(ctx, s.taskID, source.TableID{Database: "db1", Table: "users"})
	require.NoError(t, err)
	assert.Equal(t, source.TableStatePending, lc.GetState())
	assert.Equal(t, "uuid:100", lc.SnapshotPosition.TxID)
}

func TestSchedulerFullLifecycle(t *testing.T) {
	s := newTestScheduler(t)
	ctx := context.Background()
	tid := source.TableID{Database: "db1", Table: "users"}

	err := s.AddTable(tid, &event.Position{TxID: "uuid:100"})
	require.NoError(t, err)

	// Start snapshot (manual transition for test)
	lc, err := s.store.Get(ctx, s.taskID, tid)
	require.NoError(t, err)
	err = lc.TransitionTo(source.TableStateSnapshotting, &event.Position{TxID: "uuid:100"})
	require.NoError(t, err)
	err = s.store.Save(ctx, s.taskID, lc)
	require.NoError(t, err)

	// Complete snapshot → catching_up
	err = s.OnSnapshotComplete(tid)
	require.NoError(t, err)
	lc, err = s.store.Get(ctx, s.taskID, tid)
	require.NoError(t, err)
	assert.Equal(t, source.TableStateCatchingUp, lc.GetState())

	// Caught up → streaming
	err = s.OnCaughtUp(tid)
	require.NoError(t, err)
	lc, err = s.store.Get(ctx, s.taskID, tid)
	require.NoError(t, err)
	assert.Equal(t, source.TableStateStreaming, lc.GetState())
}

func TestSchedulerSnapshotError(t *testing.T) {
	s := newTestScheduler(t)
	ctx := context.Background()
	tid := source.TableID{Database: "db1", Table: "users"}

	err := s.AddTable(tid, &event.Position{TxID: "uuid:100"})
	require.NoError(t, err)

	lc, err := s.store.Get(ctx, s.taskID, tid)
	require.NoError(t, err)
	err = lc.TransitionTo(source.TableStateSnapshotting, &event.Position{TxID: "uuid:100"})
	require.NoError(t, err)
	err = s.store.Save(ctx, s.taskID, lc)
	require.NoError(t, err)

	err = s.OnSnapshotError(tid, "connection timeout")
	require.NoError(t, err)

	lc, err = s.store.Get(ctx, s.taskID, tid)
	require.NoError(t, err)
	assert.Equal(t, source.TableStateError, lc.GetState())
	assert.Equal(t, "connection timeout", lc.LastError)
}

func TestSchedulerRestartTable(t *testing.T) {
	s := newTestScheduler(t)
	ctx := context.Background()
	tid := source.TableID{Database: "db1", Table: "users"}

	err := s.AddTable(tid, &event.Position{TxID: "uuid:100"})
	require.NoError(t, err)

	lc, err := s.store.Get(ctx, s.taskID, tid)
	require.NoError(t, err)
	err = lc.TransitionTo(source.TableStateSnapshotting, &event.Position{TxID: "uuid:100"})
	require.NoError(t, err)
	lc.SetError("fail")
	err = s.store.Save(ctx, s.taskID, lc)
	require.NoError(t, err)

	err = s.RestartTable(tid, &event.Position{TxID: "uuid:200"}, false)
	require.NoError(t, err)

	lc, err = s.store.Get(ctx, s.taskID, tid)
	require.NoError(t, err)
	assert.Equal(t, source.TableStatePending, lc.GetState())
	assert.Equal(t, "uuid:200", lc.SnapshotPosition.TxID)
}

func TestSchedulerRestartStreamingRequiresForce(t *testing.T) {
	s := newTestScheduler(t)
	ctx := context.Background()
	tid := source.TableID{Database: "db1", Table: "users"}

	err := s.AddTable(tid, &event.Position{TxID: "uuid:100"})
	require.NoError(t, err)

	lc, err := s.store.Get(ctx, s.taskID, tid)
	require.NoError(t, err)
	err = lc.TransitionTo(source.TableStateSnapshotting, &event.Position{TxID: "uuid:100"})
	require.NoError(t, err)
	err = lc.TransitionTo(source.TableStateCatchingUp, nil)
	require.NoError(t, err)
	err = lc.TransitionTo(source.TableStateStreaming, nil)
	require.NoError(t, err)
	err = s.store.Save(ctx, s.taskID, lc)
	require.NoError(t, err)

	// Without force → error
	err = s.RestartTable(tid, &event.Position{TxID: "uuid:200"}, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "streaming")
	assert.Contains(t, err.Error(), "force=true")

	// With force → ok
	err = s.RestartTable(tid, &event.Position{TxID: "uuid:200"}, true)
	assert.NoError(t, err)

	lc, err = s.store.Get(ctx, s.taskID, tid)
	require.NoError(t, err)
	assert.Equal(t, source.TableStatePending, lc.GetState())
	assert.Equal(t, "uuid:200", lc.SnapshotPosition.TxID)
}

func TestSchedulerRestartSchema(t *testing.T) {
	s := newTestScheduler(t)
	ctx := context.Background()

	err := s.AddTable(source.TableID{Database: "db1", Table: "t1"}, &event.Position{TxID: "uuid:100"})
	require.NoError(t, err)
	err = s.AddTable(source.TableID{Database: "db1", Table: "t2"}, &event.Position{TxID: "uuid:100"})
	require.NoError(t, err)
	err = s.AddTable(source.TableID{Database: "db2", Table: "t3"}, &event.Position{TxID: "uuid:100"})
	require.NoError(t, err)

	// Put db1 tables in error state
	for _, tbl := range []string{"t1", "t2"} {
		tid := source.TableID{Database: "db1", Table: tbl}
		lc, err := s.store.Get(ctx, s.taskID, tid)
		require.NoError(t, err)
		err = lc.TransitionTo(source.TableStateSnapshotting, &event.Position{TxID: "uuid:100"})
		require.NoError(t, err)
		lc.SetError("fail")
		err = s.store.Save(ctx, s.taskID, lc)
		require.NoError(t, err)
	}

	restarted, err := s.RestartSchema("db1", &event.Position{TxID: "uuid:300"}, false)
	require.NoError(t, err)
	assert.Len(t, restarted, 2)

	// Verify all db1 tables are pending with new position
	for _, tbl := range []string{"t1", "t2"} {
		tid := source.TableID{Database: "db1", Table: tbl}
		lc, err := s.store.Get(ctx, s.taskID, tid)
		require.NoError(t, err)
		assert.Equal(t, source.TableStatePending, lc.GetState())
		assert.Equal(t, "uuid:300", lc.SnapshotPosition.TxID)
	}

	// db2 table should be unaffected
	lc, err := s.store.Get(ctx, s.taskID, source.TableID{Database: "db2", Table: "t3"})
	require.NoError(t, err)
	assert.Equal(t, source.TableStatePending, lc.GetState())
	assert.Equal(t, "uuid:100", lc.SnapshotPosition.TxID)
}

func TestSchedulerGlobalMinPosition(t *testing.T) {
	s := newTestScheduler(t)
	ctx := context.Background()

	tid1 := source.TableID{Database: "db1", Table: "t1"}
	tid2 := source.TableID{Database: "db1", Table: "t2"}

	err := s.AddTable(tid1, &event.Position{TxID: "uuid:100", CommitTime: time.Unix(100, 0)})
	require.NoError(t, err)
	err = s.AddTable(tid2, &event.Position{TxID: "uuid:200", CommitTime: time.Unix(200, 0)})
	require.NoError(t, err)

	lc1, err := s.store.Get(ctx, s.taskID, tid1)
	require.NoError(t, err)
	err = lc1.TransitionTo(source.TableStateSnapshotting, &event.Position{TxID: "uuid:100", CommitTime: time.Unix(100, 0)})
	require.NoError(t, err)
	err = s.store.Save(ctx, s.taskID, lc1)
	require.NoError(t, err)

	lc2, err := s.store.Get(ctx, s.taskID, tid2)
	require.NoError(t, err)
	err = lc2.TransitionTo(source.TableStateSnapshotting, &event.Position{TxID: "uuid:200", CommitTime: time.Unix(200, 0)})
	require.NoError(t, err)
	err = lc2.TransitionTo(source.TableStateCatchingUp, nil)
	require.NoError(t, err)
	lc2.UpdateStreamPosition(&event.Position{TxID: "uuid:500", CommitTime: time.Unix(500, 0)})
	err = s.store.Save(ctx, s.taskID, lc2)
	require.NoError(t, err)

	pos := s.GetGlobalMinPosition()
	require.NotNil(t, pos)
	assert.Equal(t, int64(100), pos.CommitTime.Unix()) // t1's snapshot position is earliest
}

func TestSchedulerListErrors(t *testing.T) {
	s := newTestScheduler(t)
	ctx := context.Background()

	tid1 := source.TableID{Database: "db1", Table: "t1"}
	tid2 := source.TableID{Database: "db1", Table: "t2"}

	err := s.AddTable(tid1, &event.Position{TxID: "uuid:100"})
	require.NoError(t, err)
	err = s.AddTable(tid2, &event.Position{TxID: "uuid:200"})
	require.NoError(t, err)

	// Put t1 in error state
	lc1, err := s.store.Get(ctx, s.taskID, tid1)
	require.NoError(t, err)
	lc1.SetError("some error")
	err = s.store.Save(ctx, s.taskID, lc1)
	require.NoError(t, err)

	errors, err := s.ListErrors()
	require.NoError(t, err)
	assert.Len(t, errors, 1)
	assert.Equal(t, tid1, errors[0].TableID)
}

func TestSchedulerPauseResume(t *testing.T) {
	s := newTestScheduler(t)
	ctx := context.Background()
	tid := source.TableID{Database: "db1", Table: "users"}

	err := s.AddTable(tid, &event.Position{TxID: "uuid:100"})
	require.NoError(t, err)

	// Transition to streaming
	lc, err := s.store.Get(ctx, s.taskID, tid)
	require.NoError(t, err)
	err = lc.TransitionTo(source.TableStateSnapshotting, &event.Position{TxID: "uuid:100"})
	require.NoError(t, err)
	err = lc.TransitionTo(source.TableStateCatchingUp, nil)
	require.NoError(t, err)
	err = lc.TransitionTo(source.TableStateStreaming, nil)
	require.NoError(t, err)
	err = s.store.Save(ctx, s.taskID, lc)
	require.NoError(t, err)

	// Pause
	err = s.PauseTable(tid)
	require.NoError(t, err)
	lc, err = s.store.Get(ctx, s.taskID, tid)
	require.NoError(t, err)
	assert.Equal(t, source.TableStatePaused, lc.GetState())

	// Resume
	err = s.ResumeTable(tid)
	require.NoError(t, err)
	lc, err = s.store.Get(ctx, s.taskID, tid)
	require.NoError(t, err)
	assert.Equal(t, source.TableStateStreaming, lc.GetState())
}
