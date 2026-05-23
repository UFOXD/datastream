package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBackend(t *testing.T) *LocalBackend {
	t.Helper()
	dir := t.TempDir()
	lb, err := NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { lb.Close() })
	return lb
}

func makeEvent(gtid string, seq int64, payload []byte) *CacheEvent {
	return &CacheEvent{
		Gtid:        gtid,
		EventSeq:    seq,
		Payload:     payload,
		TimestampMs: time.Now().UnixMilli(),
	}
}

func TestLocalBackendWriteAndRead(t *testing.T) {
	lb := newTestBackend(t)
	ctx := context.Background()

	events := []*CacheEvent{
		makeEvent("gtid-1", 1, []byte("row-insert-1")),
		makeEvent("gtid-1", 2, []byte("row-insert-2")),
		makeEvent("gtid-2", 1, []byte("row-update-1")),
	}

	for _, ev := range events {
		require.NoError(t, lb.Write(ctx, "db1.users", ev))
	}

	ch, err := lb.Read(ctx, "db1.users", "", 0)
	require.NoError(t, err)

	var got []*CacheEvent
	for ev := range ch {
		got = append(got, ev)
	}

	require.Len(t, got, 3)
	assert.Equal(t, "gtid-1", got[0].Gtid)
	assert.Equal(t, int64(1), got[0].EventSeq)
	assert.Equal(t, []byte("row-insert-1"), got[0].Payload)

	assert.Equal(t, "gtid-1", got[1].Gtid)
	assert.Equal(t, int64(2), got[1].EventSeq)

	assert.Equal(t, "gtid-2", got[2].Gtid)
	assert.Equal(t, int64(1), got[2].EventSeq)
	assert.Equal(t, []byte("row-update-1"), got[2].Payload)
}

func TestLocalBackendReadFromOffset(t *testing.T) {
	lb := newTestBackend(t)
	ctx := context.Background()

	for i := int64(1); i <= 5; i++ {
		require.NoError(t, lb.Write(ctx, "db1.orders", makeEvent("tx-A", i, []byte("data"))))
	}

	// Read from GTID="tx-A", EventSeq=3 => should get events with seq 3, 4, 5
	ch, err := lb.Read(ctx, "db1.orders", "tx-A", 3)
	require.NoError(t, err)

	var got []*CacheEvent
	for ev := range ch {
		got = append(got, ev)
	}

	require.Len(t, got, 3)
	assert.Equal(t, int64(3), got[0].EventSeq)
	assert.Equal(t, int64(4), got[1].EventSeq)
	assert.Equal(t, int64(5), got[2].EventSeq)
}

func TestLocalBackendDelete(t *testing.T) {
	lb := newTestBackend(t)
	ctx := context.Background()

	require.NoError(t, lb.Write(ctx, "db1.users", makeEvent("g1", 1, []byte("x"))))

	exists, err := lb.Exists(ctx, "db1.users")
	require.NoError(t, err)
	assert.True(t, exists)

	require.NoError(t, lb.Delete(ctx, "db1.users"))

	exists, err = lb.Exists(ctx, "db1.users")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestLocalBackendSize(t *testing.T) {
	lb := newTestBackend(t)
	ctx := context.Background()

	bigPayload := make([]byte, 1000)
	for i := range bigPayload {
		bigPayload[i] = byte(i % 256)
	}
	require.NoError(t, lb.Write(ctx, "db1.big", makeEvent("g1", 1, bigPayload)))

	size, err := lb.Size(ctx, "db1.big")
	require.NoError(t, err)
	assert.Greater(t, size, int64(1000))
}

func TestLocalBackendTotalSize(t *testing.T) {
	lb := newTestBackend(t)
	ctx := context.Background()

	payload := make([]byte, 500)
	require.NoError(t, lb.Write(ctx, "db1.t1", makeEvent("g1", 1, payload)))
	require.NoError(t, lb.Write(ctx, "db1.t2", makeEvent("g2", 1, payload)))

	size1, err := lb.Size(ctx, "db1.t1")
	require.NoError(t, err)

	size2, err := lb.Size(ctx, "db1.t2")
	require.NoError(t, err)

	total, err := lb.TotalSize(ctx)
	require.NoError(t, err)

	assert.Equal(t, size1+size2, total)
	assert.Greater(t, total, size1)
	assert.Greater(t, total, size2)
}

func TestLocalBackendReadCancellation(t *testing.T) {
	lb := newTestBackend(t)
	ctx := context.Background()

	// Write many events so the goroutine has work to do
	for i := int64(0); i < 100; i++ {
		require.NoError(t, lb.Write(ctx, "db1.cancel", makeEvent("g1", i, []byte("data"))))
	}

	cancelCtx, cancel := context.WithCancel(ctx)

	ch, err := lb.Read(cancelCtx, "db1.cancel", "", 0)
	require.NoError(t, err)

	// Read a few events, then cancel
	<-ch
	<-ch
	cancel()

	// Channel should eventually close (drain remaining buffered events)
	timeout := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // success: channel closed
			}
		case <-timeout:
			t.Fatal("channel was not closed after context cancellation within timeout")
		}
	}
}
