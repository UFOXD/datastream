package pipeline

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/stretchr/testify/require"
)

func TestPersistentBuffer_PutAndGet(t *testing.T) {
	// Create temp directory for database
	tmpDir, err := os.MkdirTemp("", "persistent_buffer_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := &PersistentBufferConfig{
		Capacity:      100,
		Path:          tmpDir,
		SyncWrites:    true,
		FlushInterval: 0,
	}

	pb, err := NewPersistentBuffer(config)
	require.NoError(t, err)
	defer pb.Close()

	// Put events
	for i := 0; i < 5; i++ {
		e := &event.ChangeEvent{
			Table: event.TableInfo{
				Database: "test_db",
				Table:    "test_table",
			},
			ID:   string(rune('A' + i)),
			Type: event.EventTypeInsert,
		}
		err := pb.Put(context.Background(), e)
		require.NoError(t, err)
	}

	// Get events
	events, err := pb.Get(context.Background(), 3)
	require.NoError(t, err)
	require.Len(t, events, 3)

	// Check stats
	stats, err := pb.Stats()
	require.NoError(t, err)
	require.Equal(t, 5, stats.PersistedCount)
	require.Equal(t, 2, stats.InMemoryCount) // 5 put, 3 got
}

func TestPersistentBuffer_Replay(t *testing.T) {
	// Create temp directory for database
	tmpDir, err := os.MkdirTemp("", "persistent_buffer_replay_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := &PersistentBufferConfig{
		Capacity:      100,
		Path:          tmpDir,
		SyncWrites:    true,
	}

	// Create buffer and add events
	pb, err := NewPersistentBuffer(config)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		e := &event.ChangeEvent{
			Table: event.TableInfo{
				Database: "test_db",
				Table:    "test_table",
			},
			ID:   string(rune('X' + i)),
			Type: event.EventTypeInsert,
		}
		err := pb.Put(context.Background(), e)
		require.NoError(t, err)
	}

	// Close the buffer
	require.NoError(t, pb.Close())

	// Reopen and replay
	pb2, err := NewPersistentBuffer(config)
	require.NoError(t, err)
	defer pb2.Close()

	count, err := pb2.Replay(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, count)

	// Verify events are available
	events, err := pb2.Get(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, events, 3)
}

func TestPersistentBuffer_Clear(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "persistent_buffer_clear_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := &PersistentBufferConfig{
		Capacity:   100,
		Path:       tmpDir,
		SyncWrites: true,
	}

	pb, err := NewPersistentBuffer(config)
	require.NoError(t, err)
	defer pb.Close()

	// Add events
	for i := 0; i < 5; i++ {
		e := &event.ChangeEvent{
			Table: event.TableInfo{
				Database: "test_db",
				Table:    "test_table",
			},
			ID:   string(rune('A' + i)),
			Type: event.EventTypeInsert,
		}
		err := pb.Put(context.Background(), e)
		require.NoError(t, err)
	}

	// Clear
	require.NoError(t, pb.Clear())

	// Check stats
	stats, err := pb.Stats()
	require.NoError(t, err)
	require.Equal(t, 0, stats.PersistedCount)
}

func TestPersistentBuffer_ContextCancellation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "persistent_buffer_cancel_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := &PersistentBufferConfig{
		Capacity:   10,
		Path:       tmpDir,
		SyncWrites: true,
	}

	pb, err := NewPersistentBuffer(config)
	require.NoError(t, err)
	defer pb.Close()

	// Fill the buffer
	for i := 0; i < 10; i++ {
		e := &event.ChangeEvent{
			Table: event.TableInfo{
				Database: "test_db",
				Table:    "test_table",
			},
			ID:   string(rune('A' + i)),
			Type: event.EventTypeInsert,
		}
		err := pb.Put(context.Background(), e)
		require.NoError(t, err)
	}

	// Try to put with cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	e := &event.ChangeEvent{
		Table: event.TableInfo{
			Database: "test_db",
			Table:    "test_table",
		},
		ID:   "Z",
		Type: event.EventTypeInsert,
	}
	err = pb.Put(ctx, e)
	require.Error(t, err) // Should fail due to context cancellation or buffer full
}

func TestPersistentBuffer_FlushInterval(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "persistent_buffer_flush_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := &PersistentBufferConfig{
		Capacity:      100,
		Path:          tmpDir,
		SyncWrites:    false, // Async writes to test flush
		FlushInterval: 100 * time.Millisecond,
	}

	pb, err := NewPersistentBuffer(config)
	require.NoError(t, err)

	// Add events
	for i := 0; i < 5; i++ {
		e := &event.ChangeEvent{
			Table: event.TableInfo{
				Database: "test_db",
				Table:    "test_table",
			},
			ID:   string(rune('A' + i)),
			Type: event.EventTypeInsert,
		}
		err := pb.Put(context.Background(), e)
		require.NoError(t, err)
	}

	// Wait for flush
	time.Sleep(150 * time.Millisecond)

	// Close and verify
	require.NoError(t, pb.Close())
}
