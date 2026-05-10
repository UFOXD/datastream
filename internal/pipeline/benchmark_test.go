package pipeline

import (
	"context"
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
)

// BenchmarkMemoryBufferPut benchmarks writing to memory buffer
func BenchmarkMemoryBufferPut(b *testing.B) {
	buffer := NewMemoryBuffer(10000)
	ctx := context.Background()

	testEvent := &event.ChangeEvent{
		ID:   "bench-event",
		Type: event.EventTypeInsert,
		Table: event.TableInfo{
			Database: "test",
			Table:    "users",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buffer.Put(ctx, testEvent)
	}
	buffer.Close()
}

// BenchmarkMemoryBufferGet benchmarks reading from memory buffer
func BenchmarkMemoryBufferGet(b *testing.B) {
	buffer := NewMemoryBuffer(10000)
	ctx := context.Background()

	// Pre-fill buffer
	testEvent := &event.ChangeEvent{
		ID:   "bench-event",
		Type: event.EventTypeInsert,
		Table: event.TableInfo{
			Database: "test",
			Table:    "users",
		},
	}

	for i := 0; i < 1000; i++ {
		_ = buffer.Put(ctx, testEvent)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = buffer.Get(ctx, 100)
	}
	buffer.Close()
}

// BenchmarkBatchBufferPut benchmarks batch buffer write operations
func BenchmarkBatchBufferPut(b *testing.B) {
	batchBuffer := NewBatchBuffer(10000, 100, 1000)
	ctx := context.Background()

	testEvent := &event.ChangeEvent{
		ID:   "bench-event",
		Type: event.EventTypeInsert,
		Table: event.TableInfo{
			Database: "test",
			Table:    "users",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = batchBuffer.Put(ctx, testEvent)
	}
	batchBuffer.Close()
}

// BenchmarkBatchBufferGet benchmarks batch buffer get operations
func BenchmarkBatchBufferGet(b *testing.B) {
	batchBuffer := NewBatchBuffer(10000, 100, 1000)
	ctx := context.Background()

	// Pre-fill buffer
	testEvent := &event.ChangeEvent{
		ID:   "bench-event",
		Type: event.EventTypeInsert,
		Table: event.TableInfo{
			Database: "test",
			Table:    "users",
		},
	}

	for i := 0; i < 500; i++ {
		_ = batchBuffer.Put(ctx, testEvent)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = batchBuffer.Get(ctx, 100)
		// Refill for next iteration
		for j := 0; j < 100; j++ {
			_ = batchBuffer.Put(ctx, testEvent)
		}
	}
	batchBuffer.Close()
}

// BenchmarkMemoryCoordinator benchmarks memory coordinator operations
func BenchmarkMemoryCoordinator(b *testing.B) {
	coord := NewMemoryCoordinator("bench-node")
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = coord.GetPosition(ctx, "test-task")
	}
}
