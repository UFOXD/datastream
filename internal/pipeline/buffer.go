package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

// Buffer is the interface for event buffering.
type Buffer interface {
	// Put adds an event to the buffer.
	Put(ctx context.Context, e *event.ChangeEvent) error

	// Get retrieves events from the buffer.
	Get(ctx context.Context, batchSize int) ([]*event.ChangeEvent, error)

	// Flush flushes the buffer.
	Flush(ctx context.Context) error

	// Close closes the buffer.
	Close() error

	// Len returns the current buffer length.
	Len() int

	// Cap returns the buffer capacity.
	Cap() int
}

// MemoryBuffer is an in-memory event buffer.
type MemoryBuffer struct {
	events   chan *event.ChangeEvent
	capacity int
	mu       sync.RWMutex
	closed   bool
}

// NewMemoryBuffer creates a new in-memory buffer.
func NewMemoryBuffer(capacity int) *MemoryBuffer {
	return &MemoryBuffer{
		events:   make(chan *event.ChangeEvent, capacity),
		capacity: capacity,
	}
}

// Put adds an event to the buffer.
func (b *MemoryBuffer) Put(ctx context.Context, e *event.ChangeEvent) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return ErrBufferFull
	}

	select {
	case b.events <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrBufferFull
	}
}

// Get retrieves events from the buffer.
func (b *MemoryBuffer) Get(ctx context.Context, batchSize int) ([]*event.ChangeEvent, error) {
	events := make([]*event.ChangeEvent, 0, batchSize)

	for i := 0; i < batchSize; i++ {
		select {
		case e, ok := <-b.events:
			if !ok {
				return events, nil
			}
			events = append(events, e)
		case <-ctx.Done():
			return events, ctx.Err()
		default:
			return events, nil
		}
	}

	return events, nil
}

// Flush flushes the buffer (no-op for memory buffer).
func (b *MemoryBuffer) Flush(ctx context.Context) error {
	return nil
}

// Close closes the buffer.
func (b *MemoryBuffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}

	b.closed = true
	close(b.events)
	return nil
}

// Len returns the current buffer length.
func (b *MemoryBuffer) Len() int {
	return len(b.events)
}

// Cap returns the buffer capacity.
func (b *MemoryBuffer) Cap() int {
	return b.capacity
}

// BatchBuffer collects events into batches before sending.
type BatchBuffer struct {
	buffer    *MemoryBuffer
	batchSize int
	timeout   time.Duration
	pending   []*event.ChangeEvent
	mu        sync.Mutex
}

// NewBatchBuffer creates a new batch buffer.
func NewBatchBuffer(capacity, batchSize, timeoutMs int) *BatchBuffer {
	return &BatchBuffer{
		buffer:    NewMemoryBuffer(capacity),
		batchSize: batchSize,
		timeout:   time.Duration(timeoutMs) * time.Millisecond,
		pending:   make([]*event.ChangeEvent, 0, batchSize),
	}
}

// Put adds an event to the batch buffer.
func (b *BatchBuffer) Put(ctx context.Context, e *event.ChangeEvent) error {
	return b.buffer.Put(ctx, e)
}

// Get retrieves a batch of events.
func (b *BatchBuffer) Get(ctx context.Context, batchSize int) ([]*event.ChangeEvent, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Use configured batch size
	if batchSize <= 0 {
		batchSize = b.batchSize
	}

	// Try to get a full batch
	events := make([]*event.ChangeEvent, 0, batchSize)
	timeout := time.NewTimer(b.timeout)
	defer timeout.Stop()

	for len(events) < batchSize {
		select {
		case e, ok := <-b.buffer.events:
			if !ok {
				return events, nil
			}
			events = append(events, e)
		case <-timeout.C:
			// Return partial batch on timeout
			if len(events) > 0 {
				return events, nil
			}
			// Reset timer and wait for first event
			timeout.Reset(b.timeout)
		case <-ctx.Done():
			return events, ctx.Err()
		}
	}

	return events, nil
}

// Flush flushes the batch buffer.
func (b *BatchBuffer) Flush(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Drain the buffer
	for {
		select {
		case e, ok := <-b.buffer.events:
			if !ok {
				return nil
			}
			b.pending = append(b.pending, e)
		default:
			return nil
		}
	}
}

// Close closes the batch buffer.
func (b *BatchBuffer) Close() error {
	return b.buffer.Close()
}

// Len returns the current buffer length.
func (b *BatchBuffer) Len() int {
	return b.buffer.Len()
}

// Cap returns the buffer capacity.
func (b *BatchBuffer) Cap() int {
	return b.buffer.Cap()
}

// NewBuffer creates a buffer based on configuration.
func NewBuffer(config BufferConfig) Buffer {
	if config.BatchSize > 0 {
		return NewBatchBuffer(config.Size, config.BatchSize, config.FlushTimeout)
	}
	return NewMemoryBuffer(config.Size)
}

