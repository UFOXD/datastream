package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/dgraph-io/badger/v4"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// PersistentBuffer is a buffer with persistence support using Badger KV store.
// It persists events to disk to survive restarts and provide durability.
type PersistentBuffer struct {
	*MemoryBuffer // Embedded for fast in-memory access

	db       *badger.DB
	path     string
	mu       sync.RWMutex
	closed   bool
	seq      uint64 // Sequence number for ordering
	seqMu    sync.Mutex
	flushCh  chan struct{}
	stopCh   chan struct{}
}

// PersistentBufferConfig holds configuration for the persistent buffer.
type PersistentBufferConfig struct {
	// Capacity is the in-memory buffer capacity.
	Capacity int

	// Path is the directory for the Badger database.
	Path string

	// SyncWrites enables synchronous writes for durability.
	SyncWrites bool

	// FlushInterval is the interval for periodic flushing.
	FlushInterval time.Duration
}

// NewPersistentBuffer creates a new persistent buffer with Badger backend.
func NewPersistentBuffer(config *PersistentBufferConfig) (*PersistentBuffer, error) {
	if config.Capacity <= 0 {
		config.Capacity = 10000
	}

	// Open Badger database
	opts := badger.DefaultOptions(config.Path).
		WithSyncWrites(config.SyncWrites).
		WithLoggingLevel(badger.ERROR)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger database: %w", err)
	}

	pb := &PersistentBuffer{
		MemoryBuffer: NewMemoryBuffer(config.Capacity),
		db:           db,
		path:         config.Path,
		flushCh:      make(chan struct{}, 1),
		stopCh:       make(chan struct{}),
	}

	// Start background flush goroutine if interval is set
	if config.FlushInterval > 0 {
		go pb.flushLoop(config.FlushInterval)
	}

	log.Info("created persistent buffer",
		zap.String("path", config.Path),
		zap.Int("capacity", config.Capacity))

	return pb, nil
}

// Put adds an event to the buffer and persists it.
func (b *PersistentBuffer) Put(ctx context.Context, e *event.ChangeEvent) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return ErrBufferFull
	}

	// Generate sequence number
	seq := b.nextSeq()

	// Create key with sequence for ordering
	key := b.makeKey(seq, e)

	// Serialize event
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Write to Badger
	err = b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
	if err != nil {
		return fmt.Errorf("failed to persist event: %w", err)
	}

	// Also add to in-memory buffer for fast access
	return b.MemoryBuffer.Put(ctx, e)
}

// Get retrieves events from the buffer.
func (b *PersistentBuffer) Get(ctx context.Context, batchSize int) ([]*event.ChangeEvent, error) {
	return b.MemoryBuffer.Get(ctx, batchSize)
}

// Flush persists any pending in-memory events to disk.
func (b *PersistentBuffer) Flush(ctx context.Context) error {
	// Badger handles its own buffering, but we can force a sync
	return b.db.Sync()
}

// Close closes the persistent buffer and persists any pending events.
func (b *PersistentBuffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}
	b.closed = true

	// Stop background flush goroutine
	close(b.stopCh)

	// Final sync
	if err := b.db.Sync(); err != nil {
		log.Error("failed to sync database on close", zap.Error(err))
	}

	// Close Badger database
	if err := b.db.Close(); err != nil {
		log.Error("failed to close database", zap.Error(err))
	}

	log.Info("closed persistent buffer", zap.String("path", b.path))

	return b.MemoryBuffer.Close()
}

// Replay reads persisted events and replays them to the buffer.
// This is useful for recovery after a restart.
func (b *PersistentBuffer) Replay(ctx context.Context) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return 0, ErrBufferClosed
	}

	count := 0

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			item := it.Item()
			err := item.Value(func(val []byte) error {
				var e event.ChangeEvent
				if err := json.Unmarshal(val, &e); err != nil {
					log.Warn("failed to unmarshal event during replay",
						zap.Error(err),
						zap.Binary("key", item.Key()))
					return nil // Skip malformed events
				}

				// Add to in-memory buffer
				if err := b.MemoryBuffer.Put(ctx, &e); err != nil {
					return err
				}
				count++
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return count, fmt.Errorf("replay failed: %w", err)
	}

	log.Info("replayed events from persistent buffer",
		zap.Int("count", count),
		zap.String("path", b.path))

	return count, nil
}

// Clear removes all persisted events.
func (b *PersistentBuffer) Clear() error {
	return b.db.DropAll()
}

// Stats returns statistics about the persistent buffer.
func (b *PersistentBuffer) Stats() (*PersistentBufferStats, error) {
	stats := &PersistentBufferStats{}

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Only count keys

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			stats.PersistedCount++
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	stats.InMemoryCount = b.MemoryBuffer.Len()
	stats.Capacity = b.MemoryBuffer.Cap()

	return stats, nil
}

// nextSeq generates the next sequence number.
func (b *PersistentBuffer) nextSeq() uint64 {
	b.seqMu.Lock()
	defer b.seqMu.Unlock()
	b.seq++
	return b.seq
}

// makeKey creates a storage key from sequence number and event metadata.
func (b *PersistentBuffer) makeKey(seq uint64, e *event.ChangeEvent) []byte {
	// Format: seq_timestamp_database_table
	// This ensures ordering by sequence while including metadata
	key := fmt.Sprintf("%020d_%s_%s_%s",
		seq,
		time.Now().UTC().Format("20060102150405"),
		e.Table.Database,
		e.Table.Table)
	return []byte(key)
}

// flushLoop periodically flushes the buffer.
func (b *PersistentBuffer) flushLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := b.Flush(context.Background()); err != nil {
				log.Error("periodic flush failed", zap.Error(err))
			}
		case <-b.stopCh:
			return
		}
	}
}

// PersistentBufferStats holds statistics about the persistent buffer.
type PersistentBufferStats struct {
	PersistedCount int
	InMemoryCount  int
	Capacity       int
}

// ErrBufferClosed is returned when operations are attempted on a closed buffer.
var ErrBufferClosed = fmt.Errorf("buffer is closed")
