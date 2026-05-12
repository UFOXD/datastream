package sink

import (
	"context"
	"fmt"
	"sync"

	"github.com/UFOXD/datastream/pkg/event"
)

// NoPKTableStrategy defines how to handle tables without a primary key or unique index.
const (
	NoPKStrategySingle = "single" // All no-PK rows go to worker 0
	NoPKStrategyTable  = "table"  // Each table is pinned to a consistent worker
)

// DispatcherConfig holds dispatcher configuration.
type DispatcherConfig struct {
	WorkerCount       int    // Number of workers
	BufferSize        int    // Channel buffer size per worker
	NoPKTableStrategy string // "single" or "table" for no-PK tables
}

// DefaultDispatcherConfig returns a default configuration.
func DefaultDispatcherConfig() *DispatcherConfig {
	return &DispatcherConfig{
		WorkerCount:       4,
		BufferSize:        1024,
		NoPKTableStrategy: NoPKStrategyTable,
	}
}

// HashDispatcher dispatches events to workers based on a hash of the row key.
// Events for the same row always go to the same worker, preserving per-row ordering.
type HashDispatcher struct {
	config         *DispatcherConfig
	workerChans    []chan *event.ChangeEvent
	tableWorkerMap sync.Map // map[string]int — table key → worker ID (for no-PK tables)
}

// NewHashDispatcher creates a new HashDispatcher with the given configuration.
func NewHashDispatcher(config *DispatcherConfig) *HashDispatcher {
	if config == nil {
		config = DefaultDispatcherConfig()
	}
	if config.WorkerCount <= 0 {
		config.WorkerCount = 4
	}
	if config.BufferSize <= 0 {
		config.BufferSize = 1024
	}
	if config.NoPKTableStrategy == "" {
		config.NoPKTableStrategy = NoPKStrategyTable
	}

	chans := make([]chan *event.ChangeEvent, config.WorkerCount)
	for i := range chans {
		chans[i] = make(chan *event.ChangeEvent, config.BufferSize)
	}

	return &HashDispatcher{
		config:      config,
		workerChans: chans,
	}
}

// Dispatch sends the event to the appropriate worker channel.
// It returns an error if the context is cancelled or the channel is full and the
// context deadline is exceeded.
func (d *HashDispatcher) Dispatch(ctx context.Context, e *event.ChangeEvent, schema *event.TableInfo) error {
	workerID := d.calculateWorkerID(e, schema)
	select {
	case d.workerChans[workerID] <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// calculateWorkerID determines which worker should handle the given event.
//
// Rules:
//  1. Table has PK or unique index → hash the row key → consistent worker.
//  2. Table has no PK/UK and strategy is "single" → always worker 0.
//  3. Table has no PK/UK and strategy is "table" → pin the table to a worker
//     (determined once by hashing the table name, then stored in tableWorkerMap).
func (d *HashDispatcher) calculateWorkerID(e *event.ChangeEvent, schema *event.TableInfo) int {
	rid := BuildRowIdentifier(e, schema)

	switch rid.KeyType {
	case KeyTypePrimaryKey, KeyTypeUniqueIndex:
		// Hash by the full row key for consistent per-row routing.
		return int(fnv32(rid.HashKey())) % d.config.WorkerCount

	default: // KeyTypeFullRow — table has no PK or unique index
		switch d.config.NoPKTableStrategy {
		case NoPKStrategySingle:
			return 0

		default: // NoPKStrategyTable
			tableKey := fmt.Sprintf("%s.%s.%s", schema.Schema, schema.Database, schema.Table)
			if id, ok := d.tableWorkerMap.Load(tableKey); ok {
				return id.(int)
			}
			// Assign a stable worker by hashing the table key, then cache it.
			workerID := int(fnv32(tableKey)) % d.config.WorkerCount
			d.tableWorkerMap.Store(tableKey, workerID)
			return workerID
		}
	}
}

// WorkerChannels returns the read-only view of all worker channels.
func (d *HashDispatcher) WorkerChannels() []chan *event.ChangeEvent {
	return d.workerChans
}

// Close closes all worker channels, signalling workers to stop.
func (d *HashDispatcher) Close() {
	for _, ch := range d.workerChans {
		close(ch)
	}
}

// fnv32 computes the FNV-1a 32-bit hash of the given string.
func fnv32(key string) uint32 {
	const (
		offset32 uint32 = 2166136261
		prime32  uint32 = 16777619
	)
	h := offset32
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime32
	}
	return h
}
