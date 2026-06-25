package cache

import (
	"context"

	"github.com/UFOXD/datastream/pkg/event"
)

// ReadResult contains an event stream and an independent error stream.
// Normal end: both channels close, Err has no value.
// Error: one error sent to Err, then both channels close.
// Context cancelled: both channels close, Err has no value.
type ReadResult struct {
	Events <-chan *CacheEvent
	Err    <-chan error
}

// SyncMode controls fsync behavior.
type SyncMode string

const (
	SyncModeEvery SyncMode = "every" // fsync after every event
	SyncModeBatch SyncMode = "batch" // fsync every N events or every transaction
	SyncModeNone  SyncMode = "none"  // no explicit fsync (rely on OS)
)

// BinlogCacheBackend is the interface for buffered binlog event storage.
type BinlogCacheBackend interface {
	// Write appends a single CacheEvent to the table's buffer file.
	Write(ctx context.Context, tableID string, event *CacheEvent) error

	// WriteBatch atomically writes a batch of CacheEvents (transaction-atomic).
	// All events land on disk or none do.
	WriteBatch(ctx context.Context, tableID string, events []*CacheEvent) error

	// Read streams CacheEvents from the table's buffer file.
	Read(ctx context.Context, tableID string, fromTxID string, fromEventSeq int64) ReadResult

	// Delete removes the buffer file for the given table.
	Delete(ctx context.Context, tableID string) error

	// Size returns the size in bytes of the buffer file for the given table.
	Size(ctx context.Context, tableID string) (int64, error)

	// TotalSize returns the total size of all buffer files.
	TotalSize(ctx context.Context) (int64, error)

	// Exists returns true if the buffer file for the given table exists.
	Exists(ctx context.Context, tableID string) (bool, error)

	// Sync forces fsync on the table's buffer file.
	Sync(ctx context.Context, tableID string) error

	// TruncateToLastComplete scans the table's buffer file from the tail,
	// finds the last complete record, and truncates everything after it.
	// Returns the last complete position, or nil if the file is empty.
	TruncateToLastComplete(ctx context.Context, tableID string) (*event.Position, error)

	// Close closes all open file handles.
	Close() error
}
