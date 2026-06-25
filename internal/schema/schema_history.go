package schema

import (
	"context"

	"github.com/UFOXD/datastream/pkg/event"
)

// SchemaHistory is the interface for persistent DDL history storage.
type SchemaHistory interface {
	// Record appends a SchemaHistoryRecord to the history log.
	Record(ctx context.Context, record *event.SchemaHistoryRecord) error

	// Recover replays history records into Tables.
	// Only records with position ≤ offset are applied.
	Recover(ctx context.Context, tables *Tables, offset *event.Position) error

	// Exists returns true if the history log file exists.
	Exists(ctx context.Context) (bool, error)

	// Close releases resources.
	Close() error
}
