// Package store provides unified task metadata storage in the target database.
// All task state (positions, lifecycle, schema history, DDL state) is stored
// in a per-task database ds_{task_id} on the target side.
package store

import (
	"context"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

// TargetStore is the interface for unified task metadata storage.
type TargetStore interface {
	// InitDatabase creates the ds_{task_id} database and all tables.
	InitDatabase(ctx context.Context) error

	// --- Task Position ---
	SaveFlushedPosition(ctx context.Context, pos *event.Position) error
	SaveCurrentPosition(ctx context.Context, pos *event.Position) error
	LoadPositions(ctx context.Context) (flushed, current *event.Position, err error)

	// --- Table Lifecycle ---
	SaveTableLifecycle(ctx context.Context, db, tbl, state string, snapshotPos *event.Position, errMsg string) error
	LoadTableLifecycles(ctx context.Context) ([]*TableLifecycleRow, error)
	DeleteTableLifecycle(ctx context.Context, db, tbl string) error

	// --- Schema History ---
	SaveSchemaHistory(ctx context.Context, rec *SchemaHistoryRow) error
	LoadSchemaHistory(ctx context.Context) ([]*SchemaHistoryRow, error)

	// --- DDL State ---
	SaveDDLState(ctx context.Context, rec *DDLStateRow) error
	LoadDDLState(ctx context.Context, db, tbl string) (*DDLStateRow, error)
	LoadPendingDDLStates(ctx context.Context) ([]*DDLStateRow, error)
	DeleteDDLState(ctx context.Context, db, tbl string) error

	// --- Committed Position (MySQL GTID) ---
	SaveCommittedPosition(ctx context.Context, gtidSet string) error
	LoadCommittedPosition(ctx context.Context) (string, error)

	// Close releases resources.
	Close() error
}

// TableLifecycleRow represents a row in the table_lifecycle table.
type TableLifecycleRow struct {
	DBName           string
	TableName        string
	State            string
	SnapshotPosition *event.Position
	ErrorMsg         string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// SchemaHistoryRow represents a row in the schema_history table.
type SchemaHistoryRow struct {
	ID         int64
	Position   event.Position
	DBName     string
	TableName  string
	DDL        string
	TableInfo  *event.TableInfo
	ChangeType string
	CreatedAt  time.Time
}

// DDLStateRow represents a row in the ddl_state table.
type DDLStateRow struct {
	DBName          string
	TableName       string
	DDL             string
	LastSuccessInfo *event.TableInfo
	Status          string // applying / failed
	ErrorMsg        string
	RetryCount      int
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
