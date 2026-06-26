// Package schema provides in-memory table definitions and schema history management.
package schema

import (
	"context"

	"github.com/UFOXD/datastream/internal/store"
	"github.com/UFOXD/datastream/pkg/event"
)

// TargetStoreSchemaHistory adapts store.TargetStore to the SchemaHistory interface.
// It bridges the unified task metadata store with the schema history contract
// used by DDLRecordManager and connector recovery logic.
type TargetStoreSchemaHistory struct {
	store store.TargetStore
}

// NewTargetStoreSchemaHistory creates a new TargetStoreSchemaHistory adapter.
func NewTargetStoreSchemaHistory(s store.TargetStore) *TargetStoreSchemaHistory {
	return &TargetStoreSchemaHistory{store: s}
}

// Record appends a SchemaHistoryRecord to the persistent store.
func (h *TargetStoreSchemaHistory) Record(ctx context.Context, record *event.SchemaHistoryRecord) error {
	row := &store.SchemaHistoryRow{
		Position:   record.Position,
		DBName:     record.Database,
		TableName:  record.Table,
		DDL:        record.DDL,
		TableInfo:  record.TableInfo,
		ChangeType: record.ChangeType,
	}
	return h.store.SaveSchemaHistory(ctx, row)
}

// Recover replays history records into Tables.
// Only records with position <= offset are applied.
func (h *TargetStoreSchemaHistory) Recover(ctx context.Context, tables *Tables, offset *event.Position) error {
	rows, err := h.store.LoadSchemaHistory(ctx)
	if err != nil {
		return err
	}

	for _, row := range rows {
		// Stop if we've passed the offset.
		if offset != nil && !offset.IsZero() && row.Position.CommitTime.After(offset.CommitTime) {
			return nil
		}

		// Apply to Tables.
		if row.TableInfo != nil {
			tables.Put(row.TableInfo)
		} else if row.ChangeType == "DROP" {
			tables.Remove(row.DBName, row.TableName)
		}
	}
	return nil
}

// Exists returns true if there are any schema history records.
func (h *TargetStoreSchemaHistory) Exists(ctx context.Context) (bool, error) {
	rows, err := h.store.LoadSchemaHistory(ctx)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

// Close is a no-op; the underlying store is managed externally.
func (h *TargetStoreSchemaHistory) Close() error {
	return nil
}
