package schema

import (
	"context"
	"time"

	"github.com/UFOXD/datastream/internal/store"
	"github.com/UFOXD/datastream/pkg/event"
)

// StoreSchemaHistory adapts store.TargetStore to the SchemaHistory interface.
// It delegates persistence to TargetStore.SaveSchemaHistory / LoadSchemaHistory.
type StoreSchemaHistory struct {
	store store.TargetStore
}

// NewStoreSchemaHistory creates a new StoreSchemaHistory adapter.
func NewStoreSchemaHistory(s store.TargetStore) *StoreSchemaHistory {
	return &StoreSchemaHistory{store: s}
}

// Record persists a SchemaHistoryRecord via TargetStore.
func (h *StoreSchemaHistory) Record(ctx context.Context, record *event.SchemaHistoryRecord) error {
	row := &store.SchemaHistoryRow{
		Position:   record.Position,
		DBName:     record.Database,
		TableName:  record.Table,
		DDL:        record.DDL,
		TableInfo:  record.TableInfo,
		ChangeType: record.ChangeType,
		CreatedAt:  record.Timestamp,
	}
	return h.store.SaveSchemaHistory(ctx, row)
}

// Recover replays persisted schema history records into Tables.
func (h *StoreSchemaHistory) Recover(ctx context.Context, tables *Tables, offset *event.Position) error {
	rows, err := h.store.LoadSchemaHistory(ctx)
	if err != nil {
		return err
	}

	for _, row := range rows {
		// Stop if past offset.
		if offset != nil && !offset.IsZero() && row.Position.CommitTime.After(offset.CommitTime) {
			return nil
		}

		if row.TableInfo != nil {
			tables.Put(row.TableInfo)
		} else if row.ChangeType == "DROP" {
			tables.Remove(row.DBName, row.TableName)
		}
	}
	return nil
}

// Exists returns true if there are any schema history records.
func (h *StoreSchemaHistory) Exists(ctx context.Context) (bool, error) {
	rows, err := h.store.LoadSchemaHistory(ctx)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

// Close is a no-op; the underlying store is closed separately.
func (h *StoreSchemaHistory) Close() error {
	return nil
}

// BuildSchemaHistoryRecord creates a SchemaHistoryRecord from DDL application result.
func BuildSchemaHistoryRecord(
	pos event.Position,
	database, schema, table, ddl string,
	newTableInfo *event.TableInfo,
	changeType string,
) *event.SchemaHistoryRecord {
	return &event.SchemaHistoryRecord{
		Position:   pos,
		Database:   database,
		Schema:     schema,
		Table:      table,
		DDL:        ddl,
		TableInfo:  newTableInfo,
		ChangeType: changeType,
		DDLStatus:  event.DDLStatusCompleted,
		Timestamp:  time.Now(),
	}
}
