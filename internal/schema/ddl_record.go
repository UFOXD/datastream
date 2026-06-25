package schema

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

// Sentinel errors for DDLRecordManager operations.
var (
	// ErrDDLRecordNotFound is returned when a record ID does not exist.
	ErrDDLRecordNotFound = errors.New("ddl record not found")
)

// DDLRecordManager manages the lifecycle of DDL application records.
// It coordinates in-memory state (Tables) with persistent history (SchemaHistory).
type DDLRecordManager struct {
	records map[string]*event.DDLRecord
	tables  *Tables
	history SchemaHistory
	mu      sync.RWMutex
}

// NewDDLRecordManager creates a new DDLRecordManager.
func NewDDLRecordManager(tables *Tables, history SchemaHistory) *DDLRecordManager {
	return &DDLRecordManager{
		records: make(map[string]*event.DDLRecord),
		tables:  tables,
		history: history,
	}
}

// Create stores a new DDL record with Pending status.
func (m *DDLRecordManager) Create(_ context.Context, record *event.DDLRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	record.Status = event.DDLStatusPending
	record.CreatedAt = time.Now()
	m.records[record.ID] = record
	return nil
}

// MarkApplying transitions a record to Applying status.
func (m *DDLRecordManager) MarkApplying(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.records[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrDDLRecordNotFound, id)
	}

	now := time.Now()
	rec.Status = event.DDLStatusApplying
	rec.AppliedAt = &now
	return nil
}

// MarkCompleted transitions a record to Completed status.
// It updates the Tables collection and writes a SchemaHistoryRecord.
func (m *DDLRecordManager) MarkCompleted(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.records[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrDDLRecordNotFound, id)
	}

	now := time.Now()
	rec.Status = event.DDLStatusCompleted
	rec.CompletedAt = &now

	// Determine change type BEFORE updating Tables.
	changeType := m.changeTypeFromDDL(rec)

	// Update Tables: put new table info or remove for DROP.
	if rec.NewTableInfo != nil {
		m.tables.Put(rec.NewTableInfo)
	} else {
		m.tables.Remove(rec.Database, rec.Table)
	}

	// Write to persistent history.
	histRec := &event.SchemaHistoryRecord{
		Position:   *rec.Position,
		Database:   rec.Database,
		Table:      rec.Table,
		DDL:        rec.DDL,
		TableInfo:  rec.NewTableInfo,
		ChangeType: changeType,
		DDLStatus:  event.DDLStatusCompleted,
		Timestamp:  now,
	}
	if rec.NewTableInfo != nil {
		histRec.Schema = rec.NewTableInfo.Schema
	}
	return m.history.Record(ctx, histRec)
}

// MarkFailed transitions a record to Failed status.
// Tables are not modified.
func (m *DDLRecordManager) MarkFailed(_ context.Context, id string, errStr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.records[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrDDLRecordNotFound, id)
	}

	rec.Status = event.DDLStatusFailed
	rec.Error = errStr
	return nil
}

// MarkSkipped transitions a record to Skipped status.
// Tables are not modified.
func (m *DDLRecordManager) MarkSkipped(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.records[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrDDLRecordNotFound, id)
	}

	rec.Status = event.DDLStatusSkipped
	return nil
}

// Get retrieves a DDL record by ID.
func (m *DDLRecordManager) Get(_ context.Context, id string) (*event.DDLRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rec, ok := m.records[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrDDLRecordNotFound, id)
	}
	return rec, nil
}

// PendingRecords returns all records with Pending status.
func (m *DDLRecordManager) PendingRecords(_ context.Context) ([]*event.DDLRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var pending []*event.DDLRecord
	for _, rec := range m.records {
		if rec.Status == event.DDLStatusPending {
			pending = append(pending, rec)
		}
	}
	return pending, nil
}

// changeTypeFromDDL infers the change type from the DDL record.
// It uses the manager's Tables to distinguish CREATE from ALTER.
func (m *DDLRecordManager) changeTypeFromDDL(rec *event.DDLRecord) string {
	if rec.NewTableInfo == nil {
		return "DROP"
	}
	if m.tables.Get(rec.Database, rec.Table) != nil {
		return "ALTER"
	}
	return "CREATE"
}
