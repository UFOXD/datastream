package schema

import (
	"context"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

// mockSchemaHistory is a test double that records calls without persistence.
type mockSchemaHistory struct {
	records []*event.SchemaHistoryRecord
}

func (m *mockSchemaHistory) Record(_ context.Context, record *event.SchemaHistoryRecord) error {
	m.records = append(m.records, record)
	return nil
}

func (m *mockSchemaHistory) Recover(_ context.Context, _ *Tables, _ *event.Position) error {
	return nil
}

func (m *mockSchemaHistory) Exists(_ context.Context) (bool, error) {
	return len(m.records) > 0, nil
}

func (m *mockSchemaHistory) Close() error { return nil }

func newTestRecord(id, db, tbl, ddl string) *event.DDLRecord {
	return &event.DDLRecord{
		ID:       id,
		Position: &event.Position{CommitTime: time.Now()},
		Database: db,
		Table:    tbl,
		DDL:      ddl,
	}
}

func TestDDLRecordManager_CreateAndMarkCompleted(t *testing.T) {
	tables := NewTables()
	history := &mockSchemaHistory{}
	mgr := NewDDLRecordManager(tables, history)
	ctx := context.Background()

	newInfo := &event.TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns:  []event.ColumnInfo{{Name: "id", Type: "int"}, {Name: "name", Type: "varchar"}},
	}

	rec := newTestRecord("r1", "testdb", "users", "ALTER TABLE users ADD COLUMN name VARCHAR(255)")
	rec.NewTableInfo = newInfo

	if err := mgr.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}
	assertStatus(t, mgr, "r1", event.DDLStatusPending)

	if err := mgr.MarkApplying(ctx, "r1"); err != nil {
		t.Fatalf("MarkApplying: %v", err)
	}
	assertStatus(t, mgr, "r1", event.DDLStatusApplying)

	got, _ := mgr.Get(ctx, "r1")
	if got.AppliedAt == nil {
		t.Fatal("expected AppliedAt to be set")
	}

	if err := mgr.MarkCompleted(ctx, "r1"); err != nil {
		t.Fatalf("MarkCompleted: %v", err)
	}
	assertStatus(t, mgr, "r1", event.DDLStatusCompleted)

	got, _ = mgr.Get(ctx, "r1")
	if got.CompletedAt == nil {
		t.Fatal("expected CompletedAt to be set")
	}

	// Tables should be updated.
	if tables.Get("testdb", "users") == nil {
		t.Fatal("expected table to be in Tables after MarkCompleted")
	}

	// History should have been written.
	if len(history.records) != 1 {
		t.Fatalf("expected 1 history record, got %d", len(history.records))
	}
	if history.records[0].ChangeType != "CREATE" {
		// First time the table appears → CREATE
		t.Errorf("expected changeType CREATE, got %s", history.records[0].ChangeType)
	}
}

func TestDDLRecordManager_MarkCompleted_Alter(t *testing.T) {
	tables := NewTables()
	// Pre-populate the table so the next DDL is ALTER.
	tables.Put(&event.TableInfo{Database: "testdb", Table: "users"})

	history := &mockSchemaHistory{}
	mgr := NewDDLRecordManager(tables, history)
	ctx := context.Background()

	rec := newTestRecord("r2", "testdb", "users", "ALTER TABLE users ADD COLUMN age INT")
	rec.NewTableInfo = &event.TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns:  []event.ColumnInfo{{Name: "id", Type: "int"}, {Name: "age", Type: "int"}},
	}

	_ = mgr.Create(ctx, rec)
	_ = mgr.MarkApplying(ctx, "r2")
	if err := mgr.MarkCompleted(ctx, "r2"); err != nil {
		t.Fatalf("MarkCompleted: %v", err)
	}

	if history.records[0].ChangeType != "ALTER" {
		t.Errorf("expected changeType ALTER, got %s", history.records[0].ChangeType)
	}
}

func TestDDLRecordManager_MarkFailed_TablesUnchanged(t *testing.T) {
	tables := NewTables()
	history := &mockSchemaHistory{}
	mgr := NewDDLRecordManager(tables, history)
	ctx := context.Background()

	rec := newTestRecord("r3", "testdb", "orders", "DROP TABLE orders")

	if err := mgr.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := mgr.MarkFailed(ctx, "r3", "connection refused"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}

	assertStatus(t, mgr, "r3", event.DDLStatusFailed)

	got, _ := mgr.Get(ctx, "r3")
	if got.Error != "connection refused" {
		t.Errorf("expected error 'connection refused', got %q", got.Error)
	}

	// Tables must not be modified on failure.
	if tables.Count() != 0 {
		t.Errorf("expected 0 tables, got %d", tables.Count())
	}
	// History must not be written on failure.
	if len(history.records) != 0 {
		t.Errorf("expected 0 history records, got %d", len(history.records))
	}
}

func TestDDLRecordManager_MarkSkipped_TablesUnchanged(t *testing.T) {
	tables := NewTables()
	history := &mockSchemaHistory{}
	mgr := NewDDLRecordManager(tables, history)
	ctx := context.Background()

	rec := newTestRecord("r4", "testdb", "logs", "CREATE TABLE logs (id INT)")

	if err := mgr.Create(ctx, rec); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := mgr.MarkSkipped(ctx, "r4"); err != nil {
		t.Fatalf("MarkSkipped: %v", err)
	}

	assertStatus(t, mgr, "r4", event.DDLStatusSkipped)

	// Tables must not be modified on skip.
	if tables.Count() != 0 {
		t.Errorf("expected 0 tables, got %d", tables.Count())
	}
	// History must not be written on skip.
	if len(history.records) != 0 {
		t.Errorf("expected 0 history records, got %d", len(history.records))
	}
}

func TestDDLRecordManager_NotFound(t *testing.T) {
	tables := NewTables()
	history := &mockSchemaHistory{}
	mgr := NewDDLRecordManager(tables, history)
	ctx := context.Background()

	if _, err := mgr.Get(ctx, "nonexistent"); err == nil {
		t.Fatal("expected error for nonexistent record")
	}
	if err := mgr.MarkApplying(ctx, "nonexistent"); err == nil {
		t.Fatal("expected error for nonexistent record")
	}
	if err := mgr.MarkCompleted(ctx, "nonexistent"); err == nil {
		t.Fatal("expected error for nonexistent record")
	}
	if err := mgr.MarkFailed(ctx, "nonexistent", "err"); err == nil {
		t.Fatal("expected error for nonexistent record")
	}
	if err := mgr.MarkSkipped(ctx, "nonexistent"); err == nil {
		t.Fatal("expected error for nonexistent record")
	}
}

func TestDDLRecordManager_PendingRecords(t *testing.T) {
	tables := NewTables()
	history := &mockSchemaHistory{}
	mgr := NewDDLRecordManager(tables, history)
	ctx := context.Background()

	_ = mgr.Create(ctx, newTestRecord("p1", "db", "t1", "DDL1"))
	_ = mgr.Create(ctx, newTestRecord("p2", "db", "t2", "DDL2"))
	_ = mgr.Create(ctx, newTestRecord("p3", "db", "t3", "DDL3"))
	_ = mgr.MarkApplying(ctx, "p2")

	pending, err := mgr.PendingRecords(ctx)
	if err != nil {
		t.Fatalf("PendingRecords: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending records, got %d", len(pending))
	}
}

// assertStatus checks the status of a record by ID.
func assertStatus(t *testing.T, mgr *DDLRecordManager, id string, want event.DDLStatus) {
	t.Helper()
	rec, err := mgr.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	if rec.Status != want {
		t.Errorf("record %s: want status %q, got %q", id, want, rec.Status)
	}
}
