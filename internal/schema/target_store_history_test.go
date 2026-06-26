package schema

import (
	"context"
	"testing"
	"time"

	"github.com/UFOXD/datastream/internal/store"
	"github.com/UFOXD/datastream/pkg/event"
)

// mockTargetStore is a minimal mock of store.TargetStore for testing.
type mockTargetStore struct {
	history []*store.SchemaHistoryRow
}

func (m *mockTargetStore) InitDatabase(_ context.Context) error { return nil }
func (m *mockTargetStore) SaveFlushedPosition(_ context.Context, _ *event.Position) error {
	return nil
}
func (m *mockTargetStore) SaveCurrentPosition(_ context.Context, _ *event.Position) error {
	return nil
}
func (m *mockTargetStore) LoadPositions(_ context.Context) (*event.Position, *event.Position, error) {
	return nil, nil, nil
}
func (m *mockTargetStore) SaveTableLifecycle(_ context.Context, _, _, _ string, _ *event.Position, _ string) error {
	return nil
}
func (m *mockTargetStore) LoadTableLifecycles(_ context.Context) ([]*store.TableLifecycleRow, error) {
	return nil, nil
}
func (m *mockTargetStore) DeleteTableLifecycle(_ context.Context, _, _ string) error { return nil }
func (m *mockTargetStore) SaveSchemaHistory(_ context.Context, rec *store.SchemaHistoryRow) error {
	m.history = append(m.history, rec)
	return nil
}
func (m *mockTargetStore) LoadSchemaHistory(_ context.Context) ([]*store.SchemaHistoryRow, error) {
	return m.history, nil
}
func (m *mockTargetStore) SaveDDLState(_ context.Context, _ *store.DDLStateRow) error { return nil }
func (m *mockTargetStore) LoadDDLState(_ context.Context, _, _ string) (*store.DDLStateRow, error) {
	return nil, nil
}
func (m *mockTargetStore) LoadPendingDDLStates(_ context.Context) ([]*store.DDLStateRow, error) {
	return nil, nil
}
func (m *mockTargetStore) DeleteDDLState(_ context.Context, _, _ string) error { return nil }
func (m *mockTargetStore) SaveCommittedPosition(_ context.Context, _ string) error {
	return nil
}
func (m *mockTargetStore) LoadCommittedPosition(_ context.Context) (string, error) { return "", nil }
func (m *mockTargetStore) Close() error                                             { return nil }

var _ store.TargetStore = (*mockTargetStore)(nil)

func TestTargetStoreSchemaHistory_Record(t *testing.T) {
	mock := &mockTargetStore{}
	h := NewTargetStoreSchemaHistory(mock)

	ctx := context.Background()
	rec := &event.SchemaHistoryRecord{
		Position:   event.Position{SCN: 100, CommitTime: time.Now()},
		Database:   "ORCL",
		Table:      "USERS",
		DDL:        "CREATE TABLE USERS (ID INT)",
		TableInfo:  &event.TableInfo{Database: "ORCL", Table: "USERS"},
		ChangeType: "CREATE",
		DDLStatus:  event.DDLStatusCompleted,
		Timestamp:  time.Now(),
	}

	if err := h.Record(ctx, rec); err != nil {
		t.Fatalf("Record failed: %v", err)
	}

	if len(mock.history) != 1 {
		t.Fatalf("expected 1 record, got %d", len(mock.history))
	}

	saved := mock.history[0]
	if saved.DBName != "ORCL" {
		t.Errorf("expected DBName ORCL, got %s", saved.DBName)
	}
	if saved.TableName != "USERS" {
		t.Errorf("expected TableName USERS, got %s", saved.TableName)
	}
	if saved.DDL != "CREATE TABLE USERS (ID INT)" {
		t.Errorf("unexpected DDL: %s", saved.DDL)
	}
	if saved.ChangeType != "CREATE" {
		t.Errorf("expected ChangeType CREATE, got %s", saved.ChangeType)
	}
	if saved.TableInfo == nil {
		t.Error("expected TableInfo to be set")
	}
}

func TestTargetStoreSchemaHistory_Recover(t *testing.T) {
	now := time.Now()
	mock := &mockTargetStore{
		history: []*store.SchemaHistoryRow{
			{
				Position:   event.Position{SCN: 100, CommitTime: now},
				DBName:     "ORCL",
				TableName:  "USERS",
				DDL:        "CREATE TABLE USERS (ID INT)",
				TableInfo:  &event.TableInfo{Database: "ORCL", Table: "USERS"},
				ChangeType: "CREATE",
			},
			{
				Position:   event.Position{SCN: 200, CommitTime: now.Add(time.Second)},
				DBName:     "ORCL",
				TableName:  "ORDERS",
				DDL:        "CREATE TABLE ORDERS (ID INT)",
				TableInfo:  &event.TableInfo{Database: "ORCL", Table: "ORDERS"},
				ChangeType: "CREATE",
			},
			{
				Position:   event.Position{SCN: 300, CommitTime: now.Add(2 * time.Second)},
				DBName:     "ORCL",
				TableName:  "OLD_TABLE",
				DDL:        "DROP TABLE OLD_TABLE",
				TableInfo:  nil,
				ChangeType: "DROP",
			},
		},
	}

	h := NewTargetStoreSchemaHistory(mock)
	tables := NewTables()

	// Recover all (no offset)
	if err := h.Recover(context.Background(), tables, nil); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if tables.Get("ORCL", "USERS") == nil {
		t.Error("expected USERS table to be recovered")
	}
	if tables.Get("ORCL", "ORDERS") == nil {
		t.Error("expected ORDERS table to be recovered")
	}
	if tables.Get("ORCL", "OLD_TABLE") != nil {
		t.Error("expected OLD_TABLE to be removed (DROP)")
	}
}

func TestTargetStoreSchemaHistory_RecoverWithOffset(t *testing.T) {
	now := time.Now()
	mock := &mockTargetStore{
		history: []*store.SchemaHistoryRow{
			{
				Position:   event.Position{SCN: 100, CommitTime: now},
				DBName:     "ORCL",
				TableName:  "USERS",
				DDL:        "CREATE TABLE USERS (ID INT)",
				TableInfo:  &event.TableInfo{Database: "ORCL", Table: "USERS"},
				ChangeType: "CREATE",
			},
			{
				Position:   event.Position{SCN: 200, CommitTime: now.Add(time.Second)},
				DBName:     "ORCL",
				TableName:  "ORDERS",
				DDL:        "CREATE TABLE ORDERS (ID INT)",
				TableInfo:  &event.TableInfo{Database: "ORCL", Table: "ORDERS"},
				ChangeType: "CREATE",
			},
		},
	}

	h := NewTargetStoreSchemaHistory(mock)
	tables := NewTables()

	// Recover only up to SCN 100
	offset := &event.Position{SCN: 100, CommitTime: now}
	if err := h.Recover(context.Background(), tables, offset); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if tables.Get("ORCL", "USERS") == nil {
		t.Error("expected USERS table to be recovered")
	}
	if tables.Get("ORCL", "ORDERS") != nil {
		t.Error("expected ORDERS table NOT to be recovered (after offset)")
	}
}

func TestTargetStoreSchemaHistory_Exists(t *testing.T) {
	mock := &mockTargetStore{}
	h := NewTargetStoreSchemaHistory(mock)

	exists, err := h.Exists(context.Background())
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("expected Exists=false for empty history")
	}

	// Add a record
	mock.history = append(mock.history, &store.SchemaHistoryRow{
		DBName: "ORCL", TableName: "T", DDL: "CREATE TABLE T (ID INT)",
	})

	exists, err = h.Exists(context.Background())
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected Exists=true after adding record")
	}
}

func TestTargetStoreSchemaHistory_Close(t *testing.T) {
	mock := &mockTargetStore{}
	h := NewTargetStoreSchemaHistory(mock)
	if err := h.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}
