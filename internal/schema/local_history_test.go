package schema

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

func TestLocalSchemaHistory_RecordAndRecover(t *testing.T) {
	dir := t.TempDir()

	h, err := NewLocalSchemaHistory(dir)
	if err != nil {
		t.Fatalf("NewLocalSchemaHistory: %v", err)
	}
	defer h.Close()

	ctx := context.Background()
	now := time.Date(2026, 6, 25, 10, 0, 0, 0, time.UTC)

	// Record 1: CREATE users table.
	rec1 := &event.SchemaHistoryRecord{
		Position:   event.Position{CommitTime: now, BinlogFile: "mysql-bin.000001", BinlogPos: 100},
		Database:   "testdb",
		Table:      "users",
		DDL:        "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))",
		ChangeType: "CREATE",
		DDLStatus:  event.DDLStatusCompleted,
		Timestamp:  now,
		TableInfo: &event.TableInfo{
			Database:          "testdb",
			Table:             "users",
			Columns:           []event.ColumnInfo{{Name: "id", Type: "int"}, {Name: "name", Type: "varchar", Length: 255}},
			PrimaryKeyColumns: []string{"id"},
		},
	}
	if err := h.Record(ctx, rec1); err != nil {
		t.Fatalf("Record rec1: %v", err)
	}

	// Record 2: ALTER users add email column.
	rec2 := &event.SchemaHistoryRecord{
		Position:   event.Position{CommitTime: now.Add(1 * time.Second), BinlogFile: "mysql-bin.000001", BinlogPos: 200},
		Database:   "testdb",
		Table:      "users",
		DDL:        "ALTER TABLE users ADD COLUMN email VARCHAR(128)",
		ChangeType: "ALTER",
		DDLStatus:  event.DDLStatusCompleted,
		Timestamp:  now.Add(1 * time.Second),
		TableInfo: &event.TableInfo{
			Database:          "testdb",
			Table:             "users",
			Columns:           []event.ColumnInfo{{Name: "id", Type: "int"}, {Name: "name", Type: "varchar", Length: 255}, {Name: "email", Type: "varchar", Length: 128}},
			PrimaryKeyColumns: []string{"id"},
		},
	}
	if err := h.Record(ctx, rec2); err != nil {
		t.Fatalf("Record rec2: %v", err)
	}

	// Record 3: CREATE orders table.
	rec3 := &event.SchemaHistoryRecord{
		Position:   event.Position{CommitTime: now.Add(2 * time.Second), BinlogFile: "mysql-bin.000001", BinlogPos: 300},
		Database:   "testdb",
		Table:      "orders",
		DDL:        "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)",
		ChangeType: "CREATE",
		DDLStatus:  event.DDLStatusCompleted,
		Timestamp:  now.Add(2 * time.Second),
		TableInfo: &event.TableInfo{
			Database:          "testdb",
			Table:             "orders",
			Columns:           []event.ColumnInfo{{Name: "id", Type: "int"}, {Name: "user_id", Type: "int"}},
			PrimaryKeyColumns: []string{"id"},
		},
	}
	if err := h.Record(ctx, rec3); err != nil {
		t.Fatalf("Record rec3: %v", err)
	}

	// Record 4: DROP orders table.
	rec4 := &event.SchemaHistoryRecord{
		Position:   event.Position{CommitTime: now.Add(3 * time.Second), BinlogFile: "mysql-bin.000001", BinlogPos: 400},
		Database:   "testdb",
		Table:      "orders",
		DDL:        "DROP TABLE orders",
		ChangeType: "DROP",
		DDLStatus:  event.DDLStatusCompleted,
		Timestamp:  now.Add(3 * time.Second),
	}
	if err := h.Record(ctx, rec4); err != nil {
		t.Fatalf("Record rec4: %v", err)
	}

	// Close and create a new instance (simulates restart).
	h.Close()

	h2, err := NewLocalSchemaHistory(dir)
	if err != nil {
		t.Fatalf("NewLocalSchemaHistory (restart): %v", err)
	}
	defer h2.Close()

	// Recover all records (no offset filter).
	tables := NewTables()
	if err := h2.Recover(ctx, tables, nil); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// users should exist with the ALTER'd schema (3 columns).
	users := tables.Get("testdb", "users")
	if users == nil {
		t.Fatal("expected users table after recover, got nil")
	}
	if len(users.Columns) != 3 {
		t.Errorf("users columns count = %d, want 3", len(users.Columns))
	}

	// orders should NOT exist (was dropped).
	if tables.Get("testdb", "orders") != nil {
		t.Error("expected orders to be removed after DROP recover")
	}

	// Total tables should be 1.
	if tables.Count() != 1 {
		t.Errorf("tables.Count() = %d, want 1", tables.Count())
	}
}

func TestLocalSchemaHistory_RecoverWithOffset(t *testing.T) {
	dir := t.TempDir()

	h, err := NewLocalSchemaHistory(dir)
	if err != nil {
		t.Fatalf("NewLocalSchemaHistory: %v", err)
	}
	defer h.Close()

	ctx := context.Background()
	base := time.Date(2026, 6, 25, 10, 0, 0, 0, time.UTC)

	// Record at t=0: CREATE users.
	if err := h.Record(ctx, &event.SchemaHistoryRecord{
		Position:   event.Position{CommitTime: base, BinlogFile: "bin.001", BinlogPos: 100},
		Database:   "db",
		Table:      "users",
		ChangeType: "CREATE",
		DDLStatus:  event.DDLStatusCompleted,
		Timestamp:  base,
		TableInfo:  &event.TableInfo{Database: "db", Table: "users"},
	}); err != nil {
		t.Fatal(err)
	}

	// Record at t=2: CREATE orders.
	if err := h.Record(ctx, &event.SchemaHistoryRecord{
		Position:   event.Position{CommitTime: base.Add(2 * time.Second), BinlogFile: "bin.001", BinlogPos: 200},
		Database:   "db",
		Table:      "orders",
		ChangeType: "CREATE",
		DDLStatus:  event.DDLStatusCompleted,
		Timestamp:  base.Add(2 * time.Second),
		TableInfo:  &event.TableInfo{Database: "db", Table: "orders"},
	}); err != nil {
		t.Fatal(err)
	}

	// Record at t=4: DROP users.
	if err := h.Record(ctx, &event.SchemaHistoryRecord{
		Position:   event.Position{CommitTime: base.Add(4 * time.Second), BinlogFile: "bin.001", BinlogPos: 300},
		Database:   "db",
		Table:      "users",
		ChangeType: "DROP",
		DDLStatus:  event.DDLStatusCompleted,
		Timestamp:  base.Add(4 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}

	h.Close()

	// Restart and recover with offset at t=1 (only first record should apply).
	h2, err := NewLocalSchemaHistory(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	tables := NewTables()
	offset := &event.Position{CommitTime: base.Add(1 * time.Second)}
	if err := h2.Recover(ctx, tables, offset); err != nil {
		t.Fatal(err)
	}

	// Only users should exist (first record within offset).
	if tables.Get("db", "users") == nil {
		t.Error("expected users to exist within offset")
	}
	if tables.Count() != 1 {
		t.Errorf("tables.Count() = %d, want 1", tables.Count())
	}
}

func TestLocalSchemaHistory_Exists(t *testing.T) {
	dir := t.TempDir()

	h, err := NewLocalSchemaHistory(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	ctx := context.Background()

	// Before any records.
	exists, err := h.Exists(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Error("expected Exists() = false before any records")
	}

	// After writing a record.
	if err := h.Record(ctx, &event.SchemaHistoryRecord{
		Position:   event.Position{CommitTime: time.Now()},
		Database:   "db",
		Table:      "t",
		ChangeType: "CREATE",
		DDLStatus:  event.DDLStatusCompleted,
		Timestamp:  time.Now(),
		TableInfo:  &event.TableInfo{Database: "db", Table: "t"},
	}); err != nil {
		t.Fatal(err)
	}

	exists, err = h.Exists(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Error("expected Exists() = true after writing a record")
	}
}

func TestLocalSchemaHistory_RecoverEmptyFile(t *testing.T) {
	dir := t.TempDir()

	h, err := NewLocalSchemaHistory(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	// Recover from empty file.
	tables := NewTables()
	if err := h.Recover(context.Background(), tables, nil); err != nil {
		t.Fatalf("Recover on empty file: %v", err)
	}
	if tables.Count() != 0 {
		t.Errorf("tables.Count() = %d, want 0", tables.Count())
	}
}

func TestLocalSchemaHistory_RecoverNoFile(t *testing.T) {
	dir := t.TempDir()

	// Use a path that doesn't exist yet — Recover should return nil (no error).
	fp := filepath.Join(dir, "meta", "schema_history.log")
	h := &LocalSchemaHistory{filePath: fp}

	tables := NewTables()
	if err := h.Recover(context.Background(), tables, nil); err != nil {
		t.Fatalf("Recover on missing file: %v", err)
	}
	if tables.Count() != 0 {
		t.Errorf("tables.Count() = %d, want 0", tables.Count())
	}
}

func TestLocalSchemaHistory_Close(t *testing.T) {
	dir := t.TempDir()

	h, err := NewLocalSchemaHistory(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Write a record so the file handle is used.
	if err := h.Record(context.Background(), &event.SchemaHistoryRecord{
		Position:   event.Position{CommitTime: time.Now()},
		Database:   "db",
		Table:      "t",
		ChangeType: "CREATE",
		DDLStatus:  event.DDLStatusCompleted,
		Timestamp:  time.Now(),
		TableInfo:  &event.TableInfo{Database: "db", Table: "t"},
	}); err != nil {
		t.Fatal(err)
	}

	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// File should be flushed and readable.
	data, err := os.ReadFile(filepath.Join(dir, "meta", "schema_history.log"))
	if err != nil {
		t.Fatalf("ReadFile after Close: %v", err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty file after Close")
	}
}
