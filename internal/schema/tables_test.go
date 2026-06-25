package schema

import (
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
)

func TestTables_PutAndGet(t *testing.T) {
	tables := NewTables()

	info := &event.TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "int", Nullable: false},
			{Name: "name", Type: "varchar", Nullable: true, Length: 255},
		},
		PrimaryKeyColumns: []string{"id"},
	}

	tables.Put(info)

	got := tables.Get("testdb", "users")
	if got == nil {
		t.Fatal("expected table info, got nil")
	}
	if got.Database != "testdb" {
		t.Errorf("database = %q, want %q", got.Database, "testdb")
	}
	if got.Table != "users" {
		t.Errorf("table = %q, want %q", got.Table, "users")
	}
	if len(got.Columns) != 2 {
		t.Errorf("columns count = %d, want 2", len(got.Columns))
	}
	if len(got.PrimaryKeyColumns) != 1 || got.PrimaryKeyColumns[0] != "id" {
		t.Errorf("primaryKeyColumns = %v, want [id]", got.PrimaryKeyColumns)
	}
}

func TestTables_GetNotFound(t *testing.T) {
	tables := NewTables()

	got := tables.Get("nonexistent", "table")
	if got != nil {
		t.Errorf("expected nil for non-existent table, got %v", got)
	}
}

func TestTables_PutOverwrite(t *testing.T) {
	tables := NewTables()

	info1 := &event.TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "int"},
		},
	}
	tables.Put(info1)

	info2 := &event.TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "int"},
			{Name: "email", Type: "varchar", Length: 128},
		},
	}
	tables.Put(info2)

	got := tables.Get("testdb", "users")
	if got == nil {
		t.Fatal("expected table info after overwrite, got nil")
	}
	if len(got.Columns) != 2 {
		t.Errorf("columns count = %d after overwrite, want 2", len(got.Columns))
	}
}

func TestTables_Remove(t *testing.T) {
	tables := NewTables()

	tables.Put(&event.TableInfo{Database: "db1", Table: "t1"})
	tables.Put(&event.TableInfo{Database: "db1", Table: "t2"})
	tables.Put(&event.TableInfo{Database: "db2", Table: "t1"})

	tables.Remove("db1", "t1")

	if tables.Get("db1", "t1") != nil {
		t.Error("expected db1.t1 to be removed")
	}
	if tables.Get("db1", "t2") == nil {
		t.Error("expected db1.t2 to still exist")
	}
	if tables.Get("db2", "t1") == nil {
		t.Error("expected db2.t1 to still exist")
	}
}

func TestTables_RemoveNonExistent(t *testing.T) {
	tables := NewTables()
	// Should not panic.
	tables.Remove("nope", "nope")
}

func TestTables_All(t *testing.T) {
	tables := NewTables()

	tables.Put(&event.TableInfo{Database: "db1", Table: "t1"})
	tables.Put(&event.TableInfo{Database: "db1", Table: "t2"})
	tables.Put(&event.TableInfo{Database: "db2", Table: "t1"})

	all := tables.All()
	if len(all) != 3 {
		t.Errorf("All() count = %d, want 3", len(all))
	}

	// Verify all keys present.
	expectedKeys := []string{"db1.t1", "db1.t2", "db2.t1"}
	for _, key := range expectedKeys {
		if _, ok := all[key]; !ok {
			t.Errorf("All() missing key %q", key)
		}
	}
}

func TestTables_AllReturnsSnapshot(t *testing.T) {
	tables := NewTables()
	tables.Put(&event.TableInfo{Database: "db", Table: "t1"})

	all := tables.All()
	tables.Put(&event.TableInfo{Database: "db", Table: "t2"})

	// The snapshot taken before t2 was added should still have 1 entry.
	if len(all) != 1 {
		t.Errorf("All() snapshot changed after mutation: count = %d, want 1", len(all))
	}
}

func TestTables_Count(t *testing.T) {
	tables := NewTables()

	if tables.Count() != 0 {
		t.Errorf("Count() = %d on empty tables, want 0", tables.Count())
	}

	tables.Put(&event.TableInfo{Database: "db", Table: "t1"})
	tables.Put(&event.TableInfo{Database: "db", Table: "t2"})

	if tables.Count() != 2 {
		t.Errorf("Count() = %d, want 2", tables.Count())
	}

	tables.Remove("db", "t1")

	if tables.Count() != 1 {
		t.Errorf("Count() = %d after remove, want 1", tables.Count())
	}
}
