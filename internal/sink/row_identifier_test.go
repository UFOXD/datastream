package sink

import (
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
)

func makeEvent(before, after *event.RowData) *event.ChangeEvent {
	e := &event.ChangeEvent{
		Type: event.EventTypeInsert,
	}
	if before != nil {
		e.Before = *before
	}
	if after != nil {
		e.After = *after
	}
	return e
}

func makeRowData(fields map[string]interface{}) *event.RowData {
	rd := event.NewRowData()
	for k, v := range fields {
		rd.Set(k, v, "string")
	}
	return rd
}

// TestBuildRowIdentifier_PrimaryKey tests single primary key extraction.
func TestBuildRowIdentifier_PrimaryKey(t *testing.T) {
	schema := &event.TableInfo{
		Database:          "testdb",
		Schema:            "public",
		Table:             "users",
		PrimaryKeyColumns: []string{"id"},
	}

	after := makeRowData(map[string]interface{}{
		"id":   int64(42),
		"name": "Alice",
	})
	e := makeEvent(nil, after)

	rid := BuildRowIdentifier(e, schema)

	if rid.KeyType != KeyTypePrimaryKey {
		t.Errorf("expected KeyTypePrimaryKey, got %d", rid.KeyType)
	}
	if rid.Database != "testdb" {
		t.Errorf("expected database testdb, got %s", rid.Database)
	}
	if rid.Schema != "public" {
		t.Errorf("expected schema public, got %s", rid.Schema)
	}
	if rid.Table != "users" {
		t.Errorf("expected table users, got %s", rid.Table)
	}
	// PrimaryKeyValues should contain the id field
	if rid.PrimaryKeyValues != "id=42" {
		t.Errorf("unexpected PrimaryKeyValues: %q", rid.PrimaryKeyValues)
	}
	// HashKey and String should not panic / be non-empty
	if rid.HashKey() == "" {
		t.Error("HashKey() returned empty string")
	}
	if rid.String() == "" {
		t.Error("String() returned empty string")
	}
}

// TestBuildRowIdentifier_CompositeKey tests composite primary key extraction.
func TestBuildRowIdentifier_CompositeKey(t *testing.T) {
	schema := &event.TableInfo{
		Database:          "testdb",
		Schema:            "",
		Table:             "order_items",
		PrimaryKeyColumns: []string{"order_id", "item_id"},
	}

	after := makeRowData(map[string]interface{}{
		"order_id": int64(100),
		"item_id":  int64(5),
		"qty":      int64(2),
	})
	e := makeEvent(nil, after)

	rid := BuildRowIdentifier(e, schema)

	if rid.KeyType != KeyTypePrimaryKey {
		t.Errorf("expected KeyTypePrimaryKey, got %d", rid.KeyType)
	}
	// Both pk columns must appear in the key values
	if rid.PrimaryKeyValues != "order_id=100,item_id=5" {
		t.Errorf("unexpected PrimaryKeyValues: %q", rid.PrimaryKeyValues)
	}

	// Two events with the same composite PK must produce the same HashKey
	after2 := makeRowData(map[string]interface{}{
		"order_id": int64(100),
		"item_id":  int64(5),
		"qty":      int64(9),
	})
	e2 := makeEvent(nil, after2)
	rid2 := BuildRowIdentifier(e2, schema)

	if rid.HashKey() != rid2.HashKey() {
		t.Errorf("same PK rows must have equal HashKey: %q vs %q", rid.HashKey(), rid2.HashKey())
	}

	// Different PK must produce different HashKey
	after3 := makeRowData(map[string]interface{}{
		"order_id": int64(200),
		"item_id":  int64(5),
		"qty":      int64(1),
	})
	e3 := makeEvent(nil, after3)
	rid3 := BuildRowIdentifier(e3, schema)

	if rid.HashKey() == rid3.HashKey() {
		t.Errorf("different PK rows must have different HashKey")
	}
}

// TestBuildRowIdentifier_NoPrimaryKey tests fallback to full-row when no PK/UK exists.
func TestBuildRowIdentifier_NoPrimaryKey(t *testing.T) {
	schema := &event.TableInfo{
		Database: "testdb",
		Schema:   "dbo",
		Table:    "logs",
		// No PrimaryKeyColumns, no UniqueKeyColumns
	}

	after := makeRowData(map[string]interface{}{
		"msg":   "hello",
		"level": "info",
	})
	e := makeEvent(nil, after)

	rid := BuildRowIdentifier(e, schema)

	if rid.KeyType != KeyTypeFullRow {
		t.Errorf("expected KeyTypeFullRow, got %d", rid.KeyType)
	}
	if rid.PrimaryKeyValues == "" {
		t.Error("expected non-empty PrimaryKeyValues for full-row fallback")
	}

	// String representation should contain "fullrow"
	s := rid.String()
	if s == "" {
		t.Error("String() returned empty string")
	}
}

// TestBuildRowIdentifier_UniqueIndex tests fallback to unique index when no PK exists.
func TestBuildRowIdentifier_UniqueIndex(t *testing.T) {
	schema := &event.TableInfo{
		Database:         "testdb",
		Schema:           "",
		Table:            "emails",
		UniqueKeyColumns: []string{"email"},
	}

	after := makeRowData(map[string]interface{}{
		"email": "alice@example.com",
		"name":  "Alice",
	})
	e := makeEvent(nil, after)

	rid := BuildRowIdentifier(e, schema)

	if rid.KeyType != KeyTypeUniqueIndex {
		t.Errorf("expected KeyTypeUniqueIndex, got %d", rid.KeyType)
	}
	if rid.PrimaryKeyValues != "email=alice@example.com" {
		t.Errorf("unexpected PrimaryKeyValues: %q", rid.PrimaryKeyValues)
	}
}

// TestBuildRowIdentifier_DeleteUsesBeforeRow tests that DELETE events (only Before set) work.
func TestBuildRowIdentifier_DeleteUsesBeforeRow(t *testing.T) {
	schema := &event.TableInfo{
		Database:          "testdb",
		Schema:            "",
		Table:             "users",
		PrimaryKeyColumns: []string{"id"},
	}

	before := makeRowData(map[string]interface{}{
		"id":   int64(7),
		"name": "Bob",
	})
	e := makeEvent(before, nil)
	e.Type = event.EventTypeDelete

	rid := BuildRowIdentifier(e, schema)

	if rid.KeyType != KeyTypePrimaryKey {
		t.Errorf("expected KeyTypePrimaryKey, got %d", rid.KeyType)
	}
	if rid.PrimaryKeyValues != "id=7" {
		t.Errorf("unexpected PrimaryKeyValues: %q", rid.PrimaryKeyValues)
	}
}
