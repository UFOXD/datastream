package source

import (
	"context"
	"testing"
)

func TestMemoryLifecycleStoreBasicCRUD(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryLifecycleStore()
	taskID := "task-1"

	tid := TableID{Database: "mydb", Table: "users"}
	lc := NewTableLifecycle(tid)

	// Save
	if err := store.Save(ctx, taskID, lc); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Get
	got, err := store.Get(ctx, taskID, tid)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.TableID != tid {
		t.Errorf("Get returned wrong TableID: got %v, want %v", got.TableID, tid)
	}
	if got.GetState() != TableStatePending {
		t.Errorf("Get returned wrong state: got %v, want %v", got.GetState(), TableStatePending)
	}

	// List
	list, err := store.List(ctx, taskID)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("List returned %d items, want 1", len(list))
	}

	// Save another
	tid2 := TableID{Database: "mydb", Table: "orders"}
	lc2 := NewTableLifecycle(tid2)
	if err := store.Save(ctx, taskID, lc2); err != nil {
		t.Fatalf("Save second failed: %v", err)
	}

	list, err = store.List(ctx, taskID)
	if err != nil {
		t.Fatalf("List after second save failed: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("List returned %d items, want 2", len(list))
	}

	// Delete
	if err := store.Delete(ctx, taskID, tid); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	list, err = store.List(ctx, taskID)
	if err != nil {
		t.Fatalf("List after delete failed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("List returned %d items after delete, want 1", len(list))
	}

	// Verify deleted item is gone
	_, err = store.Get(ctx, taskID, tid)
	if err == nil {
		t.Fatal("Get deleted item should return error, got nil")
	}
}

func TestMemoryLifecycleStoreGetNotFound(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryLifecycleStore()

	// Non-existent task
	_, err := store.Get(ctx, "no-task", TableID{Database: "db", Table: "t"})
	if err == nil {
		t.Fatal("Get with non-existent task should return error")
	}

	// Existing task, non-existent table
	taskID := "task-1"
	lc := NewTableLifecycle(TableID{Database: "db", Table: "exists"})
	if err := store.Save(ctx, taskID, lc); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	_, err = store.Get(ctx, taskID, TableID{Database: "db", Table: "missing"})
	if err == nil {
		t.Fatal("Get with non-existent table should return error")
	}
}

func TestMemoryLifecycleStoreListByState(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryLifecycleStore()
	taskID := "task-1"

	// Table 1: pending (default)
	lc1 := NewTableLifecycle(TableID{Database: "db", Table: "t1"})
	if err := store.Save(ctx, taskID, lc1); err != nil {
		t.Fatalf("Save t1 failed: %v", err)
	}

	// Table 2: error
	lc2 := NewTableLifecycle(TableID{Database: "db", Table: "t2"})
	lc2.SetError("something broke")
	if err := store.Save(ctx, taskID, lc2); err != nil {
		t.Fatalf("Save t2 failed: %v", err)
	}

	// Table 3: error
	lc3 := NewTableLifecycle(TableID{Database: "db", Table: "t3"})
	lc3.SetError("another error")
	if err := store.Save(ctx, taskID, lc3); err != nil {
		t.Fatalf("Save t3 failed: %v", err)
	}

	// Filter by error state
	errorTables, err := store.ListByState(ctx, taskID, TableStateError)
	if err != nil {
		t.Fatalf("ListByState failed: %v", err)
	}
	if len(errorTables) != 2 {
		t.Fatalf("ListByState(error) returned %d items, want 2", len(errorTables))
	}

	// Filter by pending state
	pendingTables, err := store.ListByState(ctx, taskID, TableStatePending)
	if err != nil {
		t.Fatalf("ListByState(pending) failed: %v", err)
	}
	if len(pendingTables) != 1 {
		t.Fatalf("ListByState(pending) returned %d items, want 1", len(pendingTables))
	}
	if pendingTables[0].TableID.Table != "t1" {
		t.Errorf("ListByState(pending) returned wrong table: got %v, want t1", pendingTables[0].TableID.Table)
	}

	// Filter by streaming (none)
	streamTables, err := store.ListByState(ctx, taskID, TableStateStreaming)
	if err != nil {
		t.Fatalf("ListByState(streaming) failed: %v", err)
	}
	if len(streamTables) != 0 {
		t.Fatalf("ListByState(streaming) returned %d items, want 0", len(streamTables))
	}
}

func TestMemoryLifecycleStoreListBySchema(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryLifecycleStore()
	taskID := "task-1"

	// db1 tables
	lc1 := NewTableLifecycle(TableID{Database: "db1", Table: "users"})
	lc2 := NewTableLifecycle(TableID{Database: "db1", Table: "orders"})
	// db2 table
	lc3 := NewTableLifecycle(TableID{Database: "db2", Table: "products"})

	for _, lc := range []*TableLifecycle{lc1, lc2, lc3} {
		if err := store.Save(ctx, taskID, lc); err != nil {
			t.Fatalf("Save failed: %v", err)
		}
	}

	// Filter by db1
	db1Tables, err := store.ListBySchema(ctx, taskID, "db1")
	if err != nil {
		t.Fatalf("ListBySchema(db1) failed: %v", err)
	}
	if len(db1Tables) != 2 {
		t.Fatalf("ListBySchema(db1) returned %d items, want 2", len(db1Tables))
	}

	// Filter by db2
	db2Tables, err := store.ListBySchema(ctx, taskID, "db2")
	if err != nil {
		t.Fatalf("ListBySchema(db2) failed: %v", err)
	}
	if len(db2Tables) != 1 {
		t.Fatalf("ListBySchema(db2) returned %d items, want 1", len(db2Tables))
	}
	if db2Tables[0].TableID.Table != "products" {
		t.Errorf("ListBySchema(db2) returned wrong table: got %v, want products", db2Tables[0].TableID.Table)
	}

	// Filter by non-existent schema
	noTables, err := store.ListBySchema(ctx, taskID, "db_none")
	if err != nil {
		t.Fatalf("ListBySchema(db_none) failed: %v", err)
	}
	if len(noTables) != 0 {
		t.Fatalf("ListBySchema(db_none) returned %d items, want 0", len(noTables))
	}
}
