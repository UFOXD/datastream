package source

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

// mockSchemaFetcher is a test implementation of SchemaFetcher.
type mockSchemaFetcher struct {
	schemas map[string]*event.TableInfo
	err     error
}

func (m *mockSchemaFetcher) FetchSchema(_ context.Context, database, table string) (*event.TableInfo, error) {
	if m.err != nil {
		return nil, m.err
	}
	key := database + "." + table
	if schema, ok := m.schemas[key]; ok {
		return schema, nil
	}
	return &event.TableInfo{
		Database: database,
		Table:    table,
	}, nil
}

func makeEventChannel() chan *TableOperationEvent {
	return make(chan *TableOperationEvent, 10)
}

func TestTableManager_AddTables(t *testing.T) {
	eventCh := makeEventChannel()
	scope := &TableScope{}
	tm := NewTableManager(&TableManagerConfig{
		Scope:        scope,
		EventChannel: eventCh,
	})

	ctx := context.Background()
	err := tm.AddTables(ctx, []string{"db1.users", "db1.orders"})
	if err != nil {
		t.Fatalf("AddTables() error = %v", err)
	}

	// Verify tables are tracked
	if !tm.HasTable("db1", "users") {
		t.Error("HasTable(db1, users) = false, want true")
	}
	if !tm.HasTable("db1", "orders") {
		t.Error("HasTable(db1, orders) = false, want true")
	}

	// Verify events were emitted
	if len(eventCh) != 2 {
		t.Fatalf("expected 2 events, got %d", len(eventCh))
	}

	ev1 := <-eventCh
	if ev1.Operation != TableOpAdd {
		t.Errorf("event 1 operation = %q, want %q", ev1.Operation, TableOpAdd)
	}
	if ev1.TableID.Database != "db1" || ev1.TableID.Table != "users" {
		t.Errorf("event 1 tableID = %v, want db1.users", ev1.TableID)
	}
	if ev1.Timestamp.IsZero() {
		t.Error("event 1 timestamp is zero")
	}

	ev2 := <-eventCh
	if ev2.Operation != TableOpAdd {
		t.Errorf("event 2 operation = %q, want %q", ev2.Operation, TableOpAdd)
	}
	if ev2.TableID.Database != "db1" || ev2.TableID.Table != "orders" {
		t.Errorf("event 2 tableID = %v, want db1.orders", ev2.TableID)
	}

	// Verify scope was updated
	if len(scope.Names) != 2 {
		t.Errorf("scope.Names = %v, want 2 entries", scope.Names)
	}

	// Adding duplicate should fail
	err = tm.AddTables(ctx, []string{"db1.users"})
	if err == nil {
		t.Error("AddTables() with duplicate should return error")
	}
}

func TestTableManager_AddTables_WithSchemaFetcher(t *testing.T) {
	fetcher := &mockSchemaFetcher{
		schemas: map[string]*event.TableInfo{
			"db1.users": {
				Database:          "db1",
				Table:             "users",
				PrimaryKeyColumns: []string{"id"},
			},
		},
	}
	eventCh := makeEventChannel()
	tm := NewTableManager(&TableManagerConfig{
		SchemaFetcher: fetcher,
		EventChannel:  eventCh,
	})

	ctx := context.Background()
	err := tm.AddTables(ctx, []string{"db1.users"})
	if err != nil {
		t.Fatalf("AddTables() error = %v", err)
	}

	state, err := tm.GetTableState("db1", "users")
	if err != nil {
		t.Fatalf("GetTableState() error = %v", err)
	}
	if state.Schema == nil {
		t.Fatal("expected schema to be fetched, got nil")
	}
	if state.Schema.Database != "db1" || state.Schema.Table != "users" {
		t.Errorf("schema = %v, want db1.users", state.Schema)
	}

	// Verify event carries schema
	ev := <-eventCh
	if ev.Schema == nil {
		t.Error("event schema should not be nil when schema fetcher is present")
	}
}

func TestTableManager_RemoveTables(t *testing.T) {
	eventCh := makeEventChannel()
	scope := &TableScope{
		Names: []string{"db1.users", "db1.orders", "db2.products"},
	}
	tm := NewTableManager(&TableManagerConfig{
		Scope:        scope,
		EventChannel: eventCh,
	})

	ctx := context.Background()

	// Remove one table
	err := tm.RemoveTables(ctx, []string{"db1.orders"})
	if err != nil {
		t.Fatalf("RemoveTables() error = %v", err)
	}

	if tm.HasTable("db1", "orders") {
		t.Error("HasTable(db1, orders) = true after removal, want false")
	}
	if !tm.HasTable("db1", "users") {
		t.Error("HasTable(db1, users) = false, want true")
	}
	if !tm.HasTable("db2", "products") {
		t.Error("HasTable(db2, products) = false, want true")
	}

	// Verify remove event
	if len(eventCh) != 1 {
		t.Fatalf("expected 1 event, got %d", len(eventCh))
	}
	ev := <-eventCh
	if ev.Operation != TableOpRemove {
		t.Errorf("event operation = %q, want %q", ev.Operation, TableOpRemove)
	}
	if ev.TableID.Database != "db1" || ev.TableID.Table != "orders" {
		t.Errorf("event tableID = %v, want db1.orders", ev.TableID)
	}

	// Verify scope was updated
	for _, name := range scope.Names {
		if name == "db1.orders" {
			t.Error("scope.Names still contains db1.orders after removal")
		}
	}

	// Removing non-existent table should fail
	err = tm.RemoveTables(ctx, []string{"db1.orders"})
	if err == nil {
		t.Error("RemoveTables() non-existent table should return error")
	}
}

func TestTableManager_ListTables(t *testing.T) {
	scope := &TableScope{
		Names: []string{"db1.users", "db1.orders", "db2.products"},
	}
	tm := NewTableManager(&TableManagerConfig{
		Scope: scope,
	})

	tables := tm.ListTables()
	if len(tables) != 3 {
		t.Fatalf("ListTables() returned %d tables, want 3", len(tables))
	}

	sort.Strings(tables)
	want := []string{"db1.orders", "db1.users", "db2.products"}
	for i, got := range tables {
		if got != want[i] {
			t.Errorf("tables[%d] = %q, want %q", i, got, want[i])
		}
	}

	// After adding one more
	ctx := context.Background()
	_ = tm.AddTables(ctx, []string{"db3.events"})
	tables = tm.ListTables()
	if len(tables) != 4 {
		t.Errorf("ListTables() returned %d tables after add, want 4", len(tables))
	}

	// After removing one
	_ = tm.RemoveTables(ctx, []string{"db1.users"})
	tables = tm.ListTables()
	if len(tables) != 3 {
		t.Errorf("ListTables() returned %d tables after remove, want 3", len(tables))
	}
}

func TestTableManager_GetTableState(t *testing.T) {
	scope := &TableScope{
		Names: []string{"db1.users"},
	}
	tm := NewTableManager(&TableManagerConfig{
		Scope: scope,
	})

	// Get state for existing table
	state, err := tm.GetTableState("db1", "users")
	if err != nil {
		t.Fatalf("GetTableState() error = %v", err)
	}
	if state.TableID.Database != "db1" || state.TableID.Table != "users" {
		t.Errorf("state.TableID = %v, want db1.users", state.TableID)
	}
	if state.Status != TableStatusPending {
		t.Errorf("state.Status = %q, want %q", state.Status, TableStatusPending)
	}
	if state.AddedAt.IsZero() {
		t.Error("state.AddedAt is zero")
	}

	// Get state for non-existent table
	_, err = tm.GetTableState("db1", "nonexistent")
	if err == nil {
		t.Error("GetTableState() non-existent table should return error")
	}
}

func TestTableManager_UpdateTableStatus(t *testing.T) {
	scope := &TableScope{
		Names: []string{"db1.users"},
	}
	tm := NewTableManager(&TableManagerConfig{
		Scope: scope,
	})

	err := tm.UpdateTableStatus("db1", "users", TableStatusSnapshot)
	if err != nil {
		t.Fatalf("UpdateTableStatus() error = %v", err)
	}

	state, _ := tm.GetTableState("db1", "users")
	if state.Status != TableStatusSnapshot {
		t.Errorf("status = %q, want %q", state.Status, TableStatusSnapshot)
	}

	// Transition to streaming sets SyncStarted
	err = tm.UpdateTableStatus("db1", "users", TableStatusStreaming)
	if err != nil {
		t.Fatalf("UpdateTableStatus() error = %v", err)
	}
	state, _ = tm.GetTableState("db1", "users")
	if state.SyncStarted.IsZero() {
		t.Error("SyncStarted should be set when status becomes streaming")
	}

	// Updating non-existent table
	err = tm.UpdateTableStatus("db1", "nonexistent", TableStatusPending)
	if err == nil {
		t.Error("UpdateTableStatus() non-existent table should return error")
	}
}

func TestTableID_String(t *testing.T) {
	tests := []struct {
		id   TableID
		want string
	}{
		{
			id:   TableID{Database: "db1", Table: "users"},
			want: "db1.users",
		},
		{
			id:   TableID{Database: "db1", Schema: "public", Table: "users"},
			want: "db1.public.users",
		},
	}

	for _, tt := range tests {
		got := tt.id.String()
		if got != tt.want {
			t.Errorf("TableID.String() = %q, want %q", got, tt.want)
		}
	}
}

func TestTableManager_NewTableManager_PrePopulatesScope(t *testing.T) {
	scope := &TableScope{
		Names: []string{"db1.users", "db2.products"},
	}
	tm := NewTableManager(&TableManagerConfig{
		Scope: scope,
	})

	if !tm.HasTable("db1", "users") {
		t.Error("HasTable(db1, users) = false, expected pre-populated from scope")
	}
	if !tm.HasTable("db2", "products") {
		t.Error("HasTable(db2, products) = false, expected pre-populated from scope")
	}

	tables := tm.ListTables()
	if len(tables) != 2 {
		t.Errorf("ListTables() = %d, want 2", len(tables))
	}
}

func TestTableManager_AddTables_InvalidFormat(t *testing.T) {
	tm := NewTableManager(&TableManagerConfig{})
	ctx := context.Background()

	err := tm.AddTables(ctx, []string{"invalid"})
	if err == nil {
		t.Error("AddTables() with invalid name should return error")
	}

	err = tm.AddTables(ctx, []string{".table"})
	if err == nil {
		t.Error("AddTables() with empty database should return error")
	}

	err = tm.AddTables(ctx, []string{"db."})
	if err == nil {
		t.Error("AddTables() with empty table should return error")
	}
}

func TestTableManager_PauseTable(t *testing.T) {
	eventCh := makeEventChannel()
	scope := &TableScope{
		Names: []string{"db1.users"},
	}
	tm := NewTableManager(&TableManagerConfig{
		Scope:        scope,
		EventChannel: eventCh,
	})

	ctx := context.Background()

	// Pause existing table
	err := tm.PauseTable(ctx, "db1", "users")
	if err != nil {
		t.Fatalf("PauseTable() error = %v", err)
	}

	state, err := tm.GetTableState("db1", "users")
	if err != nil {
		t.Fatalf("GetTableState() error = %v", err)
	}
	if state.Status != TableStatusPaused {
		t.Errorf("status = %q, want %q", state.Status, TableStatusPaused)
	}
	if state.PausedAt.IsZero() {
		t.Error("PausedAt should be set after pausing")
	}

	// Verify event
	if len(eventCh) != 1 {
		t.Fatalf("expected 1 event, got %d", len(eventCh))
	}
	ev := <-eventCh
	if ev.Operation != TableOpPause {
		t.Errorf("event operation = %q, want %q", ev.Operation, TableOpPause)
	}
	if ev.TableID.Database != "db1" || ev.TableID.Table != "users" {
		t.Errorf("event tableID = %v, want db1.users", ev.TableID)
	}
	if ev.Timestamp.IsZero() {
		t.Error("event timestamp is zero")
	}

	// Pause non-existent table should fail
	err = tm.PauseTable(ctx, "db1", "nonexistent")
	if err == nil {
		t.Error("PauseTable() non-existent table should return error")
	}
}

func TestTableManager_ResumeTable(t *testing.T) {
	eventCh := makeEventChannel()
	scope := &TableScope{
		Names: []string{"db1.users"},
	}
	tm := NewTableManager(&TableManagerConfig{
		Scope:        scope,
		EventChannel: eventCh,
	})

	ctx := context.Background()

	// First pause the table
	err := tm.PauseTable(ctx, "db1", "users")
	if err != nil {
		t.Fatalf("PauseTable() error = %v", err)
	}
	<-eventCh // drain pause event

	// Resume the table
	err = tm.ResumeTable(ctx, "db1", "users")
	if err != nil {
		t.Fatalf("ResumeTable() error = %v", err)
	}

	state, err := tm.GetTableState("db1", "users")
	if err != nil {
		t.Fatalf("GetTableState() error = %v", err)
	}
	if state.Status != TableStatusPending {
		t.Errorf("status = %q, want %q", state.Status, TableStatusPending)
	}
	if !state.PausedAt.IsZero() {
		t.Error("PausedAt should be cleared after resuming")
	}

	// Verify event
	if len(eventCh) != 1 {
		t.Fatalf("expected 1 event, got %d", len(eventCh))
	}
	ev := <-eventCh
	if ev.Operation != TableOpResume {
		t.Errorf("event operation = %q, want %q", ev.Operation, TableOpResume)
	}
	if ev.TableID.Database != "db1" || ev.TableID.Table != "users" {
		t.Errorf("event tableID = %v, want db1.users", ev.TableID)
	}
	if ev.Timestamp.IsZero() {
		t.Error("event timestamp is zero")
	}

	// Resume non-existent table should fail
	err = tm.ResumeTable(ctx, "db1", "nonexistent")
	if err == nil {
		t.Error("ResumeTable() non-existent table should return error")
	}
}

func TestTableManager_EventTimestamp(t *testing.T) {
	eventCh := makeEventChannel()
	tm := NewTableManager(&TableManagerConfig{
		EventChannel: eventCh,
	})

	before := time.Now()
	ctx := context.Background()
	_ = tm.AddTables(ctx, []string{"db1.users"})
	after := time.Now()

	ev := <-eventCh
	if ev.Timestamp.Before(before) || ev.Timestamp.After(after) {
		t.Errorf("event timestamp %v is not between %v and %v", ev.Timestamp, before, after)
	}
}
