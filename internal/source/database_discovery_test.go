package source

import (
	"context"
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/parser"
)

// makeChangeEvent builds a minimal DDL ChangeEvent with an explicit ddl_type metadata key.
func makeChangeEvent(db, table string, ddlType parser.DDLType) *event.ChangeEvent {
	return &event.ChangeEvent{
		Type: event.EventTypeDDL,
		Table: event.TableInfo{
			Database: db,
			Table:    table,
		},
		Metadata: map[string]string{
			"ddl_type": string(ddlType),
		},
	}
}

// --- IsWildcardMode ---

func TestDatabaseDiscovery_IsWildcardMode(t *testing.T) {
	tests := []struct {
		name  string
		names []string
		want  bool
	}{
		{"wildcard single asterisk", []string{"*"}, true},
		{"single named db", []string{"mydb"}, false},
		{"multiple named dbs", []string{"db1", "db2"}, false},
		{"asterisk plus name", []string{"*", "db1"}, false},
		{"empty list", []string{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope := &SyncScope{
				Level:     SyncLevelDatabase,
				Databases: DatabaseScope{Names: tt.names},
			}
			d := NewDatabaseDiscovery(scope, nil)
			if got := d.IsWildcardMode(); got != tt.want {
				t.Errorf("IsWildcardMode() = %v, want %v", got, tt.want)
			}
		})
	}
}

// --- ShouldSyncDatabase ---

func TestDatabaseDiscovery_ShouldSyncDatabase(t *testing.T) {
	scope := &SyncScope{
		Level:     SyncLevelDatabase,
		Databases: DatabaseScope{Names: []string{"*"}},
	}
	d := NewDatabaseDiscovery(scope, nil)

	for _, db := range []string{"any_db", "another", "information_schema"} {
		if !d.ShouldSyncDatabase(db) {
			t.Errorf("ShouldSyncDatabase(%q) = false, want true in wildcard mode", db)
		}
	}
}

func TestDatabaseDiscovery_ShouldSyncDatabase_SpecificDBs(t *testing.T) {
	scope := &SyncScope{
		Level:     SyncLevelDatabase,
		Databases: DatabaseScope{Names: []string{"db1", "db2"}},
	}
	d := NewDatabaseDiscovery(scope, nil)

	if !d.ShouldSyncDatabase("db1") {
		t.Error("ShouldSyncDatabase(db1) = false, want true")
	}
	if !d.ShouldSyncDatabase("db2") {
		t.Error("ShouldSyncDatabase(db2) = false, want true")
	}
	if d.ShouldSyncDatabase("db3") {
		t.Error("ShouldSyncDatabase(db3) = true, want false")
	}
	if d.ShouldSyncDatabase("") {
		t.Error("ShouldSyncDatabase(\"\") = true, want false")
	}
}

// --- ShouldSyncTable ---

func TestDatabaseDiscovery_ShouldSyncTable(t *testing.T) {
	tests := []struct {
		name     string
		scope    *SyncScope
		db       string
		table    string
		wantSync bool
	}{
		{
			name: "wildcard, no filter: sync all tables",
			scope: &SyncScope{
				Level:     SyncLevelDatabase,
				Databases: DatabaseScope{Names: []string{"*"}},
			},
			db: "anydb", table: "anytable", wantSync: true,
		},
		{
			name: "wildcard with table filter matching",
			scope: &SyncScope{
				Level: SyncLevelDatabase,
				Databases: DatabaseScope{
					Names:       []string{"*"},
					TableFilter: []string{`.*\.users`},
				},
			},
			db: "db1", table: "users", wantSync: true,
		},
		{
			name: "wildcard with table filter not matching",
			scope: &SyncScope{
				Level: SyncLevelDatabase,
				Databases: DatabaseScope{
					Names:       []string{"*"},
					TableFilter: []string{`.*\.orders`},
				},
			},
			db: "db1", table: "users", wantSync: false,
		},
		{
			name: "specific db, table in ignore list",
			scope: &SyncScope{
				Level: SyncLevelDatabase,
				Databases: DatabaseScope{
					Names:        []string{"db1"},
					IgnoreTables: []string{`db1\.users`},
				},
			},
			db: "db1", table: "users", wantSync: false,
		},
		{
			name: "specific db, table not in ignore list",
			scope: &SyncScope{
				Level: SyncLevelDatabase,
				Databases: DatabaseScope{
					Names:        []string{"db1"},
					IgnoreTables: []string{`db1\.users`},
				},
			},
			db: "db1", table: "orders", wantSync: true,
		},
		{
			name: "db not in scope",
			scope: &SyncScope{
				Level:     SyncLevelDatabase,
				Databases: DatabaseScope{Names: []string{"db1"}},
			},
			db: "db2", table: "users", wantSync: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDatabaseDiscovery(tt.scope, nil)
			got := d.ShouldSyncTable(tt.db, tt.table)
			if got != tt.wantSync {
				t.Errorf("ShouldSyncTable(%q, %q) = %v, want %v", tt.db, tt.table, got, tt.wantSync)
			}
		})
	}
}

// --- OnDDLEvent: CREATE TABLE ---

func TestDatabaseDiscovery_OnDDLEvent_CreateTable(t *testing.T) {
	scope := &SyncScope{
		Level:     SyncLevelDatabase,
		Databases: DatabaseScope{Names: []string{"*"}},
	}
	d := NewDatabaseDiscovery(scope, nil)
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	defer d.Stop()

	ev := makeChangeEvent("mydb", "users", parser.DDLTypeCreateTable)
	if err := d.OnDDLEvent(ev); err != nil {
		t.Fatalf("OnDDLEvent() error: %v", err)
	}

	select {
	case got := <-d.Events():
		if got.Type != DiscoveryTypeTableCreated {
			t.Errorf("event type = %v, want %v", got.Type, DiscoveryTypeTableCreated)
		}
		if got.Database != "mydb" {
			t.Errorf("database = %q, want %q", got.Database, "mydb")
		}
		if got.Table != "users" {
			t.Errorf("table = %q, want %q", got.Table, "users")
		}
		if got.Timestamp.IsZero() {
			t.Error("Timestamp is zero")
		}
	default:
		t.Error("expected a DiscoveryEvent, got none")
	}

	tables := d.KnownTables()
	if len(tables) != 1 || tables[0] != "mydb.users" {
		t.Errorf("KnownTables() = %v, want [mydb.users]", tables)
	}
}

// --- OnDDLEvent: CREATE DATABASE ---

func TestDatabaseDiscovery_OnDDLEvent_CreateDatabase(t *testing.T) {
	scope := &SyncScope{
		Level:     SyncLevelDatabase,
		Databases: DatabaseScope{Names: []string{"*"}},
	}
	d := NewDatabaseDiscovery(scope, nil)
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	defer d.Stop()

	ev := makeChangeEvent("newdb", "", parser.DDLTypeCreateDatabase)
	if err := d.OnDDLEvent(ev); err != nil {
		t.Fatalf("OnDDLEvent() error: %v", err)
	}

	select {
	case got := <-d.Events():
		if got.Type != DiscoveryTypeDatabaseCreated {
			t.Errorf("event type = %v, want %v", got.Type, DiscoveryTypeDatabaseCreated)
		}
		if got.Database != "newdb" {
			t.Errorf("database = %q, want %q", got.Database, "newdb")
		}
	default:
		t.Error("expected a DiscoveryEvent, got none")
	}

	dbs := d.KnownDatabases()
	if len(dbs) != 1 || dbs[0] != "newdb" {
		t.Errorf("KnownDatabases() = %v, want [newdb]", dbs)
	}
}

// --- OnDDLEvent: DROP TABLE ---

func TestDatabaseDiscovery_OnDDLEvent_DropTable(t *testing.T) {
	scope := &SyncScope{
		Level:     SyncLevelDatabase,
		Databases: DatabaseScope{Names: []string{"*"}},
	}
	d := NewDatabaseDiscovery(scope, nil)
	// Pre-seed a table.
	d.knownTables["mydb.orders"] = struct{}{}

	ev := makeChangeEvent("mydb", "orders", parser.DDLTypeDropTable)
	if err := d.OnDDLEvent(ev); err != nil {
		t.Fatalf("OnDDLEvent() error: %v", err)
	}

	select {
	case got := <-d.Events():
		if got.Type != DiscoveryTypeTableDropped {
			t.Errorf("event type = %v, want %v", got.Type, DiscoveryTypeTableDropped)
		}
	default:
		t.Error("expected a DiscoveryEvent, got none")
	}

	if len(d.KnownTables()) != 0 {
		t.Errorf("expected no known tables after DROP, got %v", d.KnownTables())
	}
}

// --- OnDDLEvent: nil / non-DDL event ignored ---

func TestDatabaseDiscovery_OnDDLEvent_Nil(t *testing.T) {
	scope := &SyncScope{
		Level:     SyncLevelDatabase,
		Databases: DatabaseScope{Names: []string{"*"}},
	}
	d := NewDatabaseDiscovery(scope, nil)

	if err := d.OnDDLEvent(nil); err != nil {
		t.Errorf("OnDDLEvent(nil) returned error: %v", err)
	}

	nonDDL := &event.ChangeEvent{Type: event.EventTypeInsert}
	if err := d.OnDDLEvent(nonDDL); err != nil {
		t.Errorf("OnDDLEvent(non-DDL) returned error: %v", err)
	}

	if len(d.Events()) != 0 {
		t.Errorf("expected no events, got %d", len(d.Events()))
	}
}

// --- OnDDLEvent: DROP DATABASE cleans up tables ---

func TestDatabaseDiscovery_OnDDLEvent_DropDatabase_CleansUpTables(t *testing.T) {
	scope := &SyncScope{
		Level:     SyncLevelDatabase,
		Databases: DatabaseScope{Names: []string{"*"}},
	}
	d := NewDatabaseDiscovery(scope, nil)
	// Pre-seed a database and two tables belonging to it, plus a table from another db.
	d.knownDBs["mydb"] = struct{}{}
	d.knownTables["mydb.users"] = struct{}{}
	d.knownTables["mydb.orders"] = struct{}{}
	d.knownTables["otherdb.foo"] = struct{}{}

	ev := makeChangeEvent("mydb", "", parser.DDLTypeDropDatabase)
	if err := d.OnDDLEvent(ev); err != nil {
		t.Fatalf("OnDDLEvent() error: %v", err)
	}

	select {
	case got := <-d.Events():
		if got.Type != DiscoveryTypeDatabaseDropped {
			t.Errorf("event type = %v, want %v", got.Type, DiscoveryTypeDatabaseDropped)
		}
	default:
		t.Error("expected a DiscoveryEvent, got none")
	}

	if len(d.KnownDatabases()) != 0 {
		t.Errorf("expected no known databases after DROP, got %v", d.KnownDatabases())
	}
	tables := d.KnownTables()
	if len(tables) != 1 || tables[0] != "otherdb.foo" {
		t.Errorf("KnownTables() = %v, want [otherdb.foo]", tables)
	}
}

// --- Start/Stop lifecycle ---

func TestDatabaseDiscovery_StartStop(t *testing.T) {
	scope := &SyncScope{
		Level:     SyncLevelDatabase,
		Databases: DatabaseScope{Names: []string{"*"}},
	}
	d := NewDatabaseDiscovery(scope, nil)

	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	// Second Start should fail with ErrAlreadyRunning.
	if err := d.Start(ctx); err == nil {
		t.Error("second Start() should return an error")
	}

	if err := d.Stop(); err != nil {
		t.Fatalf("Stop() error: %v", err)
	}

	// Second Stop is a no-op.
	if err := d.Stop(); err != nil {
		t.Fatalf("second Stop() error: %v", err)
	}
}
