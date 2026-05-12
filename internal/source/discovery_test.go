package source

import (
	"testing"

	"github.com/UFOXD/datastream/pkg/parser"
)

func TestDatabaseDiscovery_HandleCreateDB(t *testing.T) {
	ch := make(chan *DiscoveryEvent, 10)
	d := NewDatabaseDiscovery(&DiscoveryConfig{
		Scope:         &DatabaseScope{Names: []string{"*"}},
		EventChannel:  ch,
		InitialDBs:    map[string]struct{}{},
		InitialTables: map[string]struct{}{},
	})

	d.HandleDDL(&DDLEvent{
		Type:     parser.DDLTypeCreateDatabase,
		Database: "newdb",
	})

	// Should emit a database-created event
	if len(ch) != 1 {
		t.Fatalf("expected 1 event, got %d", len(ch))
	}
	ev := <-ch
	if ev.Type != DiscoveryTypeDatabaseCreated {
		t.Errorf("expected event type %s, got %s", DiscoveryTypeDatabaseCreated, ev.Type)
	}
	if ev.Database != "newdb" {
		t.Errorf("expected database 'newdb', got %q", ev.Database)
	}
	if ev.Timestamp.IsZero() {
		t.Error("expected Timestamp to be set, got zero value")
	}

	// Database should now be known
	if !d.IsDatabaseKnown("newdb") {
		t.Error("expected 'newdb' to be known after CREATE DATABASE")
	}

	// Duplicate CREATE DATABASE should not emit another event
	d.HandleDDL(&DDLEvent{
		Type:     parser.DDLTypeCreateDatabase,
		Database: "newdb",
	})
	if len(ch) != 0 {
		t.Errorf("expected no extra event for duplicate CREATE DATABASE, got %d", len(ch))
	}
}

func TestDatabaseDiscovery_HandleCreateTable(t *testing.T) {
	ch := make(chan *DiscoveryEvent, 10)
	d := NewDatabaseDiscovery(&DiscoveryConfig{
		Scope:         &DatabaseScope{Names: []string{"*"}},
		EventChannel:  ch,
		InitialDBs:    map[string]struct{}{},
		InitialTables: map[string]struct{}{},
	})

	d.HandleDDL(&DDLEvent{
		Type:     parser.DDLTypeCreateTable,
		Database: "mydb",
		Table:    "users",
	})

	// Should emit a table-created event
	if len(ch) != 1 {
		t.Fatalf("expected 1 event, got %d", len(ch))
	}
	ev := <-ch
	if ev.Type != DiscoveryTypeTableCreated {
		t.Errorf("expected event type %s, got %s", DiscoveryTypeTableCreated, ev.Type)
	}
	if ev.Database != "mydb" {
		t.Errorf("expected database 'mydb', got %q", ev.Database)
	}
	if ev.Table != "users" {
		t.Errorf("expected table 'users', got %q", ev.Table)
	}
	if ev.Timestamp.IsZero() {
		t.Error("expected Timestamp to be set, got zero value")
	}

	// Table should now be known
	if !d.IsTableKnown("mydb", "users") {
		t.Error("expected 'mydb.users' to be known after CREATE TABLE")
	}
}

func TestDatabaseDiscovery_HandleDropTable(t *testing.T) {
	ch := make(chan *DiscoveryEvent, 10)
	d := NewDatabaseDiscovery(&DiscoveryConfig{
		Scope:        &DatabaseScope{Names: []string{"*"}},
		EventChannel: ch,
		InitialDBs:   map[string]struct{}{},
		InitialTables: map[string]struct{}{
			"mydb.orders": {},
		},
	})

	// Verify table is initially known
	if !d.IsTableKnown("mydb", "orders") {
		t.Fatal("expected 'mydb.orders' to be known initially")
	}

	d.HandleDDL(&DDLEvent{
		Type:     parser.DDLTypeDropTable,
		Database: "mydb",
		Table:    "orders",
	})

	// Should emit a table-dropped event
	if len(ch) != 1 {
		t.Fatalf("expected 1 event, got %d", len(ch))
	}
	ev := <-ch
	if ev.Type != DiscoveryTypeTableDropped {
		t.Errorf("expected event type %s, got %s", DiscoveryTypeTableDropped, ev.Type)
	}
	if ev.Database != "mydb" {
		t.Errorf("expected database 'mydb', got %q", ev.Database)
	}
	if ev.Table != "orders" {
		t.Errorf("expected table 'orders', got %q", ev.Table)
	}
	if ev.Timestamp.IsZero() {
		t.Error("expected Timestamp to be set, got zero value")
	}

	// Table should no longer be known
	if d.IsTableKnown("mydb", "orders") {
		t.Error("expected 'mydb.orders' to be unknown after DROP TABLE")
	}
}

func TestDatabaseDiscovery_HandleAlterTable(t *testing.T) {
	ch := make(chan *DiscoveryEvent, 10)
	d := NewDatabaseDiscovery(&DiscoveryConfig{
		Scope:        &DatabaseScope{Names: []string{"*"}},
		EventChannel: ch,
		InitialDBs:   map[string]struct{}{},
		InitialTables: map[string]struct{}{
			"mydb.users": {},
		},
	})

	d.HandleDDL(&DDLEvent{
		Type:     parser.DDLTypeAlterTable,
		Database: "mydb",
		Table:    "users",
	})

	// Should emit a table-altered event
	if len(ch) != 1 {
		t.Fatalf("expected 1 event, got %d", len(ch))
	}
	ev := <-ch
	if ev.Type != DiscoveryTypeTableAltered {
		t.Errorf("expected event type %s, got %s", DiscoveryTypeTableAltered, ev.Type)
	}
	if ev.Database != "mydb" {
		t.Errorf("expected database 'mydb', got %q", ev.Database)
	}
	if ev.Table != "users" {
		t.Errorf("expected table 'users', got %q", ev.Table)
	}
	if ev.Timestamp.IsZero() {
		t.Error("expected Timestamp to be set, got zero value")
	}
}

func TestDatabaseDiscovery_IgnoreOutOfScopeDDL(t *testing.T) {
	ch := make(chan *DiscoveryEvent, 10)
	// Scope is limited to "alloweddb" only (not wildcard)
	d := NewDatabaseDiscovery(&DiscoveryConfig{
		Scope:         &DatabaseScope{Names: []string{"alloweddb"}},
		EventChannel:  ch,
		InitialDBs:    map[string]struct{}{},
		InitialTables: map[string]struct{}{},
	})

	// CREATE DATABASE on non-wildcard scope should be ignored
	d.HandleDDL(&DDLEvent{
		Type:     parser.DDLTypeCreateDatabase,
		Database: "otherdb",
	})

	// CREATE TABLE for out-of-scope database should be ignored
	d.HandleDDL(&DDLEvent{
		Type:     parser.DDLTypeCreateTable,
		Database: "otherdb",
		Table:    "users",
	})

	if len(ch) != 0 {
		t.Errorf("expected no events for out-of-scope DDL, got %d", len(ch))
	}

	if d.IsDatabaseKnown("otherdb") {
		t.Error("out-of-scope database should not be known")
	}
	if d.IsTableKnown("otherdb", "users") {
		t.Error("out-of-scope table should not be known")
	}
}
