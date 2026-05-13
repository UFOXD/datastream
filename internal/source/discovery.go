// Package source defines the source connector interfaces for DataStream.
package source

import (
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/parser"
)

// DDLEvent represents a DDL event from a source connector.
type DDLEvent struct {
	Database string
	Table    string
	Type     parser.DDLType
}

// DiscoveryType represents discovery event types.
type DiscoveryType string

const (
	DiscoveryTypeDatabaseCreated DiscoveryType = "database-created"
	DiscoveryTypeDatabaseDropped DiscoveryType = "database-dropped"
	DiscoveryTypeTableCreated    DiscoveryType = "table-created"
	DiscoveryTypeTableDropped    DiscoveryType = "table-dropped"
	DiscoveryTypeTableAltered    DiscoveryType = "table-altered"
)

// DiscoveryEvent represents a discovery event.
type DiscoveryEvent struct {
	Type      DiscoveryType
	Database  string
	Table     string
	Timestamp time.Time
	Schema    *parser.TableInfo
}

// DiscoveryConfig holds configuration for DatabaseDiscovery.
type DiscoveryConfig struct {
	// Scope defines which databases/tables to discover.
	Scope *DatabaseScope

	// EventChannel receives discovery events.
	EventChannel chan *DiscoveryEvent

	// InitialDBs is the set of already-known databases at startup.
	InitialDBs map[string]struct{}

	// InitialTables is the set of already-known tables at startup (key: "db.table").
	InitialTables map[string]struct{}
}

// DDLDiscovery handles auto-discovery of databases and tables in wildcard mode
// by processing raw DDL events (no connector dependency).
type DDLDiscovery struct {
	config      *DiscoveryConfig
	knownDBs    map[string]struct{}
	knownTables map[string]struct{}
	mu          sync.RWMutex
}

// NewDDLDiscovery creates a new DDLDiscovery with the given configuration.
func NewDDLDiscovery(cfg *DiscoveryConfig) *DDLDiscovery {
	d := &DDLDiscovery{
		config:      cfg,
		knownDBs:    make(map[string]struct{}),
		knownTables: make(map[string]struct{}),
	}

	// Seed with initial known databases
	for db := range cfg.InitialDBs {
		d.knownDBs[db] = struct{}{}
	}

	// Seed with initial known tables
	for table := range cfg.InitialTables {
		d.knownTables[table] = struct{}{}
	}

	return d
}

// HandleDDL processes a DDL event and emits discovery events as appropriate.
func (d *DDLDiscovery) HandleDDL(event *DDLEvent) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.handleDDL(event)
}

// handleDDL is the internal (unlocked) handler for DDL events.
func (d *DDLDiscovery) handleDDL(event *DDLEvent) {
	scope := d.config.Scope

	switch event.Type {
	case parser.DDLTypeCreateDatabase:
		// Only handle in wildcard mode
		if !scope.IsWildcardDatabase() {
			return
		}
		if _, known := d.knownDBs[event.Database]; !known {
			d.knownDBs[event.Database] = struct{}{}
			d.emit(&DiscoveryEvent{
				Type:     DiscoveryTypeDatabaseCreated,
				Database: event.Database,
			})
		}

	case parser.DDLTypeDropDatabase:
		if _, known := d.knownDBs[event.Database]; known {
			delete(d.knownDBs, event.Database)
			d.emit(&DiscoveryEvent{
				Type:     DiscoveryTypeDatabaseDropped,
				Database: event.Database,
			})
		}

	case parser.DDLTypeCreateTable:
		// Check if database is in scope
		if !scope.ShouldSyncDatabase(event.Database) {
			return
		}
		key := event.Database + "." + event.Table
		if _, known := d.knownTables[key]; !known {
			d.knownTables[key] = struct{}{}
			d.emit(&DiscoveryEvent{
				Type:     DiscoveryTypeTableCreated,
				Database: event.Database,
				Table:    event.Table,
			})
		}

	case parser.DDLTypeDropTable:
		// Check if database is in scope
		if !scope.ShouldSyncDatabase(event.Database) {
			return
		}
		key := event.Database + "." + event.Table
		if _, known := d.knownTables[key]; known {
			delete(d.knownTables, key)
			d.emit(&DiscoveryEvent{
				Type:     DiscoveryTypeTableDropped,
				Database: event.Database,
				Table:    event.Table,
			})
		}

	case parser.DDLTypeAlterTable:
		// Check if database is in scope
		if !scope.ShouldSyncDatabase(event.Database) {
			return
		}
		d.emit(&DiscoveryEvent{
			Type:     DiscoveryTypeTableAltered,
			Database: event.Database,
			Table:    event.Table,
		})
	}
}

// emit sends a discovery event on the event channel (non-blocking).
func (d *DDLDiscovery) emit(event *DiscoveryEvent) {
	if d.config.EventChannel == nil {
		return
	}
	event.Timestamp = time.Now()
	select {
	case d.config.EventChannel <- event:
	default:
	}
}

// IsDatabaseKnown returns true if the database has been discovered.
func (d *DDLDiscovery) IsDatabaseKnown(db string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.knownDBs[db]
	return ok
}

// IsTableKnown returns true if the table (in the given database) has been discovered.
func (d *DDLDiscovery) IsTableKnown(db, table string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.knownTables[db+"."+table]
	return ok
}
