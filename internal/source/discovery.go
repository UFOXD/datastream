// Package source defines the source connector interfaces for DataStream.
package source

import "sync"

// DDLType represents DDL event types.
type DDLType string

const (
	DDLTypeCreateDB    DDLType = "CREATE_DATABASE"
	DDLTypeDropDB      DDLType = "DROP_DATABASE"
	DDLTypeCreateTable DDLType = "CREATE_TABLE"
	DDLTypeDropTable   DDLType = "DROP_TABLE"
	DDLTypeAlterTable  DDLType = "ALTER_TABLE"
)

// DDLEvent represents a DDL event.
type DDLEvent struct {
	Database string
	Table    string
	Type     DDLType
}

// DiscoveryType represents discovery event types.
type DiscoveryType string

const (
	DiscoveryTypeDatabaseCreated DiscoveryType = "database_created"
	DiscoveryTypeDatabaseDropped DiscoveryType = "database_dropped"
	DiscoveryTypeTableCreated    DiscoveryType = "table_created"
	DiscoveryTypeTableDropped    DiscoveryType = "table_dropped"
)

// DiscoveryEvent represents a discovery event.
type DiscoveryEvent struct {
	Type     DiscoveryType
	Database string
	Table    string
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

// DatabaseDiscovery handles auto-discovery of databases and tables in wildcard mode.
type DatabaseDiscovery struct {
	config      *DiscoveryConfig
	knownDBs    map[string]struct{}
	knownTables map[string]struct{}
	mu          sync.RWMutex
}

// NewDatabaseDiscovery creates a new DatabaseDiscovery with the given configuration.
func NewDatabaseDiscovery(cfg *DiscoveryConfig) *DatabaseDiscovery {
	d := &DatabaseDiscovery{
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
func (d *DatabaseDiscovery) HandleDDL(event *DDLEvent) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.handleDDL(event)
}

// handleDDL is the internal (unlocked) handler for DDL events.
func (d *DatabaseDiscovery) handleDDL(event *DDLEvent) {
	scope := d.config.Scope

	switch event.Type {
	case DDLTypeCreateDB:
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

	case DDLTypeDropDB:
		if _, known := d.knownDBs[event.Database]; known {
			delete(d.knownDBs, event.Database)
			d.emit(&DiscoveryEvent{
				Type:     DiscoveryTypeDatabaseDropped,
				Database: event.Database,
			})
		}

	case DDLTypeCreateTable:
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

	case DDLTypeDropTable:
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
	}
}

// emit sends a discovery event on the event channel (non-blocking).
func (d *DatabaseDiscovery) emit(event *DiscoveryEvent) {
	if d.config.EventChannel == nil {
		return
	}
	select {
	case d.config.EventChannel <- event:
	default:
	}
}

// IsDatabaseKnown returns true if the database has been discovered.
func (d *DatabaseDiscovery) IsDatabaseKnown(db string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.knownDBs[db]
	return ok
}

// IsTableKnown returns true if the table (in the given database) has been discovered.
func (d *DatabaseDiscovery) IsTableKnown(db, table string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.knownTables[db+"."+table]
	return ok
}
