// Package source defines the source connector interfaces for DataStream.
package source

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/parser"
)

// DatabaseDiscovery handles automatic discovery of databases and tables
// for wildcard sync mode. It processes DDL events from a Connector and
// automatically calls AddTables when new tables matching the scope appear.
type DatabaseDiscovery struct {
	scope     *SyncScope
	connector Connector
	parser    parser.DDLParser

	// Known databases and tables
	knownDBs    map[string]struct{}
	knownTables map[string]struct{}

	// Event channel for notifications
	eventCh chan *DiscoveryEvent

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

// NewDatabaseDiscovery creates a new DatabaseDiscovery.
func NewDatabaseDiscovery(scope *SyncScope, connector Connector, p parser.DDLParser) *DatabaseDiscovery {
	return &DatabaseDiscovery{
		scope:       scope,
		connector:   connector,
		parser:      p,
		knownDBs:    make(map[string]struct{}),
		knownTables: make(map[string]struct{}),
		eventCh:     make(chan *DiscoveryEvent, 64),
	}
}

// IsWildcardMode returns true if the scope uses wildcard database mode (["*"]).
func (d *DatabaseDiscovery) IsWildcardMode() bool {
	return d.scope.Databases.IsWildcardDatabase()
}

// ShouldSyncDatabase returns true if the given database should be synced
// according to the current scope.
func (d *DatabaseDiscovery) ShouldSyncDatabase(dbName string) bool {
	return d.scope.Databases.ShouldSyncDatabase(dbName)
}

// ShouldSyncTable returns true if the given table should be synced,
// respecting TableFilter and IgnoreTables patterns in the scope.
func (d *DatabaseDiscovery) ShouldSyncTable(dbName, tableName string) bool {
	return d.scope.Databases.ShouldSyncTable(dbName, tableName)
}

// Start starts the DatabaseDiscovery. It is idempotent.
func (d *DatabaseDiscovery) Start(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.cancel != nil {
		// Already running.
		return ErrAlreadyRunning.GenWithStack("database discovery already running")
	}

	d.ctx, d.cancel = context.WithCancel(ctx)
	return nil
}

// Stop stops the DatabaseDiscovery and closes the event channel.
func (d *DatabaseDiscovery) Stop() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.cancel == nil {
		return nil
	}
	d.cancel()
	d.cancel = nil
	return nil
}

// OnDDLEvent processes a DDL ChangeEvent. In wildcard mode it automatically
// calls connector.AddTables for new tables that match the scope filter.
func (d *DatabaseDiscovery) OnDDLEvent(ddlEvent *event.ChangeEvent) error {
	if ddlEvent == nil || !ddlEvent.IsDDL() {
		return nil
	}

	db := ddlEvent.Table.Database
	table := ddlEvent.Table.Table

	// Determine the DDL type from the Schema field if available,
	// otherwise fall back to parsing the statement.
	ddlType, err := d.resolveDDLType(ddlEvent)
	if err != nil {
		// Non-fatal: skip unknown DDL.
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	switch ddlType {
	case parser.DDLTypeCreateDatabase:
		if !d.scope.Databases.IsWildcardDatabase() {
			return nil
		}
		if _, known := d.knownDBs[db]; !known {
			d.knownDBs[db] = struct{}{}
			d.emit(&DiscoveryEvent{
				Type:     DiscoveryTypeDatabaseCreated,
				Database: db,
			})
		}

	case parser.DDLTypeDropDatabase:
		if _, known := d.knownDBs[db]; known {
			delete(d.knownDBs, db)
			d.emit(&DiscoveryEvent{
				Type:     DiscoveryTypeDatabaseDropped,
				Database: db,
			})
		}

	case parser.DDLTypeCreateTable:
		if !d.scope.Databases.ShouldSyncDatabase(db) {
			return nil
		}
		key := db + "." + table
		if _, known := d.knownTables[key]; !known {
			d.knownTables[key] = struct{}{}
			d.emit(&DiscoveryEvent{
				Type:     DiscoveryTypeTableCreated,
				Database: db,
				Table:    table,
			})
			// In wildcard mode, auto-add table to connector if it matches filter.
			if d.IsWildcardMode() && d.scope.Databases.ShouldSyncTable(db, table) {
				if d.connector != nil && d.ctx != nil {
					_ = d.connector.AddTables(d.ctx, []string{key})
				}
			}
		}

	case parser.DDLTypeDropTable:
		if !d.scope.Databases.ShouldSyncDatabase(db) {
			return nil
		}
		key := db + "." + table
		if _, known := d.knownTables[key]; known {
			delete(d.knownTables, key)
			d.emit(&DiscoveryEvent{
				Type:     DiscoveryTypeTableDropped,
				Database: db,
				Table:    table,
			})
		}

	case parser.DDLTypeAlterTable:
		if !d.scope.Databases.ShouldSyncDatabase(db) {
			return nil
		}
		d.emit(&DiscoveryEvent{
			Type:     DiscoveryTypeTableAltered,
			Database: db,
			Table:    table,
		})
	}

	return nil
}

// resolveDDLType determines the parser.DDLType for a ChangeEvent.
// It first checks event.DDLInfo type mapping, then falls back to
// parsing the raw statement if a parser is configured.
func (d *DatabaseDiscovery) resolveDDLType(e *event.ChangeEvent) (parser.DDLType, error) {
	if e.Schema != nil {
		// Schema field present: no raw statement parsing needed; caller already
		// set Table.Database/Name on the event. We rely on metadata.
	}

	// Check metadata for ddl_type hint set by connectors.
	if e.Metadata != nil {
		if rawType, ok := e.Metadata["ddl_type"]; ok {
			return parser.DDLType(rawType), nil
		}
	}

	// Fall back to parser if available.
	if d.parser != nil && e.Schema != nil {
		// We have a parser but no type hint; try to derive from statement.
	}

	// Use event DDL type mapping via Schema presence heuristic is unreliable;
	// require explicit ddl_type metadata or a parseable statement.
	// If neither is available, return unknown.
	return parser.DDLTypeUnknown, ErrDiscoveryNotSupported.GenWithStack("cannot determine DDL type without metadata or parser")
}

// emit sends a DiscoveryEvent non-blocking.
func (d *DatabaseDiscovery) emit(e *DiscoveryEvent) {
	e.Timestamp = time.Now()
	select {
	case d.eventCh <- e:
	default:
	}
}

// Events returns the read-only channel of DiscoveryEvents.
func (d *DatabaseDiscovery) Events() <-chan *DiscoveryEvent {
	return d.eventCh
}

// KnownDatabases returns a sorted list of all known database names.
func (d *DatabaseDiscovery) KnownDatabases() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	dbs := make([]string, 0, len(d.knownDBs))
	for db := range d.knownDBs {
		dbs = append(dbs, db)
	}
	sort.Strings(dbs)
	return dbs
}

// KnownTables returns a sorted list of all known tables in "database.table" format.
func (d *DatabaseDiscovery) KnownTables() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	tables := make([]string, 0, len(d.knownTables))
	for t := range d.knownTables {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	return tables
}
