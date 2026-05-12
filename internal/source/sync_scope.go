// Package source defines the source connector interfaces for DataStream.
package source

import (
	"regexp"
	"strings"
)

// SyncScope defines the synchronization scope for a source connector.
// It supports two levels: Database level and Table level.
type SyncScope struct {
	// Level specifies the sync level: "database" or "table"
	Level SyncLevel `json:"level" toml:"level"`

	// Databases holds database-level sync configuration
	Databases DatabaseScope `json:"databases" toml:"databases"`

	// Tables holds table-level sync configuration
	Tables TableScope `json:"tables" toml:"tables"`
}

// SyncLevel defines the synchronization level.
type SyncLevel string

const (
	// SyncLevelDatabase syncs at database level
	SyncLevelDatabase SyncLevel = "database"
	// SyncLevelTable syncs at table level
	SyncLevelTable SyncLevel = "table"
)

// DatabaseScope holds database-level synchronization configuration.
type DatabaseScope struct {
	// Names is the list of databases to sync.
	// Three modes supported:
	// 1. ["db1"] - single database
	// 2. ["db1", "db2", "db3"] - multiple databases
	// 3. ["*"] - all databases (wildcard mode with auto-discovery)
	Names []string `json:"names" toml:"names"`

	// EnableDDL enables DDL synchronization
	EnableDDL bool `json:"enable-ddl" toml:"enable-ddl"`

	// TableFilter is a list of regex patterns to filter tables
	// Empty means sync all tables
	TableFilter []string `json:"table-filter" toml:"table-filter"`

	// IgnoreTables is a list of regex patterns for tables to ignore
	IgnoreTables []string `json:"ignore-tables" toml:"ignore-tables"`

	// Compiled patterns (internal use)
	tableFilterPatterns   []*regexp.Regexp
	ignoreTablePatterns   []*regexp.Regexp
	compiled              bool
}

// TableScope holds table-level synchronization configuration.
type TableScope struct {
	// Names is the list of tables to sync, format: "database.table"
	// Example: ["db1.users", "db1.orders", "db2.products"]
	Names []string `json:"names" toml:"names"`

	// EnableDDL enables DDL synchronization
	EnableDDL bool `json:"enable-ddl" toml:"enable-ddl"`
}

// IsWildcardDatabase returns true if using wildcard mode (sync all databases).
func (d *DatabaseScope) IsWildcardDatabase() bool {
	return len(d.Names) == 1 && d.Names[0] == "*"
}

// compilePatterns compiles regex patterns for filtering.
func (d *DatabaseScope) compilePatterns() error {
	if d.compiled {
		return nil
	}

	// Compile table filter patterns
	for _, pattern := range d.TableFilter {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return err
		}
		d.tableFilterPatterns = append(d.tableFilterPatterns, re)
	}

	// Compile ignore table patterns
	for _, pattern := range d.IgnoreTables {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return err
		}
		d.ignoreTablePatterns = append(d.ignoreTablePatterns, re)
	}

	d.compiled = true
	return nil
}

// ShouldSyncDatabase checks if a database should be synchronized.
func (d *DatabaseScope) ShouldSyncDatabase(dbName string) bool {
	// Wildcard mode: sync all databases
	if d.IsWildcardDatabase() {
		return true
	}

	// Check against specified database list
	for _, name := range d.Names {
		if name == dbName {
			return true
		}
	}
	return false
}

// ShouldSyncTable checks if a table should be synchronized.
func (d *DatabaseScope) ShouldSyncTable(dbName, tableName string) bool {
	// First check if database is in scope
	if !d.ShouldSyncDatabase(dbName) {
		return false
	}

	// Compile patterns if needed
	if !d.compiled {
		if err := d.compilePatterns(); err != nil {
			return false
		}
	}

	// Check ignore patterns first
	fullName := dbName + "." + tableName
	for _, re := range d.ignoreTablePatterns {
		if re.MatchString(fullName) {
			return false
		}
	}

	// No filter patterns means sync all tables
	if len(d.tableFilterPatterns) == 0 {
		return true
	}

	// Check against filter patterns
	for _, re := range d.tableFilterPatterns {
		if re.MatchString(fullName) {
			return true
		}
	}
	return false
}

// TableScopeEntry represents a parsed table scope entry.
type TableScopeEntry struct {
	Database string
	Table    string
}

// ParseTableNames parses table names from "database.table" format.
func (t *TableScope) ParseTableNames() []TableScopeEntry {
	var entries []TableScopeEntry
	for _, name := range t.Names {
		parts := strings.SplitN(name, ".", 2)
		if len(parts) == 2 {
			entries = append(entries, TableScopeEntry{
				Database: parts[0],
				Table:    parts[1],
			})
		}
	}
	return entries
}

// ShouldSyncTable checks if a table should be synchronized (table-level scope).
func (t *TableScope) ShouldSyncTable(dbName, tableName string) bool {
	for _, name := range t.Names {
		if name == dbName+"."+tableName {
			return true
		}
	}
	return false
}

// Clone returns a deep copy of the SyncScope.
func (s *SyncScope) Clone() *SyncScope {
	if s == nil {
		return nil
	}
	clone := &SyncScope{
		Level: s.Level,
		Databases: DatabaseScope{
			EnableDDL:    s.Databases.EnableDDL,
			Names:        make([]string, len(s.Databases.Names)),
			TableFilter:  make([]string, len(s.Databases.TableFilter)),
			IgnoreTables: make([]string, len(s.Databases.IgnoreTables)),
		},
		Tables: TableScope{
			EnableDDL: s.Tables.EnableDDL,
			Names:     make([]string, len(s.Tables.Names)),
		},
	}
	copy(clone.Databases.Names, s.Databases.Names)
	copy(clone.Databases.TableFilter, s.Databases.TableFilter)
	copy(clone.Databases.IgnoreTables, s.Databases.IgnoreTables)
	copy(clone.Tables.Names, s.Tables.Names)
	return clone
}

// DefaultSyncScope returns the default sync scope configuration.
func DefaultSyncScope() *SyncScope {
	return &SyncScope{
		Level: SyncLevelDatabase,
		Databases: DatabaseScope{
			Names:      []string{"*"},
			EnableDDL:  true,
			TableFilter: []string{},
		},
		Tables: TableScope{
			EnableDDL: true,
		},
	}
}

// Validate validates the sync scope configuration.
func (s *SyncScope) Validate() error {
	switch s.Level {
	case SyncLevelDatabase:
		if len(s.Databases.Names) == 0 {
			return ErrInvalidSyncScope
		}
		// Compile patterns to validate regex
		return s.Databases.compilePatterns()

	case SyncLevelTable:
		if len(s.Tables.Names) == 0 {
			return ErrInvalidSyncScope
		}

	default:
		return ErrInvalidSyncScope
	}
	return nil
}
