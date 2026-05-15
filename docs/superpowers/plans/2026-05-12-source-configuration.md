# Source Configuration Subsystem Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement SyncScope, DatabaseDiscovery, and TableManager to support database-level and table-level synchronization with auto-discovery.

**Architecture:** Three-layer design: SyncScope defines what to sync, DatabaseDiscovery auto-discovers new databases/tables in wildcard mode, TableManager handles dynamic table addition/removal. All integrate with existing source connector interface.

**Tech Stack:** Go 1.21+, existing source connector interfaces, event package

---

## File Structure

```
internal/source/
├── sync_scope.go          # SyncScope types (✅ created)
├── discovery.go           # DatabaseDiscovery for wildcard mode
├── table_manager.go       # TableManager for dynamic table handling
├── errors.go              # Error definitions (modify)
└── connector.go           # Source connector interface (modify to add SyncScope)

internal/source/mysql/
└── connector.go           # MySQL connector (modify to use SyncScope)

internal/source/postgres/
└── connector.go           # PostgreSQL connector (modify to use SyncScope)
```

---

### Task 1: Complete SyncScope with Error Types

**Files:**
- Modify: `internal/source/errors.go`
- Modify: `internal/source/sync_scope.go`

- [ ] **Step 1: Add error definitions to errors.go**

```go
// Add to internal/source/errors.go

var (
	// ErrInvalidSyncScope is returned when sync scope configuration is invalid
	ErrInvalidSyncScope = errors.New("invalid sync scope configuration")
	
	// ErrDatabaseNotFound is returned when a database is not found
	ErrDatabaseNotFound = errors.New("database not found")
	
	// ErrTableNotFound is returned when a table is not found
	ErrTableNotFound = errors.New("table not found")
	
	// ErrTableAlreadyExists is returned when adding an existing table
	ErrTableAlreadyExists = errors.New("table already exists in sync scope")
	
	// ErrDiscoveryNotSupported is returned when discovery is not supported
	ErrDiscoveryNotSupported = errors.New("auto-discovery not supported for this connector")
)
```

- [ ] **Step 2: Run tests to verify compilation**

Run: `go build ./internal/source/...`
Expected: Success

- [ ] **Step 3: Commit**

```bash
git add internal/source/errors.go
git commit -m "feat(source): add sync scope error definitions"
```

---

### Task 2: Write Tests for SyncScope

**Files:**
- Create: `internal/source/sync_scope_test.go`

- [ ] **Step 1: Write failing tests for SyncScope**

```go
// internal/source/sync_scope_test.go
package source

import (
	"testing"
)

func TestDatabaseScope_IsWildcardDatabase(t *testing.T) {
	tests := []struct {
		name     string
		scope    DatabaseScope
		expected bool
	}{
		{
			name:     "wildcard mode",
			scope:    DatabaseScope{Names: []string{"*"}},
			expected: true,
		},
		{
			name:     "single database",
			scope:    DatabaseScope{Names: []string{"db1"}},
			expected: false,
		},
		{
			name:     "multiple databases",
			scope:    DatabaseScope{Names: []string{"db1", "db2"}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.scope.IsWildcardDatabase(); got != tt.expected {
				t.Errorf("IsWildcardDatabase() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestDatabaseScope_ShouldSyncDatabase(t *testing.T) {
	tests := []struct {
		name     string
		scope    DatabaseScope
		dbName   string
		expected bool
	}{
		{
			name:     "wildcard matches all",
			scope:    DatabaseScope{Names: []string{"*"}},
			dbName:   "any_db",
			expected: true,
		},
		{
			name:     "exact match",
			scope:    DatabaseScope{Names: []string{"db1", "db2"}},
			dbName:   "db1",
			expected: true,
		},
		{
			name:     "no match",
			scope:    DatabaseScope{Names: []string{"db1", "db2"}},
			dbName:   "db3",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.scope.ShouldSyncDatabase(tt.dbName); got != tt.expected {
				t.Errorf("ShouldSyncDatabase() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestDatabaseScope_ShouldSyncTable(t *testing.T) {
	tests := []struct {
		name      string
		scope     DatabaseScope
		dbName    string
		tableName string
		expected  bool
	}{
		{
			name:      "wildcard database matches all tables",
			scope:     DatabaseScope{Names: []string{"*"}},
			dbName:    "any_db",
			tableName: "any_table",
			expected:  true,
		},
		{
			name: "table filter pattern match",
			scope: DatabaseScope{
				Names:       []string{"db1"},
				TableFilter: []string{"^db1\\.(users|orders)$"},
			},
			dbName:    "db1",
			tableName: "users",
			expected:  true,
		},
		{
			name: "table filter pattern no match",
			scope: DatabaseScope{
				Names:       []string{"db1"},
				TableFilter: []string{"^db1\\.(users|orders)$"},
			},
			dbName:    "db1",
			tableName: "products",
			expected:  false,
		},
		{
			name: "ignore table pattern",
			scope: DatabaseScope{
				Names:        []string{"db1"},
				IgnoreTables: []string{"^db1\\._.*$"},
			},
			dbName:    "db1",
			tableName: "_internal",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.scope.ShouldSyncTable(tt.dbName, tt.tableName); got != tt.expected {
				t.Errorf("ShouldSyncTable() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTableScope_ShouldSyncTable(t *testing.T) {
	scope := TableScope{
		Names: []string{"db1.users", "db1.orders", "db2.products"},
	}

	if !scope.ShouldSyncTable("db1", "users") {
		t.Error("ShouldSyncTable() = false for db1.users")
	}
	if scope.ShouldSyncTable("db1", "products") {
		t.Error("ShouldSyncTable() = true for db1.products")
	}
}

func TestTableScope_ParseTableNames(t *testing.T) {
	scope := TableScope{
		Names: []string{"db1.users", "db1.orders", "db2.products"},
	}

	entries := scope.ParseTableNames()
	if len(entries) != 3 {
		t.Fatalf("ParseTableNames() returned %d entries, want 3", len(entries))
	}

	expected := []TableScopeEntry{
		{Database: "db1", Table: "users"},
		{Database: "db1", Table: "orders"},
		{Database: "db2", Table: "products"},
	}

	for i, entry := range entries {
		if entry.Database != expected[i].Database || entry.Table != expected[i].Table {
			t.Errorf("ParseTableNames()[%d] = {%s, %s}, want {%s, %s}",
				i, entry.Database, entry.Table,
				expected[i].Database, expected[i].Table)
		}
	}
}

func TestSyncScope_Validate(t *testing.T) {
	tests := []struct {
		name    string
		scope   SyncScope
		wantErr bool
	}{
		{
			name: "valid database scope",
			scope: SyncScope{
				Level: SyncLevelDatabase,
				Databases: DatabaseScope{
					Names: []string{"db1", "db2"},
				},
			},
			wantErr: false,
		},
		{
			name: "valid table scope",
			scope: SyncScope{
				Level: SyncLevelTable,
				Tables: TableScope{
					Names: []string{"db1.users"},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid empty database scope",
			scope: SyncScope{
				Level: SyncLevelDatabase,
				Databases: DatabaseScope{
					Names: []string{},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid empty table scope",
			scope: SyncScope{
				Level: SyncLevelTable,
				Tables: TableScope{
					Names: []string{},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid level",
			scope: SyncScope{
				Level: "invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.scope.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `go test ./internal/source/... -v -run TestDatabaseScope\|TestTableScope\|TestSyncScope`
Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
git add internal/source/sync_scope_test.go
git commit -m "test(source): add comprehensive tests for SyncScope"
```

---

### Task 3: Implement DatabaseDiscovery

**Files:**
- Create: `internal/source/discovery.go`
- Create: `internal/source/discovery_test.go`

- [ ] **Step 1: Write failing tests for DatabaseDiscovery**

```go
// internal/source/discovery_test.go
package source

import (
	"testing"
	"time"
)

func TestDatabaseDiscovery_HandleCreateDB(t *testing.T) {
	eventCh := make(chan *DiscoveryEvent, 10)
	scope := &DatabaseScope{Names: []string{"*"}} // Wildcard mode

	cfg := &DiscoveryConfig{
		Scope:        scope,
		EventChannel: eventCh,
		InitialDBs:   make(map[string]struct{}),
		InitialTables: make(map[string]struct{}),
	}

	discovery := NewDatabaseDiscovery(cfg)

	// Simulate CREATE DATABASE DDL
	discovery.handleDDL(&DDLEvent{
		Database: "new_db",
		Type:     DDLTypeCreateDB,
	})

	// Check discovery event
	select {
	case event := <-eventCh:
		if event.Type != DiscoveryTypeDatabaseCreated {
			t.Errorf("Expected %s, got %s", DiscoveryTypeDatabaseCreated, event.Type)
		}
		if event.Database != "new_db" {
			t.Errorf("Expected database 'new_db', got %s", event.Database)
		}
	case <-time.After(time.Second):
		t.Error("Expected discovery event, got none")
	}

	// Check database is now known
	if !discovery.IsDatabaseKnown("new_db") {
		t.Error("Database 'new_db' should be known")
	}
}

func TestDatabaseDiscovery_HandleCreateTable(t *testing.T) {
	eventCh := make(chan *DiscoveryEvent, 10)
	scope := &DatabaseScope{Names: []string{"db1"}}

	cfg := &DiscoveryConfig{
		Scope:        scope,
		EventChannel: eventCh,
		InitialDBs:   map[string]struct{}{"db1": {}},
		InitialTables: make(map[string]struct{}),
	}

	discovery := NewDatabaseDiscovery(cfg)

	// Simulate CREATE TABLE DDL
	discovery.handleDDL(&DDLEvent{
		Database: "db1",
		Table:    "new_table",
		Type:     DDLTypeCreateTable,
	})

	// Check discovery event
	select {
	case event := <-eventCh:
		if event.Type != DiscoveryTypeTableCreated {
			t.Errorf("Expected %s, got %s", DiscoveryTypeTableCreated, event.Type)
		}
		if event.Table != "new_table" {
			t.Errorf("Expected table 'new_table', got %s", event.Table)
		}
	case <-time.After(time.Second):
		t.Error("Expected discovery event, got none")
	}

	// Check table is now known
	if !discovery.IsTableKnown("db1", "new_table") {
		t.Error("Table 'db1.new_table' should be known")
	}
}

func TestDatabaseDiscovery_HandleDropTable(t *testing.T) {
	eventCh := make(chan *DiscoveryEvent, 10)
	scope := &DatabaseScope{Names: []string{"db1"}}

	cfg := &DiscoveryConfig{
		Scope:        scope,
		EventChannel: eventCh,
		InitialDBs:   map[string]struct{}{"db1": {}},
		InitialTables: map[string]struct{}{"db1.users": {}},
	}

	discovery := NewDatabaseDiscovery(cfg)

	// Verify table is initially known
	if !discovery.IsTableKnown("db1", "users") {
		t.Fatal("Table should be initially known")
	}

	// Simulate DROP TABLE DDL
	discovery.handleDDL(&DDLEvent{
		Database: "db1",
		Table:    "users",
		Type:     DDLTypeDropTable,
	})

	// Check discovery event
	select {
	case event := <-eventCh:
		if event.Type != DiscoveryTypeTableDropped {
			t.Errorf("Expected %s, got %s", DiscoveryTypeTableDropped, event.Type)
		}
	case <-time.After(time.Second):
		t.Error("Expected discovery event, got none")
	}

	// Check table is no longer known
	if discovery.IsTableKnown("db1", "users") {
		t.Error("Table 'db1.users' should no longer be known")
	}
}

func TestDatabaseDiscovery_IgnoreOutOfScopeDDL(t *testing.T) {
	eventCh := make(chan *DiscoveryEvent, 10)
	scope := &DatabaseScope{Names: []string{"db1"}} // Only db1

	cfg := &DiscoveryConfig{
		Scope:        scope,
		EventChannel: eventCh,
		InitialDBs:   map[string]struct{}{"db1": {}},
		InitialTables: make(map[string]struct{}),
	}

	discovery := NewDatabaseDiscovery(cfg)

	// Simulate CREATE TABLE in different database (db2)
	discovery.handleDDL(&DDLEvent{
		Database: "db2",
		Table:    "new_table",
		Type:     DDLTypeCreateTable,
	})

	// Should not emit event (non-blocking check)
	select {
	case event := <-eventCh:
		t.Errorf("Should not emit event for out-of-scope DDL, got: %v", event)
	default:
		// Expected: no event
	}

	// Table should not be known
	if discovery.IsTableKnown("db2", "new_table") {
		t.Error("Table 'db2.new_table' should not be known (out of scope)")
	}
}
```

- [ ] **Step 2: Implement DatabaseDiscovery**

The implementation was already started. Complete the `internal/source/discovery.go` file with the full implementation matching the tests.

- [ ] **Step 3: Run tests**

Run: `go test ./internal/source/... -v -run TestDatabaseDiscovery`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add internal/source/discovery.go internal/source/discovery_test.go
git commit -m "feat(source): implement DatabaseDiscovery for wildcard mode"
```

---

### Task 4: Implement TableManager

**Files:**
- Create: `internal/source/table_manager.go`
- Create: `internal/source/table_manager_test.go`

- [ ] **Step 1: Write failing tests for TableManager**

```go
// internal/source/table_manager_test.go
package source

import (
	"context"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

// mockSchemaFetcher implements SchemaFetcher for testing
type mockSchemaFetcher struct {
	schemas map[string]*event.TableInfo
}

func (m *mockSchemaFetcher) FetchSchema(ctx context.Context, database, table string) (*event.TableInfo, error) {
	key := database + "." + table
	if schema, ok := m.schemas[key]; ok {
		return schema, nil
	}
	return nil, ErrTableNotFound
}

func TestTableManager_AddTables(t *testing.T) {
	fetcher := &mockSchemaFetcher{
		schemas: map[string]*event.TableInfo{
			"db1.users": {
				Database: "db1",
				Table:    "users",
				Columns: []event.ColumnInfo{
					{Name: "id", Type: "int"},
					{Name: "name", Type: "varchar"},
				},
			},
		},
	}

	eventCh := make(chan *TableOperationEvent, 10)
	scope := &TableScope{Names: []string{}}

	tm := NewTableManager(&TableManagerConfig{
		Scope:        scope,
		SchemaFetcher: fetcher,
		EventChannel: eventCh,
	})

	err := tm.AddTables(context.Background(), []string{"db1.users"})
	if err != nil {
		t.Fatalf("AddTables failed: %v", err)
	}

	// Check table is now in scope
	if !tm.HasTable("db1", "users") {
		t.Error("Table db1.users should be in scope")
	}

	// Check event was emitted
	select {
	case event := <-eventCh:
		if event.Operation != TableOpAdd {
			t.Errorf("Expected %s, got %s", TableOpAdd, event.Operation)
		}
		if event.TableID.Database != "db1" || event.TableID.Table != "users" {
			t.Errorf("Unexpected table ID: %v", event.TableID)
		}
	case <-time.After(time.Second):
		t.Error("Expected TableOperationEvent, got none")
	}
}

func TestTableManager_RemoveTables(t *testing.T) {
	fetcher := &mockSchemaFetcher{
		schemas: map[string]*event.TableInfo{
			"db1.users": {Database: "db1", Table: "users"},
		},
	}

	eventCh := make(chan *TableOperationEvent, 10)
	scope := &TableScope{Names: []string{"db1.users"}}

	tm := NewTableManager(&TableManagerConfig{
		Scope:        scope,
		SchemaFetcher: fetcher,
		EventChannel: eventCh,
	})

	// First add the table
	tm.AddTables(context.Background(), []string{"db1.users"})
	<-eventCh // Consume add event

	// Now remove it
	err := tm.RemoveTables(context.Background(), []string{"db1.users"})
	if err != nil {
		t.Fatalf("RemoveTables failed: %v", err)
	}

	// Check table is no longer in scope
	if tm.HasTable("db1", "users") {
		t.Error("Table db1.users should not be in scope")
	}

	// Check event was emitted
	select {
	case event := <-eventCh:
		if event.Operation != TableOpRemove {
			t.Errorf("Expected %s, got %s", TableOpRemove, event.Operation)
		}
	case <-time.After(time.Second):
		t.Error("Expected TableOperationEvent, got none")
	}
}

func TestTableManager_ListTables(t *testing.T) {
	fetcher := &mockSchemaFetcher{
		schemas: map[string]*event.TableInfo{
			"db1.users":  {Database: "db1", Table: "users"},
			"db1.orders": {Database: "db1", Table: "orders"},
		},
	}

	eventCh := make(chan *TableOperationEvent, 10)
	scope := &TableScope{Names: []string{}}

	tm := NewTableManager(&TableManagerConfig{
		Scope:        scope,
		SchemaFetcher: fetcher,
		EventChannel: eventCh,
	})

	tm.AddTables(context.Background(), []string{"db1.users", "db1.orders"})
	// Consume events
	<-eventCh
	<-eventCh

	tables := tm.ListTables()
	if len(tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(tables))
	}
}

func TestTableManager_GetTableState(t *testing.T) {
	fetcher := &mockSchemaFetcher{
		schemas: map[string]*event.TableInfo{
			"db1.users": {Database: "db1", Table: "users"},
		},
	}

	eventCh := make(chan *TableOperationEvent, 10)
	scope := &TableScope{Names: []string{}}

	tm := NewTableManager(&TableManagerConfig{
		Scope:        scope,
		SchemaFetcher: fetcher,
		EventChannel: eventCh,
	})

	tm.AddTables(context.Background(), []string{"db1.users"})
	<-eventCh // Consume event

	state, err := tm.GetTableState("db1", "users")
	if err != nil {
		t.Fatalf("GetTableState failed: %v", err)
	}

	if state.Status != TableStatusPending {
		t.Errorf("Expected status %s, got %s", TableStatusPending, state.Status)
	}
}
```

- [ ] **Step 2: Implement TableManager**

```go
// internal/source/table_manager.go
package source

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

// TableOperationType represents table operation types.
type TableOperationType string

const (
	// TableOpAdd indicates a table was added
	TableOpAdd TableOperationType = "add"
	// TableOpRemove indicates a table was removed
	TableOpRemove TableOperationType = "remove"
)

// TableOperationEvent represents a table operation event.
type TableOperationEvent struct {
	Operation TableOperationType `json:"operation"`
	TableID   TableID            `json:"tableId"`
	Schema    *event.TableInfo   `json:"schema,omitempty"`
	Timestamp time.Time          `json:"timestamp"`
}

// TableID identifies a table.
type TableID struct {
	Database string `json:"database"`
	Schema   string `json:"schema,omitempty"`
	Table    string `json:"table"`
}

// String returns the string representation of TableID.
func (t TableID) String() string {
	if t.Schema != "" {
		return fmt.Sprintf("%s.%s.%s", t.Schema, t.Database, t.Table)
	}
	return fmt.Sprintf("%s.%s", t.Database, t.Table)
}

// TableSyncStatus represents the sync status of a table.
type TableSyncStatus string

const (
	// TableStatusPending indicates the table is waiting to sync
	TableStatusPending TableSyncStatus = "pending"
	// TableStatusSnapshot indicates the table is in snapshot phase
	TableStatusSnapshot TableSyncStatus = "snapshot"
	// TableStatusStreaming indicates the table is in streaming phase
	TableStatusStreaming TableSyncStatus = "streaming"
	// TableStatusPaused indicates the table sync is paused
	TableStatusPaused TableSyncStatus = "paused"
	// TableStatusError indicates the table has an error
	TableStatusError TableSyncStatus = "error"
)

// TableSyncState represents the sync state of a table.
type TableSyncState struct {
	TableID     TableID
	Status      TableSyncStatus
	Schema      *event.TableInfo
	AddedAt     time.Time
	SyncStarted time.Time
	Error       error
}

// SchemaFetcher fetches table schemas.
type SchemaFetcher interface {
	FetchSchema(ctx context.Context, database, table string) (*event.TableInfo, error)
}

// TableManager manages tables for table-level synchronization.
type TableManager struct {
	scope         *TableScope
	schemaFetcher SchemaFetcher
	eventCh       chan<- *TableOperationEvent

	syncTables map[string]*TableSyncState // key: "database.table"
	mu         sync.RWMutex
}

// TableManagerConfig holds configuration for TableManager.
type TableManagerConfig struct {
	Scope         *TableScope
	SchemaFetcher SchemaFetcher
	EventChannel  chan<- *TableOperationEvent
}

// NewTableManager creates a new table manager.
func NewTableManager(cfg *TableManagerConfig) *TableManager {
	tm := &TableManager{
		scope:         cfg.Scope,
		schemaFetcher: cfg.SchemaFetcher,
		eventCh:       cfg.EventChannel,
		syncTables:    make(map[string]*TableSyncState),
	}

	// Initialize from scope
	for _, entry := range cfg.Scope.ParseTableNames() {
		key := entry.Database + "." + entry.Table
		tm.syncTables[key] = &TableSyncState{
			TableID: TableID{
				Database: entry.Database,
				Table:    entry.Table,
			},
			Status:  TableStatusPending,
			AddedAt: time.Now(),
		}
	}

	return tm
}

// AddTables adds tables to the sync task.
func (tm *TableManager) AddTables(ctx context.Context, tables []string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for _, table := range tables {
		db, tbl, err := parseTableName(table)
		if err != nil {
			return err
		}

		key := db + "." + tbl

		// Check if already exists
		if _, exists := tm.syncTables[key]; exists {
			continue // Already exists, skip
		}

		// Fetch schema
		schema, err := tm.schemaFetcher.FetchSchema(ctx, db, tbl)
		if err != nil {
			return fmt.Errorf("failed to fetch schema for %s: %w", table, err)
		}

		// Create sync state
		state := &TableSyncState{
			TableID: TableID{
				Database: db,
				Table:    tbl,
			},
			Status:  TableStatusPending,
			Schema:  schema,
			AddedAt: time.Now(),
		}
		tm.syncTables[key] = state

		// Emit event
		tm.emitEvent(&TableOperationEvent{
			Operation: TableOpAdd,
			TableID:   state.TableID,
			Schema:    schema,
			Timestamp: time.Now(),
		})
	}

	return nil
}

// RemoveTables removes tables from the sync task.
func (tm *TableManager) RemoveTables(ctx context.Context, tables []string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for _, table := range tables {
		db, tbl, err := parseTableName(table)
		if err != nil {
			return err
		}

		key := db + "." + tbl

		// Check if exists
		if _, exists := tm.syncTables[key]; !exists {
			continue // Doesn't exist, skip
		}

		// Remove from sync tables
		delete(tm.syncTables, key)

		// Emit event
		tm.emitEvent(&TableOperationEvent{
			Operation: TableOpRemove,
			TableID: TableID{
				Database: db,
				Table:    tbl,
			},
			Timestamp: time.Now(),
		})
	}

	return nil
}

// HasTable checks if a table is in the sync scope.
func (tm *TableManager) HasTable(database, table string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	key := database + "." + table
	_, exists := tm.syncTables[key]
	return exists
}

// ListTables returns all tables in the sync scope.
func (tm *TableManager) ListTables() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tables := make([]string, 0, len(tm.syncTables))
	for key := range tm.syncTables {
		tables = append(tables, key)
	}
	return tables
}

// GetTableState returns the sync state for a table.
func (tm *TableManager) GetTableState(database, table string) (*TableSyncState, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	key := database + "." + table
	state, exists := tm.syncTables[key]
	if !exists {
		return nil, ErrTableNotFound
	}
	return state, nil
}

// UpdateTableStatus updates the sync status for a table.
func (tm *TableManager) UpdateTableStatus(database, table string, status TableSyncStatus) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	key := database + "." + table
	state, exists := tm.syncTables[key]
	if !exists {
		return ErrTableNotFound
	}

	state.Status = status
	return nil
}

// emitEvent sends a table operation event.
func (tm *TableManager) emitEvent(event *TableOperationEvent) {
	if tm.eventCh == nil {
		return
	}

	select {
	case tm.eventCh <- event:
	default:
		// Channel full, drop event
	}
}

// parseTableName parses "database.table" format.
func parseTableName(name string) (string, string, error) {
	parts := strings.SplitN(name, ".", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid table name format: %s (expected database.table)", name)
	}
	return parts[0], parts[1], nil
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./internal/source/... -v -run TestTableManager`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add internal/source/table_manager.go internal/source/table_manager_test.go
git commit -m "feat(source): implement TableManager for dynamic table handling"
```

---

### Task 5: Integrate SyncScope with Connector Interface

**Files:**
- Modify: `internal/source/connector.go`
- Modify: `internal/source/mysql/connector.go`

- [ ] **Step 1: Add SyncScope to Config and Connector interface**

```go
// Add to internal/source/connector.go

// Config is the configuration for a source connector.
type Config struct {
	// ... existing fields ...

	// SyncScope defines what databases/tables to sync
	SyncScope *SyncScope `json:"syncScope" toml:"sync-scope"`
}

// Connector interface - add these methods:
type Connector interface {
	// ... existing methods ...

	// SyncScope returns the current sync scope
	SyncScope() *SyncScope

	// AddTables adds tables to sync (table-level only)
	AddTables(ctx context.Context, tables []string) error

	// RemoveTables removes tables from sync (table-level only)
	RemoveTables(ctx context.Context, tables []string) error

	// ListTables returns all tables being synced
	ListTables() []string
}
```

- [ ] **Step 2: Run tests to verify compilation**

Run: `go build ./internal/source/...`
Expected: Success (may have errors in implementations, fix them)

- [ ] **Step 3: Commit**

```bash
git add internal/source/connector.go
git commit -m "feat(source): add SyncScope integration to connector interface"
```

---

### Task 6: Update MySQL Connector to Use SyncScope

**Files:**
- Modify: `internal/source/mysql/connector.go`
- Modify: `internal/source/mysql/config.go`

- [ ] **Step 1: Add SyncScope to MySQL connector**

Update MySQL connector to use SyncScope instead of raw Databases/Tables fields.

- [ ] **Step 2: Run tests**

Run: `go test ./internal/source/mysql/... -v`
Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
git add internal/source/mysql/connector.go internal/source/mysql/config.go
git commit -m "feat(source): integrate SyncScope with MySQL connector"
```

---

## Summary

This plan implements:

1. **SyncScope** - Database and table level sync configuration with pattern filtering
2. **DatabaseDiscovery** - Auto-discovery for wildcard mode (`["*"]`)
3. **TableManager** - Dynamic table addition/removal at runtime
4. **Connector Integration** - Updated interface and MySQL implementation

**Total Tasks:** 6
**Estimated Time:** 2-3 hours

---

*Plan created: 2026-05-12*
