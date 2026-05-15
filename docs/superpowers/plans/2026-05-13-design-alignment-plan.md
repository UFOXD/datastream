# Design Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement DatabaseDiscovery, TableManager, and Schemas() method to align implementation with design requirements.

**Architecture:** DatabaseDiscovery monitors DDL events for wildcard-mode auto-discovery. TableManager provides API-driven table management. Both delegate to existing Connector AddTables/RemoveTables methods.

**Tech Stack:** Go 1.21+, existing source.Connector interface, parser.DDLParser

---

## File Structure

```
internal/source/
├── database_discovery.go        # NEW - auto-discovery for wildcard mode
├── database_discovery_test.go   # NEW - tests
├── table_manager.go             # NEW - API table management
├── table_manager_test.go        # NEW - tests
├── mysql/connector.go           # MODIFY - add Schemas() method
└── mysql/connector_test.go      # MODIFY - add Schemas() test

internal/api/
└── tables.go                    # MODIFY - add table management endpoints
```

---

### Task 1: Add Schemas() Method to MySQL Connector

**Files:**
- Modify: `internal/source/mysql/connector.go`
- Modify: `internal/source/mysql/connector_test.go`

- [ ] **Step 1: Write the failing test**

```go
// Add to connector_test.go
func TestConnector_Schemas(t *testing.T) {
    conn := New()
    conn.config = &Config{
        Host:     "localhost",
        Port:     3306,
        User:     "root",
        Password: "",
        ServerID: 1,
    }

    // Initialize without starting
    ctx := context.Background()
    err := conn.Initialize(ctx, source.Config{})
    require.NoError(t, err)

    // Get all schemas
    schemas := conn.Schemas()
    require.NotNil(t, schemas)
    // Should be empty or contain cached schemas
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/source/mysql/... -run TestConnector_Schemas -v`
Expected: FAIL with "conn.Schemas undefined"

- [ ] **Step 3: Add Schemas() method to Connector**

```go
// Add to connector.go in Connector struct methods section

// Schemas returns all cached table schemas.
func (c *Connector) Schemas() map[string]*event.TableInfo {
    c.schemaMu.RLock()
    defer c.schemaMu.RUnlock()

    result := make(map[string]*event.TableInfo)
    for key, schema := range c.schemaCache {
        result[key] = schema
    }
    return result
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/source/mysql/... -run TestConnector_Schemas -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/source/mysql/connector.go internal/source/mysql/connector_test.go
git commit -m "feat(source): add Schemas() method to MySQL connector"
```

---

### Task 2: Implement DatabaseDiscovery

**Files:**
- Create: `internal/source/database_discovery.go`
- Create: `internal/source/database_discovery_test.go`

- [ ] **Step 1: Write the failing test for DatabaseDiscovery**

```go
// File: internal/source/database_discovery_test.go
package source

import (
    "context"
    "testing"

    "github.com/UFOXD/datastream/pkg/event"
    "github.com/stretchr/testify/require"
)

func TestDatabaseDiscovery_ShouldSyncDatabase(t *testing.T) {
    scope := &SyncScope{
        Level: SyncLevelDatabase,
        Database: DatabaseScope{
            Names: []string{"*"}, // Wildcard mode
        },
    }

    discovery := NewDatabaseDiscovery(scope, nil, nil)

    // Wildcard mode should sync all databases
    require.True(t, discovery.ShouldSyncDatabase("any_db"))
    require.True(t, discovery.ShouldSyncDatabase("test_db"))
}

func TestDatabaseDiscovery_ShouldSyncDatabase_SpecificDBs(t *testing.T) {
    scope := &SyncScope{
        Level: SyncLevelDatabase,
        Database: DatabaseScope{
            Names: []string{"db1", "db2"},
        },
    }

    discovery := NewDatabaseDiscovery(scope, nil, nil)

    require.True(t, discovery.ShouldSyncDatabase("db1"))
    require.True(t, discovery.ShouldSyncDatabase("db2"))
    require.False(t, discovery.ShouldSyncDatabase("db3"))
}

func TestDatabaseDiscovery_ShouldSyncTable(t *testing.T) {
    scope := &SyncScope{
        Level: SyncLevelDatabase,
        Database: DatabaseScope{
            Names:       []string{"*"},
            TableFilter: []string{"^user_.*"}, // Only user_* tables
        },
    }

    discovery := NewDatabaseDiscovery(scope, nil, nil)

    require.True(t, discovery.ShouldSyncTable("any_db", "user_profile"))
    require.True(t, discovery.ShouldSyncTable("any_db", "user_settings"))
    require.False(t, discovery.ShouldSyncTable("any_db", "orders"))
}

func TestDatabaseDiscovery_IsWildcardMode(t *testing.T) {
    scope := &SyncScope{
        Level: SyncLevelDatabase,
        Database: DatabaseScope{
            Names: []string{"*"},
        },
    }

    discovery := NewDatabaseDiscovery(scope, nil, nil)
    require.True(t, discovery.IsWildcardMode())

    scope2 := &SyncScope{
        Level: SyncLevelDatabase,
        Database: DatabaseScope{
            Names: []string{"db1"},
        },
    }

    discovery2 := NewDatabaseDiscovery(scope2, nil, nil)
    require.False(t, discovery2.IsWildcardMode())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/source/... -run TestDatabaseDiscovery -v`
Expected: FAIL with "undefined: NewDatabaseDiscovery"

- [ ] **Step 3: Implement DatabaseDiscovery struct and methods**

```go
// File: internal/source/database_discovery.go
package source

import (
    "context"
    "regexp"
    "sync"

    "github.com/UFOXD/datastream/pkg/event"
    "github.com/UFOXD/datastream/pkg/parser"
)

// DiscoveryType represents the type of discovery event
type DiscoveryType string

const (
    DiscoveryTypeDatabaseCreated DiscoveryType = "database-created"
    DiscoveryTypeDatabaseDropped DiscoveryType = "database-dropped"
    DiscoveryTypeTableCreated    DiscoveryType = "table-created"
    DiscoveryTypeTableDropped    DiscoveryType = "table-dropped"
    DiscoveryTypeTableAltered    DiscoveryType = "table-altered"
)

// DiscoveryEvent represents a database/table discovery event
type DiscoveryEvent struct {
    Type      DiscoveryType
    Database  string
    Table     string
    Timestamp time.Time
}

// DatabaseDiscovery handles automatic discovery of databases and tables
// for wildcard sync mode.
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

// NewDatabaseDiscovery creates a new database discovery instance
func NewDatabaseDiscovery(scope *SyncScope, connector Connector, p parser.DDLParser) *DatabaseDiscovery {
    return &DatabaseDiscovery{
        scope:       scope,
        connector:   connector,
        parser:      p,
        knownDBs:    make(map[string]struct{}),
        knownTables: make(map[string]struct{}),
        eventCh:     make(chan *DiscoveryEvent, 100),
    }
}

// IsWildcardMode returns true if syncing all databases
func (d *DatabaseDiscovery) IsWildcardMode() bool {
    return len(d.scope.Database.Names) == 1 && d.scope.Database.Names[0] == "*"
}

// ShouldSyncDatabase checks if a database should be synced
func (d *DatabaseDiscovery) ShouldSyncDatabase(dbName string) bool {
    if d.IsWildcardMode() {
        return true
    }

    for _, name := range d.scope.Database.Names {
        if name == dbName {
            return true
        }
    }
    return false
}

// ShouldSyncTable checks if a table should be synced
func (d *DatabaseDiscovery) ShouldSyncTable(dbName, tableName string) bool {
    if !d.ShouldSyncDatabase(dbName) {
        return false
    }

    // Check ignore list first
    for _, pattern := range d.scope.Database.IgnoreTables {
        if matched, _ := regexp.MatchString(pattern, tableName); matched {
            return false
        }
    }

    // No filter means sync all
    if len(d.scope.Database.TableFilter) == 0 {
        return true
    }

    // Check filter patterns
    for _, pattern := range d.scope.Database.TableFilter {
        if matched, _ := regexp.MatchString(pattern, tableName); matched {
            return true
        }
    }
    return false
}

// Start begins monitoring for new databases/tables
func (d *DatabaseDiscovery) Start(ctx context.Context) error {
    d.ctx, d.cancel = context.WithCancel(ctx)
    return nil
}

// Stop stops the discovery process
func (d *DatabaseDiscovery) Stop() error {
    if d.cancel != nil {
        d.cancel()
    }
    close(d.eventCh)
    return nil
}

// OnDDLEvent processes a DDL event for discovery
func (d *DatabaseDiscovery) OnDDLEvent(ddlEvent *event.ChangeEvent) error {
    if !d.IsWildcardMode() {
        return nil // Only process in wildcard mode
    }

    d.mu.Lock()
    defer d.mu.Unlock()

    dbName := ddlEvent.Table.Database
    tableName := ddlEvent.Table.Table

    switch ddlEvent.Type {
    case event.EventTypeDDL:
        // Check if this is a CREATE TABLE or CREATE DATABASE
        if d.shouldAddTable(dbName, tableName) {
            d.knownTables[dbName+"."+tableName] = struct{}{}

            // Notify connector to add this table
            if d.connector != nil {
                d.connector.AddTables(d.ctx, []string{dbName + "." + tableName})
            }

            // Emit discovery event
            d.eventCh <- &DiscoveryEvent{
                Type:      DiscoveryTypeTableCreated,
                Database:  dbName,
                Table:     tableName,
                Timestamp: time.Now(),
            }
        }
    }

    return nil
}

// shouldAddTable checks if a table should be added to sync
func (d *DatabaseDiscovery) shouldAddTable(dbName, tableName string) bool {
    key := dbName + "." + tableName
    if _, exists := d.knownTables[key]; exists {
        return false // Already known
    }
    return d.ShouldSyncTable(dbName, tableName)
}

// Events returns the discovery event channel
func (d *DatabaseDiscovery) Events() <-chan *DiscoveryEvent {
    return d.eventCh
}

// KnownDatabases returns list of known databases
func (d *DatabaseDiscovery) KnownDatabases() []string {
    d.mu.RLock()
    defer d.mu.RUnlock()

    dbs := make([]string, 0, len(d.knownDBs))
    for db := range d.knownDBs {
        dbs = append(dbs, db)
    }
    return dbs
}

// KnownTables returns list of known tables
func (d *DatabaseDiscovery) KnownTables() []string {
    d.mu.RLock()
    defer d.mu.RUnlock()

    tables := make([]string, 0, len(d.knownTables))
    for table := range d.knownTables {
        tables = append(tables, table)
    }
    return tables
}
```

- [ ] **Step 4: Add missing imports to database_discovery.go**

```go
// Add to imports at top of file
import (
    "context"
    "regexp"
    "sync"
    "time"

    "github.com/UFOXD/datastream/pkg/event"
    "github.com/UFOXD/datastream/pkg/parser"
)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/source/... -run TestDatabaseDiscovery -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/source/database_discovery.go internal/source/database_discovery_test.go
git commit -m "feat(source): implement DatabaseDiscovery for wildcard mode"
```

---

### Task 3: Implement TableManager

**Files:**
- Create: `internal/source/table_manager.go`
- Create: `internal/source/table_manager_test.go`

- [ ] **Step 1: Write the failing test for TableManager**

```go
// File: internal/source/table_manager_test.go
package source

import (
    "context"
    "testing"

    "github.com/stretchr/testify/require"
)

// MockConnector for testing
type mockConnector struct {
    tables    []string
    addError  error
    removeError error
}

func (m *mockConnector) AddTables(ctx context.Context, tables []string) error {
    m.tables = append(m.tables, tables...)
    return m.addError
}

func (m *mockConnector) RemoveTables(ctx context.Context, tables []string) error {
    for _, t := range tables {
        for i, existing := range m.tables {
            if existing == t {
                m.tables = append(m.tables[:i], m.tables[i+1:]...)
                break
            }
        }
    }
    return m.removeError
}

func (m *mockConnector) ListTables() []string {
    return m.tables
}

func TestTableManager_AddTables(t *testing.T) {
    mock := &mockConnector{}
    tm := NewTableManager(mock)

    results := tm.AddTables(context.Background(), []string{"db1.table1", "db2.table2"})

    require.Len(t, results, 2)
    require.True(t, results[0].Success)
    require.True(t, results[1].Success)
    require.Contains(t, mock.tables, "db1.table1")
    require.Contains(t, mock.tables, "db2.table2")
}

func TestTableManager_RemoveTables(t *testing.T) {
    mock := &mockConnector{tables: []string{"db1.table1", "db1.table2"}}
    tm := NewTableManager(mock)

    results := tm.RemoveTables(context.Background(), []string{"db1.table1"})

    require.Len(t, results, 1)
    require.True(t, results[0].Success)
    require.NotContains(t, mock.tables, "db1.table1")
    require.Contains(t, mock.tables, "db1.table2")
}

func TestTableManager_ListTables(t *testing.T) {
    mock := &mockConnector{tables: []string{"db1.table1"}}
    tm := NewTableManager(mock)

    // Add a table via manager
    tm.AddTables(context.Background(), []string{"db2.table2"})

    statuses := tm.ListTables()
    require.Len(t, statuses, 2)
}

func TestTableManager_GetTableStatus(t *testing.T) {
    mock := &mockConnector{}
    tm := NewTableManager(mock)

    tm.AddTables(context.Background(), []string{"db1.table1"})

    status, err := tm.GetTableStatus("db1", "table1")
    require.NoError(t, err)
    require.Equal(t, "db1", status.Database)
    require.Equal(t, "table1", status.Table)
    require.Equal(t, TableStatusPending, status.Status)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/source/... -run TestTableManager -v`
Expected: FAIL with "undefined: NewTableManager"

- [ ] **Step 3: Implement TableManager struct and methods**

```go
// File: internal/source/table_manager.go
package source

import (
    "context"
    "fmt"
    "sync"
    "time"

    "github.com/UFOXD/datastream/pkg/event"
)

// TableStatus represents the sync status of a table
type TableStatus string

const (
    TableStatusPending      TableStatus = "pending"
    TableStatusSnapshotting TableStatus = "snapshotting"
    TableStatusStreaming    TableStatus = "streaming"
    TableStatusPaused       TableStatus = "paused"
    TableStatusError        TableStatus = "error"
)

// TableSyncStatus holds the sync status of a single table
type TableSyncStatus struct {
    Database    string       `json:"database"`
    Table       string       `json:"table"`
    Status      TableStatus  `json:"status"`
    AddedAt     time.Time    `json:"added_at"`
    SyncStarted time.Time    `json:"sync_started,omitempty"`
    Position    *event.Position `json:"position,omitempty"`
    Error       string       `json:"error,omitempty"`
}

// TableOperationType represents a table operation type
type TableOperationType string

const (
    TableOpAdded   TableOperationType = "added"
    TableOpRemoved TableOperationType = "removed"
    TableOpPaused  TableOperationType = "paused"
    TableOpResumed TableOperationType = "resumed"
    TableOpError   TableOperationType = "error"
)

// TableOperationResult represents the result of a table operation
type TableOperationResult struct {
    Database string `json:"database"`
    Table    string `json:"table"`
    Success  bool   `json:"success"`
    Error    string `json:"error,omitempty"`
}

// TableOperationEvent represents a table operation event
type TableOperationEvent struct {
    Type      TableOperationType
    Database  string
    Table     string
    Timestamp time.Time
    Error     error
}

// TableManager provides API-driven table management
type TableManager struct {
    connector Connector

    // Table sync status
    tables map[string]*TableSyncStatus
    mu     sync.RWMutex

    // Event channel
    eventCh chan *TableOperationEvent

    ctx    context.Context
    cancel context.CancelFunc
}

// NewTableManager creates a new table manager
func NewTableManager(connector Connector) *TableManager {
    return &TableManager{
        connector: connector,
        tables:    make(map[string]*TableSyncStatus),
        eventCh:   make(chan *TableOperationEvent, 100),
    }
}

// AddTables adds tables to the sync list
func (tm *TableManager) AddTables(ctx context.Context, tables []string) []TableOperationResult {
    results := make([]TableOperationResult, len(tables))

    for i, table := range tables {
        dbName, tableName, err := parseTableName(table)
        if err != nil {
            results[i] = TableOperationResult{
                Database: "",
                Table:    table,
                Success:  false,
                Error:    err.Error(),
            }
            continue
        }

        tm.mu.Lock()
        key := dbName + "." + tableName

        // Check if already exists
        if _, exists := tm.tables[key]; exists {
            tm.mu.Unlock()
            results[i] = TableOperationResult{
                Database: dbName,
                Table:    tableName,
                Success:  true, // Already added, consider success
            }
            continue
        }

        // Add to connector
        if err := tm.connector.AddTables(ctx, []string{table}); err != nil {
            tm.mu.Unlock()
            results[i] = TableOperationResult{
                Database: dbName,
                Table:    tableName,
                Success:  false,
                Error:    err.Error(),
            }
            continue
        }

        // Record status
        tm.tables[key] = &TableSyncStatus{
            Database: dbName,
            Table:    tableName,
            Status:   TableStatusPending,
            AddedAt:  time.Now(),
        }
        tm.mu.Unlock()

        results[i] = TableOperationResult{
            Database: dbName,
            Table:    tableName,
            Success:  true,
        }

        // Emit event
        tm.eventCh <- &TableOperationEvent{
            Type:      TableOpAdded,
            Database:  dbName,
            Table:     tableName,
            Timestamp: time.Now(),
        }
    }

    return results
}

// RemoveTables removes tables from the sync list
func (tm *TableManager) RemoveTables(ctx context.Context, tables []string) []TableOperationResult {
    results := make([]TableOperationResult, len(tables))

    for i, table := range tables {
        dbName, tableName, err := parseTableName(table)
        if err != nil {
            results[i] = TableOperationResult{
                Database: "",
                Table:    table,
                Success:  false,
                Error:    err.Error(),
            }
            continue
        }

        tm.mu.Lock()
        key := dbName + "." + tableName

        // Remove from connector
        if err := tm.connector.RemoveTables(ctx, []string{table}); err != nil {
            tm.mu.Unlock()
            results[i] = TableOperationResult{
                Database: dbName,
                Table:    tableName,
                Success:  false,
                Error:    err.Error(),
            }
            continue
        }

        // Remove from status map
        delete(tm.tables, key)
        tm.mu.Unlock()

        results[i] = TableOperationResult{
            Database: dbName,
            Table:    tableName,
            Success:  true,
        }

        // Emit event
        tm.eventCh <- &TableOperationEvent{
            Type:      TableOpRemoved,
            Database:  dbName,
            Table:     tableName,
            Timestamp: time.Now(),
        }
    }

    return results
}

// ListTables returns all tables and their sync status
func (tm *TableManager) ListTables() []*TableSyncStatus {
    tm.mu.RLock()
    defer tm.mu.RUnlock()

    result := make([]*TableSyncStatus, 0, len(tm.tables))
    for _, status := range tm.tables {
        result = append(result, status)
    }
    return result
}

// GetTableStatus returns the status of a specific table
func (tm *TableManager) GetTableStatus(database, table string) (*TableSyncStatus, error) {
    tm.mu.RLock()
    defer tm.mu.RUnlock()

    key := database + "." + table
    status, exists := tm.tables[key]
    if !exists {
        return nil, fmt.Errorf("table %s.%s not found", database, table)
    }
    return status, nil
}

// PauseTable pauses syncing of a table
func (tm *TableManager) PauseTable(ctx context.Context, database, table string) error {
    tm.mu.Lock()
    defer tm.mu.Unlock()

    key := database + "." + table
    status, exists := tm.tables[key]
    if !exists {
        return fmt.Errorf("table %s.%s not found", database, table)
    }

    status.Status = TableStatusPaused

    tm.eventCh <- &TableOperationEvent{
        Type:      TableOpPaused,
        Database:  database,
        Table:     table,
        Timestamp: time.Now(),
    }

    return nil
}

// ResumeTable resumes syncing of a paused table
func (tm *TableManager) ResumeTable(ctx context.Context, database, table string) error {
    tm.mu.Lock()
    defer tm.mu.Unlock()

    key := database + "." + table
    status, exists := tm.tables[key]
    if !exists {
        return fmt.Errorf("table %s.%s not found", database, table)
    }

    status.Status = TableStatusPending

    tm.eventCh <- &TableOperationEvent{
        Type:      TableOpResumed,
        Database:  database,
        Table:     table,
        Timestamp: time.Now(),
    }

    return nil
}

// Events returns the table operation event channel
func (tm *TableManager) Events() <-chan *TableOperationEvent {
    return tm.eventCh
}

// parseTableName parses "database.table" format
func parseTableName(name string) (string, string, error) {
    parts := strings.Split(name, ".")
    if len(parts) != 2 {
        return "", "", fmt.Errorf("invalid table name format: %s (expected database.table)", name)
    }
    return parts[0], parts[1], nil
}
```

- [ ] **Step 4: Add missing imports**

```go
// Add to imports at top of file
import (
    "context"
    "fmt"
    "strings"
    "sync"
    "time"

    "github.com/UFOXD/datastream/pkg/event"
)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/source/... -run TestTableManager -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/source/table_manager.go internal/source/table_manager_test.go
git commit -m "feat(source): implement TableManager for API-driven table management"
```

---

### Task 4: Add API Endpoints for Table Management

**Files:**
- Modify: `internal/api/api.go`
- Create: `internal/api/tables.go`

- [ ] **Step 1: Write the failing test for API endpoints**

```go
// Add to internal/api/api_test.go or create tables_test.go
func TestAPI_TablesEndpoints(t *testing.T) {
    router := mux.NewRouter()
    api := NewAPI(nil) // nil for coordinator in test
    api.RegisterRoutes(router)

    // Test POST /api/v1/tables
    body := strings.NewReader(`{"tables": ["db1.table1"]}`)
    req := httptest.NewRequest("POST", "/api/v1/tables", body)
    req.Header.Set("Content-Type", "application/json")
    w := httptest.NewRecorder()
    router.ServeHTTP(w, req)

    // Should return 200 or 201
    assert.True(t, w.Code == 200 || w.Code == 201)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/api/... -run TestAPI_TablesEndpoints -v`
Expected: FAIL

- [ ] **Step 3: Implement tables API handlers**

```go
// File: internal/api/tables.go
package api

import (
    "encoding/json"
    "net/http"

    "github.com/gorilla/mux"
)

// TablesRequest represents a request to add/remove tables
type TablesRequest struct {
    Tables []string `json:"tables"`
}

// TablesResponse represents the response for table operations
type TablesResponse struct {
    Results []TableOperationResult `json:"results"`
}

// RegisterTableRoutes registers table management routes
func (a *API) RegisterTableRoutes(r *mux.Router) {
    r.HandleFunc("/api/v1/tables", a.handleAddTables).Methods("POST")
    r.HandleFunc("/api/v1/tables", a.handleRemoveTables).Methods("DELETE")
    r.HandleFunc("/api/v1/tables", a.handleListTables).Methods("GET")
    r.HandleFunc("/api/v1/tables/{db}/{table}", a.handleGetTableStatus).Methods("GET")
    r.HandleFunc("/api/v1/tables/{db}/{table}/pause", a.handlePauseTable).Methods("POST")
    r.HandleFunc("/api/v1/tables/{db}/{table}/resume", a.handleResumeTable).Methods("POST")
}

// handleAddTables handles POST /api/v1/tables
func (a *API) handleAddTables(w http.ResponseWriter, r *http.Request) {
    var req TablesRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        respondError(w, http.StatusBadRequest, "invalid request body")
        return
    }

    if a.coordinator == nil || a.coordinator.TableManager() == nil {
        respondError(w, http.StatusServiceUnavailable, "table manager not available")
        return
    }

    results := a.coordinator.TableManager().AddTables(r.Context(), req.Tables)
    respondJSON(w, http.StatusOK, TablesResponse{Results: results})
}

// handleRemoveTables handles DELETE /api/v1/tables
func (a *API) handleRemoveTables(w http.ResponseWriter, r *http.Request) {
    var req TablesRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        respondError(w, http.StatusBadRequest, "invalid request body")
        return
    }

    if a.coordinator == nil || a.coordinator.TableManager() == nil {
        respondError(w, http.StatusServiceUnavailable, "table manager not available")
        return
    }

    results := a.coordinator.TableManager().RemoveTables(r.Context(), req.Tables)
    respondJSON(w, http.StatusOK, TablesResponse{Results: results})
}

// handleListTables handles GET /api/v1/tables
func (a *API) handleListTables(w http.ResponseWriter, r *http.Request) {
    if a.coordinator == nil || a.coordinator.TableManager() == nil {
        respondError(w, http.StatusServiceUnavailable, "table manager not available")
        return
    }

    tables := a.coordinator.TableManager().ListTables()
    respondJSON(w, http.StatusOK, map[string]interface{}{"tables": tables})
}

// handleGetTableStatus handles GET /api/v1/tables/{db}/{table}
func (a *API) handleGetTableStatus(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    db := vars["db"]
    table := vars["table"]

    if a.coordinator == nil || a.coordinator.TableManager() == nil {
        respondError(w, http.StatusServiceUnavailable, "table manager not available")
        return
    }

    status, err := a.coordinator.TableManager().GetTableStatus(db, table)
    if err != nil {
        respondError(w, http.StatusNotFound, err.Error())
        return
    }

    respondJSON(w, http.StatusOK, status)
}

// handlePauseTable handles POST /api/v1/tables/{db}/{table}/pause
func (a *API) handlePauseTable(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    db := vars["db"]
    table := vars["table"]

    if a.coordinator == nil || a.coordinator.TableManager() == nil {
        respondError(w, http.StatusServiceUnavailable, "table manager not available")
        return
    }

    if err := a.coordinator.TableManager().PauseTable(r.Context(), db, table); err != nil {
        respondError(w, http.StatusNotFound, err.Error())
        return
    }

    respondJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

// handleResumeTable handles POST /api/v1/tables/{db}/{table}/resume
func (a *API) handleResumeTable(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    db := vars["db"]
    table := vars["table"]

    if a.coordinator == nil || a.coordinator.TableManager() == nil {
        respondError(w, http.StatusServiceUnavailable, "table manager not available")
        return
    }

    if err := a.coordinator.TableManager().ResumeTable(r.Context(), db, table); err != nil {
        respondError(w, http.StatusNotFound, err.Error())
        return
    }

    respondJSON(w, http.StatusOK, map[string]string{"status": "resumed"})
}

func respondJSON(w http.ResponseWriter, code int, payload interface{}) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(code)
    json.NewEncoder(w).Encode(payload)
}

func respondError(w http.ResponseWriter, code int, message string) {
    respondJSON(w, code, map[string]string{"error": message})
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/api/... -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/api/tables.go internal/api/api.go
git commit -m "feat(api): add table management API endpoints"
```

---

### Task 5: Update Design Documents

**Files:**
- Modify: `docs/design/pipeline-design.md`
- Modify: `docs/design/connector-design.md`

- [ ] **Step 1: Update pipeline-design.md module structure**

Change from:
```markdown
pkg/
├── pipeline/
├── filter/
├── transform/
└── router/
```

To:
```markdown
internal/
├── pipeline/
├── filter/
├── transform/
└── router/
```

- [ ] **Step 2: Update connector-design.md with DatabaseDiscovery and TableManager sections**

Add note that these are implemented in `internal/source/`.

- [ ] **Step 3: Commit**

```bash
git add docs/design/pipeline-design.md docs/design/connector-design.md
git commit -m "docs: update design documents to reflect actual implementation"
```

---

### Task 6: Final Integration Test

**Files:**
- Run: Full test suite

- [ ] **Step 1: Run all tests**

Run: `go test ./... -v`
Expected: All PASS

- [ ] **Step 2: Run build verification**

Run: `go build ./...`
Expected: No errors

- [ ] **Step 3: Update MEMORY.md with new features**

Add DatabaseDiscovery and TableManager to the completed features list.

---

*Plan version: v1.0*
*Created: 2026-05-13*
