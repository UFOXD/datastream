// Package source defines the source connector interfaces for DataStream.
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
	TableOpAdd    TableOperationType = "add"
	TableOpRemove TableOperationType = "remove"
	TableOpPause  TableOperationType = "paused"
	TableOpResume TableOperationType = "resumed"
)

// TableOperationEvent represents a table operation event.
type TableOperationEvent struct {
	Operation TableOperationType
	TableID   TableID
	Schema    *event.TableInfo
	Timestamp time.Time
}

// TableID identifies a table.
type TableID struct {
	Database string
	Schema   string // optional, for PostgreSQL
	Table    string
}

// String returns "database.table" or "schema.database.table" representation.
func (t TableID) String() string {
	if t.Schema != "" {
		return fmt.Sprintf("%s.%s.%s", t.Database, t.Schema, t.Table)
	}
	return fmt.Sprintf("%s.%s", t.Database, t.Table)
}

// TableSyncStatus represents sync status of a table.
type TableSyncStatus string

const (
	TableStatusPending   TableSyncStatus = "pending"
	TableStatusSnapshot  TableSyncStatus = "snapshot"
	TableStatusStreaming TableSyncStatus = "streaming"
	TableStatusPaused   TableSyncStatus = "paused"
	TableStatusError    TableSyncStatus = "error"
)

// TableSyncState represents the sync state of a table.
type TableSyncState struct {
	TableID     TableID
	Status      TableSyncStatus
	Schema      *event.TableInfo
	AddedAt     time.Time
	SyncStarted time.Time
	PausedAt    time.Time `json:"paused_at,omitempty"`
	Error       error
}

// SchemaFetcher fetches table schema information.
type SchemaFetcher interface {
	FetchSchema(ctx context.Context, database, table string) (*event.TableInfo, error)
}

// TableManagerConfig holds configuration for TableManager.
type TableManagerConfig struct {
	// Scope defines the table-level sync scope.
	Scope *TableScope

	// SchemaFetcher fetches table schemas (optional, may be nil).
	SchemaFetcher SchemaFetcher

	// EventChannel receives table operation events.
	EventChannel chan *TableOperationEvent
}

// TableManager manages tables for table-level synchronization.
type TableManager struct {
	scope         *TableScope
	schemaFetcher SchemaFetcher
	eventCh       chan *TableOperationEvent
	syncTables    map[string]*TableSyncState
	mu            sync.RWMutex
}

// NewTableManager creates a new TableManager.
func NewTableManager(cfg *TableManagerConfig) *TableManager {
	tm := &TableManager{
		scope:         cfg.Scope,
		schemaFetcher: cfg.SchemaFetcher,
		eventCh:       cfg.EventChannel,
		syncTables:    make(map[string]*TableSyncState),
	}

	// Pre-populate from scope if provided
	if cfg.Scope != nil {
		for _, entry := range cfg.Scope.ParseTableNames() {
			key := tableKey(entry.Database, entry.Table)
			tm.syncTables[key] = &TableSyncState{
				TableID: TableID{
					Database: entry.Database,
					Table:    entry.Table,
				},
				Status:  TableStatusPending,
				AddedAt: time.Now(),
			}
		}
	}

	return tm
}

// tableKey returns a map key for database.table.
func tableKey(database, table string) string {
	return database + "." + table
}

// parseTableName parses "database.table" into its components.
func parseTableName(name string) (database, table string, err error) {
	parts := strings.SplitN(name, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", ErrInvalidConfig.GenWithStackByArgs(fmt.Sprintf("invalid table name %q, expected format: database.table", name))
	}
	return parts[0], parts[1], nil
}

// AddTables adds tables to the sync scope and emits TableOpAdd events.
func (tm *TableManager) AddTables(ctx context.Context, tables []string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for _, name := range tables {
		database, table, err := parseTableName(name)
		if err != nil {
			return err
		}

		key := tableKey(database, table)
		if _, exists := tm.syncTables[key]; exists {
			return ErrTableAlreadyExists.GenWithStackByArgs(name)
		}

		// Fetch schema if fetcher is available
		var schema *event.TableInfo
		if tm.schemaFetcher != nil {
			schema, err = tm.schemaFetcher.FetchSchema(ctx, database, table)
			if err != nil {
				return err
			}
		}

		tableID := TableID{Database: database, Table: table}
		now := time.Now()

		tm.syncTables[key] = &TableSyncState{
			TableID: tableID,
			Status:  TableStatusPending,
			Schema:  schema,
			AddedAt: now,
		}

		// Update scope names if scope is provided
		if tm.scope != nil {
			tm.scope.Names = append(tm.scope.Names, name)
		}

		// Emit event
		if tm.eventCh != nil {
			tm.eventCh <- &TableOperationEvent{
				Operation: TableOpAdd,
				TableID:   tableID,
				Schema:    schema,
				Timestamp: now,
			}
		}
	}

	return nil
}

// RemoveTables removes tables from the sync scope and emits TableOpRemove events.
func (tm *TableManager) RemoveTables(ctx context.Context, tables []string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for _, name := range tables {
		database, table, err := parseTableName(name)
		if err != nil {
			return err
		}

		key := tableKey(database, table)
		state, exists := tm.syncTables[key]
		if !exists {
			return ErrTableNotFound.GenWithStackByArgs(name)
		}

		delete(tm.syncTables, key)

		// Remove from scope names if scope is provided
		if tm.scope != nil {
			names := tm.scope.Names[:0]
			for _, n := range tm.scope.Names {
				if n != name {
					names = append(names, n)
				}
			}
			tm.scope.Names = names
		}

		// Emit event
		if tm.eventCh != nil {
			tm.eventCh <- &TableOperationEvent{
				Operation: TableOpRemove,
				TableID:   state.TableID,
				Schema:    state.Schema,
				Timestamp: time.Now(),
			}
		}
	}

	return nil
}

// HasTable returns true if the table is in the sync scope.
func (tm *TableManager) HasTable(database, table string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	_, exists := tm.syncTables[tableKey(database, table)]
	return exists
}

// ListTables returns all table names in "database.table" format.
func (tm *TableManager) ListTables() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tables := make([]string, 0, len(tm.syncTables))
	for key := range tm.syncTables {
		tables = append(tables, key)
	}
	return tables
}

// GetTableState returns the sync state for the given table.
func (tm *TableManager) GetTableState(database, table string) (*TableSyncState, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	state, exists := tm.syncTables[tableKey(database, table)]
	if !exists {
		return nil, ErrTableNotFound.GenWithStackByArgs(tableKey(database, table))
	}
	return state, nil
}

// UpdateTableStatus updates the sync status for the given table.
func (tm *TableManager) UpdateTableStatus(database, table string, status TableSyncStatus) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	key := tableKey(database, table)
	state, exists := tm.syncTables[key]
	if !exists {
		return ErrTableNotFound.GenWithStackByArgs(key)
	}

	state.Status = status
	if status == TableStatusStreaming && state.SyncStarted.IsZero() {
		state.SyncStarted = time.Now()
	}

	return nil
}

// PauseTable pauses syncing of a table.
func (tm *TableManager) PauseTable(ctx context.Context, database, table string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	key := database + "." + table
	state, exists := tm.syncTables[key]
	if !exists {
		return fmt.Errorf("table %s.%s not found", database, table)
	}

	state.Status = TableStatusPaused
	state.PausedAt = time.Now()

	if tm.eventCh != nil {
		tm.eventCh <- &TableOperationEvent{
			Operation: TableOpPause,
			TableID: TableID{
				Database: database,
				Table:    table,
			},
			Timestamp: time.Now(),
		}
	}

	return nil
}

// ResumeTable resumes syncing of a paused table.
func (tm *TableManager) ResumeTable(ctx context.Context, database, table string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	key := database + "." + table
	state, exists := tm.syncTables[key]
	if !exists {
		return fmt.Errorf("table %s.%s not found", database, table)
	}

	state.Status = TableStatusPending
	state.PausedAt = time.Time{} // Clear paused time

	if tm.eventCh != nil {
		tm.eventCh <- &TableOperationEvent{
			Operation: TableOpResume,
			TableID: TableID{
				Database: database,
				Table:    table,
			},
			Timestamp: time.Now(),
		}
	}

	return nil
}

// Events returns a read-only channel for table operation events.
func (tm *TableManager) Events() <-chan *TableOperationEvent {
	return tm.eventCh
}
