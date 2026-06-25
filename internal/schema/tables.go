package schema

import (
	"sync"

	"github.com/UFOXD/datastream/pkg/event"
)

// Tables is an in-memory collection of table definitions.
// It is a pure storage container — schema synthesis is done by Parser.ApplyDDL().
type Tables struct {
	tables map[string]*event.TableInfo // "database.table" → TableInfo
	mu     sync.RWMutex
}

// NewTables creates a new empty Tables collection.
func NewTables() *Tables {
	return &Tables{tables: make(map[string]*event.TableInfo)}
}

func tableKey(database, table string) string {
	return database + "." + table
}

// Put stores or updates a table definition.
func (t *Tables) Put(info *event.TableInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tables[tableKey(info.Database, info.Table)] = info
}

// Get retrieves a table definition. Returns nil if not found.
func (t *Tables) Get(database, table string) *event.TableInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tables[tableKey(database, table)]
}

// Remove deletes a table definition.
func (t *Tables) Remove(database, table string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.tables, tableKey(database, table))
}

// All returns a snapshot of all table definitions.
func (t *Tables) All() map[string]*event.TableInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	m := make(map[string]*event.TableInfo, len(t.tables))
	for k, v := range t.tables {
		m[k] = v
	}
	return m
}

// Count returns the number of stored table definitions.
func (t *Tables) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.tables)
}
