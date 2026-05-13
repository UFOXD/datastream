// Package mysql provides a MySQL source connector for DataStream.
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/UFOXD/datastream/pkg/event"
)

// TableSchemaCache caches table schema information.
// It queries INFORMATION_SCHEMA to get column definitions and primary keys.
type TableSchemaCache struct {
	mu      sync.RWMutex
	schemas map[string]*event.TableInfo // key: database.table
	db      *sql.DB
}

// NewTableSchemaCache creates a new schema cache.
func NewTableSchemaCache(db *sql.DB) *TableSchemaCache {
	return &TableSchemaCache{
		schemas: make(map[string]*event.TableInfo),
		db:      db,
	}
}

// Get retrieves schema from cache or queries the database.
func (c *TableSchemaCache) Get(ctx context.Context, database, table string) (*event.TableInfo, error) {
	key := database + "." + table

	// Check cache first
	c.mu.RLock()
	if schema, ok := c.schemas[key]; ok {
		c.mu.RUnlock()
		return schema.Clone(), nil
	}
	c.mu.RUnlock()

	// Query from database
	schema, err := c.querySchema(ctx, database, table)
	if err != nil {
		return nil, err
	}

	// Cache it
	c.mu.Lock()
	c.schemas[key] = schema
	c.mu.Unlock()

	return schema.Clone(), nil
}

// GetOrFetch retrieves schema, fetching from DB if not cached.
func (c *TableSchemaCache) GetOrFetch(ctx context.Context, database, table string) (*event.TableInfo, error) {
	return c.Get(ctx, database, table)
}

// Update updates the cached schema for a table.
func (c *TableSchemaCache) Update(database, table string, schema *event.TableInfo) {
	key := database + "." + table
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas[key] = schema
}

// Invalidate removes a cached schema.
func (c *TableSchemaCache) Invalidate(database, table string) {
	key := database + "." + table
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.schemas, key)
}

// InvalidateAll clears all cached schemas.
func (c *TableSchemaCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas = make(map[string]*event.TableInfo)
}

// querySchema queries the table schema from INFORMATION_SCHEMA.
func (c *TableSchemaCache) querySchema(ctx context.Context, database, table string) (*event.TableInfo, error) {
	info := &event.TableInfo{
		Database: database,
		Table:    table,
	}

	// Query columns
	rows, err := c.db.QueryContext(ctx, `
		SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_TYPE
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`, database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	columns := make([]event.ColumnInfo, 0)
	keyColumns := make([]string, 0)

	for rows.Next() {
		var colName, dataType, isNullable, columnKey, columnType string
		if err := rows.Scan(&colName, &dataType, &isNullable, &columnKey, &columnType); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		columns = append(columns, event.ColumnInfo{
			Name:     colName,
			Type:     columnType, // Use full column type (e.g., "varchar(255)")
			Nullable: isNullable == "YES",
		})

		if columnKey == "PRI" {
			keyColumns = append(keyColumns, colName)
		}
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s.%s not found", database, table)
	}

	info.Columns = columns
	info.PrimaryKeyColumns = keyColumns

	return info, nil
}

// All returns a snapshot of all cached schemas.
// The returned map is a copy; mutations do not affect the internal cache.
// Each value is cloned for safety.
func (c *TableSchemaCache) All() map[string]*event.TableInfo {
	if c == nil {
		return make(map[string]*event.TableInfo)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]*event.TableInfo, len(c.schemas))
	for k, v := range c.schemas {
		result[k] = v.Clone()
	}
	return result
}

// Refresh refreshes the schema for a table from the database.
func (c *TableSchemaCache) Refresh(ctx context.Context, database, table string) error {
	schema, err := c.querySchema(ctx, database, table)
	if err != nil {
		return err
	}

	c.Update(database, table, schema)
	return nil
}
