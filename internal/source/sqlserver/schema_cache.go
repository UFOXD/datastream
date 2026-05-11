// Package sqlserver provides SQL Server source connector for DataStream.
package sqlserver

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
	schemas map[string]*event.TableInfo // key: schema.table
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
func (c *TableSchemaCache) Get(ctx context.Context, schema, table string) (*event.TableInfo, error) {
	key := schema + "." + table

	// Check cache first
	c.mu.RLock()
	if info, ok := c.schemas[key]; ok {
		c.mu.RUnlock()
		return info.Clone(), nil
	}
	c.mu.RUnlock()

	// Query from database
	info, err := c.querySchema(ctx, schema, table)
	if err != nil {
		return nil, err
	}

	// Cache it
	c.mu.Lock()
	c.schemas[key] = info
	c.mu.Unlock()

	return info.Clone(), nil
}

// GetOrFetch retrieves schema, fetching from DB if not cached.
func (c *TableSchemaCache) GetOrFetch(ctx context.Context, schema, table string) (*event.TableInfo, error) {
	return c.Get(ctx, schema, table)
}

// Update updates the cached schema for a table.
func (c *TableSchemaCache) Update(schema, table string, info *event.TableInfo) {
	key := schema + "." + table
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas[key] = info
}

// Invalidate removes a cached schema.
func (c *TableSchemaCache) Invalidate(schema, table string) {
	key := schema + "." + table
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
func (c *TableSchemaCache) querySchema(ctx context.Context, schema, table string) (*event.TableInfo, error) {
	info := &event.TableInfo{
		Database: schema,
		Table:    table,
	}

	// Query columns
	rows, err := c.db.QueryContext(ctx, `
		SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2
		ORDER BY ORDINAL_POSITION
	`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	columns := make([]event.ColumnInfo, 0)

	for rows.Next() {
		var colName, dataType, isNullable string
		if err := rows.Scan(&colName, &dataType, &isNullable); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		columns = append(columns, event.ColumnInfo{
			Name:     colName,
			Type:     dataType,
			Nullable: isNullable == "YES",
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate columns: %w", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s.%s not found", schema, table)
	}

	info.Columns = columns

	// Query primary key columns
	pkRows, err := c.db.QueryContext(ctx, `
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2
		AND CONSTRAINT_NAME LIKE 'PK_%'
		ORDER BY ORDINAL_POSITION
	`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary keys: %w", err)
	}
	defer pkRows.Close()

	keyColumns := make([]string, 0)

	for pkRows.Next() {
		var colName string
		if err := pkRows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %w", err)
		}
		keyColumns = append(keyColumns, colName)
	}

	if err := pkRows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate primary keys: %w", err)
	}

	info.PrimaryKeyColumns = keyColumns

	return info, nil
}

// Refresh refreshes the schema for a table from the database.
func (c *TableSchemaCache) Refresh(ctx context.Context, schema, table string) error {
	info, err := c.querySchema(ctx, schema, table)
	if err != nil {
		return err
	}

	c.Update(schema, table, info)
	return nil
}
