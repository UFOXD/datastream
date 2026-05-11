// Package oracle provides an Oracle source connector for DataStream.
package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/UFOXD/datastream/pkg/event"
)

// TableSchemaCache caches table schema information for Oracle tables.
// It queries ALL_TAB_COLUMNS and ALL_CONS_COLUMNS to get column definitions and primary keys.
type TableSchemaCache struct {
	mu      sync.RWMutex
	schemas map[string]*event.TableInfo // key: owner.table
	db      *sql.DB
}

// NewTableSchemaCache creates a new Oracle schema cache.
func NewTableSchemaCache(db *sql.DB) *TableSchemaCache {
	return &TableSchemaCache{
		schemas: make(map[string]*event.TableInfo),
		db:      db,
	}
}

// Get retrieves schema from cache or queries the database.
func (c *TableSchemaCache) Get(ctx context.Context, owner, table string) (*event.TableInfo, error) {
	key := owner + "." + table

	// Check cache first
	c.mu.RLock()
	if schema, ok := c.schemas[key]; ok {
		c.mu.RUnlock()
		return schema.Clone(), nil
	}
	c.mu.RUnlock()

	// Query from database
	schema, err := c.querySchema(ctx, owner, table)
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
func (c *TableSchemaCache) GetOrFetch(ctx context.Context, owner, table string) (*event.TableInfo, error) {
	return c.Get(ctx, owner, table)
}

// Update updates the cached schema for a table.
func (c *TableSchemaCache) Update(owner, table string, schema *event.TableInfo) {
	key := owner + "." + table
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas[key] = schema
}

// Invalidate removes a cached schema.
func (c *TableSchemaCache) Invalidate(owner, table string) {
	key := owner + "." + table
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

// Refresh refreshes the schema for a table from the database.
func (c *TableSchemaCache) Refresh(ctx context.Context, owner, table string) error {
	schema, err := c.querySchema(ctx, owner, table)
	if err != nil {
		return err
	}
	c.Update(owner, table, schema)
	return nil
}

// querySchema queries the table schema from Oracle data dictionary views.
func (c *TableSchemaCache) querySchema(ctx context.Context, owner, table string) (*event.TableInfo, error) {
	info := &event.TableInfo{
		Database: owner, // Oracle uses schema/owner instead of database
		Schema:   owner,
		Table:    table,
	}

	// Query columns from ALL_TAB_COLUMNS
	rows, err := c.db.QueryContext(ctx, `
		SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_LENGTH, DATA_SCALE
		FROM ALL_TAB_COLUMNS
		WHERE OWNER = :1 AND TABLE_NAME = :2
		ORDER BY COLUMN_ID
	`, owner, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	columns := make([]event.ColumnInfo, 0)
	for rows.Next() {
		var colName, dataType, nullable string
		var dataLength, dataScale sql.NullInt64
		if err := rows.Scan(&colName, &dataType, &nullable, &dataLength, &dataScale); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		col := event.ColumnInfo{
			Name:     colName,
			Type:     dataType,
			Nullable: nullable == "Y",
		}
		if dataLength.Valid {
			col.Length = int(dataLength.Int64)
		}
		if dataScale.Valid {
			col.Scale = int(dataScale.Int64)
		}
		columns = append(columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate columns: %w", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s.%s not found or has no columns", owner, table)
	}
	info.Columns = columns

	// Query primary key columns from ALL_CONSTRAINTS + ALL_CONS_COLUMNS
	pkRows, err := c.db.QueryContext(ctx, `
		SELECT cols.COLUMN_NAME
		FROM ALL_CONSTRAINTS cons
		JOIN ALL_CONS_COLUMNS cols
		  ON cons.OWNER = cols.OWNER
		 AND cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
		 AND cons.TABLE_NAME = cols.TABLE_NAME
		WHERE cons.OWNER = :1
		  AND cons.TABLE_NAME = :2
		  AND cons.CONSTRAINT_TYPE = 'P'
		ORDER BY cols.POSITION
	`, owner, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary keys: %w", err)
	}
	defer pkRows.Close()

	pkColumns := make([]string, 0)
	for pkRows.Next() {
		var colName string
		if err := pkRows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %w", err)
		}
		pkColumns = append(pkColumns, colName)
	}
	if err := pkRows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate primary key columns: %w", err)
	}

	info.PrimaryKeyColumns = pkColumns
	return info, nil
}
