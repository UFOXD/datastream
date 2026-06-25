// Package parser provides DDL parsing functionality for DataStream.
package parser

import (
	"context"

	"github.com/UFOXD/datastream/pkg/event"
)

// DDLParser defines the interface for DDL statement parsing.
// Only MySQL/MariaDB require full DDL parsing; other databases
// provide structured output through their CDC mechanisms.
type DDLParser interface {
	// Parse parses one or more DDL statements (separated by semicolons) and returns structured results.
	// Returns a slice of DDLResult, one for each successfully parsed statement.
	Parse(ctx context.Context, ddl string) ([]*DDLResult, error)

	// ApplyDDL applies a DDL statement to a table structure and returns the resulting new table structure.
	// For CREATE: oldTable is nil, returns new TableInfo built from the DDL.
	// For ALTER: oldTable is the existing structure, returns cloned structure with changes applied.
	// For DROP: returns DDLResult with NewTableInfo=nil.
	ApplyDDL(ctx context.Context, oldTable *event.TableInfo, ddl string) (*DDLResult, error)

	// SupportedTypes returns the DDL types this parser can handle.
	SupportedTypes() []DDLType
}

// DDLResults holds multiple DDL parsing results for use in visitor pattern.
type DDLResults struct {
	Results []*DDLResult
}

// Add appends a result to the collection.
func (r *DDLResults) Add(result *DDLResult) {
	if result != nil {
		r.Results = append(r.Results, result)
	}
}

// DDLType represents the type of DDL statement.
type DDLType string

const (
	// Database operations
	DDLTypeCreateDatabase DDLType = "create_database"
	DDLTypeDropDatabase   DDLType = "drop_database"
	DDLTypeAlterDatabase  DDLType = "alter_database"

	// Table operations
	DDLTypeCreateTable DDLType = "create_table"
	DDLTypeDropTable   DDLType = "drop_table"
	DDLTypeAlterTable  DDLType = "alter_table"
	DDLTypeRenameTable DDLType = "rename_table"
	DDLTypeTruncate    DDLType = "truncate_table"

	// Index operations
	DDLTypeCreateIndex DDLType = "create_index"
	DDLTypeDropIndex   DDLType = "drop_index"

	// View operations
	DDLTypeCreateView DDLType = "create_view"
	DDLTypeDropView   DDLType = "drop_view"

	// Unknown or unsupported
	DDLTypeUnknown DDLType = "unknown"
)

// DDLResult contains the parsed result of a DDL statement.
type DDLResult struct {
	// DDL type
	Type DDLType `json:"type"`

	// Database name (if applicable)
	Database string `json:"database,omitempty"`

	// Table name (if applicable)
	Table string `json:"table,omitempty"`

	// Original DDL statement
	Statement string `json:"statement"`

	// Table structure changes (for CREATE/ALTER TABLE)
	TableChanges *TableChanges `json:"tableChanges,omitempty"`

	// Index changes (for CREATE/DROP INDEX)
	IndexChanges *IndexChanges `json:"indexChanges,omitempty"`

	// NewTableInfo is the resulting table structure after applying the DDL.
	// For CREATE: built from DDL column definitions.
	// For ALTER: cloned from oldTable with changes applied.
	// For DROP: nil.
	NewTableInfo *event.TableInfo `json:"newTableInfo,omitempty"`

	// Additional metadata
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// TableOperation represents the type of table operation.
type TableOperation string

const (
	TableOpCreate TableOperation = "create"
	TableOpAlter  TableOperation = "alter"
	TableOpDrop   TableOperation = "drop"
	TableOpRename TableOperation = "rename"
)

// TableChanges represents changes to a table structure.
type TableChanges struct {
	// Operation type
	Operation TableOperation `json:"operation"`

	// Table information
	Table *TableInfo `json:"table,omitempty"`

	// Added columns
	AddedColumns []ColumnInfo `json:"addedColumns,omitempty"`

	// Dropped columns
	DroppedColumns []string `json:"droppedColumns,omitempty"`

	// Modified columns
	ModifiedColumns []ColumnModification `json:"modifiedColumns,omitempty"`

	// Primary key change
	PrimaryKeyChange *PrimaryKeyChange `json:"primaryKeyChange,omitempty"`
}

// TableInfo contains table metadata.
type TableInfo struct {
	// Database name
	Database string `json:"database"`

	// Table name
	Name string `json:"name"`

	// Column definitions
	Columns []ColumnInfo `json:"columns"`

	// Primary key columns
	PrimaryKey []string `json:"primaryKey,omitempty"`

	// Index definitions
	Indexes []IndexInfo `json:"indexes,omitempty"`

	// Table comment
	Comment string `json:"comment,omitempty"`
}

// ColumnInfo contains column metadata.
type ColumnInfo struct {
	// Column name
	Name string `json:"name"`

	// Column type (e.g., "INT", "VARCHAR(255)")
	Type string `json:"type"`

	// Whether the column is nullable
	Nullable bool `json:"nullable"`

	// Default value
	DefaultValue interface{} `json:"defaultValue,omitempty"`

	// Column comment
	Comment string `json:"comment,omitempty"`

	// Whether it's an auto-increment column
	AutoIncrement bool `json:"autoIncrement,omitempty"`
}

// ColumnModification represents a column change.
type ColumnModification struct {
	// Old column definition
	Old ColumnInfo `json:"old"`

	// New column definition
	New ColumnInfo `json:"new"`
}

// PrimaryKeyChange represents a primary key modification.
type PrimaryKeyChange struct {
	// Old primary key columns
	OldColumns []string `json:"oldColumns,omitempty"`

	// New primary key columns
	NewColumns []string `json:"newColumns,omitempty"`
}

// IndexInfo contains index metadata.
type IndexInfo struct {
	// Index name
	Name string `json:"name"`

	// Index type (e.g., "BTREE", "HASH")
	Type string `json:"type,omitempty"`

	// Indexed columns
	Columns []string `json:"columns"`

	// Whether it's a unique index
	Unique bool `json:"unique"`

	// Whether it's a primary key
	Primary bool `json:"primary,omitempty"`
}

// IndexChanges represents index modifications.
type IndexChanges struct {
	// Index name
	IndexName string `json:"indexName"`

	// Table name
	TableName string `json:"tableName"`

	// Database name
	DatabaseName string `json:"databaseName"`

	// Operation type
	Operation string `json:"operation"` // "create" or "drop"

	// Index info (for create)
	Index *IndexInfo `json:"index,omitempty"`
}
