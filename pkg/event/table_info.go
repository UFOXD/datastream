package event

import "fmt"

// TableInfo holds table metadata.
type TableInfo struct {
	// Database name
	Database string `json:"database"`

	// Schema name (PostgreSQL/Oracle)
	Schema string `json:"schema,omitempty"`

	// Table name
	Table string `json:"table"`

	// Primary key columns
	PrimaryKeyColumns []string `json:"primaryKeyColumns,omitempty"`

	// Unique key columns (for tables without primary key)
	UniqueKeyColumns []string `json:"uniqueKeyColumns,omitempty"`

	// Column information
	Columns []ColumnInfo `json:"columns,omitempty"`
}

// ColumnInfo holds column metadata.
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`     // Database native type
	Nullable bool   `json:"nullable"`
	Length   int    `json:"length,omitempty"`
	Scale    int    `json:"scale,omitempty"`
}

// String returns a string representation of the table.
func (t *TableInfo) String() string {
	if t.Schema != "" {
		return fmt.Sprintf("%s.%s.%s", t.Database, t.Schema, t.Table)
	}
	return fmt.Sprintf("%s.%s", t.Database, t.Table)
}

// HasPrimaryKey returns true if the table has a primary key.
func (t *TableInfo) HasPrimaryKey() bool {
	return len(t.PrimaryKeyColumns) > 0
}

// HasUniqueKey returns true if the table has a unique key.
func (t *TableInfo) HasUniqueKey() bool {
	return len(t.UniqueKeyColumns) > 0
}

// GetKeyColumns returns the columns used for identifying rows.
// Returns primary key columns if available, otherwise unique key columns.
func (t *TableInfo) GetKeyColumns() []string {
	if len(t.PrimaryKeyColumns) > 0 {
		return t.PrimaryKeyColumns
	}
	return t.UniqueKeyColumns
}

// GetColumn returns the ColumnInfo for a given column name.
func (t *TableInfo) GetColumn(name string) *ColumnInfo {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return &t.Columns[i]
		}
	}
	return nil
}

// Clone returns a deep copy of the TableInfo.
func (t *TableInfo) Clone() *TableInfo {
	return &TableInfo{
		Database:          t.Database,
		Schema:            t.Schema,
		Table:             t.Table,
		PrimaryKeyColumns: copyStringSlice(t.PrimaryKeyColumns),
		UniqueKeyColumns:  copyStringSlice(t.UniqueKeyColumns),
		Columns:           copyColumnSlice(t.Columns),
	}
}

func copyStringSlice(s []string) []string {
	if s == nil {
		return nil
	}
	result := make([]string, len(s))
	copy(result, s)
	return result
}

func copyColumnSlice(s []ColumnInfo) []ColumnInfo {
	if s == nil {
		return nil
	}
	result := make([]ColumnInfo, len(s))
	copy(result, s)
	return result
}
