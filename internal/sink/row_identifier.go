package sink

import (
	"fmt"
	"sort"
	"strings"

	"github.com/UFOXD/datastream/pkg/event"
)

// RowKeyType represents the type of row key
type RowKeyType int

const (
	KeyTypePrimaryKey  RowKeyType = iota
	KeyTypeUniqueIndex
	KeyTypeFullRow
)

// RowIdentifier uniquely identifies a row for ordering
type RowIdentifier struct {
	Schema           string
	Database         string
	Table            string
	PrimaryKeyValues string
	KeyType          RowKeyType
}

// BuildRowIdentifier creates a row identifier from an event.
// It tries primary key first, then unique index, then falls back to full row.
func BuildRowIdentifier(e *event.ChangeEvent, schema *event.TableInfo) *RowIdentifier {
	rid := &RowIdentifier{
		Schema:   schema.Schema,
		Database: schema.Database,
		Table:    schema.Table,
	}

	// Choose the row data to extract key values from (prefer After, fall back to Before)
	row := &e.After
	if row.IsEmpty() {
		row = &e.Before
	}

	// 1. Try primary key
	if schema.HasPrimaryKey() {
		rid.PrimaryKeyValues = extractKeyValues(row, schema.PrimaryKeyColumns)
		rid.KeyType = KeyTypePrimaryKey
		return rid
	}

	// 2. Try unique index
	if schema.HasUniqueKey() {
		rid.PrimaryKeyValues = extractKeyValues(row, schema.UniqueKeyColumns)
		rid.KeyType = KeyTypeUniqueIndex
		return rid
	}

	// 3. Fall back to full row
	rid.PrimaryKeyValues = extractFullRow(row)
	rid.KeyType = KeyTypeFullRow
	return rid
}

// extractKeyValues builds a deterministic key string from specific columns.
func extractKeyValues(row *event.RowData, columns []string) string {
	parts := make([]string, 0, len(columns))
	for _, col := range columns {
		val, _ := row.Get(col)
		parts = append(parts, fmt.Sprintf("%s=%v", col, val))
	}
	return strings.Join(parts, ",")
}

// extractFullRow builds a deterministic key string from all fields in sorted order.
func extractFullRow(row *event.RowData) string {
	if row.IsEmpty() {
		return ""
	}
	names := row.ColumnNames()
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		val, _ := row.Get(name)
		parts = append(parts, fmt.Sprintf("%s=%v", name, val))
	}
	return strings.Join(parts, ",")
}

// HashKey generates a hash key for distribution.
// Format: schema.database.table:keyValues
func (r *RowIdentifier) HashKey() string {
	return fmt.Sprintf("%s.%s.%s:%s", r.Schema, r.Database, r.Table, r.PrimaryKeyValues)
}

// String returns a human-readable string representation.
func (r *RowIdentifier) String() string {
	keyTypeName := "fullrow"
	switch r.KeyType {
	case KeyTypePrimaryKey:
		keyTypeName = "pk"
	case KeyTypeUniqueIndex:
		keyTypeName = "uk"
	}
	return fmt.Sprintf("%s.%s.%s[%s:%s]", r.Schema, r.Database, r.Table, keyTypeName, r.PrimaryKeyValues)
}
