package elasticsearch

import (
	"fmt"
	"strings"

	"github.com/UFOXD/datastream/pkg/event"
)

// BulkAction represents a single Elasticsearch bulk API action.
type BulkAction struct {
	Index string                 // Target index name
	ID    string                 // Document ID
	Op    string                 // Operation: "index", "update", "delete"
	Doc   map[string]interface{} // Document body (nil for delete)
}

// DocumentMapper maps ChangeEvents to Elasticsearch BulkActions.
type DocumentMapper struct {
	config *Config
}

// NewDocumentMapper creates a new DocumentMapper with the given config.
func NewDocumentMapper(config *Config) *DocumentMapper {
	return &DocumentMapper{config: config}
}

// GenerateDocID generates a document ID from the primary key columns of a row.
// Multiple PK columns are joined with underscore. Missing columns are skipped.
func (m *DocumentMapper) GenerateDocID(row event.RowData, pkColumns []string) string {
	var parts []string
	for _, col := range pkColumns {
		if val, ok := row.Fields[col]; ok {
			parts = append(parts, fmt.Sprintf("%v", val.Value))
		}
	}
	return strings.Join(parts, "_")
}

// ResolveIndex resolves the Elasticsearch index name for a given table.
// Replaces {database} and {table} placeholders and prepends IndexPrefix if set.
func (m *DocumentMapper) ResolveIndex(table event.TableInfo) string {
	indexName := m.config.IndexPattern
	indexName = strings.ReplaceAll(indexName, "{database}", strings.ToLower(table.Database))
	indexName = strings.ReplaceAll(indexName, "{table}", strings.ToLower(table.Table))
	if m.config.IndexPrefix != "" {
		indexName = m.config.IndexPrefix + indexName
	}
	return indexName
}

// BuildDocument converts RowData into a plain map suitable for Elasticsearch indexing.
func (m *DocumentMapper) BuildDocument(row event.RowData) map[string]interface{} {
	doc := make(map[string]interface{}, len(row.Fields))
	for name, field := range row.Fields {
		doc[name] = field.Value
	}
	return doc
}

// MapEvent maps a ChangeEvent to a BulkAction.
// Returns nil for non-data events (DDL, heartbeat, etc.).
func (m *DocumentMapper) MapEvent(e *event.ChangeEvent) *BulkAction {
	switch e.Type {
	case event.EventTypeInsert:
		return &BulkAction{
			Index: m.ResolveIndex(e.Table),
			ID:    m.GenerateDocID(e.After, e.Table.PrimaryKeyColumns),
			Op:    "index",
			Doc:   m.BuildDocument(e.After),
		}

	case event.EventTypeUpdate:
		return &BulkAction{
			Index: m.ResolveIndex(e.Table),
			ID:    m.GenerateDocID(e.After, e.Table.PrimaryKeyColumns),
			Op:    "update",
			Doc:   m.BuildDocument(e.After),
		}

	case event.EventTypeDelete:
		return &BulkAction{
			Index: m.ResolveIndex(e.Table),
			ID:    m.GenerateDocID(e.Before, e.Table.PrimaryKeyColumns),
			Op:    "delete",
			Doc:   nil,
		}

	default:
		return nil
	}
}
