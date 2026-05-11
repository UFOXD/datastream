package elasticsearch

import (
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDocumentMapper(t *testing.T) {
	cfg := DefaultConfig()
	m := NewDocumentMapper(cfg)
	require.NotNil(t, m)
}

func TestDocumentMapper_GenerateDocID(t *testing.T) {
	cfg := DefaultConfig()
	m := NewDocumentMapper(cfg)

	tests := []struct {
		name      string
		row       event.RowData
		pkColumns []string
		expected  string
	}{
		{
			name: "single primary key",
			row: event.RowData{
				Fields: map[string]event.Field{
					"id": {Name: "id", Value: int64(42), Type: "bigint"},
				},
			},
			pkColumns: []string{"id"},
			expected:  "42",
		},
		{
			name: "composite primary key",
			row: event.RowData{
				Fields: map[string]event.Field{
					"order_id": {Name: "order_id", Value: int64(1), Type: "bigint"},
					"item_id":  {Name: "item_id", Value: int64(2), Type: "bigint"},
				},
			},
			pkColumns: []string{"order_id", "item_id"},
			expected:  "1_2",
		},
		{
			name: "string primary key",
			row: event.RowData{
				Fields: map[string]event.Field{
					"uuid": {Name: "uuid", Value: "abc-123", Type: "varchar"},
				},
			},
			pkColumns: []string{"uuid"},
			expected:  "abc-123",
		},
		{
			name: "missing pk column falls back to empty string part",
			row: event.RowData{
				Fields: map[string]event.Field{
					"id": {Name: "id", Value: int64(1), Type: "bigint"},
				},
			},
			pkColumns: []string{"id", "missing_col"},
			expected:  "1",
		},
		{
			name:      "empty pk columns",
			row:       event.RowData{Fields: map[string]event.Field{}},
			pkColumns: []string{},
			expected:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := m.GenerateDocID(tt.row, tt.pkColumns)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestDocumentMapper_ResolveIndex(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		table    event.TableInfo
		expected string
	}{
		{
			name: "default pattern",
			config: &Config{
				IndexPattern: "{database}_{table}",
			},
			table: event.TableInfo{
				Database: "mydb",
				Table:    "orders",
			},
			expected: "mydb_orders",
		},
		{
			name: "with prefix",
			config: &Config{
				IndexPattern: "{database}_{table}",
				IndexPrefix:  "prod_",
			},
			table: event.TableInfo{
				Database: "mydb",
				Table:    "orders",
			},
			expected: "prod_mydb_orders",
		},
		{
			name: "uppercase database and table lowercased",
			config: &Config{
				IndexPattern: "{database}_{table}",
			},
			table: event.TableInfo{
				Database: "MyDB",
				Table:    "Orders",
			},
			expected: "mydb_orders",
		},
		{
			name: "table only pattern",
			config: &Config{
				IndexPattern: "events_{table}",
			},
			table: event.TableInfo{
				Database: "mydb",
				Table:    "users",
			},
			expected: "events_users",
		},
		{
			name: "database only pattern",
			config: &Config{
				IndexPattern: "{database}",
			},
			table: event.TableInfo{
				Database: "AppDB",
				Table:    "users",
			},
			expected: "appdb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewDocumentMapper(tt.config)
			got := m.ResolveIndex(tt.table)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestDocumentMapper_BuildDocument(t *testing.T) {
	cfg := DefaultConfig()
	m := NewDocumentMapper(cfg)

	row := event.RowData{
		Fields: map[string]event.Field{
			"id":    {Name: "id", Value: int64(1), Type: "bigint"},
			"name":  {Name: "name", Value: "Alice", Type: "varchar"},
			"score": {Name: "score", Value: 9.5, Type: "double"},
			"nil_field": {Name: "nil_field", Value: nil, Type: "varchar", Null: true},
		},
	}

	doc := m.BuildDocument(row)
	require.NotNil(t, doc)
	assert.Equal(t, int64(1), doc["id"])
	assert.Equal(t, "Alice", doc["name"])
	assert.Equal(t, 9.5, doc["score"])
	assert.Nil(t, doc["nil_field"])
}

func TestDocumentMapper_MapEvent(t *testing.T) {
	cfg := &Config{
		IndexPattern:    "{database}_{table}",
		RetryOnConflict: 3,
	}
	m := NewDocumentMapper(cfg)

	table := event.TableInfo{
		Database:          "testdb",
		Table:             "users",
		PrimaryKeyColumns: []string{"id"},
	}

	t.Run("insert event maps to index op", func(t *testing.T) {
		e := &event.ChangeEvent{
			Type:  event.EventTypeInsert,
			Table: table,
			After: event.RowData{
				Fields: map[string]event.Field{
					"id":   {Name: "id", Value: int64(1), Type: "bigint"},
					"name": {Name: "name", Value: "Bob", Type: "varchar"},
				},
			},
		}

		action := m.MapEvent(e)
		require.NotNil(t, action)
		assert.Equal(t, "testdb_users", action.Index)
		assert.Equal(t, "1", action.ID)
		assert.Equal(t, "index", action.Op)
		assert.Equal(t, int64(1), action.Doc["id"])
		assert.Equal(t, "Bob", action.Doc["name"])
	})

	t.Run("update event maps to update op", func(t *testing.T) {
		e := &event.ChangeEvent{
			Type:  event.EventTypeUpdate,
			Table: table,
			Before: event.RowData{
				Fields: map[string]event.Field{
					"id":   {Name: "id", Value: int64(2), Type: "bigint"},
					"name": {Name: "name", Value: "Old", Type: "varchar"},
				},
			},
			After: event.RowData{
				Fields: map[string]event.Field{
					"id":   {Name: "id", Value: int64(2), Type: "bigint"},
					"name": {Name: "name", Value: "New", Type: "varchar"},
				},
			},
		}

		action := m.MapEvent(e)
		require.NotNil(t, action)
		assert.Equal(t, "testdb_users", action.Index)
		assert.Equal(t, "2", action.ID)
		assert.Equal(t, "update", action.Op)
		assert.Equal(t, "New", action.Doc["name"])
	})

	t.Run("delete event maps to delete op with no doc", func(t *testing.T) {
		e := &event.ChangeEvent{
			Type:  event.EventTypeDelete,
			Table: table,
			Before: event.RowData{
				Fields: map[string]event.Field{
					"id":   {Name: "id", Value: int64(3), Type: "bigint"},
					"name": {Name: "name", Value: "Carol", Type: "varchar"},
				},
			},
		}

		action := m.MapEvent(e)
		require.NotNil(t, action)
		assert.Equal(t, "testdb_users", action.Index)
		assert.Equal(t, "3", action.ID)
		assert.Equal(t, "delete", action.Op)
		assert.Nil(t, action.Doc)
	})

	t.Run("DDL event returns nil", func(t *testing.T) {
		e := &event.ChangeEvent{
			Type:  event.EventTypeDDL,
			Table: table,
		}
		action := m.MapEvent(e)
		assert.Nil(t, action)
	})

	t.Run("heartbeat event returns nil", func(t *testing.T) {
		e := &event.ChangeEvent{
			Type:  event.EventTypeHeartbeat,
			Table: table,
		}
		action := m.MapEvent(e)
		assert.Nil(t, action)
	})
}

func TestDocumentMapper_MapEvent_CompositeKey(t *testing.T) {
	cfg := &Config{
		IndexPattern: "{database}_{table}",
	}
	m := NewDocumentMapper(cfg)

	table := event.TableInfo{
		Database:          "shop",
		Table:             "order_items",
		PrimaryKeyColumns: []string{"order_id", "item_id"},
	}

	e := &event.ChangeEvent{
		Type:  event.EventTypeInsert,
		Table: table,
		After: event.RowData{
			Fields: map[string]event.Field{
				"order_id": {Name: "order_id", Value: int64(10), Type: "bigint"},
				"item_id":  {Name: "item_id", Value: int64(20), Type: "bigint"},
				"qty":      {Name: "qty", Value: int64(3), Type: "int"},
			},
		},
	}

	action := m.MapEvent(e)
	require.NotNil(t, action)
	assert.Equal(t, "shop_order_items", action.Index)
	assert.Equal(t, "10_20", action.ID)
	assert.Equal(t, "index", action.Op)
}
