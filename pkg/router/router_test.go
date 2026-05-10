package router

import (
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableRouter(t *testing.T) {
	t.Run("route to mapped table", func(t *testing.T) {
		cfg := &RouterConfig{
			TableMapping: map[string]string{
				"testdb.users":  "sink-users",
				"testdb.orders": "sink-orders",
			},
			DefaultSink: "sink-default",
		}
		tr := NewTableRouter(cfg)

		tests := []struct {
			database string
			table    string
			expected string
		}{
			{"testdb", "users", "sink-users"},
			{"testdb", "orders", "sink-orders"},
			{"testdb", "products", "sink-default"},
			{"otherdb", "users", "sink-default"},
		}

		for _, tt := range tests {
			e := &event.ChangeEvent{
				Type: event.EventTypeInsert,
				Table: event.TableInfo{
					Database: tt.database,
					Table:    tt.table,
				},
			}

			dest, err := tr.Route(e)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, dest)
		}
	})

	t.Run("route to default sink", func(t *testing.T) {
		cfg := &RouterConfig{
			TableMapping: map[string]string{},
			DefaultSink:  "sink-default",
		}
		tr := NewTableRouter(cfg)

		e := &event.ChangeEvent{
			Type: event.EventTypeInsert,
			Table: event.TableInfo{
				Database: "testdb",
				Table:    "unknown",
			},
		}

		dest, err := tr.Route(e)
		require.NoError(t, err)
		assert.Equal(t, "sink-default", dest)
	})

	t.Run("empty default sink", func(t *testing.T) {
		cfg := &RouterConfig{
			TableMapping: map[string]string{},
			DefaultSink:  "",
		}
		tr := NewTableRouter(cfg)

		e := &event.ChangeEvent{
			Type: event.EventTypeInsert,
			Table: event.TableInfo{
				Database: "testdb",
				Table:    "unknown",
			},
		}

		dest, err := tr.Route(e)
		require.NoError(t, err)
		assert.Equal(t, "", dest)
	})
}

func TestPartitionRouter(t *testing.T) {
	t.Run("partition by table", func(t *testing.T) {
		cfg := &RouterConfig{
			PartitionStrategy: PartitionByTable,
			PartitionCount:    4,
		}
		pr := NewPartitionRouter(cfg)

		// Same table should always route to same partition
		e1 := &event.ChangeEvent{
			Type: event.EventTypeInsert,
			Table: event.TableInfo{
				Database: "testdb",
				Table:    "users",
			},
		}
		e2 := &event.ChangeEvent{
			Type: event.EventTypeUpdate,
			Table: event.TableInfo{
				Database: "testdb",
				Table:    "users",
			},
		}

		dest1, err := pr.Route(e1)
		require.NoError(t, err)
		dest2, err := pr.Route(e2)
		require.NoError(t, err)
		assert.Equal(t, dest1, dest2, "same table should route to same partition")
	})

	t.Run("partition by pk", func(t *testing.T) {
		cfg := &RouterConfig{
			PartitionStrategy: PartitionByPK,
			PartitionCount:    4,
		}
		pr := NewPartitionRouter(cfg)

		e := &event.ChangeEvent{
			Type: event.EventTypeInsert,
			Table: event.TableInfo{
				Database:          "testdb",
				Table:             "users",
				PrimaryKeyColumns: []string{"id"},
			},
			After: event.RowData{
				Fields: map[string]event.Field{
					"id":   {Name: "id", Value: 123},
					"name": {Name: "name", Value: "test"},
				},
			},
		}

		dest, err := pr.Route(e)
		require.NoError(t, err)
		// Verify partition is within range
		assert.Contains(t, []string{"0", "1", "2", "3"}, dest)
	})

	t.Run("partition by field", func(t *testing.T) {
		cfg := &RouterConfig{
			PartitionStrategy: PartitionByField,
			PartitionCount:    4,
			PartitionKey:      []string{"user_id"},
		}
		pr := NewPartitionRouter(cfg)

		e := &event.ChangeEvent{
			Type: event.EventTypeInsert,
			Table: event.TableInfo{
				Database: "testdb",
				Table:    "orders",
			},
			After: event.RowData{
				Fields: map[string]event.Field{
					"id":      {Name: "id", Value: 1},
					"user_id": {Name: "user_id", Value: "user-123"},
				},
			},
		}

		dest, err := pr.Route(e)
		require.NoError(t, err)
		assert.Contains(t, []string{"0", "1", "2", "3"}, dest)
	})

	t.Run("partition random", func(t *testing.T) {
		cfg := &RouterConfig{
			PartitionStrategy: PartitionRandom,
			PartitionCount:    4,
		}
		pr := NewPartitionRouter(cfg)

		e := &event.ChangeEvent{
			Type: event.EventTypeInsert,
			Table: event.TableInfo{
				Database: "testdb",
				Table:    "users",
			},
		}

		for i := 0; i < 10; i++ {
			dest, err := pr.Route(e)
			require.NoError(t, err)
			assert.Contains(t, []string{"0", "1", "2", "3"}, dest)
		}
	})

	t.Run("different tables different partitions", func(t *testing.T) {
		cfg := &RouterConfig{
			PartitionStrategy: PartitionByTable,
			PartitionCount:    10,
		}
		pr := NewPartitionRouter(cfg)

		e1 := &event.ChangeEvent{
			Table: event.TableInfo{Database: "db1", Table: "table1"},
		}
		e2 := &event.ChangeEvent{
			Table: event.TableInfo{Database: "db1", Table: "table2"},
		}

		dest1, _ := pr.Route(e1)
		dest2, _ := pr.Route(e2)
		// Different tables might route to different partitions (not guaranteed, but likely)
		// We just verify both are valid partitions
		assert.Contains(t, []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}, dest1)
		assert.Contains(t, []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}, dest2)
	})
}

func TestPartitionStrategyConstants(t *testing.T) {
	assert.Equal(t, PartitionStrategy("table"), PartitionByTable)
	assert.Equal(t, PartitionStrategy("pk"), PartitionByPK)
	assert.Equal(t, PartitionStrategy("field"), PartitionByField)
	assert.Equal(t, PartitionStrategy("random"), PartitionRandom)
}
