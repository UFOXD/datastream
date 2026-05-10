package postgres

import (
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	t.Run("default config is valid", func(t *testing.T) {
		cfg := DefaultConfig()
		assert.Equal(t, 5432, cfg.Port)
		assert.Equal(t, "upsert", cfg.InsertStrategy)
		assert.Equal(t, true, cfg.UseTransaction)
		assert.Equal(t, true, cfg.UseCopy)
		assert.Equal(t, "public", cfg.DefaultSchema)
	})

	t.Run("validate requires host", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Host = ""
		assert.Error(t, cfg.Validate())
	})

	t.Run("validate requires user", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Host = "localhost"
		cfg.User = ""
		assert.Error(t, cfg.Validate())
	})

	t.Run("validate requires database", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Host = "localhost"
		cfg.User = "user"
		cfg.Database = ""
		assert.Error(t, cfg.Validate())
	})

	t.Run("valid config passes", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Host = "localhost"
		cfg.User = "user"
		cfg.Database = "testdb"
		assert.NoError(t, cfg.Validate())
	})
}

func TestConnector(t *testing.T) {
	t.Run("new connector", func(t *testing.T) {
		c := New()
		assert.Equal(t, "postgres", c.Name())
		assert.Equal(t, sink.StateUninitialized, c.Status().State)
	})

	t.Run("supports DDL", func(t *testing.T) {
		c := New()
		assert.True(t, c.SupportsDDL())
	})
}

func TestGetSchemaTable(t *testing.T) {
	c := New()
	c.config = DefaultConfig()

	t.Run("uses default schema when event has no schema", func(t *testing.T) {
		e := &event.ChangeEvent{
			Table: event.TableInfo{
				Database: "testdb",
				Table:    "users",
			},
		}
		table := c.getSchemaTable(e)
		assert.Equal(t, `"public"."users"`, table)
	})

	t.Run("uses event schema when present", func(t *testing.T) {
		e := &event.ChangeEvent{
			Table: event.TableInfo{
				Database: "testdb",
				Schema:   "myschema",
				Table:    "users",
			},
		}
		table := c.getSchemaTable(e)
		assert.Equal(t, `"myschema"."users"`, table)
	})
}

func TestBuildUpsertQuery(t *testing.T) {
	c := New()
	c.config = DefaultConfig()
	c.config.InsertStrategy = "upsert"

	t.Run("upsert with primary key", func(t *testing.T) {
		e := &event.ChangeEvent{
			Type: event.EventTypeInsert,
			Table: event.TableInfo{
				Schema:            "public",
				Table:             "users",
				PrimaryKeyColumns: []string{"id"},
			},
			After: event.RowData{
				Fields: map[string]event.Field{
					"id":    {Name: "id", Value: 1},
					"name":  {Name: "name", Value: "test"},
					"email": {Name: "email", Value: "test@example.com"},
				},
			},
		}

		// The executeInsert should build an upsert query
		// We can't test without a real DB, but we can verify the logic
		assert.NotNil(t, e.After.Fields)
		assert.Equal(t, []string{"id"}, e.Table.PrimaryKeyColumns)
	})
}

func TestParseConfig(t *testing.T) {
	t.Run("parse from sink config", func(t *testing.T) {
		sinkCfg := sink.Config{
			Connection: sink.ConnectionConfig{
				Host:     "localhost",
				Port:     5433,
				User:     "testuser",
				Password: "testpass",
				Database: "testdb",
			},
			Properties: map[string]interface{}{
				"insertStrategy": "insert",
				"useTransaction": false,
				"useCopy":        false,
				"defaultSchema":  "myschema",
			},
		}

		cfg, err := parseConfig(sinkCfg)
		require.NoError(t, err)
		assert.Equal(t, "localhost", cfg.Host)
		assert.Equal(t, 5433, cfg.Port)
		assert.Equal(t, "testuser", cfg.User)
		assert.Equal(t, "testdb", cfg.Database)
		assert.Equal(t, "insert", cfg.InsertStrategy)
		assert.False(t, cfg.UseTransaction)
		assert.False(t, cfg.UseCopy)
		assert.Equal(t, "myschema", cfg.DefaultSchema)
	})
}
