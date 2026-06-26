package sqlserver

import (
	"testing"
	"time"

	"github.com/UFOXD/datastream/internal/schema"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
)

// Interface compliance check
var _ source.Connector = (*Connector)(nil)

func TestConnectorName(t *testing.T) {
	c := New()
	if c.Name() != "sqlserver" {
		t.Errorf("expected name 'sqlserver', got %q", c.Name())
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "empty config",
			config:  &Config{},
			wantErr: true,
		},
		{
			name: "missing host",
			config: &Config{
				User:     "sa",
				Password: "Test@123456",
				Database: "mydb",
			},
			wantErr: true,
		},
		{
			name: "missing user",
			config: &Config{
				Host:     "localhost",
				Password: "Test@123456",
				Database: "mydb",
			},
			wantErr: true,
		},
		{
			name: "missing password",
			config: &Config{
				Host:     "localhost",
				User:     "sa",
				Database: "mydb",
			},
			wantErr: true,
		},
		{
			name: "missing database",
			config: &Config{
				Host:     "localhost",
				User:     "sa",
				Password: "Test@123456",
			},
			wantErr: true,
		},
		{
			name: "valid config",
			config: &Config{
				Host:     "localhost",
				Port:     1433,
				User:     "sa",
				Password: "Test@123456",
				Database: "mydb",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Port != 1433 {
		t.Errorf("expected default port 1433, got %d", cfg.Port)
	}
	if cfg.PollInterval != time.Second {
		t.Errorf("expected default poll interval 1s, got %v", cfg.PollInterval)
	}
	if cfg.BatchSize != 1000 {
		t.Errorf("expected default batch size 1000, got %d", cfg.BatchSize)
	}
	if len(cfg.Schemas) != 1 || cfg.Schemas[0] != "dbo" {
		t.Errorf("expected default schemas [dbo], got %v", cfg.Schemas)
	}
}

func TestConnectorNewHasTables(t *testing.T) {
	c := New()
	if c.tables != nil {
		t.Error("expected tables to be nil before Initialize")
	}
}

func TestEnrichDMLTableInfo_PreferTables(t *testing.T) {
	c := New()
	c.tables = schema.NewTables()
	c.schemaCache = nil // not needed for this test

	// Put a table into Tables
	c.tables.Put(&event.TableInfo{
		Database: "mydb",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "NVARCHAR"},
		},
	})

	ev := &event.ChangeEvent{
		Table: event.TableInfo{
			Database: "mydb",
			Table:    "users",
		},
	}

	c.enrichDMLTableInfo(ev)

	if len(ev.Table.Columns) != 2 {
		t.Errorf("expected 2 columns from Tables, got %d", len(ev.Table.Columns))
	}
	if ev.Table.Columns[0].Name != "id" {
		t.Errorf("expected first column 'id', got %q", ev.Table.Columns[0].Name)
	}
}

func TestEnrichDMLTableInfo_FallbackSchemaCache(t *testing.T) {
	c := New()
	c.tables = schema.NewTables()
	// schemaCache is nil, so fallback will fail gracefully

	ev := &event.ChangeEvent{
		Table: event.TableInfo{
			Database: "mydb",
			Table:    "unknown",
		},
	}

	// Should not panic even with nil schemaCache
	c.enrichDMLTableInfo(ev)

	// Table should remain unchanged since no Tables entry and no schemaCache
	if ev.Table.Database != "mydb" {
		t.Errorf("expected database 'mydb', got %q", ev.Table.Database)
	}
}

func TestSchemas_ReturnsTablesData(t *testing.T) {
	c := New()
	c.tables = schema.NewTables()

	c.tables.Put(&event.TableInfo{Database: "db1", Table: "t1"})
	c.tables.Put(&event.TableInfo{Database: "db1", Table: "t2"})

	schemas := c.Schemas()
	if len(schemas) != 2 {
		t.Errorf("expected 2 schemas, got %d", len(schemas))
	}
}

func TestSchemas_NilTables(t *testing.T) {
	c := New()
	// tables is nil

	schemas := c.Schemas()
	if len(schemas) != 0 {
		t.Errorf("expected 0 schemas, got %d", len(schemas))
	}
}
