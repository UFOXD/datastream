package sqlserver

import (
	"testing"

	"github.com/UFOXD/datastream/internal/sink"
)

// Interface compliance
func TestConnectorImplementsSinkConnector(t *testing.T) {
	var _ sink.Connector = (*Connector)(nil)
}

func TestConnectorName(t *testing.T) {
	c := New()
	if c.Name() != "sqlserver" {
		t.Errorf("Name() = %q, want %q", c.Name(), "sqlserver")
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Port != 1433 {
		t.Errorf("Port = %d, want 1433", cfg.Port)
	}
	if cfg.ConnectTimeout != 30 {
		t.Errorf("ConnectTimeout = %d, want 30", cfg.ConnectTimeout)
	}
	if cfg.BatchSize != 1000 {
		t.Errorf("BatchSize = %d, want 1000", cfg.BatchSize)
	}
	if !cfg.UseTransaction {
		t.Error("UseTransaction should default to true")
	}
	if cfg.MaxConnections != 10 {
		t.Errorf("MaxConnections = %d, want 10", cfg.MaxConnections)
	}
	if cfg.Schema != "dbo" {
		t.Errorf("Schema = %q, want %q", cfg.Schema, "dbo")
	}
	if cfg.DDLPolicy != "apply" {
		t.Errorf("DDLPolicy = %q, want %q", cfg.DDLPolicy, "apply")
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{"valid config", &Config{Host: "localhost", Port: 1433, User: "sa", Password: "pass", Database: "testdb"}, false},
		{"missing host", &Config{Port: 1433, User: "sa", Password: "pass", Database: "testdb"}, true},
		{"missing user", &Config{Host: "localhost", Port: 1433, Password: "pass", Database: "testdb"}, true},
		{"missing database", &Config{Host: "localhost", Port: 1433, User: "sa", Password: "pass"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSupportsDDL(t *testing.T) {
	c := New()
	if !c.SupportsDDL() {
		t.Error("SQL Server should support DDL")
	}
}

func TestSupportsTransaction(t *testing.T) {
	c := New()
	if !c.SupportsTransaction() {
		t.Error("SQL Server should support transactions")
	}
}

func TestConnectorInitialStatus(t *testing.T) {
	c := New()
	status := c.Status()
	if status.State != sink.StateUninitialized {
		t.Errorf("initial State = %q, want %q", status.State, sink.StateUninitialized)
	}
}

func TestGetPositionBeforeWrite(t *testing.T) {
	c := New()
	pos := c.GetPosition()
	if pos != nil {
		t.Errorf("GetPosition() before write should be nil, got %v", pos)
	}
}

func TestBuildDSN(t *testing.T) {
	cfg := &Config{
		Host:     "dbhost",
		Port:     1433,
		User:     "sa",
		Password: "secret",
		Database: "mydb",
	}
	dsn := buildDSN(cfg)
	expected := "sqlserver://sa:secret@dbhost:1433?database=mydb"
	if dsn != expected {
		t.Errorf("buildDSN() = %q, want %q", dsn, expected)
	}
}

func TestBuildDSNIPv6(t *testing.T) {
	cfg := &Config{
		Host:     "::1",
		Port:     1433,
		User:     "sa",
		Password: "secret",
		Database: "mydb",
	}
	dsn := buildDSN(cfg)
	expected := "sqlserver://sa:secret@[::1]:1433?database=mydb"
	if dsn != expected {
		t.Errorf("buildDSN() = %q, want %q", dsn, expected)
	}
}

func TestGetSchemaTable(t *testing.T) {
	c := New()
	c.config = DefaultConfig()
	c.config.Schema = "dbo"

	tests := []struct {
		name   string
		schema string
		table  string
		want   string
	}{
		{"with explicit schema", "sales", "orders", "[sales].[orders]"},
		{"with default schema", "", "employees", "[dbo].[employees]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.getSchemaTable(tt.schema, tt.table)
			if got != tt.want {
				t.Errorf("getSchemaTable(%q, %q) = %q, want %q", tt.schema, tt.table, got, tt.want)
			}
		})
	}
}
