package oracle

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
	if c.Name() != "oracle" {
		t.Errorf("Name() = %q, want %q", c.Name(), "oracle")
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Port != 1521 {
		t.Errorf("Port = %d, want 1521", cfg.Port)
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
		{"valid with service name", &Config{Host: "localhost", Port: 1521, User: "sys", Password: "pass", ServiceName: "ORCL"}, false},
		{"missing host", &Config{Port: 1521, User: "sys", Password: "pass", ServiceName: "ORCL"}, true},
		{"missing user", &Config{Host: "localhost", Port: 1521, Password: "pass", ServiceName: "ORCL"}, true},
		{"missing service name", &Config{Host: "localhost", Port: 1521, User: "sys", Password: "pass"}, true},
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
		t.Error("Oracle should support DDL")
	}
}

func TestSupportsTransaction(t *testing.T) {
	c := New()
	if !c.SupportsTransaction() {
		t.Error("Oracle should support transactions")
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
		Host:        "dbhost",
		Port:        1521,
		User:        "admin",
		Password:    "secret",
		ServiceName: "ORCL",
	}
	dsn := buildDSN(cfg)
	expected := "oracle://admin:secret@dbhost:1521/ORCL"
	if dsn != expected {
		t.Errorf("buildDSN() = %q, want %q", dsn, expected)
	}
}

func TestBuildDSNIPv6(t *testing.T) {
	cfg := &Config{
		Host:        "::1",
		Port:        1521,
		User:        "admin",
		Password:    "secret",
		ServiceName: "ORCL",
	}
	dsn := buildDSN(cfg)
	expected := "oracle://admin:secret@[::1]:1521/ORCL"
	if dsn != expected {
		t.Errorf("buildDSN() = %q, want %q", dsn, expected)
	}
}

func TestGetSchemaTable(t *testing.T) {
	c := New()
	c.config = DefaultConfig()
	c.config.DefaultSchema = "HR"

	tests := []struct {
		name   string
		schema string
		table  string
		want   string
	}{
		{"with explicit schema", "SALES", "ORDERS", `"SALES"."ORDERS"`},
		{"with default schema", "", "EMPLOYEES", `"HR"."EMPLOYEES"`},
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
