package oracle

import (
	"testing"

	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
)

// Compile-time interface compliance check
var _ source.Connector = (*Connector)(nil)

func TestConnectorName(t *testing.T) {
	c := New()
	if c.Name() != "oracle" {
		t.Errorf("Name() = %s, want oracle", c.Name())
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
				User:           "system",
				Password:       "password",
				ServiceName:    "XE",
				MiningStrategy: "continuous",
			},
			wantErr: true,
		},
		{
			name: "missing user",
			config: &Config{
				Host:           "localhost",
				Password:       "password",
				ServiceName:    "XE",
				MiningStrategy: "continuous",
			},
			wantErr: true,
		},
		{
			name: "missing password",
			config: &Config{
				Host:           "localhost",
				User:           "system",
				ServiceName:    "XE",
				MiningStrategy: "continuous",
			},
			wantErr: true,
		},
		{
			name: "missing service_name",
			config: &Config{
				Host:           "localhost",
				User:           "system",
				Password:       "password",
				MiningStrategy: "continuous",
			},
			wantErr: true,
		},
		{
			name: "invalid mining strategy",
			config: &Config{
				Host:           "localhost",
				User:           "system",
				Password:       "password",
				ServiceName:    "XE",
				MiningStrategy: "invalid",
			},
			wantErr: true,
		},
		{
			name: "valid config continuous",
			config: &Config{
				Host:           "localhost",
				Port:           1521,
				User:           "system",
				Password:       "password",
				ServiceName:    "XE",
				MiningStrategy: "continuous",
			},
			wantErr: false,
		},
		{
			name: "valid config online",
			config: &Config{
				Host:           "localhost",
				Port:           1521,
				User:           "system",
				Password:       "password",
				ServiceName:    "XE",
				MiningStrategy: "online",
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

	if cfg.Port != 1521 {
		t.Errorf("expected default port 1521, got %d", cfg.Port)
	}
	if cfg.MiningStrategy != "continuous" {
		t.Errorf("expected default mining strategy 'continuous', got '%s'", cfg.MiningStrategy)
	}
	if cfg.BatchSize != 1000 {
		t.Errorf("expected default batch size 1000, got %d", cfg.BatchSize)
	}
}

func TestIsDDLStatement(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		expect bool
	}{
		{"CREATE TABLE", "CREATE TABLE users (id INT)", true},
		{"ALTER TABLE", "ALTER TABLE users ADD col VARCHAR2(100)", true},
		{"DROP TABLE", "DROP TABLE users", true},
		{"TRUNCATE TABLE", "TRUNCATE TABLE users", true},
		{"RENAME TABLE", "RENAME users TO accounts", true},
		{"lowercase create", "create table t (id int)", true},
		{"leading whitespace", "  ALTER TABLE t ADD col INT", true},
		{"INSERT", "INSERT INTO users (id) VALUES (1)", false},
		{"UPDATE", "UPDATE users SET name = 'a'", false},
		{"DELETE", "DELETE FROM users WHERE id = 1", false},
		{"SELECT", "SELECT * FROM users", false},
		{"empty", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isDDLStatement(tt.sql)
			if got != tt.expect {
				t.Errorf("isDDLStatement(%q) = %v, want %v", tt.sql, got, tt.expect)
			}
		})
	}
}

func TestConnectorFieldsInitialized(t *testing.T) {
	c := New()
	// Before Initialize, these should be zero-valued
	if c.tables != nil {
		t.Error("tables should be nil before Initialize")
	}
	if c.store != nil {
		t.Error("store should be nil before Initialize")
	}
	if c.history != nil {
		t.Error("history should be nil before Initialize")
	}
	if c.ddlParser != nil {
		t.Error("ddlParser should be nil before Initialize")
	}
}

func TestHandleDDLEventWithNoMetadata(t *testing.T) {
	c := New()
	// handleDDLEvent should not panic with empty metadata
	ev := &event.ChangeEvent{
		Type:     event.EventTypeDDL,
		Metadata: map[string]string{},
	}
	c.handleDDLEvent(nil, ev) // should not panic
}

func TestHandleDDLEventWithDML(t *testing.T) {
	c := New()
	// DML in metadata should be ignored by isDDLStatement check
	ev := &event.ChangeEvent{
		Type:     event.EventTypeDDL,
		Metadata: map[string]string{"sql": "INSERT INTO t VALUES (1)"},
		Table:    event.TableInfo{Database: "TEST", Table: "T"},
	}
	c.handleDDLEvent(nil, ev) // should not panic, should be no-op
}
