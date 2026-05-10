package postgres

import (
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/internal/source"
)

func TestNew(t *testing.T) {
	conn := New()
	if conn == nil {
		t.Fatal("expected connector to be created")
	}
	if conn.Name() != "postgres" {
		t.Errorf("expected name 'postgres', got '%s'", conn.Name())
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
				User:     "postgres",
				Database: "test",
				SlotName: "test_slot",
			},
			wantErr: true,
		},
		{
			name: "missing user",
			config: &Config{
				Host:     "localhost",
				Database: "test",
				SlotName: "test_slot",
			},
			wantErr: true,
		},
		{
			name: "missing database",
			config: &Config{
				Host:     "localhost",
				User:     "postgres",
				SlotName: "test_slot",
			},
			wantErr: true,
		},
		{
			name: "missing slot name",
			config: &Config{
				Host:     "localhost",
				User:     "postgres",
				Database: "test",
			},
			wantErr: true,
		},
		{
			name: "valid config",
			config: &Config{
				Host:     "localhost",
				Port:     5432,
				User:     "postgres",
				Password: "password",
				Database: "test",
				SlotName: "test_slot",
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

	if cfg.Port != 5432 {
		t.Errorf("expected default port 5432, got %d", cfg.Port)
	}
	if cfg.PluginName != "pgoutput" {
		t.Errorf("expected plugin name 'pgoutput', got '%s'", cfg.PluginName)
	}
	if cfg.SlotName != "datastream_slot" {
		t.Errorf("expected slot name 'datastream_slot', got '%s'", cfg.SlotName)
	}
	if cfg.PublicationName != "datastream_pub" {
		t.Errorf("expected publication name 'datastream_pub', got '%s'", cfg.PublicationName)
	}
	if cfg.CreateSlot != true {
		t.Error("expected CreateSlot to be true")
	}
	if cfg.DropSlotOnStop != false {
		t.Error("expected DropSlotOnStop to be false")
	}
	if cfg.SnapshotMode != source.SnapshotModeInitial {
		t.Errorf("expected snapshot mode 'initial', got '%s'", cfg.SnapshotMode)
	}
	if cfg.MaxConnections != 10 {
		t.Errorf("expected max connections 10, got %d", cfg.MaxConnections)
	}
}

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name     string
		input    source.Config
		checkVal func(*Config) bool
	}{
		{
			name: "parse connection settings",
			input: source.Config{
				Connection: source.ConnectionConfig{
					Host:     "localhost",
					Port:     5433,
					User:     "testuser",
					Password: "testpass",
					Database: "testdb",
				},
			},
			checkVal: func(c *Config) bool {
				return c.Host == "localhost" &&
					c.Port == 5433 &&
					c.User == "testuser" &&
					c.Password == "testpass" &&
					c.Database == "testdb"
			},
		},
		{
			name: "parse plugin name",
			input: source.Config{
				Properties: map[string]interface{}{
					"pluginName": "wal2json",
				},
			},
			checkVal: func(c *Config) bool {
				return c.PluginName == "wal2json"
			},
		},
		{
			name: "parse slot and publication names",
			input: source.Config{
				Properties: map[string]interface{}{
					"slotName":       "custom_slot",
					"publicationName": "custom_pub",
				},
			},
			checkVal: func(c *Config) bool {
				return c.SlotName == "custom_slot" &&
					c.PublicationName == "custom_pub"
			},
		},
		{
			name: "parse start LSN",
			input: source.Config{
				Properties: map[string]interface{}{
					"startLsn": float64(12345678),
				},
			},
			checkVal: func(c *Config) bool {
				return c.StartLSN == 12345678
			},
		},
		{
			name: "parse SSL mode",
			input: source.Config{
				Properties: map[string]interface{}{
					"sslMode": "require",
				},
			},
			checkVal: func(c *Config) bool {
				return c.SSLMode == "require"
			},
		},
		{
			name: "parse create slot option",
			input: source.Config{
				Properties: map[string]interface{}{
					"createSlot": false,
				},
			},
			checkVal: func(c *Config) bool {
				return c.CreateSlot == false
			},
		},
		{
			name: "parse drop slot on stop",
			input: source.Config{
				Properties: map[string]interface{}{
					"dropSlotOnStop": true,
				},
			},
			checkVal: func(c *Config) bool {
				return c.DropSlotOnStop == true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parseConfig(tt.input)
			if err != nil {
				t.Fatalf("parseConfig() error = %v", err)
			}
			if !tt.checkVal(cfg) {
				t.Error("config check failed")
			}
		})
	}
}

func TestConnectorStatus(t *testing.T) {
	conn := New()

	// Initial status should be uninitialized
	status := conn.Status()
	if status.State != source.StateUninitialized {
		t.Errorf("expected state 'uninitialized', got '%s'", status.State)
	}
}

func TestConnectorPosition(t *testing.T) {
	conn := New()

	// Initial position should be nil
	pos := conn.GetPosition()
	if pos != nil {
		t.Errorf("expected nil position, got %v", pos)
	}

	// Set position
	testPos := &event.Position{
		LSN: 12345678,
	}
	err := conn.SetPosition(testPos)
	if err != nil {
		t.Fatalf("SetPosition() error = %v", err)
	}

	// Get position should return a clone
	pos = conn.GetPosition()
	if pos == nil {
		t.Fatal("expected non-nil position")
	}
	if pos.LSN != 12345678 {
		t.Errorf("expected LSN 12345678, got %d", pos.LSN)
	}

	// Modify returned position should not affect original
	pos.LSN = 99999999
	originalPos := conn.GetPosition()
	if originalPos.LSN == 99999999 {
		t.Error("position was not properly cloned")
	}
}

func TestShouldCapture(t *testing.T) {
	conn := New()
	conn.config = &Config{
		Database: "test_db",
		Schemas:  []string{"public", "custom"},
	}

	tests := []struct {
		schema   string
		table    string
		expected bool
	}{
		{"public", "users", true},
		{"custom", "orders", true},
		{"other", "users", false},
		{"public", "orders", true},
	}

	for _, tt := range tests {
		result := conn.shouldCapture(tt.schema, tt.table)
		if result != tt.expected {
			t.Errorf("shouldCapture(%s, %s) = %v, want %v",
				tt.schema, tt.table, result, tt.expected)
		}
	}
}

func TestShouldCaptureEmptySchemas(t *testing.T) {
	conn := New()
	conn.config = &Config{
		Schemas: []string{},
	}

	// With empty schemas, should capture all tables
	if !conn.shouldCapture("any_schema", "any_table") {
		t.Error("expected shouldCapture to return true with empty schemas list")
	}
}

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		pattern string
		s       string
		want    bool
	}{
		{"*", "anything", true},
		{"users", "users", true},
		{"users", "orders", false},
	}

	for _, tt := range tests {
		result := matchPattern(tt.pattern, tt.s)
		if result != tt.want {
			t.Errorf("matchPattern(%s, %s) = %v, want %v",
				tt.pattern, tt.s, result, tt.want)
		}
	}
}

func TestEventsChannel(t *testing.T) {
	conn := New()

	eventsCh := conn.Events()
	if eventsCh == nil {
		t.Error("expected non-nil events channel")
	}
}

func TestErrorsChannel(t *testing.T) {
	conn := New()

	errorsCh := conn.Errors()
	if errorsCh == nil {
		t.Error("expected non-nil errors channel")
	}
}
