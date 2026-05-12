package mysql

import (
	"context"
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/internal/source"
)

func TestNew(t *testing.T) {
	conn := New()
	if conn == nil {
		t.Fatal("expected connector to be created")
	}
	if conn.Name() != "mysql" {
		t.Errorf("expected name 'mysql', got '%s'", conn.Name())
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
				User:     "root",
				ServerID: 1,
			},
			wantErr: true,
		},
		{
			name: "missing user",
			config: &Config{
				Host:     "localhost",
				ServerID: 1,
			},
			wantErr: true,
		},
		{
			name: "missing server ID",
			config: &Config{
				Host: "localhost",
				User: "root",
			},
			wantErr: true,
		},
		{
			name: "valid config",
			config: &Config{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "password",
				ServerID: 1,
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

	if cfg.Port != 3306 {
		t.Errorf("expected default port 3306, got %d", cfg.Port)
	}
	if cfg.ServerID == 0 {
		t.Error("expected non-zero server ID")
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
					Port:     3307,
					User:     "testuser",
					Password: "testpass",
				},
			},
			checkVal: func(c *Config) bool {
				return c.Host == "localhost" &&
					c.Port == 3307 &&
					c.User == "testuser" &&
					c.Password == "testpass"
			},
		},
		{
			name: "parse server ID",
			input: source.Config{
				Properties: map[string]interface{}{
					"serverId": float64(12345),
				},
			},
			checkVal: func(c *Config) bool {
				return c.ServerID == 12345
			},
		},
		{
			name: "parse binlog settings",
			input: source.Config{
				Properties: map[string]interface{}{
					"binlogFile": "mysql-bin.000001",
					"binlogPos":  float64(1234),
				},
			},
			checkVal: func(c *Config) bool {
				return c.BinlogFile == "mysql-bin.000001" &&
					c.BinlogPos == 1234
			},
		},
		{
			name: "parse SSL mode",
			input: source.Config{
				Properties: map[string]interface{}{
					"sslMode": "required",
				},
			},
			checkVal: func(c *Config) bool {
				return c.SSLMode == "required"
			},
		},
		{
			name: "parse snapshot mode",
			input: source.Config{
				Properties: map[string]interface{}{
					"snapshotMode": "never",
				},
			},
			checkVal: func(c *Config) bool {
				return c.SnapshotMode == source.SnapshotModeNever
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
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
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
	if pos.BinlogFile != "mysql-bin.000001" {
		t.Errorf("expected binlog file 'mysql-bin.000001', got '%s'", pos.BinlogFile)
	}
	if pos.BinlogPos != 12345 {
		t.Errorf("expected binlog pos 12345, got %d", pos.BinlogPos)
	}

	// Modify returned position should not affect original
	pos.BinlogFile = "mysql-bin.000002"
	originalPos := conn.GetPosition()
	if originalPos.BinlogFile == "mysql-bin.000002" {
		t.Error("position was not properly cloned")
	}
}

func TestShouldCapture(t *testing.T) {
	conn := New()
	conn.config = &Config{
		Databases: []string{"test_db"},
	}

	tests := []struct {
		database string
		table    string
		expected bool
	}{
		{"test_db", "users", true},
		{"other_db", "users", false},
		{"test_db", "orders", true},
	}

	for _, tt := range tests {
		result := conn.shouldCapture(tt.database, tt.table)
		if result != tt.expected {
			t.Errorf("shouldCapture(%s, %s) = %v, want %v",
				tt.database, tt.table, result, tt.expected)
		}
	}
}

func TestShouldCaptureEmptyDatabases(t *testing.T) {
	conn := New()
	conn.config = &Config{
		Databases: []string{},
	}

	// With empty databases, should capture all tables
	if !conn.shouldCapture("any_db", "any_table") {
		t.Error("expected shouldCapture to return true with empty databases list")
	}
}

func TestShouldCaptureWithSyncScopeDatabaseLevel(t *testing.T) {
	conn := New()
	conn.config = &Config{} // no legacy databases set
	conn.syncScope = &source.SyncScope{
		Level: source.SyncLevelDatabase,
		Databases: source.DatabaseScope{
			Names: []string{"prod_db"},
		},
	}

	tests := []struct {
		database string
		table    string
		expected bool
	}{
		{"prod_db", "users", true},
		{"prod_db", "orders", true},
		{"staging_db", "users", false},
		{"other_db", "anything", false},
	}

	for _, tt := range tests {
		result := conn.shouldCapture(tt.database, tt.table)
		if result != tt.expected {
			t.Errorf("shouldCapture(%s, %s) = %v, want %v",
				tt.database, tt.table, result, tt.expected)
		}
	}
}

func TestShouldCaptureWithSyncScopeTableLevel(t *testing.T) {
	conn := New()
	conn.config = &Config{} // no legacy databases set
	conn.syncScope = &source.SyncScope{
		Level: source.SyncLevelTable,
		Tables: source.TableScope{
			Names: []string{"db1.users", "db1.orders", "db2.products"},
		},
	}

	tests := []struct {
		database string
		table    string
		expected bool
	}{
		{"db1", "users", true},
		{"db1", "orders", true},
		{"db2", "products", true},
		{"db1", "payments", false},
		{"db2", "users", false},
		{"db3", "anything", false},
	}

	for _, tt := range tests {
		result := conn.shouldCapture(tt.database, tt.table)
		if result != tt.expected {
			t.Errorf("shouldCapture(%s, %s) = %v, want %v",
				tt.database, tt.table, result, tt.expected)
		}
	}
}

func TestShouldCaptureWithSyncScopeWildcard(t *testing.T) {
	conn := New()
	conn.config = &Config{}
	conn.syncScope = &source.SyncScope{
		Level: source.SyncLevelDatabase,
		Databases: source.DatabaseScope{
			Names: []string{"*"}, // wildcard: all databases
		},
	}

	// Wildcard should capture everything
	if !conn.shouldCapture("any_db", "any_table") {
		t.Error("expected shouldCapture to return true with wildcard database scope")
	}
	if !conn.shouldCapture("prod", "users") {
		t.Error("expected shouldCapture to return true for prod.users with wildcard scope")
	}
}

func TestShouldCaptureSyncScopeTakesPriorityOverLegacy(t *testing.T) {
	conn := New()
	// Legacy config allows "legacy_db", SyncScope allows "scope_db"
	conn.config = &Config{
		Databases: []string{"legacy_db"},
	}
	conn.syncScope = &source.SyncScope{
		Level: source.SyncLevelDatabase,
		Databases: source.DatabaseScope{
			Names: []string{"scope_db"},
		},
	}

	// SyncScope takes priority: scope_db should be captured, legacy_db should not
	if !conn.shouldCapture("scope_db", "users") {
		t.Error("expected shouldCapture to return true for scope_db (SyncScope)")
	}
	if conn.shouldCapture("legacy_db", "users") {
		t.Error("expected shouldCapture to return false for legacy_db when SyncScope is set")
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
		{"user*", "users", false}, // Simple matching, no wildcards
	}

	for _, tt := range tests {
		result := matchPattern(tt.pattern, tt.s)
		if result != tt.want {
			t.Errorf("matchPattern(%s, %s) = %v, want %v",
				tt.pattern, tt.s, result, tt.want)
		}
	}
}

func TestConnectorStartStop(t *testing.T) {
	conn := New()
	conn.config = &Config{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "",
		ServerID: 1,
	}

	ctx := context.Background()

	// Cannot start without initialization
	err := conn.Start(ctx)
	if err == nil {
		t.Error("expected error when starting uninitialized connector")
	}

	// Stop on non-running connector should succeed
	err = conn.Stop(ctx)
	if err != nil {
		t.Errorf("Stop() error = %v", err)
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
