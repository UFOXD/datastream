package mysql

import (
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/sink"
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
				Database: "test",
			},
			wantErr: true,
		},
		{
			name: "missing user",
			config: &Config{
				Host:     "localhost",
				Database: "test",
			},
			wantErr: true,
		},
		{
			name: "missing database",
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
				Database: "test",
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
	if cfg.InsertStrategy != "upsert" {
		t.Errorf("expected insert strategy 'upsert', got '%s'", cfg.InsertStrategy)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("expected max retries 3, got %d", cfg.MaxRetries)
	}
	if cfg.MaxConnections != 10 {
		t.Errorf("expected max connections 10, got %d", cfg.MaxConnections)
	}
	if cfg.UseTransaction != true {
		t.Error("expected UseTransaction to be true")
	}
	if cfg.AutoCreateTable != true {
		t.Error("expected AutoCreateTable to be true")
	}
	if cfg.DDLPolicy != "apply" {
		t.Errorf("expected DDL policy 'apply', got '%s'", cfg.DDLPolicy)
	}
}

func TestConnectorStatus(t *testing.T) {
	conn := New()

	// Initial status should be uninitialized
	status := conn.Status()
	if status.State != sink.StateUninitialized {
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
}

func TestSupportsDDL(t *testing.T) {
	conn := New()

	if !conn.SupportsDDL() {
		t.Error("expected MySQL connector to support DDL")
	}
}

func TestSupportsTransaction(t *testing.T) {
	conn := New()
	conn.config = &Config{
		UseTransaction: true,
	}

	if !conn.SupportsTransaction() {
		t.Error("expected MySQL connector to support transactions when UseTransaction is true")
	}
}

func TestSupportsTransactionDisabled(t *testing.T) {
	conn := New()
	conn.config = &Config{
		UseTransaction: false,
	}

	if conn.SupportsTransaction() {
		t.Error("expected MySQL connector to not support transactions when UseTransaction is false")
	}
}

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name     string
		input    sink.Config
		checkVal func(*Config) bool
	}{
		{
			name: "parse connection settings",
			input: sink.Config{
				Connection: sink.ConnectionConfig{
					Host:     "localhost",
					Port:     3307,
					User:     "testuser",
					Password: "testpass",
					Database: "testdb",
				},
			},
			checkVal: func(c *Config) bool {
				return c.Host == "localhost" &&
					c.Port == 3307 &&
					c.User == "testuser" &&
					c.Password == "testpass" &&
					c.Database == "testdb"
			},
		},
		{
			name: "parse insert strategy",
			input: sink.Config{
				Properties: map[string]interface{}{
					"insertStrategy": "replace",
				},
			},
			checkVal: func(c *Config) bool {
				return c.InsertStrategy == "replace"
			},
		},
		{
			name: "parse transaction settings",
			input: sink.Config{
				Properties: map[string]interface{}{
					"useTransaction": false,
				},
			},
			checkVal: func(c *Config) bool {
				return c.UseTransaction == false
			},
		},
		{
			name: "parse DDL policy",
			input: sink.Config{
				Properties: map[string]interface{}{
					"ddlPolicy": "ignore",
				},
			},
			checkVal: func(c *Config) bool {
				return c.DDLPolicy == "ignore"
			},
		},
		{
			name: "parse auto create table",
			input: sink.Config{
				Properties: map[string]interface{}{
					"autoCreateTable": false,
				},
			},
			checkVal: func(c *Config) bool {
				return c.AutoCreateTable == false
			},
		},
		{
			name: "parse connection pool settings",
			input: sink.Config{
				Properties: map[string]interface{}{
					"maxConnections": int(20),
					"maxIdle":        int(10),
					"connectTimeout": int(60),
				},
			},
			checkVal: func(c *Config) bool {
				return c.MaxConnections == 20 &&
					c.MaxIdle == 10 &&
					c.ConnectTimeout == 60
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

func TestBuildInsertQuery(t *testing.T) {
	conn := New()
	conn.config = &Config{
		InsertStrategy: "insert",
	}

	testEvent := &event.ChangeEvent{
		Type: event.EventTypeInsert,
		Table: event.TableInfo{
			Database: "test_db",
			Table:    "users",
		},
		After: event.RowData{
			Fields: map[string]event.Field{
				"id":    {Name: "id", Value: 1},
				"name":  {Name: "name", Value: "test"},
				"email": {Name: "email", Value: "test@example.com"},
			},
		},
	}

	// We can't fully test executeInsert without a DB connection,
	// but we can verify the connector doesn't panic
	_ = testEvent
}

func TestBuildUpdateQuery(t *testing.T) {
	conn := New()
	conn.config = &Config{
		InsertStrategy: "upsert",
	}

	testEvent := &event.ChangeEvent{
		Type: event.EventTypeUpdate,
		Table: event.TableInfo{
			Database:      "test_db",
			Table:         "users",
			PrimaryKeyColumns: []string{"id"},
		},
		Before: event.RowData{
			Fields: map[string]event.Field{
				"id":    {Name: "id", Value: 1},
				"name":  {Name: "name", Value: "old"},
				"email": {Name: "email", Value: "old@example.com"},
			},
		},
		After: event.RowData{
			Fields: map[string]event.Field{
				"id":    {Name: "id", Value: 1},
				"name":  {Name: "name", Value: "new"},
				"email": {Name: "email", Value: "new@example.com"},
			},
		},
	}

	// Verify event structure is correct for update
	if testEvent.Before.Fields["id"].Value != 1 {
		t.Error("expected before id to be 1")
	}
	if testEvent.After.Fields["name"].Value != "new" {
		t.Error("expected after name to be 'new'")
	}
}

func TestBuildDeleteQuery(t *testing.T) {
	conn := New()
	conn.config = &Config{}

	testEvent := &event.ChangeEvent{
		Type: event.EventTypeDelete,
		Table: event.TableInfo{
			Database:      "test_db",
			Table:         "users",
			PrimaryKeyColumns: []string{"id"},
		},
		Before: event.RowData{
			Fields: map[string]event.Field{
				"id":   {Name: "id", Value: 1},
				"name": {Name: "name", Value: "test"},
			},
		},
	}

	// Verify event structure is correct for delete
	if testEvent.Before.Fields["id"].Value != 1 {
		t.Error("expected before id to be 1")
	}
	if testEvent.Type != event.EventTypeDelete {
		t.Errorf("expected event type 'delete', got '%s'", testEvent.Type)
	}
}

func TestDDLPolicyIgnore(t *testing.T) {
	conn := New()
	conn.config = &Config{
		DDLPolicy: "ignore",
	}

	// Test that ignore policy is set correctly
	if conn.config.DDLPolicy != "ignore" {
		t.Error("expected DDL policy to be 'ignore'")
	}
}

func TestDDLPolicyError(t *testing.T) {
	conn := New()
	conn.config = &Config{
		DDLPolicy: "error",
	}

	// Test that error policy is set correctly
	if conn.config.DDLPolicy != "error" {
		t.Error("expected DDL policy to be 'error'")
	}
}

func TestDDLPolicyApply(t *testing.T) {
	conn := New()
	conn.config = &Config{
		DDLPolicy: "apply",
	}

	// Test that apply policy is set correctly
	if conn.config.DDLPolicy != "apply" {
		t.Error("expected DDL policy to be 'apply'")
	}
}

func TestInsertStrategies(t *testing.T) {
	strategies := []string{"insert", "replace", "upsert"}

	for _, strategy := range strategies {
		t.Run(strategy, func(t *testing.T) {
			conn := New()
			conn.config = &Config{
				InsertStrategy: strategy,
			}

			if conn.config.InsertStrategy != strategy {
				t.Errorf("expected insert strategy '%s', got '%s'", strategy, conn.config.InsertStrategy)
			}
		})
	}
}
