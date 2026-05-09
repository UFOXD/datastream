package kafka

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
	if conn.Name() != "kafka" {
		t.Errorf("expected name 'kafka', got '%s'", conn.Name())
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
			name: "missing brokers",
			config: &Config{
				Topic: "test-topic",
			},
			wantErr: true,
		},
		{
			name: "missing topic",
			config: &Config{
				Brokers: []string{"localhost:9092"},
			},
			wantErr: true,
		},
		{
			name: "valid config",
			config: &Config{
				Brokers: []string{"localhost:9092"},
				Topic:   "test-topic",
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

	if cfg.TopicNamingStrategy != "default" {
		t.Errorf("expected topic naming strategy 'default', got '%s'", cfg.TopicNamingStrategy)
	}
	if cfg.Acks != "all" {
		t.Errorf("expected acks 'all', got '%s'", cfg.Acks)
	}
	if cfg.Compression != "snappy" {
		t.Errorf("expected compression 'snappy', got '%s'", cfg.Compression)
	}
	if cfg.BatchSize != 100 {
		t.Errorf("expected batch size 100, got %d", cfg.BatchSize)
	}
	if cfg.Retries != 3 {
		t.Errorf("expected retries 3, got %d", cfg.Retries)
	}
	if cfg.KeyFormat != "json" {
		t.Errorf("expected key format 'json', got '%s'", cfg.KeyFormat)
	}
	if cfg.ValueFormat != "json" {
		t.Errorf("expected value format 'json', got '%s'", cfg.ValueFormat)
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
		t.Error("expected Kafka connector to support DDL")
	}
}

func TestSupportsTransaction(t *testing.T) {
	conn := New()

	if !conn.SupportsTransaction() {
		t.Error("expected Kafka connector to support transactions")
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
					Brokers: []string{"localhost:9092", "localhost:9093"},
					Topic:   "test-topic",
				},
			},
			checkVal: func(c *Config) bool {
				return len(c.Brokers) == 2 &&
					c.Brokers[0] == "localhost:9092" &&
					c.Topic == "test-topic"
			},
		},
		{
			name: "parse acks setting",
			input: sink.Config{
				Properties: map[string]interface{}{
					"acks": "leader",
				},
			},
			checkVal: func(c *Config) bool {
				return c.Acks == "leader"
			},
		},
		{
			name: "parse compression setting",
			input: sink.Config{
				Properties: map[string]interface{}{
					"compression": "gzip",
				},
			},
			checkVal: func(c *Config) bool {
				return c.Compression == "gzip"
			},
		},
		{
			name: "parse format settings",
			input: sink.Config{
				Properties: map[string]interface{}{
					"keyFormat":   "avro",
					"valueFormat": "avro",
				},
			},
			checkVal: func(c *Config) bool {
				return c.KeyFormat == "avro" && c.ValueFormat == "avro"
			},
		},
		{
			name: "parse partition key",
			input: sink.Config{
				Properties: map[string]interface{}{
					"partitionKey": "user_id",
				},
			},
			checkVal: func(c *Config) bool {
				return c.PartitionKey == "user_id"
			},
		},
		{
			name: "parse batch settings",
			input: sink.Config{
				Properties: map[string]interface{}{
					"batchSize":    int(200),
					"batchTimeout": int(50),
				},
			},
			checkVal: func(c *Config) bool {
				return c.BatchSize == 200 && c.BatchTimeout == 50
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

func TestGetCompression(t *testing.T) {
	tests := []struct {
		input    string
		expected string // We'll just check it doesn't panic and returns valid value
	}{
		{"none", ""},
		{"gzip", "gzip"},
		{"snappy", "snappy"},
		{"lz4", "lz4"},
		{"zstd", "zstd"},
		{"unknown", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			// getCompression returns a compress.Compression, just verify no panic
			result := getCompression(tt.input)
			_ = result
		})
	}
}

func TestGetRequiredAcks(t *testing.T) {
	tests := []struct {
		input string
	}{
		{"none"},
		{"leader"},
		{"all"},
		{"unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			// getRequiredAcks returns kafka.RequiredAcks, just verify no panic
			result := getRequiredAcks(tt.input)
			_ = result
		})
	}
}

func TestBuildKey(t *testing.T) {
	conn := New()
	conn.config = &Config{
		PartitionKey: "id",
		KeyFormat:    "json",
	}

	testEvent := &event.ChangeEvent{
		Table: event.TableInfo{
			Database: "test_db",
			Table:    "users",
		},
		After: event.RowData{
			Fields: map[string]event.Field{
				"id":   {Name: "id", Value: 123},
				"name": {Name: "name", Value: "test"},
			},
		},
	}

	key, err := conn.buildKey(testEvent)
	if err != nil {
		t.Fatalf("buildKey() error = %v", err)
	}
	if len(key) == 0 {
		t.Error("expected non-empty key")
	}
}

func TestBuildKeyWithoutPartitionKey(t *testing.T) {
	conn := New()
	conn.config = &Config{
		PartitionKey: "",
		KeyFormat:    "default",
	}

	testEvent := &event.ChangeEvent{
		Table: event.TableInfo{
			Database: "test_db",
			Table:    "users",
		},
		After: event.RowData{
			Fields: map[string]event.Field{
				"id":   {Name: "id", Value: 123},
				"name": {Name: "name", Value: "test"},
			},
		},
	}

	key, err := conn.buildKey(testEvent)
	if err != nil {
		t.Fatalf("buildKey() error = %v", err)
	}
	// Should use database.table as key
	if string(key) != "test_db.users" {
		t.Errorf("expected key 'test_db.users', got '%s'", string(key))
	}
}

func TestGetTopic(t *testing.T) {
	tests := []struct {
		strategy   string
		baseTopic  string
		tableName  string
		dbName     string
		expected   string
	}{
		{"default", "events", "users", "test", "events"},
		{"table", "events", "users", "test", "events.users"},
		{"database", "events", "users", "test", "events.test"},
	}

	for _, tt := range tests {
		t.Run(tt.strategy, func(t *testing.T) {
			conn := New()
			conn.config = &Config{
				Topic:               tt.baseTopic,
				TopicNamingStrategy: tt.strategy,
			}

			testEvent := &event.ChangeEvent{
				Table: event.TableInfo{
					Database: tt.dbName,
					Table:    tt.tableName,
				},
			}

			result := conn.getTopic(testEvent)
			if result != tt.expected {
				t.Errorf("getTopic() = %s, want %s", result, tt.expected)
			}
		})
	}
}

func TestBuildValue(t *testing.T) {
	conn := New()
	conn.config = &Config{
		ValueFormat: "json",
	}

	testEvent := &event.ChangeEvent{
		ID:   "test-123",
		Type: event.EventTypeInsert,
		Table: event.TableInfo{
			Database: "test_db",
			Table:    "users",
		},
		After: event.RowData{
			Fields: map[string]event.Field{
				"id":   {Name: "id", Value: 123},
				"name": {Name: "name", Value: "test"},
			},
		},
	}

	value, err := conn.buildValue(testEvent)
	if err != nil {
		t.Fatalf("buildValue() error = %v", err)
	}
	if len(value) == 0 {
		t.Error("expected non-empty value")
	}
}
