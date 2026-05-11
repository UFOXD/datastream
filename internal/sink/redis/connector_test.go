package redis

import (
	"testing"
	"time"

	"github.com/UFOXD/datastream/internal/sink"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "default config valid",
			config:  DefaultConfig(),
			wantErr: false,
		},
		{
			name: "missing addr",
			config: &Config{
				Addr:      "",
				Format:    "hash",
				BatchSize: 1000,
			},
			wantErr: true,
		},
		{
			name: "invalid format",
			config: &Config{
				Addr:      "localhost:6379",
				Format:    "invalid",
				BatchSize: 1000,
			},
			wantErr: true,
		},
		{
			name: "zero batch size",
			config: &Config{
				Addr:      "localhost:6379",
				Format:    "hash",
				BatchSize: 0,
			},
			wantErr: true,
		},
		{
			name: "hash format valid",
			config: &Config{
				Addr:      "localhost:6379",
				Format:    "hash",
				BatchSize: 100,
			},
			wantErr: false,
		},
		{
			name: "json format valid",
			config: &Config{
				Addr:      "localhost:6379",
				Format:    "json",
				BatchSize: 100,
			},
			wantErr: false,
		},
		{
			name: "string format valid",
			config: &Config{
				Addr:      "localhost:6379",
				Format:    "string",
				BatchSize: 100,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	assert.NotNil(t, cfg)
	assert.Equal(t, "localhost:6379", cfg.Addr)
	assert.Equal(t, "{database}:{table}:{pk}", cfg.KeyPattern)
	assert.Equal(t, 1000, cfg.BatchSize)
	assert.Equal(t, time.Second, cfg.FlushInterval)
	assert.Equal(t, "hash", cfg.Format)
	assert.Equal(t, time.Duration(0), cfg.TTL)
}

func TestConnector_New(t *testing.T) {
	conn := New()
	assert.NotNil(t, conn)
	assert.Equal(t, "redis", conn.Name())
	assert.Equal(t, sink.StateUninitialized, conn.Status().State)
}

func TestConnector_SupportsDDL(t *testing.T) {
	conn := New()
	assert.False(t, conn.SupportsDDL())
}

func TestConnector_SupportsTransaction(t *testing.T) {
	conn := New()
	assert.False(t, conn.SupportsTransaction())
}

func TestConnector_GetPosition(t *testing.T) {
	conn := New()
	pos := conn.GetPosition()
	assert.Nil(t, pos)
}

func TestPipelineWriter_GenerateKey(t *testing.T) {
	cfg := DefaultConfig()
	writer := &PipelineWriter{config: cfg}

	table := event.TableInfo{
		Database: "mydb",
		Table:    "users",
	}

	row := event.RowData{
		Fields: map[string]event.Field{
			"id": {Name: "id", Value: 42},
		},
	}

	tests := []struct {
		name      string
		pkColumns []string
		wantKey   string
	}{
		{
			name:      "single pk column",
			pkColumns: []string{"id"},
			wantKey:   "mydb:users:42",
		},
		{
			name:      "no pk columns",
			pkColumns: []string{},
			wantKey:   "mydb:users:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := writer.generateKey(table, row, tt.pkColumns)
			assert.Equal(t, tt.wantKey, key)
		})
	}
}

func TestPipelineWriter_GenerateKey_CompositePK(t *testing.T) {
	cfg := DefaultConfig()
	writer := &PipelineWriter{config: cfg}

	table := event.TableInfo{
		Database: "mydb",
		Table:    "orders",
	}

	row := event.RowData{
		Fields: map[string]event.Field{
			"order_id": {Name: "order_id", Value: 100},
			"item_id":  {Name: "item_id", Value: 200},
		},
	}

	key := writer.generateKey(table, row, []string{"order_id", "item_id"})
	assert.Equal(t, "mydb:orders:100_200", key)
}

func TestPipelineWriter_GenerateKey_CustomPattern(t *testing.T) {
	cfg := &Config{
		KeyPattern: "cache:{database}:{table}:{pk}",
		Format:     "hash",
		BatchSize:  100,
	}
	writer := &PipelineWriter{config: cfg}

	table := event.TableInfo{
		Database: "mydb",
		Table:    "products",
	}

	row := event.RowData{
		Fields: map[string]event.Field{
			"id": {Name: "id", Value: "abc"},
		},
	}

	key := writer.generateKey(table, row, []string{"id"})
	assert.Equal(t, "cache:mydb:products:abc", key)
}

func TestPipelineWriter_BuildCommands(t *testing.T) {
	tests := []struct {
		name      string
		format    string
		eventType event.EventType
		wantOp    string
	}{
		{
			name:      "insert hash",
			format:    "hash",
			eventType: event.EventTypeInsert,
			wantOp:    "hset",
		},
		{
			name:      "update hash",
			format:    "hash",
			eventType: event.EventTypeUpdate,
			wantOp:    "hset",
		},
		{
			name:      "delete hash",
			format:    "hash",
			eventType: event.EventTypeDelete,
			wantOp:    "del",
		},
		{
			name:      "insert json",
			format:    "json",
			eventType: event.EventTypeInsert,
			wantOp:    "set",
		},
		{
			name:      "update json",
			format:    "json",
			eventType: event.EventTypeUpdate,
			wantOp:    "set",
		},
		{
			name:      "delete json",
			format:    "json",
			eventType: event.EventTypeDelete,
			wantOp:    "del",
		},
		{
			name:      "insert string",
			format:    "string",
			eventType: event.EventTypeInsert,
			wantOp:    "set",
		},
		{
			name:      "delete string",
			format:    "string",
			eventType: event.EventTypeDelete,
			wantOp:    "del",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				KeyPattern: "{database}:{table}:{pk}",
				Format:     tt.format,
				BatchSize:  100,
			}
			writer := &PipelineWriter{config: cfg}

			afterData := event.RowData{
				Fields: map[string]event.Field{
					"id": {Name: "id", Value: 1},
				},
			}

			e := &event.ChangeEvent{
				Type: tt.eventType,
				Table: event.TableInfo{
					Database:          "db",
					Table:             "tbl",
					PrimaryKeyColumns: []string{"id"},
				},
				After: afterData,
			}

			if tt.eventType == event.EventTypeDelete {
				e.Before = afterData
				e.After = event.RowData{}
			}

			cmd := writer.buildCommand(e)
			if cmd != nil {
				assert.Equal(t, tt.wantOp, cmd.Op)
			}
		})
	}
}
