package mongodb

import (
	"testing"

	"github.com/UFOXD/datastream/internal/sink"
	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "default config with database",
			config:  &Config{Hosts: []string{"localhost:27017"}, Database: "testdb", WriteStrategy: "upsert", DDLPolicy: "ignore"},
			wantErr: false,
		},
		{
			name: "missing hosts",
			config: &Config{
				Hosts:         []string{},
				Database:      "testdb",
				WriteStrategy: "upsert",
				DDLPolicy:     "ignore",
			},
			wantErr: true,
		},
		{
			name: "missing database",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				WriteStrategy: "upsert",
				DDLPolicy:     "ignore",
			},
			wantErr: true,
		},
		{
			name: "invalid write strategy",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				WriteStrategy: "invalid",
				DDLPolicy:     "ignore",
			},
			wantErr: true,
		},
		{
			name: "invalid DDL policy",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				WriteStrategy: "upsert",
				DDLPolicy:     "invalid",
			},
			wantErr: true,
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

func TestConfig_ConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		contains []string
	}{
		{
			name: "basic connection string",
			config: &Config{
				Hosts:    []string{"localhost:27017"},
				Database: "testdb",
			},
			contains: []string{"mongodb://localhost:27017/testdb"},
		},
		{
			name: "connection string with auth",
			config: &Config{
				Hosts:      []string{"localhost:27017"},
				User:       "testuser",
				Password:   "testpass",
				Database:   "testdb",
				AuthSource: "admin",
			},
			contains: []string{"mongodb://testuser:testpass@localhost:27017/testdb", "authSource=admin"},
		},
		{
			name: "connection string with replica set",
			config: &Config{
				Hosts:      []string{"host1:27017", "host2:27017"},
				ReplicaSet: "rs0",
				Database:   "testdb",
			},
			contains: []string{"mongodb://host1:27017,host2:27017/testdb", "replicaSet=rs0"},
		},
		{
			name: "connection string with SSL",
			config: &Config{
				Hosts:    []string{"localhost:27017"},
				Database: "testdb",
				SSLMode:  true,
			},
			contains: []string{"tls=true"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := tt.config.ConnectionString()
			for _, s := range tt.contains {
				assert.Contains(t, cs, s)
			}
		})
	}
}

func TestConnector_New(t *testing.T) {
	conn := New()
	assert.NotNil(t, conn)
	assert.Equal(t, "mongodb", conn.Name())
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

	// Initially nil
	pos := conn.GetPosition()
	assert.Nil(t, pos)
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	assert.NotNil(t, cfg)
	assert.Contains(t, cfg.Hosts, "localhost:27017")
	assert.Equal(t, "upsert", cfg.WriteStrategy)
	assert.Equal(t, "majority", cfg.WriteConcern)
	assert.False(t, cfg.Ordered)
	assert.Equal(t, 1000, cfg.BatchSize)
}
