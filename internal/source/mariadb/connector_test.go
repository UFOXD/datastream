package mariadb

import (
	"testing"

	"github.com/UFOXD/datastream/internal/source"
	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "default config",
			config:  DefaultConfig(),
			wantErr: false,
		},
		{
			name: "missing host",
			config: &Config{
				Port:     3306,
				ServerID: 1001,
			},
			wantErr: true,
		},
		{
			name: "missing serverId",
			config: &Config{
				Host: "localhost",
				Port: 3306,
			},
			wantErr: true,
		},
		{
			name: "invalid SSL mode",
			config: &Config{
				Host:     "localhost",
				Port:     3306,
				ServerID: 1001,
				SSLMode:  "invalid",
			},
			wantErr: true,
		},
		{
			name: "valid config with all fields",
			config: &Config{
				Host:                "localhost",
				Port:                3306,
				User:                "test",
				Password:            "test",
				ServerID:            1001,
				UseGTID:             true,
				SnapshotMode:        source.SnapshotModeInitial,
				IncludeSchemaEvents: true,
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

func TestConfig_DefaultValues(t *testing.T) {
	cfg := DefaultConfig()
	assert.Equal(t, "localhost", cfg.Host)
	assert.Equal(t, 3306, cfg.Port)
	assert.Equal(t, uint32(1001), cfg.ServerID)
	assert.Equal(t, source.SnapshotModeInitial, cfg.SnapshotMode)
	assert.Equal(t, "UTC", cfg.Timezone)
	assert.True(t, cfg.IncludeSchemaEvents)
	assert.False(t, cfg.UseGTID)
}

func TestConnector_New(t *testing.T) {
	conn := New()
	assert.NotNil(t, conn)
	assert.Equal(t, "mariadb", conn.Name())
	assert.Equal(t, source.StateUninitialized, conn.Status().State)
}

func TestConnector_Events(t *testing.T) {
	conn := New()
	assert.NotNil(t, conn.Events())
}

func TestConnector_Errors(t *testing.T) {
	conn := New()
	assert.NotNil(t, conn.Errors())
}

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		pattern string
		s       string
		want    bool
	}{
		{"*", "anything", true},
		{"test", "test", true},
		{"test", "other", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.s, func(t *testing.T) {
			assert.Equal(t, tt.want, matchPattern(tt.pattern, tt.s))
		})
	}
}

func TestFactory_Create(t *testing.T) {
	f := &factory{}
	conn, err := f.Create(source.Config{})
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.Equal(t, "mariadb", conn.Name())
}
