package sqlserver

import (
	"testing"
	"time"

	"github.com/UFOXD/datastream/internal/source"
)

// Interface compliance check
var _ source.Connector = (*Connector)(nil)

func TestConnectorName(t *testing.T) {
	c := New()
	if c.Name() != "sqlserver" {
		t.Errorf("expected name 'sqlserver', got %q", c.Name())
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
				User:     "sa",
				Password: "Test@123456",
				Database: "mydb",
			},
			wantErr: true,
		},
		{
			name: "missing user",
			config: &Config{
				Host:     "localhost",
				Password: "Test@123456",
				Database: "mydb",
			},
			wantErr: true,
		},
		{
			name: "missing password",
			config: &Config{
				Host:     "localhost",
				User:     "sa",
				Database: "mydb",
			},
			wantErr: true,
		},
		{
			name: "missing database",
			config: &Config{
				Host:     "localhost",
				User:     "sa",
				Password: "Test@123456",
			},
			wantErr: true,
		},
		{
			name: "valid config",
			config: &Config{
				Host:     "localhost",
				Port:     1433,
				User:     "sa",
				Password: "Test@123456",
				Database: "mydb",
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

	if cfg.Port != 1433 {
		t.Errorf("expected default port 1433, got %d", cfg.Port)
	}
	if cfg.PollInterval != time.Second {
		t.Errorf("expected default poll interval 1s, got %v", cfg.PollInterval)
	}
	if cfg.BatchSize != 1000 {
		t.Errorf("expected default batch size 1000, got %d", cfg.BatchSize)
	}
	if len(cfg.Schemas) != 1 || cfg.Schemas[0] != "dbo" {
		t.Errorf("expected default schemas [dbo], got %v", cfg.Schemas)
	}
}
