package oracle

import (
	"testing"

	"github.com/UFOXD/datastream/internal/source"
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
