package config

import (
	"os"
	"testing"
)

func TestLoadFromFile(t *testing.T) {
	// Create a temporary config file
	content := `
name = "test-datastream"

[server]
addr = ":8300"
api-addr = ":8301"
data-dir = "/tmp/data"

[log]
level = "debug"

[coordinator]
backend = "memory"
`

	tmpFile, err := os.CreateTemp("", "config-*.toml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write([]byte(content)); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	tmpFile.Close()

	cfg, err := LoadFromFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadFromFile failed: %v", err)
	}

	if cfg.Name != "test-datastream" {
		t.Errorf("Expected name 'test-datastream', got '%s'", cfg.Name)
	}

	if cfg.Server.Addr != ":8300" {
		t.Errorf("Expected addr ':8300', got '%s'", cfg.Server.Addr)
	}

	if cfg.Log.Level != "debug" {
		t.Errorf("Expected log level 'debug', got '%s'", cfg.Log.Level)
	}

	if cfg.Coordinator.Backend != "memory" {
		t.Errorf("Expected coordinator backend 'memory', got '%s'", cfg.Coordinator.Backend)
	}
}

func TestAdjust(t *testing.T) {
	cfg := &Config{}
	cfg.Adjust()

	if cfg.Server.Addr != ":8300" {
		t.Errorf("Expected default addr ':8300', got '%s'", cfg.Server.Addr)
	}

	if cfg.Log.Level != "info" {
		t.Errorf("Expected default log level 'info', got '%s'", cfg.Log.Level)
	}

	if cfg.Coordinator.Backend != "etcd" {
		t.Errorf("Expected default coordinator backend 'etcd', got '%s'", cfg.Coordinator.Backend)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				Server: ServerConfig{
					Addr:    ":8300",
					DataDir: "/tmp/data",
				},
				Coordinator: CoordinatorConfig{
					Backend: "memory",
				},
			},
			wantErr: false,
		},
		{
			name: "missing addr",
			config: &Config{
				Server: ServerConfig{
					DataDir: "/tmp/data",
				},
			},
			wantErr: true,
		},
		{
			name: "missing data-dir",
			config: &Config{
				Server: ServerConfig{
					Addr: ":8300",
				},
			},
			wantErr: true,
		},
		{
			name: "missing endpoints for etcd",
			config: &Config{
				Server: ServerConfig{
					Addr:    ":8300",
					DataDir: "/tmp/data",
				},
				Coordinator: CoordinatorConfig{
					Backend: "etcd",
				},
			},
			wantErr: true,
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

func TestConfig_Cluster_Default(t *testing.T) {
	cfg := &Config{}
	cfg.Adjust()
	if cfg.Cluster != "default" {
		t.Errorf("cluster default = %q, want %q", cfg.Cluster, "default")
	}
}

func TestConfig_Metrics_Defaults(t *testing.T) {
	cfg := &Config{}
	cfg.Adjust()
	if !cfg.Metrics.Enabled {
		t.Error("metrics.enabled default should be true")
	}
	if cfg.Metrics.ScrapeInterval == 0 {
		t.Error("metrics.scrape_interval default unset")
	}
	if cfg.Metrics.StatsTimeout == 0 {
		t.Error("metrics.stats_timeout default unset")
	}
}
