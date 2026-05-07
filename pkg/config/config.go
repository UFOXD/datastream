// Package config provides configuration management for DataStream.
package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/pelletier/go-toml/v2"
	"github.com/your-org/datastream/pkg/logutil"
)

// Config is the root configuration for DataStream.
type Config struct {
	Name       string             `toml:"name" json:"name"`
	Server     ServerConfig       `toml:"server" json:"server"`
	Log        logutil.LogConfig  `toml:"log" json:"log"`
	Coordinator CoordinatorConfig `toml:"coordinator" json:"coordinator"`
	Security   SecurityConfig     `toml:"security" json:"security"`
}

// ServerConfig holds server-related configuration.
type ServerConfig struct {
	Addr          string `toml:"addr" json:"addr"`
	APIAddr       string `toml:"api-addr" json:"api-addr"`
	AdvertiseAddr string `toml:"advertise-addr" json:"advertise-addr"`
	DataDir       string `toml:"data-dir" json:"data-dir"`
	GCTTL         int64  `toml:"gc-ttl" json:"gc-ttl"`
}

// CoordinatorConfig holds coordinator backend configuration.
type CoordinatorConfig struct {
	Backend         string   `toml:"backend" json:"backend"`
	Endpoints       []string `toml:"endpoints" json:"endpoints"`
	SessionTTL      int      `toml:"session-ttl" json:"session-ttl"`
	ElectionTimeout int      `toml:"election-timeout" json:"election-timeout"`
}

// SecurityConfig holds security/TLS configuration.
type SecurityConfig struct {
	SSLCa    string `toml:"ssl-ca" json:"ssl-ca"`
	SSLCert  string `toml:"ssl-cert" json:"ssl-cert"`
	SSLKey   string `toml:"ssl-key" json:"ssl-key"`
	Insecure bool   `toml:"insecure" json:"insecure"`
}

// Default values.
const (
	defaultAddr              = ":8300"
	defaultAPIAddr           = ":8301"
	defaultLogLevel          = "info"
	defaultLogMaxSize        = 512 // MB
	defaultLogMaxDays        = 7
	defaultDataDir           = "./data"
	defaultGCTTL             = 86400 // 24 hours
	defaultCoordinatorBackend = "etcd"
	defaultSessionTTL        = 10
	defaultElectionTimeout   = 5000
)

// Adjust fills in default values for empty fields.
func (c *Config) Adjust() {
	if c.Server.Addr == "" {
		c.Server.Addr = defaultAddr
	}
	if c.Server.APIAddr == "" {
		c.Server.APIAddr = defaultAPIAddr
	}
	if c.Server.DataDir == "" {
		c.Server.DataDir = defaultDataDir
	}
	if c.Server.GCTTL == 0 {
		c.Server.GCTTL = defaultGCTTL
	}

	if c.Log.Level == "" {
		c.Log.Level = defaultLogLevel
	}
	if c.Log.Level == "warning" {
		c.Log.Level = "warn"
	}
	if c.Log.MaxSize == 0 {
		c.Log.MaxSize = defaultLogMaxSize
	}
	if c.Log.MaxDays == 0 {
		c.Log.MaxDays = defaultLogMaxDays
	}
	if c.Log.MaxBackups == 0 {
		c.Log.MaxBackups = 3
	}

	if c.Coordinator.Backend == "" {
		c.Coordinator.Backend = defaultCoordinatorBackend
	}
	if c.Coordinator.SessionTTL == 0 {
		c.Coordinator.SessionTTL = defaultSessionTTL
	}
	if c.Coordinator.ElectionTimeout == 0 {
		c.Coordinator.ElectionTimeout = defaultElectionTimeout
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.Server.Addr == "" {
		return fmt.Errorf("server addr is required")
	}
	if c.Server.DataDir == "" {
		return fmt.Errorf("data-dir is required")
	}
	if c.Coordinator.Backend != "memory" && len(c.Coordinator.Endpoints) == 0 {
		return fmt.Errorf("coordinator endpoints is required when backend is not memory")
	}
	return nil
}

// LoadFromFile loads configuration from a TOML file.
func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := &Config{}
	if err := toml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	cfg.Adjust()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Load loads configuration from file and environment variables.
func Load(path string) (*Config, error) {
	cfg, err := LoadFromFile(path)
	if err != nil {
		return nil, err
	}

	// Override with environment variables
	if err := cfg.LoadFromEnv(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// LoadFromEnv overrides configuration from environment variables.
// Format: DATASTREAM_<SECTION>_<KEY>
// Example: DATASTREAM_SERVER_ADDR=:8302
func (c *Config) LoadFromEnv() error {
	envPrefix := "DATASTREAM_"

	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, envPrefix) {
			continue
		}
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimPrefix(parts[0], envPrefix)
		val := parts[1]

		// Simple environment variable mapping
		switch strings.ToLower(key) {
		case "name":
			c.Name = val
		case "server_addr":
			c.Server.Addr = val
		case "server_api_addr":
			c.Server.APIAddr = val
		case "server_advertise_addr":
			c.Server.AdvertiseAddr = val
		case "server_data_dir":
			c.Server.DataDir = val
		case "server_gc_ttl":
			fmt.Sscanf(val, "%d", &c.Server.GCTTL)
		case "log_level":
			c.Log.Level = val
		case "log_file":
			c.Log.File = val
		case "coordinator_backend":
			c.Coordinator.Backend = val
		case "coordinator_endpoints":
			c.Coordinator.Endpoints = strings.Split(val, ",")
		}
	}

	return nil
}
