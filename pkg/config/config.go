// Package config provides configuration management for DataStream.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/UFOXD/datastream/pkg/logutil"
	"github.com/pelletier/go-toml/v2"
)

// Config is the root configuration for DataStream.
type Config struct {
	Name        string            `toml:"name" json:"name"`
	Cluster     string            `toml:"cluster" json:"cluster"`
	Server      ServerConfig      `toml:"server" json:"server"`
	Log         logutil.LogConfig `toml:"log" json:"log"`
	Coordinator CoordinatorConfig `toml:"coordinator" json:"coordinator"`
	Security    SecurityConfig    `toml:"security" json:"security"`
	Pipeline    PipelineConfig    `toml:"pipeline" json:"pipeline"`
	Metrics     MetricsConfig     `toml:"metrics" json:"metrics"`
}

// PipelineConfig holds pipeline-level configuration.
type PipelineConfig struct {
	Cache CacheConfig `toml:"cache" json:"cache"`
}

// CacheConfig holds binlog cache configuration.
type CacheConfig struct {
	MaxSize string `toml:"max-size" json:"max-size"` // e.g. "80%", "100GB", "500MB"
	Sync    string `toml:"sync" json:"sync"`          // "none", "batch", "every"
}

// MetricsConfig configures Prometheus metric collection.
type MetricsConfig struct {
	Enabled        bool          `toml:"enabled" json:"enabled"`
	ScrapeInterval time.Duration `toml:"scrape-interval" json:"scrape-interval"`
	StatsTimeout   time.Duration `toml:"stats-timeout" json:"stats-timeout"`
}

// ServerConfig holds server-related configuration.
type ServerConfig struct {
	Addr          string `toml:"addr" json:"addr"`
	APIAddr       string `toml:"api-addr" json:"api-addr"`
	HTTPAddr      string `toml:"http-addr" json:"http-addr"`
	AdvertiseAddr string `toml:"advertise-addr" json:"advertise-addr"`
	DataDir       string `toml:"data-dir" json:"data-dir"`
	GCTTL         int64  `toml:"gc-ttl" json:"gc-ttl"`
	ReadTimeout   int    `toml:"read-timeout" json:"read-timeout"`
	WriteTimeout  int    `toml:"write-timeout" json:"write-timeout"`
	IdleTimeout   int    `toml:"idle-timeout" json:"idle-timeout"`
}

// CoordinatorConfig holds coordinator backend configuration.
type CoordinatorConfig struct {
	Type            string     `toml:"type" json:"type"`
	Backend         string     `toml:"backend" json:"backend"`
	Endpoints       []string   `toml:"endpoints" json:"endpoints"`
	SessionTTL      int        `toml:"session-ttl" json:"session-ttl"`
	ElectionTimeout int        `toml:"election-timeout" json:"election-timeout"`
	Etcd            EtcdConfig `toml:"etcd" json:"etcd"`
}

// EtcdConfig holds etcd-specific configuration.
type EtcdConfig struct {
	Endpoints   []string `toml:"endpoints" json:"endpoints"`
	DialTimeout int      `toml:"dial-timeout" json:"dial-timeout"`
	Username    string   `toml:"username" json:"username"`
	Password    string   `toml:"password" json:"password"`
	TLSCA       string   `toml:"tls-ca" json:"tls-ca"`
	TLSCert     string   `toml:"tls-cert" json:"tls-cert"`
	TLSKey      string   `toml:"tls-key" json:"tls-key"`
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
	defaultAddr               = ":8300"
	defaultAPIAddr            = ":8301"
	defaultLogLevel           = "info"
	defaultLogMaxSize         = 512 // MB
	defaultLogMaxDays         = 7
	defaultDataDir            = "./data"
	defaultGCTTL              = 86400 // 24 hours
	defaultCoordinatorBackend = "etcd"
	defaultSessionTTL         = 10
	defaultElectionTimeout    = 5000
)

// Adjust fills in default values for empty fields.
func (c *Config) Adjust() {
	if c.Server.Addr == "" {
		c.Server.Addr = defaultAddr
	}
	if c.Server.APIAddr == "" {
		c.Server.APIAddr = defaultAPIAddr
	}
	if c.Server.HTTPAddr == "" {
		c.Server.HTTPAddr = defaultAddr
	}
	if c.Server.DataDir == "" {
		c.Server.DataDir = defaultDataDir
	}
	if c.Server.GCTTL == 0 {
		c.Server.GCTTL = defaultGCTTL
	}
	if c.Server.ReadTimeout == 0 {
		c.Server.ReadTimeout = 30
	}
	if c.Server.WriteTimeout == 0 {
		c.Server.WriteTimeout = 30
	}
	if c.Server.IdleTimeout == 0 {
		c.Server.IdleTimeout = 120
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

	if c.Coordinator.Type == "" {
		c.Coordinator.Type = defaultCoordinatorBackend
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
	if len(c.Coordinator.Etcd.Endpoints) == 0 && len(c.Coordinator.Endpoints) > 0 {
		c.Coordinator.Etcd.Endpoints = c.Coordinator.Endpoints
	}
	if c.Coordinator.Etcd.DialTimeout == 0 {
		c.Coordinator.Etcd.DialTimeout = 5
	}

	// Pipeline defaults
	if c.Pipeline.Cache.MaxSize == "" {
		c.Pipeline.Cache.MaxSize = "80%"
	}
	if c.Pipeline.Cache.Sync == "" {
		c.Pipeline.Cache.Sync = "batch"
	}

	// Cluster + metrics defaults
	if c.Cluster == "" {
		c.Cluster = "default"
	}
	if c.Metrics.ScrapeInterval == 0 {
		c.Metrics.ScrapeInterval = 5 * time.Second
	}
	if c.Metrics.StatsTimeout == 0 {
		c.Metrics.StatsTimeout = time.Second
	}
	// Enabled defaults to true: if the entire MetricsConfig is empty (no
	// settings provided), treat as enabled. Users who want to disable must
	// either set enabled=false explicitly in TOML or pass a non-zero interval.
	if !c.Metrics.Enabled && c.Metrics.ScrapeInterval == 5*time.Second && c.Metrics.StatsTimeout == time.Second {
		c.Metrics.Enabled = true
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
		case "pipeline_cache_max_size":
			c.Pipeline.Cache.MaxSize = val
		case "pipeline_cache_sync":
			c.Pipeline.Cache.Sync = val
		}
	}

	return nil
}
