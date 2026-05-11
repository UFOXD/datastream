// Package redis provides a Redis sink connector for DataStream.
package redis

import (
	"fmt"
	"time"
)

// Config holds the Redis sink connector configuration.
type Config struct {
	// Connection
	Addr     string `toml:"addr"`     // host:port
	Password string `toml:"password"`
	DB       int    `toml:"db"`

	// Write settings
	KeyPattern    string        `toml:"key_pattern"`    // default: "{database}:{table}:{pk}"
	TTL           time.Duration `toml:"ttl"`            // 0 = no expiration
	BatchSize     int           `toml:"batch_size"`     // default: 1000
	FlushInterval time.Duration `toml:"flush_interval"` // default: 1s

	// Data format
	Format string `toml:"format"` // "hash", "json", "string"
}

// DefaultConfig returns the default Redis sink configuration.
func DefaultConfig() *Config {
	return &Config{
		Addr:          "localhost:6379",
		KeyPattern:    "{database}:{table}:{pk}",
		BatchSize:     1000,
		FlushInterval: time.Second,
		Format:        "hash",
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.Addr == "" {
		return fmt.Errorf("redis addr is required")
	}

	switch c.Format {
	case "hash", "json", "string":
		// Valid
	default:
		return fmt.Errorf("invalid format: %q, must be one of: hash, json, string", c.Format)
	}

	if c.BatchSize <= 0 {
		return fmt.Errorf("batch_size must be positive")
	}

	return nil
}
