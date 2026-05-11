// Package elasticsearch provides an Elasticsearch sink connector for DataStream.
package elasticsearch

import (
	"fmt"
	"time"
)

// Config holds the Elasticsearch sink connector configuration.
type Config struct {
	// Connection
	URLs     []string `toml:"urls"`
	Username string   `toml:"username"`
	Password string   `toml:"password"`
	APIKey   string   `toml:"api_key"`

	// Index settings
	IndexPrefix  string `toml:"index_prefix"`
	IndexPattern string `toml:"index_pattern"` // default: "{database}_{table}"

	// Bulk settings
	BatchSize     int           `toml:"batch_size"`     // default: 1000
	FlushInterval time.Duration `toml:"flush_interval"` // default: 1s

	// Write settings
	RefreshPolicy   string `toml:"refresh_policy"`    // "true", "wait_for", "false"
	RetryOnConflict int    `toml:"retry_on_conflict"` // default: 3
}

// DefaultConfig returns the default Elasticsearch sink configuration.
func DefaultConfig() *Config {
	return &Config{
		IndexPattern:    "{database}_{table}",
		BatchSize:       1000,
		FlushInterval:   time.Second,
		RefreshPolicy:   "false",
		RetryOnConflict: 3,
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if len(c.URLs) == 0 {
		return fmt.Errorf("at least one URL is required")
	}

	// Validate refresh policy: "true", "wait_for", "false"
	switch c.RefreshPolicy {
	case "true", "wait_for", "false":
		// Valid
	default:
		return fmt.Errorf("invalid refresh policy: %q, must be one of: true, wait_for, false", c.RefreshPolicy)
	}

	return nil
}
