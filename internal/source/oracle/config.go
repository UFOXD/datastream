// Package oracle provides an Oracle source connector for DataStream.
package oracle

import (
	"fmt"
	"time"

	"github.com/UFOXD/datastream/internal/source"
)

// Config holds Oracle-specific configuration.
type Config struct {
	// Connection settings
	Host        string `toml:"host"`
	Port        int    `toml:"port"`         // default: 1521
	User        string `toml:"user"`
	Password    string `toml:"password"`
	ServiceName string `toml:"service_name"`

	// LogMiner settings
	MiningStrategy string        `toml:"mining_strategy"` // "continuous", "online"
	PollInterval   time.Duration `toml:"poll_interval"`   // default: 1s
	BatchSize      int           `toml:"batch_size"`      // default: 1000

	// Tables
	Schemas []string          `toml:"schemas"`
	Tables  map[string]string `toml:"tables"`
}

// DefaultConfig returns the default Oracle configuration.
func DefaultConfig() *Config {
	return &Config{
		Port:           1521,
		MiningStrategy: "continuous",
		PollInterval:   time.Second,
		BatchSize:      1000,
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.Host == "" {
		return source.ErrInvalidConfig
	}
	if c.User == "" {
		return source.ErrInvalidConfig
	}
	if c.Password == "" {
		return source.ErrInvalidConfig
	}
	if c.ServiceName == "" {
		return source.ErrInvalidConfig
	}
	switch c.MiningStrategy {
	case "continuous", "online":
		// valid
	default:
		return fmt.Errorf("invalid mining_strategy %q: must be \"continuous\" or \"online\"", c.MiningStrategy)
	}
	return nil
}
