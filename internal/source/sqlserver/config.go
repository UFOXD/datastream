// Package sqlserver provides SQL Server source connector for DataStream.
package sqlserver

import (
	"time"

	"github.com/UFOXD/datastream/internal/source"
)

// Config holds SQL Server-specific configuration.
type Config struct {
	// Connection settings
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`

	// CDC settings
	PollInterval time.Duration `json:"pollInterval"`
	BatchSize    int           `json:"batchSize"`

	// Tables
	Schemas []string          `json:"schemas"`
	Tables  map[string]string `json:"tables"` // schema -> pattern

	// Data directory for local schema history
	DataDir string `json:"dataDir,omitempty"`
}

// DefaultConfig returns the default SQL Server configuration.
func DefaultConfig() *Config {
	return &Config{
		Port:         1433,
		PollInterval: time.Second,
		BatchSize:    1000,
		Schemas:      []string{"dbo"},
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
	if c.Database == "" {
		return source.ErrInvalidConfig
	}
	return nil
}
