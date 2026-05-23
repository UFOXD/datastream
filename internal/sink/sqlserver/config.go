// Package sqlserver provides SQL Server sink connector for DataStream.
package sqlserver

import (
	"github.com/UFOXD/datastream/internal/sink"
)

// Config holds SQL Server-specific configuration.
type Config struct {
	// Connection settings
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`

	// Schema settings
	Schema string `json:"schema"` // default "dbo"

	// SSL settings
	SSLMode string `json:"sslMode"`

	// Connection pool
	MaxConnections int `json:"maxConnections"`
	ConnectTimeout int `json:"connectTimeout"` // seconds

	// Write settings
	BatchSize  int `json:"batchSize"`
	MaxRetries int `json:"maxRetries"`

	// Transaction settings
	UseTransaction bool `json:"useTransaction"`

	// DDL settings
	DDLPolicy string `json:"ddlPolicy"` // ignore, apply, error

	// Batch config
	Batch sink.BatchConfig `json:"batch"`
}

// DefaultConfig returns the default SQL Server configuration.
func DefaultConfig() *Config {
	return &Config{
		Port:           1433,
		Schema:         "dbo",
		MaxConnections: 10,
		ConnectTimeout: 30,
		BatchSize:      1000,
		MaxRetries:     3,
		UseTransaction: true,
		DDLPolicy:      "apply",
		Batch: sink.BatchConfig{
			Size:    100,
			Timeout: 1000,
			Retries: 3,
		},
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.Host == "" {
		return sink.ErrInvalidConfig
	}
	if c.User == "" {
		return sink.ErrInvalidConfig
	}
	if c.Database == "" {
		return sink.ErrInvalidConfig
	}
	return nil
}
