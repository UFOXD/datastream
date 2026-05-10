// Package postgres provides PostgreSQL sink connector for DataStream.
package postgres

import (
	"github.com/UFOXD/datastream/internal/sink"
)

// Config holds PostgreSQL-specific configuration.
type Config struct {
	// Connection settings
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`

	// SSL settings
	SSLMode     string `json:"sslMode"`
	SSLCert     string `json:"sslCert"`
	SSLKey      string `json:"sslKey"`
	SSLRootCert string `json:"sslRootCert"`

	// Connection pool
	MaxConnections int `json:"maxConnections"`
	MaxIdle        int `json:"maxIdle"`
	ConnectTimeout int `json:"connectTimeout"` // seconds

	// Write settings
	BatchSize      int    `json:"batchSize"`
	InsertStrategy string `json:"insertStrategy"` // insert, upsert
	MaxRetries     int    `json:"maxRetries"`
	RetryBackoff   int    `json:"retryBackoff"` // ms

	// Transaction settings
	UseTransaction bool `json:"useTransaction"`

	// COPY protocol settings
	UseCopy       bool `json:"useCopy"`       // Use COPY for bulk inserts
	CopyBatchSize int  `json:"copyBatchSize"` // Rows per COPY operation

	// DDL settings
	AutoCreateTable bool   `json:"autoCreateTable"`
	DDLPolicy       string `json:"ddlPolicy"` // ignore, apply, error

	// Schema settings
	DefaultSchema string `json:"defaultSchema"` // Default schema (default: public)

	// Batch config
	Batch sink.BatchConfig `json:"batch"`
}

// DefaultConfig returns the default PostgreSQL configuration.
func DefaultConfig() *Config {
	return &Config{
		Port:            5432,
		InsertStrategy:  "upsert",
		MaxRetries:      3,
		RetryBackoff:    100,
		MaxConnections:  10,
		MaxIdle:         5,
		ConnectTimeout:  30,
		BatchSize:       100,
		UseTransaction:  true,
		UseCopy:         true,
		CopyBatchSize:   1000,
		AutoCreateTable: false,
		DDLPolicy:       "apply",
		DefaultSchema:   "public",
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
