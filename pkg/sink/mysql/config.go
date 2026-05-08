// Package mysql provides MySQL sink connector for DataStream.
package mysql

import (
	"github.com/UFOXD/datastream/pkg/sink"
)

// Config holds MySQL-specific configuration.
type Config struct {
	// Connection settings
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`

	// SSL settings
	SSLMode   string `json:"sslMode"`
	SSLCert   string `json:"sslCert"`
	SSLKey    string `json:"sslKey"`
	SSLCACert string `json:"sslCaCert"`

	// Connection pool
	MaxConnections int `json:"maxConnections"`
	MaxIdle        int `json:"maxIdle"`
	ConnectTimeout int `json:"connectTimeout"`

	// Write settings
	BatchSize      int    `json:"batchSize"`
	InsertStrategy string `json:"insertStrategy"` // insert, replace, upsert
	MaxRetries     int    `json:"maxRetries"`
	RetryBackoff   int    `json:"retryBackoff"` // ms

	// Transaction settings
	UseTransaction bool   `json:"useTransaction"`
	IsolationLevel string `json:"isolationLevel"`

	// DDL settings
	AutoCreateTable bool   `json:"autoCreateTable"`
	AutoAlterTable  bool   `json:"autoAlterTable"`
	DDLPolicy       string `json:"ddlPolicy"` // ignore, apply, error

	// Batch config
	Batch sink.BatchConfig `json:"batch"`
}

// DefaultConfig returns the default MySQL configuration.
func DefaultConfig() *Config {
	return &Config{
		Port:            3306,
		InsertStrategy:  "upsert",
		MaxRetries:      3,
		RetryBackoff:    100,
		MaxConnections:  10,
		MaxIdle:         5,
		ConnectTimeout:  30,
		BatchSize:       100,
		UseTransaction:  true,
		IsolationLevel:  "READ COMMITTED",
		AutoCreateTable: true,
		AutoAlterTable:  false,
		DDLPolicy:       "apply",
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
