// Package oracle provides Oracle sink connector for DataStream.
package oracle

import (
	"github.com/UFOXD/datastream/internal/sink"
)

// Config holds Oracle-specific configuration.
type Config struct {
	// Connection settings
	Host        string `json:"host"`
	Port        int    `json:"port"`
	User        string `json:"user"`
	Password    string `json:"password"`
	ServiceName string `json:"serviceName"`

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
	BatchSize  int `json:"batchSize"`
	MaxRetries int `json:"maxRetries"`

	// Transaction settings
	UseTransaction bool `json:"useTransaction"`

	// DDL settings
	DDLPolicy string `json:"ddlPolicy"` // ignore, apply, error

	// Schema settings
	DefaultSchema string `json:"defaultSchema"`

	// Batch config
	Batch sink.BatchConfig `json:"batch"`
}

// DefaultConfig returns the default Oracle configuration.
func DefaultConfig() *Config {
	return &Config{
		Port:           1521,
		MaxConnections: 10,
		MaxIdle:        5,
		ConnectTimeout: 30,
		BatchSize:      1000,
		MaxRetries:     3,
		UseTransaction: true,
		DDLPolicy:      "apply",
		DefaultSchema:  "",
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
	if c.ServiceName == "" {
		return sink.ErrInvalidConfig
	}
	return nil
}
