// Package mongodb provides a MongoDB sink connector for DataStream.
package mongodb

import (
	"fmt"

	"github.com/UFOXD/datastream/internal/sink"
)

// Config holds the MongoDB sink connector configuration.
type Config struct {
	// Connection settings
	Hosts         []string `json:"hosts"`         // MongoDB hosts (e.g., ["localhost:27017"])
	User          string   `json:"user"`          // Username
	Password      string   `json:"password"`      // Password
	Database      string   `json:"database"`      // Database name
	ReplicaSet    string   `json:"replicaSet"`    // Replica set name
	AuthSource    string   `json:"authSource"`    // Auth source database (default: admin)
	AuthMechanism string   `json:"authMechanism"` // Auth mechanism

	// SSL/TLS settings
	SSLMode     bool   `json:"sslMode"`     // Enable SSL/TLS
	SSLCert     string `json:"sslCert"`     // Client certificate file
	SSLKey      string `json:"sslKey"`      // Client private key file
	SSLRootCert string `json:"sslRootCert"` // CA certificate file

	// Connection pool settings
	MaxConnections  int `json:"maxConnections"`  // Max pool size
	MaxIdle         int `json:"maxIdle"`         // Min pool size
	ConnectTimeout  int `json:"connectTimeout"`  // Connection timeout in seconds
	SocketTimeout   int `json:"socketTimeout"`   // Socket timeout in seconds
	ServerSelection int `json:"serverSelection"` // Server selection timeout in seconds

	// Write settings
	WriteStrategy string `json:"writeStrategy"` // insert, replace, update, upsert
	WriteConcern  string `json:"writeConcern"`  // w: majority, w: 1, etc.
	Ordered       bool   `json:"ordered"`       // Ordered writes
	BatchSize     int    `json:"batchSize"`     // Bulk write batch size

	// Document settings
	IDField string `json:"idField"` // Field to use as _id (default: _id)

	// DDL settings
	DDLPolicy string `json:"ddlPolicy"` // ignore, error

	// Retry settings
	MaxRetries   int `json:"maxRetries"`   // Maximum number of retries
	RetryBackoff int `json:"retryBackoff"` // Backoff in milliseconds

	// Batch configuration
	Batch sink.BatchConfig `json:"batch"`
}

// DefaultConfig returns the default MongoDB sink configuration.
func DefaultConfig() *Config {
	return &Config{
		Hosts:          []string{"localhost:27017"},
		AuthSource:     "admin",
		AuthMechanism:  "SCRAM-SHA-256",
		MaxConnections: 10,
		MaxIdle:        2,
		ConnectTimeout: 30,
		WriteStrategy:  "upsert",
		WriteConcern:   "majority",
		Ordered:        false,
		BatchSize:      1000,
		IDField:        "_id",
		DDLPolicy:      "ignore",
		MaxRetries:     3,
		RetryBackoff:   1000,
		Batch: sink.BatchConfig{
			Size:    1000,
			Timeout: 5000,
			Retries: 3,
		},
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if len(c.Hosts) == 0 {
		return fmt.Errorf("at least one host is required")
	}

	if c.Database == "" {
		return fmt.Errorf("database name is required")
	}

	// Validate write strategy
	switch c.WriteStrategy {
	case "insert", "replace", "update", "upsert":
		// Valid
	default:
		return fmt.Errorf("invalid write strategy: %s", c.WriteStrategy)
	}

	// Validate write concern
	switch c.WriteConcern {
	case "majority", "1", "0":
		// Valid
	default:
		// Could be custom write concern like "w:2"
	}

	// Validate DDL policy
	switch c.DDLPolicy {
	case "ignore", "error":
		// Valid
	default:
		return fmt.Errorf("invalid DDL policy: %s", c.DDLPolicy)
	}

	return nil
}

// ConnectionString builds the MongoDB connection string.
func (c *Config) ConnectionString() string {
	// Build connection string
	var cs string
	if c.User != "" && c.Password != "" {
		cs = fmt.Sprintf("mongodb://%s:%s@", c.User, c.Password)
	} else {
		cs = "mongodb://"
	}

	// Add hosts
	for i, host := range c.Hosts {
		if i > 0 {
			cs += ","
		}
		cs += host
	}

	// Add database
	cs += "/" + c.Database

	// Add options
	options := make([]string, 0)

	if c.ReplicaSet != "" {
		options = append(options, fmt.Sprintf("replicaSet=%s", c.ReplicaSet))
	}

	if c.AuthSource != "" {
		options = append(options, fmt.Sprintf("authSource=%s", c.AuthSource))
	}

	if c.AuthMechanism != "" {
		options = append(options, fmt.Sprintf("authMechanism=%s", c.AuthMechanism))
	}

	if c.SSLMode {
		options = append(options, "tls=true")
	}

	if c.ConnectTimeout > 0 {
		options = append(options, fmt.Sprintf("connectTimeoutMS=%d", c.ConnectTimeout*1000))
	}

	if c.ServerSelection > 0 {
		options = append(options, fmt.Sprintf("serverSelectionTimeoutMS=%d", c.ServerSelection*1000))
	}

	if c.SocketTimeout > 0 {
		options = append(options, fmt.Sprintf("socketTimeoutMS=%d", c.SocketTimeout*1000))
	}

	if c.MaxConnections > 0 {
		options = append(options, fmt.Sprintf("maxPoolSize=%d", c.MaxConnections))
	}

	if c.MaxIdle > 0 {
		options = append(options, fmt.Sprintf("minPoolSize=%d", c.MaxIdle))
	}

	if len(options) > 0 {
		cs += "?"
		for i, opt := range options {
			if i > 0 {
				cs += "&"
			}
			cs += opt
		}
	}

	return cs
}
