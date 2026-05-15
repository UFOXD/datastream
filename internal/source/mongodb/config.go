// Package mongodb provides a MongoDB source connector for DataStream.
package mongodb

import (
	"fmt"
	"time"

	"github.com/UFOXD/datastream/internal/source"
)

// Config holds the MongoDB source connector configuration.
type Config struct {
	// Connection settings
	Hosts         []string `json:"hosts"`         // MongoDB hosts (e.g., ["localhost:27017"])
	User          string   `json:"user"`          // Username
	Password      string   `json:"password"`      // Password
	Database      string   `json:"database"`      // Database name (for auth)
	ReplicaSet    string   `json:"replicaSet"`    // Replica set name
	AuthSource    string   `json:"authSource"`    // Auth source database (default: admin)
	AuthMechanism string   `json:"authMechanism"` // Auth mechanism (SCRAM-SHA-1, SCRAM-SHA-256, etc.)

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

	// Change Stream settings
	ResumeToken        string `json:"resumeToken"`        // Resume token for change stream
	FullDocument       string `json:"fullDocument"`       // Full document mode: default, updateLookup, whenAvailable, required
	FullDocumentBefore string `json:"fullDocumentBefore"` // Full document before mode: default, whenAvailable, required
	MaxAwaitTime       int    `json:"maxAwaitTime"`       // Max await time for change stream in milliseconds
	BatchSize          int32  `json:"batchSize"`          // Batch size for change stream

	// Snapshot settings
	SnapshotMode      source.SnapshotMode `json:"snapshotMode"`      // Snapshot mode: never, initial, always
	SnapshotThreads   int                 `json:"snapshotThreads"`   // Number of parallel snapshot threads
	SnapshotBatchSize int                 `json:"snapshotBatchSize"` // Batch size for snapshot

	// Filter settings
	Databases   []string          `json:"databases"`   // Databases to capture (empty = all)
	Collections map[string]string `json:"collections"` // Database -> collection pattern

	// Offset storage settings
	OffsetBackend string `json:"offsetBackend"`
	OffsetPath    string `json:"offsetPath"`
	OffsetFlushMs int    `json:"offsetFlushMs"`
}

// DefaultConfig returns the default MongoDB source configuration.
func DefaultConfig() *Config {
	return &Config{
		Hosts:             []string{"localhost:27017"},
		AuthSource:        "admin",
		AuthMechanism:     "SCRAM-SHA-256",
		MaxConnections:    10,
		MaxIdle:           2,
		ConnectTimeout:    30,
		SocketTimeout:     0, // No timeout
		ServerSelection:   30,
		FullDocument:      "updateLookup",
		MaxAwaitTime:      1000,
		BatchSize:         100,
		SnapshotMode:      source.SnapshotModeInitial,
		SnapshotThreads:   4,
		SnapshotBatchSize: 1000,
		OffsetFlushMs:     1000,
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if len(c.Hosts) == 0 {
		return fmt.Errorf("at least one host is required")
	}

	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 30
	}

	if c.ServerSelection <= 0 {
		c.ServerSelection = 30
	}

	// Validate full document mode
	switch c.FullDocument {
	case "", "default", "updateLookup", "whenAvailable", "required":
		// Valid
	default:
		return fmt.Errorf("invalid fullDocument mode: %s", c.FullDocument)
	}

	// Validate full document before mode
	switch c.FullDocumentBefore {
	case "", "default", "whenAvailable", "required":
		// Valid
	default:
		return fmt.Errorf("invalid fullDocumentBefore mode: %s", c.FullDocumentBefore)
	}

	return nil
}

// ConnectionString builds the MongoDB connection string.
func (c *Config) ConnectionString() string {
	// Build connection string
	// mongodb://[username:password@]host1[:port1][,host2[:port2],...]/[database][?options]

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

// MaxAwaitTimeDuration returns the max await time as a duration.
func (c *Config) MaxAwaitTimeDuration() time.Duration {
	if c.MaxAwaitTime <= 0 {
		return time.Second
	}
	return time.Duration(c.MaxAwaitTime) * time.Millisecond
}
