// Package mysql provides MySQL source connector for DataStream.
package mysql

import (
	"time"

	"github.com/UFOXD/datastream/pkg/source"
)

// Config holds MySQL-specific configuration.
type Config struct {
	// Connection settings
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`

	// Server ID for binlog replication (must be unique)
	ServerID uint32 `json:"serverId"`

	// Databases and tables to capture
	Databases []string          `json:"databases"`
	Tables    map[string]string `json:"tables"` // db -> table pattern

	// Binlog settings
	BinlogFile string `json:"binlogFile,omitempty"` // Start from specific binlog file
	BinlogPos  uint32 `json:"binlogPos,omitempty"`  // Start from specific position

	// Snapshot settings
	SnapshotMode      source.SnapshotMode `json:"snapshotMode"`
	SnapshotThreads   int                 `json:"snapshotThreads"`
	SnapshotLock      bool                `json:"snapshotLock"` // Use FTWRL during snapshot
	SnapshotHighWater int                 `json:"snapshotHighWater"`

	// Timezone for timestamp conversion
	Timezone string `json:"timezone"`

	// SSL settings
	SSLMode     string `json:"sslMode"`
	SSLCert     string `json:"sslCert"`
	SSLKey      string `json:"sslKey"`
	SSLCACert   string `json:"sslCaCert"`
	SSLAllowNaN bool   `json:"sslAllowNaN"`

	// Connection pool
	MaxConnections int `json:"maxConnections"`
	MaxIdle        int `json:"maxIdle"`
	ConnectTimeout int `json:"connectTimeout"` // seconds
	ReadTimeout    int `json:"readTimeout"`    // seconds
	WriteTimeout   int `json:"writeTimeout"`   // seconds

	// Binlog settings
	BinlogRowImage      string `json:"binlogRowImage"`      // full, minimal, noblob
	BinlogBuffer        int    `json:"binlogBuffer"`        // bytes
	HeartbeatInterval   int    `json:"heartbeatInterval"`   // seconds
	HeartbeatTimeout    int    `json:"heartbeatTimeout"`    // seconds
	BinlogQueueSize     int    `json:"binlogQueueSize"`     // events
	IncludeSchemaEvents bool   `json:"includeSchemaEvents"` // capture DDL events

	// Big TX handling
	BigTXThreshold int64 `json:"bigTxThreshold"` // bytes
	BigTXTimeout   int   `json:"bigTxTimeout"`   // seconds

	// Decimal handling
	DecimalHandling string `json:"decimalHandling"` // string, double

	// Offset storage
	OffsetBackend   string `json:"offsetBackend"`
	OffsetPath      string `json:"offsetPath"`
	OffsetFlushMs   int    `json:"offsetFlushMs"`
	OffsetEtcdAddrs string `json:"offsetEtcdAddrs"`
}

// DefaultConfig returns the default MySQL configuration.
func DefaultConfig() *Config {
	return &Config{
		Port:              3306,
		ServerID:          uint32(time.Now().Unix()),
		SnapshotMode:      source.SnapshotModeInitial,
		SnapshotThreads:   1,
		SnapshotLock:      true,
		Timezone:          "UTC",
		MaxConnections:    10,
		MaxIdle:           5,
		ConnectTimeout:    30,
		ReadTimeout:       30,
		WriteTimeout:      30,
		BinlogRowImage:    "full",
		BinlogBuffer:      1024 * 1024,
		HeartbeatInterval: 30,
		HeartbeatTimeout:  60,
		BinlogQueueSize:   1000,
		DecimalHandling:   "string",
		OffsetBackend:     "file",
		OffsetFlushMs:     1000,
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
	if c.ServerID == 0 {
		return source.ErrInvalidConfig
	}
	return nil
}
