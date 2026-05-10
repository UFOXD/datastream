// Package postgres provides PostgreSQL source connector for DataStream.
package postgres

import (
	"github.com/UFOXD/datastream/internal/source"
)

// Config holds PostgreSQL-specific configuration.
type Config struct {
	// Connection settings
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`

	// Replication settings
	PluginName     string `json:"pluginName"`     // pgoutput, wal2json, decoderbufs
	SlotName       string `json:"slotName"`       // Replication slot name
	CreateSlot     bool   `json:"createSlot"`     // Create slot if not exists
	DropSlotOnStop bool   `json:"dropSlotOnStop"` // Drop slot when stopped

	// Publication settings (for pgoutput)
	PublicationName   string `json:"publicationName"`
	CreatePublication bool   `json:"createPublication"`

	// Schemas and tables
	Schemas []string          `json:"schemas"`
	Tables  map[string]string `json:"tables"` // schema.table -> filter

	// LSN settings
	StartLSN uint64 `json:"startLsn,omitempty"` // Start from specific LSN

	// Snapshot settings
	SnapshotMode    source.SnapshotMode `json:"snapshotMode"`
	SnapshotThreads int                 `json:"snapshotThreads"`

	// Timezone
	Timezone string `json:"timezone"`

	// SSL settings
	SSLMode     string `json:"sslMode"`
	SSLCert     string `json:"sslCert"`
	SSLKey      string `json:"sslKey"`
	SSLRootCert string `json:"sslRootCert"`

	// Connection pool
	MaxConnections int `json:"maxConnections"`
	MaxIdle        int `json:"maxIdle"`
	ConnectTimeout int `json:"connectTimeout"` // seconds

	// Replication settings
	ReplicationTimeout int `json:"replicationTimeout"` // seconds
	HeartbeatInterval  int `json:"heartbeatInterval"`  // seconds
	StatusInterval     int `json:"statusInterval"`     // seconds

	// Offset storage
	OffsetBackend   string `json:"offsetBackend"`
	OffsetPath      string `json:"offsetPath"`
	OffsetFlushMs   int    `json:"offsetFlushMs"`
	OffsetEtcdAddrs string `json:"offsetEtcdAddrs"`
}

// DefaultConfig returns the default PostgreSQL configuration.
func DefaultConfig() *Config {
	return &Config{
		Port:               5432,
		PluginName:         "pgoutput",
		SlotName:           "datastream_slot",
		CreateSlot:         true,
		DropSlotOnStop:     false,
		PublicationName:    "datastream_pub",
		CreatePublication:  true,
		Schemas:            []string{"public"},
		SnapshotMode:       source.SnapshotModeInitial,
		SnapshotThreads:    1,
		Timezone:           "UTC",
		MaxConnections:     10,
		MaxIdle:            5,
		ConnectTimeout:     30,
		ReplicationTimeout: 30,
		HeartbeatInterval:  30,
		StatusInterval:     10,
		OffsetBackend:      "file",
		OffsetFlushMs:      1000,
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
	if c.Database == "" {
		return source.ErrInvalidConfig
	}
	if c.SlotName == "" {
		return source.ErrInvalidConfig
	}
	return nil
}
