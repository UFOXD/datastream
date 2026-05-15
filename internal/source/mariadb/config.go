// Package mariadb provides a MariaDB source connector for DataStream.
package mariadb

import (
	"fmt"

	"github.com/UFOXD/datastream/internal/source"
)

// Config holds the MariaDB source connector configuration.
type Config struct {
	// Connection settings
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`

	// SSL/TLS settings
	SSLMode string `json:"sslMode"` // disabled, preferred, required, verify_ca, verify_identity

	// Connection pool settings
	MaxConnections int `json:"maxConnections"`
	MaxIdle        int `json:"maxIdle"`
	ConnectTimeout int `json:"connectTimeout"` // seconds

	// Binlog settings
	ServerID   uint32 `json:"serverId"`   // Unique server ID for replication
	BinlogFile string `json:"binlogFile"` // Starting binlog file (optional)
	BinlogPos  uint32 `json:"binlogPos"`  // Starting binlog position (optional)

	// GTID settings (MariaDB-specific)
	UseGTID    bool   `json:"useGTID"`    // Use GTID-based replication
	GTIDDomain uint32 `json:"gtidDomain"` // GTID domain ID (MariaDB-specific)

	// Filter settings
	Databases []string          `json:"databases"` // Databases to capture (empty = all)
	Tables    map[string]string `json:"tables"`    // Database -> table pattern

	// Snapshot settings
	SnapshotMode        source.SnapshotMode `json:"snapshotMode"`
	SnapshotThreads     int                 `json:"snapshotThreads"`
	SnapshotLockTimeout int                 `json:"snapshotLockTimeout"` // seconds

	// Timezone
	Timezone string `json:"timezone"`

	// Schema changes
	IncludeSchemaEvents bool `json:"includeSchemaEvents"`

	// Offset storage settings
	OffsetBackend string `json:"offsetBackend"`
	OffsetPath    string `json:"offsetPath"`
	OffsetFlushMs int    `json:"offsetFlushMs"`
}

// DefaultConfig returns the default MariaDB source configuration.
func DefaultConfig() *Config {
	return &Config{
		Host:                "localhost",
		Port:                3306,
		SSLMode:             "disabled",
		MaxConnections:      10,
		MaxIdle:             2,
		ConnectTimeout:      30,
		ServerID:            1001,
		SnapshotMode:        source.SnapshotModeInitial,
		SnapshotThreads:     1,
		SnapshotLockTimeout: 10,
		Timezone:            "UTC",
		IncludeSchemaEvents: true,
		OffsetFlushMs:       1000,
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("host is required")
	}

	if c.Port <= 0 {
		c.Port = 3306
	}

	if c.ServerID == 0 {
		return fmt.Errorf("serverId is required for binlog replication")
	}

	// Validate SSL mode
	switch c.SSLMode {
	case "disabled", "preferred", "required", "verify_ca", "verify_identity", "":
		// Valid
	default:
		return fmt.Errorf("invalid SSL mode: %s", c.SSLMode)
	}

	return nil
}
