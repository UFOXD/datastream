// Package source defines the source connector interfaces for DataStream.
package source

import (
	"context"

	"github.com/UFOXD/datastream/pkg/event"
)

// Connector defines the interface for a source connector.
type Connector interface {
	// Name returns the connector name.
	Name() string

	// Initialize initializes the connector with the given configuration.
	Initialize(ctx context.Context, config Config) error

	// Start starts the connector and begins emitting events.
	Start(ctx context.Context) error

	// Stop stops the connector gracefully.
	Stop(ctx context.Context) error

	// Status returns the current status of the connector.
	Status() Status

	// Events returns the channel for receiving change events.
	Events() <-chan *event.ChangeEvent

	// Errors returns the channel for receiving errors.
	Errors() <-chan error

	// GetPosition returns the current position.
	GetPosition() *event.Position

	// SetPosition sets the position to start from (for recovery).
	SetPosition(pos *event.Position) error

	// GetSchema returns the schema for a table.
	GetSchema(database, table string) (*event.TableInfo, error)
}

// Config is the configuration for a source connector.
type Config struct {
	// Connector type (mysql, postgres, mongodb, oracle, sqlserver, mariadb)
	Type string `json:"type"`

	// Connection configuration
	Connection ConnectionConfig `json:"connection"`

	// Source-specific configuration
	Properties map[string]interface{} `json:"properties"`

	// Tables to capture (empty means all tables)
	Tables []TableFilter `json:"tables"`

	// Snapshot configuration
	Snapshot SnapshotConfig `json:"snapshot"`

	// Offset storage configuration
	Offset OffsetConfig `json:"offset"`
}

// ConnectionConfig holds database connection configuration.
type ConnectionConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`

	// SSL/TLS configuration
	SSLMode     string `json:"sslMode,omitempty"`
	SSLCert     string `json:"sslCert,omitempty"`
	SSLKey      string `json:"sslKey,omitempty"`
	SSLRootCert string `json:"sslRootCert,omitempty"`

	// Connection pool settings
	MaxConnections int `json:"maxConnections,omitempty"`
	MaxIdle        int `json:"maxIdle,omitempty"`
	ConnectTimeout int `json:"connectTimeout,omitempty"` // seconds
}

// TableFilter specifies which tables to capture.
type TableFilter struct {
	Database string   `json:"database"`
	Schema   string   `json:"schema,omitempty"`
	Tables   []string `json:"tables"` // Empty means all tables
}

// SnapshotConfig configures the initial snapshot behavior.
type SnapshotConfig struct {
	Mode    SnapshotMode `json:"mode"`    // never, initial, always
	Threads int          `json:"threads"` // Parallel snapshot threads
}

// SnapshotMode defines when to take a snapshot.
type SnapshotMode string

const (
	SnapshotModeNever   SnapshotMode = "never"   // Never take snapshot
	SnapshotModeInitial SnapshotMode = "initial" // Snapshot on first start
	SnapshotModeAlways  SnapshotMode = "always"  // Always snapshot on start
)

// OffsetConfig configures offset storage.
type OffsetConfig struct {
	// Storage backend: memory, file, etcd
	Backend string `json:"backend"`

	// File path for file backend
	Path string `json:"path,omitempty"`

	// Auto-flush interval in milliseconds
	FlushInterval int `json:"flushInterval,omitempty"`
}

// Status represents the status of a connector.
type Status struct {
	State     State  `json:"state"`
	Message   string `json:"message,omitempty"`
	Timestamp string `json:"timestamp"`
}

// State represents the connector state.
type State string

const (
	StateUninitialized State = "uninitialized"
	StateInitializing  State = "initializing"
	StateRunning       State = "running"
	StatePaused        State = "paused"
	StateStopped       State = "stopped"
	StateError         State = "error"
)

// Factory creates source connectors.
type Factory interface {
	Create(config Config) (Connector, error)
}

// Registry holds registered connector factories.
var registry = make(map[string]Factory)

// Register registers a connector factory.
func Register(name string, factory Factory) {
	registry[name] = factory
}

// Create creates a connector by name.
func Create(name string, config Config) (Connector, error) {
	factory, ok := registry[name]
	if !ok {
		return nil, ErrUnsupportedConnector
	}
	return factory.Create(config)
}
