// Package sink defines the sink connector interfaces for DataStream.
package sink

import (
	"context"

	"github.com/your-org/datastream/pkg/event"
)

// Connector defines the interface for a sink connector.
type Connector interface {
	// Name returns the connector name.
	Name() string

	// Initialize initializes the connector with the given configuration.
	Initialize(ctx context.Context, config Config) error

	// Start starts the connector.
	Start(ctx context.Context) error

	// Stop stops the connector gracefully.
	Stop(ctx context.Context) error

	// Status returns the current status of the connector.
	Status() Status

	// Write writes events to the sink.
	Write(ctx context.Context, events []*event.ChangeEvent) error

	// Flush flushes any buffered data.
	Flush(ctx context.Context) error

	// GetPosition returns the last committed position.
	GetPosition() *event.Position

	// SupportsDDL returns true if the sink supports DDL events.
	SupportsDDL() bool

	// SupportsTransaction returns true if the sink supports transactions.
	SupportsTransaction() bool
}

// BatchConnector is an optional interface for sinks that support batch writes.
type BatchConnector interface {
	Connector

	// WriteBatch writes events in batches with the given batch size.
	WriteBatch(ctx context.Context, events []*event.ChangeEvent, batchSize int) error
}

// AsyncConnector is an optional interface for sinks that support async writes.
type AsyncConnector interface {
	Connector

	// WriteAsync writes events asynchronously and returns immediately.
	WriteAsync(ctx context.Context, events []*event.ChangeEvent) error

	// Acknowledgments returns the channel for receiving write acknowledgments.
	Acknowledgments() <-chan *Ack
}

// Ack represents an acknowledgment for an async write.
type Ack struct {
	Position *event.Position
	Success  bool
	Error    error
}

// Config is the configuration for a sink connector.
type Config struct {
	// Connector type (kafka, mysql, postgres, mongodb, redis, elasticsearch, etc.)
	Type string `json:"type"`

	// Connection configuration
	Connection ConnectionConfig `json:"connection"`

	// Sink-specific configuration
	Properties map[string]interface{} `json:"properties"`

	// Batch configuration
	Batch BatchConfig `json:"batch"`

	// Retry configuration
	Retry RetryConfig `json:"retry"`
}

// ConnectionConfig holds sink connection configuration.
type ConnectionConfig struct {
	// For database sinks
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	User     string `json:"user,omitempty"`
	Password string `json:"password,omitempty"`
	Database string `json:"database,omitempty"`

	// For Kafka sinks
	Brokers []string `json:"brokers,omitempty"`
	Topic   string   `json:"topic,omitempty"`

	// For Redis sinks
	Addr           string `json:"addr,omitempty"`
	RedisPassword  string `json:"redisPassword,omitempty"`
	RedisDB        int    `json:"redisDb,omitempty"`

	// For Elasticsearch sinks
	URLs []string `json:"urls,omitempty"`
	Index string  `json:"index,omitempty"`

	// SSL/TLS configuration
	SSLMode     string `json:"sslMode,omitempty"`
	SSLCert     string `json:"sslCert,omitempty"`
	SSLKey      string `json:"sslKey,omitempty"`
	SSLRootCert string `json:"sslRootCert,omitempty"`

	// Connection pool settings
	MaxConnections int `json:"maxConnections,omitempty"`
	ConnectTimeout int `json:"connectTimeout,omitempty"`
}

// BatchConfig configures batching behavior.
type BatchConfig struct {
	Size      int `json:"size"`      // Number of events per batch
	Timeout   int `json:"timeout"`   // Batch timeout in milliseconds
	Retries   int `json:"retries"`   // Number of retries on failure
	RetryWait int `json:"retryWait"` // Wait between retries in milliseconds
}

// RetryConfig configures retry behavior.
type RetryConfig struct {
	MaxRetries   int `json:"maxRetries"`   // Maximum number of retries
	InitialWait  int `json:"initialWait"`  // Initial wait time in milliseconds
	MaxWait      int `json:"maxWait"`      // Maximum wait time in milliseconds
	Multiplier   float64 `json:"multiplier"` // Backoff multiplier
}

// Status represents the status of a sink connector.
type Status struct {
	State     State  `json:"state"`
	Message   string `json:"message,omitempty"`
	Timestamp string `json:"timestamp"`

	// Statistics
	EventsWritten int64 `json:"eventsWritten"`
	EventsFailed  int64 `json:"eventsFailed"`
	BytesWritten  int64 `json:"bytesWritten"`
}

// State represents the sink state.
type State string

const (
	StateUninitialized State = "uninitialized"
	StateReady         State = "ready"
	StateWriting       State = "writing"
	StateFlushing      State = "flushing"
	StateError         State = "error"
	StateStopped       State = "stopped"
)

// Factory creates sink connectors.
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
