package kafka

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/sink"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Connector implements the sink.Connector interface for Kafka.
type Connector struct {
	config   *Config
	status   sink.Status
	position *event.Position
	mu       sync.RWMutex
}

// New creates a new Kafka sink connector.
func New() *Connector {
	return &Connector{
		status: sink.Status{
			State:     sink.StateUninitialized,
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
}

// Name returns the connector name.
func (c *Connector) Name() string {
	return "kafka"
}

// Initialize initializes the connector.
func (c *Connector) Initialize(ctx context.Context, config sink.Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cfg, err := parseConfig(config)
	if err != nil {
		return err
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	c.config = cfg
	c.status.State = sink.StateReady
	c.status.Timestamp = time.Now().Format(time.RFC3339)

	log.Info("Kafka sink initialized",
		zap.Strings("brokers", cfg.Brokers),
		zap.String("topic", cfg.Topic))
	return nil
}

// Start starts the connector.
func (c *Connector) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status.State == sink.StateWriting {
		return nil
	}

	c.status.State = sink.StateReady
	log.Info("Kafka sink started")
	return nil
}

// Stop stops the connector.
func (c *Connector) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.status.State = sink.StateStopped
	log.Info("Kafka sink stopped")
	return nil
}

// Status returns the current status.
func (c *Connector) Status() sink.Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// Write writes events to Kafka.
func (c *Connector) Write(ctx context.Context, events []*event.ChangeEvent) error {
	c.mu.Lock()
	c.status.State = sink.StateWriting
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.status.State = sink.StateReady
		c.mu.Unlock()
	}()

	for _, e := range events {
		if err := c.writeEvent(ctx, e); err != nil {
			c.mu.Lock()
			c.status.EventsFailed++
			c.mu.Unlock()
			return err
		}
		c.mu.Lock()
		c.status.EventsWritten++
		c.position = &e.Position
		c.mu.Unlock()
	}

	return nil
}

// Flush flushes any buffered data.
func (c *Connector) Flush(ctx context.Context) error {
	c.mu.Lock()
	c.status.State = sink.StateFlushing
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.status.State = sink.StateReady
		c.mu.Unlock()
	}()

	// TODO: Implement actual Kafka flush
	log.Debug("Kafka sink flushed")
	return nil
}

// GetPosition returns the last committed position.
func (c *Connector) GetPosition() *event.Position {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.position == nil {
		return nil
	}
	return c.position.Clone()
}

// SupportsDDL returns true (Kafka supports DDL events).
func (c *Connector) SupportsDDL() bool {
	return true
}

// SupportsTransaction returns true (Kafka supports transactions).
func (c *Connector) SupportsTransaction() bool {
	return true
}

// writeEvent writes a single event to Kafka.
func (c *Connector) writeEvent(ctx context.Context, e *event.ChangeEvent) error {
	// Serialize event
	data, err := json.Marshal(e)
	if err != nil {
		return sink.ErrWriteFailed
	}

	// TODO: Implement actual Kafka produce
	topic := c.getTopic(e)
	_ = topic
	_ = data

	log.Debug("writing event to Kafka",
		zap.String("event_id", e.ID),
		zap.String("type", string(e.Type)))
	return nil
}

// getTopic returns the topic name for an event.
func (c *Connector) getTopic(e *event.ChangeEvent) string {
	switch c.config.TopicNamingStrategy {
	case "table":
		return c.config.Topic + "." + e.Table.Table
	case "database":
		return c.config.Topic + "." + e.Table.Database
	default:
		return c.config.Topic
	}
}

func parseConfig(config sink.Config) (*Config, error) {
	cfg := DefaultConfig()

	cfg.Brokers = config.Connection.Brokers
	cfg.Topic = config.Connection.Topic

	if v, ok := config.Properties["acks"].(string); ok {
		cfg.Acks = v
	}
	if v, ok := config.Properties["compression"].(string); ok {
		cfg.Compression = v
	}
	if v, ok := config.Properties["keyFormat"].(string); ok {
		cfg.KeyFormat = v
	}
	if v, ok := config.Properties["valueFormat"].(string); ok {
		cfg.ValueFormat = v
	}
	if v, ok := config.Properties["partitionKey"].(string); ok {
		cfg.PartitionKey = v
	}
	if v, ok := config.Properties["securityProtocol"].(string); ok {
		cfg.SecurityProtocol = v
	}
	if v, ok := config.Properties["schemaRegistryUrl"].(string); ok {
		cfg.SchemaRegistryURL = v
	}

	cfg.Batch = config.Batch
	cfg.Retries = config.Retry.MaxRetries
	cfg.RetryBackoff = config.Retry.InitialWait

	return cfg, nil
}

func init() {
	sink.Register("kafka", &factory{})
}

type factory struct{}

func (f *factory) Create(config sink.Config) (sink.Connector, error) {
	return New(), nil
}
