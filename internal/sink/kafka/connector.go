package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/internal/sink"
	"github.com/pingcap/log"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.uber.org/zap"
)

// Connector implements the sink.Connector interface for Kafka.
type Connector struct {
	config   *Config
	status   sink.Status
	position *event.Position
	writer   *kafka.Writer
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

	// Create Kafka writer
	c.writer = &kafka.Writer{
		Addr:          kafka.TCP(cfg.Brokers...),
		Topic:         cfg.Topic,
		Balancer:      &kafka.LeastBytes{},
		MaxAttempts:   cfg.Retries + 1,
		BatchSize:     cfg.BatchSize,
		BatchTimeout:  time.Duration(cfg.BatchTimeout) * time.Millisecond,
		Compression:   getCompression(cfg.Compression),
		RequiredAcks:  getRequiredAcks(cfg.Acks),
		Async:         false,
	}

	c.status.State = sink.StateReady
	c.status.Timestamp = time.Now().Format(time.RFC3339)

	log.Info("Kafka sink initialized",
		zap.Strings("brokers", cfg.Brokers),
		zap.String("topic", cfg.Topic),
		zap.String("compression", cfg.Compression))
	return nil
}

// Start starts the connector.
func (c *Connector) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status.State == sink.StateWriting {
		return nil
	}

	if c.writer == nil {
		return sink.ErrNotInitialized
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

// Close closes the Kafka writer.
func (c *Connector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.writer != nil {
		return c.writer.Close()
	}
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

	// Build Kafka messages
	messages := make([]kafka.Message, 0, len(events))

	for _, e := range events {
		msg, err := c.buildMessage(e)
		if err != nil {
			c.mu.Lock()
			c.status.EventsFailed++
			c.mu.Unlock()
			return err
		}
		messages = append(messages, *msg)
	}

	// Write messages to Kafka
	c.mu.RLock()
	writer := c.writer
	c.mu.RUnlock()

	if writer == nil {
		return sink.ErrNotInitialized
	}

	if err := writer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("failed to write messages to Kafka: %w", err)
	}

	// Update status and position
	c.mu.Lock()
	c.status.EventsWritten += int64(len(events))
	if len(events) > 0 {
		c.position = &events[len(events)-1].Position
	}
	c.mu.Unlock()

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

	// The kafka-go writer is synchronous by default, so flush is a no-op
	// But we can wait for any pending messages by doing an empty write
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

// buildMessage builds a Kafka message from an event.
func (c *Connector) buildMessage(e *event.ChangeEvent) (*kafka.Message, error) {
	// Get topic for this event
	topic := c.getTopic(e)

	// Build key
	key, err := c.buildKey(e)
	if err != nil {
		return nil, err
	}

	// Build value
	value, err := c.buildValue(e)
	if err != nil {
		return nil, err
	}

	// Build headers
	headers := []kafka.Header{
		{Key: "event_type", Value: []byte(e.Type)},
		{Key: "source", Value: []byte(e.Source.Connector)},
		{Key: "database", Value: []byte(e.Table.Database)},
		{Key: "table", Value: []byte(e.Table.Table)},
		{Key: "timestamp", Value: []byte(e.Timestamp.Format(time.RFC3339Nano))},
	}

	// Add position info
	if e.Position.BinlogFile != "" {
		headers = append(headers, kafka.Header{
			Key: "binlog_file", Value: []byte(e.Position.BinlogFile),
		})
	}

	return &kafka.Message{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: headers,
		Time:    e.Timestamp,
	}, nil
}

// buildKey builds the message key from an event.
func (c *Connector) buildKey(e *event.ChangeEvent) ([]byte, error) {
	// Get the partition key value
	keyValue := ""
	if c.config.PartitionKey != "" {
		if val, ok := e.After.Get(c.config.PartitionKey); ok {
			keyValue = fmt.Sprintf("%v", val)
		} else if val, ok := e.Before.Get(c.config.PartitionKey); ok {
			keyValue = fmt.Sprintf("%v", val)
		}
	}

	// Build composite key if no partition key found
	if keyValue == "" {
		// Use database.table as key for consistent partitioning
		keyValue = fmt.Sprintf("%s.%s", e.Table.Database, e.Table.Table)
	}

	// Format based on key format
	switch c.config.KeyFormat {
	case "json":
		keyData := map[string]interface{}{
			"database": e.Table.Database,
			"table":    e.Table.Table,
			"key":      keyValue,
		}
		return json.Marshal(keyData)
	default:
		return []byte(keyValue), nil
	}
}

// buildValue builds the message value from an event.
func (c *Connector) buildValue(e *event.ChangeEvent) ([]byte, error) {
	switch c.config.ValueFormat {
	case "json":
		return json.Marshal(e)
	default:
		return json.Marshal(e)
	}
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
	if v, ok := config.Properties["batchSize"].(int); ok {
		cfg.BatchSize = v
	}
	if v, ok := config.Properties["batchTimeout"].(int); ok {
		cfg.BatchTimeout = v
	}
	if v, ok := config.Properties["maxMessageBytes"].(int); ok {
		cfg.MaxMessageBytes = v
	}

	cfg.Batch = config.Batch
	cfg.Retries = config.Retry.MaxRetries
	cfg.RetryBackoff = config.Retry.InitialWait

	return cfg, nil
}

// getCompression returns the compression codec for the configured compression type.
func getCompression(compressionType string) compress.Compression {
	switch compressionType {
	case "gzip":
		return compress.Gzip
	case "snappy":
		return compress.Snappy
	case "lz4":
		return compress.Lz4
	case "zstd":
		return compress.Zstd
	default:
		return compress.None
	}
}

// getRequiredAcks returns the required acks for the configured acks type.
func getRequiredAcks(acks string) kafka.RequiredAcks {
	switch acks {
	case "none":
		return kafka.RequireNone
	case "leader":
		return kafka.RequireOne
	case "all":
		return kafka.RequireAll
	default:
		return kafka.RequireAll
	}
}

func init() {
	sink.Register("kafka", &factory{})
}

type factory struct{}

func (f *factory) Create(config sink.Config) (sink.Connector, error) {
	return New(), nil
}
