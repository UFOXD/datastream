// Package redis provides a Redis sink connector for DataStream.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/UFOXD/datastream/internal/sink"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// RedisCommand represents a Redis write command to be executed.
type RedisCommand struct {
	Op  string // "hset", "set", "del"
	Key string
	// For hset: field→value pairs
	Fields map[string]interface{}
	// For set: the serialized value
	Value string
	// TTL to apply after the write (0 = no expiration)
	TTL time.Duration
}

// PipelineWriter builds Redis commands from change events.
type PipelineWriter struct {
	config *Config
}

// generateKey renders the key pattern for the given table and row.
// Pattern placeholders: {database}, {table}, {pk}.
// Multiple PK columns are joined with "_".
func (w *PipelineWriter) generateKey(table event.TableInfo, row event.RowData, pkColumns []string) string {
	var pkParts []string
	for _, col := range pkColumns {
		if f, ok := row.Fields[col]; ok {
			pkParts = append(pkParts, fmt.Sprintf("%v", f.Value))
		}
	}
	pk := strings.Join(pkParts, "_")

	key := w.config.KeyPattern
	key = strings.ReplaceAll(key, "{database}", table.Database)
	key = strings.ReplaceAll(key, "{table}", table.Table)
	key = strings.ReplaceAll(key, "{pk}", pk)
	return key
}

// buildCommand converts a single ChangeEvent to a RedisCommand.
// Returns nil for event types that do not produce writes (e.g. DDL, heartbeat).
func (w *PipelineWriter) buildCommand(e *event.ChangeEvent) *RedisCommand {
	pkColumns := e.Table.PrimaryKeyColumns

	switch e.Type {
	case event.EventTypeDelete:
		key := w.generateKey(e.Table, e.Before, pkColumns)
		return &RedisCommand{Op: "del", Key: key}

	case event.EventTypeInsert, event.EventTypeUpdate:
		key := w.generateKey(e.Table, e.After, pkColumns)
		switch w.config.Format {
		case "hash":
			fields := make(map[string]interface{}, len(e.After.Fields))
			for name, f := range e.After.Fields {
				fields[name] = f.Value
			}
			return &RedisCommand{
				Op:     "hset",
				Key:    key,
				Fields: fields,
				TTL:    w.config.TTL,
			}
		case "json":
			data := make(map[string]interface{}, len(e.After.Fields))
			for name, f := range e.After.Fields {
				data[name] = f.Value
			}
			b, _ := json.Marshal(data)
			return &RedisCommand{
				Op:    "set",
				Key:   key,
				Value: string(b),
				TTL:   w.config.TTL,
			}
		case "string":
			data := make(map[string]interface{}, len(e.After.Fields))
			for name, f := range e.After.Fields {
				data[name] = f.Value
			}
			b, _ := json.Marshal(data)
			return &RedisCommand{
				Op:    "set",
				Key:   key,
				Value: string(b),
				TTL:   w.config.TTL,
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

// Connector implements sink.Connector for Redis.
type Connector struct {
	config   *Config
	status   sink.Status
	position *event.Position
	client   *goredis.Client
	writer   *PipelineWriter
	mu       sync.RWMutex
}

// New creates a new Redis sink connector.
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
	return "redis"
}

// Initialize initializes the connector with the given configuration.
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

	client := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return fmt.Errorf("failed to connect to Redis at %s: %w", cfg.Addr, err)
	}

	c.config = cfg
	c.client = client
	c.writer = &PipelineWriter{config: cfg}
	c.status.State = sink.StateReady
	c.status.Timestamp = time.Now().Format(time.RFC3339)

	log.Info("Redis sink initialized",
		zap.String("addr", cfg.Addr),
		zap.String("format", cfg.Format))
	return nil
}

// Start starts the connector.
func (c *Connector) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil {
		return sink.ErrNotInitialized
	}
	c.status.State = sink.StateReady
	c.status.Timestamp = time.Now().Format(time.RFC3339)
	log.Info("Redis sink started")
	return nil
}

// Stop stops the connector gracefully.
func (c *Connector) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		_ = c.client.Close()
	}
	c.status.State = sink.StateStopped
	c.status.Timestamp = time.Now().Format(time.RFC3339)
	log.Info("Redis sink stopped")
	return nil
}

// Status returns the current status.
func (c *Connector) Status() sink.Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// Write writes change events to Redis using pipelining.
func (c *Connector) Write(ctx context.Context, events []*event.ChangeEvent) error {
	c.mu.Lock()
	c.status.State = sink.StateWriting
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.status.State = sink.StateReady
		c.status.Timestamp = time.Now().Format(time.RFC3339)
		c.mu.Unlock()
	}()

	var cmds []*RedisCommand
	for _, e := range events {
		cmd := c.writer.buildCommand(e)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}

	if len(cmds) > 0 {
		if err := c.executePipeline(ctx, cmds); err != nil {
			c.mu.Lock()
			c.status.EventsFailed += int64(len(cmds))
			c.mu.Unlock()
			return err
		}
	}

	c.mu.Lock()
	c.status.EventsWritten += int64(len(events))
	if len(events) > 0 {
		pos := events[len(events)-1].Position
		c.position = &pos
	}
	c.mu.Unlock()

	return nil
}

// executePipeline sends commands via Redis pipeline.
func (c *Connector) executePipeline(ctx context.Context, cmds []*RedisCommand) error {
	pipe := c.client.Pipeline()
	for _, cmd := range cmds {
		switch cmd.Op {
		case "del":
			pipe.Del(ctx, cmd.Key)
		case "hset":
			args := make([]interface{}, 0, len(cmd.Fields)*2)
			for k, v := range cmd.Fields {
				args = append(args, k, v)
			}
			pipe.HSet(ctx, cmd.Key, args...)
			if cmd.TTL > 0 {
				pipe.Expire(ctx, cmd.Key, cmd.TTL)
			}
		case "set":
			if cmd.TTL > 0 {
				pipe.Set(ctx, cmd.Key, cmd.Value, cmd.TTL)
			} else {
				pipe.Set(ctx, cmd.Key, cmd.Value, 0)
			}
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Flush is a no-op for Redis (writes are synchronous).
func (c *Connector) Flush(ctx context.Context) error {
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

// SupportsDDL returns false — Redis has no schema.
func (c *Connector) SupportsDDL() bool { return false }

// SupportsTransaction returns false — Redis uses pipelining.
func (c *Connector) SupportsTransaction() bool { return false }

// ---------------------------------------------------------------------------
// parseConfig
// ---------------------------------------------------------------------------

func parseConfig(config sink.Config) (*Config, error) {
	cfg := DefaultConfig()

	if config.Connection.Addr != "" {
		cfg.Addr = config.Connection.Addr
	}
	if config.Connection.RedisPassword != "" {
		cfg.Password = config.Connection.RedisPassword
	}
	if config.Connection.RedisDB != 0 {
		cfg.DB = config.Connection.RedisDB
	}
	if v, ok := config.Properties["keyPattern"].(string); ok {
		cfg.KeyPattern = v
	}
	if v, ok := config.Properties["format"].(string); ok {
		cfg.Format = v
	}
	if v, ok := config.Properties["batchSize"].(float64); ok {
		cfg.BatchSize = int(v)
	}
	if v, ok := config.Properties["flushInterval"].(float64); ok {
		cfg.FlushInterval = time.Duration(v) * time.Millisecond
	}
	if v, ok := config.Properties["ttl"].(float64); ok {
		cfg.TTL = time.Duration(v) * time.Millisecond
	}
	return cfg, nil
}

// ---------------------------------------------------------------------------
// Factory / registry
// ---------------------------------------------------------------------------

func init() {
	sink.Register("redis", &factory{})
}

type factory struct{}

func (f *factory) Create(config sink.Config) (sink.Connector, error) {
	return New(), nil
}
