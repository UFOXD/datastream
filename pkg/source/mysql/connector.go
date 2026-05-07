package mysql

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/your-org/datastream/pkg/event"
	"github.com/your-org/datastream/pkg/source"
	"go.uber.org/zap"
)

// Connector implements the source.Connector interface for MySQL.
type Connector struct {
	config     *Config
	status     source.Status
	position   *event.Position
	events     chan *event.ChangeEvent
	errors     chan error
	stopCh     chan struct{}
	wg         sync.WaitGroup
	mu         sync.RWMutex
	schemaCache map[string]*event.TableInfo
}

// New creates a new MySQL source connector.
func New() *Connector {
	return &Connector{
		status: source.Status{
			State:     source.StateUninitialized,
			Timestamp: time.Now().Format(time.RFC3339),
		},
		events:      make(chan *event.ChangeEvent, 1000),
		errors:      make(chan error, 100),
		stopCh:      make(chan struct{}),
		schemaCache: make(map[string]*event.TableInfo),
	}
}

// Name returns the connector name.
func (c *Connector) Name() string {
	return "mysql"
}

// Initialize initializes the connector.
func (c *Connector) Initialize(ctx context.Context, config source.Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Parse MySQL-specific config
	cfg, err := parseConfig(config)
	if err != nil {
		return err
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	c.config = cfg
	c.status.State = source.StateInitializing
	c.status.Timestamp = time.Now().Format(time.RFC3339)

	// Initialize position if provided
	if config.Offset.Path != "" {
		// TODO: Load position from offset storage
		log.Info("loading position from offset storage", zap.String("path", config.Offset.Path))
	}

	c.status.State = source.StateStopped
	log.Info("MySQL connector initialized", zap.String("host", cfg.Host), zap.Int("port", cfg.Port))
	return nil
}

// Start starts the connector.
func (c *Connector) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.status.State == source.StateRunning {
		c.mu.Unlock()
		return source.ErrAlreadyRunning
	}
	c.status.State = source.StateRunning
	c.mu.Unlock()

	log.Info("starting MySQL connector")

	// Start binlog streaming in a goroutine
	c.wg.Add(1)
	go c.run(ctx)

	return nil
}

// Stop stops the connector.
func (c *Connector) Stop(ctx context.Context) error {
	c.mu.Lock()
	if c.status.State != source.StateRunning {
		c.mu.Unlock()
		return nil
	}
	c.status.State = source.StateStopped
	c.mu.Unlock()

	log.Info("stopping MySQL connector")
	close(c.stopCh)
	c.wg.Wait()

	log.Info("MySQL connector stopped")
	return nil
}

// Status returns the current status.
func (c *Connector) Status() source.Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// Events returns the events channel.
func (c *Connector) Events() <-chan *event.ChangeEvent {
	return c.events
}

// Errors returns the errors channel.
func (c *Connector) Errors() <-chan error {
	return c.errors
}

// GetPosition returns the current position.
func (c *Connector) GetPosition() *event.Position {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.position == nil {
		return nil
	}
	return c.position.Clone()
}

// SetPosition sets the starting position.
func (c *Connector) SetPosition(pos *event.Position) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.position = pos.Clone()
	return nil
}

// GetSchema returns the schema for a table.
func (c *Connector) GetSchema(database, table string) (*event.TableInfo, error) {
	c.mu.RLock()
	key := database + "." + table
	if schema, ok := c.schemaCache[key]; ok {
		c.mu.RUnlock()
		return schema.Clone(), nil
	}
	c.mu.RUnlock()

	// TODO: Query schema from MySQL
	return nil, source.ErrSchemaNotFound
}

// run is the main event loop.
func (c *Connector) run(ctx context.Context) {
	defer c.wg.Done()

	// TODO: Implement actual binlog streaming
	// For now, just a skeleton that handles shutdown
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("MySQL connector context done")
			return
		case <-c.stopCh:
			log.Info("MySQL connector stop signal received")
			return
		case <-ticker.C:
			// Send heartbeat
			c.sendHeartbeat(ctx)
		}
	}
}

// sendHeartbeat sends a heartbeat event.
func (c *Connector) sendHeartbeat(ctx context.Context) {
	c.mu.RLock()
	pos := c.position
	c.mu.RUnlock()

	if pos == nil {
		pos = &event.Position{CommitTime: time.Now()}
	}

	hb := event.NewHeartbeat(event.SourceInfo{
		Connector: "mysql",
		Database:  c.config.Databases[0],
	}, *pos)

	select {
	case c.events <- hb.ToChangeEvent():
	case <-ctx.Done():
	case <-c.stopCh:
	}
}

func parseConfig(config source.Config) (*Config, error) {
	cfg := DefaultConfig()

	// Copy connection settings
	cfg.Host = config.Connection.Host
	cfg.Port = config.Connection.Port
	cfg.User = config.Connection.User
	cfg.Password = config.Connection.Password

	// Parse properties
	if v, ok := config.Properties["serverId"].(float64); ok {
		cfg.ServerID = uint32(v)
	}
	if v, ok := config.Properties["binlogFile"].(string); ok {
		cfg.BinlogFile = v
	}
	if v, ok := config.Properties["binlogPos"].(float64); ok {
		cfg.BinlogPos = uint32(v)
	}
	if v, ok := config.Properties["snapshotMode"].(string); ok {
		cfg.SnapshotMode = source.SnapshotMode(v)
	}
	if v, ok := config.Properties["timezone"].(string); ok {
		cfg.Timezone = v
	}
	if v, ok := config.Properties["sslMode"].(string); ok {
		cfg.SSLMode = v
	}

	// Copy databases
	for _, tf := range config.Tables {
		cfg.Databases = append(cfg.Databases, tf.Database)
	}

	// Copy offset settings
	cfg.OffsetBackend = config.Offset.Backend
	cfg.OffsetPath = config.Offset.Path
	cfg.OffsetFlushMs = config.Offset.FlushInterval

	return cfg, nil
}

func init() {
	source.Register("mysql", &factory{})
}

type factory struct{}

func (f *factory) Create(config source.Config) (source.Connector, error) {
	conn := New()
	return conn, nil
}
