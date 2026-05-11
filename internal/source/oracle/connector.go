// Package oracle provides Oracle source connector for DataStream.
package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/UFOXD/datastream/internal/offset"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/pingcap/log"
	_ "github.com/sijms/go-ora/v2"
	"go.uber.org/zap"
)

// Connector implements the source.Connector interface for Oracle.
// Uses LogMiner with SCN-based position tracking.
type Connector struct {
	config      *Config
	status      source.Status
	position    *event.Position
	events      chan *event.ChangeEvent
	errors      chan error
	stopCh      chan struct{}
	wg          sync.WaitGroup
	mu          sync.RWMutex

	// Database connection
	db *sql.DB

	// LogMiner reader
	logMiner *LogMinerReader

	// Schema cache
	schemaCache *TableSchemaCache

	// Offset storage
	offsetStorage offset.Storage
	taskID        string
}

// New creates a new Oracle source connector.
func New() *Connector {
	return &Connector{
		status: source.Status{
			State:     source.StateUninitialized,
			Timestamp: time.Now().Format(time.RFC3339),
		},
		events: make(chan *event.ChangeEvent, 1000),
		errors: make(chan error, 100),
		stopCh: make(chan struct{}),
	}
}

// Name returns the connector name.
func (c *Connector) Name() string {
	return "oracle"
}

// Initialize initializes the connector.
func (c *Connector) Initialize(ctx context.Context, config source.Config) error {
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
	c.status.State = source.StateInitializing
	c.status.Timestamp = time.Now().Format(time.RFC3339)

	// Build DSN: oracle://user:password@host:port/service_name
	dsn := fmt.Sprintf("oracle://%s:%s@%s:%d/%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.ServiceName,
	)

	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	c.db = db
	c.schemaCache = NewTableSchemaCache(db)

	// Initialize offset storage
	if config.Offset.Backend != "" {
		offsetCfg := &offset.Config{
			Backend:       config.Offset.Backend,
			Path:          config.Offset.Path,
			FlushInterval: config.Offset.FlushInterval,
		}
		storage, err := offset.NewStorage(offsetCfg)
		if err != nil {
			return fmt.Errorf("failed to create offset storage: %w", err)
		}
		c.offsetStorage = storage
		c.taskID = config.Type

		if pos, err := storage.Load(ctx, c.taskID); err != nil {
			log.Warn("failed to load position from offset storage",
				zap.String("taskId", c.taskID),
				zap.Error(err))
		} else if pos != nil {
			c.position = pos
			log.Info("loaded position from offset storage",
				zap.Uint64("scn", pos.SCN))
		}
	}

	// Initialize LogMiner reader
	c.logMiner = NewLogMinerReader(db, c.config, c.schemaCache)

	// Set initial position
	if c.position != nil && c.position.SCN > 0 {
		c.logMiner.SetSCN(c.position.SCN)
	}

	c.status.State = source.StateStopped
	log.Info("Oracle connector initialized",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.String("serviceName", cfg.ServiceName))

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
	c.status.Timestamp = time.Now().Format(time.RFC3339)
	c.mu.Unlock()

	log.Info("starting Oracle connector")

	c.wg.Add(1)
	go c.runPollingLoop(ctx)

	log.Info("Oracle connector started")
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
	c.status.Timestamp = time.Now().Format(time.RFC3339)
	c.mu.Unlock()

	log.Info("stopping Oracle connector")
	close(c.stopCh)
	c.wg.Wait()

	// Save final position to offset storage
	if c.offsetStorage != nil && c.position != nil {
		if err := c.offsetStorage.Save(ctx, c.taskID, c.position); err != nil {
			log.Warn("failed to save final position to offset storage", zap.Error(err))
		}
		c.offsetStorage.Close()
	}

	if c.db != nil {
		c.db.Close()
	}

	log.Info("Oracle connector stopped")
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

	if c.logMiner != nil && pos.SCN > 0 {
		c.logMiner.SetSCN(pos.SCN)
	}

	if c.offsetStorage != nil {
		ctx := context.Background()
		if err := c.offsetStorage.Save(ctx, c.taskID, c.position); err != nil {
			log.Warn("failed to save position to offset storage", zap.Error(err))
		}
	}
	return nil
}

// GetSchema returns the schema for a table.
func (c *Connector) GetSchema(database, table string) (*event.TableInfo, error) {
	return c.schemaCache.Get(context.Background(), database, table)
}

// runPollingLoop polls LogMiner on every PollInterval tick.
func (c *Connector) runPollingLoop(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.pollChanges(ctx)
		}
	}
}

// pollChanges reads changes from LogMiner and emits events.
func (c *Connector) pollChanges(ctx context.Context) {
	changes, lastSCN, err := c.logMiner.ReadChanges(ctx)
	if err != nil {
		log.Warn("LogMiner read error", zap.Error(err))
		select {
		case c.errors <- err:
		default:
		}
		return
	}

	for _, ev := range changes {
		select {
		case c.events <- ev:
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}

	// Update position
	if lastSCN > 0 {
		c.mu.Lock()
		c.position = &event.Position{
			SCN:        lastSCN,
			CommitTime: time.Now(),
		}
		pos := c.position.Clone()
		c.mu.Unlock()

		c.logMiner.UpdatePosition(lastSCN)

		if c.offsetStorage != nil {
			if err := c.offsetStorage.Save(ctx, c.taskID, pos); err != nil {
				log.Warn("failed to save position to offset storage", zap.Error(err))
			}
		}
	}
}

func parseConfig(config source.Config) (*Config, error) {
	cfg := DefaultConfig()

	cfg.Host = config.Connection.Host
	cfg.Port = config.Connection.Port
	cfg.User = config.Connection.User
	cfg.Password = config.Connection.Password

	if v, ok := config.Properties["serviceName"].(string); ok {
		cfg.ServiceName = v
	}
	if v, ok := config.Properties["miningStrategy"].(string); ok {
		cfg.MiningStrategy = v
	}
	if v, ok := config.Properties["batchSize"].(float64); ok {
		cfg.BatchSize = int(v)
	}

	// Collect schemas from table filters
	for _, tf := range config.Tables {
		schema := tf.Schema
		if schema == "" {
			schema = tf.Database
		}
		if schema == "" {
			continue
		}
		found := false
		for _, s := range cfg.Schemas {
			if s == schema {
				found = true
				break
			}
		}
		if !found {
			cfg.Schemas = append(cfg.Schemas, schema)
		}
	}

	return cfg, nil
}

func init() {
	source.Register("oracle", &factory{})
}

type factory struct{}

func (f *factory) Create(config source.Config) (source.Connector, error) {
	return New(), nil
}
