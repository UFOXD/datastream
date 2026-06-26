package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/UFOXD/datastream/internal/offset"
	"github.com/UFOXD/datastream/internal/schema"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Connector implements the source.Connector interface for MySQL.
// Uses replication.BinlogSyncer directly instead of canal.Canal.
type Connector struct {
	config      *Config
	status      source.Status
	position    *event.Position
	events      chan *event.ChangeEvent
	errors      chan error
	stopCh      chan struct{}
	wg          sync.WaitGroup
	mu          sync.RWMutex

	// Binlog syncer (replaces canal.Canal)
	syncer *BinlogSyncer

	// Schema cache for independent schema management
	schemaCache *TableSchemaCache

	// In-memory table definitions from SchemaHistory
	tables *schema.Tables

	// Database connection for schema queries
	db *sql.DB

	// Current binlog file
	currentBinlog string

	// Offset storage
	offsetStorage offset.Storage
	taskID        string

	// Sync scope
	syncScope *source.SyncScope
}

// New creates a new MySQL source connector.
func New() *Connector {
	return &Connector{
		status: source.Status{
			State:     source.StateUninitialized,
			Timestamp: time.Now().Format(time.RFC3339),
		},
		events:    make(chan *event.ChangeEvent, 1000),
		errors:    make(chan error, 100),
		stopCh:    make(chan struct{}),
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
	c.syncScope = config.SyncScope
	c.status.State = source.StateInitializing
	c.status.Timestamp = time.Now().Format(time.RFC3339)

	// Initialize database connection for schema queries
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?parseTime=true&timeout=%ds",
		cfg.User,
		cfg.Password,
		net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		cfg.ConnectTimeout,
	)

	if cfg.SSLMode != "" {
		dsn += fmt.Sprintf("&tls=%s", cfg.SSLMode)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxConnections)
	db.SetMaxIdleConns(cfg.MaxIdle)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	c.db = db

	// Initialize schema cache with database connection
	c.schemaCache = NewTableSchemaCache(db)

	// Initialize in-memory Tables
	c.tables = schema.NewTables()

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
		c.taskID = config.Type // Use connector type as task ID if not specified

		// Load position from offset storage
		if pos, err := storage.Load(ctx, c.taskID); err != nil {
			log.Warn("failed to load position from offset storage",
				zap.String("taskId", c.taskID),
				zap.Error(err))
		} else if pos != nil {
			c.position = pos
			c.currentBinlog = pos.BinlogFile
			log.Info("loaded position from offset storage",
				zap.String("binlogFile", pos.BinlogFile),
				zap.Uint32("binlogPos", pos.BinlogPos))
		}
	}

	// Set initial binlog file if configured
	if cfg.BinlogFile != "" {
		c.currentBinlog = cfg.BinlogFile
		c.position = &event.Position{
			BinlogFile: cfg.BinlogFile,
			BinlogPos:  cfg.BinlogPos,
		}
	}

	c.status.State = source.StateStopped
	log.Info("MySQL connector initialized",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.Uint32("serverId", cfg.ServerID))

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

	// Create binlog syncer (replaces canal.Canal)
	c.syncer = NewBinlogSyncer(c.config, c.syncScope, c.schemaCache, c.tables, c.events, c.errors)

	// Start the syncer
	if err := c.syncer.Start(ctx, c.position); err != nil {
		c.mu.Lock()
		c.status.State = source.StateError
		c.status.Message = err.Error()
		c.mu.Unlock()
		return fmt.Errorf("failed to start binlog syncer: %w", err)
	}

	// Start position saver goroutine
	c.wg.Add(1)
	go c.runPositionSaver(ctx)

	log.Info("MySQL connector started")
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

	// Stop the syncer
	if c.syncer != nil {
		c.syncer.Stop()
	}

	c.wg.Wait()

	// Save final position to offset storage
	if c.offsetStorage != nil && c.position != nil {
		if err := c.offsetStorage.Save(ctx, c.taskID, c.position); err != nil {
			log.Warn("failed to save final position to offset storage", zap.Error(err))
		}
	}

	// Close offset storage
	if c.offsetStorage != nil {
		c.offsetStorage.Close()
	}

	// Close database connection
	if c.db != nil {
		c.db.Close()
	}

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

	// Save to offset storage
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

// Schemas returns all cached table schemas.
// The returned map is a copy; mutations do not affect the internal cache.
// Keys are in "database.table" format.
func (c *Connector) Schemas() map[string]*event.TableInfo {
	return c.schemaCache.All()
}

// SyncScope returns the current sync scope.
func (c *Connector) SyncScope() *source.SyncScope {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.syncScope
}

// AddTables adds tables to sync (table-level only).
func (c *Connector) AddTables(ctx context.Context, tables []string) error {
	c.mu.Lock()
	if c.syncScope == nil || c.syncScope.Level != source.SyncLevelTable {
		c.mu.Unlock()
		return source.ErrInvalidSyncScope
	}
	for _, t := range tables {
		c.syncScope.Tables.Names = append(c.syncScope.Tables.Names, t)
	}
	// Capture updated scope while still holding the lock, then propagate.
	updated := c.syncScope
	syncer := c.syncer
	c.mu.Unlock()

	if syncer != nil {
		syncer.UpdateSyncScope(updated)
	}
	return nil
}

// RemoveTables removes tables from sync (table-level only).
func (c *Connector) RemoveTables(ctx context.Context, tables []string) error {
	c.mu.Lock()
	if c.syncScope == nil || c.syncScope.Level != source.SyncLevelTable {
		c.mu.Unlock()
		return source.ErrInvalidSyncScope
	}
	remove := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		remove[t] = struct{}{}
	}
	names := c.syncScope.Tables.Names[:0]
	for _, n := range c.syncScope.Tables.Names {
		if _, ok := remove[n]; !ok {
			names = append(names, n)
		}
	}
	c.syncScope.Tables.Names = names
	// Capture updated scope while still holding the lock, then propagate.
	updated := c.syncScope
	syncer := c.syncer
	c.mu.Unlock()

	if syncer != nil {
		syncer.UpdateSyncScope(updated)
	}
	return nil
}

// ListTables returns all tables being synced.
func (c *Connector) ListTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.syncScope == nil || c.syncScope.Level != source.SyncLevelTable {
		return nil
	}
	result := make([]string, len(c.syncScope.Tables.Names))
	copy(result, c.syncScope.Tables.Names)
	return result
}

// shouldCapture checks if a table should be captured.
// It prefers SyncScope when set, falling back to legacy config.Databases/Tables.
func (c *Connector) shouldCapture(database, table string) bool {
	// Use SyncScope when available
	if c.syncScope != nil {
		switch c.syncScope.Level {
		case source.SyncLevelDatabase:
			return c.syncScope.Databases.ShouldSyncTable(database, table)
		case source.SyncLevelTable:
			return c.syncScope.Tables.ShouldSyncTable(database, table)
		}
	}

	// Fallback: legacy config.Databases / config.Tables
	if len(c.config.Databases) == 0 {
		return true
	}

	for _, db := range c.config.Databases {
		if db == database || db == "*" {
			return true
		}
	}

	// Check table patterns
	for db, pattern := range c.config.Tables {
		if db == database || db == "*" {
			if pattern == "*" || pattern == "" {
				return true
			}
			// Simple pattern matching
			if matchPattern(pattern, table) {
				return true
			}
		}
	}

	return false
}

// runPositionSaver periodically saves position to offset storage.
func (c *Connector) runPositionSaver(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			if c.offsetStorage != nil && c.syncer != nil {
				pos := c.syncer.GetPosition()
				if pos != nil {
					c.mu.Lock()
					c.position = pos
					c.mu.Unlock()
					if err := c.offsetStorage.Save(ctx, c.taskID, pos); err != nil {
						log.Warn("failed to save position to offset storage", zap.Error(err))
					}
				}
			}
		}
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
	if v, ok := config.Properties["includeSchemaEvents"].(bool); ok {
		cfg.IncludeSchemaEvents = v
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
