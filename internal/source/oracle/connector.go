// Package oracle provides Oracle source connector for DataStream.
package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/UFOXD/datastream/internal/offset"
	"github.com/UFOXD/datastream/internal/schema"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/internal/store"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/parser"
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

	// Sync scope
	syncScope *source.SyncScope

	// In-memory table definitions from SchemaHistory
	tables *schema.Tables

	// Unified task metadata storage (positions, schema history)
	store store.TargetStore

	// Persistent schema history (backed by store)
	history schema.SchemaHistory

	// DDL parser for Oracle
	ddlParser parser.DDLParser
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
	c.syncScope = config.SyncScope
	c.status.State = source.StateInitializing
	c.status.Timestamp = time.Now().Format(time.RFC3339)

	dsn := fmt.Sprintf("oracle://%s:%s@%s/%s",
		cfg.User,
		cfg.Password,
		net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
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

	// Initialize in-memory Tables
	c.tables = schema.NewTables()

	// Determine task ID
	taskID := config.Type
	if taskID == "" {
		taskID = "oracle"
	}
	c.taskID = taskID

	// Initialize unified TargetStore (uses source DB; creates ds_{taskID} schema)
	mysqlStore := store.NewMySQLStore(db, taskID)
	if err := mysqlStore.InitDatabase(ctx); err != nil {
		log.Warn("failed to init target store database, continuing without store",
			zap.String("taskId", taskID),
			zap.Error(err))
	} else {
		c.store = mysqlStore
		c.history = schema.NewTargetStoreSchemaHistory(mysqlStore)

		// Recover Tables from SchemaHistory
		if err := c.history.Recover(ctx, c.tables, c.position); err != nil {
			log.Warn("failed to recover schema history",
				zap.Error(err))
		} else {
			log.Info("recovered tables from schema history",
				zap.Int("count", c.tables.Count()))
		}
	}

	// Initialize DDL parser from registry
	if p := parser.DefaultRegistry.Get("oracle"); p != nil {
		c.ddlParser = p
	} else {
		log.Warn("Oracle DDL parser not found in registry, DDL events will be passed as raw SQL")
	}

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
		zap.String("serviceName", cfg.ServiceName),
		zap.Int("recoveredTables", c.tables.Count()))

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

	// Close schema history
	if c.history != nil {
		c.history.Close()
	}

	// Close target store
	if c.store != nil {
		c.store.Close()
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

// Schemas returns all cached table schemas.
func (c *Connector) Schemas() map[string]*event.TableInfo {
	return make(map[string]*event.TableInfo)
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
	defer c.mu.Unlock()
	if c.syncScope == nil || c.syncScope.Level != source.SyncLevelTable {
		return source.ErrInvalidSyncScope
	}
	for _, t := range tables {
		c.syncScope.Tables.Names = append(c.syncScope.Tables.Names, t)
	}
	return nil
}

// RemoveTables removes tables from sync (table-level only).
func (c *Connector) RemoveTables(ctx context.Context, tables []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.syncScope == nil || c.syncScope.Level != source.SyncLevelTable {
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
		// Handle DDL events: parse, update Tables, save to SchemaHistory
		if ev.Type == event.EventTypeDDL {
			c.handleDDLEvent(ctx, ev)
		}

		// SaveCurrentPosition when event arrives
		if c.store != nil {
			if err := c.store.SaveCurrentPosition(ctx, &ev.Position); err != nil {
				log.Warn("failed to save current position to store",
					zap.Uint64("scn", ev.Position.SCN),
					zap.Error(err))
			}
		}

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

// handleDDLEvent processes a DDL event: parses it, updates Tables, and saves to SchemaHistory.
func (c *Connector) handleDDLEvent(ctx context.Context, ev *event.ChangeEvent) {
	ddl, ok := ev.Metadata["sql"]
	if !ok || ddl == "" {
		return
	}

	// Only process actual DDL statements (not DML that LogMiner may emit as opCode 5)
	if !isDDLStatement(ddl) {
		return
	}

	database := ev.Table.Database
	tableName := ev.Table.Table

	log.Info("DDL event detected",
		zap.String("sql", ddl),
		zap.String("database", database),
		zap.String("table", tableName),
		zap.Uint64("scn", ev.Position.SCN))

	if c.ddlParser == nil {
		log.Warn("no DDL parser available, skipping DDL processing")
		return
	}

	// Get old table info from Tables (nil for new tables)
	var oldTable *event.TableInfo
	if c.tables != nil {
		oldTable = c.tables.Get(database, tableName)
	}

	// Apply DDL via parser
	applyResult, err := c.ddlParser.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		log.Warn("failed to apply DDL",
			zap.String("sql", ddl),
			zap.Error(err))
		return
	}

	// Update in-memory Tables
	if c.tables != nil && applyResult != nil {
		if applyResult.NewTableInfo != nil {
			c.tables.Put(applyResult.NewTableInfo)
			log.Info("updated table in Tables",
				zap.String("database", database),
				zap.String("table", tableName))
		} else if applyResult.Type == parser.DDLTypeDropTable {
			c.tables.Remove(database, tableName)
			log.Info("removed table from Tables",
				zap.String("database", database),
				zap.String("table", tableName))
		}
	}

	// Invalidate schema cache for affected table
	if c.schemaCache != nil {
		if tableName != "" {
			c.schemaCache.Invalidate(database, tableName)
		} else {
			c.schemaCache.InvalidateAll()
		}
	}

	// Save to SchemaHistory via store
	if c.history != nil && applyResult != nil {
		changeType := string(applyResult.Type)
		if applyResult.NewTableInfo != nil {
			// Determine if CREATE or ALTER
			if oldTable != nil {
				changeType = "ALTER"
			} else {
				changeType = "CREATE"
			}
		} else {
			changeType = "DROP"
		}

		histRec := &event.SchemaHistoryRecord{
			Position:   ev.Position,
			Database:   database,
			Table:      tableName,
			DDL:        ddl,
			TableInfo:  applyResult.NewTableInfo,
			ChangeType: changeType,
			DDLStatus:  event.DDLStatusCompleted,
			Timestamp:  ev.Timestamp,
		}
		if applyResult.NewTableInfo != nil {
			histRec.Schema = applyResult.NewTableInfo.Schema
		}

		if err := c.history.Record(ctx, histRec); err != nil {
			log.Warn("failed to save schema history",
				zap.String("sql", ddl),
				zap.Error(err))
		}
	}
}

// isDDLStatement checks if a SQL statement is a DDL statement.
func isDDLStatement(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(upper, "CREATE ") ||
		strings.HasPrefix(upper, "ALTER ") ||
		strings.HasPrefix(upper, "DROP ") ||
		strings.HasPrefix(upper, "TRUNCATE ") ||
		strings.HasPrefix(upper, "RENAME ")
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
