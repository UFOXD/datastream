// Package sqlserver provides SQL Server source connector for DataStream.
package sqlserver

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
	"github.com/UFOXD/datastream/pkg/parser"
	sqlserverparser "github.com/UFOXD/datastream/pkg/parser/sqlserver"
	"github.com/pingcap/log"
	_ "github.com/microsoft/go-mssqldb"
	"go.uber.org/zap"
)

// Connector implements the source.Connector interface for SQL Server.
// Uses CDC (Change Data Capture) with LSN-based position tracking.
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

	// One CDCReader per capture instance
	cdcReaders map[string]*CDCReader

	// Schema cache
	schemaCache *TableSchemaCache

	// In-memory table definitions from SchemaHistory
	tables *schema.Tables

	// DDL parser and record manager
	ddlParser      parser.DDLParser
	ddlManager     *schema.DDLRecordManager
	schemaHistory  schema.SchemaHistory

	// Offset storage
	offsetStorage offset.Storage
	taskID        string

	// Sync scope
	syncScope *source.SyncScope

	// DDL detection state
	lastDDLCheckTime time.Time
}

// New creates a new SQL Server source connector.
func New() *Connector {
	return &Connector{
		status: source.Status{
			State:     source.StateUninitialized,
			Timestamp: time.Now().Format(time.RFC3339),
		},
		events:     make(chan *event.ChangeEvent, 1000),
		errors:     make(chan error, 100),
		stopCh:     make(chan struct{}),
		cdcReaders: make(map[string]*CDCReader),
	}
}

// Name returns the connector name.
func (c *Connector) Name() string {
	return "sqlserver"
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

	dsn := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s",
		cfg.User,
		cfg.Password,
		net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		cfg.Database,
	)

	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	c.db = db
	c.schemaCache = NewTableSchemaCache(db)

	// Initialize in-memory Tables and DDL management
	c.tables = schema.NewTables()
	c.ddlParser = sqlserverparser.NewParser()

	// Initialize local schema history for DDL persistence
	if cfg.DataDir != "" {
		history, err := schema.NewLocalSchemaHistory(cfg.DataDir)
		if err != nil {
			log.Warn("failed to create local schema history, DDL recovery disabled",
				zap.Error(err))
		} else {
			c.schemaHistory = history
			c.ddlManager = schema.NewDDLRecordManager(c.tables, history)
		}
	}

	// Recover tables from schema history (replay DDL records up to saved position)
	if c.schemaHistory != nil {
		if err := c.schemaHistory.Recover(ctx, c.tables, c.position); err != nil {
			log.Warn("failed to recover schema history",
				zap.Error(err))
		} else {
			log.Info("recovered tables from schema history",
				zap.Int("tableCount", c.tables.Count()))
		}
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
		c.taskID = config.Type

		if pos, err := storage.Load(ctx, c.taskID); err != nil {
			log.Warn("failed to load position from offset storage",
				zap.String("taskId", c.taskID),
				zap.Error(err))
		} else if pos != nil {
			c.position = pos
			log.Info("loaded position from offset storage",
				zap.String("changeLsn", pos.ChangeLsn))
		}
	}

	// Discover CDC-enabled capture instances for configured schemas
	if err := c.initCDCReaders(ctx); err != nil {
		return fmt.Errorf("failed to initialize CDC readers: %w", err)
	}

	c.status.State = source.StateStopped
	log.Info("SQL Server connector initialized",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.String("database", cfg.Database))

	return nil
}

// initCDCReaders discovers capture instances and creates CDCReaders.
func (c *Connector) initCDCReaders(ctx context.Context) error {
	rows, err := c.db.QueryContext(ctx, `
		SELECT capture_instance, source_schema, source_name
		FROM cdc.change_tables
		ORDER BY capture_instance
	`)
	if err != nil {
		return fmt.Errorf("failed to query CDC change tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var captureInstance, sourceSchema, sourceName string
		if err := rows.Scan(&captureInstance, &sourceSchema, &sourceName); err != nil {
			return fmt.Errorf("failed to scan CDC table row: %w", err)
		}

		if !c.shouldCapture(sourceSchema, sourceName) {
			continue
		}

		reader := NewCDCReader(c.db, c.config, c.schemaCache, captureInstance)

		// Set initial position from connector position
		if c.position != nil && c.position.ChangeLsn != "" {
			reader.SetPosition(&Position{
				StartLSN:   c.position.ChangeLsn,
				CommitTime: c.position.CommitTime,
			})
		}

		c.cdcReaders[captureInstance] = reader
		log.Info("registered CDC reader",
			zap.String("captureInstance", captureInstance),
			zap.String("schema", sourceSchema),
			zap.String("table", sourceName))
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate CDC change tables: %w", err)
	}

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

	log.Info("starting SQL Server connector")

	c.wg.Add(1)
	go c.runPollingLoop(ctx)

	log.Info("SQL Server connector started")
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

	log.Info("stopping SQL Server connector")
	close(c.stopCh)
	c.wg.Wait()

	// Save final position to offset storage
	if c.offsetStorage != nil && c.position != nil {
		if err := c.offsetStorage.Save(ctx, c.taskID, c.position); err != nil {
			log.Warn("failed to save final position to offset storage", zap.Error(err))
		}
		c.offsetStorage.Close()
	}

	if c.schemaHistory != nil {
		c.schemaHistory.Close()
	}

	if c.db != nil {
		c.db.Close()
	}

	log.Info("SQL Server connector stopped")
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

	// Propagate to all readers
	for _, reader := range c.cdcReaders {
		reader.SetPosition(&Position{
			StartLSN:   pos.ChangeLsn,
			CommitTime: pos.CommitTime,
		})
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

// Schemas returns all cached table schemas from in-memory Tables.
func (c *Connector) Schemas() map[string]*event.TableInfo {
	if c.tables != nil {
		return c.tables.All()
	}
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

// runPollingLoop polls each CDCReader on every PollInterval tick.
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
			c.pollAll(ctx)
		}
	}
}

// pollAll reads changes from every CDC reader and emits events.
func (c *Connector) pollAll(ctx context.Context) {
	// Step 1: Detect and process DDL changes
	c.detectDDLChanges(ctx)

	// Step 2: Read CDC changes (DML)
	var latestLSN string

	for captureInstance, reader := range c.cdcReaders {
		changes, lastLSN, err := reader.ReadChanges(ctx)
		if err != nil {
			log.Warn("CDC read error",
				zap.String("captureInstance", captureInstance),
				zap.Error(err))
			select {
			case c.errors <- err:
			default:
			}
			continue
		}

		// Enrich DML events with table info from Tables (preferred) or schemaCache
		for _, ev := range changes {
			c.enrichDMLTableInfo(ev)
			select {
			case c.events <- ev:
			case <-c.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}

		if lastLSN != "" {
			reader.UpdatePosition(lastLSN)
			// Track the highest LSN seen across all readers
			if latestLSN == "" || lastLSN > latestLSN {
				latestLSN = lastLSN
			}
		}
	}

	// Update connector-level position (ChangeLsn + SeqVal for SQL Server)
	if latestLSN != "" {
		c.mu.Lock()
		c.position = &event.Position{
			ChangeLsn:  latestLSN,
			CommitTime: time.Now(),
		}
		pos := c.position.Clone()
		c.mu.Unlock()

		if c.offsetStorage != nil {
			if err := c.offsetStorage.Save(ctx, c.taskID, pos); err != nil {
				log.Warn("failed to save position to offset storage", zap.Error(err))
			}
		}
	}
}

// enrichDMLTableInfo sets the event's Table field using Tables (preferred)
// or schemaCache (fallback). This ensures DML events carry accurate schema info.
func (c *Connector) enrichDMLTableInfo(ev *event.ChangeEvent) {
	db := ev.Table.Database
	tbl := ev.Table.Table

	// Prefer in-memory Tables (populated from SchemaHistory)
	if c.tables != nil {
		if info := c.tables.Get(db, tbl); info != nil {
			ev.Table = *info
			return
		}
	}

	// Fallback to schemaCache (live database queries)
	if c.schemaCache != nil {
		if info, err := c.schemaCache.Get(context.Background(), db, tbl); err == nil && info != nil {
			ev.Table = *info
		}
	}
}

// detectDDLChanges checks for recent DDL activity and emits DDL events.
func (c *Connector) detectDDLChanges(ctx context.Context) {
	now := time.Now()
	checkTime := c.lastDDLCheckTime
	if checkTime.IsZero() {
		// First check: look back 5 minutes
		checkTime = now.Add(-5 * time.Minute)
	}

	rows, err := c.db.QueryContext(ctx, `
		SELECT s.name AS schema_name, o.name AS table_name, o.modify_date
		FROM sys.objects o
		JOIN sys.schemas s ON o.schema_id = s.schema_id
		WHERE o.type = 'U'
		  AND o.modify_date > @p1
		ORDER BY o.modify_date
	`, checkTime)
	if err != nil {
		log.Warn("failed to detect DDL changes", zap.Error(err))
		c.lastDDLCheckTime = now
		return
	}
	defer rows.Close()

	for rows.Next() {
		var schemaName, tableName string
		var modifyDate time.Time
		if err := rows.Scan(&schemaName, &tableName, &modifyDate); err != nil {
			log.Warn("failed to scan DDL change row", zap.Error(err))
			continue
		}

		captureInstance := schemaName + "_" + tableName

		// Check if we have a CDC reader for this table (means we're tracking it)
		c.mu.RLock()
		reader, tracked := c.cdcReaders[captureInstance]
		c.mu.RUnlock()
		if !tracked {
			continue
		}

		// Get fresh schema from database
		freshInfo, err := c.schemaCache.Get(ctx, c.config.Database, tableName)
		if err != nil {
			log.Warn("failed to get fresh schema for DDL",
				zap.String("table", tableName),
				zap.Error(err))
			continue
		}

		// Invalidate schema cache so next DML gets fresh schema
		c.schemaCache.Invalidate(c.config.Database, tableName)

		// Build DDL event and update Tables
		ddlPos := event.Position{
			CommitTime: modifyDate,
		}
		if c.position != nil {
			ddlPos.ChangeLsn = c.position.ChangeLsn
		}
		if reader.GetPosition() != nil {
			ddlPos.ChangeLsn = reader.GetPosition().StartLSN
		}

		// Apply DDL to update in-memory Tables
		if c.tables != nil {
			// Detect change type
			changeType := "ALTER"
			if c.tables.Get(c.config.Database, tableName) == nil {
				changeType = "CREATE"
			}
			c.tables.Put(freshInfo)

			// Write schema history via DDLRecordManager
			if c.ddlManager != nil {
				recID := fmt.Sprintf("ddl_%s_%s_%d", c.config.Database, tableName, modifyDate.UnixNano())
				ddlRecord := &event.DDLRecord{
					ID:           recID,
					Position:     &ddlPos,
					Database:     c.config.Database,
					Table:        tableName,
					DDL:          fmt.Sprintf("-- %s detected via sys.objects", changeType),
					NewTableInfo: freshInfo,
				}
				if err := c.ddlManager.Create(ctx, ddlRecord); err == nil {
					_ = c.ddlManager.MarkApplying(ctx, recID)
					_ = c.ddlManager.MarkCompleted(ctx, recID)
				}
			}
		}

		// Emit DDL event
		ddlEvent := &event.ChangeEvent{
			ID:   event.GenerateEventID(&event.SourceInfo{Connector: "sqlserver"}, modifyDate, 0),
			Type: event.EventTypeDDL,
			Source: event.SourceInfo{
				Connector: "sqlserver",
				Database:  c.config.Database,
			},
			Table:     *freshInfo,
			Timestamp: modifyDate,
			Position:  ddlPos,
			Metadata: map[string]string{
				"ddl":     fmt.Sprintf("-- %s detected via sys.objects", captureInstance),
				"ddlType": "alter_table",
			},
		}

		select {
		case c.events <- ddlEvent:
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}

	c.lastDDLCheckTime = now
}

// shouldCapture checks if a table should be captured based on configured schemas.
func (c *Connector) shouldCapture(schema, table string) bool {
	if len(c.config.Schemas) == 0 {
		return true
	}

	for _, s := range c.config.Schemas {
		if s == schema || s == "*" {
			// Check table pattern if configured
			if pattern, ok := c.config.Tables[schema]; ok {
				return pattern == "*" || pattern == "" || matchPattern(pattern, table)
			}
			return true
		}
	}
	return false
}

// matchPattern performs simple wildcard pattern matching (* and ?).
func matchPattern(pattern, name string) bool {
	if pattern == "*" || pattern == "" {
		return true
	}
	// Simple prefix/suffix wildcard support
	if len(pattern) > 0 && pattern[0] == '*' {
		suffix := pattern[1:]
		return len(name) >= len(suffix) && name[len(name)-len(suffix):] == suffix
	}
	if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
		prefix := pattern[:len(pattern)-1]
		return len(name) >= len(prefix) && name[:len(prefix)] == prefix
	}
	return pattern == name
}

func parseConfig(config source.Config) (*Config, error) {
	cfg := DefaultConfig()

	cfg.Host = config.Connection.Host
	cfg.Port = config.Connection.Port
	cfg.User = config.Connection.User
	cfg.Password = config.Connection.Password
	cfg.Database = config.Connection.Database

	if v, ok := config.Properties["pollInterval"].(float64); ok {
		cfg.PollInterval = time.Duration(v) * time.Millisecond
	}
	if v, ok := config.Properties["batchSize"].(float64); ok {
		cfg.BatchSize = int(v)
	}
	if v, ok := config.Properties["dataDir"].(string); ok {
		cfg.DataDir = v
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
	source.Register("sqlserver", &factory{})
}

type factory struct{}

func (f *factory) Create(config source.Config) (source.Connector, error) {
	return New(), nil
}
