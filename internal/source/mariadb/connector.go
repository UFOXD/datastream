package mariadb

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/UFOXD/datastream/internal/offset"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/parser"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Connector implements the source.Connector interface for MariaDB.
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

	// Database connection for schema queries
	db *sql.DB

	// Current binlog file
	currentBinlog string

	// GTID support
	useGTID bool

	// Offset storage
	offsetStorage offset.Storage
	taskID        string

	// Sync scope
	syncScope *source.SyncScope
}

// New creates a new MariaDB source connector.
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
	return "mariadb"
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

	// Initialize database connection for schema queries
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?parseTime=true&timeout=%ds",
		cfg.User,
		cfg.Password,
		net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		cfg.ConnectTimeout,
	)

	if cfg.SSLMode != "" && cfg.SSLMode != "disabled" {
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
	c.useGTID = cfg.UseGTID

	// Initialize schema cache with database connection
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
	log.Info("MariaDB connector initialized",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.Uint32("serverId", cfg.ServerID),
		zap.Bool("useGTID", cfg.UseGTID))

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

	log.Info("starting MariaDB connector")

	// Create binlog syncer (replaces canal.Canal)
	c.syncer = NewBinlogSyncer(c.config, c.schemaCache, c.events, c.errors)

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

	log.Info("MariaDB connector started")
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

	log.Info("stopping MariaDB connector")
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

	log.Info("MariaDB connector stopped")
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

	cfg.Host = config.Connection.Host
	cfg.Port = config.Connection.Port
	cfg.User = config.Connection.User
	cfg.Password = config.Connection.Password

	if v, ok := config.Properties["serverId"].(float64); ok {
		cfg.ServerID = uint32(v)
	}
	if v, ok := config.Properties["binlogFile"].(string); ok {
		cfg.BinlogFile = v
	}
	if v, ok := config.Properties["binlogPos"].(float64); ok {
		cfg.BinlogPos = uint32(v)
	}
	if v, ok := config.Properties["useGTID"].(bool); ok {
		cfg.UseGTID = v
	}
	if v, ok := config.Properties["gtidDomain"].(float64); ok {
		cfg.GTIDDomain = uint32(v)
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
	source.Register("mariadb", &factory{})
}

type factory struct{}

func (f *factory) Create(config source.Config) (source.Connector, error) {
	return New(), nil
}

// ============================================================
// TableSchemaCache - Independent schema management
// ============================================================

// TableSchemaCache caches table schema information.
type TableSchemaCache struct {
	mu      sync.RWMutex
	schemas map[string]*event.TableInfo // key: database.table
	db      *sql.DB
}

// NewTableSchemaCache creates a new schema cache.
func NewTableSchemaCache(db *sql.DB) *TableSchemaCache {
	return &TableSchemaCache{
		schemas: make(map[string]*event.TableInfo),
		db:      db,
	}
}

// Get retrieves schema from cache or queries the database.
func (c *TableSchemaCache) Get(ctx context.Context, database, table string) (*event.TableInfo, error) {
	key := database + "." + table

	// Check cache first
	c.mu.RLock()
	if schema, ok := c.schemas[key]; ok {
		c.mu.RUnlock()
		return schema.Clone(), nil
	}
	c.mu.RUnlock()

	// Query from database
	schema, err := c.querySchema(ctx, database, table)
	if err != nil {
		return nil, err
	}

	// Cache it
	c.mu.Lock()
	c.schemas[key] = schema
	c.mu.Unlock()

	return schema.Clone(), nil
}

// Update updates the cached schema for a table.
func (c *TableSchemaCache) Update(database, table string, schema *event.TableInfo) {
	key := database + "." + table
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas[key] = schema
}

// Invalidate removes a cached schema.
func (c *TableSchemaCache) Invalidate(database, table string) {
	key := database + "." + table
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.schemas, key)
}

// InvalidateAll clears all cached schemas.
func (c *TableSchemaCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas = make(map[string]*event.TableInfo)
}

// querySchema queries the table schema from INFORMATION_SCHEMA.
func (c *TableSchemaCache) querySchema(ctx context.Context, database, table string) (*event.TableInfo, error) {
	info := &event.TableInfo{
		Database: database,
		Table:    table,
	}

	// Query columns
	rows, err := c.db.QueryContext(ctx, `
		SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_TYPE
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`, database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	columns := make([]event.ColumnInfo, 0)
	keyColumns := make([]string, 0)

	for rows.Next() {
		var colName, dataType, isNullable, columnKey, columnType string
		if err := rows.Scan(&colName, &dataType, &isNullable, &columnKey, &columnType); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		columns = append(columns, event.ColumnInfo{
			Name:     colName,
			Type:     columnType,
			Nullable: isNullable == "YES",
		})

		if columnKey == "PRI" {
			keyColumns = append(keyColumns, colName)
		}
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s.%s not found", database, table)
	}

	info.Columns = columns
	info.PrimaryKeyColumns = keyColumns

	return info, nil
}

// ============================================================
// BinlogSyncer - Direct replication package usage
// ============================================================

// BinlogSyncer wraps the replication.BinlogSyncer and handles binlog streaming.
type BinlogSyncer struct {
	config      *Config
	syncer      *replication.BinlogSyncer
	streamer    *replication.BinlogStreamer
	parser      parser.DDLParser
	schemaCache *TableSchemaCache

	events chan *event.ChangeEvent
	errors chan error

	position    *event.Position
	positionMu  sync.RWMutex
	currentFile string

	tableColumnTypes map[uint64][]byte
	tableColumnMetas map[uint64][]uint16
	tableMu          sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewBinlogSyncer creates a new binlog syncer.
func NewBinlogSyncer(config *Config, schemaCache *TableSchemaCache, events chan *event.ChangeEvent, errors chan error) *BinlogSyncer {
	return &BinlogSyncer{
		config:           config,
		schemaCache:      schemaCache,
		events:           events,
		errors:           errors,
		tableColumnTypes: make(map[uint64][]byte),
		tableColumnMetas: make(map[uint64][]uint16),
	}
}

// Start starts the binlog syncer.
func (s *BinlogSyncer) Start(ctx context.Context, startPos *event.Position) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Create binlog syncer config - use "mariadb" flavor for MariaDB-specific features
	syncerCfg := replication.BinlogSyncerConfig{
		ServerID:        s.config.ServerID,
		Flavor:          "mariadb",
		Host:            s.config.Host,
		Port:            uint16(s.config.Port),
		User:            s.config.User,
		Password:        s.config.Password,
		Charset:         "utf8mb4",
		RawModeEnabled:  false,
		SemiSyncEnabled: false,
		ParseTime:       true,
	}

	s.syncer = replication.NewBinlogSyncer(syncerCfg)

	// Get DDL parser from registry (MySQL parser works for MariaDB DDL)
	if p := parser.DefaultRegistry.Get("mysql"); p != nil {
		s.parser = p
	} else {
		log.Warn("DDL parser not found, DDL events will be passed as raw SQL")
	}

	// Start streaming from position
	var err error
	if startPos != nil && startPos.BinlogFile != "" {
		pos := mysql.Position{
			Name: startPos.BinlogFile,
			Pos:  startPos.BinlogPos,
		}
		s.streamer, err = s.syncer.StartSync(pos)
		s.currentFile = startPos.BinlogFile
		s.position = startPos.Clone()
		log.Info("starting MariaDB binlog sync from position",
			zap.String("file", startPos.BinlogFile),
			zap.Uint32("pos", startPos.BinlogPos))
	} else {
		s.streamer, err = s.syncer.StartSync(mysql.Position{})
		log.Info("starting MariaDB binlog sync from latest position")
	}

	if err != nil {
		return fmt.Errorf("failed to start binlog syncer: %w", err)
	}

	s.wg.Add(1)
	go s.run()

	return nil
}

// Stop stops the binlog syncer.
func (s *BinlogSyncer) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	if s.syncer != nil {
		s.syncer.Close()
	}
	return nil
}

// GetPosition returns the current position.
func (s *BinlogSyncer) GetPosition() *event.Position {
	s.positionMu.RLock()
	defer s.positionMu.RUnlock()
	if s.position == nil {
		return nil
	}
	return s.position.Clone()
}

// run is the main event processing loop.
func (s *BinlogSyncer) run() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			ev, err := s.streamer.GetEvent(s.ctx)
			if err != nil {
				if s.ctx.Err() != nil {
					return
				}
				log.Error("failed to get binlog event", zap.Error(err))
				select {
				case s.errors <- err:
				case <-time.After(5 * time.Second):
					log.Warn("error channel full, dropping error", zap.Error(err))
				}
				continue
			}

			if err := s.processEvent(ev); err != nil {
				log.Error("failed to process binlog event", zap.Error(err))
				select {
				case s.errors <- err:
				case <-time.After(5 * time.Second):
					log.Warn("error channel full, dropping error", zap.Error(err))
				}
			}
		}
	}
}

// processEvent processes a single binlog event.
func (s *BinlogSyncer) processEvent(ev *replication.BinlogEvent) error {
	switch ev.Event.(type) {
	case *replication.RotateEvent:
		return s.handleRotateEvent(ev)
	case *replication.QueryEvent:
		return s.handleQueryEvent(ev)
	case *replication.TableMapEvent:
		return s.handleTableMapEvent(ev)
	case *replication.RowsEvent:
		return s.handleRowsEvent(ev)
	case *replication.XIDEvent:
		return s.handleXIDEvent(ev)
	case *replication.GTIDEvent:
		return s.handleGTIDEvent(ev)
	}
	return nil
}

func (s *BinlogSyncer) handleRotateEvent(ev *replication.BinlogEvent) error {
	rotateEvent, ok := ev.Event.(*replication.RotateEvent)
	if !ok {
		return nil
	}
	s.positionMu.Lock()
	s.currentFile = string(rotateEvent.NextLogName)
	s.positionMu.Unlock()
	log.Info("binlog rotation",
		zap.String("file", string(rotateEvent.NextLogName)),
		zap.Uint64("position", rotateEvent.Position))
	return nil
}

func (s *BinlogSyncer) handleTableMapEvent(ev *replication.BinlogEvent) error {
	tableMapEvent, ok := ev.Event.(*replication.TableMapEvent)
	if !ok {
		return nil
	}
	s.tableMu.Lock()
	s.tableColumnTypes[tableMapEvent.TableID] = tableMapEvent.ColumnType
	s.tableColumnMetas[tableMapEvent.TableID] = tableMapEvent.ColumnMeta
	s.tableMu.Unlock()
	return nil
}

func (s *BinlogSyncer) handleQueryEvent(ev *replication.BinlogEvent) error {
	queryEvent, ok := ev.Event.(*replication.QueryEvent)
	if !ok {
		return nil
	}

	query := string(queryEvent.Query)
	if !isDDL(query) {
		return nil
	}

	database := string(queryEvent.Schema)
	log.Info("DDL event received",
		zap.String("query", query),
		zap.String("database", database))

	var ddlResult *parser.DDLResult
	if s.parser != nil {
		results, err := s.parser.Parse(s.ctx, query)
		if err != nil {
			log.Warn("failed to parse DDL", zap.String("query", query), zap.Error(err))
		} else if len(results) > 0 {
			ddlResult = results[0]
		}
	}

	changeEvent := &event.ChangeEvent{
		ID:   event.GenerateEventID(&event.SourceInfo{Connector: "mariadb"}, time.Now(), int(ev.Header.LogPos)),
		Type: event.EventTypeDDL,
		Source: event.SourceInfo{
			Connector: "mariadb",
			Database:  database,
		},
		Timestamp: time.Unix(int64(ev.Header.Timestamp), 0),
		Position: event.Position{
			BinlogFile: s.currentFile,
			BinlogPos:  ev.Header.LogPos,
			CommitTime: time.Now(),
		},
	}

	if ddlResult != nil {
		changeEvent.Table = event.TableInfo{
			Database: ddlResult.Database,
			Table:    ddlResult.Table,
		}
		changeEvent.Metadata = map[string]string{
			"ddl":          query,
			"ddlType":      string(ddlResult.Type),
			"ddlDatabase":  ddlResult.Database,
			"ddlTable":     ddlResult.Table,
			"ddlStatement": ddlResult.Statement,
		}
	} else {
		changeEvent.Metadata = map[string]string{"ddl": query}
	}

	if ddlResult != nil && ddlResult.Table != "" {
		s.schemaCache.Invalidate(ddlResult.Database, ddlResult.Table)
	} else {
		s.schemaCache.InvalidateAll()
	}

	s.positionMu.Lock()
	s.position = &changeEvent.Position
	s.positionMu.Unlock()

	select {
	case s.events <- changeEvent:
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout sending DDL event")
	}

	return nil
}

func (s *BinlogSyncer) handleRowsEvent(ev *replication.BinlogEvent) error {
	rowsEvent, ok := ev.Event.(*replication.RowsEvent)
	if !ok {
		return nil
	}

	database := string(rowsEvent.Table.Schema)
	table := string(rowsEvent.Table.Table)

	if !s.shouldCapture(database, table) {
		return nil
	}

	tableInfo, err := s.schemaCache.Get(s.ctx, database, table)
	if err != nil {
		log.Warn("failed to get table schema", zap.String("database", database), zap.String("table", table), zap.Error(err))
		tableInfo = &event.TableInfo{Database: database, Table: table}
	}

	var eventType event.EventType
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		eventType = event.EventTypeInsert
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		eventType = event.EventTypeUpdate
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		eventType = event.EventTypeDelete
	default:
		return nil
	}

	events := s.buildChangeEvents(eventType, ev.Header, tableInfo, rowsEvent)

	for _, changeEvent := range events {
		s.positionMu.Lock()
		s.position = &changeEvent.Position
		s.positionMu.Unlock()

		select {
		case s.events <- changeEvent:
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout sending change event")
		}
	}

	return nil
}

func (s *BinlogSyncer) handleXIDEvent(ev *replication.BinlogEvent) error {
	s.positionMu.Lock()
	s.position = &event.Position{
		BinlogFile: s.currentFile,
		BinlogPos:  ev.Header.LogPos,
		CommitTime: time.Now(),
	}
	s.positionMu.Unlock()
	return nil
}

func (s *BinlogSyncer) handleGTIDEvent(ev *replication.BinlogEvent) error {
	return nil
}

func (s *BinlogSyncer) shouldCapture(database, table string) bool {
	if len(s.config.Databases) == 0 {
		return true
	}
	for _, db := range s.config.Databases {
		if db == database || db == "*" {
			return true
		}
	}
	for db, pattern := range s.config.Tables {
		if db == database || db == "*" {
			if pattern == "*" || pattern == "" || matchPattern(pattern, table) {
				return true
			}
		}
	}
	return false
}

func (s *BinlogSyncer) buildChangeEvents(eventType event.EventType, header *replication.EventHeader, tableInfo *event.TableInfo, rowsEvent *replication.RowsEvent) []*event.ChangeEvent {
	var events []*event.ChangeEvent

	switch eventType {
	case event.EventTypeInsert:
		for _, row := range rowsEvent.Rows {
			afterData := s.buildRowData(tableInfo.Columns, row)
			events = append(events, &event.ChangeEvent{
				ID:        event.GenerateEventID(&event.SourceInfo{Connector: "mariadb"}, time.Now(), int(header.LogPos)),
				Type:      eventType,
				Source:    event.SourceInfo{Connector: "mariadb", Database: tableInfo.Database},
				Table:     *tableInfo,
				Timestamp: time.Unix(int64(header.Timestamp), 0),
				After:     afterData,
				Position:  event.Position{BinlogFile: s.currentFile, BinlogPos: header.LogPos, CommitTime: time.Now()},
			})
		}
	case event.EventTypeUpdate:
		for i := 0; i < len(rowsEvent.Rows); i += 2 {
			if i+1 >= len(rowsEvent.Rows) {
				break
			}
			beforeData := s.buildRowData(tableInfo.Columns, rowsEvent.Rows[i])
			afterData := s.buildRowData(tableInfo.Columns, rowsEvent.Rows[i+1])
			events = append(events, &event.ChangeEvent{
				ID:        event.GenerateEventID(&event.SourceInfo{Connector: "mariadb"}, time.Now(), int(header.LogPos)),
				Type:      eventType,
				Source:    event.SourceInfo{Connector: "mariadb", Database: tableInfo.Database},
				Table:     *tableInfo,
				Timestamp: time.Unix(int64(header.Timestamp), 0),
				Before:    beforeData,
				After:     afterData,
				Position:  event.Position{BinlogFile: s.currentFile, BinlogPos: header.LogPos, CommitTime: time.Now()},
			})
		}
	case event.EventTypeDelete:
		for _, row := range rowsEvent.Rows {
			beforeData := s.buildRowData(tableInfo.Columns, row)
			events = append(events, &event.ChangeEvent{
				ID:        event.GenerateEventID(&event.SourceInfo{Connector: "mariadb"}, time.Now(), int(header.LogPos)),
				Type:      eventType,
				Source:    event.SourceInfo{Connector: "mariadb", Database: tableInfo.Database},
				Table:     *tableInfo,
				Timestamp: time.Unix(int64(header.Timestamp), 0),
				Before:    beforeData,
				Position:  event.Position{BinlogFile: s.currentFile, BinlogPos: header.LogPos, CommitTime: time.Now()},
			})
		}
	}
	return events
}

func (s *BinlogSyncer) buildRowData(columns []event.ColumnInfo, values []interface{}) event.RowData {
	fields := make(map[string]event.Field)
	for i, col := range columns {
		if i >= len(values) {
			break
		}
		fields[col.Name] = event.Field{
			Name:  col.Name,
			Value: values[i],
			Type:  col.Type,
		}
	}
	return event.RowData{Fields: fields}
}

func isDDL(query string) bool {
	upper := query[:min(20, len(query))]
	for i := 0; i < len(upper); i++ {
		if upper[i] != ' ' && upper[i] != '\t' && upper[i] != '\n' {
			upper = upper[i:]
			break
		}
	}
	return hasPrefix(upper, "CREATE ") || hasPrefix(upper, "ALTER ") || hasPrefix(upper, "DROP ") || hasPrefix(upper, "TRUNCATE ") || hasPrefix(upper, "RENAME ")
}

func hasPrefix(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return s[:len(prefix)] == prefix
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func matchPattern(pattern, s string) bool {
	if pattern == "*" {
		return true
	}
	return pattern == s
}
