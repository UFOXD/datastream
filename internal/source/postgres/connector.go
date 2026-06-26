package postgres

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
	"github.com/UFOXD/datastream/internal/store"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/parser"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pingcap/log"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

// Connector implements the source.Connector interface for PostgreSQL.
type Connector struct {
	config      *Config
	status      source.Status
	position    *event.Position
	events      chan *event.ChangeEvent
	errors      chan error
	stopCh      chan struct{}
	wg          sync.WaitGroup
	mu          sync.RWMutex
	schemaCache map[string]*event.TableInfo

	// In-memory table definitions from SchemaHistory
	tables *schema.Tables

	// Unified task metadata storage (positions, schema history, etc.)
	store store.TargetStore

	// Schema history persistence
	schemaHistory schema.SchemaHistory

	// DDL parser
	ddlParser parser.DDLParser

	// Replication connection
	pgConn *pgconn.PgConn

	// Database connection for schema queries
	db *sql.DB

	// Current LSN
	currentLSNValue pglogrepl.LSN

	// Handler for pgoutput messages
	handler *PGOutputHandler

	// Offset storage
	offsetStorage offset.Storage
	taskID        string

	// Sync scope
	syncScope *source.SyncScope
}

// New creates a new PostgreSQL source connector.
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
	return "postgres"
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

	// Build connection string
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=%d",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.SSLMode, cfg.ConnectTimeout)

	// Initialize database connection for schema queries
	db, err := sql.Open("postgres", connStr)
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

	// Initialize in-memory Tables
	c.tables = schema.NewTables()

	// Initialize TargetStore (noop until a target DB is configured)
	c.store = store.NewNoopStore()

	// Initialize schema history and recover into Tables
	c.schemaHistory = schema.NewStoreSchemaHistory(c.store)
	if err := c.schemaHistory.Recover(ctx, c.tables, c.position); err != nil {
		log.Warn("failed to recover schema history into Tables",
			zap.Error(err))
	}

	// Get DDL parser from registry
	if p := parser.DefaultRegistry.Get("postgres"); p != nil {
		c.ddlParser = p
	} else {
		log.Warn("PostgreSQL DDL parser not found, DDL events will use relation messages only")
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
		c.taskID = config.Type // Use connector type as task ID if not specified

		// Load position from offset storage
		if pos, err := storage.Load(ctx, c.taskID); err != nil {
			log.Warn("failed to load position from offset storage",
				zap.String("taskId", c.taskID),
				zap.Error(err))
		} else if pos != nil {
			c.position = pos
			c.currentLSNValue = pglogrepl.LSN(pos.LSN)
			log.Info("loaded position from offset storage",
				zap.Uint64("lsn", pos.LSN))
		}
	}

	// Set initial LSN if configured
	if cfg.StartLSN > 0 {
		c.currentLSNValue = pglogrepl.LSN(cfg.StartLSN)
		c.position = &event.Position{
			LSN: cfg.StartLSN,
		}
	}

	c.status.State = source.StateStopped
	log.Info("PostgreSQL connector initialized",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.String("database", cfg.Database),
		zap.String("slotName", cfg.SlotName))

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

	log.Info("starting PostgreSQL connector")

	// Connect to PostgreSQL with replication mode
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s&replication=database",
		c.config.User, c.config.Password,
		net.JoinHostPort(c.config.Host, strconv.Itoa(c.config.Port)),
		c.config.Database, c.config.SSLMode)

	pgConn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		c.mu.Lock()
		c.status.State = source.StateError
		c.status.Message = err.Error()
		c.mu.Unlock()
		return fmt.Errorf("failed to connect with replication mode: %w", err)
	}

	c.mu.Lock()
	c.pgConn = pgConn
	c.mu.Unlock()

	// Identify system to get current LSN if not set
	if c.currentLSNValue == 0 {
		sysInfo, err := pglogrepl.IdentifySystem(ctx, pgConn)
		if err != nil {
			pgConn.Close(ctx)
			return fmt.Errorf("failed to identify system: %w", err)
		}
		c.currentLSNValue = sysInfo.XLogPos
		log.Info("identified system",
			zap.String("systemID", sysInfo.SystemID),
			zap.Uint64("xlogPos", uint64(sysInfo.XLogPos)))
	}

	// Create replication slot if needed
	if c.config.CreateSlot {
		_, err = pglogrepl.CreateReplicationSlot(ctx, pgConn, c.config.SlotName, c.config.PluginName,
			pglogrepl.CreateReplicationSlotOptions{Temporary: false})
		if err != nil {
			// Slot might already exist, which is fine
			log.Warn("replication slot creation", zap.Error(err))
		} else {
			log.Info("created replication slot", zap.String("slotName", c.config.SlotName))
		}
	}

	// Start replication
	var startLSN pglogrepl.LSN
	if c.position != nil && c.position.LSN > 0 {
		startLSN = pglogrepl.LSN(c.position.LSN)
	} else {
		startLSN = c.currentLSNValue
	}

	err = pglogrepl.StartReplication(ctx, pgConn, c.config.SlotName, startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				fmt.Sprintf("proto_version '%d'", 1),
				fmt.Sprintf("publication_names '%s'", c.config.PublicationName),
			},
		})
	if err != nil {
		pgConn.Close(ctx)
		return fmt.Errorf("failed to start replication: %w", err)
	}

	log.Info("started logical replication",
		zap.String("slotName", c.config.SlotName),
		zap.Uint64("startLSN", uint64(startLSN)))

	// Create handler
	c.handler = NewPGOutputHandler(ctx, c)

	// Start streaming in a goroutine
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

	log.Info("stopping PostgreSQL connector")
	close(c.stopCh)

	// Close replication connection
	c.mu.RLock()
	pgConn := c.pgConn
	c.mu.RUnlock()

	if pgConn != nil {
		// Drop replication slot if configured
		if c.config.DropSlotOnStop {
			pglogrepl.DropReplicationSlot(ctx, pgConn, c.config.SlotName,
				pglogrepl.DropReplicationSlotOptions{Wait: true})
		}
		pgConn.Close(ctx)
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

	// Close schema history
	if c.schemaHistory != nil {
		c.schemaHistory.Close()
	}

	// Close store
	if c.store != nil {
		c.store.Close()
	}

	log.Info("PostgreSQL connector stopped")
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
	if pos.LSN > 0 {
		c.currentLSNValue = pglogrepl.LSN(pos.LSN)
	}

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
// Prefers in-memory Tables (from SchemaHistory), falls back to schemaCache.
func (c *Connector) GetSchema(database, table string) (*event.TableInfo, error) {
	// Prefer Tables from SchemaHistory
	if c.tables != nil {
		if info := c.tables.Get(database, table); info != nil {
			return info.Clone(), nil
		}
	}

	// Fallback to schema cache (populated by relation messages)
	c.mu.RLock()
	key := database + "." + table
	if cached, ok := c.schemaCache[key]; ok {
		c.mu.RUnlock()
		return cached.Clone(), nil
	}
	c.mu.RUnlock()

	// Query schema from PostgreSQL
	return c.querySchema(table)
}

// Schemas returns all cached table schemas.
// Returns Tables (from SchemaHistory) merged with schemaCache.
func (c *Connector) Schemas() map[string]*event.TableInfo {
	result := make(map[string]*event.TableInfo)

	// Start with Tables (SchemaHistory has priority)
	if c.tables != nil {
		for k, v := range c.tables.All() {
			result[k] = v
		}
	}

	// Merge in schemaCache for any tables not in Tables
	c.mu.RLock()
	for k, v := range c.schemaCache {
		if _, exists := result[k]; !exists {
			result[k] = v
		}
	}
	c.mu.RUnlock()

	return result
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

// shouldCapture checks if a table should be captured.
func (c *Connector) shouldCapture(schema, table string) bool {
	if len(c.config.Schemas) == 0 {
		return true
	}

	for _, s := range c.config.Schemas {
		if s == schema || s == "*" {
			return true
		}
	}

	// Check table patterns
	for s, pattern := range c.config.Tables {
		if s == schema || s == "*" {
			if pattern == "*" || pattern == "" {
				return true
			}
			if matchPattern(pattern, table) {
				return true
			}
		}
	}

	return false
}

// currentLSN returns the current LSN.
func (c *Connector) currentLSN() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return uint64(c.currentLSNValue)
}

// matchPattern performs simple pattern matching.
func matchPattern(pattern, s string) bool {
	if pattern == "*" {
		return true
	}
	return pattern == s
}

// run is the main event loop.
func (c *Connector) run(ctx context.Context) {
	defer c.wg.Done()

	// Status interval for sending standby status
	statusTicker := time.NewTicker(time.Duration(c.config.StatusInterval) * time.Second)
	defer statusTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("PostgreSQL connector context done")
			return
		case <-c.stopCh:
			log.Info("PostgreSQL connector stop signal received")
			return
		case <-statusTicker.C:
			c.sendStandbyStatus(ctx)
		default:
			// Receive next message
			msg, err := c.receiveMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Error("failed to receive message", zap.Error(err))
				c.sendError(err)
				continue
			}

			if msg == nil {
				continue
			}

			// Handle the message
			if err := c.handler.HandleMessage(msg); err != nil {
				log.Error("failed to handle message", zap.Error(err))
				c.sendError(err)
			}
		}
	}
}

// receiveMessage receives the next logical replication message.
func (c *Connector) receiveMessage(ctx context.Context) (pglogrepl.Message, error) {
	c.mu.RLock()
	pgConn := c.pgConn
	c.mu.RUnlock()

	if pgConn == nil {
		return nil, fmt.Errorf("not connected")
	}

	// Receive message from the replication stream
	msg, err := pgConn.ReceiveMessage(ctx)
	if err != nil {
		return nil, err
	}

	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		// Parse the copy data as a logical replication message
		switch msg.Data[0] {
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return nil, fmt.Errorf("failed to parse XLogData: %w", err)
			}

			// Update current LSN
			c.mu.Lock()
			c.currentLSNValue = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			c.mu.Unlock()

			// Parse the logical replication message
			return pglogrepl.Parse(xld.WALData)
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return nil, fmt.Errorf("failed to parse keepalive: %w", err)
			}

			// Update current LSN from keepalive
			c.mu.Lock()
			if pkm.ServerWALEnd > c.currentLSNValue {
				c.currentLSNValue = pkm.ServerWALEnd
			}
			c.mu.Unlock()

			// Send standby status if requested
			if pkm.ReplyRequested {
				c.sendStandbyStatus(ctx)
			}

			return nil, nil
		}
	}

	return nil, nil
}

// sendStandbyStatus sends a standby status update.
func (c *Connector) sendStandbyStatus(ctx context.Context) {
	c.mu.RLock()
	pgConn := c.pgConn
	lsn := c.currentLSNValue
	c.mu.RUnlock()

	if pgConn == nil {
		return
	}

	ssu := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
	}

	if err := pglogrepl.SendStandbyStatusUpdate(ctx, pgConn, ssu); err != nil {
		log.Warn("failed to send standby status", zap.Error(err))
	}
}

// sendError sends an error to the errors channel.
func (c *Connector) sendError(err error) {
	select {
	case c.errors <- err:
	case <-time.After(time.Second * 5):
		log.Warn("failed to send error, channel full", zap.Error(err))
	}
}

// querySchema queries the table schema from PostgreSQL.
func (c *Connector) querySchema(table string) (*event.TableInfo, error) {
	c.mu.RLock()
	db := c.db
	c.mu.RUnlock()

	if db == nil {
		return nil, source.ErrNotInitialized
	}

	info := &event.TableInfo{
		Database: c.config.Database,
		Table:    table,
	}

	// Query columns
	rows, err := db.Query(`
		SELECT column_name, data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position
	`, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	columns := make([]event.ColumnInfo, 0)
	for rows.Next() {
		var colName, dataType, isNullable string
		var columnDefault sql.NullString
		if err := rows.Scan(&colName, &dataType, &isNullable, &columnDefault); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		columns = append(columns, event.ColumnInfo{
			Name:     colName,
			Type:     dataType,
			Nullable: isNullable == "YES",
		})
	}

	info.Columns = columns

	// Query primary key columns
	pkRows, err := db.Query(`
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
		WHERE tc.constraint_type = 'PRIMARY KEY'
			AND tc.table_schema = 'public'
			AND tc.table_name = $1
		ORDER BY kcu.ordinal_position
	`, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary key: %w", err)
	}
	defer pkRows.Close()

	keyColumns := make([]string, 0)
	for pkRows.Next() {
		var colName string
		if err := pkRows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %w", err)
		}
		keyColumns = append(keyColumns, colName)
	}

	info.PrimaryKeyColumns = keyColumns

	// Cache it
	c.mu.Lock()
	c.schemaCache["public."+table] = info
	c.mu.Unlock()

	return info, nil
}

func parseConfig(config source.Config) (*Config, error) {
	cfg := DefaultConfig()

	cfg.Host = config.Connection.Host
	cfg.Port = config.Connection.Port
	cfg.User = config.Connection.User
	cfg.Password = config.Connection.Password
	cfg.Database = config.Connection.Database

	if v, ok := config.Properties["pluginName"].(string); ok {
		cfg.PluginName = v
	}
	if v, ok := config.Properties["slotName"].(string); ok {
		cfg.SlotName = v
	}
	if v, ok := config.Properties["publicationName"].(string); ok {
		cfg.PublicationName = v
	}
	if v, ok := config.Properties["startLsn"].(float64); ok {
		cfg.StartLSN = uint64(v)
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
	if v, ok := config.Properties["createSlot"].(bool); ok {
		cfg.CreateSlot = v
	}
	if v, ok := config.Properties["dropSlotOnStop"].(bool); ok {
		cfg.DropSlotOnStop = v
	}

	for _, tf := range config.Tables {
		cfg.Schemas = append(cfg.Schemas, tf.Schema)
	}

	cfg.OffsetBackend = config.Offset.Backend
	cfg.OffsetPath = config.Offset.Path
	cfg.OffsetFlushMs = config.Offset.FlushInterval

	return cfg, nil
}

func init() {
	source.Register("postgres", &factory{})
}

type factory struct{}

func (f *factory) Create(config source.Config) (source.Connector, error) {
	conn := New()
	return conn, nil
}
