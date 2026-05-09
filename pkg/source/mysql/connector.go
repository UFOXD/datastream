package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/offset"
	"github.com/UFOXD/datastream/pkg/source"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Connector implements the source.Connector interface for MySQL.
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

	// Canal for binlog replication
	canal *canal.Canal

	// Database connection for schema queries
	db *sql.DB

	// Current binlog file
	currentBinlog string

	// Offset storage
	offsetStorage offset.Storage
	taskID        string
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

	// Initialize database connection for schema queries
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?parseTime=true&timeout=%ds",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
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

	// Create canal config
	canalCfg := canal.NewDefaultConfig()
	canalCfg.Addr = fmt.Sprintf("%s:%d", c.config.Host, c.config.Port)
	canalCfg.User = c.config.User
	canalCfg.Password = c.config.Password
	canalCfg.ServerID = c.config.ServerID
	canalCfg.Flavor = "mysql"

	// Disable initial dump - we'll handle snapshot separately
	canalCfg.Dump.ExecutionPath = ""

	// Set databases/tables filter
	if len(c.config.Databases) > 0 {
		canalCfg.IncludeTableRegex = make([]string, 0, len(c.config.Databases))
		for _, db := range c.config.Databases {
			canalCfg.IncludeTableRegex = append(canalCfg.IncludeTableRegex,
				fmt.Sprintf("^%s\\..*$", db))
		}
	}

	// Create canal
	canal, err := canal.NewCanal(canalCfg)
	if err != nil {
		c.mu.Lock()
		c.status.State = source.StateError
		c.status.Message = err.Error()
		c.mu.Unlock()
		return fmt.Errorf("failed to create canal: %w", err)
	}

	c.mu.Lock()
	c.canal = canal
	c.mu.Unlock()

	// Set event handler
	handler := NewBinlogHandler(ctx, c)
	canal.SetEventHandler(handler)

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

	// Close canal
	c.mu.RLock()
	canal := c.canal
	c.mu.RUnlock()

	if canal != nil {
		canal.Close()
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
	c.mu.RLock()
	key := database + "." + table
	if schema, ok := c.schemaCache[key]; ok {
		c.mu.RUnlock()
		return schema.Clone(), nil
	}
	c.mu.RUnlock()

	// Query schema from MySQL
	return c.querySchema(database, table)
}

// shouldCapture checks if a table should be captured.
func (c *Connector) shouldCapture(database, table string) bool {
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

// matchPattern performs simple pattern matching.
func matchPattern(pattern, s string) bool {
	if pattern == "*" {
		return true
	}
	// TODO: Implement proper pattern matching with wildcards
	return pattern == s
}

// getTableInfo gets or builds table info from canal table.
func (c *Connector) getTableInfo(table *schema.Table) *event.TableInfo {
	key := table.Schema + "." + table.Name

	c.mu.RLock()
	if info, ok := c.schemaCache[key]; ok {
		c.mu.RUnlock()
		return info
	}
	c.mu.RUnlock()

	// Build table info
	info := &event.TableInfo{
		Database: table.Schema,
		Table:    table.Name,
	}

	// Build column info
	columns := make([]event.ColumnInfo, 0, len(table.Columns))
	keyColumns := make([]string, 0)

	for i, col := range table.Columns {
		columns = append(columns, event.ColumnInfo{
			Name:     col.Name,
			Type:     col.RawType,
			Nullable: true, // Default to nullable
		})

		// Check if this is a primary key column
		if table.IsPrimaryKey(i) {
			keyColumns = append(keyColumns, col.Name)
		}
	}

	info.Columns = columns
	info.PrimaryKeyColumns = keyColumns

	// Cache it
	c.mu.Lock()
	c.schemaCache[key] = info
	c.mu.Unlock()

	return info
}

// currentBinlogFile returns the current binlog file name.
func (c *Connector) currentBinlogFile() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.currentBinlog
}

// run is the main event loop.
func (c *Connector) run(ctx context.Context) {
	defer c.wg.Done()

	// Get canal and starting position
	c.mu.RLock()
	canal := c.canal
	pos := c.position
	c.mu.RUnlock()

	if canal == nil {
		log.Error("canal not initialized")
		c.sendError(fmt.Errorf("canal not initialized"))
		return
	}

	// Run canal
	errCh := make(chan error, 1)
	go func() {
		var err error
		if pos != nil && pos.BinlogFile != "" {
			err = canal.RunFrom(mysql.Position{
				Name: pos.BinlogFile,
				Pos:  pos.BinlogPos,
			})
		} else {
			err = canal.Run()
		}
		errCh <- err
	}()

	// Wait for stop signal or error
	select {
	case <-ctx.Done():
		log.Info("MySQL connector context done")
		canal.Close()
	case <-c.stopCh:
		log.Info("MySQL connector stop signal received")
		canal.Close()
	case err := <-errCh:
		if err != nil {
			log.Error("canal error", zap.Error(err))
			c.sendError(err)
		}
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

// querySchema queries the table schema from MySQL.
func (c *Connector) querySchema(database, table string) (*event.TableInfo, error) {
	c.mu.RLock()
	db := c.db
	c.mu.RUnlock()

	if db == nil {
		return nil, source.ErrNotInitialized
	}

	info := &event.TableInfo{
		Database: database,
		Table:    table,
	}

	// Query columns
	rows, err := db.Query(`
		SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
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
		var colName, dataType, isNullable, columnKey string
		if err := rows.Scan(&colName, &dataType, &isNullable, &columnKey); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		columns = append(columns, event.ColumnInfo{
			Name:     colName,
			Type:     dataType,
			Nullable: isNullable == "YES",
		})

		if columnKey == "PRI" {
			keyColumns = append(keyColumns, colName)
		}
	}

	info.Columns = columns
	info.PrimaryKeyColumns = keyColumns

	// Cache it
	c.mu.Lock()
	c.schemaCache[database+"."+table] = info
	c.mu.Unlock()

	return info, nil
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
