package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/internal/sink"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Connector implements the sink.Connector interface for MySQL.
type Connector struct {
	config   *Config
	status   sink.Status
	position *event.Position
	db       *sql.DB
	tx       *sql.Tx
	mu       sync.RWMutex
}

// New creates a new MySQL sink connector.
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
	return "mysql"
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

	// Build DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&timeout=%ds",
		cfg.User,
		cfg.Password,
		net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		cfg.Database,
		cfg.ConnectTimeout,
	)

	// Add SSL parameters if configured
	if cfg.SSLMode != "" {
		dsn += fmt.Sprintf("&tls=%s", cfg.SSLMode)
	}

	// Connect to database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(cfg.MaxConnections)
	db.SetMaxIdleConns(cfg.MaxIdle)
	db.SetConnMaxLifetime(time.Hour)

	// Verify connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	c.db = db
	c.status.State = sink.StateReady
	c.status.Timestamp = time.Now().Format(time.RFC3339)

	log.Info("MySQL sink initialized",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.String("database", cfg.Database))
	return nil
}

// Start starts the connector.
func (c *Connector) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status.State == sink.StateWriting {
		return nil
	}

	if c.db == nil {
		return sink.ErrNotInitialized
	}

	c.status.State = sink.StateReady
	log.Info("MySQL sink started")
	return nil
}

// Stop stops the connector.
func (c *Connector) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Rollback any pending transaction
	if c.tx != nil {
		c.tx.Rollback()
		c.tx = nil
	}

	c.status.State = sink.StateStopped
	log.Info("MySQL sink stopped")
	return nil
}

// Close closes the database connection.
func (c *Connector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Status returns the current status.
func (c *Connector) Status() sink.Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// Write writes events to MySQL.
func (c *Connector) Write(ctx context.Context, events []*event.ChangeEvent) error {
	c.mu.Lock()
	c.status.State = sink.StateWriting
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.status.State = sink.StateReady
		c.mu.Unlock()
	}()

	// Start transaction if configured
	if c.config.UseTransaction {
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		c.mu.Lock()
		c.tx = tx
		c.mu.Unlock()

		defer func() {
			c.mu.Lock()
			c.tx = nil
			c.mu.Unlock()
		}()
	}

	for _, e := range events {
		if e.IsDDL() {
			if err := c.handleDDL(ctx, e); err != nil {
				c.mu.Lock()
				c.status.EventsFailed++
				c.mu.Unlock()
				return err
			}
		} else if e.IsDataEvent() {
			if err := c.writeDataEvent(ctx, e); err != nil {
				c.mu.Lock()
				c.status.EventsFailed++
				c.mu.Unlock()
				return err
			}
		}

		c.mu.Lock()
		c.status.EventsWritten++
		c.position = &e.Position
		c.mu.Unlock()
	}

	// Commit transaction if configured
	if c.config.UseTransaction {
		c.mu.RLock()
		tx := c.tx
		c.mu.RUnlock()

		if tx != nil {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}
		}
	}

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

	// Commit any pending transaction
	c.mu.RLock()
	tx := c.tx
	c.mu.RUnlock()

	if tx != nil {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to flush transaction: %w", err)
		}

		// Start new transaction
		newTx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin new transaction: %w", err)
		}
		c.mu.Lock()
		c.tx = newTx
		c.mu.Unlock()
	}

	log.Debug("MySQL sink flushed")
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

// SupportsDDL returns true (MySQL supports DDL events).
func (c *Connector) SupportsDDL() bool {
	return true
}

// ApplyDDL applies a DDL event to MySQL.
func (c *Connector) ApplyDDL(ctx context.Context, ddl *event.ChangeEvent) error {
	if ddl == nil || ddl.Metadata == nil {
		return nil
	}
	sql, ok := ddl.Metadata["sql"]
	if !ok || sql == "" {
		return nil
	}
	log.Info("applying DDL", zap.String("sql", sql))
	_, err := c.db.ExecContext(ctx, sql)
	return err
}

// SupportsTransaction returns true (MySQL supports transactions).
func (c *Connector) SupportsTransaction() bool {
	return c.config.UseTransaction
}

// getExecutor returns the appropriate executor (transaction or database).
func (c *Connector) getExecutor() executor {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.tx != nil {
		return c.tx
	}
	return c.db
}

type executor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// handleDDL handles DDL events.
func (c *Connector) handleDDL(ctx context.Context, e *event.ChangeEvent) error {
	switch c.config.DDLPolicy {
	case "ignore":
		return nil
	case "error":
		return sink.ErrDDLNotSupported
	}

	ddlStatement := e.Metadata["ddl"]
	if ddlStatement == "" {
		return nil
	}

	exec := c.getExecutor()
	_, err := exec.ExecContext(ctx, ddlStatement)
	if err != nil {
		return fmt.Errorf("failed to execute DDL: %w", err)
	}

	log.Info("executed DDL",
		zap.String("statement", ddlStatement))
	return nil
}

// writeDataEvent writes a data event to MySQL.
func (c *Connector) writeDataEvent(ctx context.Context, e *event.ChangeEvent) error {
	switch e.Type {
	case event.EventTypeInsert:
		return c.executeInsert(ctx, e)
	case event.EventTypeUpdate:
		return c.executeUpdate(ctx, e)
	case event.EventTypeDelete:
		return c.executeDelete(ctx, e)
	}
	return nil
}

// executeInsert executes an INSERT statement.
func (c *Connector) executeInsert(ctx context.Context, e *event.ChangeEvent) error {
	table := fmt.Sprintf("`%s`.`%s`", e.Table.Database, e.Table.Table)
	columns := e.After.ColumnNames()
	if len(columns) == 0 {
		return fmt.Errorf("no columns to insert")
	}

	placeholders := make([]string, len(columns))
	args := make([]interface{}, len(columns))

	for i, col := range columns {
		placeholders[i] = "?"
		field, _ := e.After.GetField(col)
		args[i] = field.Value
	}

	var query string
	switch c.config.InsertStrategy {
	case "replace":
		query = fmt.Sprintf("REPLACE INTO %s (`%s`) VALUES (%s)",
			table, strings.Join(columns, "`, `"), strings.Join(placeholders, ", "))
	case "upsert":
		// Build ON DUPLICATE KEY UPDATE clause
		updateParts := make([]string, len(columns))
		for i, col := range columns {
			updateParts[i] = fmt.Sprintf("`%s` = VALUES(`%s`)", col, col)
		}
		query = fmt.Sprintf("INSERT INTO %s (`%s`) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
			table, strings.Join(columns, "`, `"), strings.Join(placeholders, ", "), strings.Join(updateParts, ", "))
	default: // "insert"
		query = fmt.Sprintf("INSERT INTO %s (`%s`) VALUES (%s)",
			table, strings.Join(columns, "`, `"), strings.Join(placeholders, ", "))
	}

	exec := c.getExecutor()
	result, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute insert: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	log.Debug("executed insert",
		zap.String("table", table),
		zap.Int64("rowsAffected", rowsAffected))
	return nil
}

// executeUpdate executes an UPDATE statement.
func (c *Connector) executeUpdate(ctx context.Context, e *event.ChangeEvent) error {
	table := fmt.Sprintf("`%s`.`%s`", e.Table.Database, e.Table.Table)

	// Build SET clause
	setParts := make([]string, 0)
	args := make([]interface{}, 0)

	for _, col := range e.After.ColumnNames() {
		field, _ := e.After.GetField(col)
		setParts = append(setParts, fmt.Sprintf("`%s` = ?", col))
		args = append(args, field.Value)
	}

	if len(setParts) == 0 {
		return nil // Nothing to update
	}

	// Build WHERE clause from primary key
	whereParts := make([]string, 0)
	keyColumns := e.Table.GetKeyColumns()
	if len(keyColumns) == 0 {
		return fmt.Errorf("no primary key columns for update")
	}

	for _, keyCol := range keyColumns {
		field, ok := e.Before.GetField(keyCol)
		if !ok {
			return fmt.Errorf("primary key column %s not found in before image", keyCol)
		}
		whereParts = append(whereParts, fmt.Sprintf("`%s` = ?", keyCol))
		args = append(args, field.Value)
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		table, strings.Join(setParts, ", "), strings.Join(whereParts, " AND "))

	exec := c.getExecutor()
	result, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	log.Debug("executed update",
		zap.String("table", table),
		zap.Int64("rowsAffected", rowsAffected))
	return nil
}

// executeDelete executes a DELETE statement.
func (c *Connector) executeDelete(ctx context.Context, e *event.ChangeEvent) error {
	table := fmt.Sprintf("`%s`.`%s`", e.Table.Database, e.Table.Table)

	// Build WHERE clause from primary key
	whereParts := make([]string, 0)
	args := make([]interface{}, 0)
	keyColumns := e.Table.GetKeyColumns()

	if len(keyColumns) == 0 {
		return fmt.Errorf("no primary key columns for delete")
	}

	for _, keyCol := range keyColumns {
		field, ok := e.Before.GetField(keyCol)
		if !ok {
			return fmt.Errorf("primary key column %s not found in before image", keyCol)
		}
		whereParts = append(whereParts, fmt.Sprintf("`%s` = ?", keyCol))
		args = append(args, field.Value)
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		table, strings.Join(whereParts, " AND "))

	exec := c.getExecutor()
	result, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute delete: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	log.Debug("executed delete",
		zap.String("table", table),
		zap.Int64("rowsAffected", rowsAffected))
	return nil
}

func parseConfig(config sink.Config) (*Config, error) {
	cfg := DefaultConfig()

	cfg.Host = config.Connection.Host
	cfg.Port = config.Connection.Port
	cfg.User = config.Connection.User
	cfg.Password = config.Connection.Password
	cfg.Database = config.Connection.Database

	if v, ok := config.Properties["insertStrategy"].(string); ok {
		cfg.InsertStrategy = v
	}
	if v, ok := config.Properties["useTransaction"].(bool); ok {
		cfg.UseTransaction = v
	}
	if v, ok := config.Properties["autoCreateTable"].(bool); ok {
		cfg.AutoCreateTable = v
	}
	if v, ok := config.Properties["ddlPolicy"].(string); ok {
		cfg.DDLPolicy = v
	}
	if v, ok := config.Properties["maxConnections"].(int); ok {
		cfg.MaxConnections = v
	}
	if v, ok := config.Properties["maxIdle"].(int); ok {
		cfg.MaxIdle = v
	}
	if v, ok := config.Properties["connectTimeout"].(int); ok {
		cfg.ConnectTimeout = v
	}

	cfg.Batch = config.Batch
	cfg.MaxRetries = config.Retry.MaxRetries
	cfg.RetryBackoff = config.Retry.InitialWait

	return cfg, nil
}

func init() {
	sink.Register("mysql", &factory{})
}

type factory struct{}

func (f *factory) Create(config sink.Config) (sink.Connector, error) {
	return New(), nil
}
