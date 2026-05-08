package mysql

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/sink"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Connector implements the sink.Connector interface for MySQL.
type Connector struct {
	config   *Config
	status   sink.Status
	position *event.Position
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

	c.status.State = sink.StateReady
	log.Info("MySQL sink started")
	return nil
}

// Stop stops the connector.
func (c *Connector) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.status.State = sink.StateStopped
	log.Info("MySQL sink stopped")
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

// SupportsTransaction returns true (MySQL supports transactions).
func (c *Connector) SupportsTransaction() bool {
	return c.config.UseTransaction
}

// handleDDL handles DDL events.
func (c *Connector) handleDDL(ctx context.Context, e *event.ChangeEvent) error {
	switch c.config.DDLPolicy {
	case "ignore":
		return nil
	case "error":
		return sink.ErrDDLNotSupported
	}

	// TODO: Execute DDL statement
	log.Info("executing DDL",
		zap.String("statement", e.Metadata["ddl"]))
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
	table := fmt.Sprintf("%s.%s", e.Table.Database, e.Table.Table)
	columns := e.After.ColumnNames()
	values := make([]string, len(columns))
	args := make([]interface{}, len(columns))

	for i, col := range columns {
		field, _ := e.After.GetField(col)
		values[i] = "?"
		args[i] = field.Value
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table, strings.Join(columns, ", "), strings.Join(values, ", "))

	// TODO: Execute query
	_ = query
	_ = args

	log.Debug("executing insert", zap.String("table", table))
	return nil
}

// executeUpdate executes an UPDATE statement.
func (c *Connector) executeUpdate(ctx context.Context, e *event.ChangeEvent) error {
	table := fmt.Sprintf("%s.%s", e.Table.Database, e.Table.Table)

	// Build SET clause
	setParts := make([]string, 0)
	args := make([]interface{}, 0)

	for _, col := range e.After.ColumnNames() {
		field, _ := e.After.GetField(col)
		setParts = append(setParts, fmt.Sprintf("%s = ?", col))
		args = append(args, field.Value)
	}

	// Build WHERE clause from primary key
	whereParts := make([]string, 0)
	keyColumns := e.Table.GetKeyColumns()
	for _, keyCol := range keyColumns {
		field, _ := e.Before.GetField(keyCol)
		whereParts = append(whereParts, fmt.Sprintf("%s = ?", keyCol))
		args = append(args, field.Value)
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		table, strings.Join(setParts, ", "), strings.Join(whereParts, " AND "))

	// TODO: Execute query
	_ = query
	_ = args

	log.Debug("executing update", zap.String("table", table))
	return nil
}

// executeDelete executes a DELETE statement.
func (c *Connector) executeDelete(ctx context.Context, e *event.ChangeEvent) error {
	table := fmt.Sprintf("%s.%s", e.Table.Database, e.Table.Table)

	// Build WHERE clause from primary key
	whereParts := make([]string, 0)
	args := make([]interface{}, 0)
	keyColumns := e.Table.GetKeyColumns()

	for _, keyCol := range keyColumns {
		field, _ := e.Before.GetField(keyCol)
		whereParts = append(whereParts, fmt.Sprintf("%s = ?", keyCol))
		args = append(args, field.Value)
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s",
		table, strings.Join(whereParts, " AND "))

	// TODO: Execute query
	_ = query
	_ = args

	log.Debug("executing delete", zap.String("table", table))
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
