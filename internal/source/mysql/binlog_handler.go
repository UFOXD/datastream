package mysql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// BinlogHandler handles binlog events from MySQL.
type BinlogHandler struct {
	connector *Connector
	ctx       context.Context
}

// NewBinlogHandler creates a new binlog handler.
func NewBinlogHandler(ctx context.Context, connector *Connector) *BinlogHandler {
	return &BinlogHandler{
		connector: connector,
		ctx:       ctx,
	}
}

// OnRow handles row events from the binlog.
func (h *BinlogHandler) OnRow(e *canal.RowsEvent) error {
	// Check if this table should be captured
	if !h.connector.shouldCapture(e.Table.Schema, e.Table.Name) {
		return nil
	}

	// Get or build table info
	tableInfo := h.connector.getTableInfo(e.Table)

	// Build change events based on action
	var changeEvent *event.ChangeEvent
	switch e.Action {
	case canal.InsertAction:
		changeEvent = h.buildInsertEvent(e, tableInfo)
	case canal.UpdateAction:
		changeEvent = h.buildUpdateEvent(e, tableInfo)
	case canal.DeleteAction:
		changeEvent = h.buildDeleteEvent(e, tableInfo)
	default:
		log.Warn("unknown action", zap.String("action", e.Action))
		return nil
	}

	if changeEvent == nil {
		return nil
	}

	// Send event
	select {
	case h.connector.events <- changeEvent:
		h.connector.mu.Lock()
		h.connector.position = &changeEvent.Position
		h.connector.mu.Unlock()
	case <-h.ctx.Done():
		return h.ctx.Err()
	case <-h.connector.stopCh:
		return fmt.Errorf("connector stopped")
	}

	return nil
}

// OnDDL handles DDL events.
func (h *BinlogHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	query := string(queryEvent.Query)
	query = strings.TrimSpace(query)

	// Check if this is a DDL statement
	upperQuery := strings.ToUpper(query)
	isDDL := strings.HasPrefix(upperQuery, "CREATE ") ||
		strings.HasPrefix(upperQuery, "ALTER ") ||
		strings.HasPrefix(upperQuery, "DROP ") ||
		strings.HasPrefix(upperQuery, "TRUNCATE ") ||
		strings.HasPrefix(upperQuery, "RENAME ")

	if !isDDL {
		return nil
	}

	log.Info("DDL event received",
		zap.String("query", query),
		zap.String("database", string(queryEvent.Schema)))

	// Build DDL event
	changeEvent := &event.ChangeEvent{
		ID:   event.GenerateEventID(&event.SourceInfo{Connector: "mysql"}, time.Now(), int(nextPos.Pos)),
		Type: event.EventTypeDDL,
		Source: event.SourceInfo{
			Connector: "mysql",
			Database:  string(queryEvent.Schema),
		},
		Timestamp: time.Now(),
		Position: event.Position{
			BinlogFile: nextPos.Name,
			BinlogPos:  nextPos.Pos,
			CommitTime: time.Now(),
		},
	}

	// Store DDL in metadata
	changeEvent.Metadata = map[string]string{
		"ddl": query,
	}

	// Clear schema cache for affected database
	h.connector.mu.Lock()
	h.connector.schemaCache = make(map[string]*event.TableInfo)
	h.connector.mu.Unlock()

	// Send DDL event
	select {
	case h.connector.events <- changeEvent:
		h.connector.mu.Lock()
		h.connector.position = &changeEvent.Position
		h.connector.mu.Unlock()
	case <-h.ctx.Done():
		return h.ctx.Err()
	case <-h.connector.stopCh:
		return fmt.Errorf("connector stopped")
	}

	return nil
}

// OnXID handles transaction commit events.
func (h *BinlogHandler) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	h.connector.mu.Lock()
	h.connector.position = &event.Position{
		BinlogFile: nextPos.Name,
		BinlogPos:  nextPos.Pos,
		CommitTime: time.Now(),
	}
	h.connector.mu.Unlock()
	return nil
}

// OnRotate handles binlog file rotation.
func (h *BinlogHandler) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	log.Info("binlog rotation",
		zap.String("file", string(rotateEvent.NextLogName)),
		zap.Uint64("position", rotateEvent.Position))

	h.connector.mu.Lock()
	h.connector.currentBinlog = string(rotateEvent.NextLogName)
	h.connector.mu.Unlock()

	return nil
}

// OnTableChanged handles table schema changes.
func (h *BinlogHandler) OnTableChanged(header *replication.EventHeader, schema string, table string) error {
	log.Info("table changed",
		zap.String("schema", schema),
		zap.String("table", table))

	// Clear cached schema
	key := schema + "." + table
	h.connector.mu.Lock()
	delete(h.connector.schemaCache, key)
	h.connector.mu.Unlock()

	return nil
}

// OnGTID handles GTID events.
func (h *BinlogHandler) OnGTID(header *replication.EventHeader, gtidEvent mysql.BinlogGTIDEvent) error {
	return nil
}

// OnPosSynced handles position sync events.
func (h *BinlogHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	h.connector.mu.Lock()
	h.connector.position = &event.Position{
		BinlogFile: pos.Name,
		BinlogPos:  pos.Pos,
		CommitTime: time.Now(),
	}
	h.connector.mu.Unlock()
	return nil
}

// OnRowsQueryEvent handles rows query events.
func (h *BinlogHandler) OnRowsQueryEvent(e *replication.RowsQueryEvent) error {
	return nil
}

// OnTableNotFound handles table not found events.
func (h *BinlogHandler) OnTableNotFound(header *replication.EventHeader, e *replication.RowsEvent) error {
	log.Warn("table not found",
		zap.String("schema", string(e.Table.Schema)),
		zap.String("table", string(e.Table.Table)))
	return nil
}

// String returns the handler name.
func (h *BinlogHandler) String() string {
	return "datastream-binlog-handler"
}

// buildInsertEvent builds an insert event.
func (h *BinlogHandler) buildInsertEvent(e *canal.RowsEvent, tableInfo *event.TableInfo) *event.ChangeEvent {
	if len(e.Rows) == 0 {
		return nil
	}

	// Use the first row (typically there's only one per event)
	row := e.Rows[0]
	afterData := h.buildRowData(e.Table.Columns, row)

	return &event.ChangeEvent{
		ID:   event.GenerateEventID(&event.SourceInfo{Connector: "mysql"}, time.Now(), int(e.Header.LogPos)),
		Type: event.EventTypeInsert,
		Source: event.SourceInfo{
			Connector: "mysql",
			Database:  e.Table.Schema,
		},
		Table:     *tableInfo,
		Timestamp: time.Unix(int64(e.Header.Timestamp), 0),
		After:     afterData,
		Position: event.Position{
			BinlogFile: h.connector.currentBinlogFile(),
			BinlogPos:  e.Header.LogPos,
			CommitTime: time.Now(),
		},
	}
}

// buildUpdateEvent builds an update event.
func (h *BinlogHandler) buildUpdateEvent(e *canal.RowsEvent, tableInfo *event.TableInfo) *event.ChangeEvent {
	if len(e.Rows) < 2 {
		return nil
	}

	// Update events have before and after rows
	beforeRow := e.Rows[0]
	afterRow := e.Rows[1]

	beforeData := h.buildRowData(e.Table.Columns, beforeRow)
	afterData := h.buildRowData(e.Table.Columns, afterRow)

	return &event.ChangeEvent{
		ID:   event.GenerateEventID(&event.SourceInfo{Connector: "mysql"}, time.Now(), int(e.Header.LogPos)),
		Type: event.EventTypeUpdate,
		Source: event.SourceInfo{
			Connector: "mysql",
			Database:  e.Table.Schema,
		},
		Table:     *tableInfo,
		Timestamp: time.Unix(int64(e.Header.Timestamp), 0),
		Before:    beforeData,
		After:     afterData,
		Position: event.Position{
			BinlogFile: h.connector.currentBinlogFile(),
			BinlogPos:  e.Header.LogPos,
			CommitTime: time.Now(),
		},
	}
}

// buildDeleteEvent builds a delete event.
func (h *BinlogHandler) buildDeleteEvent(e *canal.RowsEvent, tableInfo *event.TableInfo) *event.ChangeEvent {
	if len(e.Rows) == 0 {
		return nil
	}

	row := e.Rows[0]
	beforeData := h.buildRowData(e.Table.Columns, row)

	return &event.ChangeEvent{
		ID:   event.GenerateEventID(&event.SourceInfo{Connector: "mysql"}, time.Now(), int(e.Header.LogPos)),
		Type: event.EventTypeDelete,
		Source: event.SourceInfo{
			Connector: "mysql",
			Database:  e.Table.Schema,
		},
		Table:     *tableInfo,
		Timestamp: time.Unix(int64(e.Header.Timestamp), 0),
		Before:    beforeData,
		Position: event.Position{
			BinlogFile: h.connector.currentBinlogFile(),
			BinlogPos:  e.Header.LogPos,
			CommitTime: time.Now(),
		},
	}
}

// buildRowData builds RowData from column definitions and values.
func (h *BinlogHandler) buildRowData(columns []schema.TableColumn, values []interface{}) event.RowData {
	fields := make(map[string]event.Field)

	for i, col := range columns {
		if i >= len(values) {
			break
		}

		value := values[i]
		fieldType := h.mapColumnType(col.RawType)

		fields[col.Name] = event.Field{
			Name:  col.Name,
			Value: value,
			Type:  fieldType,
		}
	}

	return event.RowData{Fields: fields}
}

// mapColumnType maps MySQL column type string to our field type.
func (h *BinlogHandler) mapColumnType(rawType string) string {
	// Convert to lowercase for comparison
	rawType = strings.ToLower(rawType)

	// Numeric types
	if strings.Contains(rawType, "int") ||
		strings.Contains(rawType, "tinyint") ||
		strings.Contains(rawType, "smallint") ||
		strings.Contains(rawType, "mediumint") ||
		strings.Contains(rawType, "bigint") ||
		strings.Contains(rawType, "float") ||
		strings.Contains(rawType, "double") ||
		strings.Contains(rawType, "decimal") ||
		strings.Contains(rawType, "numeric") {
		return "number"
	}

	// Date/time types
	if strings.Contains(rawType, "date") ||
		strings.Contains(rawType, "time") ||
		strings.Contains(rawType, "timestamp") ||
		strings.Contains(rawType, "year") ||
		strings.Contains(rawType, "datetime") {
		return "datetime"
	}

	// String types
	if strings.Contains(rawType, "char") ||
		strings.Contains(rawType, "varchar") ||
		strings.Contains(rawType, "text") ||
		strings.Contains(rawType, "blob") ||
		strings.Contains(rawType, "binary") ||
		strings.Contains(rawType, "varbinary") ||
		strings.Contains(rawType, "enum") ||
		strings.Contains(rawType, "set") ||
		strings.Contains(rawType, "json") {
		return "string"
	}

	// Bit type
	if strings.Contains(rawType, "bit") {
		return "bit"
	}

	// Geometry types
	if strings.Contains(rawType, "geometry") ||
		strings.Contains(rawType, "point") ||
		strings.Contains(rawType, "linestring") ||
		strings.Contains(rawType, "polygon") {
		return "geometry"
	}

	return "unknown"
}
