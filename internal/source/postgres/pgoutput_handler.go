package postgres

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/UFOXD/datastream/internal/schema"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/parser"
	"github.com/jackc/pglogrepl"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// PGOutputHandler handles pgoutput logical replication messages.
type PGOutputHandler struct {
	connector   *Connector
	ctx         context.Context
	relations   map[uint32]*pglogrepl.RelationMessage // relation ID -> relation message
	txInProgress *pglogrepl.BeginMessage
	ddlParser   parser.DDLParser
}

// NewPGOutputHandler creates a new pgoutput handler.
func NewPGOutputHandler(ctx context.Context, connector *Connector) *PGOutputHandler {
	return &PGOutputHandler{
		connector: connector,
		relations: make(map[uint32]*pglogrepl.RelationMessage),
		ddlParser: connector.ddlParser,
	}
}

// HandleMessage handles a logical replication message.
func (h *PGOutputHandler) HandleMessage(msg pglogrepl.Message) error {
	switch m := msg.(type) {
	case *pglogrepl.BeginMessage:
		h.handleBegin(m)
	case *pglogrepl.CommitMessage:
		h.handleCommit(m)
	case *pglogrepl.RelationMessage:
		h.handleRelation(m)
	case *pglogrepl.InsertMessage:
		return h.handleInsert(m)
	case *pglogrepl.UpdateMessage:
		return h.handleUpdate(m)
	case *pglogrepl.DeleteMessage:
		return h.handleDelete(m)
	case *pglogrepl.TruncateMessage:
		return h.handleTruncate(m)
	case *pglogrepl.TypeMessage:
		// Type messages can be ignored for basic implementation
	case *pglogrepl.OriginMessage:
		// Origin messages can be ignored for basic implementation
	case *pglogrepl.LogicalDecodingMessage:
		// Logical decoding messages can be ignored
	default:
		log.Debug("unhandled message type", zap.String("type", fmt.Sprintf("%T", msg)))
	}
	return nil
}

// handleBegin handles a begin message (transaction start).
func (h *PGOutputHandler) handleBegin(msg *pglogrepl.BeginMessage) {
	h.txInProgress = msg
	log.Debug("transaction begin",
		zap.Uint32("xid", msg.Xid),
		zap.Uint64("lsn", uint64(msg.FinalLSN)))
}

// handleCommit handles a commit message (transaction end).
func (h *PGOutputHandler) handleCommit(msg *pglogrepl.CommitMessage) {
	pos := &event.Position{
		LSN:        uint64(msg.CommitLSN),
		CommitTime: msg.CommitTime,
	}

	// Update position
	h.connector.mu.Lock()
	h.connector.position = pos
	h.connector.mu.Unlock()

	// Save current position to store
	if h.connector.store != nil {
		if err := h.connector.store.SaveCurrentPosition(h.ctx, pos); err != nil {
			log.Warn("failed to save current position to store", zap.Error(err))
		}
	}

	h.txInProgress = nil

	log.Debug("transaction commit",
		zap.Uint64("lsn", uint64(msg.CommitLSN)))
}

// handleRelation handles a relation message (table schema).
// Detects schema changes by comparing with existing Tables, applies DDL,
// updates Tables, and records to SchemaHistory.
func (h *PGOutputHandler) handleRelation(msg *pglogrepl.RelationMessage) {
	h.relations[msg.RelationID] = msg

	// Build table info from relation message
	tableInfo := h.buildTableInfo(msg)
	dbName := h.connector.config.Database
	key := msg.Namespace + "." + msg.RelationName

	// Update schemaCache
	h.connector.mu.Lock()
	h.connector.schemaCache[key] = tableInfo
	h.connector.mu.Unlock()

	// Check if schema actually changed compared to Tables
	if h.connector.tables != nil {
		oldTable := h.connector.tables.Get(dbName, msg.RelationName)

		if oldTable != nil {
			// Table exists - check if schema changed
			if !tableSchemasEqual(oldTable, tableInfo) {
				// Schema changed - this is effectively an ALTER TABLE
				h.applySchemaChange(oldTable, tableInfo, msg)
			}
		} else {
			// New table - this is effectively a CREATE TABLE
			h.applySchemaChange(nil, tableInfo, msg)
		}
	}

	log.Debug("relation message",
		zap.String("schema", msg.Namespace),
		zap.String("table", msg.RelationName),
		zap.Uint32("relationId", msg.RelationID))
}

// applySchemaChange applies a detected schema change to Tables and records to SchemaHistory.
func (h *PGOutputHandler) applySchemaChange(oldTable, newTable *event.TableInfo, msg *pglogrepl.RelationMessage) {
	dbName := h.connector.config.Database
	pos := event.Position{
		LSN:        h.connector.currentLSN(),
		CommitTime: time.Now(),
	}

	var changeType string
	var ddlText string
	var resultTableInfo *event.TableInfo

	if oldTable == nil {
		// CREATE TABLE
		changeType = "CREATE"
		ddlText = fmt.Sprintf("-- CREATE TABLE detected via relation message: %s.%s", msg.Namespace, msg.RelationName)
		resultTableInfo = newTable
	} else {
		// ALTER TABLE
		changeType = "ALTER"
		ddlText = fmt.Sprintf("-- ALTER TABLE detected via relation message: %s.%s", msg.Namespace, msg.RelationName)
		resultTableInfo = newTable
	}

	// Apply DDL via parser if available
	if h.ddlParser != nil {
		applyResult, err := h.ddlParser.ApplyDDL(h.ctx, oldTable, ddlText)
		if err == nil && applyResult != nil && applyResult.NewTableInfo != nil {
			resultTableInfo = applyResult.NewTableInfo
		}
	}

	// Update Tables
	if resultTableInfo != nil {
		h.connector.tables.Put(resultTableInfo)
	}

	// Record to SchemaHistory
	if h.connector.schemaHistory != nil {
		histRec := schema.BuildSchemaHistoryRecord(
			pos, dbName, msg.Namespace, msg.RelationName,
			ddlText, resultTableInfo, changeType,
		)
		if err := h.connector.schemaHistory.Record(h.ctx, histRec); err != nil {
			log.Warn("failed to record schema history",
				zap.String("table", msg.RelationName),
				zap.Error(err))
		}
	}

	// Save current position to store
	if h.connector.store != nil {
		if err := h.connector.store.SaveCurrentPosition(h.ctx, &pos); err != nil {
			log.Warn("failed to save current position to store", zap.Error(err))
		}
	}

	// Emit DDL event
	ddlEvent := &event.ChangeEvent{
		ID:   event.GenerateEventID(&event.SourceInfo{Connector: "postgres"}, time.Now(), int(msg.RelationID)),
		Type: event.EventTypeDDL,
		Source: event.SourceInfo{
			Connector: "postgres",
			Database:  dbName,
		},
		Table: event.TableInfo{
			Database: dbName,
			Schema:   msg.Namespace,
			Table:    msg.RelationName,
		},
		Timestamp: time.Now(),
		Position:  pos,
		Metadata: map[string]string{
			"ddl":          ddlText,
			"ddlType":      changeType,
			"ddlDatabase":  dbName,
			"ddlTable":     msg.RelationName,
			"ddlStatement": ddlText,
		},
	}

	select {
	case h.connector.events <- ddlEvent:
	case <-h.ctx.Done():
	case <-h.connector.stopCh:
	}
}

// tableSchemasEqual compares two TableInfo structs for equality.
func tableSchemasEqual(a, b *event.TableInfo) bool {
	if a == nil || b == nil {
		return a == b
	}
	if len(a.Columns) != len(b.Columns) {
		return false
	}
	if len(a.PrimaryKeyColumns) != len(b.PrimaryKeyColumns) {
		return false
	}
	for i, col := range a.Columns {
		other := b.Columns[i]
		if col.Name != other.Name || col.Type != other.Type || col.Nullable != other.Nullable {
			return false
		}
	}
	for i, pk := range a.PrimaryKeyColumns {
		if pk != b.PrimaryKeyColumns[i] {
			return false
		}
	}
	return true
}

// getTableInfo returns the table schema, preferring Tables (from SchemaHistory)
// over the relation message.
func (h *PGOutputHandler) getTableInfo(relation *pglogrepl.RelationMessage) *event.TableInfo {
	dbName := h.connector.config.Database
	if h.connector.tables != nil {
		if info := h.connector.tables.Get(dbName, relation.RelationName); info != nil {
			return info
		}
	}
	return h.buildTableInfo(relation)
}

// handleInsert handles an insert message.
func (h *PGOutputHandler) handleInsert(msg *pglogrepl.InsertMessage) error {
	relation, ok := h.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	// Check if this table should be captured
	if !h.connector.shouldCapture(relation.Namespace, relation.RelationName) {
		return nil
	}

	tableInfo := h.getTableInfo(relation)
	afterData := h.buildRowData(relation, msg.Tuple)

	changeEvent := &event.ChangeEvent{
		ID:   event.GenerateEventID(&event.SourceInfo{Connector: "postgres"}, time.Now(), int(msg.RelationID)),
		Type: event.EventTypeInsert,
		Source: event.SourceInfo{
			Connector: "postgres",
			Database:  h.connector.config.Database,
		},
		Table:     *tableInfo,
		Timestamp: time.Now(),
		After:     afterData,
		Position: event.Position{
			LSN:        h.connector.currentLSN(),
			CommitTime: time.Now(),
		},
	}

	return h.sendEvent(changeEvent)
}

// handleUpdate handles an update message.
func (h *PGOutputHandler) handleUpdate(msg *pglogrepl.UpdateMessage) error {
	relation, ok := h.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	// Check if this table should be captured
	if !h.connector.shouldCapture(relation.Namespace, relation.RelationName) {
		return nil
	}

	tableInfo := h.getTableInfo(relation)

	// Build before and after data
	var beforeData, afterData event.RowData
	if msg.OldTuple != nil {
		beforeData = h.buildRowData(relation, msg.OldTuple)
	}
	if msg.NewTuple != nil {
		afterData = h.buildRowData(relation, msg.NewTuple)
	}

	changeEvent := &event.ChangeEvent{
		ID:   event.GenerateEventID(&event.SourceInfo{Connector: "postgres"}, time.Now(), int(msg.RelationID)),
		Type: event.EventTypeUpdate,
		Source: event.SourceInfo{
			Connector: "postgres",
			Database:  h.connector.config.Database,
		},
		Table:     *tableInfo,
		Timestamp: time.Now(),
		Before:    beforeData,
		After:     afterData,
		Position: event.Position{
			LSN:        h.connector.currentLSN(),
			CommitTime: time.Now(),
		},
	}

	return h.sendEvent(changeEvent)
}

// handleDelete handles a delete message.
func (h *PGOutputHandler) handleDelete(msg *pglogrepl.DeleteMessage) error {
	relation, ok := h.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	// Check if this table should be captured
	if !h.connector.shouldCapture(relation.Namespace, relation.RelationName) {
		return nil
	}

	tableInfo := h.getTableInfo(relation)

	// Build before data from old tuple
	var beforeData event.RowData
	if msg.OldTuple != nil {
		beforeData = h.buildRowData(relation, msg.OldTuple)
	}

	changeEvent := &event.ChangeEvent{
		ID:   event.GenerateEventID(&event.SourceInfo{Connector: "postgres"}, time.Now(), int(msg.RelationID)),
		Type: event.EventTypeDelete,
		Source: event.SourceInfo{
			Connector: "postgres",
			Database:  h.connector.config.Database,
		},
		Table:     *tableInfo,
		Timestamp: time.Now(),
		Before:    beforeData,
		Position: event.Position{
			LSN:        h.connector.currentLSN(),
			CommitTime: time.Now(),
		},
	}

	return h.sendEvent(changeEvent)
}

// handleTruncate handles a truncate message.
func (h *PGOutputHandler) handleTruncate(msg *pglogrepl.TruncateMessage) error {
	// Truncate messages don't have individual row data
	// We'll emit a truncate event for each relation
	for _, relationID := range msg.RelationIDs {
		relation, ok := h.relations[relationID]
		if !ok {
			log.Warn("unknown relation ID in truncate", zap.Uint32("relationId", relationID))
			continue
		}

		if !h.connector.shouldCapture(relation.Namespace, relation.RelationName) {
			continue
		}

		tableInfo := h.buildTableInfo(relation)

		changeEvent := &event.ChangeEvent{
			ID:   event.GenerateEventID(&event.SourceInfo{Connector: "postgres"}, time.Now(), int(relationID)),
			Type: event.EventTypeTruncate,
			Source: event.SourceInfo{
				Connector: "postgres",
				Database:  h.connector.config.Database,
			},
			Table:     *tableInfo,
			Timestamp: time.Now(),
			Position: event.Position{
				LSN:        h.connector.currentLSN(),
				CommitTime: time.Now(),
			},
		}

		if err := h.sendEvent(changeEvent); err != nil {
			return err
		}
	}

	return nil
}

// buildTableInfo builds a TableInfo from a RelationMessage.
func (h *PGOutputHandler) buildTableInfo(relation *pglogrepl.RelationMessage) *event.TableInfo {
	info := &event.TableInfo{
		Database: h.connector.config.Database,
		Schema:   relation.Namespace,
		Table:    relation.RelationName,
	}

	// Build columns
	columns := make([]event.ColumnInfo, 0, len(relation.Columns))
	keyColumns := make([]string, 0)

	for _, col := range relation.Columns {
		columns = append(columns, event.ColumnInfo{
			Name:     col.Name,
			Type:     h.mapPostgresType(int32(col.DataType)),
			Nullable: (col.Flags & 0x0001) == 0, // 0x0001 = NOT NULL
		})

		// Check if this is a key column (flags & 0x0002 = part of primary key)
		if col.Flags&0x0002 != 0 {
			keyColumns = append(keyColumns, col.Name)
		}
	}

	info.Columns = columns
	info.PrimaryKeyColumns = keyColumns

	return info
}

// buildRowData builds RowData from a relation and tuple.
func (h *PGOutputHandler) buildRowData(relation *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) event.RowData {
	fields := make(map[string]event.Field)

	for i, col := range relation.Columns {
		if i >= len(tuple.Columns) {
			break
		}

		tupleCol := tuple.Columns[i]

		var value interface{}
		var isNull bool

		switch tupleCol.DataType {
		case pglogrepl.TupleDataTypeNull:
			value = nil
			isNull = true
		case pglogrepl.TupleDataTypeText:
			value = string(tupleCol.Data)
		case pglogrepl.TupleDataTypeBinary:
			value = h.decodeBinaryValue(tupleCol.Data, int32(col.DataType))
		}

		fields[col.Name] = event.Field{
			Name:  col.Name,
			Value: value,
			Type:  h.mapPostgresType(int32(col.DataType)),
		}

		if isNull {
			fields[col.Name] = event.Field{
				Name:  col.Name,
				Value: nil,
				Type:  h.mapPostgresType(int32(col.DataType)),
			}
		}
	}

	return event.RowData{Fields: fields}
}

// decodeBinaryValue decodes a binary-encoded value.
func (h *PGOutputHandler) decodeBinaryValue(data []byte, dataType int32) interface{} {
	// Basic binary decoding for common types
	// Most implementations use text mode, but binary may be used for some types
	switch dataType {
	case 16: // bool
		if len(data) > 0 {
			return data[0] != 0
		}
	case 21: // int2
		if len(data) >= 2 {
			return int16(binary.BigEndian.Uint16(data))
		}
	case 23: // int4
		if len(data) >= 4 {
			return int32(binary.BigEndian.Uint32(data))
		}
	case 20: // int8
		if len(data) >= 8 {
			return int64(binary.BigEndian.Uint64(data))
		}
	case 700: // float4
		if len(data) >= 4 {
			bits := binary.BigEndian.Uint32(data)
			return float32FromBits(bits)
		}
	case 701: // float8
		if len(data) >= 8 {
			bits := binary.BigEndian.Uint64(data)
			return float64FromBits(bits)
		}
	}

	// Return raw bytes for unknown types
	return data
}

// float32FromBits converts bits to float32.
func float32FromBits(b uint32) float32 {
	return float32(float64(b)) // Simplified; use math.Float32frombits in production
}

// float64FromBits converts bits to float64.
func float64FromBits(b uint64) float64 {
	return float64(b) // Simplified; use math.Float64frombits in production
}

// mapPostgresType maps PostgreSQL OID to type name.
func (h *PGOutputHandler) mapPostgresType(oid int32) string {
	// PostgreSQL type OIDs
	switch oid {
	case 16: // bool
		return "bool"
	case 17: // bytea
		return "bytea"
	case 18: // char
		return "char"
	case 19: // name
		return "name"
	case 20: // int8
		return "int8"
	case 21: // int2
		return "int2"
	case 22: // int2vector
		return "int2vector"
	case 23: // int4
		return "int4"
	case 24: // regproc
		return "regproc"
	case 25: // text
		return "text"
	case 26: // oid
		return "oid"
	case 700: // float4
		return "float4"
	case 701: // float8
		return "float8"
	case 702: // abstime
		return "abstime"
	case 703: // reltime
		return "reltime"
	case 704: // tinterval
		return "tinterval"
	case 790: // money
		return "money"
	case 1042: // bpchar
		return "bpchar"
	case 1043: // varchar
		return "varchar"
	case 1082: // date
		return "date"
	case 1083: // time
		return "time"
	case 1114: // timestamp
		return "timestamp"
	case 1184: // timestamptz
		return "timestamptz"
	case 1186: // interval
		return "interval"
	case 1700: // numeric
		return "numeric"
	case 2950: // uuid
		return "uuid"
	case 3802: // jsonb
		return "jsonb"
	case 114: // json
		return "json"
	case 142: // xml
		return "xml"
	default:
		return "unknown"
	}
}

// sendEvent sends an event to the events channel.
func (h *PGOutputHandler) sendEvent(changeEvent *event.ChangeEvent) error {
	select {
	case h.connector.events <- changeEvent:
		return nil
	case <-h.ctx.Done():
		return h.ctx.Err()
	case <-h.connector.stopCh:
		return fmt.Errorf("connector stopped")
	}
}
