// Package oracle provides an Oracle source connector for DataStream.
package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

// Position represents the LogMiner read position using SCN.
type Position struct {
	SCN        uint64    `json:"scn"`
	CommitTime time.Time `json:"commit_time"`
}

// LogMinerReader reads changes from Oracle using LogMiner.
type LogMinerReader struct {
	db          *sql.DB
	config      *Config
	schemaCache *TableSchemaCache
	mu          sync.RWMutex
	scn         uint64
}

// NewLogMinerReader creates a new LogMiner reader.
func NewLogMinerReader(db *sql.DB, config *Config, schemaCache *TableSchemaCache) *LogMinerReader {
	return &LogMinerReader{
		db:          db,
		config:      config,
		schemaCache: schemaCache,
	}
}

// GetSCN returns the current SCN position.
func (r *LogMinerReader) GetSCN() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.scn
}

// SetSCN sets the SCN position to resume from.
func (r *LogMinerReader) SetSCN(scn uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scn = scn
}

// getCurrentSCN retrieves the current SCN from the database.
func (r *LogMinerReader) getCurrentSCN(ctx context.Context) (uint64, error) {
	var scn uint64
	err := r.db.QueryRowContext(ctx, `SELECT CURRENT_SCN FROM V$DATABASE`).Scan(&scn)
	if err != nil {
		return 0, fmt.Errorf("failed to get current SCN: %w", err)
	}
	return scn, nil
}

// startMining starts a LogMiner session beginning at startSCN.
func (r *LogMinerReader) startMining(ctx context.Context, startSCN uint64) error {
	var pl string
	switch r.config.MiningStrategy {
	case "continuous":
		pl = `BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => :1, OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.CONTINUOUS_MINE); END;`
	default: // "online"
		pl = `BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => :1, OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;`
	}
	_, err := r.db.ExecContext(ctx, pl, startSCN)
	if err != nil {
		return fmt.Errorf("failed to start LogMiner: %w", err)
	}
	return nil
}

// stopMining ends the LogMiner session.
func (r *LogMinerReader) stopMining(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `BEGIN DBMS_LOGMNR.END_LOGMNR(); END;`)
	if err != nil {
		return fmt.Errorf("failed to stop LogMiner: %w", err)
	}
	return nil
}

// queryChanges queries V$LOGMNR_CONTENTS for changes after startSCN.
func (r *LogMinerReader) queryChanges(ctx context.Context, startSCN uint64) ([]*event.ChangeEvent, error) {
	query := `
		SELECT SCN, SQL_REDO, OPERATION_CODE, TABLE_NAME, SEG_OWNER, TIMESTAMP
		FROM V$LOGMNR_CONTENTS
		WHERE SCN > :1
		  AND OPERATION_CODE IN (1, 2, 3, 5)
		ORDER BY SCN
		FETCH FIRST :2 ROWS ONLY
	`

	rows, err := r.db.QueryContext(ctx, query, startSCN, r.config.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query LogMiner contents: %w", err)
	}
	defer rows.Close()

	return r.parseRows(ctx, rows)
}

// parseRows converts V$LOGMNR_CONTENTS rows into ChangeEvents.
func (r *LogMinerReader) parseRows(ctx context.Context, rows *sql.Rows) ([]*event.ChangeEvent, error) {
	sourceInfo := event.SourceInfo{
		Connector: "oracle",
		Database:  r.config.ServiceName,
	}

	parser := NewDmlParser()

	var (
		events []*event.ChangeEvent
		seqNo  int
	)

	for rows.Next() {
		var (
			scn       uint64
			sqlRedo   sql.NullString
			opCode    int64
			tableName string
			segOwner  string
			ts        time.Time
		)

		if err := rows.Scan(&scn, &sqlRedo, &opCode, &tableName, &segOwner, &ts); err != nil {
			return nil, fmt.Errorf("failed to scan LogMiner row: %w", err)
		}

		tableInfo := event.TableInfo{
			Database: segOwner,
			Schema:   segOwner,
			Table:    tableName,
		}

		pos := event.Position{
			SCN:        scn,
			CommitTime: ts,
		}

		seqNo++

		switch opCode {
		case 1: // Insert
			entry, err := parser.Parse(sqlRedo.String)
			if err != nil {
				continue
			}
			ev := &event.ChangeEvent{
				ID:        event.GenerateEventID(&sourceInfo, ts, seqNo),
				Type:      event.EventTypeInsert,
				Table:     tableInfo,
				Source:    sourceInfo,
				After:     entryToRowData(entry.NewValues),
				Timestamp: ts,
				Position:  pos,
			}
			events = append(events, ev)

		case 2: // Delete
			entry, err := parser.Parse(sqlRedo.String)
			if err != nil {
				continue
			}
			ev := &event.ChangeEvent{
				ID:        event.GenerateEventID(&sourceInfo, ts, seqNo),
				Type:      event.EventTypeDelete,
				Table:     tableInfo,
				Source:    sourceInfo,
				Before:    entryToRowData(entry.OldValues),
				Timestamp: ts,
				Position:  pos,
			}
			events = append(events, ev)

		case 3: // Update
			entry, err := parser.Parse(sqlRedo.String)
			if err != nil {
				continue
			}
			mergeUpdateValues(entry)
			ev := &event.ChangeEvent{
				ID:        event.GenerateEventID(&sourceInfo, ts, seqNo),
				Type:      event.EventTypeUpdate,
				Table:     tableInfo,
				Source:    sourceInfo,
				After:     entryToRowData(entry.NewValues),
				Before:    entryToRowData(entry.OldValues),
				Timestamp: ts,
				Position:  pos,
			}
			events = append(events, ev)

		case 5: // DDL
			ev := &event.ChangeEvent{
				ID:        event.GenerateEventID(&sourceInfo, ts, seqNo),
				Type:      event.EventTypeDDL,
				Table:     tableInfo,
				Source:    sourceInfo,
				Timestamp: ts,
				Position:  pos,
				Metadata:  map[string]string{"sql": sqlRedo.String},
			}
			events = append(events, ev)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("LogMiner row iteration error: %w", err)
	}

	return events, nil
}

// mergeUpdateValues copies WHERE columns not present in SET to NewValues.
func mergeUpdateValues(entry *DmlEntry) {
	for col, oldVal := range entry.OldValues {
		if _, exists := entry.NewValues[col]; !exists {
			entry.NewValues[col] = oldVal
		}
	}
}

// entryToRowData converts map[string]string to event.RowData using parseValue for type inference.
func entryToRowData(vals map[string]string) event.RowData {
	rd := event.NewRowData()
	for col, raw := range vals {
		rd.Set(col, parseValue(raw), "")
	}
	return *rd
}

// ReadChanges starts a LogMiner session, reads changes, stops the session, and returns events.
func (r *LogMinerReader) ReadChanges(ctx context.Context) ([]*event.ChangeEvent, uint64, error) {
	currentSCN, err := r.getCurrentSCN(ctx)
	if err != nil {
		return nil, 0, err
	}

	startSCN := r.GetSCN()
	if startSCN == 0 {
		// Start from just before the current SCN so we don't miss anything
		if currentSCN > 0 {
			startSCN = currentSCN - 1
		}
	}

	if startSCN >= currentSCN {
		return nil, currentSCN, nil
	}

	if err := r.startMining(ctx, startSCN); err != nil {
		return nil, 0, err
	}
	defer r.stopMining(ctx) //nolint:errcheck

	changes, err := r.queryChanges(ctx, startSCN)
	if err != nil {
		return nil, 0, err
	}

	return changes, currentSCN, nil
}

// UpdatePosition advances the reader position to the given SCN.
func (r *LogMinerReader) UpdatePosition(scn uint64) {
	r.SetSCN(scn)
}

// ---- SQL value helpers ----

// parseValue converts a raw SQL value string to a Go value.
// Handles: NULL, single-quoted strings, numeric literals.
func parseValue(raw string) interface{} {
	raw = strings.TrimSpace(raw)

	if strings.EqualFold(raw, "NULL") {
		return nil
	}

	// Single-quoted string
	if len(raw) >= 2 && raw[0] == '\'' && raw[len(raw)-1] == '\'' {
		inner := raw[1 : len(raw)-1]
		// Unescape doubled single quotes
		return strings.ReplaceAll(inner, "''", "'")
	}

	// Try integer
	if i, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return i
	}

	// Try float
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f
	}

	// Return as-is
	return raw
}
