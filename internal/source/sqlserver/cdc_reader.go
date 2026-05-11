// Package sqlserver provides SQL Server source connector for DataStream.
package sqlserver

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

// Position represents the CDC read position using LSN.
type Position struct {
	StartLSN   string    `json:"start_lsn"`   // Binary(10) as hex string
	CommitTime time.Time `json:"commit_time"`
}

// CDCReader reads changes from SQL Server CDC tables.
type CDCReader struct {
	db              *sql.DB
	config          *Config
	schemaCache     *TableSchemaCache
	captureInstance string
	position        *Position
}

// NewCDCReader creates a new CDC reader for the given capture instance.
func NewCDCReader(db *sql.DB, config *Config, schemaCache *TableSchemaCache, captureInstance string) *CDCReader {
	return &CDCReader{
		db:              db,
		config:          config,
		schemaCache:     schemaCache,
		captureInstance: captureInstance,
	}
}

// GetPosition returns the current CDC position.
func (r *CDCReader) GetPosition() *Position {
	return r.position
}

// SetPosition sets the CDC position to resume from.
func (r *CDCReader) SetPosition(pos *Position) {
	r.position = pos
}

// getCurrentLSN retrieves the current maximum LSN from SQL Server.
func (r *CDCReader) getCurrentLSN(ctx context.Context) (string, error) {
	var lsn []byte
	err := r.db.QueryRowContext(ctx, `SELECT sys.fn_cdc_get_max_lsn()`).Scan(&lsn)
	if err != nil {
		return "", fmt.Errorf("failed to get current LSN: %w", err)
	}
	return hex.EncodeToString(lsn), nil
}

// incrementLSN increments an LSN by 1 to avoid re-reading the last record.
func (r *CDCReader) incrementLSN(lsn string) string {
	data, _ := hex.DecodeString(lsn)
	for i := len(data) - 1; i >= 0; i-- {
		data[i]++
		if data[i] != 0 {
			break
		}
	}
	return hex.EncodeToString(data)
}

// lsnToBytes converts a hex LSN string to []byte for SQL Server queries.
func lsnToBytes(lsn string) ([]byte, error) {
	return hex.DecodeString(lsn)
}

// ReadChanges reads CDC changes between fromLSN and the current max LSN.
// If fromLSN is empty, it starts from the minimum available LSN.
func (r *CDCReader) ReadChanges(ctx context.Context) ([]*event.ChangeEvent, string, error) {
	// Get current max LSN
	toLSN, err := r.getCurrentLSN(ctx)
	if err != nil {
		return nil, "", err
	}

	// Determine starting LSN
	fromLSN := ""
	if r.position != nil {
		fromLSN = r.position.StartLSN
	}

	if fromLSN == "" {
		// Get minimum available LSN for the capture instance
		minLSN, err := r.getMinLSN(ctx)
		if err != nil {
			return nil, "", err
		}
		fromLSN = minLSN
	}

	if fromLSN == toLSN {
		return nil, toLSN, nil
	}

	changes, err := r.queryChanges(ctx, fromLSN, toLSN)
	if err != nil {
		return nil, "", err
	}

	return changes, toLSN, nil
}

// getMinLSN retrieves the minimum available LSN for this capture instance.
func (r *CDCReader) getMinLSN(ctx context.Context) (string, error) {
	var lsn []byte
	err := r.db.QueryRowContext(ctx,
		`SELECT sys.fn_cdc_get_min_lsn(@p1)`, r.captureInstance,
	).Scan(&lsn)
	if err != nil {
		return "", fmt.Errorf("failed to get min LSN: %w", err)
	}
	return hex.EncodeToString(lsn), nil
}

// queryChanges queries CDC changes between fromLSN and toLSN.
func (r *CDCReader) queryChanges(ctx context.Context, fromLSN, toLSN string) ([]*event.ChangeEvent, error) {
	fromBytes, err := lsnToBytes(fromLSN)
	if err != nil {
		return nil, fmt.Errorf("invalid fromLSN %q: %w", fromLSN, err)
	}
	toBytes, err := lsnToBytes(toLSN)
	if err != nil {
		return nil, fmt.Errorf("invalid toLSN %q: %w", toLSN, err)
	}

	query := fmt.Sprintf(`
		SELECT __$start_lsn, __$operation, __$update_mask, *
		FROM cdc.fn_cdc_get_all_changes_%s(@p1, @p2, N'all')
		ORDER BY __$start_lsn
	`, r.captureInstance)

	rows, err := r.db.QueryContext(ctx, query, fromBytes, toBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to query CDC changes for %s: %w", r.captureInstance, err)
	}
	defer rows.Close()

	return r.parseRows(ctx, rows)
}

// parseRows parses CDC result rows into ChangeEvents.
// Operation codes:
//
//	1 = Delete
//	2 = Insert
//	3 = Update before-image (used to populate Before field)
//	4 = Update after-image
func (r *CDCReader) parseRows(ctx context.Context, rows *sql.Rows) ([]*event.ChangeEvent, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// CDC metadata columns: __$start_lsn, __$operation, __$update_mask
	// Data columns start at index 3.
	const cdcMetaCols = 3
	if len(cols) < cdcMetaCols {
		return nil, fmt.Errorf("unexpected column count: %d", len(cols))
	}
	dataCols := cols[cdcMetaCols:]

	sourceInfo := event.SourceInfo{
		Connector: "sqlserver",
		Database:  r.config.Database,
	}

	tableInfo := event.TableInfo{
		Database: r.config.Database,
		Table:    r.captureInstance,
	}

	var (
		events    []*event.ChangeEvent
		beforeRow event.RowData // holds the before-image for operation 3
		beforeLSN string
	)

	for rows.Next() {
		// Scan all values
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Extract metadata
		lsnBytes, _ := values[0].([]byte)
		lsn := hex.EncodeToString(lsnBytes)
		opCode, _ := values[1].(int64)
		// values[2] is __$update_mask — not used for field filtering currently

		// Build row data from data columns
		rowData := event.RowData{
			Fields: make(map[string]event.Field, len(dataCols)),
		}
		for i, colName := range dataCols {
			rowData.Fields[colName] = event.Field{
				Name:  colName,
				Value: values[cdcMetaCols+i],
			}
		}

		pos := event.Position{
			ChangeLsn:  lsn,
			CommitTime: time.Now(),
		}

		switch opCode {
		case 1: // Delete
			ev := &event.ChangeEvent{
				Type:      event.EventTypeDelete,
				Table:     tableInfo,
				Source:    sourceInfo,
				Before:    rowData,
				Timestamp: time.Now(),
				Position:  pos,
			}
			events = append(events, ev)

		case 2: // Insert
			ev := &event.ChangeEvent{
				Type:      event.EventTypeInsert,
				Table:     tableInfo,
				Source:    sourceInfo,
				After:     rowData,
				Timestamp: time.Now(),
				Position:  pos,
			}
			events = append(events, ev)

		case 3: // Update before-image — hold for pairing with op 4
			beforeRow = rowData
			beforeLSN = lsn

		case 4: // Update after-image
			ev := &event.ChangeEvent{
				Type:      event.EventTypeUpdate,
				Table:     tableInfo,
				Source:    sourceInfo,
				After:     rowData,
				Timestamp: time.Now(),
				Position:  pos,
			}
			// Attach before-image if it matches this LSN
			if beforeLSN == lsn && len(beforeRow.Fields) > 0 {
				ev.Before = beforeRow
			}
			events = append(events, ev)
			// Reset before-image state
			beforeRow = event.RowData{}
			beforeLSN = ""
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("CDC row iteration error: %w", err)
	}

	return events, nil
}

// UpdatePosition advances the reader position to the given LSN.
func (r *CDCReader) UpdatePosition(lsn string) {
	r.position = &Position{
		StartLSN:   r.incrementLSN(lsn),
		CommitTime: time.Now(),
	}
}
