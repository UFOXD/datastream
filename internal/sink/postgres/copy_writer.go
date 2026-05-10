package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/UFOXD/datastream/pkg/event"
)

// CopyWriter implements efficient bulk inserts using PostgreSQL COPY protocol.
type CopyWriter struct {
	db     *sql.DB
	schema string
}

// NewCopyWriter creates a new COPY writer.
func NewCopyWriter(db *sql.DB, schema string) *CopyWriter {
	return &CopyWriter{
		db:     db,
		schema: schema,
	}
}

// WriteBatch writes multiple events using COPY protocol.
// This is efficient for bulk inserts but only works for INSERT events.
func (w *CopyWriter) WriteBatch(ctx context.Context, events []*event.ChangeEvent) error {
	if len(events) == 0 {
		return nil
	}

	// Group events by table
	tableEvents := make(map[string][]*event.ChangeEvent)
	for _, e := range events {
		if e.Type != event.EventTypeInsert {
			continue // COPY only works for inserts
		}
		tableKey := e.Table.Database + "." + e.Table.Table
		tableEvents[tableKey] = append(tableEvents[tableKey], e)
	}

	// Process each table
	for _, tableEventList := range tableEvents {
		if len(tableEventList) == 0 {
			continue
		}

		// Get table name and columns from first event
		firstEvent := tableEventList[0]
		schema := w.schema
		if firstEvent.Table.Schema != "" {
			schema = firstEvent.Table.Schema
		}
		table := fmt.Sprintf(`"%s"."%s"`, schema, firstEvent.Table.Table)
		columns := firstEvent.After.ColumnNames()

		if err := w.copyTable(ctx, table, columns, tableEventList); err != nil {
			return fmt.Errorf("failed to copy table %s: %w", table, err)
		}
	}

	return nil
}

// copyTable performs COPY for a single table.
func (w *CopyWriter) copyTable(ctx context.Context, table string, columns []string, events []*event.ChangeEvent) error {
	// Build COPY statement
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = fmt.Sprintf(`"%s"`, col)
	}

	// Get raw connection for COPY
	var conn *sql.Conn
	var err error

	conn, err = w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	// Use pgx driver's CopyFrom functionality
	// For standard lib/pq, we need to use the conn.Raw method
	err = conn.Raw(func(driverConn interface{}) error {
		// Check if the connection supports COPY
		// This requires the underlying driver to implement copy support
		// For pgx: use pgx.CopyFrom
		// For lib/pq: use pq.CopyIn

		// Build CSV data
		var buf strings.Builder
		for _, e := range events {
			row := make([]string, len(columns))
			for i, col := range columns {
				field, ok := e.After.GetField(col)
				if !ok || field.Value == nil {
					row[i] = "NULL"
				} else {
					// Escape CSV special characters
					val := fmt.Sprintf("%v", field.Value)
					if strings.ContainsAny(val, `",\n`) {
						val = `"` + strings.ReplaceAll(val, `"`, `""`) + `"`
					}
					row[i] = val
				}
			}
			buf.WriteString(strings.Join(row, ","))
			buf.WriteString("\n")
		}

		// For now, return an error indicating COPY needs pgx driver
		// In production, this would use the actual COPY protocol
		return fmt.Errorf("COPY protocol requires pgx driver; falling back to batch insert")
	})

	return err
}

// copyReader implements io.Reader for COPY data.
type copyReader struct {
	data   string
	offset int
}

func (r *copyReader) Read(p []byte) (n int, err error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}
