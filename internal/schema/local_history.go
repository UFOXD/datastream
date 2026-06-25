package schema

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/UFOXD/datastream/pkg/event"
)

// LocalSchemaHistory implements SchemaHistory using a single append-only file.
// Format: [4B length][N bytes JSON] per record.
type LocalSchemaHistory struct {
	filePath string
	file     *os.File
}

// NewLocalSchemaHistory creates a LocalSchemaHistory at {dataDir}/meta/schema_history.log.
func NewLocalSchemaHistory(dataDir string) (*LocalSchemaHistory, error) {
	metaDir := filepath.Join(dataDir, "meta")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		return nil, fmt.Errorf("create meta dir: %w", err)
	}

	fp := filepath.Join(metaDir, "schema_history.log")
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open history file: %w", err)
	}

	return &LocalSchemaHistory{filePath: fp, file: f}, nil
}

// Record appends a SchemaHistoryRecord to the log file.
func (h *LocalSchemaHistory) Record(_ context.Context, record *event.SchemaHistoryRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

	if _, err := h.file.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write length: %w", err)
	}
	if _, err := h.file.Write(data); err != nil {
		return fmt.Errorf("write data: %w", err)
	}
	return h.file.Sync()
}

// Recover replays history records into Tables.
// Only records with position ≤ offset are applied.
func (h *LocalSchemaHistory) Recover(ctx context.Context, tables *Tables, offset *event.Position) error {
	f, err := os.Open(h.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open history file: %w", err)
	}
	defer f.Close()

	var lenBuf [4]byte
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("read length: %w", err)
		}

		dataLen := binary.BigEndian.Uint32(lenBuf[:])
		data := make([]byte, dataLen)
		if _, err := io.ReadFull(f, data); err != nil {
			return fmt.Errorf("read data: %w", err)
		}

		var rec event.SchemaHistoryRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			return fmt.Errorf("unmarshal record: %w", err)
		}

		// Stop if we've passed the offset.
		if offset != nil && !offset.IsZero() && rec.Position.CommitTime.After(offset.CommitTime) {
			return nil
		}

		// Apply to Tables.
		if rec.TableInfo != nil {
			tables.Put(rec.TableInfo)
		} else if rec.ChangeType == "DROP" {
			tables.Remove(rec.Database, rec.Table)
		}
	}
}

// Exists returns true if the history log file exists and is non-empty.
func (h *LocalSchemaHistory) Exists(_ context.Context) (bool, error) {
	info, err := os.Stat(h.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.Size() > 0, nil
}

// Close closes the file handle.
func (h *LocalSchemaHistory) Close() error {
	if h.file != nil {
		return h.file.Close()
	}
	return nil
}
