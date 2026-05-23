package cache

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
)

// Compile-time check: LocalBackend implements BinlogCacheBackend.
var _ BinlogCacheBackend = (*LocalBackend)(nil)

var sanitizeRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// LocalBackend is a filesystem-based BinlogCacheBackend. Each table ID maps to
// a single append-only file under baseDir using length-prefixed protobuf
// records.
type LocalBackend struct {
	baseDir string
	files   map[string]*os.File   // tableID -> open append file
	locks   map[string]*sync.Mutex // tableID -> per-table write lock
	mu      sync.Mutex             // protects files and locks maps
}

// NewLocalBackend creates a LocalBackend rooted at baseDir. The directory is
// created if it does not already exist.
func NewLocalBackend(baseDir string) (*LocalBackend, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create base dir: %w", err)
	}
	return &LocalBackend{
		baseDir: baseDir,
		files:   make(map[string]*os.File),
		locks:   make(map[string]*sync.Mutex),
	}, nil
}

// sanitizeTableID replaces every non-alphanumeric, non-underscore character
// with an underscore.
func sanitizeTableID(tableID string) string {
	return sanitizeRe.ReplaceAllString(tableID, "_")
}

// filePath returns the on-disk path for a table's binlog file.
func (lb *LocalBackend) filePath(tableID string) string {
	return filepath.Join(lb.baseDir, sanitizeTableID(tableID)+".binlog")
}

// tableLock returns (or creates) the per-table mutex.
func (lb *LocalBackend) tableLock(tableID string) *sync.Mutex {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	m, ok := lb.locks[tableID]
	if !ok {
		m = &sync.Mutex{}
		lb.locks[tableID] = m
	}
	return m
}

// getFile returns (or lazily opens) the append-mode file handle for tableID.
// Caller must hold the per-table lock.
func (lb *LocalBackend) getFile(tableID string) (*os.File, error) {
	lb.mu.Lock()
	f, ok := lb.files[tableID]
	lb.mu.Unlock()
	if ok {
		return f, nil
	}

	f, err := os.OpenFile(lb.filePath(tableID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open file for table %s: %w", tableID, err)
	}

	lb.mu.Lock()
	lb.files[tableID] = f
	lb.mu.Unlock()
	return f, nil
}

// Write appends a single CacheEvent to the table's binlog file using a
// 4-byte big-endian length prefix followed by the proto-marshaled payload.
func (lb *LocalBackend) Write(_ context.Context, tableID string, event *CacheEvent) error {
	data, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	tl := lb.tableLock(tableID)
	tl.Lock()
	defer tl.Unlock()

	f, err := lb.getFile(tableID)
	if err != nil {
		return err
	}

	// Length prefix (4 bytes, big-endian).
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := f.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write length prefix: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	return nil
}

// Read opens the table's binlog file and sends CacheEvents to the returned
// channel. If fromGTID is empty the entire file is streamed. Otherwise events
// are skipped until we find one whose GTID matches fromGTID and whose
// EventSeq >= fromEventSeq; from that point on every subsequent event is sent.
func (lb *LocalBackend) Read(ctx context.Context, tableID string, fromGTID string, fromEventSeq int64) (<-chan *CacheEvent, error) {
	fp := lb.filePath(tableID)
	f, err := os.Open(fp)
	if err != nil {
		return nil, fmt.Errorf("open file for reading table %s: %w", tableID, err)
	}

	ch := make(chan *CacheEvent, 64)
	go func() {
		defer close(ch)
		defer f.Close()

		emitting := fromGTID == ""
		var lenBuf [4]byte

		for {
			// Check cancellation before every record.
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Read length prefix.
			if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
				return // EOF or read error — done
			}
			payloadLen := binary.BigEndian.Uint32(lenBuf[:])
			data := make([]byte, payloadLen)
			if _, err := io.ReadFull(f, data); err != nil {
				return
			}

			ev := &CacheEvent{}
			if err := proto.Unmarshal(data, ev); err != nil {
				return // corrupt record — stop
			}

			if !emitting {
				if ev.Gtid == fromGTID && ev.EventSeq >= fromEventSeq {
					emitting = true
				}
			}
			if emitting {
				select {
				case ch <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ch, nil
}

// Delete removes the binlog file for the given table and cleans up any cached
// file handle.
func (lb *LocalBackend) Delete(_ context.Context, tableID string) error {
	tl := lb.tableLock(tableID)
	tl.Lock()
	defer tl.Unlock()

	lb.mu.Lock()
	if f, ok := lb.files[tableID]; ok {
		f.Close()
		delete(lb.files, tableID)
	}
	lb.mu.Unlock()

	if err := os.Remove(lb.filePath(tableID)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove file for table %s: %w", tableID, err)
	}
	return nil
}

// Size returns the size in bytes of the binlog file for the given table.
func (lb *LocalBackend) Size(_ context.Context, tableID string) (int64, error) {
	info, err := os.Stat(lb.filePath(tableID))
	if err != nil {
		return 0, fmt.Errorf("stat file for table %s: %w", tableID, err)
	}
	return info.Size(), nil
}

// TotalSize walks baseDir and sums the sizes of all .binlog files.
func (lb *LocalBackend) TotalSize(_ context.Context) (int64, error) {
	var total int64
	entries, err := os.ReadDir(lb.baseDir)
	if err != nil {
		return 0, fmt.Errorf("read base dir: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".binlog") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return 0, fmt.Errorf("stat %s: %w", entry.Name(), err)
		}
		total += info.Size()
	}
	return total, nil
}

// Exists returns true if the binlog file for the given table exists on disk.
func (lb *LocalBackend) Exists(_ context.Context, tableID string) (bool, error) {
	_, err := os.Stat(lb.filePath(tableID))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("stat file for table %s: %w", tableID, err)
}

// Close closes all open file handles.
func (lb *LocalBackend) Close() error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	var firstErr error
	for tableID, f := range lb.files {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close file for table %s: %w", tableID, err)
		}
		delete(lb.files, tableID)
	}
	return firstErr
}
