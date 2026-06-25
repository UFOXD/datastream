package cache

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/UFOXD/datastream/pkg/event"
)

// Compile-time check: LocalBackend implements BinlogCacheBackend.
var _ BinlogCacheBackend = (*LocalBackend)(nil)

var sanitizeRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

const (
	// MaxRecordSize is the maximum allowed record size (64MB).
	MaxRecordSize uint32 = 64 * 1024 * 1024
	// recordOverhead is the fixed overhead per record: 4B header len + 4B CRC32 + 4B tail len.
	recordOverhead = 12
)

// record layout: [4B len][4B CRC32][N bytes payload][4B len]
// CRC32 covers [4B len][N bytes payload]

// LocalBackend is a filesystem-based BinlogCacheBackend. Each table ID maps to
// a single append-only file under baseDir.
type LocalBackend struct {
	baseDir string
	syncMode SyncMode
	files   map[string]*os.File
	locks   map[string]*sync.Mutex
	mu      sync.Mutex
}

// NewLocalBackend creates a LocalBackend rooted at baseDir.
func NewLocalBackend(baseDir string, syncMode SyncMode) (*LocalBackend, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create base dir: %w", err)
	}
	return &LocalBackend{
		baseDir:  baseDir,
		syncMode: syncMode,
		files:    make(map[string]*os.File),
		locks:    make(map[string]*sync.Mutex),
	}, nil
}

func sanitizeTableID(tableID string) string {
	return sanitizeRe.ReplaceAllString(tableID, "_")
}

func (lb *LocalBackend) filePath(tableID string) string {
	return filepath.Join(lb.baseDir, sanitizeTableID(tableID)+".binlog")
}

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

// marshalAndWrite marshals a CacheEvent and writes it with the new record format.
// Caller must hold the per-table lock.
func (lb *LocalBackend) marshalAndWrite(f *os.File, ev *CacheEvent) error {
	payload, err := ev.Marshal()
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	payloadLen := uint32(len(payload))
	totalLen := payloadLen + uint32(recordOverhead) // 4B header + 4B crc + payload + 4B tail

	// Build record: [4B len][4B CRC][payload][4B len]
	// CRC32 covers [4B len || payload]
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], payloadLen)
	crcInput := append(lenBuf[:], payload...)
	crc := crc32.ChecksumIEEE(crcInput)

	buf := make([]byte, totalLen)
	binary.BigEndian.PutUint32(buf[0:4], payloadLen)
	binary.BigEndian.PutUint32(buf[4:8], crc)
	copy(buf[8:8+payloadLen], payload)
	binary.BigEndian.PutUint32(buf[8+payloadLen:], payloadLen) // tail length

	if _, err := f.Write(buf); err != nil {
		return fmt.Errorf("write record: %w", err)
	}
	return nil
}

func (lb *LocalBackend) maybeSync(f *os.File) error {
	if lb.syncMode == SyncModeEvery {
		return f.Sync()
	}
	return nil
}

// Write appends a single CacheEvent to the table's buffer file.
func (lb *LocalBackend) Write(_ context.Context, tableID string, ev *CacheEvent) error {
	tl := lb.tableLock(tableID)
	tl.Lock()
	defer tl.Unlock()

	f, err := lb.getFile(tableID)
	if err != nil {
		return err
	}

	if err := lb.marshalAndWrite(f, ev); err != nil {
		return err
	}
	return lb.maybeSync(f)
}

// WriteBatch atomically writes a batch of CacheEvents.
func (lb *LocalBackend) WriteBatch(_ context.Context, tableID string, events []*CacheEvent) error {
	tl := lb.tableLock(tableID)
	tl.Lock()
	defer tl.Unlock()

	f, err := lb.getFile(tableID)
	if err != nil {
		return err
	}

	for _, ev := range events {
		if err := lb.marshalAndWrite(f, ev); err != nil {
			return err
		}
	}

	// Batch mode: always sync after a batch.
	if lb.syncMode != SyncModeNone {
		return f.Sync()
	}
	return nil
}

// Read streams CacheEvents from the table's buffer file.
func (lb *LocalBackend) Read(ctx context.Context, tableID string, fromTxID string, fromEventSeq int64) ReadResult {
	fp := lb.filePath(tableID)
	ch := make(chan *CacheEvent, 64)
	errCh := make(chan error, 1)

	go func() {
		defer close(ch)
		defer close(errCh)

		f, err := os.Open(fp)
		if err != nil {
			if os.IsNotExist(err) {
				return // no file = no events
			}
			errCh <- fmt.Errorf("open file for reading table %s: %w", tableID, err)
			return
		}
		defer f.Close()

		emitting := fromTxID == ""
		var lenBuf [4]byte
		var crcBuf [4]byte
		var tailBuf [4]byte

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Read header length.
			if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
				if err != io.EOF && err != io.ErrUnexpectedEOF {
					errCh <- fmt.Errorf("read length prefix: %w", err)
				}
				return
			}
			payloadLen := binary.BigEndian.Uint32(lenBuf[:])
			if payloadLen > MaxRecordSize {
				errCh <- fmt.Errorf("record too large: %d bytes (max %d)", payloadLen, MaxRecordSize)
				return
			}

			// Read CRC32.
			if _, err := io.ReadFull(f, crcBuf[:]); err != nil {
				errCh <- fmt.Errorf("read crc: %w", err)
				return
			}

			// Read payload.
			payload := make([]byte, payloadLen)
			if _, err := io.ReadFull(f, payload); err != nil {
				errCh <- fmt.Errorf("read payload: %w", err)
				return
			}

			// Read tail length.
			if _, err := io.ReadFull(f, tailBuf[:]); err != nil {
				errCh <- fmt.Errorf("read tail length: %w", err)
				return
			}
			tailLen := binary.BigEndian.Uint32(tailBuf[:])
			if tailLen != payloadLen {
				errCh <- fmt.Errorf("tail length mismatch: header=%d tail=%d", payloadLen, tailLen)
				return
			}

			// Verify CRC32.
			expectedCRC := binary.BigEndian.Uint32(crcBuf[:])
			actualCRC := crc32.ChecksumIEEE(append(lenBuf[:], payload...))
			if expectedCRC != actualCRC {
				errCh <- fmt.Errorf("CRC32 mismatch: expected %08x got %08x", expectedCRC, actualCRC)
				return
			}

			// Unmarshal.
			ev, err := UnmarshalCacheEvent(payload)
			if err != nil {
				errCh <- fmt.Errorf("unmarshal event: %w", err)
				return
			}

			if !emitting {
				if ev.TxID == fromTxID && ev.EventSeq >= fromEventSeq {
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

	return ReadResult{Events: ch, Err: errCh}
}

// Delete removes the binlog file for the given table.
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

// Sync forces fsync on the table's buffer file.
func (lb *LocalBackend) Sync(_ context.Context, tableID string) error {
	lb.mu.Lock()
	f, ok := lb.files[tableID]
	lb.mu.Unlock()
	if !ok {
		return nil // no file open, nothing to sync
	}
	return f.Sync()
}

// TruncateToLastComplete scans the table's buffer file from the tail,
// finds the last complete record, and truncates everything after it.
func (lb *LocalBackend) TruncateToLastComplete(_ context.Context, tableID string) (*event.Position, error) {
	fp := lb.filePath(tableID)
	f, err := os.OpenFile(fp, os.O_RDWR, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}
	fileSize := stat.Size()

	if fileSize == 0 {
		return nil, nil
	}

	var tailBuf [4]byte
	var headerBuf [4]byte

	// Scan backwards from end of file, trying each position as a potential tail.
	for fileSize >= int64(recordOverhead) {
		// Read potential tail length.
		if _, err := f.ReadAt(tailBuf[:], fileSize-4); err != nil {
			fileSize--
			continue
		}
		payloadLen := binary.BigEndian.Uint32(tailBuf[:])
		if payloadLen > MaxRecordSize {
			fileSize--
			continue
		}

		recordSize := int64(payloadLen) + int64(recordOverhead)
		if recordSize > fileSize {
			fileSize--
			continue
		}

		// Read header length.
		recordStart := fileSize - recordSize
		if _, err := f.ReadAt(headerBuf[:], recordStart); err != nil {
			fileSize--
			continue
		}
		headerLen := binary.BigEndian.Uint32(headerBuf[:])
		if headerLen != payloadLen {
			// Not a valid record boundary, step back.
			fileSize--
			continue
		}

		// Read the full record for CRC check.
		recordBuf := make([]byte, recordSize)
		if _, err := f.ReadAt(recordBuf, recordStart); err != nil {
			fileSize--
			continue
		}

		storedCRC := binary.BigEndian.Uint32(recordBuf[4:8])
		actualCRC := crc32.ChecksumIEEE(recordBuf[0 : 4+payloadLen])
		if storedCRC != actualCRC {
			fileSize--
			continue
		}

		// Valid record found.
		payload := recordBuf[8 : 8+payloadLen]
		ev, err := UnmarshalCacheEvent(payload)
		if err != nil {
			fileSize--
			continue
		}

		pos, _ := ev.GetPosition()

		// Truncate to end of this valid record.
		validEnd := recordStart + recordSize
		if err := f.Truncate(validEnd); err != nil {
			return nil, fmt.Errorf("truncate: %w", err)
		}

		// Invalidate cached file handle.
		lb.mu.Lock()
		if oldF, ok := lb.files[tableID]; ok {
			oldF.Close()
			delete(lb.files, tableID)
		}
		lb.mu.Unlock()

		return pos, nil
	}

	// No valid records found, truncate to empty.
	if err := f.Truncate(0); err != nil {
		return nil, err
	}
	return nil, nil
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
