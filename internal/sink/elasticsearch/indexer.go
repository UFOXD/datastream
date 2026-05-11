package elasticsearch

import (
	"encoding/json"
	"sync"
)

// BulkIndexer buffers BulkActions and builds Elasticsearch Bulk API request bodies.
// It is safe for concurrent use.
//
// Lifecycle (goroutine-based flushing) is the responsibility of the connector;
// BulkIndexer itself only manages the buffer and serialisation.
type BulkIndexer struct {
	config   *Config
	buffer   []*BulkAction
	bufferMu sync.Mutex
	stopCh   chan struct{}
	flushCh  chan struct{}
}

// BulkAction represents a single Elasticsearch bulk API action.
// It is defined in mapper.go (shared within the package).

// NewBulkIndexer creates a new BulkIndexer with the given config.
func NewBulkIndexer(config *Config) *BulkIndexer {
	return &BulkIndexer{
		config:  config,
		buffer:  make([]*BulkAction, 0),
		stopCh:  make(chan struct{}),
		flushCh: make(chan struct{}, 1),
	}
}

// Add appends action to the buffer. If the buffer reaches BatchSize it signals
// the flush channel (non-blocking) so a background goroutine can drain it.
func (b *BulkIndexer) Add(action *BulkAction) error {
	b.bufferMu.Lock()
	b.buffer = append(b.buffer, action)
	shouldFlush := len(b.buffer) >= b.config.BatchSize
	b.bufferMu.Unlock()

	if shouldFlush {
		select {
		case b.flushCh <- struct{}{}:
		default:
		}
	}
	return nil
}

// BufferSize returns the current number of buffered actions.
func (b *BulkIndexer) BufferSize() int {
	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()
	return len(b.buffer)
}

// Clear discards all buffered actions.
func (b *BulkIndexer) Clear() {
	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()
	b.buffer = b.buffer[:0]
}

// GetAndClear atomically returns the current buffer contents and resets the
// buffer to empty. The returned slice is owned by the caller.
func (b *BulkIndexer) GetAndClear() []*BulkAction {
	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()

	if len(b.buffer) == 0 {
		return nil
	}
	actions := b.buffer
	b.buffer = make([]*BulkAction, 0, cap(actions))
	return actions
}

// ShouldFlush reports whether the buffer has reached (or exceeded) BatchSize.
func (b *BulkIndexer) ShouldFlush() bool {
	b.bufferMu.Lock()
	defer b.bufferMu.Unlock()
	return len(b.buffer) >= b.config.BatchSize
}

// BuildRequestBody serialises actions into an Elasticsearch Bulk API ND-JSON
// body. The format is:
//
//	index:  {"index":{"_index":"…","_id":"…"}}\n{doc}\n
//	update: {"update":{"_index":"…","_id":"…","retry_on_conflict":N}}\n{"doc":{…},"doc_as_upsert":true}\n
//	delete: {"delete":{"_index":"…","_id":"…"}}\n
//
// The returned slice is nil when actions is empty.
func (b *BulkIndexer) BuildRequestBody(actions []*BulkAction) []byte {
	if len(actions) == 0 {
		return nil
	}

	// Pre-allocate a reasonable buffer (rough estimate: ~200 bytes per action pair).
	buf := make([]byte, 0, len(actions)*200)

	for _, a := range actions {
		switch a.Op {
		case "index":
			buf = b.appendLine(buf, map[string]interface{}{
				"index": map[string]interface{}{
					"_index": a.Index,
					"_id":    a.ID,
				},
			})
			buf = b.appendLine(buf, a.Doc)

		case "update":
			buf = b.appendLine(buf, map[string]interface{}{
				"update": map[string]interface{}{
					"_index":            a.Index,
					"_id":               a.ID,
					"retry_on_conflict": b.config.RetryOnConflict,
				},
			})
			buf = b.appendLine(buf, map[string]interface{}{
				"doc":          a.Doc,
				"doc_as_upsert": true,
			})

		case "delete":
			buf = b.appendLine(buf, map[string]interface{}{
				"delete": map[string]interface{}{
					"_index": a.Index,
					"_id":    a.ID,
				},
			})
		}
	}

	return buf
}

// appendLine marshals v to JSON, appends it and a newline to buf, and returns
// the extended slice. Marshalling errors are silently ignored (should never
// occur with map[string]interface{} inputs).
func (b *BulkIndexer) appendLine(buf []byte, v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		return buf
	}
	buf = append(buf, data...)
	buf = append(buf, '\n')
	return buf
}
