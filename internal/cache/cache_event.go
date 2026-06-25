package cache

import (
	"encoding/json"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

// SourceType identifies the upstream database type.
type SourceType int

const (
	SourceTypeUnspecified SourceType = iota
	SourceTypeMySQLGTID
	SourceTypeMySQLFilePos
	SourceTypePostgres
	SourceTypeOracle
	SourceTypeSQLServer
	SourceTypeMongoDB
)

// CacheEvent is the on-disk representation of a buffered change event.
// Serialized as JSON (compact, no protobuf dependency).
type CacheEvent struct {
	SourceType SourceType `json:"source_type"`
	Position   []byte     `json:"position,omitempty"` // serialized event.Position JSON
	TxID       string     `json:"tx_id,omitempty"`
	EventSeq   int64      `json:"event_seq"`
	IsBegin    bool       `json:"is_begin,omitempty"`
	IsCommit   bool       `json:"is_commit,omitempty"`
	Payload    []byte     `json:"payload,omitempty"`
	TimestampMs int64     `json:"timestamp_ms"`
	ByteOffset uint64     `json:"byte_offset,omitempty"` // MySQL non-GTID
	SeqVal     string     `json:"seq_val,omitempty"`     // SQL Server
}

// NewCacheEvent creates a CacheEvent with timestamp set to now.
func NewCacheEvent() *CacheEvent {
	return &CacheEvent{
		TimestampMs: time.Now().UnixMilli(),
	}
}

// SetPosition serializes and stores the position.
func (e *CacheEvent) SetPosition(pos *event.Position) error {
	data, err := json.Marshal(pos)
	if err != nil {
		return err
	}
	e.Position = data
	return nil
}

// GetPosition deserializes the stored position.
func (e *CacheEvent) GetPosition() (*event.Position, error) {
	if len(e.Position) == 0 {
		return nil, nil
	}
	var pos event.Position
	if err := json.Unmarshal(e.Position, &pos); err != nil {
		return nil, err
	}
	return &pos, nil
}

// Marshal serializes the CacheEvent to JSON bytes.
func (e *CacheEvent) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

// UnmarshalCacheEvent deserializes JSON bytes to a CacheEvent.
func UnmarshalCacheEvent(data []byte) (*CacheEvent, error) {
	var ev CacheEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return nil, err
	}
	return &ev, nil
}
