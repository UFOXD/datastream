package event

import (
	"encoding/json"
	"fmt"
	"time"
)

// Position records the data position for recovery.
type Position struct {
	// Binary log position (MySQL/MariaDB)
	BinlogFile string `json:"binlogFile,omitempty"`
	BinlogPos  uint32 `json:"binlogPos,omitempty"`

	// LSN position (PostgreSQL)
	LSN uint64 `json:"lsn,omitempty"`

	// SCN position (Oracle)
	SCN uint64 `json:"scn,omitempty"`

	// Oplog/Timestamp position (MongoDB)
	Timestamp uint64 `json:"timestamp,omitempty"`
	Order     int    `json:"order,omitempty"`

	// SQL Server position
	ChangeLsn string `json:"changeLsn,omitempty"`

	// Generic timestamp (for comparison)
	CommitTime time.Time `json:"commitTime"`

	// Transaction sequence
	TxID  string `json:"txId,omitempty"`
	SeqNo int    `json:"seqNo,omitempty"` // Sequence within transaction
	Total int    `json:"total,omitempty"` // Total events in transaction
}

// Compare compares two Positions.
// Returns: -1 if p < other, 0 if p == other, 1 if p > other
func (p *Position) Compare(other *Position) (int, error) {
	// Compare by commit time
	if p.CommitTime.Before(other.CommitTime) {
		return -1, nil
	}
	if p.CommitTime.After(other.CommitTime) {
		return 1, nil
	}

	// Same time, compare by sequence number within transaction
	if p.SeqNo < other.SeqNo {
		return -1, nil
	}
	if p.SeqNo > other.SeqNo {
		return 1, nil
	}

	return 0, nil
}

// MarshalBinary serializes Position to bytes.
func (p *Position) MarshalBinary() ([]byte, error) {
	return json.Marshal(p)
}

// UnmarshalBinary deserializes Position from bytes.
func (p *Position) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, p)
}

// String returns a string representation of the Position.
func (p *Position) String() string {
	if p.TxID != "" {
		return fmt.Sprintf("%s:%d:%d", p.TxID, p.SeqNo, p.Total)
	}
	return fmt.Sprintf("%s:%d", p.CommitTime.Format("20060102150405"), p.SeqNo)
}

// IsZero returns true if the position is not set.
func (p *Position) IsZero() bool {
	return p.CommitTime.IsZero() && p.BinlogFile == "" && p.LSN == 0 &&
		p.SCN == 0 && p.Timestamp == 0 && p.ChangeLsn == ""
}

// Clone returns a deep copy of the Position.
func (p *Position) Clone() *Position {
	return &Position{
		BinlogFile: p.BinlogFile,
		BinlogPos:  p.BinlogPos,
		LSN:        p.LSN,
		SCN:        p.SCN,
		Timestamp:  p.Timestamp,
		Order:      p.Order,
		ChangeLsn:  p.ChangeLsn,
		CommitTime: p.CommitTime,
		TxID:       p.TxID,
		SeqNo:      p.SeqNo,
		Total:      p.Total,
	}
}
