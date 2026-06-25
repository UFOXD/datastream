package event

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
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
	Timestamp    uint64 `json:"timestamp,omitempty"`
	Order        int    `json:"order,omitempty"`
	ResumeToken  []byte `json:"resumeToken,omitempty"` // MongoDB resume token (opaque BSON)

	// SQL Server position
	ChangeLsn string `json:"changeLsn,omitempty"`
	SeqVal    string `json:"seqVal,omitempty"` // SQL Server seqval for precise skip

	// Generic timestamp (for comparison)
	CommitTime time.Time `json:"commitTime"`

	// Transaction sequence
	TxID  string `json:"txId,omitempty"`
	SeqNo int    `json:"seqNo,omitempty"` // Sequence within transaction
	Total int    `json:"total,omitempty"` // Total events in transaction
}

// Compare compares two Positions by source-specific fields.
// Only for sources with operation-level position precision (PG, Oracle, SQLServer, MySQL non-GTID).
// MySQL GTID uses gtid_set membership; MongoDB uses SetResumeAfter — neither uses Compare.
// Returns: -1 if p < other, 0 if p == other, 1 if p > other.
func (p *Position) Compare(other *Position, sourceType SourceType) (int, error) {
	switch sourceType {
	case SourceTypePostgres:
		return compareUint64(p.LSN, other.LSN), nil
	case SourceTypeOracle:
		return compareUint64(p.SCN, other.SCN), nil
	case SourceTypeSQLServer:
		if c := strings.Compare(p.ChangeLsn, other.ChangeLsn); c != 0 {
			return c, nil
		}
		return strings.Compare(p.SeqVal, other.SeqVal), nil
	case SourceTypeMySQLFilePos:
		if c := strings.Compare(p.BinlogFile, other.BinlogFile); c != 0 {
			return c, nil
		}
		return compareUint64(uint64(p.BinlogPos), uint64(other.BinlogPos)), nil
	case SourceTypeMySQLGTID:
		return 0, fmt.Errorf("MySQL GTID mode does not use Compare(); use gtid_set membership instead")
	case SourceTypeMongoDB:
		return 0, fmt.Errorf("MongoDB does not use Compare(); use SetResumeAfter() instead")
	default:
		return 0, fmt.Errorf("unsupported source type for comparison: %d", sourceType)
	}
}

func compareUint64(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
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
		p.SCN == 0 && p.Timestamp == 0 && p.ChangeLsn == "" && p.SeqVal == "" &&
		len(p.ResumeToken) == 0
}

// Clone returns a deep copy of the Position.
func (p *Position) Clone() *Position {
	var rt []byte
	if p.ResumeToken != nil {
		rt = make([]byte, len(p.ResumeToken))
		copy(rt, p.ResumeToken)
	}
	return &Position{
		BinlogFile:  p.BinlogFile,
		BinlogPos:   p.BinlogPos,
		LSN:         p.LSN,
		SCN:         p.SCN,
		Timestamp:   p.Timestamp,
		Order:       p.Order,
		ResumeToken: rt,
		ChangeLsn:   p.ChangeLsn,
		SeqVal:      p.SeqVal,
		CommitTime:  p.CommitTime,
		TxID:        p.TxID,
		SeqNo:       p.SeqNo,
		Total:       p.Total,
	}
}
