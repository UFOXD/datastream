package event

import (
	"time"
)

// DDLStatus represents the application state of a DDL statement.
type DDLStatus string

const (
	DDLStatusPending   DDLStatus = "pending"
	DDLStatusApplying  DDLStatus = "applying"
	DDLStatusCompleted DDLStatus = "completed"
	DDLStatusFailed    DDLStatus = "failed"
	DDLStatusSkipped   DDLStatus = "skipped"
)

// DDLRecord tracks the lifecycle of a DDL statement from parsing to application.
type DDLRecord struct {
	ID           string      `json:"id"`
	Position     *Position   `json:"position"`
	Database     string      `json:"database"`
	Table        string      `json:"table"`
	DDL          string      `json:"ddl"`
	NewTableInfo *TableInfo  `json:"newTableInfo,omitempty"` // nil for DROP
	Status       DDLStatus   `json:"status"`
	AppliedAt    *time.Time  `json:"appliedAt,omitempty"`
	CompletedAt  *time.Time  `json:"completedAt,omitempty"`
	Error        string      `json:"error,omitempty"`
	RetryCount   int         `json:"retryCount"`
	CreatedAt    time.Time   `json:"createdAt"`
}

// SchemaHistoryRecord is a single entry in the Schema History chain.
// Only completed or skipped DDLs are recorded.
type SchemaHistoryRecord struct {
	Position   Position  `json:"position"`
	Database   string    `json:"database"`
	Schema     string    `json:"schema,omitempty"`
	Table      string    `json:"table"`
	DDL        string    `json:"ddl"`
	TableInfo  *TableInfo `json:"tableInfo,omitempty"` // nil for DROP
	ChangeType string    `json:"changeType"`           // CREATE / ALTER / DROP
	DDLStatus  DDLStatus `json:"ddlStatus"`            // completed / skipped
	Timestamp  time.Time `json:"timestamp"`
}
