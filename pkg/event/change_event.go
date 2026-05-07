// Package event defines the unified event model for DataStream.
package event

import (
	"fmt"
	"time"
)

// EventType represents the type of a change event.
type EventType string

const (
	EventTypeInsert    EventType = "insert"    // Insert operation
	EventTypeUpdate    EventType = "update"    // Update operation
	EventTypeDelete    EventType = "delete"    // Delete operation
	EventTypeTruncate  EventType = "truncate"  // Table truncation
	EventTypeDDL       EventType = "ddl"       // DDL statement
	EventTypeHeartbeat EventType = "heartbeat" // Heartbeat message
	EventTypeTombstone EventType = "tombstone" // Tombstone message for cleanup
)

// ChangeEvent represents a single data change event.
type ChangeEvent struct {
	// Unique identifier
	ID string `json:"id"` // Format: {source}:{timestamp}:{sequence}

	// Event source
	Source SourceInfo `json:"source"`

	// Event type
	Type EventType `json:"type"`

	// Data position
	Position Position `json:"position"`

	// Table information
	Table TableInfo `json:"table"`

	// Change data
	Before RowData `json:"before,omitempty"` // Before state (UPDATE/DELETE)
	After  RowData `json:"after,omitempty"`  // After state (INSERT/UPDATE)

	// Timestamp
	Timestamp time.Time `json:"timestamp"`

	// Transaction information
	Transaction *TransactionInfo `json:"transaction,omitempty"`

	// Schema information
	Schema *SchemaInfo `json:"schema,omitempty"`

	// Metadata
	Metadata map[string]string `json:"metadata,omitempty"`
}

// GenerateEventID generates a unique event ID.
func GenerateEventID(source *SourceInfo, timestamp time.Time, seqNo int) string {
	return fmt.Sprintf("%s:%s:%s:%d",
		source.Connector,
		source.Database,
		timestamp.Format("20060102150405.000"),
		seqNo,
	)
}

// IsSnapshot returns true if this event is from a snapshot.
func (e *ChangeEvent) IsSnapshot() bool {
	return e.Source.Snapshot
}

// IsDDL returns true if this is a DDL event.
func (e *ChangeEvent) IsDDL() bool {
	return e.Type == EventTypeDDL
}

// IsHeartbeat returns true if this is a heartbeat event.
func (e *ChangeEvent) IsHeartbeat() bool {
	return e.Type == EventTypeHeartbeat
}

// IsDataEvent returns true if this is a data change event.
func (e *ChangeEvent) IsDataEvent() bool {
	return e.Type == EventTypeInsert ||
		e.Type == EventTypeUpdate ||
		e.Type == EventTypeDelete
}
