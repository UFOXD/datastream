package event

import "time"

// DDLType represents the type of DDL operation.
type DDLType string

const (
	DDLTypeCreate   DDLType = "create"
	DDLTypeAlter    DDLType = "alter"
	DDLTypeDrop     DDLType = "drop"
	DDLTypeRename   DDLType = "rename"
	DDLTypeTruncate DDLType = "truncate"
)

// DDLEvent represents a DDL change event.
type DDLEvent struct {
	ChangeEvent
	DDL DDLInfo `json:"ddl"`
}

// DDLInfo holds DDL information.
type DDLInfo struct {
	Type      DDLType `json:"type"`      // CREATE, ALTER, DROP, RENAME, TRUNCATE
	Statement string  `json:"statement"` // Original DDL statement
	Database  string  `json:"database"`
	Table     string  `json:"table,omitempty"`
}

// NewDDLEvent creates a new DDL event.
func NewDDLEvent(source SourceInfo, ddl DDLInfo, pos Position, stmt string) *DDLEvent {
	return &DDLEvent{
		ChangeEvent: ChangeEvent{
			ID:        GenerateEventID(&source, time.Now(), 0),
			Source:    source,
			Type:      EventTypeDDL,
			Position:  pos,
			Timestamp: time.Now(),
		},
		DDL: ddl,
	}
}

// IsCreate returns true if this is a CREATE statement.
func (d *DDLEvent) IsCreate() bool {
	return d.DDL.Type == DDLTypeCreate
}

// IsAlter returns true if this is an ALTER statement.
func (d *DDLEvent) IsAlter() bool {
	return d.DDL.Type == DDLTypeAlter
}

// IsDrop returns true if this is a DROP statement.
func (d *DDLEvent) IsDrop() bool {
	return d.DDL.Type == DDLTypeDrop
}
