package event

import (
	"fmt"
	"time"
)

// HeartbeatEvent represents a heartbeat message.
type HeartbeatEvent struct {
	Source    SourceInfo `json:"source"`
	Timestamp time.Time  `json:"timestamp"`
	Position  Position   `json:"position"`
}

// ToChangeEvent converts the heartbeat to a ChangeEvent.
func (h *HeartbeatEvent) ToChangeEvent() *ChangeEvent {
	return &ChangeEvent{
		ID:        fmt.Sprintf("%s:heartbeat:%d", h.Source.Connector, h.Timestamp.UnixNano()),
		Source:    h.Source,
		Type:      EventTypeHeartbeat,
		Position:  h.Position,
		Timestamp: h.Timestamp,
	}
}

// NewHeartbeat creates a new heartbeat event.
func NewHeartbeat(source SourceInfo, pos Position) *HeartbeatEvent {
	return &HeartbeatEvent{
		Source:    source,
		Timestamp: time.Now(),
		Position:  pos,
	}
}
