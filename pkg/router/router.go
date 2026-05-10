// Package router provides event routing capabilities for DataStream.
package router

import (
	"github.com/UFOXD/datastream/pkg/event"
)

// Router is the interface for event routers.
// Routers determine where events should be sent.
type Router interface {
	// Route calculates the routing destination for an event.
	// Returns the target Sink ID or partition ID.
	Route(e *event.ChangeEvent) (string, error)
}

// RouterConfig holds the configuration for routers.
type RouterConfig struct {
	// TableMapping maps table names to sink IDs (TableRouter only).
	// Format: "database.table" -> "sink_id"
	TableMapping map[string]string `json:"table-mapping" toml:"table-mapping"`

	// DefaultSink is the default sink ID for unmapped tables (TableRouter only).
	DefaultSink string `json:"default-sink" toml:"default-sink"`

	// PartitionStrategy is the partition selection strategy (PartitionRouter only).
	PartitionStrategy PartitionStrategy `json:"partition-strategy" toml:"partition-strategy"`

	// PartitionCount is the total number of partitions (PartitionRouter only).
	PartitionCount int `json:"partition-count" toml:"partition-count"`

	// PartitionKey specifies the fields for PartitionByField strategy (PartitionRouter only).
	PartitionKey []string `json:"partition-key" toml:"partition-key"`
}
