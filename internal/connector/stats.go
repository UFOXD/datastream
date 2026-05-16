// Package connector defines interfaces shared by source and sink connectors.
package connector

import (
	"context"
	"time"
)

// StatsProvider is an optional interface that source/sink connectors MAY
// implement to expose runtime state for Prometheus gauge metrics.
// Connectors that don't implement this interface are skipped by the stats
// collector (StatsCollector logs which connectors do/don't support stats at startup).
type StatsProvider interface {
	// Stats returns a snapshot of the connector's runtime state.
	// Implementations MUST:
	//   - be safe for concurrent calls
	//   - return promptly; honor ctx for cancellation
	//   - return zero values for fields that are "not applicable"
	//   - NOT panic; collector recovers and skips this sample on panic
	Stats(ctx context.Context) Stats
}

// Stats holds a snapshot of a connector's runtime state.
// Zero values mean "not applicable" for this connector type.
type Stats struct {
	// Queue
	QueueSize     int64
	QueueCapacity int64

	// Position - opaque string. Format varies by connector. For MySQL/MariaDB,
	// implementations MUST honor the connector's binlog mode (file-pos OR GTID
	// set); do NOT hard-code one format.
	Position string

	// Lag & progress
	LagSeconds       float64   // now - event_time. NaN if unknown. Clamp to 0 if negative.
	LastEventTime    time.Time // zero if no event observed yet
	SnapshotRunning  bool
	SnapshotProgress float64 // 0-100
	SnapshotTotalTables     int64
	SnapshotRemainingTables int64

	// Connection
	Connected bool
}
