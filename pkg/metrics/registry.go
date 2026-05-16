package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	regMu           sync.Mutex
	currentRegistry prometheus.Registerer
)

// MustRegisterAll creates all DataStream metrics and registers them with r.
// Panics if called twice on the same Registerer (duplicate registration).
// Call ResetForTest() first to switch registries.
func MustRegisterAll(r prometheus.Registerer) {
	regMu.Lock()
	defer regMu.Unlock()

	TaskTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: Namespace, Name: "task_total", Help: "Cluster-level task state distribution"},
		[]string{"cluster", "status"},
	)
	TaskState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: Namespace, Name: "task_state", Help: "Per-task current state (0/1)"},
		[]string{"cluster", "task", "state"},
	)
	TaskEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: Namespace, Name: "task_events_total", Help: "Total events processed by task"},
		[]string{"cluster", "task", "type", "result"},
	)
	TaskEventsBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: Namespace, Name: "task_events_bytes", Help: "Total bytes processed by task"},
		[]string{"cluster", "task"},
	)
	TaskLatencySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Namespace: Namespace, Name: "task_latency_seconds", Help: "Event processing latency", Buckets: prometheus.ExponentialBuckets(0.001, 2, 15)},
		[]string{"cluster", "task"},
	)
	SourcePosition = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: Namespace, Name: "source_position", Help: "Current source position (numeric only)"},
		[]string{"cluster", "task", "source"},
	)
	SourceSnapshotProgress = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: Namespace, Name: "source_snapshot_progress", Help: "Snapshot progress 0-100"},
		[]string{"cluster", "task"},
	)
	SourceLagSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: Namespace, Name: "source_lag_seconds", Help: "CDC lag: now - event_time"},
		[]string{"cluster", "task", "source"},
	)
	SourceLastEventSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: Namespace, Name: "source_last_event_seconds", Help: "Unix timestamp of last observed event"},
		[]string{"cluster", "task", "source"},
	)
	SnapshotTablesTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: Namespace, Name: "snapshot_tables_total", Help: "Total tables to snapshot"},
		[]string{"cluster", "task"},
	)
	SnapshotTablesRemaining = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: Namespace, Name: "snapshot_tables_remaining", Help: "Remaining unsnapshot tables"},
		[]string{"cluster", "task"},
	)
	SinkWriteLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Namespace: Namespace, Name: "sink_write_latency_seconds", Help: "Sink write latency", Buckets: prometheus.ExponentialBuckets(0.001, 2, 15)},
		[]string{"cluster", "task", "sink"},
	)
	SinkWriteErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: Namespace, Name: "sink_write_errors_total", Help: "Sink write errors"},
		[]string{"cluster", "task", "sink", "error_type"},
	)
	PipelineQueueSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: Namespace, Name: "pipeline_queue_size", Help: "Pipeline stage queue current size"},
		[]string{"cluster", "task", "stage"},
	)
	PipelineQueueCapacity = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: Namespace, Name: "pipeline_queue_capacity", Help: "Pipeline stage queue capacity"},
		[]string{"cluster", "task", "stage"},
	)
	PipelineProcessTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Namespace: Namespace, Name: "pipeline_process_time_seconds", Help: "Pipeline stage processing time", Buckets: prometheus.ExponentialBuckets(0.0001, 2, 15)},
		[]string{"cluster", "task", "stage"},
	)
	ConnectorConnected = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: Namespace, Name: "connector_connected", Help: "Connector connection health (0/1)"},
		[]string{"cluster", "task", "role", "type"},
	)
	NodeStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Namespace: Namespace, Name: "node_status", Help: "Cluster node status (0/1)"},
		[]string{"node"},
	)
	LeaderStatus = prometheus.NewGauge(
		prometheus.GaugeOpts{Namespace: Namespace, Name: "leader_status", Help: "Whether this node is leader (0/1)"},
	)
	LeaderChanges = prometheus.NewCounter(
		prometheus.CounterOpts{Namespace: Namespace, Name: "leader_changes_total", Help: "Total leader changes"},
	)

	r.MustRegister(
		TaskTotal, TaskState, TaskEventsTotal, TaskEventsBytes, TaskLatencySeconds,
		SourcePosition, SourceSnapshotProgress, SourceLagSeconds, SourceLastEventSeconds,
		SnapshotTablesTotal, SnapshotTablesRemaining,
		SinkWriteLatency, SinkWriteErrors,
		PipelineQueueSize, PipelineQueueCapacity, PipelineProcessTime,
		ConnectorConnected,
		NodeStatus, LeaderStatus, LeaderChanges,
	)

	currentRegistry = r
}

// ResetForTest unregisters all metrics from the current registry and clears
// package vars. Intended for tests; subsequent MustRegisterAll(...) reinstalls.
func ResetForTest() {
	regMu.Lock()
	defer regMu.Unlock()

	if currentRegistry != nil {
		all := []prometheus.Collector{
			TaskTotal, TaskState, TaskEventsTotal, TaskEventsBytes, TaskLatencySeconds,
			SourcePosition, SourceSnapshotProgress, SourceLagSeconds, SourceLastEventSeconds,
			SnapshotTablesTotal, SnapshotTablesRemaining,
			SinkWriteLatency, SinkWriteErrors,
			PipelineQueueSize, PipelineQueueCapacity, PipelineProcessTime,
			ConnectorConnected,
			NodeStatus, LeaderStatus, LeaderChanges,
		}
		for _, c := range all {
			if c != nil {
				currentRegistry.Unregister(c)
			}
		}
	}
	TaskTotal, TaskState, TaskEventsTotal, TaskEventsBytes, TaskLatencySeconds = nil, nil, nil, nil, nil
	SourcePosition, SourceSnapshotProgress, SourceLagSeconds, SourceLastEventSeconds = nil, nil, nil, nil
	SnapshotTablesTotal, SnapshotTablesRemaining = nil, nil
	SinkWriteLatency, SinkWriteErrors = nil, nil
	PipelineQueueSize, PipelineQueueCapacity, PipelineProcessTime = nil, nil, nil
	ConnectorConnected = nil
	NodeStatus = nil
	LeaderStatus = nil
	LeaderChanges = nil
	currentRegistry = nil
}

// init registers all metrics with the default Prometheus registry on package load.
func init() {
	MustRegisterAll(prometheus.DefaultRegisterer)
}
