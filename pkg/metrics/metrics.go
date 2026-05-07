// Package metrics provides Prometheus metrics for DataStream.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Namespace is the Prometheus namespace for all DataStream metrics.
const Namespace = "datastream"

// Task metrics.
var (
	TaskTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "task_total",
			Help:      "Total number of tasks",
		},
		[]string{"namespace", "status"},
	)

	TaskEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "task_events_total",
			Help:      "Total number of events processed by task",
		},
		[]string{"namespace", "task", "type"},
	)

	TaskEventsBytes = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "task_events_bytes",
			Help:      "Total bytes of events processed by task",
		},
		[]string{"namespace", "task"},
	)

	TaskLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "task_latency_seconds",
			Help:      "Latency of event processing in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms ~ 16s
		},
		[]string{"namespace", "task"},
	)
)

// Source metrics.
var (
	SourcePosition = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "source_position",
			Help:      "Current position of source",
		},
		[]string{"namespace", "task", "source"},
	)

	SourceSnapshotProgress = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "source_snapshot_progress",
			Help:      "Snapshot progress percentage (0-100)",
		},
		[]string{"namespace", "task"},
	)
)

// Sink metrics.
var (
	SinkWriteLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "sink_write_latency_seconds",
			Help:      "Latency of sink write operations",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
		},
		[]string{"namespace", "task", "sink"},
	)

	SinkWriteErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "sink_write_errors_total",
			Help:      "Total number of sink write errors",
		},
		[]string{"namespace", "task", "sink", "error_type"},
	)
)

// Pipeline metrics.
var (
	PipelineQueueSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "pipeline_queue_size",
			Help:      "Current size of pipeline queue",
		},
		[]string{"namespace", "task", "stage"},
	)

	PipelineProcessTime = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "pipeline_process_time_seconds",
			Help:      "Time spent in pipeline stage",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15), // 0.1ms ~ 1.6s
		},
		[]string{"namespace", "task", "stage"},
	)
)

// Cluster metrics.
var (
	NodeStatus = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "node_status",
			Help:      "Status of cluster node (1=online, 0=offline)",
		},
		[]string{"node"},
	)

	LeaderStatus = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "leader_status",
			Help:      "Whether current node is leader (1=yes, 0=no)",
		},
	)

	LeaderChanges = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "leader_changes_total",
			Help:      "Total number of leader changes",
		},
	)
)
