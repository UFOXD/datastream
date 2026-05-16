// Package metrics provides Prometheus metrics for DataStream.
//
// Metrics are declared as package-level vars but NOT initialized at import.
// Use MustRegisterAll(r) to create and register them. The package init()
// calls MustRegisterAll(prometheus.DefaultRegisterer) for normal operation;
// tests can call ResetForTest() + MustRegisterAll(newRegistry) for isolation.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Namespace is the Prometheus metric name prefix.
const Namespace = "datastream"

// Task metrics.
var (
	TaskTotal          *prometheus.GaugeVec   // cluster-level state distribution
	TaskState          *prometheus.GaugeVec   // per-task state 0/1 (new)
	TaskEventsTotal    *prometheus.CounterVec // per-task event counter (now with result)
	TaskEventsBytes    *prometheus.CounterVec
	TaskLatencySeconds *prometheus.HistogramVec
)

// Source metrics.
var (
	SourcePosition          *prometheus.GaugeVec
	SourceSnapshotProgress  *prometheus.GaugeVec
	SourceLagSeconds        *prometheus.GaugeVec // new
	SourceLastEventSeconds  *prometheus.GaugeVec // new
	SnapshotTablesTotal     *prometheus.GaugeVec // new
	SnapshotTablesRemaining *prometheus.GaugeVec // new
)

// Sink metrics.
var (
	SinkWriteLatency *prometheus.HistogramVec
	SinkWriteErrors  *prometheus.CounterVec
)

// Pipeline metrics.
var (
	PipelineQueueSize     *prometheus.GaugeVec
	PipelineQueueCapacity *prometheus.GaugeVec // new
	PipelineProcessTime   *prometheus.HistogramVec
)

// Connector metrics.
var (
	ConnectorConnected *prometheus.GaugeVec // new
)

// Cluster metrics.
var (
	NodeStatus    *prometheus.GaugeVec
	LeaderStatus  prometheus.Gauge
	LeaderChanges prometheus.Counter
)
