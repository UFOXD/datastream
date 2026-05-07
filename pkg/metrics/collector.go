package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ObserveTaskLatency records a task event processing latency sample.
func ObserveTaskLatency(ns, task string, d time.Duration) {
	TaskLatencySeconds.With(prometheus.Labels{
		"namespace": ns,
		"task":      task,
	}).Observe(d.Seconds())
}

// ObserveSinkWriteLatency records a sink write latency sample.
func ObserveSinkWriteLatency(ns, task, sink string, d time.Duration) {
	SinkWriteLatency.With(prometheus.Labels{
		"namespace": ns,
		"task":      task,
		"sink":      sink,
	}).Observe(d.Seconds())
}

// ObservePipelineProcessTime records a pipeline stage processing time sample.
func ObservePipelineProcessTime(ns, task, stage string, d time.Duration) {
	PipelineProcessTime.With(prometheus.Labels{
		"namespace": ns,
		"task":      task,
		"stage":     stage,
	}).Observe(d.Seconds())
}

// IncTaskEventsTotal increments the event counter for a task.
func IncTaskEventsTotal(ns, task, eventType string) {
	TaskEventsTotal.With(prometheus.Labels{
		"namespace": ns,
		"task":      task,
		"type":      eventType,
	}).Inc()
}

// AddTaskEventsBytes adds n bytes to the event byte counter for a task.
func AddTaskEventsBytes(ns, task string, n float64) {
	TaskEventsBytes.With(prometheus.Labels{
		"namespace": ns,
		"task":      task,
	}).Add(n)
}

// IncSinkWriteErrors increments the sink write error counter.
func IncSinkWriteErrors(ns, task, sink, errorType string) {
	SinkWriteErrors.With(prometheus.Labels{
		"namespace":  ns,
		"task":       task,
		"sink":       sink,
		"error_type": errorType,
	}).Inc()
}

// SetTaskTotal sets the task count gauge for a given namespace and status.
func SetTaskTotal(ns, status string, count float64) {
	TaskTotal.With(prometheus.Labels{
		"namespace": ns,
		"status":    status,
	}).Set(count)
}

// SetSourcePosition sets the current replication position of a source.
func SetSourcePosition(ns, task, source string, position float64) {
	SourcePosition.With(prometheus.Labels{
		"namespace": ns,
		"task":      task,
		"source":    source,
	}).Set(position)
}

// SetSourceSnapshotProgress sets the snapshot progress percentage for a task.
func SetSourceSnapshotProgress(ns, task string, pct float64) {
	SourceSnapshotProgress.With(prometheus.Labels{
		"namespace": ns,
		"task":      task,
	}).Set(pct)
}

// SetPipelineQueueSize sets the current queue depth for a pipeline stage.
func SetPipelineQueueSize(ns, task, stage string, size float64) {
	PipelineQueueSize.With(prometheus.Labels{
		"namespace": ns,
		"task":      task,
		"stage":     stage,
	}).Set(size)
}

// SetNodeStatus sets the online/offline status of a cluster node.
// Use 1.0 for online, 0.0 for offline.
func SetNodeStatus(node string, status float64) {
	NodeStatus.With(prometheus.Labels{
		"node": node,
	}).Set(status)
}

// SetLeaderStatus sets whether the current node is the leader.
// Use 1.0 for leader, 0.0 for follower.
func SetLeaderStatus(isLeader float64) {
	LeaderStatus.Set(isLeader)
}

// IncLeaderChanges increments the leader change counter.
func IncLeaderChanges() {
	LeaderChanges.Inc()
}

// Timer is a helper for measuring elapsed time.
type Timer struct {
	start time.Time
}

// NewTimer creates a Timer that starts immediately.
func NewTimer() *Timer {
	return &Timer{start: time.Now()}
}

// ObserveTask records elapsed time as a TaskLatencySeconds sample.
func (t *Timer) ObserveTask(ns, task string) {
	ObserveTaskLatency(ns, task, time.Since(t.start))
}

// ObserveSink records elapsed time as a SinkWriteLatency sample.
func (t *Timer) ObserveSink(ns, task, sink string) {
	ObserveSinkWriteLatency(ns, task, sink, time.Since(t.start))
}

// ObservePipeline records elapsed time as a PipelineProcessTime sample.
func (t *Timer) ObservePipeline(ns, task, stage string) {
	ObservePipelineProcessTime(ns, task, stage, time.Since(t.start))
}
