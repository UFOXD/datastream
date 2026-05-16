package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func ObserveTaskLatency(cluster, task string, d time.Duration) {
	TaskLatencySeconds.With(prometheus.Labels{"cluster": cluster, "task": task}).Observe(d.Seconds())
}

func ObserveSinkWriteLatency(cluster, task, sink string, d time.Duration) {
	SinkWriteLatency.With(prometheus.Labels{"cluster": cluster, "task": task, "sink": sink}).Observe(d.Seconds())
}

func ObservePipelineProcessTime(cluster, task, stage string, d time.Duration) {
	PipelineProcessTime.With(prometheus.Labels{"cluster": cluster, "task": task, "stage": stage}).Observe(d.Seconds())
}

// IncTaskEvents increments the event counter with a known type and result.
func IncTaskEvents(cluster, task, eventType, result string) {
	TaskEventsTotal.With(prometheus.Labels{
		"cluster": cluster, "task": task, "type": eventType, "result": result,
	}).Inc()
}

func AddTaskEventsBytes(cluster, task string, n float64) {
	TaskEventsBytes.With(prometheus.Labels{"cluster": cluster, "task": task}).Add(n)
}

func IncSinkWriteErrors(cluster, task, sink, errorType string) {
	SinkWriteErrors.With(prometheus.Labels{
		"cluster": cluster, "task": task, "sink": sink, "error_type": errorType,
	}).Inc()
}

// SetTaskTotal sets the cluster-level state distribution gauge.
func SetTaskTotal(cluster, status string, count float64) {
	TaskTotal.With(prometheus.Labels{"cluster": cluster, "status": status}).Set(count)
}

func SetSourcePosition(cluster, task, source string, position float64) {
	SourcePosition.With(prometheus.Labels{"cluster": cluster, "task": task, "source": source}).Set(position)
}

func SetSourceSnapshotProgress(cluster, task string, pct float64) {
	SourceSnapshotProgress.With(prometheus.Labels{"cluster": cluster, "task": task}).Set(pct)
}

func SetPipelineQueueSize(cluster, task, stage string, size float64) {
	PipelineQueueSize.With(prometheus.Labels{"cluster": cluster, "task": task, "stage": stage}).Set(size)
}

func SetNodeStatus(node string, status float64) {
	NodeStatus.With(prometheus.Labels{"node": node}).Set(status)
}

func SetLeaderStatus(isLeader float64) {
	LeaderStatus.Set(isLeader)
}

func IncLeaderChanges() {
	LeaderChanges.Inc()
}

type Timer struct{ start time.Time }

func NewTimer() *Timer { return &Timer{start: time.Now()} }

func (t *Timer) ObserveTask(cluster, task string) {
	ObserveTaskLatency(cluster, task, time.Since(t.start))
}

func (t *Timer) ObserveSink(cluster, task, sink string) {
	ObserveSinkWriteLatency(cluster, task, sink, time.Since(t.start))
}

func (t *Timer) ObservePipeline(cluster, task, stage string) {
	ObservePipelineProcessTime(cluster, task, stage, time.Since(t.start))
}
