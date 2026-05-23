// Package decorator wraps sink.Connector implementations with Prometheus metric emission.
package decorator

import (
	"context"
	"time"

	"github.com/UFOXD/datastream/internal/connector"
	"github.com/UFOXD/datastream/internal/sink"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// MetricsSink wraps a sink.Connector and emits metrics on Write/WriteBatch/WriteAsync.
// It implements sink.Connector and ALSO satisfies sink.BatchConnector and
// sink.AsyncConnector (delegating to inner where supported, falling back to
// Write otherwise). StatsProvider is forwarded if inner implements it.
type MetricsSink struct {
	inner    sink.Connector
	cluster  string
	taskID   string
	sinkType string

	// Pre-cached label vectors to avoid hashmap lookup on hot path.
	successCounters map[event.EventType]prometheus.Counter
	failedCounters  map[event.EventType]prometheus.Counter
	bytesAdder      prometheus.Counter
	latencyObserver prometheus.Observer
	errRetriable    prometheus.Counter
	errNonRetriable prometheus.Counter
}

// WrapSink returns a metric-emitting wrapper around s.
func WrapSink(s sink.Connector, cluster, taskID, sinkType string) sink.Connector {
	m := &MetricsSink{
		inner:    s,
		cluster:  cluster,
		taskID:   taskID,
		sinkType: sinkType,
	}
	m.precacheLabels()
	return m
}

func (m *MetricsSink) precacheLabels() {
	types := []event.EventType{
		event.EventTypeInsert, event.EventTypeUpdate, event.EventTypeDelete,
		event.EventTypeTruncate, event.EventTypeDDL,
		event.EventTypeHeartbeat, event.EventTypeTombstone,
	}
	m.successCounters = make(map[event.EventType]prometheus.Counter, len(types))
	m.failedCounters = make(map[event.EventType]prometheus.Counter, len(types))
	for _, t := range types {
		m.successCounters[t] = metrics.TaskEventsTotal.WithLabelValues(m.cluster, m.taskID, string(t), "success")
		m.failedCounters[t] = metrics.TaskEventsTotal.WithLabelValues(m.cluster, m.taskID, string(t), "failed")
	}
	m.bytesAdder = metrics.TaskEventsBytes.WithLabelValues(m.cluster, m.taskID)
	m.latencyObserver = metrics.SinkWriteLatency.WithLabelValues(m.cluster, m.taskID, m.sinkType)
	m.errRetriable = metrics.SinkWriteErrors.WithLabelValues(m.cluster, m.taskID, m.sinkType, string(metrics.ErrorTypeRetriable))
	m.errNonRetriable = metrics.SinkWriteErrors.WithLabelValues(m.cluster, m.taskID, m.sinkType, string(metrics.ErrorTypeNonRetriable))
}

// --- sink.Connector forwarding ---

func (m *MetricsSink) Name() string { return m.inner.Name() }
func (m *MetricsSink) Initialize(ctx context.Context, c sink.Config) error {
	return m.inner.Initialize(ctx, c)
}
func (m *MetricsSink) Start(ctx context.Context) error      { return m.inner.Start(ctx) }
func (m *MetricsSink) Stop(ctx context.Context) error       { return m.inner.Stop(ctx) }
func (m *MetricsSink) Status() sink.Status                  { return m.inner.Status() }
func (m *MetricsSink) Flush(ctx context.Context) error      { return m.inner.Flush(ctx) }
func (m *MetricsSink) GetPosition() *event.Position         { return m.inner.GetPosition() }
func (m *MetricsSink) SupportsDDL() bool                    { return m.inner.SupportsDDL() }
func (m *MetricsSink) ApplyDDL(ctx context.Context, ddl *event.ChangeEvent) error {
	return m.inner.ApplyDDL(ctx, ddl)
}
func (m *MetricsSink) SupportsTransaction() bool            { return m.inner.SupportsTransaction() }

// Write — primary instrumented path.
func (m *MetricsSink) Write(ctx context.Context, events []*event.ChangeEvent) error {
	start := time.Now()
	err := m.inner.Write(ctx, events)
	m.latencyObserver.Observe(time.Since(start).Seconds())
	m.recordResult(events, err)
	return err
}

// WriteBatch — implements sink.BatchConnector. Delegates to inner if it
// supports BatchConnector, else falls back to Write.
func (m *MetricsSink) WriteBatch(ctx context.Context, events []*event.ChangeEvent, batchSize int) error {
	bc, ok := m.inner.(sink.BatchConnector)
	if !ok {
		return m.Write(ctx, events)
	}
	start := time.Now()
	err := bc.WriteBatch(ctx, events, batchSize)
	m.latencyObserver.Observe(time.Since(start).Seconds())
	m.recordResult(events, err)
	return err
}

// WriteAsync — implements sink.AsyncConnector. Records only enqueue time
// and synchronous enqueue errors. Success path doesn't count events (would
// require callback wiring — out of scope).
func (m *MetricsSink) WriteAsync(ctx context.Context, events []*event.ChangeEvent) error {
	ac, ok := m.inner.(sink.AsyncConnector)
	if !ok {
		return m.Write(ctx, events)
	}
	start := time.Now()
	err := ac.WriteAsync(ctx, events)
	m.latencyObserver.Observe(time.Since(start).Seconds())
	if err != nil {
		m.recordResult(events, err)
	}
	return err
}

// Acknowledgments forwards to inner if it implements AsyncConnector.
// Returns nil if inner doesn't support async.
func (m *MetricsSink) Acknowledgments() <-chan *sink.Ack {
	if ac, ok := m.inner.(sink.AsyncConnector); ok {
		return ac.Acknowledgments()
	}
	return nil
}

// Stats forwards to inner if it implements StatsProvider; else returns zero Stats.
func (m *MetricsSink) Stats(ctx context.Context) connector.Stats {
	if sp, ok := m.inner.(connector.StatsProvider); ok {
		return sp.Stats(ctx)
	}
	return connector.Stats{}
}

// SupportsStats reports whether inner implements StatsProvider — used by App
// startup to log which connectors have pull-mode metrics.
func (m *MetricsSink) SupportsStats() bool {
	_, ok := m.inner.(connector.StatsProvider)
	return ok
}

func (m *MetricsSink) recordResult(events []*event.ChangeEvent, err error) {
	if err != nil {
		errType := metrics.ClassifyError(err)
		if errType == metrics.ErrorTypeRetriable {
			m.errRetriable.Inc()
		} else {
			m.errNonRetriable.Inc()
		}
		for _, e := range events {
			if c, ok := m.failedCounters[e.Type]; ok {
				c.Inc()
			}
		}
		return
	}
	var totalBytes int
	for _, e := range events {
		if c, ok := m.successCounters[e.Type]; ok {
			c.Inc()
		}
		totalBytes += e.Size()
	}
	m.bytesAdder.Add(float64(totalBytes))
}
