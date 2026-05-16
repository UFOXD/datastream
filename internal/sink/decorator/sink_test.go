package decorator_test

import (
	"context"
	"errors"
	"testing"

	"github.com/UFOXD/datastream/internal/connector"
	"github.com/UFOXD/datastream/internal/sink"
	"github.com/UFOXD/datastream/internal/sink/decorator"
	"github.com/UFOXD/datastream/pkg/event"
	dserrors "github.com/UFOXD/datastream/pkg/errors"
	"github.com/UFOXD/datastream/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type mockSink struct {
	writeCalls      int
	writeBatchCalls int
	asyncCalls      int
	failNext        error
}

func (m *mockSink) Name() string                                       { return "mock" }
func (m *mockSink) Initialize(ctx context.Context, c sink.Config) error { return nil }
func (m *mockSink) Start(ctx context.Context) error                    { return nil }
func (m *mockSink) Stop(ctx context.Context) error                     { return nil }
func (m *mockSink) Status() sink.Status                                { return sink.Status{} }
func (m *mockSink) Flush(ctx context.Context) error                    { return nil }
func (m *mockSink) GetPosition() *event.Position                       { return nil }
func (m *mockSink) SupportsDDL() bool                                  { return false }
func (m *mockSink) SupportsTransaction() bool                          { return false }
func (m *mockSink) Write(ctx context.Context, events []*event.ChangeEvent) error {
	m.writeCalls++
	if m.failNext != nil {
		err := m.failNext
		m.failNext = nil
		return err
	}
	return nil
}

type mockBatchSink struct{ mockSink }

func (m *mockBatchSink) WriteBatch(ctx context.Context, events []*event.ChangeEvent, batchSize int) error {
	m.writeBatchCalls++
	if m.failNext != nil {
		err := m.failNext
		m.failNext = nil
		return err
	}
	return nil
}

type mockStatsSink struct {
	mockSink
	statsCalls int
}

func (m *mockStatsSink) Stats(ctx context.Context) connector.Stats {
	m.statsCalls++
	return connector.Stats{QueueSize: 99, Connected: true}
}

func setupRegistry(t *testing.T) *prometheus.Registry {
	t.Helper()
	metrics.ResetForTest()
	r := prometheus.NewRegistry()
	metrics.MustRegisterAll(r)
	t.Cleanup(func() {
		metrics.ResetForTest()
		metrics.MustRegisterAll(prometheus.DefaultRegisterer)
	})
	return r
}

func TestMetricsSink_WriteSuccess_IncSuccess(t *testing.T) {
	r := setupRegistry(t)
	inner := &mockSink{}
	ms := decorator.WrapSink(inner, "c1", "t1", "mysql")
	ev := []*event.ChangeEvent{{Type: event.EventTypeInsert}}
	if err := ms.Write(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if inner.writeCalls != 1 {
		t.Errorf("inner.Write calls=%d, want 1", inner.writeCalls)
	}
	got := counterValue(t, r, "datastream_task_events_total", map[string]string{
		"cluster": "c1", "task": "t1", "type": "insert", "result": "success",
	})
	if got != 1 {
		t.Errorf("task_events_total success = %v, want 1", got)
	}
}

func TestMetricsSink_WriteFailure_NonRetriable(t *testing.T) {
	r := setupRegistry(t)
	inner := &mockSink{failNext: dserrors.ErrInvalidArgument.GenWithStackByArgs("bad")}
	ms := decorator.WrapSink(inner, "c1", "t1", "mysql")
	_ = ms.Write(context.Background(), []*event.ChangeEvent{{Type: event.EventTypeUpdate}})

	got := counterValue(t, r, "datastream_sink_write_errors_total", map[string]string{
		"cluster": "c1", "task": "t1", "sink": "mysql", "error_type": "non_retriable",
	})
	if got != 1 {
		t.Errorf("non_retriable error counter = %v, want 1", got)
	}
}

func TestMetricsSink_WriteFailure_Retriable(t *testing.T) {
	r := setupRegistry(t)
	inner := &mockSink{failNext: errors.New("network blip")}
	ms := decorator.WrapSink(inner, "c1", "t1", "mysql")
	_ = ms.Write(context.Background(), []*event.ChangeEvent{{Type: event.EventTypeInsert}})

	got := counterValue(t, r, "datastream_sink_write_errors_total", map[string]string{
		"cluster": "c1", "task": "t1", "sink": "mysql", "error_type": "retriable",
	})
	if got != 1 {
		t.Errorf("retriable error counter = %v, want 1", got)
	}
}

func TestMetricsSink_WriteBatch_DelegatesToInnerBatch(t *testing.T) {
	_ = setupRegistry(t)
	inner := &mockBatchSink{}
	ms := decorator.WrapSink(inner, "c1", "t1", "kafka")
	bc, ok := ms.(sink.BatchConnector)
	if !ok {
		t.Fatal("wrapped sink should expose BatchConnector")
	}
	if err := bc.WriteBatch(context.Background(), []*event.ChangeEvent{{Type: event.EventTypeInsert}}, 10); err != nil {
		t.Fatal(err)
	}
	if inner.writeBatchCalls != 1 {
		t.Errorf("inner.WriteBatch calls=%d, want 1", inner.writeBatchCalls)
	}
}

func TestMetricsSink_WriteBatch_FallsBackToWriteWhenInnerLacksBatch(t *testing.T) {
	_ = setupRegistry(t)
	inner := &mockSink{}
	ms := decorator.WrapSink(inner, "c1", "t1", "mysql")
	bc, ok := ms.(sink.BatchConnector)
	if !ok {
		t.Fatal("wrapped sink should expose BatchConnector")
	}
	if err := bc.WriteBatch(context.Background(), []*event.ChangeEvent{{Type: event.EventTypeInsert}}, 10); err != nil {
		t.Fatal(err)
	}
	if inner.writeCalls != 1 {
		t.Errorf("inner.Write fallback calls=%d, want 1", inner.writeCalls)
	}
}

func TestMetricsSink_StatsForwarding(t *testing.T) {
	_ = setupRegistry(t)
	inner := &mockStatsSink{}
	ms := decorator.WrapSink(inner, "c1", "t1", "mysql")
	sp, ok := ms.(connector.StatsProvider)
	if !ok {
		t.Fatal("wrapped sink should expose StatsProvider when inner does")
	}
	s := sp.Stats(context.Background())
	if s.QueueSize != 99 || !s.Connected {
		t.Errorf("forwarded stats = %+v, want QueueSize=99 Connected=true", s)
	}
}

func TestMetricsSink_StatsZeroWhenInnerHasNone(t *testing.T) {
	_ = setupRegistry(t)
	inner := &mockSink{}
	ms := decorator.WrapSink(inner, "c1", "t1", "mysql")
	if sp, ok := ms.(connector.StatsProvider); ok {
		s := sp.Stats(context.Background())
		if s != (connector.Stats{}) {
			t.Errorf("expected zero Stats, got %+v", s)
		}
	}
}

func TestMetricsSink_UnknownEventType_NoPanic(t *testing.T) {
	_ = setupRegistry(t)
	inner := &mockSink{}
	ms := decorator.WrapSink(inner, "c1", "t1", "mysql")
	// future enum value
	_ = ms.Write(context.Background(), []*event.ChangeEvent{{Type: event.EventType("future_value")}})
}

func counterValue(t *testing.T, r *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, _ := r.Gather()
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			match := true
			for _, lp := range m.GetLabel() {
				if v, ok := labels[lp.GetName()]; ok && v != lp.GetValue() {
					match = false
					break
				}
			}
			if match && m.GetCounter() != nil {
				return m.GetCounter().GetValue()
			}
		}
	}
	return -1
}
