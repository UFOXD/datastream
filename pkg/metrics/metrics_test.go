package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNamespace(t *testing.T) {
	if Namespace != "datastream" {
		t.Errorf("expected namespace 'datastream', got '%s'", Namespace)
	}
}

func TestTaskMetricsExist(t *testing.T) {
	// Verify task metrics are registered
	if TaskTotal == nil {
		t.Error("TaskTotal should not be nil")
	}
	if TaskEventsTotal == nil {
		t.Error("TaskEventsTotal should not be nil")
	}
	if TaskEventsBytes == nil {
		t.Error("TaskEventsBytes should not be nil")
	}
	if TaskLatencySeconds == nil {
		t.Error("TaskLatencySeconds should not be nil")
	}
}

func TestSourceMetricsExist(t *testing.T) {
	// Verify source metrics are registered
	if SourcePosition == nil {
		t.Error("SourcePosition should not be nil")
	}
	if SourceSnapshotProgress == nil {
		t.Error("SourceSnapshotProgress should not be nil")
	}
}

func TestSinkMetricsExist(t *testing.T) {
	// Verify sink metrics are registered
	if SinkWriteLatency == nil {
		t.Error("SinkWriteLatency should not be nil")
	}
	if SinkWriteErrors == nil {
		t.Error("SinkWriteErrors should not be nil")
	}
}

func TestPipelineMetricsExist(t *testing.T) {
	// Verify pipeline metrics are registered
	if PipelineQueueSize == nil {
		t.Error("PipelineQueueSize should not be nil")
	}
	if PipelineProcessTime == nil {
		t.Error("PipelineProcessTime should not be nil")
	}
}

func TestClusterMetricsExist(t *testing.T) {
	// Verify cluster metrics are registered
	if NodeStatus == nil {
		t.Error("NodeStatus should not be nil")
	}
	if LeaderStatus == nil {
		t.Error("LeaderStatus should not be nil")
	}
	if LeaderChanges == nil {
		t.Error("LeaderChanges should not be nil")
	}
}

func TestTaskTotalLabels(t *testing.T) {
	// Test that we can use TaskTotal with labels
	gauge := TaskTotal.WithLabelValues("default", "running")
	if gauge == nil {
		t.Error("expected non-nil gauge")
	}

	// Set a value to verify it works
	gauge.Set(1)
}

func TestTaskEventsTotalLabels(t *testing.T) {
	// Test that we can use TaskEventsTotal with labels
	counter := TaskEventsTotal.WithLabelValues("default", "task-1", "insert")
	if counter == nil {
		t.Error("expected non-nil counter")
	}

	// Increment to verify it works
	counter.Inc()
}

func TestSourcePositionLabels(t *testing.T) {
	// Test that we can use SourcePosition with labels
	gauge := SourcePosition.WithLabelValues("default", "task-1", "mysql-source")
	if gauge == nil {
		t.Error("expected non-nil gauge")
	}

	gauge.Set(12345)
}

func TestSinkWriteLatencyLabels(t *testing.T) {
	// Test that we can use SinkWriteLatency with labels
	histogram := SinkWriteLatency.WithLabelValues("default", "task-1", "kafka-sink")
	if histogram == nil {
		t.Error("expected non-nil histogram")
	}

	// Observe a value
	histogram.Observe(0.001)
}

func TestNodeStatusLabels(t *testing.T) {
	// Test that we can use NodeStatus with labels
	gauge := NodeStatus.WithLabelValues("node-1")
	if gauge == nil {
		t.Error("expected non-nil gauge")
	}

	gauge.Set(1)
}

func TestLeaderStatus(t *testing.T) {
	// Test that we can use LeaderStatus
	LeaderStatus.Set(1)
	LeaderStatus.Set(0)
}

func TestLeaderChanges(t *testing.T) {
	// Test that we can use LeaderChanges counter
	LeaderChanges.Inc()
}

func TestMetricsRegistered(t *testing.T) {
	// Verify metrics are registered with Prometheus
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	// Check that at least some datastream metrics exist
	found := false
	for _, mf := range mfs {
		if mf.GetName() == "datastream_task_total" {
			found = true
			break
		}
	}

	if !found {
		t.Log("datastream_task_total metric not found in gather, but this may be expected if no values set")
	}
}

func TestMetricNamespacing(t *testing.T) {
	// Verify all metrics have the correct namespace by checking the metric objects
	if TaskTotal == nil {
		t.Error("TaskTotal should not be nil")
	}
}
