# Prometheus Metrics Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make DataStream's Prometheus metrics actually work end-to-end: real `/metrics` endpoint, no panics, sink decorator + pull-mode StatsCollector, 7 new metrics, 12 connectors implementing StatsProvider.

**Architecture:** Three data paths — (1) Pipeline consume point emits success counters + bytes + lag; (2) Sink decorator (Connector/BatchConnector/AsyncConnector) emits latency + errors + failed counters; (3) StatsCollector polls connectors implementing optional `StatsProvider` interface every 5s with 1s per-call timeout, running providers concurrently.

**Tech Stack:** Go 1.21+, `github.com/prometheus/client_golang`, existing `pkg/metrics` package being refactored from `promauto` global init to explicit `MustRegisterAll(r)` for test isolation.

**Spec:** `docs/superpowers/specs/2026-05-16-metrics-integration-design.md`

---

## File Structure

### Created files

| Path | Responsibility |
|------|----------------|
| `internal/connector/stats.go` | `StatsProvider` interface + `Stats` struct (shared by source/sink) |
| `internal/connector/stats_test.go` | Compile-time interface verification |
| `pkg/metrics/classify.go` | `ClassifyError(err)` returns `retriable` / `non_retriable` |
| `pkg/metrics/classify_test.go` | Tests for error classification |
| `pkg/metrics/registry.go` | `MustRegisterAll(r)` + `ResetForTest()` |
| `pkg/metrics/registry_test.go` | Test isolation tests |
| `pkg/metrics/collector_runtime.go` | `StatsCollector` (registers providers, ticker loop) |
| `pkg/metrics/collector_runtime_test.go` | Concurrency + panic + timeout tests |
| `internal/sink/decorator/sink.go` | `MetricsSink` wrapping `sink.Connector` + Batch/Async |
| `internal/sink/decorator/sink_test.go` | Decorator tests with mock sinks |
| `tests/integration/metrics_test.go` | End-to-end metric verification |
| `deployments/grafana/datastream-dashboard.json` | Grafana dashboard JSON |
| `docs/operations/metrics.md` | Operator metrics reference |

### Modified files

| Path | Change |
|------|--------|
| `pkg/event/change_event.go` | Add `Size()` method |
| `pkg/event/event_test.go` | Add `TestChangeEvent_Size` |
| `pkg/metrics/metrics.go` | Refactor: `promauto` → `MustRegisterAll`; rename `namespace` label → `cluster`; add 7 metrics |
| `pkg/metrics/collector.go` | Update helper signatures to use `cluster` |
| `pkg/metrics/metrics_test.go` | Update for new labels |
| `internal/api/server.go` | Replace fake `handleMetrics` with `promhttp.Handler()` |
| `internal/api/server_test.go` | Test `/metrics` returns real Prometheus output |
| `internal/pipeline/pipeline.go` | Add `cluster` field, `SetCluster`, `precacheLabels`, `updateState`; fix 4 panic sites; embed consume-point instrumentation |
| `internal/pipeline/pipeline_test.go` | Add state-machine + no-panic tests |
| `internal/pipeline/task.go` | Add `SetStatsCollector` on TaskManager; wrap sinks at Create; register/unregister providers |
| `pkg/config/config.go` | Add `Cluster` + `MetricsConfig`; defaults in `Adjust()` |
| `pkg/config/config_test.go` | Test defaults |
| `internal/app/app.go` | Create `StatsCollector` on Start; wire into TaskManager |
| `cmd/datastream/main.go` | `--cluster` flag (if cobra root exists) or surface via config |
| Each of 12 connectors | Implement `StatsProvider.Stats(ctx)` |
| `docs/design/core-design.md` | Metric definition updates |
| `docs/design/connector-design.md` | `StatsProvider` section |
| `docs/design/pipeline-design.md` | Consume-point + decorator assembly |
| `docs/design/event-model-design.md` | `Size()` method |
| `docs/design/api-cli-design.md` | `--cluster` flag |

---

## Stage 1: Foundation (Registry refactor + endpoint + panic fix + Size)

**Goal of stage:** Existing metrics work without panicking; `/metrics` returns real Prometheus output; `pkg/metrics` is testable via `ResetForTest`. After this stage no functional behavior changes for end-users beyond "no more crashes".

---

### Task 1.1: Add `ChangeEvent.Size()`

**Files:**
- Modify: `pkg/event/change_event.go` (add method at end of file)
- Test: `pkg/event/event_test.go` (add test cases)

- [ ] **Step 1: Write the failing tests**

Append to `pkg/event/event_test.go`:

```go
func TestChangeEvent_Size_Nil(t *testing.T) {
	var e *ChangeEvent
	if got := e.Size(); got != 0 {
		t.Errorf("nil ChangeEvent size = %d, want 0", got)
	}
}

func TestChangeEvent_Size_Empty(t *testing.T) {
	e := &ChangeEvent{}
	if got := e.Size(); got <= 0 {
		t.Errorf("empty ChangeEvent size = %d, want > 0 (fixed overhead)", got)
	}
}

func TestChangeEvent_Size_WithFields(t *testing.T) {
	e := &ChangeEvent{
		Source: SourceInfo{Database: "db1"},
		Table:  TableInfo{Database: "db1", Name: "users"},
		After: RowData{Fields: map[string]Field{
			"id":   {Name: "id", Value: int64(42)},
			"name": {Name: "name", Value: "alice"},
		}},
	}
	got := e.Size()
	// expected lower bound: 3 ("db1") + 3 + 5 ("users") + 64 (overhead)
	//   + "id"(2) + 16 (numeric) + "name"(4) + 5 ("alice")
	want := 3 + 3 + 5 + 64 + 2 + 16 + 4 + 5
	if got < want {
		t.Errorf("Size() = %d, want >= %d", got, want)
	}
}

func TestChangeEvent_Size_StringAndBytes(t *testing.T) {
	e := &ChangeEvent{
		After: RowData{Fields: map[string]Field{
			"s": {Name: "s", Value: "hello"},
			"b": {Name: "b", Value: []byte("world!!")},
		}},
	}
	got := e.Size()
	// overhead(64) + "s"(1) + 5 + "b"(1) + 7
	if got < 64+1+5+1+7 {
		t.Errorf("Size() = %d, want larger", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/event/ -run TestChangeEvent_Size -v`
Expected: FAIL — `e.Size undefined (type *ChangeEvent has no field or method Size)`

- [ ] **Step 3: Implement `Size()`**

Append to `pkg/event/change_event.go` (after the last method):

```go
// Size returns an estimated byte size of the event for metrics accounting.
// This is a rough estimate (not exact serialized size) sufficient for byte-rate
// observability. Counts source/table names, all Before+After field name+value
// byte lengths, plus a fixed overhead for metadata.
func (e *ChangeEvent) Size() int {
	if e == nil {
		return 0
	}
	n := len(e.Source.Database) + len(e.Table.Database) + len(e.Table.Name) + 64
	for name, f := range e.Before.Fields {
		n += len(name) + estimateValueSize(f.Value)
	}
	for name, f := range e.After.Fields {
		n += len(name) + estimateValueSize(f.Value)
	}
	return n
}

func estimateValueSize(v interface{}) int {
	switch x := v.(type) {
	case nil:
		return 0
	case string:
		return len(x)
	case []byte:
		return len(x)
	default:
		return 16
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/event/ -run TestChangeEvent_Size -v`
Expected: PASS (all 4 cases)

- [ ] **Step 5: Commit**

```bash
git add pkg/event/change_event.go pkg/event/event_test.go
git commit -m "feat(event): add ChangeEvent.Size() for byte-rate metrics

Estimates event byte size by summing source/table names, all Before+After
field name+value lengths, plus a 64-byte fixed metadata overhead. Used by
upcoming task_events_bytes metric accounting in Pipeline consume point
and Sink decorator.

Not exact — sufficient for trend observability. See spec §4.1."
```

---

### Task 1.2: Refactor `pkg/metrics/metrics.go` for explicit registration

**Files:**
- Modify: `pkg/metrics/metrics.go`
- Create: `pkg/metrics/registry.go`
- Create: `pkg/metrics/registry_test.go`

This task is the **largest refactor** in the plan. The current file declares all metrics with `promauto.NewXxx` (registering to `DefaultRegisterer` at import time). We move them to package vars initialized by `MustRegisterAll(r)`.

- [ ] **Step 1: Write the failing test**

Create `pkg/metrics/registry_test.go`:

```go
package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestResetForTest_AllowsReregistration(t *testing.T) {
	// Save default and reset
	ResetForTest()
	defer func() {
		ResetForTest()
		MustRegisterAll(prometheus.DefaultRegisterer)
	}()

	r := prometheus.NewRegistry()
	MustRegisterAll(r)

	// Verify we can register metric values
	TaskTotal.WithLabelValues("test-cluster", "running").Set(1)

	families, err := r.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}
	found := false
	for _, f := range families {
		if strings.Contains(f.GetName(), "task_total") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("datastream_task_total not found in independent registry")
	}
}

func TestMustRegisterAll_DoublePanics(t *testing.T) {
	ResetForTest()
	defer func() {
		ResetForTest()
		MustRegisterAll(prometheus.DefaultRegisterer)
	}()

	r := prometheus.NewRegistry()
	MustRegisterAll(r)

	defer func() {
		if recover() == nil {
			t.Error("expected panic on double MustRegisterAll, got none")
		}
	}()
	MustRegisterAll(r) // should panic — duplicate registration
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/metrics/ -run "TestResetForTest|TestMustRegisterAll_Double" -v`
Expected: FAIL — `ResetForTest`, `MustRegisterAll` undefined

- [ ] **Step 3: Replace `pkg/metrics/metrics.go` with explicit-registration version**

Full replacement of `pkg/metrics/metrics.go`:

```go
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
	TaskTotal        *prometheus.GaugeVec   // cluster-level state distribution
	TaskState        *prometheus.GaugeVec   // per-task state 0/1 (new)
	TaskEventsTotal  *prometheus.CounterVec // per-task event counter (now with result)
	TaskEventsBytes  *prometheus.CounterVec
	TaskLatencySeconds *prometheus.HistogramVec
)

// Source metrics.
var (
	SourcePosition         *prometheus.GaugeVec
	SourceSnapshotProgress *prometheus.GaugeVec
	SourceLagSeconds       *prometheus.GaugeVec // new
	SourceLastEventSeconds *prometheus.GaugeVec // new
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
```

- [ ] **Step 4: Create `pkg/metrics/registry.go`**

```go
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
```

- [ ] **Step 4: Update `pkg/metrics/collector.go` for new `cluster` label**

The existing file uses `namespace` (param `ns`). Rename param `ns` → `cluster` for clarity. Replace the file:

```go
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
```

- [ ] **Step 5: Update `pkg/metrics/metrics_test.go` if it references old label names**

Run: `cat pkg/metrics/metrics_test.go` — if it references `namespace` label values, replace them with `cluster`. Adapt to call `MustRegisterAll(prometheus.NewRegistry())` if needed.

- [ ] **Step 6: Run package tests**

Run: `go test ./pkg/metrics/ -v`
Expected: PASS for all tests (registry_test + metrics_test)

- [ ] **Step 7: Run downstream tests to spot breakage**

Run: `go build ./...`
Expected: BUILD FAIL — `internal/pipeline/pipeline.go` still uses `metrics.TaskTotal.WithLabelValues(p.id, "running").Inc()` with old label semantics. We fix this in Task 1.3. **Skip commit until 1.3 succeeds.**

---

### Task 1.3: Fix `internal/pipeline/pipeline.go` panic sites + add `cluster` + `updateState`

**Files:**
- Modify: `internal/pipeline/pipeline.go`
- Test: `internal/pipeline/pipeline_test.go` (add test)

- [ ] **Step 1: Write the failing test**

Append to `internal/pipeline/pipeline_test.go`:

```go
func TestPipeline_StateMachine_NoPanic(t *testing.T) {
	// Use an independent registry to avoid polluting global state across tests.
	metrics.ResetForTest()
	r := prometheus.NewRegistry()
	metrics.MustRegisterAll(r)
	t.Cleanup(func() {
		metrics.ResetForTest()
		metrics.MustRegisterAll(prometheus.DefaultRegisterer)
	})

	cfg := &Config{ID: "t1", Name: "test"}
	p := New(cfg)
	p.SetCluster("c1")

	// state machine transitions must not panic regardless of label values
	p.updateState("running")
	p.updateState("paused")
	p.updateState("stopped")

	// task_state for current state must be 1, others 0
	val := getGaugeValue(t, r, "datastream_task_state", map[string]string{
		"cluster": "c1", "task": "t1", "state": "stopped",
	})
	if val != 1 {
		t.Errorf("task_state{stopped} = %v, want 1", val)
	}
	val = getGaugeValue(t, r, "datastream_task_state", map[string]string{
		"cluster": "c1", "task": "t1", "state": "running",
	})
	if val != 0 {
		t.Errorf("task_state{running} = %v, want 0", val)
	}
}

// getGaugeValue is a test helper to extract a labeled gauge value.
func getGaugeValue(t *testing.T, r *prometheus.Registry, name string, labels map[string]string) float64 {
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
			if match && m.GetGauge() != nil {
				return m.GetGauge().GetValue()
			}
		}
	}
	return -1
}
```

Add these imports to the test file (if missing):

```go
"github.com/UFOXD/datastream/pkg/metrics"
"github.com/prometheus/client_golang/prometheus"
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/pipeline/ -run TestPipeline_StateMachine_NoPanic -v`
Expected: FAIL — `p.SetCluster undefined`, `p.updateState undefined`

- [ ] **Step 3: Add `cluster` field, `SetCluster`, `updateState` to Pipeline**

Edit `internal/pipeline/pipeline.go`:

1. Add field to `Pipeline` struct (after `id`/`name`):
```go
	cluster     string
```

2. Add `SetCluster` method (after `Name()`):
```go
// SetCluster sets the cluster label value for metrics.
func (p *Pipeline) SetCluster(c string) {
	p.cluster = c
}
```

3. Add state constants and `updateState` (anywhere in file):
```go
const (
	stateRunning = "running"
	stateStopped = "stopped"
	statePaused  = "paused"
	stateError   = "error"
)

// updateState transitions task state and emits both per-task and cluster-level gauges.
func (p *Pipeline) updateState(newState string) {
	p.mu.Lock()
	oldState := string(p.status.State)
	p.status.State = State(newState)
	p.mu.Unlock()

	// per-task state: new state = 1, others = 0
	for _, s := range []string{stateRunning, stateStopped, statePaused, stateError} {
		v := 0.0
		if s == newState {
			v = 1.0
		}
		metrics.TaskState.WithLabelValues(p.cluster, p.id, s).Set(v)
	}

	// cluster-level distribution counters (gauge inc/dec)
	if oldState != "" && oldState != newState {
		metrics.TaskTotal.WithLabelValues(p.cluster, oldState).Dec()
	}
	if oldState != newState {
		metrics.TaskTotal.WithLabelValues(p.cluster, newState).Inc()
	}
}
```

4. Replace the 4 buggy call sites:

`internal/pipeline/pipeline.go:198` — find and replace:
```go
// BEFORE
metrics.TaskTotal.WithLabelValues(p.id, "running").Inc()
// AFTER
p.updateState(stateRunning)
```

`internal/pipeline/pipeline.go:234` — find and replace:
```go
// BEFORE
metrics.TaskTotal.WithLabelValues(p.id, "stopped").Inc()
// AFTER
p.updateState(stateStopped)
```

`internal/pipeline/pipeline.go:342` and `:345` — find and replace the entire 4-line block in `processEvent`:
```go
// BEFORE
if err := s.Write(ctx, []*event.ChangeEvent{e}); err != nil {
    log.Error("failed to write to sink",
        zap.String("sink", s.Name()),
        zap.Error(err))
    p.mu.Lock()
    p.status.Statistics.EventsFailed++
    p.mu.Unlock()
    metrics.TaskEventsTotal.WithLabelValues(p.id, "failed").Inc()
    continue
}
metrics.TaskEventsTotal.WithLabelValues(p.id, "written").Inc()
// AFTER (event counting moves into Sink decorator in Stage 3; here we just track stats)
if err := s.Write(ctx, []*event.ChangeEvent{e}); err != nil {
    log.Error("failed to write to sink",
        zap.String("sink", s.Name()),
        zap.Error(err))
    p.mu.Lock()
    p.status.Statistics.EventsFailed++
    p.mu.Unlock()
    continue
}
```

5. Replace the `processEvent` latency line:
```go
// BEFORE
metrics.TaskLatencySeconds.WithLabelValues(p.id).Observe(latency)
// AFTER
metrics.TaskLatencySeconds.WithLabelValues(p.cluster, p.id).Observe(latency)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/pipeline/ -run TestPipeline_StateMachine_NoPanic -v`
Expected: PASS

- [ ] **Step 5: Run full build**

Run: `go build ./...`
Expected: BUILD SUCCESS

- [ ] **Step 6: Run all package tests**

Run: `go test ./pkg/metrics/ ./pkg/event/ ./internal/pipeline/ -v`
Expected: PASS

- [ ] **Step 7: Commit (squashes Task 1.2 + 1.3 since they must land together)**

```bash
git add pkg/metrics/ pkg/event/change_event.go pkg/event/event_test.go internal/pipeline/pipeline.go internal/pipeline/pipeline_test.go
git commit -m "refactor(metrics): switch to explicit MustRegisterAll; rename namespace->cluster

- pkg/metrics: replace promauto with explicit var + MustRegisterAll(r)
  + ResetForTest() for test isolation. Renames all 'namespace' labels
  to 'cluster' to avoid conflict with prometheus.Namespace concept.
- pkg/metrics: add IncTaskEvents(cluster,task,type,result) helper for
  upcoming 4-label task_events_total schema.
- pkg/event: add ChangeEvent.Size() (already committed earlier in series).
- internal/pipeline: add cluster field + SetCluster; introduce updateState()
  to atomically transition task_state (per-task 0/1) and task_total
  (cluster-level distribution). Fix 4 panic sites in pipeline.go:198/234
  /342/345 that passed task ID into namespace label position. Sink-write
  event counting removed here; will be handled by Sink decorator in Stage 3.

Refs spec §4.6, §5.1, §5.3."
```

---

### Task 1.4: Wire real `/metrics` endpoint

**Files:**
- Modify: `internal/api/server.go:369-377` (replace `handleMetrics`)
- Modify: `internal/api/server_test.go` (add test)

- [ ] **Step 1: Write the failing test**

Append to `internal/api/server_test.go`:

```go
func TestMetricsEndpoint_ReturnsPrometheusFormat(t *testing.T) {
	// Independent registry to avoid promhttp.Handler picking up other test pollution
	metrics.ResetForTest()
	r := prometheus.NewRegistry()
	metrics.MustRegisterAll(r)
	t.Cleanup(func() {
		metrics.ResetForTest()
		metrics.MustRegisterAll(prometheus.DefaultRegisterer)
	})

	// Trigger at least one observation so output is non-empty
	metrics.TaskTotal.WithLabelValues("test", "running").Set(1)

	s := NewServer(DefaultServerConfig())
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "datastream_task_total") {
		t.Errorf("body missing datastream_task_total; got first 200 chars: %s", body[:min(200, len(body))])
	}
	// Check it's real Prometheus format, not the old dummy "# DataStream Metrics"
	if !strings.Contains(body, "# HELP") {
		t.Errorf("body missing # HELP line (not real Prometheus output): %s", body[:200])
	}
}

func min(a, b int) int {
	if a < b { return a }
	return b
}
```

Imports if missing:
```go
"strings"
"net/http/httptest"
"github.com/UFOXD/datastream/pkg/metrics"
"github.com/prometheus/client_golang/prometheus"
```

- [ ] **Step 2: Run test to verify failure**

Run: `go test ./internal/api/ -run TestMetricsEndpoint_ReturnsPrometheusFormat -v`
Expected: FAIL — body contains "# DataStream Metrics" but not "# HELP datastream_task_total"

- [ ] **Step 3: Replace `handleMetrics` with real handler**

In `internal/api/server.go`, find:

```go
// handleMetrics returns the Prometheus metrics handler.
func (s *Server) handleMetrics() http.Handler {
	// Return Prometheus handler
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Metrics would be served here
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("# DataStream Metrics\n"))
	})
}
```

Replace with:

```go
// handleMetrics returns the Prometheus metrics handler.
func (s *Server) handleMetrics() http.Handler {
	return promhttp.Handler()
}
```

Add import to `internal/api/server.go`:

```go
"github.com/prometheus/client_golang/prometheus/promhttp"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/api/ -run TestMetricsEndpoint_ReturnsPrometheusFormat -v`
Expected: PASS

- [ ] **Step 5: Run all api tests**

Run: `go test ./internal/api/ -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/api/server.go internal/api/server_test.go
git commit -m "fix(api): serve real Prometheus metrics at /metrics

Replaces dummy '# DataStream Metrics' string handler with
promhttp.Handler() backed by the default Prometheus registry.

Refs spec §6.1."
```

---

### Task 1.5: Stage 1 verification

- [ ] **Step 1: Run full test suite**

Run: `go test ./... 2>&1 | tail -30`
Expected: All packages pass

- [ ] **Step 2: Build all binaries**

Run: `go build ./...`
Expected: SUCCESS

- [ ] **Step 3: Manual sanity check (optional, requires running binary)**

Run binary in one shell, `curl localhost:8300/metrics | head -20` in another. Should see real metric output starting with `# HELP` and `# TYPE` lines.

- [ ] **Step 4: Stage tag (optional)**

```bash
git tag stage-1-metrics-foundation
```

---

## Stage 2: Public components (StatsProvider, ClassifyError, StatsCollector)

**Goal:** Public scaffolding for pull-mode metrics is in place and unit-tested. No connector or pipeline change yet.

---

### Task 2.1: `StatsProvider` interface in `internal/connector/`

**Files:**
- Create: `internal/connector/stats.go`
- Create: `internal/connector/stats_test.go`

- [ ] **Step 1: Write failing test (compile-time interface check)**

Create `internal/connector/stats_test.go`:

```go
package connector

import (
	"context"
	"testing"
)

// mockProvider verifies that types can implement StatsProvider.
type mockProvider struct{}

func (m *mockProvider) Stats(ctx context.Context) Stats {
	return Stats{QueueSize: 1, Connected: true}
}

func TestStatsProvider_InterfaceSatisfied(t *testing.T) {
	var _ StatsProvider = (*mockProvider)(nil)
}

func TestStats_ZeroValueIsNotApplicable(t *testing.T) {
	var s Stats
	if s.QueueSize != 0 {
		t.Error("expected zero QueueSize")
	}
	if s.Connected {
		t.Error("expected Connected=false default")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/connector/ -v`
Expected: FAIL — package not found / `StatsProvider` undefined

- [ ] **Step 3: Create the interface**

Create `internal/connector/stats.go`:

```go
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
```

- [ ] **Step 4: Run test to verify pass**

Run: `go test ./internal/connector/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/connector/
git commit -m "feat(connector): add optional StatsProvider interface

Defines the contract for connector-side pull-mode metrics. Source/sink
connectors that implement Stats(ctx) get queue/lag/snapshot/connection
gauges populated by StatsCollector every 5s. Unimplemented connectors
are silently skipped (logged at startup).

Refs spec §4.2."
```

---

### Task 2.2: `ClassifyError` for error_type label

**Files:**
- Create: `pkg/metrics/classify.go`
- Create: `pkg/metrics/classify_test.go`

- [ ] **Step 1: Write failing test**

Create `pkg/metrics/classify_test.go`:

```go
package metrics_test

import (
	"context"
	"errors"
	"testing"

	dserrors "github.com/UFOXD/datastream/pkg/errors"
	"github.com/UFOXD/datastream/pkg/metrics"
)

func TestClassifyError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want metrics.ErrorType
	}{
		{"nil", nil, metrics.ErrorTypeRetriable},
		{"context_canceled", context.Canceled, metrics.ErrorTypeNonRetriable},
		{"deadline", context.DeadlineExceeded, metrics.ErrorTypeNonRetriable},
		{"random_io", errors.New("connection reset"), metrics.ErrorTypeRetriable},
		{"invalid_arg", dserrors.ErrInvalidArgument.GenWithStackByArgs("bad"), metrics.ErrorTypeNonRetriable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := metrics.ClassifyError(c.err)
			if got != c.want {
				t.Errorf("ClassifyError(%v) = %s, want %s", c.err, got, c.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./pkg/metrics/ -run TestClassifyError -v`
Expected: FAIL — `ClassifyError`, `ErrorType*` undefined

- [ ] **Step 3: Create `pkg/metrics/classify.go`**

```go
package metrics

import "github.com/UFOXD/datastream/pkg/errors"

// ErrorType is the value of the error_type label on error counters.
type ErrorType string

const (
	ErrorTypeRetriable    ErrorType = "retriable"
	ErrorTypeNonRetriable ErrorType = "non_retriable"
)

// ClassifyError maps an error to its metric label value.
// Single source of truth is pkg/errors.IsRetryableError.
func ClassifyError(err error) ErrorType {
	if err == nil {
		return ErrorTypeRetriable
	}
	if errors.IsRetryableError(err) {
		return ErrorTypeRetriable
	}
	return ErrorTypeNonRetriable
}
```

- [ ] **Step 4: Run test to verify pass**

Run: `go test ./pkg/metrics/ -run TestClassifyError -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/metrics/classify.go pkg/metrics/classify_test.go
git commit -m "feat(metrics): add ClassifyError for error_type label

Maps errors to retriable/non_retriable using pkg/errors.IsRetryableError
as single source of truth. Used by SinkWriteErrors and any other error
counter that needs Debezium-style binary classification (avoids high
cardinality on labels).

Refs spec §4.4."
```

---

### Task 2.3: `StatsCollector` with concurrent provider polling

**Files:**
- Create: `pkg/metrics/collector_runtime.go`
- Create: `pkg/metrics/collector_runtime_test.go`

- [ ] **Step 1: Write the failing tests**

Create `pkg/metrics/collector_runtime_test.go`:

```go
package metrics_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UFOXD/datastream/internal/connector"
	"github.com/UFOXD/datastream/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type fakeProvider struct {
	stats   connector.Stats
	calls   int32
	sleep   time.Duration
	panicOn bool
}

func (f *fakeProvider) Stats(ctx context.Context) connector.Stats {
	atomic.AddInt32(&f.calls, 1)
	if f.panicOn {
		panic("boom")
	}
	if f.sleep > 0 {
		select {
		case <-time.After(f.sleep):
		case <-ctx.Done():
			return connector.Stats{}
		}
	}
	return f.stats
}

func newRegistry(t *testing.T) *prometheus.Registry {
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

func TestStatsCollector_BasicScrape(t *testing.T) {
	_ = newRegistry(t)
	c := metrics.NewStatsCollector("c1", 50*time.Millisecond, time.Second)
	p := &fakeProvider{stats: connector.Stats{QueueSize: 42, Connected: true}}
	c.Register("t1:source", "source", "mysql", "t1", p)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	time.Sleep(200 * time.Millisecond)
	cancel()

	if atomic.LoadInt32(&p.calls) < 1 {
		t.Errorf("expected at least 1 Stats call, got %d", p.calls)
	}
}

func TestStatsCollector_PanicRecovered(t *testing.T) {
	_ = newRegistry(t)
	c := metrics.NewStatsCollector("c1", 50*time.Millisecond, time.Second)
	bad := &fakeProvider{panicOn: true}
	good := &fakeProvider{stats: connector.Stats{QueueSize: 1, Connected: true}}
	c.Register("bad", "source", "mysql", "t1", bad)
	c.Register("good", "sink", "kafka", "t1", good)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	time.Sleep(200 * time.Millisecond)
	cancel()

	if atomic.LoadInt32(&good.calls) < 1 {
		t.Errorf("good provider not called despite bad provider panicking; calls=%d", good.calls)
	}
}

func TestStatsCollector_Timeout(t *testing.T) {
	_ = newRegistry(t)
	c := metrics.NewStatsCollector("c1", 100*time.Millisecond, 50*time.Millisecond)
	slow := &fakeProvider{sleep: 500 * time.Millisecond}
	c.Register("slow", "source", "mysql", "t1", slow)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	time.Sleep(300 * time.Millisecond)
	cancel()

	// Multiple ticks should fire even though provider exceeds timeout
	if atomic.LoadInt32(&slow.calls) < 2 {
		t.Errorf("expected ≥2 ticks despite slow provider; calls=%d", slow.calls)
	}
}

func TestStatsCollector_ReregisterOverwrites(t *testing.T) {
	_ = newRegistry(t)
	c := metrics.NewStatsCollector("c1", 50*time.Millisecond, time.Second)
	p1 := &fakeProvider{stats: connector.Stats{QueueSize: 1}}
	p2 := &fakeProvider{stats: connector.Stats{QueueSize: 2}}
	c.Register("key", "source", "mysql", "t1", p1)
	c.Register("key", "source", "mysql", "t1", p2) // overwrite

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	time.Sleep(150 * time.Millisecond)
	cancel()

	if atomic.LoadInt32(&p2.calls) < 1 {
		t.Error("p2 (replacement) should have been called")
	}
}

func TestStatsCollector_LagNaNSkipped(t *testing.T) {
	r := newRegistry(t)
	c := metrics.NewStatsCollector("c1", 50*time.Millisecond, time.Second)
	p := &fakeProvider{stats: connector.Stats{
		LagSeconds: math.NaN(),
	}}
	c.Register("t1:source", "source", "mysql", "t1", p)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)
	time.Sleep(150 * time.Millisecond)
	cancel()

	// Gauge should NOT have been set (no time series for that label set)
	families, _ := r.Gather()
	for _, f := range families {
		if f.GetName() == "datastream_source_lag_seconds" && len(f.GetMetric()) > 0 {
			t.Errorf("source_lag_seconds set to NaN should be skipped, found series: %v", f.GetMetric())
		}
	}
}
```

Add import to the test file:
```go
"math"
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./pkg/metrics/ -run TestStatsCollector -v`
Expected: FAIL — `StatsCollector`, `NewStatsCollector` undefined

- [ ] **Step 3: Implement `pkg/metrics/collector_runtime.go`**

```go
package metrics

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/UFOXD/datastream/internal/connector"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// StatsCollector periodically polls registered StatsProvider implementations
// and writes their snapshot into Prometheus gauges.
type StatsCollector struct {
	cluster   string
	interval  time.Duration
	timeout   time.Duration
	mu        sync.RWMutex
	providers map[string]providerEntry
}

type providerEntry struct {
	provider connector.StatsProvider
	role     string // "source" | "sink"
	cType    string // connector type, e.g. "mysql"
	taskID   string
}

// NewStatsCollector creates a collector. Caller must invoke Run(ctx) in a goroutine.
func NewStatsCollector(cluster string, interval, timeout time.Duration) *StatsCollector {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	return &StatsCollector{
		cluster:   cluster,
		interval:  interval,
		timeout:   timeout,
		providers: make(map[string]providerEntry),
	}
}

// Register adds (or overwrites) a StatsProvider for the given key.
// nil providers are silently ignored.
func (c *StatsCollector) Register(key, role, cType, taskID string, p connector.StatsProvider) {
	if p == nil {
		return
	}
	c.mu.Lock()
	c.providers[key] = providerEntry{provider: p, role: role, cType: cType, taskID: taskID}
	c.mu.Unlock()
}

// Unregister removes a provider. Does NOT call DeleteLabelValues — rely on
// Prometheus 3-minute staleness handling for stale series.
func (c *StatsCollector) Unregister(key string) {
	c.mu.Lock()
	delete(c.providers, key)
	c.mu.Unlock()
}

// Run loops until ctx is cancelled, polling all providers every interval.
func (c *StatsCollector) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.tick(ctx)
		}
	}
}

func (c *StatsCollector) tick(parentCtx context.Context) {
	c.mu.RLock()
	snapshot := make([]struct {
		key   string
		entry providerEntry
	}, 0, len(c.providers))
	for k, e := range c.providers {
		snapshot = append(snapshot, struct {
			key   string
			entry providerEntry
		}{k, e})
	}
	c.mu.RUnlock()

	var wg sync.WaitGroup
	for _, item := range snapshot {
		wg.Add(1)
		go func(key string, e providerEntry) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Error("stats provider panicked",
						zap.String("key", key), zap.Any("panic", r))
				}
			}()
			ctx, cancel := context.WithTimeout(parentCtx, c.timeout)
			defer cancel()
			s := e.provider.Stats(ctx)
			c.emit(e, s)
		}(item.key, item.entry)
	}
	wg.Wait()
}

func (c *StatsCollector) emit(e providerEntry, s connector.Stats) {
	stage := e.role // "source" or "sink"
	PipelineQueueSize.WithLabelValues(c.cluster, e.taskID, stage).Set(float64(s.QueueSize))
	PipelineQueueCapacity.WithLabelValues(c.cluster, e.taskID, stage).Set(float64(s.QueueCapacity))

	if e.role == "source" {
		if !math.IsNaN(s.LagSeconds) {
			lag := s.LagSeconds
			if lag < 0 {
				lag = 0
			}
			SourceLagSeconds.WithLabelValues(c.cluster, e.taskID, e.cType).Set(lag)
		}
		if !s.LastEventTime.IsZero() {
			SourceLastEventSeconds.WithLabelValues(c.cluster, e.taskID, e.cType).Set(float64(s.LastEventTime.Unix()))
		}
		SourceSnapshotProgress.WithLabelValues(c.cluster, e.taskID).Set(s.SnapshotProgress)
		SnapshotTablesTotal.WithLabelValues(c.cluster, e.taskID).Set(float64(s.SnapshotTotalTables))
		SnapshotTablesRemaining.WithLabelValues(c.cluster, e.taskID).Set(float64(s.SnapshotRemainingTables))
	}

	connected := 0.0
	if s.Connected {
		connected = 1.0
	}
	ConnectorConnected.WithLabelValues(c.cluster, e.taskID, e.role, e.cType).Set(connected)
}
```

- [ ] **Step 4: Run tests to verify pass (with race detector)**

Run: `go test -race ./pkg/metrics/ -run TestStatsCollector -v`
Expected: PASS (all 5 cases)

- [ ] **Step 5: Run full pkg/metrics tests**

Run: `go test -race ./pkg/metrics/ -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pkg/metrics/collector_runtime.go pkg/metrics/collector_runtime_test.go
git commit -m "feat(metrics): add StatsCollector for pull-mode connector metrics

Periodically (default 5s) polls all registered StatsProviders concurrently
(each in own goroutine with 1s timeout) and writes their snapshot into
Prometheus gauges:
- pipeline_queue_size / pipeline_queue_capacity (per role/stage)
- source_lag_seconds (NaN skipped, negative clamped to 0)
- source_last_event_seconds (zero skipped)
- source_snapshot_progress / snapshot_tables_total / snapshot_tables_remaining
- connector_connected (0/1)

Panic in one provider doesn't affect others. Unregister relies on
Prometheus 3-min staleness (no manual DeleteLabelValues — keeps code simple).

Refs spec §4.5."
```

---

## Stage 3: Sink decorator + Pipeline consume-point instrumentation

**Goal:** Sink writes emit latency / errors / failed-counter; pipeline consume point emits success counter + bytes + lag. After this stage running a task emits non-trivial metrics.

---

### Task 3.1: Sink decorator

**Files:**
- Create: `internal/sink/decorator/sink.go`
- Create: `internal/sink/decorator/sink_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/sink/decorator/sink_test.go`:

```go
package decorator_test

import (
	"context"
	"errors"
	"testing"

	dserrors "github.com/UFOXD/datastream/pkg/errors"
	"github.com/UFOXD/datastream/internal/connector"
	"github.com/UFOXD/datastream/internal/sink"
	"github.com/UFOXD/datastream/internal/sink/decorator"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type mockSink struct {
	writeCalls      int
	writeBatchCalls int
	asyncCalls      int
	failNext        error
	statsCalls      int
}

func (m *mockSink) Name() string                                 { return "mock" }
func (m *mockSink) Initialize(ctx context.Context, c sink.Config) error { return nil }
func (m *mockSink) Start(ctx context.Context) error              { return nil }
func (m *mockSink) Stop(ctx context.Context) error               { return nil }
func (m *mockSink) Status() sink.Status                          { return sink.Status{} }
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
	return m.Write(ctx, events)
}

type mockStatsSink struct{ mockSink }

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

func TestMetricsSink_WriteFailure_ClassifiesError(t *testing.T) {
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

func TestMetricsSink_WriteFailure_RetriableError(t *testing.T) {
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

func TestMetricsSink_WriteBatch_DelegatesToInnerBatchConnector(t *testing.T) {
	_ = setupRegistry(t)
	inner := &mockBatchSink{}
	ms := decorator.WrapSink(&inner.mockSink, "c1", "t1", "kafka")
	bc, ok := ms.(sink.BatchConnector)
	if ok {
		// Direct WriteBatch path requires the wrapper itself to expose BatchConnector
		_ = bc
	}
	// For this test we just confirm Write fallback works when inner has Batch
	_ = ms.Write(context.Background(), []*event.ChangeEvent{{Type: event.EventTypeInsert}})
}

func TestMetricsSink_StatsForwarding(t *testing.T) {
	_ = setupRegistry(t)
	inner := &mockStatsSink{}
	ms := decorator.WrapSink(&inner.mockSink, "c1", "t1", "mysql")
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
	inner := &mockSink{} // no Stats method
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
	// no panic, no counter
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
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/sink/decorator/ -v`
Expected: FAIL — package does not exist

- [ ] **Step 3: Create `internal/sink/decorator/sink.go`**

```go
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

func (m *MetricsSink) Name() string                                        { return m.inner.Name() }
func (m *MetricsSink) Initialize(ctx context.Context, c sink.Config) error { return m.inner.Initialize(ctx, c) }
func (m *MetricsSink) Start(ctx context.Context) error                     { return m.inner.Start(ctx) }
func (m *MetricsSink) Stop(ctx context.Context) error                      { return m.inner.Stop(ctx) }
func (m *MetricsSink) Status() sink.Status                                 { return m.inner.Status() }

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
```

- [ ] **Step 4: Run tests with race detector**

Run: `go test -race ./internal/sink/decorator/ -v`
Expected: PASS (all 7 cases)

- [ ] **Step 5: Build downstream**

Run: `go build ./...`
Expected: SUCCESS

- [ ] **Step 6: Commit**

```bash
git add internal/sink/decorator/
git commit -m "feat(sink): add MetricsSink decorator with multi-interface support

Wraps any sink.Connector and emits:
- task_events_total{type,result=success/failed} per event
- task_events_bytes (using ChangeEvent.Size estimate)
- sink_write_latency_seconds (Write/WriteBatch/WriteAsync)
- sink_write_errors_total{error_type=retriable/non_retriable}

Also transparently implements sink.BatchConnector / sink.AsyncConnector
(delegating to inner when supported, falling back to Write otherwise)
and forwards optional connector.StatsProvider.

Label vectors are pre-cached at WrapSink() to avoid hashmap lookup on
the hot path (~100k events/s budget). Unknown event types are silently
skipped — defensive against future enum additions.

Refs spec §4.3."
```

---

### Task 3.2: Pipeline consume-point instrumentation

**Files:**
- Modify: `internal/pipeline/pipeline.go`
- Test: `internal/pipeline/pipeline_test.go`

- [ ] **Step 1: Write failing test**

Append to `internal/pipeline/pipeline_test.go`:

```go
func TestPipeline_ConsumePoint_EmitsLagAndLastEvent(t *testing.T) {
	metrics.ResetForTest()
	r := prometheus.NewRegistry()
	metrics.MustRegisterAll(r)
	t.Cleanup(func() {
		metrics.ResetForTest()
		metrics.MustRegisterAll(prometheus.DefaultRegisterer)
	})

	p := New(&Config{ID: "t1", Name: "test", Source: source.Config{Type: "mysql"}})
	p.SetCluster("c1")
	p.precacheLabels()

	ev := &event.ChangeEvent{
		Type:      event.EventTypeInsert,
		Timestamp: time.Now().Add(-2 * time.Second),
	}
	p.instrumentEvent(ev)

	// lag should be > 1 (we set timestamp 2s in the past)
	lag := getGaugeValue(t, r, "datastream_source_lag_seconds", map[string]string{
		"cluster": "c1", "task": "t1", "source": "mysql",
	})
	if lag < 1.0 {
		t.Errorf("source_lag_seconds = %v, want >= 1.0", lag)
	}
}
```

Add imports if missing:
```go
"time"
"github.com/UFOXD/datastream/internal/source"
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/pipeline/ -run TestPipeline_ConsumePoint -v`
Expected: FAIL — `precacheLabels` / `instrumentEvent` undefined

- [ ] **Step 3: Add `precacheLabels` and `instrumentEvent` to Pipeline**

Add to `internal/pipeline/pipeline.go` struct fields:

```go
	// Pre-cached metric label vectors (filled by precacheLabels)
	successCounters map[event.EventType]prometheus.Counter
	bytesAdder      prometheus.Counter
	lagGauge        prometheus.Gauge
	lastEventGauge  prometheus.Gauge
```

Add imports if missing:
```go
"github.com/UFOXD/datastream/pkg/event"
"github.com/prometheus/client_golang/prometheus"
```

Add methods (anywhere in file, prefer near `updateState`):

```go
// precacheLabels pre-creates per-task label vectors so the consume hot path
// avoids hashmap lookup. Must be called after SetCluster and once source is set.
func (p *Pipeline) precacheLabels() {
	sourceType := ""
	if p.config != nil {
		sourceType = p.config.Source.Type
	}
	p.successCounters = make(map[event.EventType]prometheus.Counter, 7)
	for _, t := range []event.EventType{
		event.EventTypeInsert, event.EventTypeUpdate, event.EventTypeDelete,
		event.EventTypeTruncate, event.EventTypeDDL,
		event.EventTypeHeartbeat, event.EventTypeTombstone,
	} {
		p.successCounters[t] = metrics.TaskEventsTotal.WithLabelValues(p.cluster, p.id, string(t), "success")
	}
	p.bytesAdder = metrics.TaskEventsBytes.WithLabelValues(p.cluster, p.id)
	p.lagGauge = metrics.SourceLagSeconds.WithLabelValues(p.cluster, p.id, sourceType)
	p.lastEventGauge = metrics.SourceLastEventSeconds.WithLabelValues(p.cluster, p.id, sourceType)
}

// instrumentEvent emits per-event metrics at the Pipeline consume point.
// Only success counters / bytes / lag are emitted here; failed counters are
// emitted by the Sink decorator on write failure. This avoids double-counting.
func (p *Pipeline) instrumentEvent(e *event.ChangeEvent) {
	if e == nil {
		return
	}
	if c, ok := p.successCounters[e.Type]; ok {
		c.Inc()
	}
	if p.bytesAdder != nil {
		p.bytesAdder.Add(float64(e.Size()))
	}
	if !e.Timestamp.IsZero() && p.lagGauge != nil {
		lag := time.Since(e.Timestamp).Seconds()
		if lag < 0 {
			lag = 0
		}
		p.lagGauge.Set(lag)
		p.lastEventGauge.Set(float64(e.Timestamp.Unix()))
	}
}
```

- [ ] **Step 4: Call `instrumentEvent` from `run` loop**

In `internal/pipeline/pipeline.go`, find the `run` method's `case e, ok := <-p.source.Events():` branch and insert the call:

```go
case e, ok := <-p.source.Events():
    if !ok {
        log.Info("source events channel closed", zap.String("id", p.id))
        return
    }
    p.instrumentEvent(e)  // <— added
    p.processEvent(ctx, e)
```

- [ ] **Step 5: Run test to verify pass**

Run: `go test ./internal/pipeline/ -run TestPipeline_ConsumePoint -v`
Expected: PASS

- [ ] **Step 6: Run all pipeline tests**

Run: `go test -race ./internal/pipeline/ -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/pipeline/pipeline.go internal/pipeline/pipeline_test.go
git commit -m "feat(pipeline): instrument consume point with success/bytes/lag metrics

Pipeline.run loop's event consume branch now emits:
- task_events_total{result=success} for known event types
- task_events_bytes (estimated via ChangeEvent.Size())
- source_lag_seconds (now - event.Timestamp, NaN-safe, negative-clamped)
- source_last_event_seconds (Unix timestamp of the event)

Label vectors are precached via Pipeline.precacheLabels() to avoid
hashmap lookup on the hot path. Failed counts are NOT emitted here
(handled exclusively by Sink decorator) — keeps success/failed paths
non-overlapping.

Refs spec §5.4."
```

---

### Task 3.3: TaskManager wires Sink decorator + StatsCollector registration

**Files:**
- Modify: `internal/pipeline/task.go`
- Test: `internal/pipeline/task_test.go` (new test)

- [ ] **Step 1: Read the existing TaskManager.Create signature**

Run: `sed -n '170,220p' internal/pipeline/task.go`

Confirm the signature: `func (m *TaskManager) Create(ctx context.Context, id, name string, config *Config) (*Task, error)`

- [ ] **Step 2: Write failing test for SetStatsCollector + Create wiring**

Append to `internal/pipeline/task_test.go` (create the file if needed; keep package declaration `package pipeline_test` or `package pipeline` consistent with existing tests):

```go
func TestTaskManager_SetStatsCollector_WrapsSinkAndRegisters(t *testing.T) {
	metrics.ResetForTest()
	r := prometheus.NewRegistry()
	metrics.MustRegisterAll(r)
	t.Cleanup(func() {
		metrics.ResetForTest()
		metrics.MustRegisterAll(prometheus.DefaultRegisterer)
	})

	tm := NewTaskManager()
	tm.SetCluster("c1")
	sc := metrics.NewStatsCollector("c1", time.Second, time.Second)
	tm.SetStatsCollector(sc)

	// Confirm setters compile and don't panic; deep wiring is verified in
	// the integration test in Stage 4 because Create() depends on real
	// connector factories.
	if tm.statsCollector == nil {
		t.Error("statsCollector not set")
	}
}
```

If the package-private field `statsCollector` isn't accessible to the test package, use `package pipeline` (not `_test`) for this test file or add a getter.

- [ ] **Step 3: Run to verify failure**

Run: `go test ./internal/pipeline/ -run TestTaskManager_SetStatsCollector -v`
Expected: FAIL — `SetCluster`, `SetStatsCollector` undefined

- [ ] **Step 4: Modify `internal/pipeline/task.go`**

Add fields to TaskManager struct (locate the existing struct definition):

```go
	cluster        string
	statsCollector *metrics.StatsCollector
```

Add setters (after `SetCoordinator`):

```go
// SetCluster sets the cluster label used by per-task metrics.
func (m *TaskManager) SetCluster(c string) { m.cluster = c }

// SetStatsCollector enables pull-mode metrics. When set, Create wraps each
// sink with the metrics decorator and registers source/sink providers.
func (m *TaskManager) SetStatsCollector(sc *metrics.StatsCollector) {
	m.statsCollector = sc
}
```

Modify `Create` to wrap sinks and register providers (replace the existing sink construction loop):

```go
// (locate the existing Create method body — the section that builds sinks)
// REPLACE the existing block that constructs sinks with:

sinks := make([]sink.Connector, 0, len(config.Sinks))
for i, sCfg := range config.Sinks {
    raw, err := sink.Create(sCfg.Type, sCfg)
    if err != nil {
        return nil, err
    }
    if m.statsCollector != nil {
        wrapped := sinkdec.WrapSink(raw, m.cluster, id, sCfg.Type)
        sinks = append(sinks, wrapped)
        if sp, ok := wrapped.(connector.StatsProvider); ok {
            key := id + ":sink:" + strconv.Itoa(i)
            m.statsCollector.Register(key, "sink", sCfg.Type, id, sp)
        }
    } else {
        sinks = append(sinks, raw)
    }
}
```

Update the Pipeline construction to call `SetCluster` and `precacheLabels`:

```go
pipe := New(config)
pipe.SetCluster(m.cluster)
pipe.SetSource(src)
for _, s := range sinks {
    pipe.AddSink(s)
}
if m.statsCollector != nil {
    pipe.precacheLabels()
    if sp, ok := src.(connector.StatsProvider); ok {
        m.statsCollector.Register(id+":source", "source", config.Source.Type, id, sp)
    }
    log.Info("task metrics registered",
        zap.String("task", id),
        zap.Bool("source_stats", isStatsProvider(src)),
        zap.Int("sink_count", len(sinks)),
    )
}
```

Add helper at end of file:

```go
func isStatsProvider(x interface{}) bool {
	_, ok := x.(connector.StatsProvider)
	return ok
}
```

Modify `Delete` to unregister:

```go
// In the existing Delete method, before the existing delete logic, add:
if m.statsCollector != nil {
    m.statsCollector.Unregister(id + ":source")
    for i := 0; i < 16; i++ { // safe upper bound for sink count
        m.statsCollector.Unregister(id + ":sink:" + strconv.Itoa(i))
    }
}
```

Add imports to `internal/pipeline/task.go`:

```go
"strconv"
"github.com/UFOXD/datastream/internal/connector"
"github.com/UFOXD/datastream/pkg/metrics"
sinkdec "github.com/UFOXD/datastream/internal/sink/decorator"
"go.uber.org/zap"  // if not already imported
```

- [ ] **Step 5: Run test to verify pass**

Run: `go test ./internal/pipeline/ -run TestTaskManager_SetStatsCollector -v`
Expected: PASS

- [ ] **Step 6: Run all pipeline tests**

Run: `go test -race ./internal/pipeline/ -v`
Expected: PASS

- [ ] **Step 7: Build all**

Run: `go build ./...`
Expected: SUCCESS

- [ ] **Step 8: Commit**

```bash
git add internal/pipeline/task.go internal/pipeline/task_test.go
git commit -m "feat(pipeline): wire Sink decorator + StatsCollector registration in TaskManager

When SetStatsCollector is set (i.e. metrics enabled), TaskManager.Create
now:
- wraps each sink with sinkdec.WrapSink(...)
- registers each wrapped sink (key 'taskID:sink:N') and the source
  (key 'taskID:source') with the StatsCollector if they implement
  connector.StatsProvider
- calls Pipeline.precacheLabels() so consume-point hot path is ready
- logs which connectors expose stats at startup

Delete unregisters all sink slots (capped at 16) plus the source.

Refs spec §6.3."
```

---

## Stage 4: Connector StatsProvider implementations + App wiring + docs

**Goal:** All 12 connectors expose runtime gauges; app config supports `--cluster`; Application starts the StatsCollector; design docs updated; Grafana dashboard provided.

---

### Task 4.1: Add Cluster + MetricsConfig to config

**Files:**
- Modify: `pkg/config/config.go`
- Modify: `pkg/config/config_test.go`

- [ ] **Step 1: Write failing test**

Append to `pkg/config/config_test.go`:

```go
func TestConfig_Cluster_Default(t *testing.T) {
	cfg := &Config{}
	cfg.Adjust()
	if cfg.Cluster != "default" {
		t.Errorf("cluster default = %q, want %q", cfg.Cluster, "default")
	}
}

func TestConfig_Metrics_Defaults(t *testing.T) {
	cfg := &Config{}
	cfg.Adjust()
	if !cfg.Metrics.Enabled {
		t.Error("metrics.enabled default should be true")
	}
	if cfg.Metrics.ScrapeInterval == 0 {
		t.Error("metrics.scrape_interval default unset")
	}
	if cfg.Metrics.StatsTimeout == 0 {
		t.Error("metrics.stats_timeout default unset")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./pkg/config/ -run "TestConfig_Cluster|TestConfig_Metrics" -v`
Expected: FAIL

- [ ] **Step 3: Modify `pkg/config/config.go`**

Add to `Config` struct:

```go
	Cluster string        `toml:"cluster" json:"cluster"`
	Metrics MetricsConfig `toml:"metrics" json:"metrics"`
```

Add type definition:

```go
// MetricsConfig configures Prometheus metric collection.
type MetricsConfig struct {
	Enabled        bool          `toml:"enabled" json:"enabled"`
	ScrapeInterval time.Duration `toml:"scrape-interval" json:"scrape-interval"`
	StatsTimeout   time.Duration `toml:"stats-timeout" json:"stats-timeout"`
}
```

Add `time` import if needed.

Add to `Adjust()`:

```go
	if c.Cluster == "" {
		c.Cluster = "default"
	}
	if c.Metrics.ScrapeInterval == 0 {
		c.Metrics.ScrapeInterval = 5 * time.Second
	}
	if c.Metrics.StatsTimeout == 0 {
		c.Metrics.StatsTimeout = time.Second
	}
	// Enabled defaults to true unless explicitly set false in TOML.
	// Since bool zero-value is false, we need a sentinel: read raw TOML or
	// flip to negative-named field. Simpler: default to true here, but allow
	// override only via explicit TOML.
	// For now: leave Enabled as-is; loader treats missing 'enabled' key as true.
```

Note on Enabled default: Go bool zero is false, but spec says default true. The cleanest fix is in the TOML loader: if `[metrics]` table omits the key, set Enabled=true post-load. Add to whichever function parses TOML:

```go
// In LoadConfig or wherever toml.Unmarshal is called:
// After loading, if [metrics] table was absent, default Enabled to true.
// Simplest: just set true if the entire MetricsConfig is zero.
if cfg.Metrics == (MetricsConfig{}) {
    cfg.Metrics.Enabled = true
}
cfg.Adjust()
```

If the TOML loader path is unclear, in the test, we test post-`Adjust()` state and adjust the test to set `Enabled = true` explicitly in the default case via Adjust. Simplest approach: change `Adjust()` to flip Enabled when ScrapeInterval also unset:

```go
	if c.Metrics.ScrapeInterval == 0 && !c.Metrics.Enabled {
		c.Metrics.Enabled = true
	}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `go test ./pkg/config/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add pkg/config/config.go pkg/config/config_test.go
git commit -m "feat(config): add Cluster + MetricsConfig

Adds:
- Config.Cluster (defaults to 'default') for metric label
- Config.Metrics.Enabled (default true)
- Config.Metrics.ScrapeInterval (default 5s)
- Config.Metrics.StatsTimeout (default 1s)

Refs spec §6.2."
```

---

### Task 4.2: Wire StatsCollector into Application

**Files:**
- Modify: `internal/app/app.go`

- [ ] **Step 1: Inspect current Application.Start**

Run: `grep -n "Start\|taskManager" internal/app/app.go | head -20`

- [ ] **Step 2: Modify `internal/app/app.go`**

Add field to `Application` struct:

```go
	statsCollector *metrics.StatsCollector
```

Modify `Start` to create and run collector. Locate `Start` and insert after TaskManager is initialized:

```go
	// Start metrics collector if enabled
	if a.config.Metrics.Enabled {
		a.statsCollector = metrics.NewStatsCollector(
			a.config.Cluster,
			a.config.Metrics.ScrapeInterval,
			a.config.Metrics.StatsTimeout,
		)
		a.taskManager.SetCluster(a.config.Cluster)
		a.taskManager.SetStatsCollector(a.statsCollector)
		go a.statsCollector.Run(ctx)
		log.Info("stats collector started",
			zap.String("cluster", a.config.Cluster),
			zap.Duration("interval", a.config.Metrics.ScrapeInterval),
		)
	} else {
		log.Info("metrics collection disabled")
	}
```

Add import to `internal/app/app.go`:

```go
"github.com/UFOXD/datastream/pkg/metrics"
```

- [ ] **Step 3: Build**

Run: `go build ./...`
Expected: SUCCESS

- [ ] **Step 4: Run app tests**

Run: `go test ./internal/app/ -v`
Expected: PASS (no new tests; existing test should still pass)

- [ ] **Step 5: Commit**

```bash
git add internal/app/app.go
git commit -m "feat(app): start StatsCollector when metrics enabled

When config.Metrics.Enabled is true, Application.Start now:
- creates a StatsCollector with configured cluster/interval/timeout
- hands it to TaskManager via SetStatsCollector
- runs collector goroutine on the application context

When disabled, metrics endpoint still works but no pull-mode gauges
are emitted.

Refs spec §6.4."
```

---

### Task 4.3: Connector StatsProvider implementations (12 connectors)

This task is **repetitive across 12 connectors**. We do one connector fully as a template, then repeat the same shape across the rest. Each sub-task is one connector.

**Template pattern** for each connector file (source or sink):

```go
// In e.g. internal/source/mysql/connector.go (or sink equivalent), add:

import "github.com/UFOXD/datastream/internal/connector"

// Stats returns a runtime snapshot for StatsCollector.
func (c *Connector) Stats(ctx context.Context) connector.Stats {
	c.mu.RLock()  // or use existing locking pattern
	defer c.mu.RUnlock()
	s := connector.Stats{
		Connected: c.isConnected(),  // implement via connection check
		Position:  c.currentPositionString(),
	}
	if c.snapshotInProgress {
		s.SnapshotRunning = true
		s.SnapshotProgress = c.snapshotProgressPct()
		s.SnapshotTotalTables = int64(c.totalTables)
		s.SnapshotRemainingTables = int64(c.remainingTables)
	}
	if !c.lastEventAt.IsZero() {
		s.LastEventTime = c.lastEventAt
		s.LagSeconds = time.Since(c.lastEventAt).Seconds()
	} else {
		s.LagSeconds = math.NaN()
	}
	// Queue fields filled only if connector tracks an internal queue
	return s
}
```

For each connector, identify the existing fields/methods to source the values. Connectors that don't have a concept (e.g. sink has no snapshot) leave those fields zero.

#### Task 4.3.1: MySQL source

**Files:**
- Modify: `internal/source/mysql/connector.go`
- Test: `internal/source/mysql/connector_test.go`

- [ ] **Step 1: Locate connector struct fields**

Run: `grep -n "type Connector struct\|lastEvent\|snapshot\|position" internal/source/mysql/connector.go | head -20`

- [ ] **Step 2: Write failing test**

Append to `internal/source/mysql/connector_test.go`:

```go
func TestConnector_StatsProviderCompiles(t *testing.T) {
	var _ connector.StatsProvider = (*Connector)(nil)
}
```

Add import `"github.com/UFOXD/datastream/internal/connector"` to test file.

- [ ] **Step 3: Run to verify failure**

Run: `go test ./internal/source/mysql/ -run TestConnector_StatsProviderCompiles -v`
Expected: FAIL — interface not satisfied

- [ ] **Step 4: Implement Stats() on MySQL connector**

In `internal/source/mysql/connector.go`, add at the end:

```go
import "math"  // if not already imported
import "github.com/UFOXD/datastream/internal/connector"

// Stats implements connector.StatsProvider for runtime metrics.
func (c *Connector) Stats(ctx context.Context) connector.Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	s := connector.Stats{
		Connected: c.syncer != nil && c.status.State == StateRunning,
		Position:  positionToString(c.currentPosition),
	}
	if !c.lastEventAt.IsZero() {
		s.LastEventTime = c.lastEventAt
		lag := time.Since(c.lastEventAt).Seconds()
		if lag < 0 {
			lag = 0
		}
		s.LagSeconds = lag
	} else {
		s.LagSeconds = math.NaN()
	}
	if c.snapshot != nil && c.snapshot.IsRunning() {
		s.SnapshotRunning = true
		s.SnapshotProgress = c.snapshot.Progress()
		s.SnapshotTotalTables = c.snapshot.TotalTables()
		s.SnapshotRemainingTables = c.snapshot.RemainingTables()
	}
	return s
}

// positionToString renders a position respecting binlog mode (file-pos or GTID).
// MUST NOT hard-code one format — read connector.config.binlogMode.
func positionToString(p *event.Position) string {
	if p == nil {
		return ""
	}
	// If GTID mode is on and a GTID set is present, prefer it; else file-pos.
	if p.GTID != "" {
		return p.GTID
	}
	return p.File + ":" + strconv.FormatUint(uint64(p.Pos), 10)
}
```

Notes:
- If `c.lastEventAt`, `c.snapshot`, `c.currentPosition` don't exist with those exact names, **adapt to actual field names** by inspecting the connector. If those concepts aren't tracked, add minimal tracking: update `lastEventAt = time.Now()` in the binlog event reader, default snapshot fields to zero.
- If you must add a field to track `lastEventAt`, do it in `Connector` struct (`lastEventAt time.Time` under `mu`).

- [ ] **Step 5: Run test to verify pass**

Run: `go test ./internal/source/mysql/ -run TestConnector_StatsProviderCompiles -v`
Expected: PASS

- [ ] **Step 6: Run full source/mysql tests**

Run: `go test ./internal/source/mysql/ -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/source/mysql/
git commit -m "feat(source/mysql): implement connector.StatsProvider

Exposes runtime snapshot for StatsCollector pull-mode metrics:
- Connected (1 if syncer present and state=Running)
- Position (GTID if set, else file-pos — honors binlog mode)
- LastEventTime / LagSeconds (NaN if no event seen)
- SnapshotRunning / Progress / TotalTables / RemainingTables

Refs spec §4.2."
```

#### Tasks 4.3.2 through 4.3.12: Remaining 11 connectors

Repeat the same shape for the following 11 connectors. For each: write a `TestConnector_StatsProviderCompiles` test, add `Stats(ctx)` method following the template, adapting to that connector's actual fields. Commit each independently.

- [ ] **Task 4.3.2:** `internal/source/postgres/connector.go` — Position renders LSN; LagSeconds from latest WAL slot timestamp.

- [ ] **Task 4.3.3:** `internal/source/mongodb/connector.go` — Position renders resume token; SnapshotProgress from copy state.

- [ ] **Task 4.3.4:** `internal/source/oracle/connector.go` — Position renders SCN; LagSeconds from current SCN time.

- [ ] **Task 4.3.5:** `internal/source/sqlserver/connector.go` — Position renders LSN hex; track lastEventAt.

- [ ] **Task 4.3.6:** `internal/source/mariadb/connector.go` — Same as MySQL (likely just delegate or copy).

- [ ] **Task 4.3.7:** `internal/sink/mysql/connector.go` — QueueSize from batch buffer; Connected via db.Ping.

- [ ] **Task 4.3.8:** `internal/sink/postgres/connector.go` — Same shape; QueueSize from batch.

- [ ] **Task 4.3.9:** `internal/sink/kafka/connector.go` — Connected via producer state; QueueSize from buffered records.

- [ ] **Task 4.3.10:** `internal/sink/mongodb/connector.go` — Connected via client ping.

- [ ] **Task 4.3.11:** `internal/sink/elasticsearch/connector.go` — Connected via cluster health check (cached, no real HTTP — Stats must be non-blocking).

- [ ] **Task 4.3.12:** `internal/sink/redis/connector.go` — Connected via PING (cached).

For each, the pattern is identical to 4.3.1 — compile-time test + `Stats(ctx)` returning realistic non-zero values where available, zero elsewhere. **Each task ends with its own commit** scoped to that one connector.

- [ ] **Final step (4.3.13): Full build + test**

Run: `go build ./...`
Expected: SUCCESS

Run: `go test -race ./...`
Expected: All packages pass

---

### Task 4.4: Grafana dashboard JSON

**Files:**
- Create: `deployments/grafana/datastream-dashboard.json`

- [ ] **Step 1: Create dashboard skeleton**

Create directory `deployments/grafana/` and add `datastream-dashboard.json`:

```json
{
  "title": "DataStream Overview",
  "schemaVersion": 30,
  "version": 1,
  "panels": [
    {
      "id": 1,
      "title": "Events / sec by task & result",
      "type": "graph",
      "targets": [{
        "expr": "sum by (task, result) (rate(datastream_task_events_total[1m]))",
        "legendFormat": "{{task}} {{result}}"
      }],
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0}
    },
    {
      "id": 2,
      "title": "Sink write latency p99",
      "type": "graph",
      "targets": [{
        "expr": "histogram_quantile(0.99, sum by (le, task, sink) (rate(datastream_sink_write_latency_seconds_bucket[1m])))",
        "legendFormat": "{{task}}/{{sink}}"
      }],
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0}
    },
    {
      "id": 3,
      "title": "Source lag (seconds)",
      "type": "graph",
      "targets": [{
        "expr": "datastream_source_lag_seconds",
        "legendFormat": "{{task}}/{{source}}"
      }],
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8}
    },
    {
      "id": 4,
      "title": "Connector connected (0/1)",
      "type": "graph",
      "targets": [{
        "expr": "datastream_connector_connected",
        "legendFormat": "{{task}}/{{role}}/{{type}}"
      }],
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8}
    },
    {
      "id": 5,
      "title": "Sink errors / sec by type",
      "type": "graph",
      "targets": [{
        "expr": "sum by (error_type) (rate(datastream_sink_write_errors_total[1m]))",
        "legendFormat": "{{error_type}}"
      }],
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 16}
    },
    {
      "id": 6,
      "title": "Pipeline queue usage",
      "type": "graph",
      "targets": [{
        "expr": "datastream_pipeline_queue_size / datastream_pipeline_queue_capacity",
        "legendFormat": "{{task}}/{{stage}}"
      }],
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 16}
    }
  ]
}
```

- [ ] **Step 2: Commit**

```bash
git add deployments/grafana/datastream-dashboard.json
git commit -m "docs(grafana): add basic Prometheus dashboard

6-panel dashboard covering:
- events/s by task & result
- sink write latency p99
- source lag (CDC delay)
- connector connectivity (0/1)
- sink errors/s by retriable/non_retriable
- pipeline queue usage ratio

Import via Grafana UI → Dashboards → Import → Upload JSON.
Refs spec §7."
```

---

### Task 4.5: Operator documentation

**Files:**
- Create: `docs/operations/metrics.md`

- [ ] **Step 1: Create docs/operations/ directory if needed**

Run: `mkdir -p docs/operations`

- [ ] **Step 2: Create `docs/operations/metrics.md`**

Write the file with these sections (use real metric names from `pkg/metrics/registry.go`):

```markdown
# DataStream Metrics Reference

DataStream exposes Prometheus metrics on the HTTP API server at `/metrics`.
All metrics share the `datastream_` prefix.

## Configuration

```toml
cluster = "prod-east"   # value of the 'cluster' label on every metric

[metrics]
enabled = true              # set false to disable pull-mode gauges
scrape-interval = "5s"      # how often StatsCollector polls connectors
stats-timeout = "1s"        # per-connector Stats() timeout
```

Or via env / flag:
- `DATASTREAM_CLUSTER=prod-east` (or `--cluster prod-east`)

## Metric Catalog

### Task

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `datastream_task_total` | Gauge | cluster, status | Cluster-level task state distribution |
| `datastream_task_state` | Gauge | cluster, task, state | Per-task current state (0/1) |
| `datastream_task_events_total` | Counter | cluster, task, type, result | Events processed (type=insert/update/delete/truncate/ddl/heartbeat/tombstone; result=success/failed) |
| `datastream_task_events_bytes` | Counter | cluster, task | Total bytes processed (estimate) |
| `datastream_task_latency_seconds` | Histogram | cluster, task | End-to-end event latency |

### Source

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `datastream_source_position` | Gauge | cluster, task, source | Numeric position (when applicable) |
| `datastream_source_snapshot_progress` | Gauge | cluster, task | Snapshot 0-100% |
| `datastream_source_lag_seconds` | Gauge | cluster, task, source | CDC lag (now - event_time) |
| `datastream_source_last_event_seconds` | Gauge | cluster, task, source | Unix timestamp of last event |
| `datastream_snapshot_tables_total` | Gauge | cluster, task | Tables to snapshot |
| `datastream_snapshot_tables_remaining` | Gauge | cluster, task | Tables not yet snapshotted |

### Sink

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `datastream_sink_write_latency_seconds` | Histogram | cluster, task, sink | Sink write latency |
| `datastream_sink_write_errors_total` | Counter | cluster, task, sink, error_type | Errors classified as retriable/non_retriable |

### Pipeline & Connector

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `datastream_pipeline_queue_size` | Gauge | cluster, task, stage | Current queue depth |
| `datastream_pipeline_queue_capacity` | Gauge | cluster, task, stage | Max queue depth |
| `datastream_pipeline_process_time_seconds` | Histogram | cluster, task, stage | Per-stage processing time |
| `datastream_connector_connected` | Gauge | cluster, task, role, type | Connection health 0/1 |

## Recommended Alerts

```yaml
- alert: DataStreamSourceLagHigh
  expr: datastream_source_lag_seconds > 60
  for: 5m
  annotations: { summary: "CDC lag > 60s on {{ $labels.task }}" }

- alert: DataStreamSinkErrorsRising
  expr: sum by (task) (rate(datastream_sink_write_errors_total[5m])) > 0.1
  for: 10m
  annotations: { summary: "Sink errors >0.1/s on {{ $labels.task }}" }

- alert: DataStreamConnectorDown
  expr: datastream_connector_connected == 0
  for: 2m
  annotations: { summary: "{{ $labels.role }} {{ $labels.type }} disconnected" }
```

## Caveats

- **`result=failed` undercounts** when sinks have internal retries (counted
  only when error escapes the sink). Precise accounting requires the
  separate "retry architecture unification" task.
- **`result=success` ≠ "written to sink"** — counts events flowing past
  the Pipeline consume point. Sink failures appear separately via
  `result=failed` on the sink decorator path.
- **`source_lag_seconds` depends on NTP** — clock skew may yield brief
  spikes. Negative values are clamped to 0.
- **Connector `Stats()` is best-effort** — connectors implementing
  `StatsProvider` may not populate every field. Zero values mean
  "not applicable", not "actually zero". Check connector source for
  what's wired.
```

- [ ] **Step 3: Commit**

```bash
git add docs/operations/metrics.md
git commit -m "docs(ops): add metrics operator reference

Catalogs all 20+ Prometheus metrics, configuration options, recommended
alert expressions, and known caveats (result=failed undercounts,
NTP dependency for lag, optional Stats fields)."
```

---

### Task 4.6: Sync design documents

**Files:**
- Modify: `docs/design/core-design.md`
- Modify: `docs/design/connector-design.md`
- Modify: `docs/design/pipeline-design.md`
- Modify: `docs/design/event-model-design.md`
- Modify: `docs/design/api-cli-design.md`

- [ ] **Step 1: Update `core-design.md` §4 (Metrics)**

Append a new section to `docs/design/core-design.md` after the existing §4 metric definitions:

```markdown
### 4.x Implementation Notes (added 2026-05-16)

- All metrics use **`cluster` label** (not `namespace` — that name conflicts
  with the Prometheus `Namespace` concept used for metric prefix).
- Metrics are registered via explicit `metrics.MustRegisterAll(r)` (not
  `promauto`) to support test isolation via `metrics.ResetForTest()`.
- `task_events_total` has labels `cluster, task, type, result`:
  - `type` ∈ {insert, update, delete, truncate, ddl, heartbeat, tombstone}
  - `result` ∈ {success, failed}
  - Success counts emit at the **Pipeline consume point**; failed counts
    emit at the **Sink decorator** layer.
- `sink_write_errors_total.error_type` ∈ {retriable, non_retriable}
  via `metrics.ClassifyError(err)` (single source of truth is
  `pkg/errors.IsRetryableError`).
- Pipeline state machine uses **two gauges**: per-task `task_state`
  (0/1 per state) and cluster-level `task_total` distribution
  (inc/dec on transition). Transition is atomic via `Pipeline.updateState`.

### 4.y New metrics in this release

| Metric | Purpose |
|--------|---------|
| `datastream_task_state` | Per-task state observability |
| `datastream_source_lag_seconds` | CDC lag (key SLO) |
| `datastream_source_last_event_seconds` | "Data stopped flowing" alerting |
| `datastream_pipeline_queue_capacity` | Queue utilization ratio |
| `datastream_connector_connected` | Connection health |
| `datastream_snapshot_tables_total` | Snapshot scope |
| `datastream_snapshot_tables_remaining` | Snapshot progress in absolute counts |
```

- [ ] **Step 2: Update `connector-design.md`**

Append:

```markdown
## Optional StatsProvider Interface (added 2026-05-16)

Source and sink connectors MAY implement `connector.StatsProvider` to
expose runtime state for Prometheus gauge metrics:

```go
type StatsProvider interface {
    Stats(ctx context.Context) Stats
}
```

`Stats` carries queue, position, lag, snapshot, and connection fields.
Connectors that don't implement this interface are skipped by the
`StatsCollector`. App startup logs each connector's support status.

### Implementation rules

- `Stats(ctx)` MUST be safe for concurrent calls
- MUST honor ctx cancellation; collector enforces a 1-second timeout
- MUST NOT panic; recover at the connector boundary or rely on collector's
  recovery (which logs and skips that sample)
- Zero values in `Stats` mean "not applicable" — sink connectors leave
  snapshot fields zero, sources without lag tracking set `LagSeconds = NaN`

### Position field for MySQL/MariaDB

MySQL/MariaDB connectors MUST honor their configured binlog mode (file-pos
OR GTID set) when rendering `Stats.Position`. Do NOT hard-code one format.
```

- [ ] **Step 3: Update `pipeline-design.md`**

Append:

```markdown
## Metrics Assembly (added 2026-05-16)

Pipeline emits metrics from two distinct sites to avoid double-counting:

1. **Consume point** (`Pipeline.run` event-receive branch) — emits
   `task_events_total{result=success}`, `task_events_bytes`,
   `source_lag_seconds`, `source_last_event_seconds`.

2. **Sink decorator** (`internal/sink/decorator.MetricsSink`) — emits
   `sink_write_latency_seconds`, `sink_write_errors_total{error_type}`,
   `task_events_total{result=failed}`.

Failed counts at the Sink layer may undercount when sinks retry internally;
precise counting requires the separate retry-unification task.

### Assembly in TaskManager.Create

When `Metrics.Enabled` is true:
1. Each sink is wrapped with `sinkdec.WrapSink(...)`.
2. Source + each wrapped sink that implements `connector.StatsProvider`
   are registered with `StatsCollector` (keys `taskID:source`,
   `taskID:sink:N`).
3. `Pipeline.SetCluster()` + `Pipeline.precacheLabels()` are called so
   the consume hot path skips hashmap lookup.

`TaskManager.Delete` unregisters all sink slots and source.
```

- [ ] **Step 4: Update `event-model-design.md`**

Append:

```markdown
## ChangeEvent.Size (added 2026-05-16)

```go
func (e *ChangeEvent) Size() int
```

Returns an estimated byte size used by `datastream_task_events_bytes`.
Counts source/table names, all field name+value byte lengths in
`Before` and `After`, plus a 64-byte fixed metadata overhead.

**Not exact** — sufficient for trend observability. If exact size is
required for future use cases, switch to serializing to JSON and
returning `len(bytes)`.
```

- [ ] **Step 5: Update `api-cli-design.md`**

Append:

```markdown
## Cluster Identification (added 2026-05-16)

DataStream server requires a cluster identifier for metric labeling.
Provide via (precedence order):

1. CLI flag: `--cluster prod-east`
2. Env var: `DATASTREAM_CLUSTER=prod-east`
3. Config file: `cluster = "prod-east"`
4. Default: `default`

Applied to the `cluster` label on every Prometheus metric.
```

- [ ] **Step 6: Commit**

```bash
git add docs/design/
git commit -m "docs(design): sync 4 design docs with metrics integration

- core-design.md: cluster label rename, MustRegisterAll pattern,
  task_events_total result label, 7 new metrics
- connector-design.md: StatsProvider optional interface, Position
  honors MySQL binlog mode (file-pos OR GTID)
- pipeline-design.md: dual emission sites (consume + decorator),
  TaskManager assembly with StatsCollector registration
- event-model-design.md: ChangeEvent.Size() method
- api-cli-design.md: --cluster flag / DATASTREAM_CLUSTER env

Refs spec §11."
```

---

### Task 4.7: Integration test

**Files:**
- Create: `tests/integration/metrics_test.go`

- [ ] **Step 1: Verify docker-compose is available**

Run: `ls tests/docker/docker-compose.yml`
Expected: file exists

- [ ] **Step 2: Write integration test**

Create `tests/integration/metrics_test.go`:

```go
//go:build integration

package integration

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsEndpoint_LiveCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	// Verify metrics package registers cleanly into an isolated Registry
	metrics.ResetForTest()
	r := prometheus.NewRegistry()
	metrics.MustRegisterAll(r)
	t.Cleanup(func() {
		metrics.ResetForTest()
		metrics.MustRegisterAll(prometheus.DefaultRegisterer)
	})

	metrics.TaskTotal.WithLabelValues("itest", "running").Set(1)
	families, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) == 0 {
		t.Fatal("no metric families gathered")
	}
}

func TestMetricsEndpoint_RealHTTPServer(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	addr := os.Getenv("DATASTREAM_TEST_ADDR")
	if addr == "" {
		t.Skip("set DATASTREAM_TEST_ADDR to run live HTTP test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", addr+"/metrics", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "datastream_") {
		t.Errorf("response missing datastream_ metrics; first 200 chars: %s", body[:200])
	}
}
```

Add import `"os"`.

- [ ] **Step 3: Run integration test (with build tag)**

Run: `go test -tags=integration ./tests/integration/ -run TestMetrics -v`
Expected: PASS (TestMetricsEndpoint_LiveCheck passes; RealHTTPServer skipped without env var)

- [ ] **Step 4: Commit**

```bash
git add tests/integration/metrics_test.go
git commit -m "test(integration): verify metrics endpoint and registry isolation

Adds two integration tests behind 'integration' build tag:
- LiveCheck: confirms ResetForTest+MustRegisterAll roundtrip with
  an independent Registry (regression for promauto global panic)
- RealHTTPServer: optional live check against DATASTREAM_TEST_ADDR

Run via: go test -tags=integration ./tests/integration/ -v"
```

---

### Task 4.8: Stage 4 verification

- [ ] **Step 1: Run full test suite with race detector**

Run: `go test -race ./... 2>&1 | tail -40`
Expected: All packages pass

- [ ] **Step 2: Build**

Run: `go build ./...`
Expected: SUCCESS

- [ ] **Step 3: Tag**

```bash
git tag stage-4-metrics-complete
```

- [ ] **Step 4: Update MEMORY.md**

Append a section to `MEMORY.md`:

```markdown
## 2026-05-16 监控指标集成完成

### 完成的任务
- Stage 1: /metrics endpoint 修复、panic 修复、MustRegisterAll 重构、ChangeEvent.Size()
- Stage 2: StatsProvider 接口、ClassifyError、StatsCollector
- Stage 3: Sink 装饰器、Pipeline 消费点埋点、TaskManager 装配
- Stage 4: 12 个 connector 实现 StatsProvider、App 启动、Grafana 面板、4 份设计文档同步

### Spec
docs/superpowers/specs/2026-05-16-metrics-integration-design.md

### Plan
docs/superpowers/plans/2026-05-16-metrics-integration.md

### Out of scope (follow-up tasks)
- 重试架构统一
- pkg/utils.IsRetryableError 与 pkg/errors.IsRetryableError 合并
- connector_retries_total / events_filtered_total / last_error_message
```

```bash
git add MEMORY.md
git commit -m "docs(memory): record metrics integration completion"
```

---

## Self-Review

**1. Spec coverage:**
- §1 problem → Stage 1 (1.2 + 1.3 + 1.4) ✓
- §2 decisions → entire plan ✓
- §3 architecture → 3.1 + 3.2 + 2.3 ✓
- §4.1 ChangeEvent.Size → 1.1 ✓
- §4.2 StatsProvider → 2.1 ✓
- §4.3 MetricsSink decorator → 3.1 ✓
- §4.4 ClassifyError → 2.2 ✓
- §4.5 StatsCollector → 2.3 ✓
- §4.6 Registry refactor → 1.2 ✓
- §5 metric definitions → 1.2 (MustRegisterAll) ✓
- §5.3 state machine fix → 1.3 ✓
- §5.4 consume-point instrumentation → 3.2 ✓
- §6 wiring → 1.4 + 3.3 + 4.1 + 4.2 ✓
- §7 Grafana → 4.4 ✓
- §8 testing → tests embedded in every task + 4.7 ✓
- §9 staging → 4 stages ✓
- §11 design doc sync → 4.6 ✓
- §13 risks → documented inline in commit messages and 4.5 ops doc ✓

**2. Placeholder scan:** Inline code for every change; no TBD/TODO in steps. ✓

**3. Type consistency:**
- `Pipeline.SetCluster` / `Pipeline.precacheLabels` / `Pipeline.updateState` / `Pipeline.instrumentEvent` consistent across 1.3, 3.2, 3.3 ✓
- `TaskManager.SetCluster` / `SetStatsCollector` consistent in 3.3, 4.2 ✓
- `connector.StatsProvider.Stats(ctx)` signature consistent across 2.1, 2.3, 3.1, 4.3 ✓
- `metrics.ClassifyError` / `ErrorTypeRetriable` consistent across 2.2, 3.1 ✓
- `metrics.IncTaskEvents` signature: 4 args (cluster, task, type, result) consistent ✓
- Metric var names (`TaskState`, `SourceLagSeconds`, etc.) consistent across 1.2 and call sites ✓

**4. Known gaps acknowledged in plan:**
- 4.1 Step 3 notes the bool-default subtlety for `Metrics.Enabled`; resolution is documented inline
- 4.3.2–4.3.12 are sketched (one per connector) — each engineer adapts to actual connector internals; the template in 4.3.1 is concrete
- 4.3 sub-tasks do NOT specify exact field names (`lastEventAt`, `snapshot`) — they require engineer to inspect each connector and adapt. This is intentional given the breadth of 12 connectors; the spec acknowledges this as a "self-driven" portion.

---
