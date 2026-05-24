package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// newLifecycleTestRegistry creates an isolated registry, registers the
// lifecycle metrics into it, and returns the registry. Deferred cleanup
// nils the package vars so the next test starts clean.
func newLifecycleTestRegistry(t *testing.T) *prometheus.Registry {
	t.Helper()
	resetLifecycleVars()
	r := prometheus.NewRegistry()
	registerLifecycleMetricsTo(r)
	t.Cleanup(resetLifecycleVars)
	return r
}

func TestUpdateTableStateMetric(t *testing.T) {
	r := newLifecycleTestRegistry(t)

	UpdateTableStateMetric("task-1", "db.users", "streaming")

	families, err := r.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}

	stateValues := make(map[string]float64) // state -> value
	for _, f := range families {
		if f.GetName() != "datastream_table_state" {
			continue
		}
		for _, m := range f.GetMetric() {
			var state string
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "state" {
					state = lp.GetValue()
				}
			}
			stateValues[state] = m.GetGauge().GetValue()
		}
	}

	allStates := []string{"pending", "snapshotting", "catching_up", "streaming", "error", "paused"}
	for _, s := range allStates {
		want := float64(0)
		if s == "streaming" {
			want = 1
		}
		got, ok := stateValues[s]
		if !ok {
			t.Errorf("state %q not found in gathered metrics", s)
			continue
		}
		if got != want {
			t.Errorf("state %q: got %v, want %v", s, got, want)
		}
	}
}

func TestUpdateTableStateMetric_Transition(t *testing.T) {
	r := newLifecycleTestRegistry(t)

	// Start in pending, then move to snapshotting.
	UpdateTableStateMetric("task-1", "db.orders", "pending")
	UpdateTableStateMetric("task-1", "db.orders", "snapshotting")

	families, err := r.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}

	for _, f := range families {
		if f.GetName() != "datastream_table_state" {
			continue
		}
		for _, m := range f.GetMetric() {
			var state string
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "state" {
					state = lp.GetValue()
				}
			}
			val := m.GetGauge().GetValue()
			if state == "snapshotting" && val != 1 {
				t.Errorf("expected snapshotting=1 after transition, got %v", val)
			}
			if state == "pending" && val != 0 {
				t.Errorf("expected pending=0 after transition, got %v", val)
			}
		}
	}
}

func TestLifecycleMetricsRegistration(t *testing.T) {
	r := newLifecycleTestRegistry(t)

	// Verify all metrics can be set without panic.
	SnapshotProgress.WithLabelValues("task-1", "db.users").Set(45.2)
	CatchingUpLagEvents.WithLabelValues("task-1", "db.users").Set(3500)
	BinlogCacheSizeBytes.WithLabelValues("task-1", "db.users").Set(1024 * 1024 * 100)
	GlobalMinPositionLagSeconds.WithLabelValues("task-1").Set(30.5)
	SnapshotRetriesTotal.WithLabelValues("task-1", "db.users").Inc()

	families, err := r.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}

	wantNames := map[string]bool{
		"datastream_snapshot_progress":              false,
		"datastream_catching_up_lag_events":         false,
		"datastream_binlog_cache_size_bytes":        false,
		"datastream_global_min_position_lag_seconds": false,
		"datastream_snapshot_retries_total":         false,
	}
	for _, f := range families {
		if _, ok := wantNames[f.GetName()]; ok {
			wantNames[f.GetName()] = true
		}
	}
	for name, found := range wantNames {
		if !found {
			t.Errorf("metric %q not found in gathered output", name)
		}
	}
}

func TestLifecycleMetrics_AllRegistered(t *testing.T) {
	r := newLifecycleTestRegistry(t)

	// Touch every metric so they appear in Gather.
	TableState.WithLabelValues("t", "tbl", "pending").Set(1)
	SnapshotProgress.WithLabelValues("t", "tbl").Set(0)
	CatchingUpLagEvents.WithLabelValues("t", "tbl").Set(0)
	BinlogCacheSizeBytes.WithLabelValues("t", "tbl").Set(0)
	GlobalMinPositionLagSeconds.WithLabelValues("t").Set(0)
	SnapshotRetriesTotal.WithLabelValues("t", "tbl").Inc()

	families, err := r.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	names := make(map[string]struct{})
	for _, f := range families {
		names[f.GetName()] = struct{}{}
	}

	want := []string{
		"datastream_table_state",
		"datastream_snapshot_progress",
		"datastream_catching_up_lag_events",
		"datastream_binlog_cache_size_bytes",
		"datastream_global_min_position_lag_seconds",
		"datastream_snapshot_retries_total",
	}
	for _, w := range want {
		if _, ok := names[w]; !ok {
			// List what we do have for debugging.
			var have []string
			for n := range names {
				have = append(have, n)
			}
			t.Errorf("missing %q; have: %s", w, strings.Join(have, ", "))
		}
	}
}
