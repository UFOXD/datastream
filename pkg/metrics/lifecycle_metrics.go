package metrics

import "github.com/prometheus/client_golang/prometheus"

// Table lifecycle metrics.
// These are registered separately from the core metrics via
// RegisterLifecycleMetrics(). Callers decide when to register.
var (
	TableState *prometheus.GaugeVec

	SnapshotProgress *prometheus.GaugeVec

	CatchingUpLagEvents *prometheus.GaugeVec

	BinlogCacheSizeBytes *prometheus.GaugeVec

	GlobalMinPositionLagSeconds *prometheus.GaugeVec

	SnapshotRetriesTotal *prometheus.CounterVec
)

// allTableStates enumerates the possible lifecycle states for a table.
var allTableStates = []string{"pending", "snapshotting", "catching_up", "streaming", "error", "paused"}

// RegisterLifecycleMetrics creates and registers lifecycle metrics with the
// default Prometheus registerer. It is NOT called at init time; callers invoke
// it explicitly when the lifecycle subsystem starts.
func RegisterLifecycleMetrics() {
	registerLifecycleMetricsTo(prometheus.DefaultRegisterer)
}

// registerLifecycleMetricsTo creates metric instances and registers them with
// the given Registerer. Used by RegisterLifecycleMetrics and tests.
func registerLifecycleMetricsTo(r prometheus.Registerer) {
	TableState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      "table_state",
		Help:      "Current state of each table (1=active in this state)",
	}, []string{"task", "table", "state"})

	SnapshotProgress = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      "snapshot_progress",
		Help:      "Snapshot progress percentage (0-100)",
	}, []string{"task", "table"})

	CatchingUpLagEvents = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      "catching_up_lag_events",
		Help:      "Number of events remaining in catching_up replay",
	}, []string{"task", "table"})

	BinlogCacheSizeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      "binlog_cache_size_bytes",
		Help:      "Size of binlog cache per table in bytes",
	}, []string{"task", "table"})

	GlobalMinPositionLagSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Name:      "global_min_position_lag_seconds",
		Help:      "Lag of global minimum position in seconds",
	}, []string{"task"})

	SnapshotRetriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace,
		Name:      "snapshot_retries_total",
		Help:      "Total number of snapshot retries",
	}, []string{"task", "table"})

	r.MustRegister(
		TableState,
		SnapshotProgress,
		CatchingUpLagEvents,
		BinlogCacheSizeBytes,
		GlobalMinPositionLagSeconds,
		SnapshotRetriesTotal,
	)
}

// resetLifecycleVars nils all lifecycle metric vars. Used by tests for
// isolation between test cases.
func resetLifecycleVars() {
	TableState = nil
	SnapshotProgress = nil
	CatchingUpLagEvents = nil
	BinlogCacheSizeBytes = nil
	GlobalMinPositionLagSeconds = nil
	SnapshotRetriesTotal = nil
}

// UpdateTableStateMetric sets the table_state gauge for the given table.
// The current state is set to 1; all other states are set to 0.
func UpdateTableStateMetric(task, table, state string) {
	for _, s := range allTableStates {
		val := float64(0)
		if s == state {
			val = 1
		}
		TableState.WithLabelValues(task, table, s).Set(val)
	}
}
