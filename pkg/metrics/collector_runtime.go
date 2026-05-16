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
	type item struct {
		key   string
		entry providerEntry
	}
	snapshot := make([]item, 0, len(c.providers))
	for k, e := range c.providers {
		snapshot = append(snapshot, item{k, e})
	}
	c.mu.RUnlock()

	var wg sync.WaitGroup
	for _, it := range snapshot {
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
		}(it.key, it.entry)
	}
	wg.Wait()
}

func (c *StatsCollector) emit(e providerEntry, s connector.Stats) {
	stage := e.role
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
