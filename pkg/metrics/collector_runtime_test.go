package metrics_test

import (
	"context"
	"math"
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

	time.Sleep(350 * time.Millisecond)
	cancel()

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
	c.Register("key", "source", "mysql", "t1", p2)

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

	families, _ := r.Gather()
	for _, f := range families {
		if f.GetName() == "datastream_source_lag_seconds" && len(f.GetMetric()) > 0 {
			t.Errorf("source_lag_seconds set to NaN should be skipped, found series: %v", f.GetMetric())
		}
	}
}
