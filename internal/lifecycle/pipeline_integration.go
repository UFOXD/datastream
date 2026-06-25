package lifecycle

import (
	"context"
	"sync"

	"github.com/UFOXD/datastream/internal/cache"
	"github.com/UFOXD/datastream/internal/sink"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
)

// SinkAdapter wraps a sink.Connector to implement the EventSink interface.
type SinkAdapter struct {
	sink sink.Connector
}

// Write forwards change events to the underlying sink connector.
func (a *SinkAdapter) Write(ctx context.Context, events []*event.ChangeEvent) error {
	return a.sink.Write(ctx, events)
}

// LifecyclePipeline is a thin adapter that connects source → BinlogConsumer → sink,
// using the SnapshotScheduler for per-table lifecycle management.
type LifecyclePipeline struct {
	source    source.Connector
	sinks     []sink.Connector
	scheduler *SnapshotScheduler
	consumer  *BinlogConsumer
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// NewLifecyclePipeline creates a new LifecyclePipeline.
func NewLifecyclePipeline(
	src source.Connector,
	sinks []sink.Connector,
	scheduler *SnapshotScheduler,
	cacheBackend cache.BinlogCacheBackend,
	store source.TableLifecycleStore,
	taskID string,
	sourceType cache.SourceType,
) *LifecyclePipeline {
	sinkAdapter := &SinkAdapter{sink: sinks[0]}
	consumer := NewBinlogConsumer(taskID, store, cacheBackend, sinkAdapter, sourceType)

	return &LifecyclePipeline{
		source:    src,
		sinks:     sinks,
		scheduler: scheduler,
		consumer:  consumer,
		stopCh:    make(chan struct{}),
	}
}

// Start starts the pipeline: starts the source connector and launches the
// event routing goroutine.
func (p *LifecyclePipeline) Start(ctx context.Context) error {
	if err := p.source.Start(ctx); err != nil {
		return err
	}

	p.wg.Add(1)
	go p.routeEvents(ctx)

	return nil
}

// Stop stops the pipeline gracefully: signals stop, stops the source,
// waits for the routing goroutine, then stops all sinks.
func (p *LifecyclePipeline) Stop(ctx context.Context) error {
	close(p.stopCh)

	if err := p.source.Stop(ctx); err != nil {
		return err
	}

	p.wg.Wait()

	for _, s := range p.sinks {
		if err := s.Stop(ctx); err != nil {
			return err
		}
	}

	return nil
}

// routeEvents reads events from the source and routes them via the BinlogConsumer.
func (p *LifecyclePipeline) routeEvents(ctx context.Context) {
	defer p.wg.Done()

	events := p.source.Events()
	for {
		select {
		case <-p.stopCh:
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			// Best-effort routing; errors are discarded for now.
			_ = p.consumer.Route(ctx, ev)
		}
	}
}
