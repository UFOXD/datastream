package sink

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/logutil"
	"go.uber.org/zap"
)

// ConcurrentSinkConfig holds configuration for concurrent writing.
type ConcurrentSinkConfig struct {
	WorkerCount   int
	BatchSize     int
	FlushInterval time.Duration
	MaxRetry      int
	RetryBackoff  time.Duration
}

// DefaultConcurrentSinkConfig returns a default configuration.
func DefaultConcurrentSinkConfig() *ConcurrentSinkConfig {
	return &ConcurrentSinkConfig{
		WorkerCount:   4,
		BatchSize:     100,
		FlushInterval: 500 * time.Millisecond,
		MaxRetry:      3,
		RetryBackoff:  100 * time.Millisecond,
	}
}

// BatchWriter writes batches of events to a sink.
type BatchWriter interface {
	WriteBatch(ctx context.Context, events []*event.ChangeEvent) error
}

// ConcurrentSinkWriter manages concurrent sink writing by distributing events
// across multiple workers using a HashDispatcher to preserve per-row ordering.
type ConcurrentSinkWriter struct {
	sink       BatchWriter
	dispatcher *HashDispatcher
	workers    []*SinkWorker
	config     *ConcurrentSinkConfig

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closeOnce     sync.Once
	batchesFailed int64
}

// NewConcurrentSinkWriter creates a new ConcurrentSinkWriter.
func NewConcurrentSinkWriter(sink BatchWriter, config *ConcurrentSinkConfig) *ConcurrentSinkWriter {
	if config == nil {
		config = DefaultConcurrentSinkConfig()
	}
	if config.WorkerCount <= 0 {
		config.WorkerCount = 4
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 500 * time.Millisecond
	}

	dispatcherCfg := &DispatcherConfig{
		WorkerCount:       config.WorkerCount,
		BufferSize:        config.BatchSize * 4,
		NoPKTableStrategy: NoPKStrategyTable,
	}
	dispatcher := NewHashDispatcher(dispatcherCfg)

	ctx, cancel := context.WithCancel(context.Background())

	chans := dispatcher.WorkerChannels()
	workers := make([]*SinkWorker, config.WorkerCount)
	for i := 0; i < config.WorkerCount; i++ {
		workers[i] = &SinkWorker{
			id:            i,
			eventCh:       chans[i],
			sink:          sink,
			batchSize:     config.BatchSize,
			flushInterval: config.FlushInterval,
			maxRetry:      config.MaxRetry,
			retryBackoff:  config.RetryBackoff,
			buffer:        make([]*event.ChangeEvent, 0, config.BatchSize),
		}
	}

	return &ConcurrentSinkWriter{
		sink:       sink,
		dispatcher: dispatcher,
		workers:    workers,
		config:     config,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start starts all workers. It must be called before Write or WriteBatch.
func (w *ConcurrentSinkWriter) Start() {
	for _, worker := range w.workers {
		w.wg.Add(1)
		go worker.Run(w.ctx, &w.wg)
	}
}

// Write dispatches a single event to the appropriate worker.
func (w *ConcurrentSinkWriter) Write(ctx context.Context, e *event.ChangeEvent, schema *event.TableInfo) error {
	return w.dispatcher.Dispatch(ctx, e, schema)
}

// WriteBatch dispatches multiple events to the appropriate workers.
// Events for the same row are always sent to the same worker, preserving ordering.
func (w *ConcurrentSinkWriter) WriteBatch(ctx context.Context, events []*event.ChangeEvent, schema *event.TableInfo) error {
	for _, e := range events {
		if err := w.dispatcher.Dispatch(ctx, e, schema); err != nil {
			return err
		}
	}
	return nil
}

// Close stops all workers gracefully, flushing any buffered events.
// It closes the dispatcher channels so workers drain and exit naturally,
// then waits for all workers to finish before returning.
// Safe to call multiple times; only the first call has any effect.
func (w *ConcurrentSinkWriter) Close() error {
	w.closeOnce.Do(func() {
		// Close dispatcher channels — workers will drain remaining events then exit
		// via the channel-closed (!ok) path.
		w.dispatcher.Close()
		// Wait for all workers to finish draining and flushing.
		w.wg.Wait()
		// Cancel the context last (for any callers holding it).
		w.cancel()
	})
	return nil
}

// Stats returns aggregate statistics across all workers.
func (w *ConcurrentSinkWriter) Stats() (totalEventsWritten, totalBatchesFlushed int64) {
	for _, worker := range w.workers {
		ew, bf := worker.Stats()
		totalEventsWritten += ew
		totalBatchesFlushed += bf
	}
	return
}

// ─────────────────────────────────────────────────────────────────────────────
// SinkWorker
// ─────────────────────────────────────────────────────────────────────────────

// SinkWorker handles events for a single worker goroutine.
// It batches events and flushes them either when the batch is full or the
// flush interval elapses.
type SinkWorker struct {
	id            int
	eventCh       <-chan *event.ChangeEvent
	sink          BatchWriter
	batchSize     int
	flushInterval time.Duration
	maxRetry      int
	retryBackoff  time.Duration
	buffer        []*event.ChangeEvent

	// Stats — accessed only from the Run goroutine except via atomic load in Stats().
	eventsWritten  int64
	batchesFlushed int64
	batchesFailed  int64
}

// Run runs the worker until its input channel is closed or ctx is cancelled.
// It batches incoming events and flushes them periodically.
//
// Normal shutdown: the dispatcher closes the channel; the worker drains it
// completely via the !ok branch and then flushes.
// Emergency shutdown: ctx is cancelled; the worker exits without draining.
func (w *SinkWorker) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case e, ok := <-w.eventCh:
			if !ok {
				// Channel closed — drain is complete, flush remainder and exit.
				_ = w.flush(context.Background())
				return
			}
			w.buffer = append(w.buffer, e)
			if len(w.buffer) >= w.batchSize {
				_ = w.flush(context.Background())
			}

		case <-ticker.C:
			if len(w.buffer) > 0 {
				_ = w.flush(context.Background())
			}

		case <-ctx.Done():
			// Emergency cancellation — best-effort flush and exit without draining.
			_ = w.flush(context.Background())
			return
		}
	}
}

// flush writes all buffered events to the sink with retries.
// Uses exponential backoff with jitter per error-handling-design.md.
func (w *SinkWorker) flush(ctx context.Context) error {
	if len(w.buffer) == 0 {
		return nil
	}

	batch := w.buffer
	w.buffer = make([]*event.ChangeEvent, 0, w.batchSize)

	// Initialize backoff state
	delay := w.retryBackoff
	const multiplier = 2.0
	const maxDelay = 30 * time.Second

	var lastErr error
	for attempt := 0; attempt <= w.maxRetry; attempt++ {
		if attempt > 0 {
			// Calculate delay with jitter (0-50% random jitter per error-handling-design.md:318-320)
			jitter := time.Duration(rand.Float64() * 0.5 * float64(delay))
			waitTime := delay + jitter

			logutil.L().Warn("retry attempt failed, waiting before retry",
				zap.Int("worker_id", w.id),
				zap.Int("attempt", attempt),
				zap.Duration("wait", waitTime),
				zap.Int("batch_size", len(batch)),
				zap.Error(lastErr),
			)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(waitTime):
			}

			// Exponential backoff (multiplier 2.0 per error-handling-design.md:252)
			delay = time.Duration(float64(delay) * multiplier)
			if delay > maxDelay {
				delay = maxDelay
			}
		}

		if err := w.sink.WriteBatch(ctx, batch); err != nil {
			lastErr = err
			continue
		}

		// Success — update stats atomically and reset backoff
		atomic.AddInt64(&w.eventsWritten, int64(len(batch)))
		atomic.AddInt64(&w.batchesFlushed, 1)
		return nil
	}

	// All retries exhausted — record failure
	atomic.AddInt64(&w.batchesFailed, 1)
	logutil.L().Error("batch write failed after all retries",
		zap.Int("worker_id", w.id),
		zap.Int("batch_size", len(batch)),
		zap.Int("attempts", w.maxRetry+1),
		zap.Error(lastErr),
	)

	return lastErr
}

// Stats returns the number of events written and batches flushed by this worker.
func (w *SinkWorker) Stats() (eventsWritten, batchesFlushed int64) {
	return atomic.LoadInt64(&w.eventsWritten), atomic.LoadInt64(&w.batchesFlushed)
}
