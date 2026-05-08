// Package pipeline defines the data pipeline for DataStream.
package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/metrics"
	"github.com/UFOXD/datastream/pkg/sink"
	"github.com/UFOXD/datastream/pkg/source"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Pipeline represents a data pipeline that reads from a source and writes to sinks.
type Pipeline struct {
	id          string
	name        string
	source      source.Connector
	sinks       []sink.Connector
	dispatcher  Dispatcher
	buffer      Buffer
	coordinator Coordinator
	status      Status
	config      *Config
	mu          sync.RWMutex
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// Config holds pipeline configuration.
type Config struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Source      source.Config     `json:"source"`
	Sinks       []sink.Config     `json:"sinks"`
	Buffer      BufferConfig      `json:"buffer"`
	Dispatcher  DispatcherConfig  `json:"dispatcher"`
	Retry       RetryConfig       `json:"retry"`
	Coordinator CoordinatorConfig `json:"coordinator"`
}

// BufferConfig configures the event buffer.
type BufferConfig struct {
	Size         int `json:"size"`         // Max events in buffer
	BatchSize    int `json:"batchSize"`    // Events per batch
	FlushTimeout int `json:"flushTimeout"` // Flush timeout in ms
}

// DispatcherConfig configures the event dispatcher.
type DispatcherConfig struct {
	Type     string `json:"type"`     // round-robin, hash, broadcast
	HashKey  string `json:"hashKey"`  // Field for hash-based dispatch
	Workers  int    `json:"workers"`  // Number of dispatch workers
	QueueLen int    `json:"queueLen"` // Queue length per worker
}

// RetryConfig configures retry behavior.
type RetryConfig struct {
	MaxRetries  int     `json:"maxRetries"`
	InitialWait int     `json:"initialWait"` // ms
	MaxWait     int     `json:"maxWait"`     // ms
	Multiplier  float64 `json:"multiplier"`
}

// CoordinatorConfig configures the coordinator.
type CoordinatorConfig struct {
	Enabled   bool     `json:"enabled"`
	Backend   string   `json:"backend"` // memory, etcd, consul
	Endpoints []string `json:"endpoints"`
}

// Status represents pipeline status.
type Status struct {
	State      State      `json:"state"`
	Message    string     `json:"message,omitempty"`
	StartedAt  time.Time  `json:"startedAt,omitempty"`
	StoppedAt  time.Time  `json:"stoppedAt,omitempty"`
	Statistics Statistics `json:"statistics"`
}

// State represents pipeline state.
type State string

const (
	StateCreated  State = "created"
	StateStarting State = "starting"
	StateRunning  State = "running"
	StatePausing  State = "pausing"
	StatePaused   State = "paused"
	StateResuming State = "resuming"
	StateStopping State = "stopping"
	StateStopped  State = "stopped"
	StateError    State = "error"
)

// Statistics holds pipeline statistics.
type Statistics struct {
	EventsRead    int64     `json:"eventsRead"`
	EventsWritten int64     `json:"eventsWritten"`
	EventsFailed  int64     `json:"eventsFailed"`
	BytesRead     int64     `json:"bytesRead"`
	BytesWritten  int64     `json:"bytesWritten"`
	CurrentLag    int64     `json:"currentLag"` // ms
	LastEventTime time.Time `json:"lastEventTime"`
}

// New creates a new pipeline.
func New(config *Config) *Pipeline {
	return &Pipeline{
		id:     config.ID,
		name:   config.Name,
		config: config,
		status: Status{State: StateCreated},
		stopCh: make(chan struct{}),
	}
}

// ID returns the pipeline ID.
func (p *Pipeline) ID() string {
	return p.id
}

// Name returns the pipeline name.
func (p *Pipeline) Name() string {
	return p.name
}

// SetSource sets the source connector.
func (p *Pipeline) SetSource(src source.Connector) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.source = src
}

// AddSink adds a sink connector.
func (p *Pipeline) AddSink(s sink.Connector) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sinks = append(p.sinks, s)
}

// SetDispatcher sets the event dispatcher.
func (p *Pipeline) SetDispatcher(d Dispatcher) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dispatcher = d
}

// SetBuffer sets the event buffer.
func (p *Pipeline) SetBuffer(b Buffer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.buffer = b
}

// Start starts the pipeline.
func (p *Pipeline) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.status.State == StateRunning {
		p.mu.Unlock()
		return ErrAlreadyRunning
	}
	p.status.State = StateStarting
	p.status.StartedAt = time.Now()
	p.mu.Unlock()

	log.Info("starting pipeline", zap.String("id", p.id))

	// Start source connector
	if err := p.source.Start(ctx); err != nil {
		p.mu.Lock()
		p.status.State = StateError
		p.status.Message = err.Error()
		p.mu.Unlock()
		return err
	}

	// Start sink connectors
	for i, s := range p.sinks {
		if err := s.Start(ctx); err != nil {
			log.Error("failed to start sink",
				zap.Int("index", i),
				zap.Error(err))
		}
	}

	p.mu.Lock()
	p.status.State = StateRunning
	p.mu.Unlock()

	// Start event processing
	p.wg.Add(1)
	go p.run(ctx)

	metrics.TaskTotal.WithLabelValues(p.id, "running").Inc()
	log.Info("pipeline started", zap.String("id", p.id))
	return nil
}

// Stop stops the pipeline.
func (p *Pipeline) Stop(ctx context.Context) error {
	p.mu.Lock()
	if p.status.State == StateStopped {
		p.mu.Unlock()
		return nil
	}
	p.status.State = StateStopping
	p.mu.Unlock()

	log.Info("stopping pipeline", zap.String("id", p.id))
	close(p.stopCh)
	p.wg.Wait()

	// Stop source
	if err := p.source.Stop(ctx); err != nil {
		log.Error("failed to stop source", zap.Error(err))
	}

	// Stop sinks
	for _, s := range p.sinks {
		if err := s.Stop(ctx); err != nil {
			log.Error("failed to stop sink", zap.Error(err))
		}
	}

	p.mu.Lock()
	p.status.State = StateStopped
	p.status.StoppedAt = time.Now()
	p.mu.Unlock()

	metrics.TaskTotal.WithLabelValues(p.id, "stopped").Inc()
	log.Info("pipeline stopped", zap.String("id", p.id))
	return nil
}

// Pause pauses the pipeline.
func (p *Pipeline) Pause(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.status.State != StateRunning {
		return ErrInvalidState
	}

	p.status.State = StatePaused
	log.Info("pipeline paused", zap.String("id", p.id))
	return nil
}

// Resume resumes the pipeline.
func (p *Pipeline) Resume(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.status.State != StatePaused {
		return ErrInvalidState
	}

	p.status.State = StateRunning
	log.Info("pipeline resumed", zap.String("id", p.id))
	return nil
}

// Status returns the current pipeline status.
func (p *Pipeline) Status() Status {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.status
}

// GetPosition returns the current position.
func (p *Pipeline) GetPosition() *event.Position {
	return p.source.GetPosition()
}

// SetPosition sets the starting position.
func (p *Pipeline) SetPosition(pos *event.Position) error {
	return p.source.SetPosition(pos)
}

// run is the main event processing loop.
func (p *Pipeline) run(ctx context.Context) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			log.Info("pipeline context done", zap.String("id", p.id))
			return
		case <-p.stopCh:
			log.Info("pipeline stop signal received", zap.String("id", p.id))
			return
		case e, ok := <-p.source.Events():
			if !ok {
				log.Info("source events channel closed", zap.String("id", p.id))
				return
			}
			p.processEvent(ctx, e)
		case err, ok := <-p.source.Errors():
			if !ok {
				continue
			}
			log.Error("source error", zap.Error(err))
			p.mu.Lock()
			p.status.Statistics.EventsFailed++
			p.mu.Unlock()
		}
	}
}

// processEvent processes a single event.
func (p *Pipeline) processEvent(ctx context.Context, e *event.ChangeEvent) {
	startTime := time.Now()

	// Update statistics
	p.mu.Lock()
	p.status.Statistics.EventsRead++
	p.status.Statistics.LastEventTime = time.Now()
	p.mu.Unlock()

	// Skip heartbeat events if no dispatcher
	if e.IsHeartbeat() && p.dispatcher == nil {
		return
	}

	// Dispatch to sinks
	if p.dispatcher != nil {
		p.dispatcher.Dispatch(ctx, e, p.sinks)
	} else {
		// Simple broadcast to all sinks
		for _, s := range p.sinks {
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
		}
	}

	// Update latency metric
	latency := time.Since(startTime).Seconds()
	metrics.TaskLatencySeconds.WithLabelValues(p.id).Observe(latency)

	p.mu.Lock()
	p.status.Statistics.EventsWritten++
	p.mu.Unlock()
}
