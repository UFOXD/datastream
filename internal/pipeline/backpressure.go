package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/logutil"
	"go.uber.org/zap"
)

// BackpressureConfig holds backpressure configuration.
type BackpressureConfig struct {
	// EnableBackpressure enables backpressure control
	EnableBackpressure bool `json:"enable-backpressure" toml:"enable-backpressure"`

	// HighWatermark is the queue usage percentage that triggers pause (0-100)
	HighWatermark int `json:"high-watermark" toml:"high-watermark"`

	// LowWatermark is the queue usage percentage that triggers resume (0-100)
	LowWatermark int `json:"low-watermark" toml:"low-watermark"`

	// MaxLatency is the maximum acceptable latency before pause
	MaxLatency time.Duration `json:"max-latency" toml:"max-latency"`

	// CheckInterval is the interval between backpressure checks
	CheckInterval time.Duration `json:"check-interval" toml:"check-interval"`
}

// DefaultBackpressureConfig returns defaults.
func DefaultBackpressureConfig() *BackpressureConfig {
	return &BackpressureConfig{
		EnableBackpressure: true,
		HighWatermark:      80,
		LowWatermark:       50,
		MaxLatency:         5 * time.Second,
		CheckInterval:      100 * time.Millisecond,
	}
}

// BackpressureState represents the backpressure state.
type BackpressureState string

const (
	// BackpressureStateNormal means normal operation
	BackpressureStateNormal BackpressureState = "normal"
	// BackpressureStateWarning means approaching limit
	BackpressureStateWarning BackpressureState = "warning"
	// BackpressureStatePaused means paused due to backpressure
	BackpressureStatePaused BackpressureState = "paused"
)

// BackpressureController manages backpressure for flow control.
type BackpressureController struct {
	config *BackpressureConfig

	// Metrics
	queueSize    int64
	maxQueueSize int64
	latency      time.Duration

	// State
	state    BackpressureState
	pauseCh  chan struct{}
	resumeCh chan struct{}

	// Callbacks
	onPause  func()
	onResume func()

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *zap.Logger
}

// NewBackpressureController creates a controller.
func NewBackpressureController(config *BackpressureConfig) *BackpressureController {
	ctx, cancel := context.WithCancel(context.Background())

	return &BackpressureController{
		config:   config,
		state:    BackpressureStateNormal,
		pauseCh:  make(chan struct{}, 1),
		resumeCh: make(chan struct{}, 1),
		ctx:      ctx,
		cancel:   cancel,
		logger:   logutil.L(),
	}
}

// Start starts the controller.
func (b *BackpressureController) Start() {
	b.wg.Add(1)
	go b.run()
}

// Stop stops the controller.
func (b *BackpressureController) Stop() {
	b.cancel()
	b.wg.Wait()
}

// run runs the backpressure loop.
func (b *BackpressureController) run() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.check()
		}
	}
}

// check checks and applies backpressure.
func (b *BackpressureController) check() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.config.EnableBackpressure {
		return
	}

	// Calculate queue usage percentage
	var usagePercent int
	if b.maxQueueSize > 0 {
		usagePercent = int((float64(b.queueSize) / float64(b.maxQueueSize)) * 100)
	}

	// Check if we need to pause
	if b.state == BackpressureStateNormal || b.state == BackpressureStateWarning {
		if usagePercent >= b.config.HighWatermark || b.latency > b.config.MaxLatency {
			b.state = BackpressureStatePaused
			b.logger.Warn("backpressure triggered",
				zap.Int("usage", usagePercent),
				zap.Duration("latency", b.latency),
			)
			b.notifyPause()
			return
		}

		// Warning state
		if usagePercent >= b.config.LowWatermark {
			b.state = BackpressureStateWarning
		}
	}

	// Check if we can resume - both queue usage AND latency must be acceptable
	if b.state == BackpressureStatePaused {
		if usagePercent <= b.config.LowWatermark && b.latency <= b.config.MaxLatency {
			b.state = BackpressureStateNormal
			b.logger.Info("backpressure released",
				zap.Int("usage", usagePercent),
				zap.Duration("latency", b.latency),
			)
			b.notifyResume()
		}
	}
}

// notifyPause notifies pause callbacks.
func (b *BackpressureController) notifyPause() {
	select {
	case b.pauseCh <- struct{}{}:
	default:
	}
	if b.onPause != nil {
		b.onPause()
	}
}

// notifyResume notifies resume callbacks.
func (b *BackpressureController) notifyResume() {
	select {
	case b.resumeCh <- struct{}{}:
	default:
	}
	if b.onResume != nil {
		b.onResume()
	}
}

// UpdateMetrics updates the controller metrics.
func (b *BackpressureController) UpdateMetrics(queueSize, maxQueueSize int64, latency time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queueSize = queueSize
	b.maxQueueSize = maxQueueSize
	b.latency = latency
}

// State returns the current state.
func (b *BackpressureController) State() BackpressureState {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state
}

// PauseCh returns the pause notification channel.
func (b *BackpressureController) PauseCh() <-chan struct{} {
	return b.pauseCh
}

// ResumeCh returns the resume notification channel.
func (b *BackpressureController) ResumeCh() <-chan struct{} {
	return b.resumeCh
}

// OnPause sets the pause callback.
func (b *BackpressureController) OnPause(fn func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onPause = fn
}

// OnResume sets the resume callback.
func (b *BackpressureController) OnResume(fn func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onResume = fn
}

// ShouldPause checks if we should pause reading.
func (b *BackpressureController) ShouldPause() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state == BackpressureStatePaused
}

// WaitForResume waits for resume signal.
func (b *BackpressureController) WaitForResume(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.resumeCh:
		return nil
	}
}

// WaitWhilePaused blocks while in paused state.
func (b *BackpressureController) WaitWhilePaused(ctx context.Context) error {
	for {
		if !b.ShouldPause() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.resumeCh:
			return nil
		case <-time.After(b.config.CheckInterval):
			// Re-check state
		}
	}
}
