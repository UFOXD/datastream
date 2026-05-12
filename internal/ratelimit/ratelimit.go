// Package ratelimit provides rate limiting utilities for DataStream.
package ratelimit

import (
	"context"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiter interface for rate limiting operations.
type RateLimiter interface {
	// Wait waits until the limiter permits an event to happen.
	Wait(ctx context.Context) error

	// WaitN waits until the limiter permits n events to happen.
	WaitN(ctx context.Context, n int) error

	// Allow checks if an event can proceed immediately.
	Allow() bool

	// AllowN checks if n events can proceed immediately.
	AllowN(n int) bool

	// SetLimit dynamically sets the rate limit (events per second).
	SetLimit(limit int)

	// SetBurst dynamically sets the burst size.
	SetBurst(burst int)
}

// Config holds rate limiting configuration.
type Config struct {
	// ===== Source-side rate limiting (recommended) =====

	// SourceEnabled enables source-side rate limiting
	SourceEnabled bool `json:"source-enabled" toml:"source-enabled"`

	// SourceRowsPerSecond limits rows per second from source
	// 0 means no limit
	SourceRowsPerSecond int `json:"source-rows-per-second" toml:"source-rows-per-second"`

	// SourceBytesPerSecond limits bytes per second from source
	// 0 means no limit
	SourceBytesPerSecond int64 `json:"source-bytes-per-second" toml:"source-bytes-per-second"`

	// ===== Sink-side rate limiting (backup) =====

	// SinkEnabled enables sink-side rate limiting
	// Note: In current architecture, sink rate limiting uses backpressure
	SinkEnabled bool `json:"sink-enabled" toml:"sink-enabled"`

	// SinkRowsPerSecond limits rows per second to sink
	SinkRowsPerSecond int `json:"sink-rows-per-second" toml:"sink-rows-per-second"`

	// SinkBytesPerSecond limits bytes per second to sink
	SinkBytesPerSecond int64 `json:"sink-bytes-per-second" toml:"sink-bytes-per-second"`

	// ===== Common settings =====

	// BurstSize allows temporary bursts
	BurstSize int `json:"burst-size" toml:"burst-size"`

	// EnableAdaptive enables adaptive rate limiting based on system load
	EnableAdaptive bool `json:"enable-adaptive" toml:"enable-adaptive"`
}

// DefaultConfig returns the default rate limit configuration.
func DefaultConfig() *Config {
	return &Config{
		SourceEnabled:        true,
		SourceRowsPerSecond:  0, // No limit
		SourceBytesPerSecond: 0, // No limit
		SinkEnabled:          false,
		SinkRowsPerSecond:    0,
		SinkBytesPerSecond:   0,
		BurstSize:            1000,
		EnableAdaptive:       false,
	}
}

// Limiter implements dual rate limiting (rows and bytes).
type Limiter struct {
	rowsLimiter  *rate.Limiter
	bytesLimiter *rate.Limiter
	config       *Config
}

// NewLimiter creates a new rate limiter.
func NewLimiter(config *Config) *Limiter {
	rl := &Limiter{
		config: config,
	}

	// Create rows limiter
	if config.SourceRowsPerSecond > 0 {
		rl.rowsLimiter = rate.NewLimiter(
			rate.Limit(config.SourceRowsPerSecond),
			config.BurstSize,
		)
	}

	// Create bytes limiter
	if config.SourceBytesPerSecond > 0 {
		rl.bytesLimiter = rate.NewLimiter(
			rate.Limit(config.SourceBytesPerSecond),
			config.BurstSize*1000, // Higher burst for bytes
		)
	}

	return rl
}

// Wait waits for a single event.
func (rl *Limiter) Wait(ctx context.Context) error {
	return rl.WaitN(ctx, 1)
}

// WaitN waits for n events.
func (rl *Limiter) WaitN(ctx context.Context, n int) error {
	return rl.WaitRowsAndBytes(ctx, n, 0)
}

// WaitRowsAndBytes waits for both row and byte limits.
func (rl *Limiter) WaitRowsAndBytes(ctx context.Context, rows int, bytes int64) error {
	// Wait for rows limit
	if rl.rowsLimiter != nil && rows > 0 {
		if err := rl.rowsLimiter.WaitN(ctx, rows); err != nil {
			return err
		}
	}

	// Wait for bytes limit
	if rl.bytesLimiter != nil && bytes > 0 {
		if err := rl.bytesLimiter.WaitN(ctx, int(bytes)); err != nil {
			return err
		}
	}

	return nil
}

// Allow checks if a single event can proceed.
func (rl *Limiter) Allow() bool {
	return rl.AllowN(1)
}

// AllowN checks if n events can proceed.
func (rl *Limiter) AllowN(n int) bool {
	if rl.rowsLimiter != nil {
		if !rl.rowsLimiter.AllowN(time.Now(), n) {
			return false
		}
	}
	return true
}

// AllowRowsAndBytes checks if both row and byte limits allow the operation.
func (rl *Limiter) AllowRowsAndBytes(rows int, bytes int64) bool {
	if rl.rowsLimiter != nil && rows > 0 {
		if !rl.rowsLimiter.AllowN(time.Now(), rows) {
			return false
		}
	}
	if rl.bytesLimiter != nil && bytes > 0 {
		if !rl.bytesLimiter.AllowN(time.Now(), int(bytes)) {
			return false
		}
	}
	return true
}

// SetLimit dynamically sets the rows per second limit.
func (rl *Limiter) SetLimit(limit int) {
	if rl.rowsLimiter != nil {
		rl.rowsLimiter.SetLimit(rate.Limit(limit))
	}
}

// SetBurst dynamically sets the burst size.
func (rl *Limiter) SetBurst(burst int) {
	if rl.rowsLimiter != nil {
		rl.rowsLimiter.SetBurst(burst)
	}
}

// SetBytesLimit dynamically sets the bytes per second limit.
func (rl *Limiter) SetBytesLimit(limit int64) {
	if rl.bytesLimiter != nil {
		rl.bytesLimiter.SetLimit(rate.Limit(limit))
	}
}

// Reserve reserves the right to perform n events.
// Returns a Reservation that can be used to wait or cancel.
func (rl *Limiter) Reserve(n int) *rate.Reservation {
	if rl.rowsLimiter == nil {
		return nil
	}
	r := rl.rowsLimiter.ReserveN(time.Now(), n)
	return r
}

// Delay returns the duration to wait before n events can proceed.
func (rl *Limiter) Delay(n int) time.Duration {
	if rl.rowsLimiter == nil {
		return 0
	}
	r := rl.rowsLimiter.ReserveN(time.Now(), n)
	if !r.OK() {
		return -1
	}
	delay := r.Delay()
	r.Cancel()
	return delay
}

// Limit returns the current rate limit (events per second).
func (rl *Limiter) Limit() rate.Limit {
	if rl.rowsLimiter == nil {
		return rate.Inf
	}
	return rl.rowsLimiter.Limit()
}

// Burst returns the current burst size.
func (rl *Limiter) Burst() int {
	if rl.rowsLimiter == nil {
		return 0
	}
	return rl.rowsLimiter.Burst()
}
