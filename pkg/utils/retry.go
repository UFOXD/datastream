// Package utils provides shared utilities for the DataStream platform.
package utils

import (
	"context"
	"errors"
	"time"
)

// RetryConfig controls the behavior of Retry.
type RetryConfig struct {
	// MaxRetries is the maximum number of retry attempts after the initial call.
	MaxRetries int
	// InitialDelay is the delay before the first retry.
	InitialDelay time.Duration
	// MaxDelay caps the per-attempt delay.
	MaxDelay time.Duration
	// Multiplier is the factor applied to the delay after each failure.
	Multiplier float64
}

// DefaultRetryConfig provides sensible defaults for Retry.
var DefaultRetryConfig = RetryConfig{
	MaxRetries:   3,
	InitialDelay: 100 * time.Millisecond,
	MaxDelay:     10 * time.Second,
	Multiplier:   2.0,
}

// retryableError is implemented by error types that explicitly signal
// whether they may be retried. The DataStream error type in pkg/errors
// satisfies this interface.
type retryableError interface {
	Retryable() bool
}

// IsRetryableError reports whether err should be retried.
//
// Rules:
//   - nil errors are not retryable.
//   - context.Canceled and context.DeadlineExceeded are not retryable.
//   - errors implementing Retryable() bool are deferred to.
//   - all other errors are considered retryable by default.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var re retryableError
	if errors.As(err, &re) {
		return re.Retryable()
	}
	return true
}

// Retry executes fn with exponential backoff according to cfg.
//
// It returns nil on the first successful call, the underlying error if it is
// not retryable, ctx.Err() if the context is cancelled while waiting, or the
// last observed error after MaxRetries attempts have been exhausted.
func Retry(ctx context.Context, cfg RetryConfig, fn func() error) error {
	var err error
	delay := cfg.InitialDelay

	for i := 0; i <= cfg.MaxRetries; i++ {
		if err = ctx.Err(); err != nil {
			return err
		}

		err = fn()
		if err == nil {
			return nil
		}

		if !IsRetryableError(err) {
			return err
		}

		if i == cfg.MaxRetries {
			break
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		delay = time.Duration(float64(delay) * cfg.Multiplier)
		if cfg.MaxDelay > 0 && delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
	}

	return err
}
