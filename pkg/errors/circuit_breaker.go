package errors

import (
	"sync"
	"time"
)

// CircuitState represents the state of a circuit breaker.
type CircuitState string

const (
	// CircuitClosed is the normal operating state where requests are allowed.
	CircuitClosed CircuitState = "closed"
	// CircuitOpen is the tripped state where requests are rejected.
	CircuitOpen CircuitState = "open"
	// CircuitHalfOpen is the probing state where a single request is allowed
	// to test whether the downstream has recovered.
	CircuitHalfOpen CircuitState = "half-open"
)

// CircuitBreakerConfig holds the configuration for a CircuitBreaker.
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of consecutive failures required to
	// trip the breaker from Closed to Open.
	FailureThreshold int

	// ResetTimeout is how long the breaker stays Open before transitioning
	// to HalfOpen to allow a probe request.
	ResetTimeout time.Duration
}

// CircuitBreaker implements the circuit breaker pattern for fault tolerance.
//
// State machine:
//   - Closed:   requests are allowed; failures are counted.
//     When failures >= FailureThreshold the state transitions to Open.
//   - Open:     all requests are rejected.
//     After ResetTimeout has elapsed since the last failure, the next
//     call to Allow() transitions to HalfOpen.
//   - HalfOpen: one probe request is allowed.
//     RecordSuccess → Closed.  RecordFailure → Open (timer resets).
type CircuitBreaker struct {
	config      CircuitBreakerConfig
	state       CircuitState
	failures    int
	lastFailure time.Time
	mu          sync.Mutex
}

// NewCircuitBreaker creates a CircuitBreaker in the Closed state.
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		config: config,
		state:  CircuitClosed,
	}
}

// Allow reports whether a request should be permitted.
//
// In the Closed state it always returns true.
// In the Open state it returns false unless ResetTimeout has elapsed,
// in which case it transitions to HalfOpen and returns true (probe).
// In the HalfOpen state it returns true for the probe request.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true

	case CircuitOpen:
		if time.Since(cb.lastFailure) > cb.config.ResetTimeout {
			cb.state = CircuitHalfOpen
			return true
		}
		return false

	case CircuitHalfOpen:
		return true
	}

	return false
}

// RecordSuccess records a successful operation.
//
// In the Closed state it resets the failure counter.
// In the HalfOpen state it transitions to Closed (downstream recovered).
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0

	if cb.state == CircuitHalfOpen {
		cb.state = CircuitClosed
	}
}

// RecordFailure records a failed operation.
//
// In the Closed state it increments the failure counter and transitions
// to Open when the threshold is reached.
// In the HalfOpen state it transitions back to Open immediately (the
// probe failed).
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailure = time.Now()

	switch cb.state {
	case CircuitHalfOpen:
		// Probe failed — re-open immediately.
		cb.state = CircuitOpen
		cb.failures = 0

	case CircuitClosed:
		if cb.failures >= cb.config.FailureThreshold {
			cb.state = CircuitOpen
		}
	}
}

// State returns the current circuit breaker state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Transparently detect timeout expiry so callers always see the
	// up-to-date logical state.
	if cb.state == CircuitOpen && time.Since(cb.lastFailure) > cb.config.ResetTimeout {
		cb.state = CircuitHalfOpen
	}

	return cb.state
}
