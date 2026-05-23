package errors

import (
	"sync"
	"time"
)

// CircuitState represents the state of a circuit breaker.
type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half-open"
)

// CircuitBreakerConfig holds the configuration for a CircuitBreaker.
type CircuitBreakerConfig struct {
	FailureThreshold int
	SuccessThreshold int
	ResetTimeout     time.Duration
}

// CircuitBreaker implements the circuit breaker pattern for fault tolerance.
type CircuitBreaker struct {
	config        CircuitBreakerConfig
	state         CircuitState
	failures      int
	successes     int
	lastFailure   time.Time
	probeInFlight bool
	mu            sync.Mutex
}

// NewCircuitBreaker creates a CircuitBreaker in the Closed state.
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	if config.SuccessThreshold <= 0 {
		config.SuccessThreshold = 1
	}
	return &CircuitBreaker{
		config: config,
		state:  CircuitClosed,
	}
}

// Allow reports whether a request should be permitted.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true

	case CircuitOpen:
		if time.Since(cb.lastFailure) > cb.config.ResetTimeout {
			cb.state = CircuitHalfOpen
			cb.probeInFlight = true
			cb.successes = 0
			return true
		}
		return false

	case CircuitHalfOpen:
		if cb.probeInFlight {
			return false
		}
		cb.probeInFlight = true
		return true
	}

	return false
}

// RecordSuccess records a successful operation.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0
	cb.probeInFlight = false

	if cb.state == CircuitHalfOpen {
		cb.successes++
		if cb.successes >= cb.config.SuccessThreshold {
			cb.state = CircuitClosed
			cb.successes = 0
		}
	}
}

// RecordFailure records a failed operation.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailure = time.Now()
	cb.probeInFlight = false

	switch cb.state {
	case CircuitHalfOpen:
		cb.state = CircuitOpen
		cb.failures = 0
		cb.successes = 0

	case CircuitClosed:
		if cb.failures >= cb.config.FailureThreshold {
			cb.state = CircuitOpen
		}
	}
}

// State returns the current circuit breaker state (read-only, no side effects).
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == CircuitOpen && time.Since(cb.lastFailure) > cb.config.ResetTimeout {
		return CircuitHalfOpen
	}
	return cb.state
}
