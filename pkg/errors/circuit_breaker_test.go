package errors

import (
	"testing"
	"time"
)

func TestCircuitBreakerInitiallyClosed(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		ResetTimeout:     time.Second,
	})
	if !cb.Allow() {
		t.Error("new circuit breaker should allow requests")
	}
	if cb.State() != CircuitClosed {
		t.Errorf("initial state should be Closed, got %v", cb.State())
	}
}

func TestCircuitBreakerTripsAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		ResetTimeout:     time.Second,
	})

	for i := 0; i < 3; i++ {
		if !cb.Allow() {
			t.Fatalf("should allow request %d", i)
		}
		cb.RecordFailure()
	}

	if cb.Allow() {
		t.Error("circuit should be open after 3 failures")
	}
	if cb.State() != CircuitOpen {
		t.Errorf("state should be Open, got %v", cb.State())
	}
}

func TestCircuitBreakerResetsAfterTimeout(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 2,
		ResetTimeout:     100 * time.Millisecond,
	})

	cb.RecordFailure()
	cb.RecordFailure()
	if cb.Allow() {
		t.Error("circuit should be open")
	}

	time.Sleep(150 * time.Millisecond)

	if !cb.Allow() {
		t.Error("circuit should allow probe request in half-open")
	}
	if cb.State() != CircuitHalfOpen {
		t.Errorf("state should be HalfOpen, got %v", cb.State())
	}

	cb.RecordSuccess()
	if cb.State() != CircuitClosed {
		t.Errorf("state should be Closed after success in half-open, got %v", cb.State())
	}
}

func TestCircuitBreakerHalfOpenFailure(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 1,
		ResetTimeout:     100 * time.Millisecond,
	})

	cb.RecordFailure()
	time.Sleep(150 * time.Millisecond)

	cb.Allow() // probe
	cb.RecordFailure()

	if cb.State() != CircuitOpen {
		t.Errorf("state should be Open after half-open failure, got %v", cb.State())
	}
}

func TestCircuitBreakerSuccessResetsCount(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		ResetTimeout:     time.Second,
	})

	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordSuccess()

	cb.RecordFailure()
	if !cb.Allow() {
		t.Error("should allow after success reset")
	}
}

// M1: HalfOpen must allow only ONE probe request
func TestCircuitBreakerHalfOpenSingleProbe(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 1,
		ResetTimeout:     100 * time.Millisecond,
	})

	cb.RecordFailure()
	time.Sleep(150 * time.Millisecond)

	// First Allow in half-open: probe allowed
	if !cb.Allow() {
		t.Error("first call in half-open should allow probe")
	}
	// Second Allow while probe in flight: must be rejected
	if cb.Allow() {
		t.Error("second call in half-open should be rejected (probe in flight)")
	}
	// Third also rejected
	if cb.Allow() {
		t.Error("third call in half-open should be rejected")
	}

	// After probe succeeds, allow again
	cb.RecordSuccess()
	if !cb.Allow() {
		t.Error("should allow after probe success")
	}
}

// M2: SuccessThreshold requires multiple successes to close from HalfOpen
func TestCircuitBreakerSuccessThreshold(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 3,
		ResetTimeout:     100 * time.Millisecond,
	})

	cb.RecordFailure()
	time.Sleep(150 * time.Millisecond)

	// First probe + success: still HalfOpen
	cb.Allow()
	cb.RecordSuccess()
	if cb.State() != CircuitHalfOpen {
		t.Errorf("should still be HalfOpen after 1 success (need 3), got %v", cb.State())
	}

	// Second probe + success: still HalfOpen
	cb.Allow()
	cb.RecordSuccess()
	if cb.State() != CircuitHalfOpen {
		t.Errorf("should still be HalfOpen after 2 successes, got %v", cb.State())
	}

	// Third probe + success: now Closed
	cb.Allow()
	cb.RecordSuccess()
	if cb.State() != CircuitClosed {
		t.Errorf("should be Closed after 3 successes, got %v", cb.State())
	}
}

// A1: State() must be read-only, no side effects
func TestCircuitBreakerStateNoSideEffects(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 1,
		ResetTimeout:     100 * time.Millisecond,
	})

	cb.RecordFailure()
	if cb.State() != CircuitOpen {
		t.Fatal("should be Open")
	}

	time.Sleep(150 * time.Millisecond)

	// State() should report HalfOpen but NOT transition internally
	if cb.State() != CircuitHalfOpen {
		t.Fatal("State() should report HalfOpen after timeout")
	}

	// Internal state should still be Open — only Allow() transitions
	cb.mu.Lock()
	internalState := cb.state
	cb.mu.Unlock()
	if internalState != CircuitOpen {
		t.Errorf("internal state should remain Open (State() is read-only), got %v", internalState)
	}

	// Allow() should still transition to HalfOpen and allow probe
	if !cb.Allow() {
		t.Error("Allow() should transition to HalfOpen and allow probe")
	}
}

func TestCircuitBreakerConcurrency(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 100,
		ResetTimeout:     time.Second,
	})

	done := make(chan struct{})
	for i := 0; i < 50; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				cb.Allow()
				cb.RecordFailure()
				cb.RecordSuccess()
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 50; i++ {
		<-done
	}
}
