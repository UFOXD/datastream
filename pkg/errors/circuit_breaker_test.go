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

	// 4th call should be blocked (circuit open)
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

	// Should be half-open now
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
	if !cb.Allow() {
		t.Error("circuit should allow requests when closed")
	}
}

func TestCircuitBreakerHalfOpenFailure(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 1,
		ResetTimeout:     100 * time.Millisecond,
	})

	cb.RecordFailure()
	time.Sleep(150 * time.Millisecond)

	// Half-open: allow one probe
	cb.Allow()
	cb.RecordFailure() // probe failed

	// Should be back to open
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
	cb.RecordSuccess() // should reset failure count

	cb.RecordFailure()
	// Only 1 failure after reset, should still allow
	if !cb.Allow() {
		t.Error("should allow after success reset")
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
	// No panic = pass
}
