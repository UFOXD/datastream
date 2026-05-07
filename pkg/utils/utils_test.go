package utils

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetrySuccess(t *testing.T) {
	calls := 0
	fn := func() error {
		calls++
		if calls < 3 {
			return errors.New("temporary error")
		}
		return nil
	}

	cfg := RetryConfig{
		MaxRetries:   3,
		InitialDelay: 1 * time.Millisecond,
		MaxDelay:     10 * time.Millisecond,
		Multiplier:   2.0,
	}

	err := Retry(context.Background(), cfg, fn)
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}
	if calls != 3 {
		t.Errorf("Expected 3 calls, got %d", calls)
	}
}

func TestRetryMaxAttempts(t *testing.T) {
	calls := 0
	expectedErr := errors.New("persistent error")
	fn := func() error {
		calls++
		return expectedErr
	}

	cfg := RetryConfig{
		MaxRetries:   2,
		InitialDelay: 1 * time.Millisecond,
		MaxDelay:     10 * time.Millisecond,
		Multiplier:   2.0,
	}

	err := Retry(context.Background(), cfg, fn)
	if err != expectedErr {
		t.Errorf("Expected %v, got %v", expectedErr, err)
	}
	if calls != 3 { // initial + 2 retries
		t.Errorf("Expected 3 calls, got %d", calls)
	}
}

func TestRetryContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	fn := func() error {
		calls++
		if calls == 1 {
			cancel()
		}
		return errors.New("error")
	}

	cfg := RetryConfig{
		MaxRetries:   3,
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		Multiplier:   2.0,
	}

	err := Retry(ctx, cfg, fn)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "context canceled",
			err:      context.Canceled,
			expected: false,
		},
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: false,
		},
		{
			name:     "generic error",
			err:      errors.New("generic error"),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRetryableError(tt.err)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestPool(t *testing.T) {
	created := 0
	pool := NewPool(2, func() int {
		created++
		return created
	})

	// Get from empty pool - should create new
	v1 := pool.Get()
	if v1 != 1 {
		t.Errorf("Expected 1, got %d", v1)
	}

	v2 := pool.Get()
	if v2 != 2 {
		t.Errorf("Expected 2, got %d", v2)
	}

	// Put back
	pool.Put(v1)
	pool.Put(v2)

	// Get from pool
	v3 := pool.Get()
	if v3 != 1 && v3 != 2 {
		t.Errorf("Expected 1 or 2, got %d", v3)
	}
}

func TestFNVHash(t *testing.T) {
	// Test FNV32
	h1 := FNV32("hello")
	if h1 == 0 {
		t.Error("FNV32 returned 0")
	}

	// Same input should produce same hash
	h2 := FNV32("hello")
	if h1 != h2 {
		t.Error("FNV32 not consistent")
	}

	// Different input should produce different hash
	h3 := FNV32("world")
	if h1 == h3 {
		t.Error("FNV32 produced same hash for different inputs")
	}

	// Test FNV64
	h4 := FNV64("hello")
	if h4 == 0 {
		t.Error("FNV64 returned 0")
	}
}
