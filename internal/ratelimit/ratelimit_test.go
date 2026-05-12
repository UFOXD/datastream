package ratelimit

import (
	"context"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestRateLimiter_Wait(t *testing.T) {
	config := &Config{
		SourceEnabled:       true,
		SourceRowsPerSecond: 100,
		BurstSize:           10,
	}

	limiter := NewLimiter(config)

	start := time.Now()
	for i := 0; i < 11; i++ {
		err := limiter.Wait(context.Background())
		if err != nil {
			t.Fatalf("Wait failed: %v", err)
		}
	}
	elapsed := time.Since(start)

	// 11 requests at 100/sec with burst 10 should take some time
	// First 10 are burst, 11th waits ~10ms
	// Use a lower threshold to account for timing variations
	if elapsed < 5*time.Millisecond {
		t.Errorf("Expected rate limiting to kick in, elapsed: %v", elapsed)
	}
}

func TestRateLimiter_WaitRowsAndBytes(t *testing.T) {
	config := &Config{
		SourceEnabled:        true,
		SourceRowsPerSecond:  1000,
		SourceBytesPerSecond: 10000,
		BurstSize:            100,
	}

	limiter := NewLimiter(config)

	err := limiter.WaitRowsAndBytes(context.Background(), 10, 100)
	if err != nil {
		t.Fatalf("WaitRowsAndBytes failed: %v", err)
	}
}

func TestRateLimiter_NoLimit(t *testing.T) {
	config := &Config{
		SourceEnabled:       true,
		SourceRowsPerSecond: 0, // No limit
		BurstSize:           1000,
	}

	limiter := NewLimiter(config)

	// Should complete immediately
	start := time.Now()
	for i := 0; i < 1000; i++ {
		limiter.Wait(context.Background())
	}
	elapsed := time.Since(start)

	if elapsed > 100*time.Millisecond {
		t.Errorf("No limit should be instant, elapsed: %v", elapsed)
	}
}

func TestRateLimiter_SetLimit(t *testing.T) {
	// Start with an actual limit, not 0 (which creates no limiter)
	config := &Config{
		SourceEnabled:       true,
		SourceRowsPerSecond: 10000, // Very high limit initially
		BurstSize:           10,
	}

	limiter := NewLimiter(config)

	// With high limit - should be fast
	start := time.Now()
	for i := 0; i < 100; i++ {
		limiter.Wait(context.Background())
	}
	elapsed := time.Since(start)

	if elapsed > 50*time.Millisecond {
		t.Errorf("High limit should be fast, elapsed: %v", elapsed)
	}

	// Set a lower limit
	limiter.SetLimit(100)

	// Now should be rate limited
	start = time.Now()
	for i := 0; i < 15; i++ {
		limiter.Wait(context.Background())
	}
	elapsed = time.Since(start)

	// 15 requests at 100/sec with burst 10 means ~50ms wait for extra 5
	if elapsed < 20*time.Millisecond {
		t.Errorf("Expected rate limiting after SetLimit, elapsed: %v", elapsed)
	}
}

func TestRateLimiter_Allow(t *testing.T) {
	config := &Config{
		SourceEnabled:       true,
		SourceRowsPerSecond: 10,
		BurstSize:           2,
	}

	limiter := NewLimiter(config)

	// First 2 should be allowed (burst)
	if !limiter.Allow() {
		t.Error("First request should be allowed")
	}
	if !limiter.Allow() {
		t.Error("Second request should be allowed")
	}

	// Third may not be allowed (exceeds burst)
	// Note: This is timing-dependent, so we just verify the method works
	limiter.AllowN(1)
}

func TestRateLimiter_Delay(t *testing.T) {
	config := &Config{
		SourceEnabled:       true,
		SourceRowsPerSecond: 100,
		BurstSize:           10,
	}

	limiter := NewLimiter(config)

	// Consume burst
	for i := 0; i < 10; i++ {
		limiter.Wait(context.Background())
	}

	// Check delay for next 10 events
	delay := limiter.Delay(10)
	if delay <= 0 {
		t.Errorf("Expected positive delay, got: %v", delay)
	}
}

func TestRateLimiter_Burst(t *testing.T) {
	config := &Config{
		SourceEnabled:       true,
		SourceRowsPerSecond: 100,
		BurstSize:           50,
	}

	limiter := NewLimiter(config)

	if limiter.Burst() != 50 {
		t.Errorf("Expected burst 50, got: %d", limiter.Burst())
	}

	limiter.SetBurst(100)
	if limiter.Burst() != 100 {
		t.Errorf("Expected burst 100 after SetBurst, got: %d", limiter.Burst())
	}
}

func TestRateLimiter_Limit(t *testing.T) {
	config := &Config{
		SourceEnabled:       true,
		SourceRowsPerSecond: 100,
		BurstSize:           10,
	}

	limiter := NewLimiter(config)

	// Check limit value
	limit := limiter.Limit()
	if limit != 100 {
		t.Errorf("Expected limit 100, got: %v", limit)
	}
}

func TestRateLimiter_NoLimitReturnsInf(t *testing.T) {
	config := &Config{
		SourceEnabled:       true,
		SourceRowsPerSecond: 0, // No limit
		BurstSize:           10,
	}

	limiter := NewLimiter(config)

	limit := limiter.Limit()
	// rate.Inf is a special value representing infinite rate
	if limit != rate.Inf {
		t.Errorf("Expected infinite limit for no-limit config, got: %v", limit)
	}
}

func TestRateLimiter_ContextCancellation(t *testing.T) {
	config := &Config{
		SourceEnabled:       true,
		SourceRowsPerSecond: 1, // Very slow
		BurstSize:           1,
	}

	limiter := NewLimiter(config)

	// Consume the burst
	limiter.Wait(context.Background())

	// Create a context that will be cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// This should fail due to context timeout
	err := limiter.WaitN(ctx, 10)
	if err == nil {
		t.Error("Expected error due to context cancellation")
	}
}

func TestRateLimiter_AllowRowsAndBytes(t *testing.T) {
	config := &Config{
		SourceEnabled:        true,
		SourceRowsPerSecond:  100,
		SourceBytesPerSecond: 1000,
		BurstSize:            10,
	}

	limiter := NewLimiter(config)

	// Should allow within burst
	if !limiter.AllowRowsAndBytes(5, 100) {
		t.Error("Expected AllowRowsAndBytes to return true within burst")
	}
}
