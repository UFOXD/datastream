package pipeline

import (
	"context"
	"testing"
	"time"
)

func TestBackpressureController_HighWatermark(t *testing.T) {
	config := DefaultBackpressureConfig()
	controller := NewBackpressureController(config)
	controller.Start()
	defer controller.Stop()

	// Initially normal
	if controller.State() != BackpressureStateNormal {
		t.Errorf("Expected initial state Normal, got %s", controller.State())
	}

	// Update to high usage
	controller.UpdateMetrics(90, 100, time.Second)

	// Wait for check
	time.Sleep(150 * time.Millisecond)

	// Should be paused
	if controller.State() != BackpressureStatePaused {
		t.Errorf("Expected state Paused, got %s", controller.State())
	}
}

func TestBackpressureController_LowWatermark(t *testing.T) {
	config := DefaultBackpressureConfig()
	controller := NewBackpressureController(config)
	controller.Start()
	defer controller.Stop()

	// Set to paused state
	controller.UpdateMetrics(90, 100, time.Second)
	time.Sleep(150 * time.Millisecond)

	if controller.State() != BackpressureStatePaused {
		t.Fatalf("Expected state Paused, got %s", controller.State())
	}

	// Reduce usage
	controller.UpdateMetrics(30, 100, time.Millisecond)
	time.Sleep(150 * time.Millisecond)

	// Should be normal
	if controller.State() != BackpressureStateNormal {
		t.Errorf("Expected state Normal, got %s", controller.State())
	}
}

func TestBackpressureController_HighLatency(t *testing.T) {
	config := &BackpressureConfig{
		EnableBackpressure: true,
		HighWatermark:      80,
		LowWatermark:       50,
		MaxLatency:         100 * time.Millisecond,
		CheckInterval:      50 * time.Millisecond,
	}

	controller := NewBackpressureController(config)
	controller.Start()
	defer controller.Stop()

	// Low queue usage but high latency
	controller.UpdateMetrics(10, 100, 200*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	// Should be paused due to latency
	if controller.State() != BackpressureStatePaused {
		t.Errorf("Expected state Paused due to latency, got %s", controller.State())
	}
}

func TestBackpressureController_Callbacks(t *testing.T) {
	config := DefaultBackpressureConfig()
	controller := NewBackpressureController(config)
	controller.Start()
	defer controller.Stop()

	pauseCalled := false
	resumeCalled := false

	controller.OnPause(func() {
		pauseCalled = true
	})
	controller.OnResume(func() {
		resumeCalled = true
	})

	// Trigger pause
	controller.UpdateMetrics(90, 100, time.Second)
	time.Sleep(150 * time.Millisecond)

	if !pauseCalled {
		t.Error("Expected onPause callback to be called")
	}

	// Trigger resume
	controller.UpdateMetrics(30, 100, time.Millisecond)
	time.Sleep(150 * time.Millisecond)

	if !resumeCalled {
		t.Error("Expected onResume callback to be called")
	}
}

func TestBackpressureController_WaitWhilePaused(t *testing.T) {
	config := &BackpressureConfig{
		EnableBackpressure: true,
		HighWatermark:      80,
		LowWatermark:       50,
		MaxLatency:         5 * time.Second,
		CheckInterval:      20 * time.Millisecond,
	}

	controller := NewBackpressureController(config)
	controller.Start()
	defer controller.Stop()

	// Set to paused
	controller.UpdateMetrics(90, 100, time.Second)
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Start goroutine to resume
	go func() {
		time.Sleep(100 * time.Millisecond)
		controller.UpdateMetrics(30, 100, time.Millisecond)
	}()

	err := controller.WaitWhilePaused(ctx)
	if err != nil {
		t.Errorf("WaitWhilePaused failed: %v", err)
	}
}

func TestBackpressureController_Disabled(t *testing.T) {
	config := &BackpressureConfig{
		EnableBackpressure: false,
		HighWatermark:      80,
		LowWatermark:       50,
		MaxLatency:         5 * time.Second,
		CheckInterval:      50 * time.Millisecond,
	}

	controller := NewBackpressureController(config)
	controller.Start()
	defer controller.Stop()

	// High usage
	controller.UpdateMetrics(90, 100, time.Second)
	time.Sleep(100 * time.Millisecond)

	// Should remain normal (disabled)
	if controller.State() != BackpressureStateNormal {
		t.Errorf("Expected state Normal (disabled), got %s", controller.State())
	}

	if controller.ShouldPause() {
		t.Error("ShouldPause should return false when disabled")
	}
}
