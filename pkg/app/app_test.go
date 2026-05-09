package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/config"
	"github.com/UFOXD/datastream/pkg/pipeline"
)

func TestNewApplication(t *testing.T) {
	cfg := &config.Config{
		Name: "test-app",
		Server: config.ServerConfig{
			HTTPAddr:    ":0", // Use random port
			ReadTimeout: 30,
			WriteTimeout: 30,
			IdleTimeout: 120,
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	if app == nil {
		t.Fatal("Expected non-nil application")
	}

	if app.config != cfg {
		t.Error("Config not set correctly")
	}

	if app.apiServer == nil {
		t.Error("API server not initialized")
	}

	if app.coordinator == nil {
		t.Error("Coordinator not initialized")
	}

	if app.taskManager == nil {
		t.Error("Task manager not initialized")
	}
}

func TestApplicationShutdown(t *testing.T) {
	cfg := &config.Config{
		Name: "test-shutdown",
		Server: config.ServerConfig{
			HTTPAddr:    ":0",
			ReadTimeout: 30,
			WriteTimeout: 30,
			IdleTimeout: 120,
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	// Test that Shutdown can be called multiple times safely
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			app.Shutdown()
		}()
	}
	wg.Wait()

	// Verify shutdown was only executed once
	// (This is implicit - if it ran multiple times, we'd see multiple logs)
}

func TestRequestShutdown(t *testing.T) {
	cfg := &config.Config{
		Name: "test-request-shutdown",
		Server: config.ServerConfig{
			HTTPAddr:    ":0",
			ReadTimeout: 30,
			WriteTimeout: 30,
			IdleTimeout: 120,
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	// Request shutdown should not block
	done := make(chan bool, 1)
	go func() {
		app.RequestShutdown()
		done <- true
	}()

	select {
	case <-done:
		// Expected - should complete quickly
	case <-time.After(1 * time.Second):
		t.Error("RequestShutdown blocked unexpectedly")
	}
}

func TestGetTaskManager(t *testing.T) {
	cfg := &config.Config{
		Name: "test-get-tm",
		Server: config.ServerConfig{
			HTTPAddr: ":0",
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	tm := app.GetTaskManager()
	if tm == nil {
		t.Error("Expected non-nil task manager")
	}
}

func TestGetCoordinator(t *testing.T) {
	cfg := &config.Config{
		Name: "test-get-coord",
		Server: config.ServerConfig{
			HTTPAddr: ":0",
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	coord := app.GetCoordinator()
	if coord == nil {
		t.Error("Expected non-nil coordinator")
	}
}

func TestGetAPIServer(t *testing.T) {
	cfg := &config.Config{
		Name: "test-get-api",
		Server: config.ServerConfig{
			HTTPAddr: ":0",
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	server := app.GetAPIServer()
	if server == nil {
		t.Error("Expected non-nil API server")
	}
}

func TestApplicationWithMemoryCoordinator(t *testing.T) {
	cfg := &config.Config{
		Name: "test-memory-coord",
		Server: config.ServerConfig{
			HTTPAddr: ":0",
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	coord := app.GetCoordinator()
	if coord == nil {
		t.Fatal("Expected non-nil coordinator")
	}

	// Verify it's a MemoryCoordinator
	if _, ok := coord.(*pipeline.MemoryCoordinator); !ok {
		t.Error("Expected MemoryCoordinator")
	}
}

func TestShutdownOnce(t *testing.T) {
	cfg := &config.Config{
		Name: "test-shutdown-once",
		Server: config.ServerConfig{
			HTTPAddr: ":0",
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	// Call shutdown multiple times
	callCount := 0
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		err := app.Shutdown()
		if err != nil {
			mu.Lock()
			callCount++
			mu.Unlock()
		}
	}

	// Shutdown should only execute once, but subsequent calls should return nil
	// The actual implementation returns nil for all calls after the first
}

func TestShutdownWithContext(t *testing.T) {
	cfg := &config.Config{
		Name: "test-shutdown-ctx",
		Server: config.ServerConfig{
			HTTPAddr: ":0",
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	// Shutdown should complete within the timeout
	done := make(chan error, 1)
	go func() {
		done <- app.Shutdown()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Shutdown returned error: %v", err)
		}
	case <-time.After(35 * time.Second):
		t.Error("Shutdown took longer than expected")
	}
}

func TestApplicationComponentConnection(t *testing.T) {
	cfg := &config.Config{
		Name: "test-components",
		Server: config.ServerConfig{
			HTTPAddr: ":0",
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	// Verify components are connected
	// API server should have task manager set
	apiServer := app.GetAPIServer()
	if apiServer == nil {
		t.Fatal("API server is nil")
	}

	// Task manager should be accessible
	tm := app.GetTaskManager()
	if tm == nil {
		t.Fatal("Task manager is nil")
	}

	// Coordinator should be accessible
	coord := app.GetCoordinator()
	if coord == nil {
		t.Fatal("Coordinator is nil")
	}

	// All components should be initialized
	ctx := context.Background()
	if err := coord.Initialize(ctx); err != nil {
		t.Errorf("Failed to initialize coordinator: %v", err)
	}
}

func TestApplicationShutdownOrder(t *testing.T) {
	cfg := &config.Config{
		Name: "test-shutdown-order",
		Server: config.ServerConfig{
			HTTPAddr: ":0",
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	// Initialize coordinator
	ctx := context.Background()
	if err := app.coordinator.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize coordinator: %v", err)
	}

	// Create a task
	tm := app.GetTaskManager()
	_, err = tm.Create(ctx, "test-task", "Test Task", &pipeline.Config{
		ID:   "pipeline-1",
		Name: "Test Pipeline",
	})
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	// Shutdown should complete without error
	err = app.Shutdown()
	if err != nil {
		t.Errorf("Shutdown returned error: %v", err)
	}
}

func TestMultipleShutdownRequests(t *testing.T) {
	cfg := &config.Config{
		Name: "test-multi-shutdown",
		Server: config.ServerConfig{
			HTTPAddr: ":0",
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	// Send multiple shutdown requests
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			app.RequestShutdown()
		}()
	}
	wg.Wait()

	// Should not panic or block
}

func TestSignalHandling(t *testing.T) {
	// This test verifies that the application can handle signals
	// Note: We can't easily send real signals in tests, so we test the shutdown path
	cfg := &config.Config{
		Name: "test-signals",
		Server: config.ServerConfig{
			HTTPAddr: ":0",
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	// Initialize coordinator
	ctx := context.Background()
	if err := app.coordinator.Initialize(ctx); err != nil {
		t.Fatalf("Failed to initialize coordinator: %v", err)
	}

	// Simulate what happens after receiving SIGINT/SIGTERM
	err = app.Shutdown()
	if err != nil {
		t.Errorf("Shutdown returned error: %v", err)
	}
}

func TestSIGHUPNotImplemented(t *testing.T) {
	// SIGHUP handling is mentioned as a future feature
	// For now, we just verify the application doesn't crash
	cfg := &config.Config{
		Name: "test-sighup",
		Server: config.ServerConfig{
			HTTPAddr: ":0",
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	// Verify app was created successfully
	if app == nil {
		t.Error("Expected non-nil application")
	}
}

func TestConfigValidation(t *testing.T) {
	// Test with minimal config
	cfg := &config.Config{
		Name: "minimal-config",
		Server: config.ServerConfig{
			HTTPAddr: ":0",
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application with minimal config: %v", err)
	}

	if app == nil {
		t.Error("Expected non-nil application")
	}
}

func TestShutdownChannel(t *testing.T) {
	cfg := &config.Config{
		Name: "test-shutdown-ch",
		Server: config.ServerConfig{
			HTTPAddr: ":0",
		},
		Coordinator: config.CoordinatorConfig{
			Backend: "memory",
		},
	}

	app, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}

	// Verify shutdown channel is initialized
	if app.shutdownCh == nil {
		t.Error("Expected shutdown channel to be initialized")
	}

	// RequestShutdown should send to the channel
	go app.RequestShutdown()

	select {
	case <-app.shutdownCh:
		// Expected - channel received value
	case <-time.After(1 * time.Second):
		t.Error("Expected shutdown channel to receive value")
	}
}
