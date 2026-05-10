package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/UFOXD/datastream/internal/pipeline"
)

func TestNewServer(t *testing.T) {
	cfg := DefaultServerConfig()
	s := NewServer(cfg)

	if s == nil {
		t.Fatal("Expected non-nil server")
	}

	if s.config.Addr != ":8300" {
		t.Errorf("Expected addr ':8300', got '%s'", s.config.Addr)
	}
}

func TestDefaultServerConfig(t *testing.T) {
	cfg := DefaultServerConfig()

	if cfg.Addr != ":8300" {
		t.Errorf("Expected addr ':8300', got '%s'", cfg.Addr)
	}

	if cfg.ReadTimeout != 30 {
		t.Errorf("Expected read timeout 30, got %d", cfg.ReadTimeout)
	}

	if cfg.WriteTimeout != 30 {
		t.Errorf("Expected write timeout 30, got %d", cfg.WriteTimeout)
	}
}

func TestHandleHealth(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	s.handleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp["status"] != "healthy" {
		t.Errorf("Expected status 'healthy', got '%s'", resp["status"])
	}
}

func TestListTasksNoManager(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks", nil)
	rec := httptest.NewRecorder()

	s.listTasks(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", rec.Code)
	}
}

func TestCreateTaskNoManager(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks", nil)
	rec := httptest.NewRecorder()

	s.createTask(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", rec.Code)
	}
}

func TestGetTaskNotFound(t *testing.T) {
	s := NewServer(nil)
	s.SetTaskManager(pipeline.NewTaskManager())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/not-exist", nil)
	rec := httptest.NewRecorder()

	// Set path variables
	req.SetPathValue("id", "not-exist")

	s.getTask(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", rec.Code)
	}
}

func TestWriteJSON(t *testing.T) {
	s := NewServer(nil)
	rec := httptest.NewRecorder()

	data := map[string]string{"key": "value"}
	s.writeJSON(rec, http.StatusOK, data)

	if rec.Header().Get("Content-Type") != "application/json" {
		t.Error("Expected Content-Type application/json")
	}

	var result map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if result["key"] != "value" {
		t.Errorf("Expected key 'value', got '%s'", result["key"])
	}
}

func TestWriteError(t *testing.T) {
	s := NewServer(nil)
	rec := httptest.NewRecorder()

	s.writeError(rec, http.StatusBadRequest, "test error")

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}

	var result map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if result["error"] != "test error" {
		t.Errorf("Expected error 'test error', got '%s'", result["error"])
	}
}

func TestAPIErrors(t *testing.T) {
	errors := []error{
		ErrInvalidRequest,
		ErrTaskNotFound,
		ErrInternalError,
		ErrServiceUnavailable,
	}

	for _, err := range errors {
		if err == nil {
			t.Error("Error should not be nil")
		}
	}
}

func TestSetTaskManager(t *testing.T) {
	s := NewServer(nil)
	tm := pipeline.NewTaskManager()

	s.SetTaskManager(tm)

	if s.taskMgr == nil {
		t.Error("Task manager should not be nil after setting")
	}
}

func TestSetCoordinator(t *testing.T) {
	s := NewServer(nil)
	c := pipeline.NewMemoryCoordinator("node-1")

	s.SetCoordinator(c)

	if s.coordinator == nil {
		t.Error("Coordinator should not be nil after setting")
	}
}

func TestRoutesSetup(t *testing.T) {
	s := NewServer(nil)

	// Test that router is set up
	if s.router == nil {
		t.Fatal("Router should not be nil")
	}

	// Test a simple route
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rec.Code)
	}
}
