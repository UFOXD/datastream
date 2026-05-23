package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/UFOXD/datastream/internal/pipeline"
	"github.com/UFOXD/datastream/pkg/metrics"
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

	var resp struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp.Code != 0 {
		t.Errorf("Expected code 0, got %d", resp.Code)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		t.Fatalf("Failed to unmarshal data: %v", err)
	}

	if data["status"] != "healthy" {
		t.Errorf("Expected status 'healthy', got '%s'", data["status"])
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

	var resp struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp.Code != 0 {
		t.Errorf("Expected code 0, got %d", resp.Code)
	}

	var inner map[string]string
	if err := json.Unmarshal(resp.Data, &inner); err != nil {
		t.Fatalf("Failed to unmarshal data: %v", err)
	}

	if inner["key"] != "value" {
		t.Errorf("Expected key 'value', got '%s'", inner["key"])
	}
}

func TestWriteError(t *testing.T) {
	s := NewServer(nil)
	rec := httptest.NewRecorder()

	s.writeError(rec, http.StatusBadRequest, "test error")

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}

	var resp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp.Code != http.StatusBadRequest {
		t.Errorf("Expected code 400, got %d", resp.Code)
	}
	if resp.Message != "test error" {
		t.Errorf("Expected message 'test error', got '%s'", resp.Message)
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

func TestMetricsEndpoint_ReturnsPrometheusFormat(t *testing.T) {
	// promhttp.Handler() uses prometheus.DefaultGatherer. We exercise the
	// real endpoint by writing into the default registry directly, then
	// rely on the running test process's pre-registered metrics.
	metrics.TaskTotal.WithLabelValues("test_endpoint", "running").Set(1)

	s := NewServer(DefaultServerConfig())
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "datastream_task_total") {
		t.Errorf("body missing datastream_task_total; got first 500 chars: %s", body[:min(500, len(body))])
	}
	if !strings.Contains(body, "# HELP") {
		t.Errorf("body missing # HELP line (not real Prometheus output)")
	}
	// Ensure old dummy string is gone
	if strings.Contains(body, "# DataStream Metrics") && !strings.Contains(body, "# HELP") {
		t.Errorf("body still contains old dummy '# DataStream Metrics' without # HELP — handler not wired")
	}
}

func TestSetTaskPositionActuallyUpdates(t *testing.T) {
	// Setup: create a server with a TaskManager containing a task "task-1".
	tm := pipeline.NewTaskManager()
	if _, err := tm.Create(context.Background(), "task-1", "test-task", nil); err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(tm)

	// Send PUT /api/v1/tasks/task-1/position with binlog position.
	body := `{"binlogFile":"mysql-bin.000003","binlogPos":1234,"txId":"tx-99","seqNo":7}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/tasks/task-1/position", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	// Assert 200 OK response.
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	// Retrieve the task and verify position was actually set.
	task, err := tm.Get("task-1")
	if err != nil {
		t.Fatalf("failed to get task: %v", err)
	}

	pos := task.GetPosition()
	if pos == nil {
		t.Fatal("Expected position to be set, got nil")
	}

	if pos.BinlogFile != "mysql-bin.000003" {
		t.Errorf("Expected BinlogFile 'mysql-bin.000003', got '%s'", pos.BinlogFile)
	}
	if pos.BinlogPos != 1234 {
		t.Errorf("Expected BinlogPos 1234, got %d", pos.BinlogPos)
	}
	if pos.TxID != "tx-99" {
		t.Errorf("Expected TxID 'tx-99', got '%s'", pos.TxID)
	}
	if pos.SeqNo != 7 {
		t.Errorf("Expected SeqNo 7, got %d", pos.SeqNo)
	}
}

func TestAPIResponseEnvelopeFormat(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", rec.Code)
	}

	var resp struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp.Code != 0 {
		t.Errorf("Expected code 0, got %d", resp.Code)
	}
	if resp.Message != "success" {
		t.Errorf("Expected message 'success', got '%s'", resp.Message)
	}
	if resp.Data == nil {
		t.Error("Expected non-nil data field")
	}

	// Verify nested data contains health info
	var data map[string]interface{}
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		t.Fatalf("Failed to unmarshal data field: %v", err)
	}
	if data["status"] != "healthy" {
		t.Errorf("Expected data.status 'healthy', got '%v'", data["status"])
	}
}

func TestAPIErrorResponseEnvelopeFormat(t *testing.T) {
	s := NewServer(nil)
	s.SetTaskManager(pipeline.NewTaskManager())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/nonexistent", nil)
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	var resp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp.Code == 0 {
		t.Error("Expected non-zero error code")
	}
	if resp.Code != http.StatusNotFound {
		t.Errorf("Expected code 404, got %d", resp.Code)
	}
	if resp.Message == "" {
		t.Error("Expected non-empty error message")
	}
}

func TestWriteJSONEnvelopeStructure(t *testing.T) {
	s := NewServer(nil)
	rec := httptest.NewRecorder()

	data := map[string]string{"key": "value"}
	s.writeJSON(rec, http.StatusOK, data)

	var resp struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp.Code != 0 {
		t.Errorf("Expected code 0, got %d", resp.Code)
	}
	if resp.Message != "success" {
		t.Errorf("Expected message 'success', got '%s'", resp.Message)
	}

	var innerData map[string]string
	if err := json.Unmarshal(resp.Data, &innerData); err != nil {
		t.Fatalf("Failed to unmarshal data: %v", err)
	}
	if innerData["key"] != "value" {
		t.Errorf("Expected key 'value', got '%s'", innerData["key"])
	}
}

func TestWriteErrorEnvelopeStructure(t *testing.T) {
	s := NewServer(nil)
	rec := httptest.NewRecorder()

	s.writeError(rec, http.StatusBadRequest, "test error")

	var resp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if resp.Code != http.StatusBadRequest {
		t.Errorf("Expected code 400, got %d", resp.Code)
	}
	if resp.Message != "test error" {
		t.Errorf("Expected message 'test error', got '%s'", resp.Message)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
