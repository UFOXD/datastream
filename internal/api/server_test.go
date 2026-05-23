package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/UFOXD/datastream/internal/pipeline"
	"github.com/UFOXD/datastream/internal/source"
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

func TestUpdateTask(t *testing.T) {
	tm := pipeline.NewTaskManager()
	if _, err := tm.Create(context.Background(), "task-1", "original-name", nil); err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(tm)

	body := `{"name":"updated-name"}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/tasks/task-1", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	// Verify the name was updated
	task, err := tm.Get("task-1")
	if err != nil {
		t.Fatalf("failed to get task: %v", err)
	}
	if task.Name != "updated-name" {
		t.Errorf("Expected name 'updated-name', got '%s'", task.Name)
	}
}

func TestUpdateTaskNoManager(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/tasks/task-1", strings.NewReader(`{"name":"x"}`))
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", rec.Code)
	}
}

func TestUpdateTaskNotFound(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(pipeline.NewTaskManager())

	req := httptest.NewRequest(http.MethodPut, "/api/v1/tasks/nonexistent", strings.NewReader(`{"name":"x"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", rec.Code)
	}
}

func TestUpdateTaskInvalidBody(t *testing.T) {
	tm := pipeline.NewTaskManager()
	if _, err := tm.Create(context.Background(), "task-1", "test", nil); err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(tm)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/tasks/task-1", strings.NewReader(`not json`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rec.Code)
	}
}

func TestRestartTaskNoPipeline(t *testing.T) {
	// A task without a Pipeline cannot be started, so restart should fail on Start.
	tm := pipeline.NewTaskManager()
	if _, err := tm.Create(context.Background(), "task-1", "test", nil); err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(tm)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/task-1/restart", nil)
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	// Task has no pipeline, so Start will fail
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500 (no pipeline), got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestRestartTaskNoManager(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/task-1/restart", nil)
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", rec.Code)
	}
}

func TestRestartTaskNotFound(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(pipeline.NewTaskManager())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/nonexistent/restart", nil)
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", rec.Code)
	}
}

func TestGetTaskStatus(t *testing.T) {
	tm := pipeline.NewTaskManager()
	if _, err := tm.Create(context.Background(), "task-1", "test", nil); err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(tm)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/task-1/status", nil)
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
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

	if _, ok := data["status"]; !ok {
		t.Error("Expected 'status' field in response data")
	}
	if data["taskId"] != "task-1" {
		t.Errorf("Expected taskId 'task-1', got '%v'", data["taskId"])
	}
}

func TestGetTaskStatusNoManager(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/task-1/status", nil)
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", rec.Code)
	}
}

func TestGetTaskStatusNotFound(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(pipeline.NewTaskManager())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/nonexistent/status", nil)
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", rec.Code)
	}
}

func TestGetTaskProgress(t *testing.T) {
	tm := pipeline.NewTaskManager()
	if _, err := tm.Create(context.Background(), "task-1", "test", nil); err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(tm)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/task-1/progress", nil)
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
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

	// Verify expected fields exist
	if data["taskId"] != "task-1" {
		t.Errorf("Expected taskId 'task-1', got '%v'", data["taskId"])
	}
	if _, ok := data["position"]; !ok {
		t.Error("Expected 'position' field in response data")
	}
	if _, ok := data["eventsRead"]; !ok {
		t.Error("Expected 'eventsRead' field in response data")
	}
	if _, ok := data["eventsWritten"]; !ok {
		t.Error("Expected 'eventsWritten' field in response data")
	}
}

func TestGetTaskProgressNoManager(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/task-1/progress", nil)
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", rec.Code)
	}
}

func TestGetTaskProgressNotFound(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(pipeline.NewTaskManager())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/nonexistent/progress", nil)
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", rec.Code)
	}
}

func TestReadyCheckWithManager(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(pipeline.NewTaskManager())

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
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
	if data["ready"] != true {
		t.Errorf("Expected ready=true, got %v", data["ready"])
	}
}

func TestReadyCheckWithoutManager(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	// Intentionally do NOT set a task manager

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("Expected status 503, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		t.Fatalf("Failed to unmarshal data: %v", err)
	}
	if data["ready"] != false {
		t.Errorf("Expected ready=false, got %v", data["ready"])
	}
}

// --- Cluster management endpoint tests ---

func TestDeleteNodeNoCoordinator(t *testing.T) {
	s := NewServer(DefaultServerConfig())

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/nodes/node-1", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", rec.Code)
	}
}

func TestDeleteNodeHappyPath(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	coord := pipeline.NewMemoryCoordinator("node-1")
	coord.RegisterNode(context.Background(), "node-1", pipeline.NodeInfo{ID: "node-1", Address: "localhost:8300"})
	s.SetCoordinator(coord)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/nodes/node-1", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if resp.Code != 0 {
		t.Errorf("Expected code 0, got %d", resp.Code)
	}

	var data map[string]string
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		t.Fatalf("Failed to unmarshal data: %v", err)
	}
	if data["status"] != "unregistered" {
		t.Errorf("Expected status 'unregistered', got '%s'", data["status"])
	}

	// Verify node was actually removed
	nodes, _ := coord.ListNodes(context.Background())
	if len(nodes) != 0 {
		t.Errorf("Expected 0 nodes after delete, got %d", len(nodes))
	}
}

func TestDrainNodeNoManager(t *testing.T) {
	s := NewServer(DefaultServerConfig())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/nodes/node-1/drain", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", rec.Code)
	}
}

func TestDrainNodeHappyPath(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	tm := pipeline.NewTaskManager()
	// Create tasks (they won't be running, so stopped count should be 0)
	tm.Create(context.Background(), "task-1", "test-1", nil)
	tm.Create(context.Background(), "task-2", "test-2", nil)
	s.SetTaskManager(tm)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/nodes/node-1/drain", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		t.Fatalf("Failed to unmarshal data: %v", err)
	}
	if data["status"] != "drained" {
		t.Errorf("Expected status 'drained', got '%v'", data["status"])
	}
	// Tasks are not running, so tasksStopped should be 0
	if data["tasksStopped"] != float64(0) {
		t.Errorf("Expected tasksStopped 0, got %v", data["tasksStopped"])
	}
}

func TestGetClusterStatusNoCoordinator(t *testing.T) {
	s := NewServer(DefaultServerConfig())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster/status", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", rec.Code)
	}
}

func TestGetClusterStatusHappyPath(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	coord := pipeline.NewMemoryCoordinator("node-1")
	coord.RegisterNode(context.Background(), "node-1", pipeline.NodeInfo{ID: "node-1"})
	s.SetCoordinator(coord)

	tm := pipeline.NewTaskManager()
	tm.Create(context.Background(), "task-1", "test", nil)
	s.SetTaskManager(tm)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster/status", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		t.Fatalf("Failed to unmarshal data: %v", err)
	}

	if data["nodeCount"] != float64(1) {
		t.Errorf("Expected nodeCount 1, got %v", data["nodeCount"])
	}
	if data["taskCount"] != float64(1) {
		t.Errorf("Expected taskCount 1, got %v", data["taskCount"])
	}
}

func TestGetClusterLeaderNoCoordinator(t *testing.T) {
	s := NewServer(DefaultServerConfig())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster/leader", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", rec.Code)
	}
}

func TestGetClusterLeaderHappyPath(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	coord := pipeline.NewMemoryCoordinator("node-1")
	coord.RegisterNode(context.Background(), "node-1", pipeline.NodeInfo{ID: "node-1", Address: "localhost:8300"})
	s.SetCoordinator(coord)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster/leader", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		t.Fatalf("Failed to unmarshal data: %v", err)
	}

	if data["leader"] == nil {
		t.Error("Expected non-nil leader")
	}
}

func TestGetClusterLeaderNoNodes(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	coord := pipeline.NewMemoryCoordinator("node-1")
	s.SetCoordinator(coord)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster/leader", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		t.Fatalf("Failed to unmarshal data: %v", err)
	}

	// With no nodes, leader should be null
	if data["leader"] != nil {
		t.Errorf("Expected nil leader when no nodes, got %v", data["leader"])
	}
}

func TestRebalanceClusterNoCoordinator(t *testing.T) {
	s := NewServer(DefaultServerConfig())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/rebalance", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", rec.Code)
	}
}

func TestRebalanceClusterHappyPath(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	coord := pipeline.NewMemoryCoordinator("node-1")
	s.SetCoordinator(coord)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/rebalance", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	var data map[string]string
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		t.Fatalf("Failed to unmarshal data: %v", err)
	}

	if data["status"] != "rebalanced" {
		t.Errorf("Expected status 'rebalanced', got '%s'", data["status"])
	}
	if data["message"] != "single-node mode, no rebalance needed" {
		t.Errorf("Expected single-node message, got '%s'", data["message"])
	}
}

func TestDiagnoseHappyPath(t *testing.T) {
	s := NewServer(DefaultServerConfig())

	req := httptest.NewRequest(http.MethodGet, "/diagnose", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
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

	// Verify expected fields
	if _, ok := data["goVersion"]; !ok {
		t.Error("Expected 'goVersion' field")
	}
	if _, ok := data["goroutines"]; !ok {
		t.Error("Expected 'goroutines' field")
	}
	if _, ok := data["heapAlloc"]; !ok {
		t.Error("Expected 'heapAlloc' field")
	}
	if _, ok := data["heapSys"]; !ok {
		t.Error("Expected 'heapSys' field")
	}
	if _, ok := data["numGC"]; !ok {
		t.Error("Expected 'numGC' field")
	}
	// Without taskMgr, "tasks" should not be present
	if _, ok := data["tasks"]; ok {
		t.Error("Expected no 'tasks' field when taskMgr is nil")
	}
}

func TestDiagnoseWithTaskManager(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	tm := pipeline.NewTaskManager()
	tm.Create(context.Background(), "task-1", "test", nil)
	tm.Create(context.Background(), "task-2", "test2", nil)
	s.SetTaskManager(tm)

	req := httptest.NewRequest(http.MethodGet, "/diagnose", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(resp.Data, &data); err != nil {
		t.Fatalf("Failed to unmarshal data: %v", err)
	}

	if data["tasks"] != float64(2) {
		t.Errorf("Expected tasks 2, got %v", data["tasks"])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------------------------------------------------------------------------
// Server Start / Stop lifecycle
// ---------------------------------------------------------------------------

func TestServerStartStop(t *testing.T) {
	cfg := &ServerConfig{
		Addr:         "127.0.0.1:0", // port 0 = OS picks free port
		ReadTimeout:  1,
		WriteTimeout: 1,
		IdleTimeout:  1,
	}
	s := NewServer(cfg)

	ctx := context.Background()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	// Stop should succeed
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}

func TestServerStopWithoutStart(t *testing.T) {
	s := NewServer(nil)
	// httpServer is nil when never started — Stop should return nil.
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("Stop without Start should return nil, got: %v", err)
	}
}

func TestNewServerNilConfig(t *testing.T) {
	s := NewServer(nil)
	if s.config.Addr != ":8300" {
		t.Errorf("Expected default addr ':8300', got '%s'", s.config.Addr)
	}
}

// ---------------------------------------------------------------------------
// Task handlers — happy-path & error paths not yet covered
// ---------------------------------------------------------------------------

func TestListTasksHappyPath(t *testing.T) {
	tm := pipeline.NewTaskManager()
	tm.Create(context.Background(), "t-1", "task-one", nil)
	tm.Create(context.Background(), "t-2", "task-two", nil)

	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(tm)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	var data map[string]interface{}
	json.Unmarshal(resp.Data, &data)

	if data["count"] != float64(2) {
		t.Errorf("Expected count 2, got %v", data["count"])
	}
}

func TestCreateTaskHappyPath(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(pipeline.NewTaskManager())

	body := `{"id":"new-task","name":"test-create","config":null}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("Expected 201, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestCreateTaskInvalidBody(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(pipeline.NewTaskManager())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks", strings.NewReader(`not-json`))
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", rec.Code)
	}
}

func TestCreateTaskDuplicate(t *testing.T) {
	tm := pipeline.NewTaskManager()
	tm.Create(context.Background(), "dup", "dup-task", nil)

	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(tm)

	body := `{"id":"dup","name":"dup-task"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 for duplicate, got %d", rec.Code)
	}
}

func TestGetTaskHappyPath(t *testing.T) {
	tm := pipeline.NewTaskManager()
	tm.Create(context.Background(), "t-1", "test", nil)

	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(tm)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t-1", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestGetTaskNoManager(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t-1", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rec.Code)
	}
}

func TestDeleteTaskHappyPath(t *testing.T) {
	tm := pipeline.NewTaskManager()
	tm.Create(context.Background(), "del-1", "del", nil)

	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(tm)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tasks/del-1", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestDeleteTaskNoManager(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tasks/del-1", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rec.Code)
	}
}

func TestDeleteTaskNotFound(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(pipeline.NewTaskManager())

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tasks/nonexistent", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500, got %d", rec.Code)
	}
}

func TestStartTaskNoManager(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/t-1/start", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rec.Code)
	}
}

func TestStartTaskNotFound(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(pipeline.NewTaskManager())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/nonexistent/start", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500, got %d", rec.Code)
	}
}

func TestStopTaskNoManager(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/t-1/stop", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rec.Code)
	}
}

func TestStopTaskNotFound(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(pipeline.NewTaskManager())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/nonexistent/stop", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500, got %d", rec.Code)
	}
}

func TestStopTaskHappyPath(t *testing.T) {
	tm := pipeline.NewTaskManager()
	tm.Create(context.Background(), "t-1", "test", nil)

	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(tm)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/t-1/stop", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestPauseTaskNoManager(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/t-1/pause", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rec.Code)
	}
}

func TestPauseTaskNotFound(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(pipeline.NewTaskManager())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/nonexistent/pause", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", rec.Code)
	}
}

func TestPauseTaskNotRunning(t *testing.T) {
	// Pause requires TaskStatusRunning; a newly created task is TaskStatusCreated.
	tm := pipeline.NewTaskManager()
	tm.Create(context.Background(), "t-1", "test", nil)

	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(tm)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/t-1/pause", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500, got %d", rec.Code)
	}
}

func TestResumeTaskNoManager(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/t-1/resume", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rec.Code)
	}
}

func TestResumeTaskNotFound(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(pipeline.NewTaskManager())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/nonexistent/resume", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", rec.Code)
	}
}

func TestResumeTaskNotPaused(t *testing.T) {
	// Resume requires TaskStatusPaused; a newly created task is TaskStatusCreated.
	tm := pipeline.NewTaskManager()
	tm.Create(context.Background(), "t-1", "test", nil)

	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(tm)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/t-1/resume", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500, got %d", rec.Code)
	}
}

func TestGetTaskPositionNoManager(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t-1/position", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rec.Code)
	}
}

func TestGetTaskPositionNotFound(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(pipeline.NewTaskManager())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/nonexistent/position", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", rec.Code)
	}
}

func TestGetTaskPositionHappyPath(t *testing.T) {
	tm := pipeline.NewTaskManager()
	tm.Create(context.Background(), "t-1", "test", nil)

	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(tm)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t-1/position", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	var data map[string]interface{}
	json.Unmarshal(resp.Data, &data)

	if data["taskId"] != "t-1" {
		t.Errorf("Expected taskId 't-1', got '%v'", data["taskId"])
	}
}

func TestSetTaskPositionNoManager(t *testing.T) {
	s := NewServer(nil)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/tasks/t-1/position", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rec.Code)
	}
}

func TestSetTaskPositionNotFound(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(pipeline.NewTaskManager())

	body := `{"binlogFile":"mysql-bin.000001","binlogPos":100}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/tasks/nonexistent/position", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", rec.Code)
	}
}

func TestSetTaskPositionInvalidBody(t *testing.T) {
	tm := pipeline.NewTaskManager()
	tm.Create(context.Background(), "t-1", "test", nil)

	s := NewServer(DefaultServerConfig())
	s.SetTaskManager(tm)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/tasks/t-1/position", strings.NewReader(`not json`))
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// Cluster / Node handlers — additional coverage
// ---------------------------------------------------------------------------

func TestListNodesNoCoordinator(t *testing.T) {
	s := NewServer(DefaultServerConfig())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/nodes", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rec.Code)
	}
}

func TestListNodesHappyPath(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	coord := pipeline.NewMemoryCoordinator("node-1")
	coord.RegisterNode(context.Background(), "node-1", pipeline.NodeInfo{ID: "node-1"})
	coord.RegisterNode(context.Background(), "node-2", pipeline.NodeInfo{ID: "node-2"})
	s.SetCoordinator(coord)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/nodes", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	var data map[string]interface{}
	json.Unmarshal(resp.Data, &data)

	if data["count"] != float64(2) {
		t.Errorf("Expected count 2, got %v", data["count"])
	}
}

func TestGetClusterStatusWithCoordinatorNoTaskMgr(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	coord := pipeline.NewMemoryCoordinator("node-1")
	coord.RegisterNode(context.Background(), "node-1", pipeline.NodeInfo{ID: "node-1"})
	s.SetCoordinator(coord)
	// Intentionally do NOT set taskMgr

	req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster/status", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	var data map[string]interface{}
	json.Unmarshal(resp.Data, &data)

	// Without taskMgr, taskCount should not appear
	if _, ok := data["taskCount"]; ok {
		t.Error("Expected no 'taskCount' when taskMgr is nil")
	}
	if data["nodeCount"] != float64(1) {
		t.Errorf("Expected nodeCount 1, got %v", data["nodeCount"])
	}
}

// ---------------------------------------------------------------------------
// Table handler tests
// ---------------------------------------------------------------------------

func newTestTableManager(tables ...string) *source.TableManager {
	scope := &source.TableScope{Names: tables}
	return source.NewTableManager(&source.TableManagerConfig{
		Scope: scope,
	})
}

func TestListTablesNoManager(t *testing.T) {
	s := NewServer(DefaultServerConfig())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tables", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rec.Code)
	}
}

func TestListTablesHappyPath(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager("db1.users", "db1.orders")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tables", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	var data map[string]interface{}
	json.Unmarshal(resp.Data, &data)

	if data["count"] != float64(2) {
		t.Errorf("Expected count 2, got %v", data["count"])
	}
}

func TestAddTablesNoManager(t *testing.T) {
	s := NewServer(DefaultServerConfig())

	body := `{"tables":["db1.t1"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rec.Code)
	}
}

func TestAddTablesHappyPath(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager()

	body := `{"tables":["db1.newtable"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	var data map[string]interface{}
	json.Unmarshal(resp.Data, &data)

	if data["status"] != "added" {
		t.Errorf("Expected status 'added', got '%v'", data["status"])
	}
}

func TestAddTablesInvalidBody(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables", strings.NewReader(`not-json`))
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", rec.Code)
	}
}

func TestAddTablesEmptyList(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager()

	body := `{"tables":[]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", rec.Code)
	}
}

func TestAddTablesDuplicate(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager("db1.users")

	body := `{"tables":["db1.users"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Errorf("Expected 409, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestAddTablesInvalidFormat(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager()

	// "noDot" is invalid — parseTableName requires "database.table" format
	body := `{"tables":["noDot"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestRemoveTablesNoManager(t *testing.T) {
	s := NewServer(DefaultServerConfig())

	body := `{"tables":["db1.t1"]}`
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tables", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rec.Code)
	}
}

func TestRemoveTablesHappyPath(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager("db1.users")

	body := `{"tables":["db1.users"]}`
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tables", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	var data map[string]interface{}
	json.Unmarshal(resp.Data, &data)

	if data["status"] != "removed" {
		t.Errorf("Expected status 'removed', got '%v'", data["status"])
	}
}

func TestRemoveTablesInvalidBody(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager()

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tables", strings.NewReader(`not-json`))
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", rec.Code)
	}
}

func TestRemoveTablesEmptyList(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager()

	body := `{"tables":[]}`
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tables", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", rec.Code)
	}
}

func TestRemoveTablesNotFound(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager()

	body := `{"tables":["db1.nonexistent"]}`
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tables", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestRemoveTablesInvalidFormat(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager()

	body := `{"tables":["noDot"]}`
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tables", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestGetTableStateNoManager(t *testing.T) {
	s := NewServer(DefaultServerConfig())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tables/db1/users", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rec.Code)
	}
}

func TestGetTableStateHappyPath(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager("db1.users")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tables/db1/users", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	var data map[string]interface{}
	json.Unmarshal(resp.Data, &data)

	if data["database"] != "db1" {
		t.Errorf("Expected database 'db1', got '%v'", data["database"])
	}
	if data["table"] != "users" {
		t.Errorf("Expected table 'users', got '%v'", data["table"])
	}
	if data["status"] != "pending" {
		t.Errorf("Expected status 'pending', got '%v'", data["status"])
	}
}

func TestGetTableStateNotFound(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tables/db1/nonexistent", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", rec.Code)
	}
}

func TestPauseTableNoManager(t *testing.T) {
	s := NewServer(DefaultServerConfig())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables/db1/users/pause", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rec.Code)
	}
}

func TestPauseTableHappyPath(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager("db1.users")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables/db1/users/pause", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	var data map[string]string
	json.Unmarshal(resp.Data, &data)

	if data["status"] != "paused" {
		t.Errorf("Expected status 'paused', got '%s'", data["status"])
	}
}

func TestPauseTableNotFound(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables/db1/nonexistent/pause", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestResumeTableNoManager(t *testing.T) {
	s := NewServer(DefaultServerConfig())

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables/db1/users/resume", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503, got %d", rec.Code)
	}
}

func TestResumeTableHappyPath(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager("db1.users")

	// First pause the table, then resume it
	s.TableManager.PauseTable(context.Background(), "db1", "users")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables/db1/users/resume", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	var data map[string]string
	json.Unmarshal(resp.Data, &data)

	if data["status"] != "running" {
		t.Errorf("Expected status 'running', got '%s'", data["status"])
	}
}

func TestResumeTableNotFound(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tables/db1/nonexistent/resume", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

// ---------------------------------------------------------------------------
// getTableState with paused and error states
// ---------------------------------------------------------------------------

func TestGetTableStatePausedTable(t *testing.T) {
	s := NewServer(DefaultServerConfig())
	s.TableManager = newTestTableManager("db1.users")

	// Pause the table to populate PausedAt field
	s.TableManager.PauseTable(context.Background(), "db1", "users")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tables/db1/users", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)

	var data map[string]interface{}
	json.Unmarshal(resp.Data, &data)

	if data["status"] != "paused" {
		t.Errorf("Expected status 'paused', got '%v'", data["status"])
	}
	if _, ok := data["paused_at"]; !ok {
		t.Error("Expected 'paused_at' field in response for paused table")
	}
}
