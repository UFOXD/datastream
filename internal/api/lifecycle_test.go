package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/UFOXD/datastream/internal/cache"
	"github.com/UFOXD/datastream/internal/lifecycle"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockCacheBackend is a no-op BinlogCacheBackend for testing.
type mockCacheBackend struct{}

func (m *mockCacheBackend) Write(_ context.Context, _ string, _ *cache.CacheEvent) error {
	return nil
}
func (m *mockCacheBackend) WriteBatch(_ context.Context, _ string, _ []*cache.CacheEvent) error {
	return nil
}
func (m *mockCacheBackend) Read(_ context.Context, _ string, _ string, _ int64) cache.ReadResult {
	ch := make(chan *cache.CacheEvent)
	close(ch)
	return cache.ReadResult{Events: ch, Err: make(chan error)}
}
func (m *mockCacheBackend) Delete(_ context.Context, _ string) error { return nil }
func (m *mockCacheBackend) Size(_ context.Context, _ string) (int64, error) {
	return 0, nil
}
func (m *mockCacheBackend) TotalSize(_ context.Context) (int64, error) { return 0, nil }
func (m *mockCacheBackend) Exists(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (m *mockCacheBackend) Sync(_ context.Context, _ string) error { return nil }
func (m *mockCacheBackend) TruncateToLastComplete(_ context.Context, _ string) (*event.Position, error) {
	return nil, nil
}
func (m *mockCacheBackend) Close() error { return nil }

// setupLifecycleServer creates a Server with a SnapshotScheduler and test data.
func setupLifecycleServer(t *testing.T) *Server {
	t.Helper()

	store := source.NewMemoryLifecycleStore()
	cacheBackend := &mockCacheBackend{}
	config := lifecycle.DefaultSchedulerConfig()
	scheduler := lifecycle.NewSnapshotScheduler(config, "task-1", store, cacheBackend)

	// Add test tables in different states
	_ = scheduler.AddTable(source.TableID{Database: "db1", Table: "users"}, &event.Position{BinlogFile: "mysql-bin.000001", BinlogPos: 100})
	_ = scheduler.AddTable(source.TableID{Database: "db1", Table: "orders"}, &event.Position{BinlogFile: "mysql-bin.000001", BinlogPos: 200})
	_ = scheduler.AddTable(source.TableID{Database: "db2", Table: "products"}, &event.Position{BinlogFile: "mysql-bin.000001", BinlogPos: 300})

	// Transition db1.orders to error state
	_ = scheduler.OnSnapshotError(source.TableID{Database: "db1", Table: "orders"}, "snapshot failed: connection reset")

	srv := NewServer(DefaultServerConfig())
	srv.SetScheduler(scheduler)
	return srv
}

func TestGetTaskDetailEndpoint(t *testing.T) {
	srv := setupLifecycleServer(t)

	req := httptest.NewRequest("GET", "/api/v1/tasks/task-1/detail", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp apiResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Code)

	// Parse inner data
	dataBytes, _ := json.Marshal(resp.Data)
	var data map[string]interface{}
	json.Unmarshal(dataBytes, &data)

	// Check summary exists and has correct total
	summary, ok := data["summary"].(map[string]interface{})
	require.True(t, ok, "expected summary in response")
	assert.Equal(t, float64(3), summary["total"])

	// Check tables field exists
	tables, ok := data["tables"]
	require.True(t, ok, "expected tables in response")
	require.NotNil(t, tables)
}

func TestGetTableErrorsEndpoint(t *testing.T) {
	srv := setupLifecycleServer(t)

	req := httptest.NewRequest("GET", "/api/v1/tasks/task-1/tables/errors", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp apiResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Code)

	dataBytes, _ := json.Marshal(resp.Data)
	var data map[string]interface{}
	json.Unmarshal(dataBytes, &data)

	assert.Equal(t, float64(1), data["count"])

	tables, ok := data["tables"].([]interface{})
	require.True(t, ok)
	assert.Len(t, tables, 1)
}

func TestRestartTablesEndpoint(t *testing.T) {
	srv := setupLifecycleServer(t)

	// Restart the error table
	body := `{"tables":["db1.orders"],"schemas":[],"force":true}`
	req := httptest.NewRequest("POST", "/api/v1/tasks/task-1/tables/restart", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp apiResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Code)

	dataBytes, _ := json.Marshal(resp.Data)
	var data map[string]interface{}
	json.Unmarshal(dataBytes, &data)

	restarted, ok := data["restartedTables"].([]interface{})
	require.True(t, ok)
	assert.Contains(t, restarted, "db1.orders")
}

func TestRestartTablesEndpointInvalidBody(t *testing.T) {
	srv := setupLifecycleServer(t)

	req := httptest.NewRequest("POST", "/api/v1/tasks/task-1/tables/restart", strings.NewReader(`not-json`))
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetTableLifecycleStateEndpoint(t *testing.T) {
	srv := setupLifecycleServer(t)

	req := httptest.NewRequest("GET", "/api/v1/tasks/task-1/tables/db1.users/lifecycle", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp apiResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, 0, resp.Code)

	dataBytes, _ := json.Marshal(resp.Data)
	var data map[string]interface{}
	json.Unmarshal(dataBytes, &data)

	assert.Equal(t, "pending", data["state"])
}

func TestGetTableLifecycleStateNotFound(t *testing.T) {
	srv := setupLifecycleServer(t)

	req := httptest.NewRequest("GET", "/api/v1/tasks/task-1/tables/db1.nonexistent/lifecycle", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestPauseTableLifecycleEndpoint(t *testing.T) {
	// Set up a table in streaming state so it can be paused
	store := source.NewMemoryLifecycleStore()
	cacheBackend := &mockCacheBackend{}
	config := lifecycle.DefaultSchedulerConfig()
	scheduler := lifecycle.NewSnapshotScheduler(config, "task-1", store, cacheBackend)

	tableID := source.TableID{Database: "db1", Table: "users"}
	_ = scheduler.AddTable(tableID, &event.Position{BinlogFile: "mysql-bin.000001", BinlogPos: 100})

	// Transition to snapshotting → catching_up → streaming so we can pause
	lc, _ := store.Get(context.Background(), "task-1", tableID)
	lc.TransitionTo(source.TableStateSnapshotting, nil)
	lc.TransitionTo(source.TableStateCatchingUp, nil)
	lc.TransitionTo(source.TableStateStreaming, nil)
	store.Save(context.Background(), "task-1", lc)

	srv := NewServer(DefaultServerConfig())
	srv.SetScheduler(scheduler)

	req := httptest.NewRequest("POST", "/api/v1/tasks/task-1/tables/db1.users/pause-lifecycle", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp apiResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	dataBytes, _ := json.Marshal(resp.Data)
	var data map[string]string
	json.Unmarshal(dataBytes, &data)
	assert.Equal(t, "paused", data["status"])
}

func TestResumeTableLifecycleEndpoint(t *testing.T) {
	// Set up a table in paused state
	store := source.NewMemoryLifecycleStore()
	cacheBackend := &mockCacheBackend{}
	config := lifecycle.DefaultSchedulerConfig()
	scheduler := lifecycle.NewSnapshotScheduler(config, "task-1", store, cacheBackend)

	tableID := source.TableID{Database: "db1", Table: "users"}
	_ = scheduler.AddTable(tableID, &event.Position{BinlogFile: "mysql-bin.000001", BinlogPos: 100})

	// Transition to streaming, then pause
	lc, _ := store.Get(context.Background(), "task-1", tableID)
	lc.TransitionTo(source.TableStateSnapshotting, nil)
	lc.TransitionTo(source.TableStateCatchingUp, nil)
	lc.TransitionTo(source.TableStateStreaming, nil)
	store.Save(context.Background(), "task-1", lc)
	_ = scheduler.PauseTable(tableID)

	srv := NewServer(DefaultServerConfig())
	srv.SetScheduler(scheduler)

	req := httptest.NewRequest("POST", "/api/v1/tasks/task-1/tables/db1.users/resume-lifecycle", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp apiResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	dataBytes, _ := json.Marshal(resp.Data)
	var data map[string]string
	json.Unmarshal(dataBytes, &data)
	assert.Equal(t, "resumed", data["status"])
}

func TestRetryTableLifecycleEndpoint(t *testing.T) {
	srv := setupLifecycleServer(t)

	// db1.orders is in error state (set up in setupLifecycleServer)
	req := httptest.NewRequest("POST", "/api/v1/tasks/task-1/tables/db1.orders/retry", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp apiResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	dataBytes, _ := json.Marshal(resp.Data)
	var data map[string]string
	json.Unmarshal(dataBytes, &data)
	assert.Equal(t, "retrying", data["status"])

	// Verify the table is now in pending state
	store := srv.scheduler.Store()
	lc, _ := store.Get(context.Background(), "task-1", source.TableID{Database: "db1", Table: "orders"})
	assert.Equal(t, source.TableStatePending, lc.GetState())
}

func TestRetryTableNotInErrorState(t *testing.T) {
	srv := setupLifecycleServer(t)

	// db1.users is in pending state, not error
	req := httptest.NewRequest("POST", "/api/v1/tasks/task-1/tables/db1.users/retry", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestSkipTableErrorEndpoint(t *testing.T) {
	srv := setupLifecycleServer(t)

	// db1.orders is in error state
	req := httptest.NewRequest("POST", "/api/v1/tasks/task-1/tables/db1.orders/skip-error", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp apiResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	dataBytes, _ := json.Marshal(resp.Data)
	var data map[string]interface{}
	json.Unmarshal(dataBytes, &data)
	assert.Equal(t, "skipped", data["status"])
	assert.NotEmpty(t, data["newState"])
}

func TestSkipTableErrorNotInErrorState(t *testing.T) {
	srv := setupLifecycleServer(t)

	// db1.users is in pending state
	req := httptest.NewRequest("POST", "/api/v1/tasks/task-1/tables/db1.users/skip-error", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestLifecycleEndpointNoScheduler(t *testing.T) {
	srv := NewServer(DefaultServerConfig())
	// Do NOT set scheduler

	endpoints := []struct {
		method string
		path   string
	}{
		{"GET", "/api/v1/tasks/task-1/detail"},
		{"GET", "/api/v1/tasks/task-1/tables/errors"},
		{"POST", "/api/v1/tasks/task-1/tables/restart"},
		{"GET", "/api/v1/tasks/task-1/tables/db1.users/lifecycle"},
		{"POST", "/api/v1/tasks/task-1/tables/db1.users/pause-lifecycle"},
		{"POST", "/api/v1/tasks/task-1/tables/db1.users/resume-lifecycle"},
		{"POST", "/api/v1/tasks/task-1/tables/db1.users/retry"},
		{"POST", "/api/v1/tasks/task-1/tables/db1.users/skip-error"},
	}

	for _, ep := range endpoints {
		t.Run(ep.method+" "+ep.path, func(t *testing.T) {
			var req *http.Request
			if ep.method == "POST" {
				req = httptest.NewRequest(ep.method, ep.path, strings.NewReader(`{}`))
			} else {
				req = httptest.NewRequest(ep.method, ep.path, nil)
			}
			w := httptest.NewRecorder()
			srv.router.ServeHTTP(w, req)

			assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		})
	}
}

func TestRestartTablesSchemaEndpoint(t *testing.T) {
	srv := setupLifecycleServer(t)

	// Restart by schema "db1" (force=true to restart even pending tables)
	body := `{"tables":[],"schemas":["db1"],"force":true}`
	req := httptest.NewRequest("POST", "/api/v1/tasks/task-1/tables/restart", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp apiResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)

	dataBytes, _ := json.Marshal(resp.Data)
	var data map[string]interface{}
	json.Unmarshal(dataBytes, &data)

	restartedSchemas, ok := data["restartedSchemas"].([]interface{})
	require.True(t, ok)
	assert.Contains(t, restartedSchemas, "db1")
}
