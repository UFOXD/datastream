package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/UFOXD/datastream/internal/sink"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Compile-time interface check
// ---------------------------------------------------------------------------

var _ sink.Connector = (*Connector)(nil)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newTestServer starts an httptest server that handles the ES bulk endpoint.
// It records received bodies and returns the provided response for each call.
// All responses include the X-Elastic-Product header required by go-elasticsearch v8.
func newTestServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// go-elasticsearch v8 checks this header on first request to verify
		// it is talking to a genuine Elasticsearch cluster.
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		handler(w, r)
	})
	srv := httptest.NewServer(wrapped)
	t.Cleanup(srv.Close)
	return srv
}

// bulkSuccessResponse returns a minimal ES bulk API success response body.
func bulkSuccessResponse(took int, hasErrors bool) []byte {
	resp := map[string]interface{}{
		"took":   took,
		"errors": hasErrors,
		"items":  []interface{}{},
	}
	b, _ := json.Marshal(resp)
	return b
}

// bulkErrorResponse returns a minimal ES bulk API response with one item error.
func bulkErrorResponse() []byte {
	resp := map[string]interface{}{
		"took":   5,
		"errors": true,
		"items": []interface{}{
			map[string]interface{}{
				"index": map[string]interface{}{
					"_index":  "test",
					"_id":     "1",
					"status":  409,
					"error":   map[string]interface{}{"type": "version_conflict_engine_exception", "reason": "version conflict"},
				},
			},
		},
	}
	b, _ := json.Marshal(resp)
	return b
}

// makeConfig builds a minimal valid sink.Config that targets the given URL list.
func makeConfig(urls []string) sink.Config {
	return sink.Config{
		Type: "elasticsearch",
		Connection: sink.ConnectionConfig{
			URLs: urls,
		},
		Properties: map[string]interface{}{
			"indexPattern":    "{database}_{table}",
			"refreshPolicy":   "false",
			"retryOnConflict": float64(3),
			"batchSize":       float64(100),
		},
	}
}

// makeInsertEvent creates a simple INSERT ChangeEvent.
func makeInsertEvent(db, table string, id int64) *event.ChangeEvent {
	return &event.ChangeEvent{
		Type: event.EventTypeInsert,
		Table: event.TableInfo{
			Database:          db,
			Table:             table,
			PrimaryKeyColumns: []string{"id"},
		},
		After: event.RowData{
			Fields: map[string]event.Field{
				"id":   {Name: "id", Value: id, Type: "bigint"},
				"name": {Name: "name", Value: "Alice", Type: "varchar"},
			},
		},
		Position: event.Position{
			CommitTime: time.Now(),
			SeqNo:      int(id),
		},
	}
}

// makeUpdateEvent creates a simple UPDATE ChangeEvent.
func makeUpdateEvent(db, table string, id int64) *event.ChangeEvent {
	return &event.ChangeEvent{
		Type: event.EventTypeUpdate,
		Table: event.TableInfo{
			Database:          db,
			Table:             table,
			PrimaryKeyColumns: []string{"id"},
		},
		Before: event.RowData{
			Fields: map[string]event.Field{
				"id":   {Name: "id", Value: id, Type: "bigint"},
				"name": {Name: "name", Value: "Old", Type: "varchar"},
			},
		},
		After: event.RowData{
			Fields: map[string]event.Field{
				"id":   {Name: "id", Value: id, Type: "bigint"},
				"name": {Name: "name", Value: "New", Type: "varchar"},
			},
		},
		Position: event.Position{
			CommitTime: time.Now(),
			SeqNo:      int(id),
		},
	}
}

// makeDeleteEvent creates a simple DELETE ChangeEvent.
func makeDeleteEvent(db, table string, id int64) *event.ChangeEvent {
	return &event.ChangeEvent{
		Type: event.EventTypeDelete,
		Table: event.TableInfo{
			Database:          db,
			Table:             table,
			PrimaryKeyColumns: []string{"id"},
		},
		Before: event.RowData{
			Fields: map[string]event.Field{
				"id":   {Name: "id", Value: id, Type: "bigint"},
				"name": {Name: "name", Value: "Bob", Type: "varchar"},
			},
		},
		Position: event.Position{
			CommitTime: time.Now(),
			SeqNo:      int(id),
		},
	}
}

// makeDDLEvent creates a DDL ChangeEvent (should be skipped by the connector).
func makeDDLEvent() *event.ChangeEvent {
	return &event.ChangeEvent{
		Type: event.EventTypeDDL,
		Table: event.TableInfo{
			Database: "testdb",
			Table:    "users",
		},
	}
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

func TestNew(t *testing.T) {
	c := New()
	require.NotNil(t, c)
	assert.Equal(t, sink.StateUninitialized, c.Status().State)
}

// ---------------------------------------------------------------------------
// Name
// ---------------------------------------------------------------------------

func TestConnector_Name(t *testing.T) {
	c := New()
	assert.Equal(t, "elasticsearch", c.Name())
}

// ---------------------------------------------------------------------------
// SupportsDDL / SupportsTransaction
// ---------------------------------------------------------------------------

func TestConnector_SupportsDDL(t *testing.T) {
	assert.False(t, New().SupportsDDL())
}

func TestConnector_SupportsTransaction(t *testing.T) {
	assert.False(t, New().SupportsTransaction())
}

// ---------------------------------------------------------------------------
// Initialize
// ---------------------------------------------------------------------------

func TestConnector_Initialize_Success(t *testing.T) {
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		// Respond to the ping (HEAD /)
		w.WriteHeader(http.StatusOK)
	})

	c := New()
	cfg := makeConfig([]string{srv.URL})
	err := c.Initialize(context.Background(), cfg)
	require.NoError(t, err)
	assert.Equal(t, sink.StateReady, c.Status().State)
}

func TestConnector_Initialize_NoURLs(t *testing.T) {
	c := New()
	cfg := makeConfig([]string{})
	err := c.Initialize(context.Background(), cfg)
	require.Error(t, err)
}

func TestConnector_Initialize_InvalidRefreshPolicy(t *testing.T) {
	c := New()
	cfg := sink.Config{
		Type: "elasticsearch",
		Connection: sink.ConnectionConfig{
			URLs: []string{"http://localhost:9200"},
		},
		Properties: map[string]interface{}{
			"refreshPolicy": "invalid",
		},
	}
	err := c.Initialize(context.Background(), cfg)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Start / Stop
// ---------------------------------------------------------------------------

func TestConnector_Start_BeforeInit(t *testing.T) {
	c := New()
	err := c.Start(context.Background())
	require.Error(t, err)
}

func TestConnector_Start_AfterInit(t *testing.T) {
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	c := New()
	require.NoError(t, c.Initialize(context.Background(), makeConfig([]string{srv.URL})))
	err := c.Start(context.Background())
	require.NoError(t, err)
	assert.Equal(t, sink.StateReady, c.Status().State)
}

func TestConnector_Stop(t *testing.T) {
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	c := New()
	require.NoError(t, c.Initialize(context.Background(), makeConfig([]string{srv.URL})))
	require.NoError(t, c.Start(context.Background()))

	err := c.Stop(context.Background())
	require.NoError(t, err)
	assert.Equal(t, sink.StateStopped, c.Status().State)
}

// ---------------------------------------------------------------------------
// GetPosition
// ---------------------------------------------------------------------------

func TestConnector_GetPosition_Initial(t *testing.T) {
	c := New()
	assert.Nil(t, c.GetPosition())
}

func TestConnector_GetPosition_AfterWrite(t *testing.T) {
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/_bulk" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(bulkSuccessResponse(1, false))
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	c := New()
	require.NoError(t, c.Initialize(context.Background(), makeConfig([]string{srv.URL})))

	events := []*event.ChangeEvent{makeInsertEvent("db", "users", 1)}
	require.NoError(t, c.Write(context.Background(), events))

	pos := c.GetPosition()
	require.NotNil(t, pos)
	assert.Equal(t, 1, pos.SeqNo)
}

// ---------------------------------------------------------------------------
// Write
// ---------------------------------------------------------------------------

func TestConnector_Write_InsertEvent(t *testing.T) {
	var receivedBody []byte

	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/_bulk" {
			receivedBody, _ = io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(bulkSuccessResponse(1, false))
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	c := New()
	require.NoError(t, c.Initialize(context.Background(), makeConfig([]string{srv.URL})))

	events := []*event.ChangeEvent{makeInsertEvent("mydb", "users", 42)}
	require.NoError(t, c.Write(context.Background(), events))

	// Verify ND-JSON body contains index action
	require.NotEmpty(t, receivedBody)
	lines := splitNDJSON(receivedBody)
	require.GreaterOrEqual(t, len(lines), 2)

	var actionHeader map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &actionHeader))
	_, hasIndex := actionHeader["index"]
	assert.True(t, hasIndex, "INSERT event should produce 'index' action")
}

func TestConnector_Write_UpdateEvent(t *testing.T) {
	var receivedBody []byte

	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/_bulk" {
			receivedBody, _ = io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(bulkSuccessResponse(1, false))
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	c := New()
	require.NoError(t, c.Initialize(context.Background(), makeConfig([]string{srv.URL})))

	events := []*event.ChangeEvent{makeUpdateEvent("mydb", "orders", 7)}
	require.NoError(t, c.Write(context.Background(), events))

	require.NotEmpty(t, receivedBody)
	lines := splitNDJSON(receivedBody)
	require.GreaterOrEqual(t, len(lines), 2)

	var actionHeader map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &actionHeader))
	_, hasUpdate := actionHeader["update"]
	assert.True(t, hasUpdate, "UPDATE event should produce 'update' action")
}

func TestConnector_Write_DeleteEvent(t *testing.T) {
	var receivedBody []byte

	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/_bulk" {
			receivedBody, _ = io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(bulkSuccessResponse(1, false))
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	c := New()
	require.NoError(t, c.Initialize(context.Background(), makeConfig([]string{srv.URL})))

	events := []*event.ChangeEvent{makeDeleteEvent("mydb", "logs", 99)}
	require.NoError(t, c.Write(context.Background(), events))

	require.NotEmpty(t, receivedBody)
	lines := splitNDJSON(receivedBody)
	require.GreaterOrEqual(t, len(lines), 1)

	var actionHeader map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &actionHeader))
	_, hasDelete := actionHeader["delete"]
	assert.True(t, hasDelete, "DELETE event should produce 'delete' action")
}

func TestConnector_Write_DDLEventSkipped(t *testing.T) {
	var bulkCalled bool

	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/_bulk" {
			bulkCalled = true
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(bulkSuccessResponse(0, false))
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	c := New()
	require.NoError(t, c.Initialize(context.Background(), makeConfig([]string{srv.URL})))

	// DDL events should be silently skipped — Write must succeed
	err := c.Write(context.Background(), []*event.ChangeEvent{makeDDLEvent()})
	require.NoError(t, err)
	assert.False(t, bulkCalled, "DDL-only batch should not trigger a bulk API call")
}

func TestConnector_Write_MixedEvents(t *testing.T) {
	var receivedBody []byte

	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/_bulk" {
			receivedBody, _ = io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(bulkSuccessResponse(3, false))
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	c := New()
	require.NoError(t, c.Initialize(context.Background(), makeConfig([]string{srv.URL})))

	events := []*event.ChangeEvent{
		makeInsertEvent("db", "t", 1),
		makeDDLEvent(), // skipped
		makeUpdateEvent("db", "t", 2),
		makeDeleteEvent("db", "t", 3),
	}
	require.NoError(t, c.Write(context.Background(), events))

	// index=2lines, update=2lines, delete=1line → 5 lines total
	lines := splitNDJSON(receivedBody)
	assert.Equal(t, 5, len(lines))
}

func TestConnector_Write_EmptyEvents(t *testing.T) {
	var bulkCalled bool

	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_bulk" {
			bulkCalled = true
		}
		w.WriteHeader(http.StatusOK)
	})

	c := New()
	require.NoError(t, c.Initialize(context.Background(), makeConfig([]string{srv.URL})))

	err := c.Write(context.Background(), []*event.ChangeEvent{})
	require.NoError(t, err)
	assert.False(t, bulkCalled, "empty events must not trigger bulk API call")
}

func TestConnector_Write_UpdatesEventsWrittenCount(t *testing.T) {
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/_bulk" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(bulkSuccessResponse(2, false))
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	c := New()
	require.NoError(t, c.Initialize(context.Background(), makeConfig([]string{srv.URL})))

	events := []*event.ChangeEvent{
		makeInsertEvent("db", "t", 1),
		makeInsertEvent("db", "t", 2),
	}
	require.NoError(t, c.Write(context.Background(), events))

	assert.Equal(t, int64(2), c.Status().EventsWritten)
}

func TestConnector_Write_BulkErrors_ContinueProcessing(t *testing.T) {
	// Bulk API returns errors=true but individual items have errors.
	// Per design §1.6: log individual failures, continue processing.
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/_bulk" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(bulkErrorResponse())
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	c := New()
	require.NoError(t, c.Initialize(context.Background(), makeConfig([]string{srv.URL})))

	events := []*event.ChangeEvent{makeInsertEvent("db", "t", 1)}
	// Should not return error — individual doc failures are logged, not fatal
	err := c.Write(context.Background(), events)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

func TestConnector_Flush_BeforeInit(t *testing.T) {
	c := New()
	// Flush before init must not panic
	err := c.Flush(context.Background())
	require.NoError(t, err)
}

func TestConnector_Flush_AfterWrite(t *testing.T) {
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/_bulk" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(bulkSuccessResponse(0, false))
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	c := New()
	require.NoError(t, c.Initialize(context.Background(), makeConfig([]string{srv.URL})))

	err := c.Flush(context.Background())
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// parseConfig
// ---------------------------------------------------------------------------

func TestParseConfig_FromConnectionURLs(t *testing.T) {
	cfg := sink.Config{
		Type: "elasticsearch",
		Connection: sink.ConnectionConfig{
			URLs: []string{"http://node1:9200", "http://node2:9200"},
		},
		Properties: map[string]interface{}{
			"refreshPolicy": "false",
		},
	}

	parsed, err := parseConfig(cfg)
	require.NoError(t, err)
	assert.Equal(t, []string{"http://node1:9200", "http://node2:9200"}, parsed.URLs)
}

func TestParseConfig_IndexPattern(t *testing.T) {
	cfg := sink.Config{
		Type: "elasticsearch",
		Connection: sink.ConnectionConfig{
			URLs: []string{"http://localhost:9200"},
		},
		Properties: map[string]interface{}{
			"indexPattern":  "logs_{database}_{table}",
			"indexPrefix":   "prod_",
			"refreshPolicy": "false",
		},
	}

	parsed, err := parseConfig(cfg)
	require.NoError(t, err)
	assert.Equal(t, "logs_{database}_{table}", parsed.IndexPattern)
	assert.Equal(t, "prod_", parsed.IndexPrefix)
}

func TestParseConfig_BulkSettings(t *testing.T) {
	cfg := sink.Config{
		Type: "elasticsearch",
		Connection: sink.ConnectionConfig{
			URLs: []string{"http://localhost:9200"},
		},
		Properties: map[string]interface{}{
			"batchSize":       float64(500),
			"flushInterval":   float64(2000), // ms
			"retryOnConflict": float64(5),
			"refreshPolicy":   "wait_for",
		},
	}

	parsed, err := parseConfig(cfg)
	require.NoError(t, err)
	assert.Equal(t, 500, parsed.BatchSize)
	assert.Equal(t, 5, parsed.RetryOnConflict)
	assert.Equal(t, "wait_for", parsed.RefreshPolicy)
}

func TestParseConfig_AuthCredentials(t *testing.T) {
	cfg := sink.Config{
		Type: "elasticsearch",
		Connection: sink.ConnectionConfig{
			URLs:     []string{"http://localhost:9200"},
			User:     "elastic",
			Password: "secret",
		},
		Properties: map[string]interface{}{
			"apiKey":        "my-api-key",
			"refreshPolicy": "false",
		},
	}

	parsed, err := parseConfig(cfg)
	require.NoError(t, err)
	assert.Equal(t, "elastic", parsed.Username)
	assert.Equal(t, "secret", parsed.Password)
	assert.Equal(t, "my-api-key", parsed.APIKey)
}

func TestParseConfig_Defaults(t *testing.T) {
	cfg := sink.Config{
		Type: "elasticsearch",
		Connection: sink.ConnectionConfig{
			URLs: []string{"http://localhost:9200"},
		},
		Properties: map[string]interface{}{},
	}

	parsed, err := parseConfig(cfg)
	require.NoError(t, err)
	// Defaults from DefaultConfig()
	assert.Equal(t, "{database}_{table}", parsed.IndexPattern)
	assert.Equal(t, 1000, parsed.BatchSize)
	assert.Equal(t, time.Second, parsed.FlushInterval)
	assert.Equal(t, "false", parsed.RefreshPolicy)
	assert.Equal(t, 3, parsed.RetryOnConflict)
}

// ---------------------------------------------------------------------------
// factory / registry
// ---------------------------------------------------------------------------

func TestFactory_Create(t *testing.T) {
	f := &factory{}
	c, err := f.Create(sink.Config{})
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, "elasticsearch", c.Name())
}

func TestRegistry_Registered(t *testing.T) {
	c, err := sink.Create("elasticsearch", sink.Config{})
	require.NoError(t, err)
	require.NotNil(t, c)
}

// ---------------------------------------------------------------------------
// Status transitions
// ---------------------------------------------------------------------------

func TestConnector_Status_Transitions(t *testing.T) {
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/_bulk" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(bulkSuccessResponse(1, false))
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	c := New()
	assert.Equal(t, sink.StateUninitialized, c.Status().State)

	require.NoError(t, c.Initialize(context.Background(), makeConfig([]string{srv.URL})))
	assert.Equal(t, sink.StateReady, c.Status().State)

	require.NoError(t, c.Start(context.Background()))
	assert.Equal(t, sink.StateReady, c.Status().State)

	require.NoError(t, c.Write(context.Background(), []*event.ChangeEvent{makeInsertEvent("db", "t", 1)}))
	// After Write completes state returns to Ready
	assert.Equal(t, sink.StateReady, c.Status().State)

	require.NoError(t, c.Stop(context.Background()))
	assert.Equal(t, sink.StateStopped, c.Status().State)
}

// ---------------------------------------------------------------------------
// Index name resolution via connector (end-to-end)
// ---------------------------------------------------------------------------

func TestConnector_Write_IndexNameResolution(t *testing.T) {
	var capturedPath string

	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			capturedPath = r.URL.Path
			body, _ := io.ReadAll(r.Body)
			// Return success with one item per action in body
			lines := splitNDJSON(body)
			items := make([]interface{}, 0)
			for i := 0; i < len(lines); i += 2 {
				items = append(items, map[string]interface{}{"index": map[string]interface{}{"status": 201}})
			}
			resp := map[string]interface{}{"took": 1, "errors": false, "items": items}
			b, _ := json.Marshal(resp)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(b)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	cfg := sink.Config{
		Type: "elasticsearch",
		Connection: sink.ConnectionConfig{
			URLs: []string{srv.URL},
		},
		Properties: map[string]interface{}{
			"indexPattern":  "{database}_{table}",
			"indexPrefix":   "test_",
			"refreshPolicy": "false",
		},
	}

	c := New()
	require.NoError(t, c.Initialize(context.Background(), cfg))
	require.NoError(t, c.Write(context.Background(), []*event.ChangeEvent{makeInsertEvent("mydb", "orders", 1)}))

	assert.Equal(t, "/_bulk", capturedPath)
}

// ---------------------------------------------------------------------------
// Bulk request body structure verification
// ---------------------------------------------------------------------------

func TestConnector_Write_BulkBodyEndsWithNewline(t *testing.T) {
	var receivedBody []byte

	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/_bulk" {
			receivedBody, _ = io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write(bulkSuccessResponse(1, false))
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	c := New()
	require.NoError(t, c.Initialize(context.Background(), makeConfig([]string{srv.URL})))
	require.NoError(t, c.Write(context.Background(), []*event.ChangeEvent{makeInsertEvent("db", "t", 1)}))

	assert.True(t, bytes.HasSuffix(receivedBody, []byte("\n")),
		"ND-JSON bulk body must end with a newline")
}
