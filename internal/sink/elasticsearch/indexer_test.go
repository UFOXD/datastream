package elasticsearch

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- constructor ----

func TestNewBulkIndexer(t *testing.T) {
	cfg := DefaultConfig()
	bi := NewBulkIndexer(cfg)
	require.NotNil(t, bi)
	assert.Equal(t, 0, bi.BufferSize())
}

// ---- Add / BufferSize ----

func TestBulkIndexer_Add(t *testing.T) {
	cfg := DefaultConfig()
	bi := NewBulkIndexer(cfg)

	action := &BulkAction{Index: "test", ID: "1", Op: "index", Doc: map[string]interface{}{"k": "v"}}
	err := bi.Add(action)
	require.NoError(t, err)
	assert.Equal(t, 1, bi.BufferSize())
}

func TestBulkIndexer_Add_Multiple(t *testing.T) {
	cfg := DefaultConfig()
	bi := NewBulkIndexer(cfg)

	for i := 0; i < 5; i++ {
		err := bi.Add(&BulkAction{Index: "test", ID: "1", Op: "index"})
		require.NoError(t, err)
	}
	assert.Equal(t, 5, bi.BufferSize())
}

func TestBulkIndexer_Add_Concurrent(t *testing.T) {
	cfg := DefaultConfig()
	bi := NewBulkIndexer(cfg)

	var wg sync.WaitGroup
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = bi.Add(&BulkAction{Index: "test", ID: "1", Op: "index"})
		}()
	}
	wg.Wait()
	assert.Equal(t, n, bi.BufferSize())
}

// ---- Clear ----

func TestBulkIndexer_Clear(t *testing.T) {
	cfg := DefaultConfig()
	bi := NewBulkIndexer(cfg)

	_ = bi.Add(&BulkAction{Index: "test", ID: "1", Op: "index"})
	assert.Equal(t, 1, bi.BufferSize())

	bi.Clear()
	assert.Equal(t, 0, bi.BufferSize())
}

// ---- GetAndClear ----

func TestBulkIndexer_GetAndClear(t *testing.T) {
	cfg := DefaultConfig()
	bi := NewBulkIndexer(cfg)

	a1 := &BulkAction{Index: "idx", ID: "1", Op: "index"}
	a2 := &BulkAction{Index: "idx", ID: "2", Op: "delete"}
	_ = bi.Add(a1)
	_ = bi.Add(a2)

	got := bi.GetAndClear()
	require.Len(t, got, 2)
	assert.Equal(t, a1, got[0])
	assert.Equal(t, a2, got[1])

	// buffer must be empty after GetAndClear
	assert.Equal(t, 0, bi.BufferSize())
}

func TestBulkIndexer_GetAndClear_Empty(t *testing.T) {
	cfg := DefaultConfig()
	bi := NewBulkIndexer(cfg)

	got := bi.GetAndClear()
	assert.Empty(t, got)
	assert.Equal(t, 0, bi.BufferSize())
}

// ---- ShouldFlush ----

func TestBulkIndexer_ShouldFlush_BelowThreshold(t *testing.T) {
	cfg := &Config{BatchSize: 5, FlushInterval: time.Second, RefreshPolicy: "false"}
	bi := NewBulkIndexer(cfg)

	for i := 0; i < 4; i++ {
		_ = bi.Add(&BulkAction{Op: "index"})
	}
	assert.False(t, bi.ShouldFlush())
}

func TestBulkIndexer_ShouldFlush_AtThreshold(t *testing.T) {
	cfg := &Config{BatchSize: 5, FlushInterval: time.Second, RefreshPolicy: "false"}
	bi := NewBulkIndexer(cfg)

	for i := 0; i < 5; i++ {
		_ = bi.Add(&BulkAction{Op: "index"})
	}
	assert.True(t, bi.ShouldFlush())
}

func TestBulkIndexer_ShouldFlush_AboveThreshold(t *testing.T) {
	cfg := &Config{BatchSize: 3, FlushInterval: time.Second, RefreshPolicy: "false"}
	bi := NewBulkIndexer(cfg)

	for i := 0; i < 10; i++ {
		_ = bi.Add(&BulkAction{Op: "index"})
	}
	assert.True(t, bi.ShouldFlush())
}

func TestBulkIndexer_ShouldFlush_EmptyBuffer(t *testing.T) {
	cfg := DefaultConfig()
	bi := NewBulkIndexer(cfg)
	assert.False(t, bi.ShouldFlush())
}

// ---- BuildRequestBody ND-JSON format ----

func TestBulkIndexer_BuildRequestBody_Index(t *testing.T) {
	cfg := DefaultConfig()
	bi := NewBulkIndexer(cfg)

	actions := []*BulkAction{
		{
			Index: "mydb_users",
			ID:    "42",
			Op:    "index",
			Doc:   map[string]interface{}{"name": "Alice", "age": 30},
		},
	}

	body := bi.BuildRequestBody(actions)
	require.NotEmpty(t, body)

	lines := splitNDJSON(body)
	require.Len(t, lines, 2, "index op must produce exactly 2 lines")

	// Line 1: action header
	var header map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &header))
	indexMeta, ok := header["index"].(map[string]interface{})
	require.True(t, ok, "header must have 'index' key")
	assert.Equal(t, "mydb_users", indexMeta["_index"])
	assert.Equal(t, "42", indexMeta["_id"])

	// Line 2: document body
	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &doc))
	assert.Equal(t, "Alice", doc["name"])
	assert.Equal(t, float64(30), doc["age"])
}

func TestBulkIndexer_BuildRequestBody_Update(t *testing.T) {
	cfg := &Config{RetryOnConflict: 3, RefreshPolicy: "false"}
	bi := NewBulkIndexer(cfg)

	actions := []*BulkAction{
		{
			Index: "mydb_orders",
			ID:    "7",
			Op:    "update",
			Doc:   map[string]interface{}{"status": "shipped"},
		},
	}

	body := bi.BuildRequestBody(actions)
	lines := splitNDJSON(body)
	require.Len(t, lines, 2, "update op must produce exactly 2 lines")

	// Line 1: action header
	var header map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &header))
	updateMeta, ok := header["update"].(map[string]interface{})
	require.True(t, ok, "header must have 'update' key")
	assert.Equal(t, "mydb_orders", updateMeta["_index"])
	assert.Equal(t, "7", updateMeta["_id"])
	assert.Equal(t, float64(3), updateMeta["retry_on_conflict"])

	// Line 2: update body
	var updateBody map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &updateBody))
	innerDoc, ok := updateBody["doc"].(map[string]interface{})
	require.True(t, ok, "update body must have 'doc' key")
	assert.Equal(t, "shipped", innerDoc["status"])
	assert.Equal(t, true, updateBody["doc_as_upsert"])
}

func TestBulkIndexer_BuildRequestBody_Delete(t *testing.T) {
	cfg := DefaultConfig()
	bi := NewBulkIndexer(cfg)

	actions := []*BulkAction{
		{
			Index: "mydb_logs",
			ID:    "99",
			Op:    "delete",
			Doc:   nil,
		},
	}

	body := bi.BuildRequestBody(actions)
	lines := splitNDJSON(body)
	require.Len(t, lines, 1, "delete op must produce exactly 1 line")

	// Only action header, no body
	var header map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &header))
	deleteMeta, ok := header["delete"].(map[string]interface{})
	require.True(t, ok, "header must have 'delete' key")
	assert.Equal(t, "mydb_logs", deleteMeta["_index"])
	assert.Equal(t, "99", deleteMeta["_id"])
}

func TestBulkIndexer_BuildRequestBody_Mixed(t *testing.T) {
	cfg := &Config{RetryOnConflict: 2, RefreshPolicy: "false"}
	bi := NewBulkIndexer(cfg)

	actions := []*BulkAction{
		{Index: "idx", ID: "1", Op: "index", Doc: map[string]interface{}{"x": 1}},
		{Index: "idx", ID: "2", Op: "update", Doc: map[string]interface{}{"x": 2}},
		{Index: "idx", ID: "3", Op: "delete"},
	}

	body := bi.BuildRequestBody(actions)
	lines := splitNDJSON(body)
	// index=2, update=2, delete=1 → 5 lines total
	assert.Len(t, lines, 5)
}

func TestBulkIndexer_BuildRequestBody_Empty(t *testing.T) {
	cfg := DefaultConfig()
	bi := NewBulkIndexer(cfg)

	body := bi.BuildRequestBody([]*BulkAction{})
	assert.Empty(t, body)
}

func TestBulkIndexer_BuildRequestBody_EndsWithNewline(t *testing.T) {
	cfg := DefaultConfig()
	bi := NewBulkIndexer(cfg)

	actions := []*BulkAction{
		{Index: "test", ID: "1", Op: "index", Doc: map[string]interface{}{"k": "v"}},
	}
	body := bi.BuildRequestBody(actions)
	assert.True(t, strings.HasSuffix(string(body), "\n"),
		"ND-JSON body must end with a newline")
}

func TestBulkIndexer_BuildRequestBody_UpdateRetryOnConflict_Zero(t *testing.T) {
	// RetryOnConflict=0 should still appear in header (ES default behavior)
	cfg := &Config{RetryOnConflict: 0, RefreshPolicy: "false"}
	bi := NewBulkIndexer(cfg)

	actions := []*BulkAction{
		{Index: "idx", ID: "1", Op: "update", Doc: map[string]interface{}{"f": "v"}},
	}
	body := bi.BuildRequestBody(actions)
	lines := splitNDJSON(body)
	require.Len(t, lines, 2)

	var header map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &header))
	updateMeta := header["update"].(map[string]interface{})
	assert.Equal(t, float64(0), updateMeta["retry_on_conflict"])
}

// ---- helper ----

// splitNDJSON splits a ND-JSON byte slice into non-empty lines.
func splitNDJSON(body []byte) []string {
	raw := strings.Split(string(body), "\n")
	var lines []string
	for _, l := range raw {
		if strings.TrimSpace(l) != "" {
			lines = append(lines, l)
		}
	}
	return lines
}
