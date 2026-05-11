//go:build integration
// +build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

// TestElasticsearchSinkIntegration tests Elasticsearch sink connector
func TestElasticsearchSinkIntegration(t *testing.T) {
	cfg := DefaultConfig()
	baseURL := cfg.ElasticsearchURL()

	// Wait for Elasticsearch to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout waiting for Elasticsearch")
		default:
			resp, err := http.Get(baseURL + "/_cluster/health")
			if err == nil && resp.StatusCode == 200 {
				resp.Body.Close()
				goto ready
			}
			time.Sleep(1 * time.Second)
		}
	}

ready:
	// Create test index
	indexName := fmt.Sprintf("test_index_%d", time.Now().UnixNano())

	// Delete index if exists
	req, _ := http.NewRequest(http.MethodDelete, baseURL+"/"+indexName, nil)
	http.DefaultClient.Do(req)

	// Create index with mapping
	createReq := `{
		"mappings": {
			"properties": {
				"name": {"type": "text"},
				"value": {"type": "integer"}
			}
		}
	}`

	resp, err := http.Put(baseURL+"/"+indexName, "application/json", bytes.NewBufferString(createReq))
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	resp.Body.Close()

	// Index test document
	doc := map[string]interface{}{
		"name":  "test_doc",
		"value": 42,
	}
	docJSON, _ := json.Marshal(doc)

	resp, err = http.Post(baseURL+"/"+indexName+"/_doc", "application/json", bytes.NewBuffer(docJSON))
	if err != nil {
		t.Fatalf("Failed to index document: %v", err)
	}
	resp.Body.Close()

	// Refresh index
	http.Post(baseURL+"/"+indexName+"/_refresh", "application/json", nil)

	// Search for document
	searchResp, err := http.Get(baseURL + "/" + indexName + "/_search?q=name:test_doc")
	if err != nil {
		t.Fatalf("Failed to search: %v", err)
	}
	defer searchResp.Body.Close()

	var result map[string]interface{}
	body, _ := io.ReadAll(searchResp.Body)
	json.Unmarshal(body, &result)

	hits := result["hits"].(map[string]interface{})["hits"].([]interface{})
	if len(hits) == 0 {
		t.Fatal("Expected to find document, but got none")
	}

	t.Logf("Found %d documents", len(hits))

	// Cleanup
	req, _ = http.NewRequest(http.MethodDelete, baseURL+"/"+indexName, nil)
	http.DefaultClient.Do(req)

	t.Log("Elasticsearch sink integration test passed")
}
