package errors

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLogAlerterSendsAlert(t *testing.T) {
	var buf bytes.Buffer
	alerter := NewLogAlerter(&buf)
	err := alerter.Alert(context.Background(), &Alert{
		Level:   AlertLevelCritical,
		Title:   "Connection Lost",
		Message: "MySQL source connection failed",
		TaskID:  "task-1",
	})
	if err != nil {
		t.Fatalf("Alert() returned error: %v", err)
	}
	output := buf.String()
	if !contains(output, "Connection Lost") {
		t.Errorf("output should contain title, got: %s", output)
	}
	if !contains(output, "CRITICAL") {
		t.Errorf("output should contain level, got: %s", output)
	}
}

func TestWebhookAlerterSendsHTTP(t *testing.T) {
	var received []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	alerter := NewWebhookAlerter(server.URL)
	err := alerter.Alert(context.Background(), &Alert{
		Level:   AlertLevelWarning,
		Title:   "High Latency",
		Message: "Pipeline latency > 5s",
		TaskID:  "task-2",
	})
	if err != nil {
		t.Fatalf("Alert() returned error: %v", err)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(received, &payload); err != nil {
		t.Fatalf("failed to parse webhook payload: %v", err)
	}
	if payload["title"] != "High Latency" {
		t.Errorf("title = %v, want 'High Latency'", payload["title"])
	}
}

func TestWebhookAlerterHandlesServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	alerter := NewWebhookAlerter(server.URL)
	err := alerter.Alert(context.Background(), &Alert{
		Level: AlertLevelFatal,
		Title: "Test",
	})
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestMultiAlerter(t *testing.T) {
	var buf1, buf2 bytes.Buffer
	multi := NewMultiAlerter(NewLogAlerter(&buf1), NewLogAlerter(&buf2))
	err := multi.Alert(context.Background(), &Alert{
		Level: AlertLevelWarning,
		Title: "Test Alert",
	})
	if err != nil {
		t.Fatalf("Alert() returned error: %v", err)
	}
	if !contains(buf1.String(), "Test Alert") {
		t.Error("first alerter should receive alert")
	}
	if !contains(buf2.String(), "Test Alert") {
		t.Error("second alerter should receive alert")
	}
}

func contains(s, substr string) bool {
	return bytes.Contains([]byte(s), []byte(substr))
}
