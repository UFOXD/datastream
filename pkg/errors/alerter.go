package errors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// AlertLevel defines the severity of an alert notification.
type AlertLevel string

const (
	AlertLevelWarning  AlertLevel = "warning"
	AlertLevelCritical AlertLevel = "critical"
	AlertLevelFatal    AlertLevel = "fatal"
)

// Alert represents an alert notification to be sent via an Alerter.
type Alert struct {
	Level     AlertLevel `json:"level"`
	Title     string     `json:"title"`
	Message   string     `json:"message"`
	TaskID    string     `json:"taskId,omitempty"`
	Error     error      `json:"-"`
	Timestamp time.Time  `json:"timestamp"`
}

// Alerter is the interface for sending alert notifications.
type Alerter interface {
	Alert(ctx context.Context, alert *Alert) error
}

// --- LogAlerter ---

// LogAlerter writes alert notifications to an io.Writer in a human-readable
// format: [LEVEL] [timestamp] title: message (task: taskId)
type LogAlerter struct {
	w io.Writer
}

// NewLogAlerter creates a LogAlerter that writes to w.
func NewLogAlerter(w io.Writer) *LogAlerter {
	return &LogAlerter{w: w}
}

// Alert writes the alert to the underlying writer.
func (a *LogAlerter) Alert(_ context.Context, alert *Alert) error {
	ts := alert.Timestamp
	if ts.IsZero() {
		ts = time.Now() // local copy, does not mutate caller's Alert
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("[%s] [%s] %s: %s",
		strings.ToUpper(string(alert.Level)),
		ts.Format(time.RFC3339),
		alert.Title,
		alert.Message,
	))
	if alert.TaskID != "" {
		sb.WriteString(fmt.Sprintf(" (task: %s)", alert.TaskID))
	}
	sb.WriteString("\n")

	_, err := io.WriteString(a.w, sb.String())
	return err
}

// --- WebhookAlerter ---

// WebhookAlerter sends alert notifications as JSON via HTTP POST to a
// configured URL.
type WebhookAlerter struct {
	url    string
	client *http.Client
}

// NewWebhookAlerter creates a WebhookAlerter that POSTs to the given URL.
func NewWebhookAlerter(url string) *WebhookAlerter {
	return &WebhookAlerter{
		url:    url,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// Alert sends the alert as a JSON payload via HTTP POST.
func (a *WebhookAlerter) Alert(ctx context.Context, alert *Alert) error {
	toSend := *alert
	if toSend.Timestamp.IsZero() {
		toSend.Timestamp = time.Now()
	}

	body, err := json.Marshal(&toSend)
	if err != nil {
		return fmt.Errorf("marshal alert: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("send webhook: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return nil
}

// --- MultiAlerter ---

// MultiAlerter fans out an alert to multiple Alerter implementations.
// It calls each alerter in order and returns the first error encountered.
type MultiAlerter struct {
	alerters []Alerter
}

// NewMultiAlerter creates a MultiAlerter that dispatches to all given alerters.
func NewMultiAlerter(alerters ...Alerter) *MultiAlerter {
	return &MultiAlerter{alerters: alerters}
}

// Alert sends the alert to all underlying alerters (fire-and-forget).
// Errors from individual alerters are logged but not returned.
func (a *MultiAlerter) Alert(ctx context.Context, alert *Alert) error {
	for _, alerter := range a.alerters {
		if err := alerter.Alert(ctx, alert); err != nil {
			fmt.Fprintf(os.Stderr, "alerter failed: %v\n", err)
		}
	}
	return nil
}
