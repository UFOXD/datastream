package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/UFOXD/datastream/internal/sink"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Connector implements sink.Connector for Elasticsearch.
type Connector struct {
	config   *Config
	status   sink.Status
	position *event.Position
	client   *elasticsearch.Client
	indexer  *BulkIndexer
	mapper   *DocumentMapper
	mu       sync.RWMutex
}

// New creates a new Elasticsearch sink connector.
func New() *Connector {
	return &Connector{
		status: sink.Status{
			State:     sink.StateUninitialized,
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
}

// Name returns the connector name.
func (c *Connector) Name() string {
	return "elasticsearch"
}

// Initialize initializes the connector with the given configuration.
func (c *Connector) Initialize(ctx context.Context, config sink.Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cfg, err := parseConfig(config)
	if err != nil {
		return err
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	// Build Elasticsearch client configuration.
	esCfg := elasticsearch.Config{
		Addresses: cfg.URLs,
	}
	if cfg.Username != "" {
		esCfg.Username = cfg.Username
		esCfg.Password = cfg.Password
	}
	if cfg.APIKey != "" {
		esCfg.APIKey = cfg.APIKey
	}

	client, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return fmt.Errorf("failed to create Elasticsearch client: %w", err)
	}

	// Ping to verify the connection with exponential backoff (max 5 retries).
	if err := c.pingWithRetry(ctx, client); err != nil {
		return fmt.Errorf("failed to connect to Elasticsearch: %w", err)
	}

	c.config = cfg
	c.client = client
	c.indexer = NewBulkIndexer(cfg)
	c.mapper = NewDocumentMapper(cfg)

	c.status.State = sink.StateReady
	c.status.Timestamp = time.Now().Format(time.RFC3339)

	log.Info("Elasticsearch sink initialized",
		zap.Strings("urls", cfg.URLs),
		zap.String("indexPattern", cfg.IndexPattern))
	return nil
}

// Start starts the connector.
func (c *Connector) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil {
		return sink.ErrNotInitialized
	}

	c.status.State = sink.StateReady
	c.status.Timestamp = time.Now().Format(time.RFC3339)
	log.Info("Elasticsearch sink started")
	return nil
}

// Stop stops the connector gracefully.
func (c *Connector) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.status.State = sink.StateStopped
	c.status.Timestamp = time.Now().Format(time.RFC3339)
	log.Info("Elasticsearch sink stopped")
	return nil
}

// Status returns the current status of the connector.
func (c *Connector) Status() sink.Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// Write writes events to Elasticsearch via the Bulk API.
//
// Per design §1.6:
//   - Connection errors: retry with exponential backoff (max 5 retries).
//   - Bulk errors: log individual document failures, continue processing.
//   - Version conflicts: already handled via retry_on_conflict in the request body.
func (c *Connector) Write(ctx context.Context, events []*event.ChangeEvent) error {
	c.mu.Lock()
	c.status.State = sink.StateWriting
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.status.State = sink.StateReady
		c.status.Timestamp = time.Now().Format(time.RFC3339)
		c.mu.Unlock()
	}()

	// Map events to bulk actions, skipping non-data events (DDL, heartbeat, …).
	var actions []*BulkAction
	for _, e := range events {
		action := c.mapper.MapEvent(e)
		if action == nil {
			continue
		}
		actions = append(actions, action)
	}

	if len(actions) == 0 {
		// Nothing to write; update position to the last event if any.
		if len(events) > 0 {
			c.mu.Lock()
			pos := events[len(events)-1].Position
			c.position = &pos
			c.mu.Unlock()
		}
		return nil
	}

	body := c.indexer.BuildRequestBody(actions)
	if err := c.sendBulkWithRetry(ctx, body); err != nil {
		c.mu.Lock()
		c.status.EventsFailed += int64(len(actions))
		c.mu.Unlock()
		return err
	}

	// Update statistics and position.
	c.mu.Lock()
	c.status.EventsWritten += int64(len(events))
	c.status.BytesWritten += int64(len(body))
	if len(events) > 0 {
		pos := events[len(events)-1].Position
		c.position = &pos
	}
	c.mu.Unlock()

	return nil
}

// Flush flushes any buffered data. The Elasticsearch connector sends data
// synchronously in Write, so Flush is effectively a no-op.
func (c *Connector) Flush(ctx context.Context) error {
	log.Debug("Elasticsearch sink flushed")
	return nil
}

// GetPosition returns the last committed position.
func (c *Connector) GetPosition() *event.Position {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.position == nil {
		return nil
	}
	return c.position.Clone()
}

// SupportsDDL returns false — Elasticsearch handles schema dynamically.
func (c *Connector) SupportsDDL() bool {
	return false
}

// ApplyDDL is a no-op for Elasticsearch — schema is handled dynamically.
func (c *Connector) ApplyDDL(_ context.Context, _ *event.ChangeEvent) error {
	return nil
}

// SupportsTransaction returns false — Elasticsearch uses bulk writes.
func (c *Connector) SupportsTransaction() bool {
	return false
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// pingWithRetry pings the Elasticsearch cluster with exponential backoff.
// Maximum 5 retries as per design §1.6.
func (c *Connector) pingWithRetry(ctx context.Context, client *elasticsearch.Client) error {
	const maxRetries = 5
	wait := 500 * time.Millisecond

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			wait *= 2
		}

		res, err := client.Info(client.Info.WithContext(ctx))
		if err != nil {
			lastErr = err
			log.Warn("Elasticsearch ping failed, retrying",
				zap.Int("attempt", attempt+1),
				zap.Error(err))
			continue
		}
		res.Body.Close()
		if res.IsError() {
			lastErr = fmt.Errorf("ping returned HTTP %s", res.Status())
			log.Warn("Elasticsearch ping error response, retrying",
				zap.Int("attempt", attempt+1),
				zap.String("status", res.Status()))
			continue
		}
		return nil
	}
	return fmt.Errorf("could not connect after %d retries: %w", maxRetries, lastErr)
}

// sendBulkWithRetry sends the ND-JSON body to the Bulk API with exponential
// backoff on connection-level errors (max 5 retries).
func (c *Connector) sendBulkWithRetry(ctx context.Context, body []byte) error {
	const maxRetries = 5
	wait := 500 * time.Millisecond

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			wait *= 2
		}

		err := c.sendBulk(ctx, body)
		if err == nil {
			return nil
		}
		lastErr = err
		log.Warn("Elasticsearch bulk request failed, retrying",
			zap.Int("attempt", attempt+1),
			zap.Error(err))
	}
	return fmt.Errorf("bulk request failed after %d retries: %w", maxRetries, lastErr)
}

// sendBulk performs a single Bulk API call and handles item-level errors per
// design §1.6: log individual document failures, continue processing.
func (c *Connector) sendBulk(ctx context.Context, body []byte) error {
	res, err := c.client.Bulk(
		bytes.NewReader(body),
		c.client.Bulk.WithRefresh(c.config.RefreshPolicy),
		c.client.Bulk.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("bulk API call failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk API returned HTTP %s", res.Status())
	}

	// Parse the response to detect item-level errors.
	rawBody, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("failed to read bulk response: %w", err)
	}

	var bulkResp struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Index  string `json:"_index"`
			ID     string `json:"_id"`
			Status int    `json:"status"`
			Error  *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error,omitempty"`
		} `json:"items"`
	}

	if err := json.Unmarshal(rawBody, &bulkResp); err != nil {
		// Cannot parse response; treat as success to avoid data loss on
		// non-standard ES responses.
		log.Warn("Failed to parse Elasticsearch bulk response", zap.Error(err))
		return nil
	}

	if !bulkResp.Errors {
		return nil
	}

	// Log individual failures but do not return an error — design §1.6.
	for _, item := range bulkResp.Items {
		for op, meta := range item {
			if meta.Error != nil {
				log.Warn("Elasticsearch document error",
					zap.String("operation", op),
					zap.String("index", meta.Index),
					zap.String("id", meta.ID),
					zap.Int("status", meta.Status),
					zap.String("errorType", meta.Error.Type),
					zap.String("reason", meta.Error.Reason))
			}
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// parseConfig parses a sink.Config into an elasticsearch.Config.
// ---------------------------------------------------------------------------

func parseConfig(config sink.Config) (*Config, error) {
	cfg := DefaultConfig()

	// URLs: prefer Connection.URLs, fall back to properties "urls".
	if len(config.Connection.URLs) > 0 {
		cfg.URLs = config.Connection.URLs
	} else if v, ok := config.Properties["urls"].([]interface{}); ok {
		cfg.URLs = make([]string, 0, len(v))
		for _, u := range v {
			if s, ok := u.(string); ok {
				cfg.URLs = append(cfg.URLs, s)
			}
		}
	}

	// Auth.
	if config.Connection.User != "" {
		cfg.Username = config.Connection.User
	}
	if config.Connection.Password != "" {
		cfg.Password = config.Connection.Password
	}
	if v, ok := config.Properties["username"].(string); ok {
		cfg.Username = v
	}
	if v, ok := config.Properties["password"].(string); ok {
		cfg.Password = v
	}
	if v, ok := config.Properties["apiKey"].(string); ok {
		cfg.APIKey = v
	}

	// Index settings.
	if v, ok := config.Properties["indexPrefix"].(string); ok {
		cfg.IndexPrefix = v
	}
	if v, ok := config.Properties["indexPattern"].(string); ok {
		cfg.IndexPattern = v
	}

	// Bulk settings.
	if v, ok := config.Properties["batchSize"].(float64); ok {
		cfg.BatchSize = int(v)
	}
	if v, ok := config.Properties["flushInterval"].(float64); ok {
		cfg.FlushInterval = time.Duration(v) * time.Millisecond
	}

	// Write settings.
	if v, ok := config.Properties["refreshPolicy"].(string); ok {
		cfg.RefreshPolicy = v
	}
	if v, ok := config.Properties["retryOnConflict"].(float64); ok {
		cfg.RetryOnConflict = int(v)
	}

	return cfg, nil
}

// ---------------------------------------------------------------------------
// Factory / registry
// ---------------------------------------------------------------------------

func init() {
	sink.Register("elasticsearch", &factory{})
}

type factory struct{}

func (f *factory) Create(config sink.Config) (sink.Connector, error) {
	return New(), nil
}
