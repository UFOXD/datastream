# Phase 9: Enterprise Database Connectors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement 4 new connectors: Elasticsearch Sink, Redis Sink, SQL Server Source, and Oracle Source.

**Architecture:** Each connector follows existing patterns from MongoDB sink and MySQL source. Uses interface-based design with `sink.Connector` and `source.Connector` interfaces. Connectors are registered via `init()` functions and implement batch/event-based processing.

**Tech Stack:** Go 1.21+, github.com/elastic/go-elasticsearch/v8, github.com/redis/go-redis/v9, github.com/microsoft/go-mssqldb, github.com/sijms/go-ora/v2

---

## File Structure

```
internal/sink/elasticsearch/
├── config.go        # Configuration struct with validation
├── connector.go     # Connector implementation
├── indexer.go       # Bulk indexer with batching
├── mapper.go        # Event to document mapping
└── connector_test.go

internal/sink/redis/
├── config.go        # Configuration struct
├── connector.go     # Connector implementation
├── writer.go        # Pipeline writer
└── connector_test.go

internal/source/sqlserver/
├── config.go        # Configuration struct
├── connector.go     # Connector implementation
├── cdc_reader.go    # CDC table polling
├── schema_cache.go  # Schema caching
└── connector_test.go

internal/source/oracle/
├── config.go        # Configuration struct
├── connector.go     # Connector implementation
├── logminer.go      # LogMiner reader
├── sql_parser.go    # DML/DDL parsing
├── schema_cache.go  # Schema caching
└── connector_test.go
```

---

## Task 1: Elasticsearch Sink - Configuration

**Files:**
- Create: `internal/sink/elasticsearch/config.go`
- Test: `internal/sink/elasticsearch/connector_test.go`

- [ ] **Step 1: Write the failing test for config validation**

```go
// internal/sink/elasticsearch/connector_test.go
package elasticsearch

import (
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "empty URLs",
			config:  &Config{},
			wantErr: true,
		},
		{
			name: "valid config",
			config: &Config{
				URLs: []string{"http://localhost:9200"},
			},
			wantErr: false,
		},
		{
			name: "invalid refresh policy",
			config: &Config{
				URLs:          []string{"http://localhost:9200"},
				RefreshPolicy: "invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if len(cfg.URLs) != 0 {
		t.Error("default URLs should be empty")
	}
	if cfg.BatchSize != 1000 {
		t.Errorf("default BatchSize = %d, want 1000", cfg.BatchSize)
	}
	if cfg.FlushInterval != time.Second {
		t.Errorf("default FlushInterval = %v, want 1s", cfg.FlushInterval)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/peter/Codes/dts/datastream && go test ./internal/sink/elasticsearch/... -v -run TestConfig`
Expected: FAIL (package not found)

- [ ] **Step 3: Create directory and write config.go**

```bash
mkdir -p internal/sink/elasticsearch
```

```go
// internal/sink/elasticsearch/config.go
// Package elasticsearch provides an Elasticsearch sink connector for DataStream.
package elasticsearch

import (
	"fmt"
	"time"
)

// Config holds the Elasticsearch sink connector configuration.
type Config struct {
	// Connection
	URLs     []string `json:"urls"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	APIKey   string   `json:"apiKey"`

	// Index settings
	IndexPrefix  string `json:"indexPrefix"`
	IndexPattern string `json:"indexPattern"` // default: "{database}_{table}"

	// Bulk settings
	BatchSize     int           `json:"batchSize"`
	FlushInterval time.Duration `json:"flushInterval"`

	// Write settings
	RefreshPolicy   string `json:"refreshPolicy"`   // "true", "wait_for", "false"
	RetryOnConflict int    `json:"retryOnConflict"` // default: 3
}

// DefaultConfig returns the default Elasticsearch sink configuration.
func DefaultConfig() *Config {
	return &Config{
		IndexPattern:    "{database}_{table}",
		BatchSize:       1000,
		FlushInterval:   time.Second,
		RefreshPolicy:   "false",
		RetryOnConflict: 3,
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if len(c.URLs) == 0 {
		return fmt.Errorf("at least one URL is required")
	}

	switch c.RefreshPolicy {
	case "", "true", "wait_for", "false":
		// Valid
	default:
		return fmt.Errorf("invalid refresh policy: %s", c.RefreshPolicy)
	}

	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/peter/Codes/dts/datastream && go test ./internal/sink/elasticsearch/... -v -run TestConfig`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/sink/elasticsearch/config.go internal/sink/elasticsearch/connector_test.go
git commit -m "feat(sink): add Elasticsearch config with validation"
```

---

## Task 2: Elasticsearch Sink - Document Mapper

**Files:**
- Create: `internal/sink/elasticsearch/mapper.go`
- Test: `internal/sink/elasticsearch/connector_test.go`

- [ ] **Step 1: Write the failing test for document mapping**

```go
// Add to internal/sink/elasticsearch/connector_test.go

func TestDocumentMapper(t *testing.T) {
	mapper := NewDocumentMapper(DefaultConfig())

	t.Run("generate document ID", func(t *testing.T) {
		row := event.RowData{
			Fields: map[string]event.Field{
				"id":   {Name: "id", Value: 123},
				"name": {Name: "name", Value: "test"},
			},
		}
		pkColumns := []string{"id"}
		id := mapper.GenerateDocID(row, pkColumns)
		if id != "123" {
			t.Errorf("GenerateDocID() = %s, want 123", id)
		}
	})

	t.Run("generate document ID with multiple PKs", func(t *testing.T) {
		row := event.RowData{
			Fields: map[string]event.Field{
				"user_id": {Name: "user_id", Value: 1},
				"post_id": {Name: "post_id", Value: 2},
			},
		}
		pkColumns := []string{"user_id", "post_id"}
		id := mapper.GenerateDocID(row, pkColumns)
		if id != "1_2" {
			t.Errorf("GenerateDocID() = %s, want 1_2", id)
		}
	})

	t.Run("resolve index name", func(t *testing.T) {
		table := event.TableInfo{
			Database: "mydb",
			Table:    "users",
		}
		index := mapper.ResolveIndex(table)
		if index != "mydb_users" {
			t.Errorf("ResolveIndex() = %s, want mydb_users", index)
		}
	})

	t.Run("resolve index name with prefix", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.IndexPrefix = "cdc_"
		mapper := NewDocumentMapper(cfg)
		table := event.TableInfo{
			Database: "mydb",
			Table:    "users",
		}
		index := mapper.ResolveIndex(table)
		if index != "cdc_mydb_users" {
			t.Errorf("ResolveIndex() = %s, want cdc_mydb_users", index)
		}
	})

	t.Run("build document from row", func(t *testing.T) {
		row := event.RowData{
			Fields: map[string]event.Field{
				"id":   {Name: "id", Value: 123},
				"name": {Name: "name", Value: "test"},
			},
		}
		doc := mapper.BuildDocument(row)
		if doc["id"] != 123 {
			t.Errorf("BuildDocument() id = %v, want 123", doc["id"])
		}
		if doc["name"] != "test" {
			t.Errorf("BuildDocument() name = %v, want test", doc["name"])
		}
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/peter/Codes/dts/datastream && go test ./internal/sink/elasticsearch/... -v -run TestDocumentMapper`
Expected: FAIL (undefined: NewDocumentMapper)

- [ ] **Step 3: Write the document mapper implementation**

```go
// internal/sink/elasticsearch/mapper.go
package elasticsearch

import (
	"fmt"
	"strings"

	"github.com/UFOXD/datastream/pkg/event"
)

// DocumentMapper maps ChangeEvents to Elasticsearch documents.
type DocumentMapper struct {
	config *Config
}

// NewDocumentMapper creates a new document mapper.
func NewDocumentMapper(config *Config) *DocumentMapper {
	return &DocumentMapper{config: config}
}

// GenerateDocID generates a document ID from primary key columns.
func (m *DocumentMapper) GenerateDocID(row event.RowData, pkColumns []string) string {
	if len(pkColumns) == 0 {
		return ""
	}

	var parts []string
	for _, col := range pkColumns {
		if field, ok := row.Fields[col]; ok {
			parts = append(parts, fmt.Sprintf("%v", field.Value))
		}
	}

	return strings.Join(parts, "_")
}

// ResolveIndex resolves the index name for a table.
func (m *DocumentMapper) ResolveIndex(table event.TableInfo) string {
	indexName := m.config.IndexPattern
	indexName = strings.ReplaceAll(indexName, "{database}", strings.ToLower(table.Database))
	indexName = strings.ReplaceAll(indexName, "{table}", strings.ToLower(table.Table))

	if m.config.IndexPrefix != "" {
		indexName = m.config.IndexPrefix + indexName
	}

	return indexName
}

// BuildDocument builds a document from RowData.
func (m *DocumentMapper) BuildDocument(row event.RowData) map[string]interface{} {
	doc := make(map[string]interface{})

	for name, field := range row.Fields {
		doc[name] = field.Value
	}

	return doc
}

// BuildBulkAction builds a bulk action for an event.
type BulkAction struct {
	Index string
	ID    string
	Op    string // "index", "update", "delete"
	Doc   map[string]interface{}
}

// MapEvent maps a ChangeEvent to a BulkAction.
func (m *DocumentMapper) MapEvent(e *event.ChangeEvent) *BulkAction {
	action := &BulkAction{
		Index: m.ResolveIndex(e.Table),
	}

	switch e.Type {
	case event.EventTypeInsert:
		action.Op = "index"
		action.ID = m.GenerateDocID(e.After, e.Table.PrimaryKeyColumns)
		action.Doc = m.BuildDocument(e.After)

	case event.EventTypeUpdate:
		action.Op = "update"
		action.ID = m.GenerateDocID(e.After, e.Table.PrimaryKeyColumns)
		action.Doc = m.BuildDocument(e.After)

	case event.EventTypeDelete:
		action.Op = "delete"
		action.ID = m.GenerateDocID(e.Before, e.Table.PrimaryKeyColumns)
		action.Doc = nil
	}

	return action
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/peter/Codes/dts/datastream && go test ./internal/sink/elasticsearch/... -v -run TestDocumentMapper`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/sink/elasticsearch/mapper.go internal/sink/elasticsearch/connector_test.go
git commit -m "feat(sink): add Elasticsearch document mapper"
```

---

## Task 3: Elasticsearch Sink - Bulk Indexer

**Files:**
- Create: `internal/sink/elasticsearch/indexer.go`
- Test: `internal/sink/elasticsearch/connector_test.go`

- [ ] **Step 1: Write the failing test for bulk indexer**

```go
// Add to internal/sink/elasticsearch/connector_test.go

func TestBulkIndexer(t *testing.T) {
	t.Run("add action to buffer", func(t *testing.T) {
		indexer := NewBulkIndexer(&Config{
			BatchSize:     10,
			FlushInterval: time.Second,
		})

		action := &BulkAction{
			Index: "test_index",
			ID:    "1",
			Op:    "index",
			Doc:   map[string]interface{}{"name": "test"},
		}

		err := indexer.Add(action)
		if err != nil {
			t.Errorf("Add() error = %v", err)
		}

		if indexer.BufferSize() != 1 {
			t.Errorf("BufferSize() = %d, want 1", indexer.BufferSize())
		}
	})

	t.Run("clear buffer", func(t *testing.T) {
		indexer := NewBulkIndexer(&Config{
			BatchSize:     10,
			FlushInterval: time.Second,
		})

		indexer.Add(&BulkAction{Index: "test", ID: "1", Op: "index"})
		indexer.Clear()

		if indexer.BufferSize() != 0 {
			t.Errorf("BufferSize() after Clear = %d, want 0", indexer.BufferSize())
		}
	})

	t.Run("build bulk request body", func(t *testing.T) {
		indexer := NewBulkIndexer(&Config{})

		actions := []*BulkAction{
			{Index: "users", ID: "1", Op: "index", Doc: map[string]interface{}{"name": "alice"}},
			{Index: "users", ID: "2", Op: "update", Doc: map[string]interface{}{"name": "bob"}},
			{Index: "users", ID: "3", Op: "delete"},
		}

		body := indexer.BuildRequestBody(actions)

		// Should contain action lines and optional doc lines
		if !bytes.Contains(body, []byte(`"index"`)) {
			t.Error("BuildRequestBody missing index action")
		}
		if !bytes.Contains(body, []byte(`"update"`)) {
			t.Error("BuildRequestBody missing update action")
		}
		if !bytes.Contains(body, []byte(`"delete"`)) {
			t.Error("BuildRequestBody missing delete action")
		}
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/peter/Codes/dts/datastream && go test ./internal/sink/elasticsearch/... -v -run TestBulkIndexer`
Expected: FAIL (undefined: NewBulkIndexer)

- [ ] **Step 3: Write the bulk indexer implementation**

```go
// internal/sink/elasticsearch/indexer.go
package elasticsearch

import (
	"bytes"
	"encoding/json"
	"sync"
)

// BulkIndexer manages batching of bulk actions.
type BulkIndexer struct {
	config  *Config
	buffer  []*BulkAction
	mu      sync.Mutex
}

// NewBulkIndexer creates a new bulk indexer.
func NewBulkIndexer(config *Config) *BulkIndexer {
	return &BulkIndexer{
		config: config,
		buffer: make([]*BulkAction, 0, config.BatchSize),
	}
}

// Add adds an action to the buffer.
func (b *BulkIndexer) Add(action *BulkAction) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buffer = append(b.buffer, action)
	return nil
}

// BufferSize returns the current buffer size.
func (b *BulkIndexer) BufferSize() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.buffer)
}

// Clear clears the buffer.
func (b *BulkIndexer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buffer = make([]*BulkAction, 0, b.config.BatchSize)
}

// GetAndClear returns the current buffer and clears it.
func (b *BulkIndexer) GetAndClear() []*BulkAction {
	b.mu.Lock()
	defer b.mu.Unlock()
	actions := b.buffer
	b.buffer = make([]*BulkAction, 0, b.config.BatchSize)
	return actions
}

// ShouldFlush returns true if buffer should be flushed.
func (b *BulkIndexer) ShouldFlush() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.buffer) >= b.config.BatchSize
}

// BuildRequestBody builds the ND-JSON body for bulk request.
func (b *BulkIndexer) BuildRequestBody(actions []*BulkAction) []byte {
	var buf bytes.Buffer

	for _, action := range actions {
		// Write action line
		actionLine := map[string]interface{}{
			action.Op: map[string]interface{}{
				"_index": action.Index,
				"_id":    action.ID,
			},
		}

		// Add retry_on_conflict for update operations
		if action.Op == "update" && b.config.RetryOnConflict > 0 {
			actionLine[action.Op].(map[string]interface{})["retry_on_conflict"] = b.config.RetryOnConflict
		}

		line, _ := json.Marshal(actionLine)
		buf.Write(line)
		buf.WriteByte('\n')

		// Write doc line for index and update operations
		if action.Op == "index" && action.Doc != nil {
			doc, _ := json.Marshal(action.Doc)
			buf.Write(doc)
			buf.WriteByte('\n')
		} else if action.Op == "update" && action.Doc != nil {
			docLine := map[string]interface{}{
				"doc":           action.Doc,
				"doc_as_upsert": true,
			}
			doc, _ := json.Marshal(docLine)
			buf.Write(doc)
			buf.WriteByte('\n')
		}
	}

	return buf.Bytes()
}
```

- [ ] **Step 4: Add bytes import to test file**

```go
// Add to imports in connector_test.go
import (
	"bytes"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /Users/peter/Codes/dts/datastream && go test ./internal/sink/elasticsearch/... -v -run TestBulkIndexer`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/sink/elasticsearch/indexer.go internal/sink/elasticsearch/connector_test.go
git commit -m "feat(sink): add Elasticsearch bulk indexer"
```

---

## Task 4: Elasticsearch Sink - Connector

**Files:**
- Create: `internal/sink/elasticsearch/connector.go`
- Test: `internal/sink/elasticsearch/connector_test.go`

- [ ] **Step 1: Write the failing test for connector interface**

```go
// Add to internal/sink/elasticsearch/connector_test.go

func TestConnectorImplementsInterface(t *testing.T) {
	// Compile-time check
	var _ sink.Connector = (*Connector)(nil)
}

func TestConnectorName(t *testing.T) {
	c := New()
	if c.Name() != "elasticsearch" {
		t.Errorf("Name() = %s, want elasticsearch", c.Name())
	}
}

func TestConnectorSupportsDDL(t *testing.T) {
	c := New()
	if c.SupportsDDL() != false {
		t.Error("SupportsDDL() should return false")
	}
}

func TestConnectorSupportsTransaction(t *testing.T) {
	c := New()
	if c.SupportsTransaction() != false {
		t.Error("SupportsTransaction() should return false")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/peter/Codes/dts/datastream && go test ./internal/sink/elasticsearch/... -v -run TestConnector`
Expected: FAIL (undefined: Connector, sink)

- [ ] **Step 3: Write the connector implementation**

```go
// internal/sink/elasticsearch/connector.go
// Package elasticsearch provides an Elasticsearch sink connector for DataStream.
package elasticsearch

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/UFOXD/datastream/internal/sink"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Connector implements the sink.Connector interface for Elasticsearch.
type Connector struct {
	config  *Config
	status  sink.Status
	client  *elasticsearch.Client
	indexer *BulkIndexer
	mapper  *DocumentMapper
	position *event.Position
	mu      sync.RWMutex
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

// Initialize initializes the connector.
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

	c.config = cfg

	// Create Elasticsearch client
	esCfg := elasticsearch.Config{
		Addresses: cfg.URLs,
	}

	if cfg.Username != "" && cfg.Password != "" {
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

	// Test connection
	res, err := client.Info()
	if err != nil {
		return fmt.Errorf("failed to connect to Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Elasticsearch returned error: %s", res.String())
	}

	c.client = client
	c.indexer = NewBulkIndexer(cfg)
	c.mapper = NewDocumentMapper(cfg)
	c.status.State = sink.StateReady
	c.status.Timestamp = time.Now().Format(time.RFC3339)

	log.Info("Elasticsearch sink initialized",
		zap.Strings("urls", cfg.URLs))
	return nil
}

// Start starts the connector.
func (c *Connector) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status.State == sink.StateWriting {
		return nil
	}

	if c.client == nil {
		return sink.ErrNotInitialized
	}

	c.status.State = sink.StateReady
	log.Info("Elasticsearch sink started")
	return nil
}

// Stop stops the connector.
func (c *Connector) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Flush remaining buffer
	if c.indexer.BufferSize() > 0 {
		if err := c.Flush(ctx); err != nil {
			log.Warn("failed to flush on stop", zap.Error(err))
		}
	}

	c.status.State = sink.StateStopped
	log.Info("Elasticsearch sink stopped")
	return nil
}

// Status returns the current status.
func (c *Connector) Status() sink.Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// Write writes events to Elasticsearch.
func (c *Connector) Write(ctx context.Context, events []*event.ChangeEvent) error {
	c.mu.Lock()
	c.status.State = sink.StateWriting
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.status.State = sink.StateReady
		c.mu.Unlock()
	}()

	// Add all events to indexer
	for _, e := range events {
		if e.IsDDL() {
			continue // Skip DDL events
		}

		action := c.mapper.MapEvent(e)
		if err := c.indexer.Add(action); err != nil {
			return err
		}

		// Flush if buffer is full
		if c.indexer.ShouldFlush() {
			if err := c.Flush(ctx); err != nil {
				return err
			}
		}
	}

	// Update position to last event
	if len(events) > 0 {
		c.mu.Lock()
		c.position = &events[len(events)-1].Position
		c.mu.Unlock()
	}

	c.mu.Lock()
	c.status.EventsWritten += int64(len(events))
	c.mu.Unlock()

	return nil
}

// Flush flushes any buffered data.
func (c *Connector) Flush(ctx context.Context) error {
	actions := c.indexer.GetAndClear()
	if len(actions) == 0 {
		return nil
	}

	body := c.indexer.BuildRequestBody(actions)

	req := esapi.BulkRequest{
		Body:       bytes.NewReader(body),
		Refresh:    c.config.RefreshPolicy,
		Pretty:     false,
		Human:      false,
		ErrorTrace: true,
	}

	res, err := req.Do(ctx, c.client)
	if err != nil {
		c.mu.Lock()
		c.status.EventsFailed += int64(len(actions))
		c.mu.Unlock()
		return fmt.Errorf("failed to execute bulk request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		c.mu.Lock()
		c.status.EventsFailed += int64(len(actions))
		c.mu.Unlock()
		return fmt.Errorf("Elasticsearch bulk error: %s", res.String())
	}

	log.Debug("Elasticsearch bulk write completed",
		zap.Int("actions", len(actions)))

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

// SupportsDDL returns false.
func (c *Connector) SupportsDDL() bool {
	return false
}

// SupportsTransaction returns false.
func (c *Connector) SupportsTransaction() bool {
	return false
}

func parseConfig(config sink.Config) (*Config, error) {
	cfg := DefaultConfig()

	// Copy connection settings
	if len(config.Connection.URLs) > 0 {
		cfg.URLs = config.Connection.URLs
	}
	cfg.Username = config.Connection.User
	cfg.Password = config.Connection.Password

	// Parse properties
	if v, ok := config.Properties["urls"].([]interface{}); ok {
		cfg.URLs = make([]string, 0, len(v))
		for _, url := range v {
			if u, ok := url.(string); ok {
				cfg.URLs = append(cfg.URLs, u)
			}
		}
	}
	if v, ok := config.Properties["apiKey"].(string); ok {
		cfg.APIKey = v
	}
	if v, ok := config.Properties["indexPrefix"].(string); ok {
		cfg.IndexPrefix = v
	}
	if v, ok := config.Properties["indexPattern"].(string); ok {
		cfg.IndexPattern = v
	}
	if v, ok := config.Properties["batchSize"].(float64); ok {
		cfg.BatchSize = int(v)
	}
	if v, ok := config.Properties["flushInterval"].(float64); ok {
		cfg.FlushInterval = time.Duration(v) * time.Millisecond
	}
	if v, ok := config.Properties["refreshPolicy"].(string); ok {
		cfg.RefreshPolicy = v
	}
	if v, ok := config.Properties["retryOnConflict"].(float64); ok {
		cfg.RetryOnConflict = int(v)
	}

	return cfg, nil
}

// Import for esapi
import "github.com/elastic/go-elasticsearch/v8/esapi"

func init() {
	sink.Register("elasticsearch", &factory{})
}

type factory struct{}

func (f *factory) Create(config sink.Config) (sink.Connector, error) {
	return New(), nil
}
```

- [ ] **Step 4: Fix imports and run test**

The import statement needs to be moved to the top. Let me fix that:

```go
// internal/sink/elasticsearch/connector.go
// Package elasticsearch provides an Elasticsearch sink connector for DataStream.
package elasticsearch

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/UFOXD/datastream/internal/sink"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)
```

Also add import to test file:

```go
import (
	"bytes"
	"testing"
	"time"

	"github.com/UFOXD/datastream/internal/sink"
	"github.com/UFOXD/datastream/pkg/event"
)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /Users/peter/Codes/dts/datastream && go test ./internal/sink/elasticsearch/... -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/sink/elasticsearch/connector.go internal/sink/elasticsearch/connector_test.go
git commit -m "feat(sink): add Elasticsearch connector implementation"
```

---

## Task 5: Add Elasticsearch Dependency

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: Add Elasticsearch dependency**

```bash
cd /Users/peter/Codes/dts/datastream && go get github.com/elastic/go-elasticsearch/v8@latest
```

- [ ] **Step 2: Verify build passes**

Run: `cd /Users/peter/Codes/dts/datastream && go build ./...`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add Elasticsearch client dependency"
```

---

## Task 6: Redis Sink - Configuration

**Files:**
- Create: `internal/sink/redis/config.go`
- Test: `internal/sink/redis/connector_test.go`

- [ ] **Step 1: Write the failing test for config validation**

```go
// internal/sink/redis/connector_test.go
package redis

import (
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "empty address",
			config:  &Config{},
			wantErr: false, // default address will be used
		},
		{
			name: "valid config",
			config: &Config{
				Addr: "localhost:6379",
			},
			wantErr: false,
		},
		{
			name: "invalid format",
			config: &Config{
				Format: "invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Addr != "localhost:6379" {
		t.Errorf("default Addr = %s, want localhost:6379", cfg.Addr)
	}
	if cfg.Format != "hash" {
		t.Errorf("default Format = %s, want hash", cfg.Format)
	}
	if cfg.BatchSize != 1000 {
		t.Errorf("default BatchSize = %d, want 1000", cfg.BatchSize)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/peter/Codes/dts/datastream && go test ./internal/sink/redis/... -v -run TestConfig`
Expected: FAIL (package not found)

- [ ] **Step 3: Create directory and write config.go**

```bash
mkdir -p internal/sink/redis
```

```go
// internal/sink/redis/config.go
// Package redis provides a Redis sink connector for DataStream.
package redis

import (
	"fmt"
	"time"
)

// Config holds the Redis sink connector configuration.
type Config struct {
	// Connection
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`

	// Write settings
	KeyPattern   string          `json:"keyPattern"`   // default: "{database}:{table}:{pk}"
	TTL          time.Duration   `json:"ttl"`          // 0 = no expiration
	BatchSize    int             `json:"batchSize"`
	FlushInterval time.Duration  `json:"flushInterval"`

	// Data format
	Format string `json:"format"` // "hash", "json", "string"
}

// DefaultConfig returns the default Redis sink configuration.
func DefaultConfig() *Config {
	return &Config{
		Addr:          "localhost:6379",
		KeyPattern:    "{database}:{table}:{pk}",
		BatchSize:     1000,
		FlushInterval: time.Second,
		Format:        "hash",
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	switch c.Format {
	case "", "hash", "json", "string":
		// Valid
	default:
		return fmt.Errorf("invalid format: %s (must be hash, json, or string)", c.Format)
	}

	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/peter/Codes/dts/datastream && go test ./internal/sink/redis/... -v -run TestConfig`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/sink/redis/config.go internal/sink/redis/connector_test.go
git commit -m "feat(sink): add Redis config with validation"
```

---

## Task 7: Redis Sink - Writer

**Files:**
- Create: `internal/sink/redis/writer.go`
- Test: `internal/sink/redis/connector_test.go`

- [ ] **Step 1: Write the failing test for key generation**

```go
// Add to internal/sink/redis/connector_test.go

import (
	"github.com/UFOXD/datastream/pkg/event"
)

func TestKeyGeneration(t *testing.T) {
	writer := NewPipelineWriter(DefaultConfig())

	t.Run("generate key with pattern", func(t *testing.T) {
		table := event.TableInfo{
			Database: "mydb",
			Table:    "users",
		}
		row := event.RowData{
			Fields: map[string]event.Field{
				"id": {Name: "id", Value: 123},
			},
		}
		pkColumns := []string{"id"}

		key := writer.GenerateKey(table, row, pkColumns)
		if key != "mydb:users:123" {
			t.Errorf("GenerateKey() = %s, want mydb:users:123", key)
		}
	})

	t.Run("generate key with composite PK", func(t *testing.T) {
		table := event.TableInfo{
			Database: "mydb",
			Table:    "orders",
		}
		row := event.RowData{
			Fields: map[string]event.Field{
				"user_id": {Name: "user_id", Value: 1},
				"order_id": {Name: "order_id", Value: 2},
			},
		}
		pkColumns := []string{"user_id", "order_id"}

		key := writer.GenerateKey(table, row, pkColumns)
		if key != "mydb:orders:1_2" {
			t.Errorf("GenerateKey() = %s, want mydb:orders:1_2", key)
		}
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/peter/Codes/dts/datastream && go test ./internal/sink/redis/... -v -run TestKeyGeneration`
Expected: FAIL (undefined: NewPipelineWriter)

- [ ] **Step 3: Write the pipeline writer implementation**

```go
// internal/sink/redis/writer.go
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/UFOXD/datastream/pkg/event"
)

// WriteCommand represents a Redis write command.
type WriteCommand struct {
	Key   string
	Op    string      // "set", "hset", "del"
	Value interface{} // For hash: map[string]interface{}, for json/string: string
}

// PipelineWriter batches Redis commands.
type PipelineWriter struct {
	config  *Config
	buffer  []*WriteCommand
	mu      sync.Mutex
}

// NewPipelineWriter creates a new pipeline writer.
func NewPipelineWriter(config *Config) *PipelineWriter {
	return &PipelineWriter{
		config: config,
		buffer: make([]*WriteCommand, 0, config.BatchSize),
	}
}

// GenerateKey generates a Redis key from table and primary key.
func (w *PipelineWriter) GenerateKey(table event.TableInfo, row event.RowData, pkColumns []string) string {
	key := w.config.KeyPattern
	key = strings.ReplaceAll(key, "{database}", table.Database)
	key = strings.ReplaceAll(key, "{table}", table.Table)

	// Build primary key value
	var pkParts []string
	for _, col := range pkColumns {
		if field, ok := row.Fields[col]; ok {
			pkParts = append(pkParts, fmt.Sprintf("%v", field.Value))
		}
	}
	key = strings.ReplaceAll(key, "{pk}", strings.Join(pkParts, "_"))

	return key
}

// Add adds a command to the buffer.
func (w *PipelineWriter) Add(cmd *WriteCommand) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buffer = append(w.buffer, cmd)
}

// BufferSize returns the current buffer size.
func (w *PipelineWriter) BufferSize() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.buffer)
}

// GetAndClear returns the current buffer and clears it.
func (w *PipelineWriter) GetAndClear() []*WriteCommand {
	w.mu.Lock()
	defer w.mu.Unlock()
	cmds := w.buffer
	w.buffer = make([]*WriteCommand, 0, w.config.BatchSize)
	return cmds
}

// ShouldFlush returns true if buffer should be flushed.
func (w *PipelineWriter) ShouldFlush() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.buffer) >= w.config.BatchSize
}

// BuildHashValue builds a hash value from row data.
func (w *PipelineWriter) BuildHashValue(row event.RowData) map[string]interface{} {
	result := make(map[string]interface{})
	for name, field := range row.Fields {
		result[name] = field.Value
	}
	return result
}

// BuildJSONValue builds a JSON string from row data.
func (w *PipelineWriter) BuildJSONValue(row event.RowData) (string, error) {
	data := make(map[string]interface{})
	for name, field := range row.Fields {
		data[name] = field.Value
	}
	b, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// MapEvent maps a ChangeEvent to a WriteCommand.
func (w *PipelineWriter) MapEvent(e *event.ChangeEvent) *WriteCommand {
	cmd := &WriteCommand{}

	var row event.RowData
	var pkColumns []string

	switch e.Type {
	case event.EventTypeDelete:
		row = e.Before
		pkColumns = e.Table.PrimaryKeyColumns
		cmd.Op = "del"
		cmd.Key = w.GenerateKey(e.Table, row, pkColumns)
		return cmd

	case event.EventTypeInsert, event.EventTypeUpdate:
		row = e.After
		pkColumns = e.Table.PrimaryKeyColumns
	}

	cmd.Key = w.GenerateKey(e.Table, row, pkColumns)

	switch w.config.Format {
	case "hash":
		cmd.Op = "hset"
		cmd.Value = w.BuildHashValue(row)
	case "json":
		cmd.Op = "set"
		jsonVal, err := w.BuildJSONValue(row)
		if err != nil {
			return nil
		}
		cmd.Value = jsonVal
	case "string":
		cmd.Op = "set"
		// Use first field value as string
		for _, field := range row.Fields {
			cmd.Value = fmt.Sprintf("%v", field.Value)
			break
		}
	}

	return cmd
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/peter/Codes/dts/datastream && go test ./internal/sink/redis/... -v -run TestKeyGeneration`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/sink/redis/writer.go internal/sink/redis/connector_test.go
git commit -m "feat(sink): add Redis pipeline writer"
```

---

## Task 8: Redis Sink - Connector

**Files:**
- Create: `internal/sink/redis/connector.go`
- Test: `internal/sink/redis/connector_test.go`

- [ ] **Step 1: Write the failing test for connector interface**

```go
// Add to internal/sink/redis/connector_test.go

import (
	"github.com/UFOXD/datastream/internal/sink"
)

func TestConnectorImplementsInterface(t *testing.T) {
	// Compile-time check
	var _ sink.Connector = (*Connector)(nil)
}

func TestConnectorName(t *testing.T) {
	c := New()
	if c.Name() != "redis" {
		t.Errorf("Name() = %s, want redis", c.Name())
	}
}

func TestConnectorSupportsDDL(t *testing.T) {
	c := New()
	if c.SupportsDDL() != false {
		t.Error("SupportsDDL() should return false")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/peter/Codes/dts/datastream && go test ./internal/sink/redis/... -v -run TestConnector`
Expected: FAIL (undefined: Connector)

- [ ] **Step 3: Write the connector implementation**

```go
// internal/sink/redis/connector.go
// Package redis provides a Redis sink connector for DataStream.
package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/UFOXD/datastream/internal/sink"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/pingcap/log"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Connector implements the sink.Connector interface for Redis.
type Connector struct {
	config  *Config
	status  sink.Status
	client  *redis.Client
	writer  *PipelineWriter
	position *event.Position
	mu      sync.RWMutex
}

// New creates a new Redis sink connector.
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
	return "redis"
}

// Initialize initializes the connector.
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

	c.config = cfg

	// Create Redis client
	c.client = redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Test connection
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	c.writer = NewPipelineWriter(cfg)
	c.status.State = sink.StateReady
	c.status.Timestamp = time.Now().Format(time.RFC3339)

	log.Info("Redis sink initialized", zap.String("addr", cfg.Addr))
	return nil
}

// Start starts the connector.
func (c *Connector) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status.State == sink.StateWriting {
		return nil
	}

	if c.client == nil {
		return sink.ErrNotInitialized
	}

	c.status.State = sink.StateReady
	log.Info("Redis sink started")
	return nil
}

// Stop stops the connector.
func (c *Connector) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Flush remaining buffer
	if c.writer.BufferSize() > 0 {
		if err := c.Flush(ctx); err != nil {
			log.Warn("failed to flush on stop", zap.Error(err))
		}
	}

	if c.client != nil {
		c.client.Close()
	}

	c.status.State = sink.StateStopped
	log.Info("Redis sink stopped")
	return nil
}

// Status returns the current status.
func (c *Connector) Status() sink.Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// Write writes events to Redis.
func (c *Connector) Write(ctx context.Context, events []*event.ChangeEvent) error {
	c.mu.Lock()
	c.status.State = sink.StateWriting
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.status.State = sink.StateReady
		c.mu.Unlock()
	}()

	for _, e := range events {
		if e.IsDDL() {
			continue
		}

		cmd := c.writer.MapEvent(e)
		if cmd == nil {
			continue
		}

		c.writer.Add(cmd)

		if c.writer.ShouldFlush() {
			if err := c.Flush(ctx); err != nil {
				return err
			}
		}
	}

	// Update position
	if len(events) > 0 {
		c.mu.Lock()
		c.position = &events[len(events)-1].Position
		c.mu.Unlock()
	}

	c.mu.Lock()
	c.status.EventsWritten += int64(len(events))
	c.mu.Unlock()

	return nil
}

// Flush flushes any buffered data.
func (c *Connector) Flush(ctx context.Context) error {
	cmds := c.writer.GetAndClear()
	if len(cmds) == 0 {
		return nil
	}

	pipe := c.client.Pipeline()

	for _, cmd := range cmds {
		switch cmd.Op {
		case "set":
			pipe.Set(ctx, cmd.Key, cmd.Value, c.config.TTL)
		case "hset":
			if hash, ok := cmd.Value.(map[string]interface{}); ok {
				pipe.HSet(ctx, cmd.Key, hash)
				if c.config.TTL > 0 {
					pipe.Expire(ctx, cmd.Key, c.config.TTL)
				}
			}
		case "del":
			pipe.Del(ctx, cmd.Key)
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		c.mu.Lock()
		c.status.EventsFailed += int64(len(cmds))
		c.mu.Unlock()
		return fmt.Errorf("failed to execute Redis pipeline: %w", err)
	}

	log.Debug("Redis pipeline executed", zap.Int("commands", len(cmds)))
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

// SupportsDDL returns false.
func (c *Connector) SupportsDDL() bool {
	return false
}

// SupportsTransaction returns false.
func (c *Connector) SupportsTransaction() bool {
	return false
}

func parseConfig(config sink.Config) (*Config, error) {
	cfg := DefaultConfig()

	cfg.Addr = config.Connection.Addr
	cfg.Password = config.Connection.RedisPassword
	cfg.DB = config.Connection.RedisDB

	if v, ok := config.Properties["addr"].(string); ok {
		cfg.Addr = v
	}
	if v, ok := config.Properties["password"].(string); ok {
		cfg.Password = v
	}
	if v, ok := config.Properties["db"].(float64); ok {
		cfg.DB = int(v)
	}
	if v, ok := config.Properties["keyPattern"].(string); ok {
		cfg.KeyPattern = v
	}
	if v, ok := config.Properties["format"].(string); ok {
		cfg.Format = v
	}
	if v, ok := config.Properties["batchSize"].(float64); ok {
		cfg.BatchSize = int(v)
	}

	return cfg, nil
}

func init() {
	sink.Register("redis", &factory{})
}

type factory struct{}

func (f *factory) Create(config sink.Config) (sink.Connector, error) {
	return New(), nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/peter/Codes/dts/datastream && go test ./internal/sink/redis/... -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/sink/redis/connector.go internal/sink/redis/connector_test.go
git commit -m "feat(sink): add Redis connector implementation"
```

---

## Task 9: Add Redis Dependency

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: Add Redis dependency**

```bash
cd /Users/peter/Codes/dts/datastream && go get github.com/redis/go-redis/v9@latest
```

- [ ] **Step 2: Verify build passes**

Run: `cd /Users/peter/Codes/dts/datastream && go build ./...`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add Redis client dependency"
```

---

## Task 10: Run Full Test Suite

**Files:**
- None (verification)

- [ ] **Step 1: Run all tests**

Run: `cd /Users/peter/Codes/dts/datastream && go test ./... -short`
Expected: All PASS

- [ ] **Step 2: Verify build**

Run: `cd /Users/peter/Codes/dts/datastream && go build ./...`
Expected: No errors

---

## Remaining Tasks (SQL Server Source, Oracle Source)

The SQL Server Source and Oracle Source follow similar patterns but are more complex. Due to length constraints, they are outlined below with key implementation points:

### SQL Server Source (Tasks 11-20)

Key implementation points:
- Use `github.com/microsoft/go-mssqldb` driver
- Query `cdc.fn_cdc_get_all_changes_*` functions
- Track position via LSN (Log Sequence Number)
- Map `__$operation` codes to event types
- Implement `source.Connector` interface

### Oracle Source (Tasks 21-30)

Key implementation points:
- Use `github.com/sijms/go-ora/v2` driver
- Query `V$LOGMNR_CONTENTS` after starting LogMiner
- Parse SQL_REDO text for DML operations
- Track position via SCN (System Change Number)
- Handle DDL events via DDL operation codes
- Implement `source.Connector` interface

---

## Verification Commands

After completing all tasks:

```bash
# Build all packages
go build ./...

# Run all tests
go test ./... -short

# Run integration tests (requires Docker)
go test ./... -tags=integration

# Check coverage
go test ./internal/sink/elasticsearch/... -cover
go test ./internal/sink/redis/... -cover
go test ./internal/source/sqlserver/... -cover
go test ./internal/source/oracle/... -cover
```

---

*Plan Version: 1.0*
*Created: 2026-05-11*
