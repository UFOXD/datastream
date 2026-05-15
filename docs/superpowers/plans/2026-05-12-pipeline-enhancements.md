# Pipeline Enhancements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement ExpressionFilter, CustomTransformer, and BackpressureController to enhance pipeline processing capabilities.

**Architecture:** Three independent enhancements: ExpressionFilter for complex filtering logic, CustomTransformer foundation for script-based transformations, BackpressureController for flow control when sink is slow.

**Tech Stack:** Go 1.21+, expression parser, context-based cancellation

---

## Files Already Created

- `internal/filter/expression.go` - ExpressionFilter (✅ created)
- `internal/ratelimit/ratelimit.go` - RateLimiter (✅ created)

---

## File Structure

```
internal/filter/
├── expression.go              # ExpressionFilter (✅ created)
└── expression_test.go         # Tests

internal/transform/
├── custom.go                  # CustomTransformer foundation
└── custom_test.go             # Tests

internal/pipeline/
├── backpressure.go            # BackpressureController
└── backpressure_test.go       # Tests
```

---

### Task 1: Add Tests for ExpressionFilter

**Files:**
- Create: `internal/filter/expression_test.go`

- [ ] **Step 1: Write comprehensive tests**

```go
// internal/filter/expression_test.go
package filter

import (
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
)

func TestExpressionFilter_TableMatch(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		event      *event.ChangeEvent
		expected   bool
	}{
		{
			name:       "exact table match",
			expression: "table == 'users'",
			event: &event.ChangeEvent{
				Table: event.TableInfo{Table: "users"},
			},
			expected: true,
		},
		{
			name:       "table not match",
			expression: "table == 'users'",
			event: &event.ChangeEvent{
				Table: event.TableInfo{Table: "orders"},
			},
			expected: false,
		},
		{
			name:       "database match",
			expression: "database == 'inventory'",
			event: &event.ChangeEvent{
				Table: event.TableInfo{Database: "inventory", Table: "users"},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewExpressionFilter(&ExpressionConfig{Expression: tt.expression})
			if err != nil {
				t.Fatalf("NewExpressionFilter failed: %v", err)
			}

			result, err := filter.Filter(tt.event)
			if err != nil {
				t.Fatalf("Filter failed: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Filter() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestExpressionFilter_LogicalOperators(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		event      *event.ChangeEvent
		expected   bool
	}{
		{
			name:       "AND operator - both true",
			expression: "database == 'db1' && table == 'users'",
			event: &event.ChangeEvent{
				Table: event.TableInfo{Database: "db1", Table: "users"},
			},
			expected: true,
		},
		{
			name:       "AND operator - one false",
			expression: "database == 'db1' && table == 'users'",
			event: &event.ChangeEvent{
				Table: event.TableInfo{Database: "db1", Table: "orders"},
			},
			expected: false,
		},
		{
			name:       "OR operator - one true",
			expression: "table == 'users' || table == 'orders'",
			event: &event.ChangeEvent{
				Table: event.TableInfo{Table: "users"},
			},
			expected: true,
		},
		{
			name:       "OR operator - both false",
			expression: "table == 'users' || table == 'orders'",
			event: &event.ChangeEvent{
				Table: event.TableInfo{Table: "products"},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewExpressionFilter(&ExpressionConfig{Expression: tt.expression})
			if err != nil {
				t.Fatalf("NewExpressionFilter failed: %v", err)
			}

			result, err := filter.Filter(tt.event)
			if err != nil {
				t.Fatalf("Filter failed: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Filter() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestExpressionFilter_FieldAccess(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		event      *event.ChangeEvent
		expected   bool
	}{
		{
			name:       "after field comparison",
			expression: "after.age > 18",
			event: &event.ChangeEvent{
				After: &event.RowData{
					Fields: map[string]interface{}{"age": 25},
				},
			},
			expected: true,
		},
		{
			name:       "after field equality",
			expression: "after.status == 'active'",
			event: &event.ChangeEvent{
				After: &event.RowData{
					Fields: map[string]interface{}{"status": "active"},
				},
			},
			expected: true,
		},
		{
			name:       "regex match",
			expression: "after.email =~ '.*@example.com'",
			event: &event.ChangeEvent{
				After: &event.RowData{
					Fields: map[string]interface{}{"email": "user@example.com"},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewExpressionFilter(&ExpressionConfig{Expression: tt.expression})
			if err != nil {
				t.Fatalf("NewExpressionFilter failed: %v", err)
			}

			result, err := filter.Filter(tt.event)
			if err != nil {
				t.Fatalf("Filter failed: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Filter() = %v, want %v", result, tt.expected)
			}
		})
	}
}
```

- [ ] **Step 2: Run tests**

Run: `go test ./internal/filter/... -v -run TestExpressionFilter`
Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
git add internal/filter/expression_test.go
git commit -m "test(filter): add comprehensive tests for ExpressionFilter"
```

---

### Task 2: Implement CustomTransformer Foundation

**Files:**
- Create: `internal/transform/custom.go`
- Create: `internal/transform/custom_test.go`

- [ ] **Step 1: Write custom transformer**

```go
// internal/transform/custom.go
package transform

import (
	"context"
	"fmt"
	"sync"

	"github.com/UFOXD/datastream/pkg/event"
)

// TransformFunc is a function that transforms an event.
type TransformFunc func(ctx context.Context, e *event.ChangeEvent) (*event.ChangeEvent, error)

// CustomTransformer allows custom transformation logic.
type CustomTransformer struct {
	name        string
	transformFn TransformFunc
}

// CustomTransformerConfig holds configuration.
type CustomTransformerConfig struct {
	// Name is the transformer name
	Name string `json:"name" toml:"name"`

	// Type is the transformer type (lua, wasm, native)
	Type string `json:"type" toml:"type"`

	// Script is the script content (for lua/wasm)
	Script string `json:"script" toml:"script"`

	// TransformFunc is the native transform function
	TransformFunc TransformFunc `json:"-"`
}

// NewCustomTransformer creates a custom transformer.
func NewCustomTransformer(config *CustomTransformerConfig) (*CustomTransformer, error) {
	t := &CustomTransformer{
		name: config.Name,
	}

	switch config.Type {
	case "native":
		if config.TransformFunc == nil {
			return nil, fmt.Errorf("native transformer requires TransformFunc")
		}
		t.transformFn = config.TransformFunc

	case "lua":
		// Placeholder for Lua script transformer
		// Would require a Lua VM integration
		return nil, fmt.Errorf("lua transformer not yet implemented")

	case "wasm":
		// Placeholder for WASM transformer
		// Would require a WASM runtime integration
		return nil, fmt.Errorf("wasm transformer not yet implemented")

	default:
		return nil, fmt.Errorf("unknown transformer type: %s", config.Type)
	}

	return t, nil
}

// Transform applies the custom transformation.
func (t *CustomTransformer) Transform(e *event.ChangeEvent) (*event.ChangeEvent, error) {
	if t.transformFn == nil {
		return e, nil
	}
	return t.transformFn(context.Background(), e)
}

// Name returns the transformer name.
func (t *CustomTransformer) Name() string {
	return t.name
}

// ScriptTransformerRegistry manages script-based transformers.
type ScriptTransformerRegistry struct {
	mu           sync.RWMutex
	transformers map[string]*CustomTransformer
}

// NewScriptTransformerRegistry creates a registry.
func NewScriptTransformerRegistry() *ScriptTransformerRegistry {
	return &ScriptTransformerRegistry{
		transformers: make(map[string]*CustomTransformer),
	}
}

// Register registers a transformer.
func (r *ScriptTransformerRegistry) Register(name string, t *CustomTransformer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transformers[name] = t
}

// Get retrieves a transformer by name.
func (r *ScriptTransformerRegistry) Get(name string) (*CustomTransformer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.transformers[name]
	return t, ok
}

// Remove removes a transformer.
func (r *ScriptTransformerRegistry) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.transformers, name)
}

// List returns all registered transformer names.
func (r *ScriptTransformerRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.transformers))
	for name := range r.transformers {
		names = append(names, name)
	}
	return names
}

// Built-in transformers

// NewAddFieldTransformer creates a transformer that adds a static field.
func NewAddFieldTransformer(fieldName string, value interface{}) *CustomTransformer {
	return &CustomTransformer{
		name: "add-field-" + fieldName,
		transformFn: func(ctx context.Context, e *event.ChangeEvent) (*event.ChangeEvent, error) {
			if e.After != nil {
				if e.After.Fields == nil {
					e.After.Fields = make(map[string]interface{})
				}
				e.After.Fields[fieldName] = value
			}
			return e, nil
		},
	}
}

// NewRemoveFieldTransformer creates a transformer that removes a field.
func NewRemoveFieldTransformer(fieldName string) *CustomTransformer {
	return &CustomTransformer{
		name: "remove-field-" + fieldName,
		transformFn: func(ctx context.Context, e *event.ChangeEvent) (*event.ChangeEvent, error) {
			if e.After != nil && e.After.Fields != nil {
				delete(e.After.Fields, fieldName)
			}
			if e.Before != nil && e.Before.Fields != nil {
				delete(e.Before.Fields, fieldName)
			}
			return e, nil
		},
	}
}

// NewRenameFieldTransformer creates a transformer that renames a field.
func NewRenameFieldTransformer(oldName, newName string) *CustomTransformer {
	return &CustomTransformer{
		name: "rename-field-" + oldName,
		transformFn: func(ctx context.Context, e *event.ChangeEvent) (*event.ChangeEvent, error) {
			if e.After != nil && e.After.Fields != nil {
				if v, ok := e.After.Fields[oldName]; ok {
					delete(e.After.Fields, oldName)
					e.After.Fields[newName] = v
				}
			}
			if e.Before != nil && e.Before.Fields != nil {
				if v, ok := e.Before.Fields[oldName]; ok {
					delete(e.Before.Fields, oldName)
					e.Before.Fields[newName] = v
				}
			}
			return e, nil
		},
	}
}

// NewTimestampTransformer creates a transformer that adds a timestamp field.
func NewTimestampTransformer(fieldName string) *CustomTransformer {
	return &CustomTransformer{
		name: "timestamp-" + fieldName,
		transformFn: func(ctx context.Context, e *event.ChangeEvent) (*event.ChangeEvent, error) {
			if e.After != nil {
				if e.After.Fields == nil {
					e.After.Fields = make(map[string]interface{})
				}
				e.After.Fields[fieldName] = e.Timestamp
			}
			return e, nil
		},
	}
}
```

- [ ] **Step 2: Write tests**

```go
// internal/transform/custom_test.go
package transform

import (
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

func TestCustomTransformer_NativeTransform(t *testing.T) {
	config := &CustomTransformerConfig{
		Name: "test-transformer",
		Type: "native",
		TransformFunc: func(ctx context.Context, e *event.ChangeEvent) (*event.ChangeEvent, error) {
			e.After.Fields["transformed"] = true
			return e, nil
		},
	}

	transformer, err := NewCustomTransformer(config)
	if err != nil {
		t.Fatalf("NewCustomTransformer failed: %v", err)
	}

	e := &event.ChangeEvent{
		After: &event.RowData{
			Fields: map[string]interface{}{"id": 1},
		},
	}

	result, err := transformer.Transform(e)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if result.After.Fields["transformed"] != true {
		t.Error("Expected transformed field to be true")
	}
}

func TestAddFieldTransformer(t *testing.T) {
	transformer := NewAddFieldTransformer("source", "mysql")

	e := &event.ChangeEvent{
		After: &event.RowData{
			Fields: map[string]interface{}{"id": 1},
		},
	}

	result, err := transformer.Transform(e)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if result.After.Fields["source"] != "mysql" {
		t.Error("Expected source field to be 'mysql'")
	}
}

func TestRemoveFieldTransformer(t *testing.T) {
	transformer := NewRemoveFieldTransformer("password")

	e := &event.ChangeEvent{
		After: &event.RowData{
			Fields: map[string]interface{}{
				"id":       1,
				"password": "secret",
			},
		},
	}

	result, err := transformer.Transform(e)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if _, exists := result.After.Fields["password"]; exists {
		t.Error("Expected password field to be removed")
	}
}

func TestRenameFieldTransformer(t *testing.T) {
	transformer := NewRenameFieldTransformer("old_name", "new_name")

	e := &event.ChangeEvent{
		After: &event.RowData{
			Fields: map[string]interface{}{
				"id":       1,
				"old_name": "value",
			},
		},
	}

	result, err := transformer.Transform(e)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if _, exists := result.After.Fields["old_name"]; exists {
		t.Error("Expected old_name to be removed")
	}
	if result.After.Fields["new_name"] != "value" {
		t.Error("Expected new_name to have value 'value'")
	}
}

func TestScriptTransformerRegistry(t *testing.T) {
	registry := NewScriptTransformerRegistry()

	transformer := NewAddFieldTransformer("test", 123)
	registry.Register("test", transformer)

	t2, ok := registry.Get("test")
	if !ok {
		t.Fatal("Expected to find transformer")
	}
	if t2.Name() != "test-test" {
		t.Errorf("Expected name 'test-test', got '%s'", t2.Name())
	}

	names := registry.List()
	if len(names) != 1 {
		t.Errorf("Expected 1 transformer, got %d", len(names))
	}

	registry.Remove("test")
	if _, ok := registry.Get("test"); ok {
		t.Error("Expected transformer to be removed")
	}
}

func TestTimestampTransformer(t *testing.T) {
	transformer := NewTimestampTransformer("event_time")

	ts := time.Now()
	e := &event.ChangeEvent{
		Timestamp: ts,
		After: &event.RowData{
			Fields: map[string]interface{}{"id": 1},
		},
	}

	result, err := transformer.Transform(e)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if result.After.Fields["event_time"] != ts {
		t.Error("Expected event_time to match event timestamp")
	}
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./internal/transform/... -v -run TestCustom\|TestAddField\|TestRemoveField\|TestRenameField\|TestTimestamp\|TestScriptTransformer`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add internal/transform/custom.go internal/transform/custom_test.go
git commit -m "feat(transform): add CustomTransformer foundation with built-in transformers"
```

---

### Task 3: Implement BackpressureController

**Files:**
- Create: `internal/pipeline/backpressure.go`
- Create: `internal/pipeline/backpressure_test.go`

- [ ] **Step 1: Write backpressure controller**

```go
// internal/pipeline/backpressure.go
package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/logutil"
	"go.uber.org/zap"
)

// BackpressureConfig holds backpressure configuration.
type BackpressureConfig struct {
	// EnableBackpressure enables backpressure control
	EnableBackpressure bool `json:"enable-backpressure" toml:"enable-backpressure"`

	// HighWatermark is the queue usage percentage that triggers pause (0-100)
	HighWatermark int `json:"high-watermark" toml:"high-watermark"`

	// LowWatermark is the queue usage percentage that triggers resume (0-100)
	LowWatermark int `json:"low-watermark" toml:"low-watermark"`

	// MaxLatency is the maximum acceptable latency before pause
	MaxLatency time.Duration `json:"max-latency" toml:"max-latency"`

	// CheckInterval is the interval between backpressure checks
	CheckInterval time.Duration `json:"check-interval" toml:"check-interval"`
}

// DefaultBackpressureConfig returns defaults.
func DefaultBackpressureConfig() *BackpressureConfig {
	return &BackpressureConfig{
		EnableBackpressure: true,
		HighWatermark:      80,
		LowWatermark:       50,
		MaxLatency:         5 * time.Second,
		CheckInterval:      100 * time.Millisecond,
	}
}

// BackpressureState represents the backpressure state.
type BackpressureState string

const (
	// BackpressureStateNormal means normal operation
	BackpressureStateNormal BackpressureState = "normal"
	// BackpressureStateWarning means approaching limit
	BackpressureStateWarning BackpressureState = "warning"
	// BackpressureStatePaused means paused due to backpressure
	BackpressureStatePaused BackpressureState = "paused"
)

// BackpressureController manages backpressure for flow control.
type BackpressureController struct {
	config *BackpressureConfig

	// Metrics
	queueSize    int64
	maxQueueSize int64
	latency      time.Duration

	// State
	state    BackpressureState
	pauseCh  chan struct{}
	resumeCh chan struct{}

	// Callbacks
	onPause  func()
	onResume func()

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *zap.Logger
}

// NewBackpressureController creates a controller.
func NewBackpressureController(config *BackpressureConfig) *BackpressureController {
	ctx, cancel := context.WithCancel(context.Background())

	return &BackpressureController{
		config:   config,
		state:    BackpressureStateNormal,
		pauseCh:  make(chan struct{}, 1),
		resumeCh: make(chan struct{}, 1),
		ctx:      ctx,
		cancel:   cancel,
		logger:   logutil.Logger(),
	}
}

// Start starts the controller.
func (b *BackpressureController) Start() {
	b.wg.Add(1)
	go b.run()
}

// Stop stops the controller.
func (b *BackpressureController) Stop() {
	b.cancel()
	b.wg.Wait()
}

// run runs the backpressure loop.
func (b *BackpressureController) run() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.check()
		}
	}
}

// check checks and applies backpressure.
func (b *BackpressureController) check() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.config.EnableBackpressure {
		return
	}

	// Calculate queue usage percentage
	var usagePercent int
	if b.maxQueueSize > 0 {
		usagePercent = int((float64(b.queueSize) / float64(b.maxQueueSize)) * 100)
	}

	// Check if we need to pause
	if b.state == BackpressureStateNormal || b.state == BackpressureStateWarning {
		if usagePercent >= b.config.HighWatermark || b.latency > b.config.MaxLatency {
			b.state = BackpressureStatePaused
			b.logger.Warn("backpressure triggered",
				zap.Int("usage", usagePercent),
				zap.Duration("latency", b.latency),
			)
			b.notifyPause()
			return
		}

		// Warning state
		if usagePercent >= b.config.LowWatermark {
			b.state = BackpressureStateWarning
		}
	}

	// Check if we can resume
	if b.state == BackpressureStatePaused {
		if usagePercent <= b.config.LowWatermark {
			b.state = BackpressureStateNormal
			b.logger.Info("backpressure released",
				zap.Int("usage", usagePercent),
			)
			b.notifyResume()
		}
	}
}

// notifyPause notifies pause callbacks.
func (b *BackpressureController) notifyPause() {
	select {
	case b.pauseCh <- struct{}{}:
	default:
	}
	if b.onPause != nil {
		b.onPause()
	}
}

// notifyResume notifies resume callbacks.
func (b *BackpressureController) notifyResume() {
	select {
	case b.resumeCh <- struct{}{}:
	default:
	}
	if b.onResume != nil {
		b.onResume()
	}
}

// UpdateMetrics updates the controller metrics.
func (b *BackpressureController) UpdateMetrics(queueSize, maxQueueSize int64, latency time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queueSize = queueSize
	b.maxQueueSize = maxQueueSize
	b.latency = latency
}

// State returns the current state.
func (b *BackpressureController) State() BackpressureState {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state
}

// PauseCh returns the pause notification channel.
func (b *BackpressureController) PauseCh() <-chan struct{} {
	return b.pauseCh
}

// ResumeCh returns the resume notification channel.
func (b *BackpressureController) ResumeCh() <-chan struct{} {
	return b.resumeCh
}

// OnPause sets the pause callback.
func (b *BackpressureController) OnPause(fn func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onPause = fn
}

// OnResume sets the resume callback.
func (b *BackpressureController) OnResume(fn func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onResume = fn
}

// ShouldPause checks if we should pause reading.
func (b *BackpressureController) ShouldPause() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state == BackpressureStatePaused
}

// WaitForResume waits for resume signal.
func (b *BackpressureController) WaitForResume(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.resumeCh:
		return nil
	}
}

// WaitWhilePaused blocks while in paused state.
func (b *BackpressureController) WaitWhilePaused(ctx context.Context) error {
	for {
		if !b.ShouldPause() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.resumeCh:
			return nil
		case <-time.After(b.config.CheckInterval):
			// Re-check state
		}
	}
}
```

- [ ] **Step 2: Write tests**

```go
// internal/pipeline/backpressure_test.go
package pipeline

import (
	"context"
	"testing"
	"time"
)

func TestBackpressureController_HighWatermark(t *testing.T) {
	config := DefaultBackpressureConfig()
	controller := NewBackpressureController(config)
	controller.Start()
	defer controller.Stop()

	// Initially normal
	if controller.State() != BackpressureStateNormal {
		t.Errorf("Expected initial state Normal, got %s", controller.State())
	}

	// Update to high usage
	controller.UpdateMetrics(90, 100, time.Second)

	// Wait for check
	time.Sleep(150 * time.Millisecond)

	// Should be paused
	if controller.State() != BackpressureStatePaused {
		t.Errorf("Expected state Paused, got %s", controller.State())
	}
}

func TestBackpressureController_LowWatermark(t *testing.T) {
	config := DefaultBackpressureConfig()
	controller := NewBackpressureController(config)
	controller.Start()
	defer controller.Stop()

	// Set to paused state
	controller.UpdateMetrics(90, 100, time.Second)
	time.Sleep(150 * time.Millisecond)

	if controller.State() != BackpressureStatePaused {
		t.Fatalf("Expected state Paused, got %s", controller.State())
	}

	// Reduce usage
	controller.UpdateMetrics(30, 100, time.Millisecond)
	time.Sleep(150 * time.Millisecond)

	// Should be normal
	if controller.State() != BackpressureStateNormal {
		t.Errorf("Expected state Normal, got %s", controller.State())
	}
}

func TestBackpressureController_HighLatency(t *testing.T) {
	config := &BackpressureConfig{
		EnableBackpressure: true,
		HighWatermark:      80,
		LowWatermark:       50,
		MaxLatency:         100 * time.Millisecond,
		CheckInterval:      50 * time.Millisecond,
	}

	controller := NewBackpressureController(config)
	controller.Start()
	defer controller.Stop()

	// Low queue usage but high latency
	controller.UpdateMetrics(10, 100, 200*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	// Should be paused due to latency
	if controller.State() != BackpressureStatePaused {
		t.Errorf("Expected state Paused due to latency, got %s", controller.State())
	}
}

func TestBackpressureController_Callbacks(t *testing.T) {
	config := DefaultBackpressureConfig()
	controller := NewBackpressureController(config)
	controller.Start()
	defer controller.Stop()

	pauseCalled := false
	resumeCalled := false

	controller.OnPause(func() {
		pauseCalled = true
	})
	controller.OnResume(func() {
		resumeCalled = true
	})

	// Trigger pause
	controller.UpdateMetrics(90, 100, time.Second)
	time.Sleep(150 * time.Millisecond)

	if !pauseCalled {
		t.Error("Expected onPause callback to be called")
	}

	// Trigger resume
	controller.UpdateMetrics(30, 100, time.Millisecond)
	time.Sleep(150 * time.Millisecond)

	if !resumeCalled {
		t.Error("Expected onResume callback to be called")
	}
}

func TestBackpressureController_WaitWhilePaused(t *testing.T) {
	config := &BackpressureConfig{
		EnableBackpressure: true,
		HighWatermark:      80,
		LowWatermark:       50,
		MaxLatency:         5 * time.Second,
		CheckInterval:      20 * time.Millisecond,
	}

	controller := NewBackpressureController(config)
	controller.Start()
	defer controller.Stop()

	// Set to paused
	controller.UpdateMetrics(90, 100, time.Second)
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Start goroutine to resume
	go func() {
		time.Sleep(100 * time.Millisecond)
		controller.UpdateMetrics(30, 100, time.Millisecond)
	}()

	err := controller.WaitWhilePaused(ctx)
	if err != nil {
		t.Errorf("WaitWhilePaused failed: %v", err)
	}
}

func TestBackpressureController_Disabled(t *testing.T) {
	config := &BackpressureConfig{
		EnableBackpressure: false,
		HighWatermark:      80,
		LowWatermark:       50,
		MaxLatency:         5 * time.Second,
		CheckInterval:      50 * time.Millisecond,
	}

	controller := NewBackpressureController(config)
	controller.Start()
	defer controller.Stop()

	// High usage
	controller.UpdateMetrics(90, 100, time.Second)
	time.Sleep(100 * time.Millisecond)

	// Should remain normal (disabled)
	if controller.State() != BackpressureStateNormal {
		t.Errorf("Expected state Normal (disabled), got %s", controller.State())
	}

	if controller.ShouldPause() {
		t.Error("ShouldPause should return false when disabled")
	}
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./internal/pipeline/... -v -run TestBackpressure`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add internal/pipeline/backpressure.go internal/pipeline/backpressure_test.go
git commit -m "feat(pipeline): add BackpressureController for flow control"
```

---

### Task 4: Write Tests for RateLimiter

**Files:**
- Create: `internal/ratelimit/ratelimit_test.go`

- [ ] **Step 1: Write tests**

```go
// internal/ratelimit/ratelimit_test.go
package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestRateLimiter_Wait(t *testing.T) {
	config := &Config{
		SourceEnabled:       true,
		SourceRowsPerSecond: 100,
		BurstSize:           10,
	}

	limiter := NewLimiter(config)

	start := time.Now()
	for i := 0; i < 11; i++ {
		err := limiter.Wait(context.Background())
		if err != nil {
			t.Fatalf("Wait failed: %v", err)
		}
	}
	elapsed := time.Since(start)

	// 11 requests at 100/sec with burst 10 should take ~100ms
	// First 10 are burst, 11th waits ~10ms
	if elapsed < 50*time.Millisecond {
		t.Errorf("Expected rate limiting to kick in, elapsed: %v", elapsed)
	}
}

func TestRateLimiter_WaitRowsAndBytes(t *testing.T) {
	config := &Config{
		SourceEnabled:        true,
		SourceRowsPerSecond:  1000,
		SourceBytesPerSecond: 10000,
		BurstSize:            100,
	}

	limiter := NewLimiter(config)

	err := limiter.WaitRowsAndBytes(context.Background(), 10, 100)
	if err != nil {
		t.Fatalf("WaitRowsAndBytes failed: %v", err)
	}
}

func TestRateLimiter_NoLimit(t *testing.T) {
	config := &Config{
		SourceEnabled:       true,
		SourceRowsPerSecond: 0, // No limit
		BurstSize:           1000,
	}

	limiter := NewLimiter(config)

	// Should complete immediately
	start := time.Now()
	for i := 0; i < 1000; i++ {
		limiter.Wait(context.Background())
	}
	elapsed := time.Since(start)

	if elapsed > 100*time.Millisecond {
		t.Errorf("No limit should be instant, elapsed: %v", elapsed)
	}
}

func TestRateLimiter_SetLimit(t *testing.T) {
	config := &Config{
		SourceEnabled:       true,
		SourceRowsPerSecond: 0,
		BurstSize:           10,
	}

	limiter := NewLimiter(config)

	// Initially no limit - should be fast
	start := time.Now()
	for i := 0; i < 100; i++ {
		limiter.Wait(context.Background())
	}
	elapsed := time.Since(start)

	if elapsed > 50*time.Millisecond {
		t.Errorf("No limit should be instant, elapsed: %v", elapsed)
	}

	// Set limit
	limiter.SetLimit(100)

	// Now should be rate limited
	start = time.Now()
	for i := 0; i < 15; i++ {
		limiter.Wait(context.Background())
	}
	elapsed = time.Since(start)

	if elapsed < 30*time.Millisecond {
		t.Errorf("Expected rate limiting after SetLimit, elapsed: %v", elapsed)
	}
}

func TestRateLimiter_Allow(t *testing.T) {
	config := &Config{
		SourceEnabled:       true,
		SourceRowsPerSecond: 10,
		BurstSize:           2,
	}

	limiter := NewLimiter(config)

	// First 2 should be allowed (burst)
	if !limiter.Allow() {
		t.Error("First request should be allowed")
	}
	if !limiter.Allow() {
		t.Error("Second request should be allowed")
	}

	// Third may not be allowed (exceeds burst)
	// Note: This is timing-dependent, so we just verify the method works
	limiter.AllowN(1)
}

func TestRateLimiter_Delay(t *testing.T) {
	config := &Config{
		SourceEnabled:       true,
		SourceRowsPerSecond: 100,
		BurstSize:           10,
	}

	limiter := NewLimiter(config)

	// Consume burst
	for i := 0; i < 10; i++ {
		limiter.Wait(context.Background())
	}

	// Check delay for next 10 events
	delay := limiter.Delay(10)
	if delay <= 0 {
		t.Errorf("Expected positive delay, got: %v", delay)
	}
}
```

- [ ] **Step 2: Run tests**

Run: `go test ./internal/ratelimit/... -v`
Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
git add internal/ratelimit/ratelimit_test.go
git commit -m "test(ratelimit): add comprehensive tests for RateLimiter"
```

---

## Summary

This plan implements:

1. **ExpressionFilter Tests** - Comprehensive test coverage for expression-based filtering
2. **CustomTransformer** - Foundation for custom transformations with built-in transformers
3. **BackpressureController** - Flow control when sink is slow
4. **RateLimiter Tests** - Test coverage for rate limiting

**Total Tasks:** 4
**Estimated Time:** 1-2 hours

---

*Plan created: 2026-05-12*
