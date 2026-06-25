# Reviewer 审查问题修复实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复 Reviewer 审查发现的全部问题，使实现与设计文档完全对齐。

**Architecture:** 按优先级分 4 个 Phase：P0 运行时 bug → P1 正确性 → P2 架构对齐 → P3 功能补全 + 文档对齐。每个 Task 使用 TDD（先写失败测试，再实现）。

**Tech Stack:** Go 1.22+, gorilla/mux, etcd client v3, pingcap/errors

---

## Phase 0: 运行时 Bug（M4, A1）

### Task 1: Pipeline.Stop() double-close 防护 (M4)

**Files:**
- Modify: `internal/pipeline/pipeline.go:19-40` (Pipeline struct) + `:294-327` (Stop method)
- Modify: `internal/pipeline/pipeline_test.go`

- [ ] **Step 1: Write failing test**

```go
func TestPipelineStopDoubleCallNoPanic(t *testing.T) {
	p := newTestPipeline(t)
	ctx := context.Background()
	require.NoError(t, p.Start(ctx))

	// Both calls must succeed without panic
	require.NoError(t, p.Stop(ctx))
	require.NoError(t, p.Stop(ctx))
}
```

- [ ] **Step 2: Run test, verify it panics**

Run: `go test ./internal/pipeline/ -run TestPipelineStopDoubleCallNoPanic -count=1 -v`
Expected: panic: close of closed channel

- [ ] **Step 3: Implement fix — add `sync.Once` to Pipeline struct**

In `pipeline.go`, add `stopOnce sync.Once` to Pipeline struct, wrap `close(p.stopCh)` with it:

```go
type Pipeline struct {
	// ... existing fields ...
	stopOnce    sync.Once
}

func (p *Pipeline) Stop(ctx context.Context) error {
	p.mu.Lock()
	if p.status.State == StateStopped {
		p.mu.Unlock()
		return nil
	}
	p.status.State = StateStopping
	p.mu.Unlock()

	log.Info("stopping pipeline", zap.String("id", p.id))
	p.stopOnce.Do(func() { close(p.stopCh) })
	p.wg.Wait()
	// ... rest unchanged ...
}
```

- [ ] **Step 4: Run test, verify pass**

Run: `go test ./internal/pipeline/ -run TestPipelineStopDoubleCallNoPanic -count=1 -v`
Expected: PASS

- [ ] **Step 5: Run all pipeline tests**

Run: `go test ./internal/pipeline/ -count=1`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add internal/pipeline/pipeline.go internal/pipeline/pipeline_test.go
git commit -m "fix(pipeline): prevent double-close panic in Stop() with sync.Once"
```

---

### Task 2: Pipeline.Pause() 真暂停 (A1)

**Files:**
- Modify: `internal/pipeline/pipeline.go:19-40` (struct) + `:329-355` (Pause/Resume) + `:374-403` (run loop)
- Modify: `internal/pipeline/pipeline_test.go`

- [ ] **Step 1: Write failing test**

```go
func TestPipelinePauseStopsProcessing(t *testing.T) {
	p := newTestPipeline(t)
	ctx := context.Background()
	require.NoError(t, p.Start(ctx))

	// Send an event, verify it's processed
	sendTestEvent(t, p)
	waitForEventsWritten(t, p, 1)

	// Pause
	require.NoError(t, p.Pause(ctx))

	// Send another event while paused
	beforeCount := p.Status().Statistics.EventsWritten
	sendTestEvent(t, p)
	time.Sleep(200 * time.Millisecond)

	// Event should NOT be processed while paused
	afterCount := p.Status().Statistics.EventsWritten
	assert.Equal(t, beforeCount, afterCount, "events should not be processed while paused")

	// Resume
	require.NoError(t, p.Resume(ctx))

	// Now event should be processed
	waitForEventsWritten(t, p, int(beforeCount)+1)

	require.NoError(t, p.Stop(ctx))
}
```

- [ ] **Step 2: Run test, verify fail**

Run: `go test ./internal/pipeline/ -run TestPipelinePauseStopsProcessing -count=1 -v`
Expected: FAIL — event count increases even while paused

- [ ] **Step 3: Implement — add pauseCh to run loop**

Add `pauseCh chan struct{}` and `resumeCh chan struct{}` to Pipeline struct. In `Pause()`, close `pauseCh`. In `Resume()`, close `resumeCh` and create new `pauseCh`. In `run()`, check paused state before processing:

```go
type Pipeline struct {
	// ... existing ...
	pauseCh  chan struct{}
	resumeCh chan struct{}
}

// In NewPipeline or Start:
p.pauseCh = make(chan struct{})
p.resumeCh = make(chan struct{})

func (p *Pipeline) Pause(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.status.State != StateRunning {
		return ErrInvalidState
	}
	p.status.State = StatePaused
	close(p.pauseCh)
	log.Info("pipeline paused", zap.String("id", p.id))
	return nil
}

func (p *Pipeline) Resume(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.status.State != StatePaused {
		return ErrInvalidState
	}
	p.status.State = StateRunning
	p.pauseCh = make(chan struct{})
	close(p.resumeCh)
	p.resumeCh = make(chan struct{})
	log.Info("pipeline resumed", zap.String("id", p.id))
	return nil
}

// In run(), after reading from source.Events():
case e, ok := <-p.source.Events():
	if !ok {
		return
	}
	// Block while paused
	p.mu.RLock()
	paused := p.status.State == StatePaused
	pauseCh := p.pauseCh
	p.mu.RUnlock()
	if paused {
		select {
		case <-p.resumeCh:
			// resumed
		case <-p.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
	p.instrumentEvent(e)
	p.processEvent(ctx, e)
```

- [ ] **Step 4: Run test, verify pass**

Run: `go test ./internal/pipeline/ -run TestPipelinePauseStopsProcessing -count=1 -v`
Expected: PASS

- [ ] **Step 5: Run all pipeline tests**

Run: `go test ./internal/pipeline/ -count=1`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add internal/pipeline/pipeline.go internal/pipeline/pipeline_test.go
git commit -m "fix(pipeline): implement real Pause/Resume with channel-based blocking in run loop"
```

---

## Phase 1: 正确性 (M3, M5, A3)

### Task 3: EventsWritten 仅在成功时递增 (M3)

**Files:**
- Modify: `internal/pipeline/pipeline.go:405-445` (processEvent)
- Modify: `internal/pipeline/pipeline_test.go`

- [ ] **Step 1: Write failing test**

```go
func TestEventsWrittenNotIncrementedOnError(t *testing.T) {
	p := newTestPipelineWithFailingSink(t) // sink.Write returns error
	ctx := context.Background()
	require.NoError(t, p.Start(ctx))

	sendTestEvent(t, p)
	time.Sleep(200 * time.Millisecond)

	stats := p.Status().Statistics
	assert.Equal(t, int64(0), stats.EventsWritten, "should not increment on write failure")
	assert.Equal(t, int64(1), stats.EventsFailed, "should increment EventsFailed")

	require.NoError(t, p.Stop(ctx))
}
```

- [ ] **Step 2: Run test, verify fail**

Expected: EventsWritten == 1 (currently incremented unconditionally)

- [ ] **Step 3: Fix processEvent — track success**

```go
func (p *Pipeline) processEvent(ctx context.Context, e *event.ChangeEvent) {
	// ... existing stats update ...

	var writeErr error
	if p.dispatcher != nil {
		writeErr = p.dispatcher.Dispatch(ctx, e, p.sinks)
	} else {
		for _, s := range p.sinks {
			if err := s.Write(ctx, []*event.ChangeEvent{e}); err != nil {
				log.Error("failed to write to sink", zap.String("sink", s.Name()), zap.Error(err))
				writeErr = err
			}
		}
	}

	p.mu.Lock()
	if writeErr != nil {
		p.status.Statistics.EventsFailed++
	} else {
		p.status.Statistics.EventsWritten++
	}
	p.mu.Unlock()

	// Update latency metric
	latency := time.Since(startTime).Seconds()
	metrics.TaskLatencySeconds.WithLabelValues(p.cluster, p.id).Observe(latency)
}
```

- [ ] **Step 4: Verify pass + no regression**

Run: `go test ./internal/pipeline/ -count=1`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add internal/pipeline/pipeline.go internal/pipeline/pipeline_test.go
git commit -m "fix(pipeline): only increment EventsWritten on successful sink write"
```

---

### Task 4: setTaskPosition API 实现 (M5)

**Files:**
- Modify: `internal/api/server.go:318-349`
- Modify: `internal/api/server_test.go`

- [ ] **Step 1: Write failing test**

```go
func TestSetTaskPositionActuallyUpdates(t *testing.T) {
	srv, mgr := newTestServerWithTask(t, "task-1")
	body := `{"binlogFile":"mysql-bin.000003","binlogPos":1234}`
	req := httptest.NewRequest("PUT", "/api/v1/tasks/task-1/position", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Verify position was actually set
	task, _ := mgr.Get("task-1")
	pos := task.GetPosition()
	assert.NotNil(t, pos)
	assert.Equal(t, "mysql-bin.000003", pos.BinlogFile)
	assert.Equal(t, uint32(1234), pos.BinlogPos)
}
```

- [ ] **Step 2: Run test, verify fail**

Expected: pos is nil (position never set)

- [ ] **Step 3: Replace stub with actual SetPosition call**

```go
func (s *Server) setTaskPosition(w http.ResponseWriter, r *http.Request) {
	// ... existing validation ...

	pos := &event.Position{
		BinlogFile: posReq.BinlogFile,
		BinlogPos:  posReq.BinlogPos,
		LSN:        posReq.LSN,
		TxID:       posReq.TxID,
		SeqNo:      posReq.SeqNo,
	}
	task.SetPosition(pos)
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}
```

- [ ] **Step 4: Verify pass**

Run: `go test ./internal/api/ -count=1`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add internal/api/server.go internal/api/server_test.go
git commit -m "fix(api): implement setTaskPosition to actually update task position"
```

---

### Task 5: etcd Election 实例缓存 (A3)

**Files:**
- Modify: `internal/coordinator/etcd.go:20-28` (struct) + `:221-262` (Acquire/Release)
- Modify: `internal/coordinator/coordinator_test.go`

- [ ] **Step 1: Write failing test**

```go
func TestAcquireAndReleaseUseSameElection(t *testing.T) {
	// This test verifies that Release uses the same election instance as Acquire.
	// With the bug, Release creates a new Election that hasn't campaigned,
	// so Resign is a no-op and the leader key remains in etcd.
	coord := newTestEtcdCoordinator(t)
	ctx := context.Background()

	ok, err := coord.AcquireLeadership(ctx, "task-1")
	require.NoError(t, err)
	require.True(t, ok)

	// Release should actually remove leadership
	require.NoError(t, coord.ReleaseLeadership(ctx, "task-1"))

	// Another node should be able to acquire now
	isLeader, err := coord.IsLeader(ctx, "task-1")
	require.NoError(t, err)
	assert.False(t, isLeader)
}
```

- [ ] **Step 2: Run test, verify fail**

Expected: isLeader is still true (Resign didn't work because it used a different Election instance)

- [ ] **Step 3: Add elections map to EtcdCoordinator**

```go
type EtcdCoordinator struct {
	// ... existing ...
	elections map[string]*concurrency.Election // taskID -> election instance
}

func (c *EtcdCoordinator) AcquireLeadership(ctx context.Context, taskID string) (bool, error) {
	if c.session == nil {
		return false, fmt.Errorf("session not initialized")
	}
	key := c.leadershipKey(taskID)
	election := concurrency.NewElection(c.session, key)
	err := election.Campaign(ctx, c.nodeID)
	if err != nil {
		if err == context.DeadlineExceeded || err == context.Canceled {
			return false, nil
		}
		return false, fmt.Errorf("campaign failed: %w", err)
	}
	c.mu.Lock()
	c.elections[taskID] = election
	c.mu.Unlock()
	return true, nil
}

func (c *EtcdCoordinator) ReleaseLeadership(ctx context.Context, taskID string) error {
	c.mu.Lock()
	election, ok := c.elections[taskID]
	if ok {
		delete(c.elections, taskID)
	}
	c.mu.Unlock()

	if !ok || election == nil {
		return nil
	}
	if err := election.Resign(ctx); err != nil {
		return fmt.Errorf("resign failed: %w", err)
	}
	return nil
}
```

Initialize `elections` map in constructor.

- [ ] **Step 4: Verify pass**

Run: `go test ./internal/coordinator/ -count=1`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add internal/coordinator/etcd.go internal/coordinator/coordinator_test.go
git commit -m "fix(coordinator): cache Election instances to ensure Resign matches Campaign"
```

---

## Phase 2: 架构对齐 (M1, M2, S1b)

### Task 6: Source 接口补 Schemas() (M1)

**Files:**
- Modify: `internal/source/connector.go` (interface)
- Modify: 5 connector files (PG, MongoDB, Oracle, SQLServer, MariaDB) — add no-op `Schemas()`
- Test: verify interface compliance

- [ ] **Step 1: Write failing test**

```go
// In a new file internal/source/interface_test.go
func TestAllConnectorsImplementSchemas(t *testing.T) {
	// Compile-time interface check
	var _ source.Connector = (*mysql.Connector)(nil)
	var _ source.Connector = (*postgres.Connector)(nil)
	// ... etc for all 6
}
```

- [ ] **Step 2: Add `Schemas() map[string]*event.TableInfo` to interface**

In `internal/source/connector.go`, add after `GetSchema`:

```go
// Schemas returns all cached table schemas.
Schemas() map[string]*event.TableInfo
```

- [ ] **Step 3: Implement no-op in connectors that lack it**

For PG, MongoDB, Oracle, SQLServer, MariaDB:

```go
func (c *Connector) Schemas() map[string]*event.TableInfo {
	return make(map[string]*event.TableInfo)
}
```

MySQL already has it at `connector.go:290`.

- [ ] **Step 4: Verify build + test**

Run: `go build ./... && go test ./internal/source/... -count=1`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add internal/source/connector.go internal/source/*/connector.go
git commit -m "feat(source): add Schemas() to Connector interface per design doc"
```

---

### Task 7: Sink 接口补 ApplyDDL (M2)

**Files:**
- Modify: `internal/sink/connector.go` (interface)
- Modify: 6 sink connector files — add `ApplyDDL`
- Modify: `internal/sink/decorator/sink.go` — proxy `ApplyDDL`

- [ ] **Step 1: Write failing test**

```go
func TestSinkConnectorImplementsApplyDDL(t *testing.T) {
	var c sink.Connector
	_ = c // interface should have ApplyDDL
}
```

- [ ] **Step 2: Add `ApplyDDL` to Sink Connector interface**

```go
// ApplyDDL applies a DDL event to the sink.
ApplyDDL(ctx context.Context, ddl *event.ChangeEvent) error
```

- [ ] **Step 3: Implement in each sink**

For MySQL and PostgreSQL (support DDL):
```go
func (c *Connector) ApplyDDL(ctx context.Context, ddl *event.ChangeEvent) error {
	sql, ok := ddl.Metadata["sql"]
	if !ok || sql == "" {
		return nil
	}
	_, err := c.db.ExecContext(ctx, sql)
	return err
}
```

For Kafka, MongoDB, ES, Redis (no DDL support):
```go
func (c *Connector) ApplyDDL(ctx context.Context, ddl *event.ChangeEvent) error {
	return nil // DDL not applicable
}
```

For MetricsSink decorator:
```go
func (m *MetricsSink) ApplyDDL(ctx context.Context, ddl *event.ChangeEvent) error {
	return m.inner.ApplyDDL(ctx, ddl)
}
```

- [ ] **Step 4: Verify build + all sink tests**

Run: `go build ./... && go test ./internal/sink/... -count=1`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add internal/sink/connector.go internal/sink/*/connector.go internal/sink/decorator/sink.go
git commit -m "feat(sink): add ApplyDDL to Connector interface per design doc"
```

---

### Task 8: API 响应格式标准化 (S1b)

**Files:**
- Modify: `internal/api/server.go` (writeJSON/writeError)
- Modify: `internal/api/server_test.go`
- Modify: `internal/api/errors.go`

- [ ] **Step 1: Write failing test**

```go
func TestAPIResponseEnvelopeFormat(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	var resp struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, 0, resp.Code)
	assert.Equal(t, "success", resp.Message)
}

func TestAPIErrorResponseEnvelopeFormat(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest("GET", "/api/v1/tasks/nonexistent", nil)
	w := httptest.NewRecorder()
	srv.router.ServeHTTP(w, req)

	var resp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.NotEqual(t, 0, resp.Code)
	assert.NotEmpty(t, resp.Message)
}
```

- [ ] **Step 2: Run test, verify fail**

Expected: response doesn't have `code`/`message`/`data` envelope structure

- [ ] **Step 3: Implement standard response envelope**

```go
type apiResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func (s *Server) writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(apiResponse{
		Code:    0,
		Message: "success",
		Data:    data,
	})
}

func (s *Server) writeError(w http.ResponseWriter, statusCode int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(apiResponse{
		Code:    statusCode,
		Message: msg,
	})
}
```

- [ ] **Step 4: Fix all existing tests to expect new format, verify pass**

Run: `go test ./internal/api/ -count=1`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add internal/api/server.go internal/api/server_test.go internal/api/errors.go
git commit -m "feat(api): standardize response format to {code, message, data} envelope"
```

---

## Phase 3: 功能补全 + 文档对齐

### Task 9: SnapshotMode when_needed (S7)

**Files:**
- Modify: `internal/source/connector.go` — add `SnapshotModeWhenNeeded`
- Modify: `internal/source/snapshot_coordinator.go` — handle `when_needed` logic
- Test: `internal/source/snapshot_config_test.go`

- [ ] **Step 1: Write failing test**

```go
func TestSnapshotModeWhenNeeded(t *testing.T) {
	cfg := &SnapshotConcurrencyConfig{/* ... */}
	cfg.Mode = SnapshotModeWhenNeeded

	// When no saved position exists, should snapshot
	assert.True(t, cfg.ShouldSnapshot(nil))

	// When position exists, should NOT snapshot
	pos := &event.Position{BinlogFile: "mysql-bin.000001"}
	assert.False(t, cfg.ShouldSnapshot(pos))
}
```

- [ ] **Step 2: Verify fail**

- [ ] **Step 3: Add `SnapshotModeWhenNeeded` and implement `ShouldSnapshot`**

```go
SnapshotModeWhenNeeded SnapshotMode = "when_needed"

func (c *SnapshotConfig) ShouldSnapshot(savedPos *event.Position) bool {
	switch c.Mode {
	case SnapshotModeNever:
		return false
	case SnapshotModeAlways:
		return true
	case SnapshotModeInitial:
		return savedPos == nil
	case SnapshotModeWhenNeeded:
		return savedPos == nil
	default:
		return false
	}
}
```

- [ ] **Step 4: Verify pass**
- [ ] **Step 5: Commit**

```bash
git commit -m "feat(source): add when_needed snapshot mode per design doc"
```

---

### Task 10: 更新设计文档 — HTTP 框架 (S1a)

**Files:**
- Modify: `docs/design/api-cli-design.md`

- [ ] **Step 1: Replace all Gin references with gorilla/mux**

Replace `Gin/Echo 框架` → `gorilla/mux`, `github.com/gin-gonic/gin` → `github.com/gorilla/mux`, `gin.Engine` → `mux.Router`, etc.

- [ ] **Step 2: Commit**

```bash
git add docs/design/api-cli-design.md
git commit -m "docs(api): update design doc to reflect gorilla/mux (was Gin)"
```

---

### Task 11: 更新设计文档 — 删除 crypto/sync utils (S6)

**Files:**
- Modify: `docs/design/core-design.md`

- [ ] **Step 1: Remove references to crypto.go and sync.go from core-design.md**

- [ ] **Step 2: Commit**

```bash
git add docs/design/core-design.md
git commit -m "docs(core): remove unreferenced crypto.go/sync.go from design doc"
```

---

### Task 12: Error Handling Phase 1 — ErrorClassification + Severity (S4)

**Files:**
- Create: `pkg/errors/classification.go`
- Create: `pkg/errors/classification_test.go`
- Modify: `pkg/errors/errors.go` — integrate with existing RFC codes

- [ ] **Step 1: Write failing test**

```go
func TestDataStreamErrorClassification(t *testing.T) {
	err := NewDataStreamError(
		ErrConnectionFailed,
		SeverityRecoverable,
		CategoryConnection,
		"connection refused",
	)
	assert.Equal(t, SeverityRecoverable, err.Severity)
	assert.Equal(t, CategoryConnection, err.Category)
	assert.True(t, err.IsRecoverable())
	assert.False(t, err.IsFatal())
}

func TestClassifyError(t *testing.T) {
	// Network timeout -> recoverable
	err := classifyError(context.DeadlineExceeded)
	assert.Equal(t, SeverityRecoverable, err.Severity)
	assert.Equal(t, CategoryNetwork, err.Category)
}
```

- [ ] **Step 2: Verify fail**

- [ ] **Step 3: Implement DataStreamError, Severity, Category, ClassifyError**

Implement the types from `error-handling-design.md:48-120` in `pkg/errors/classification.go`.

- [ ] **Step 4: Verify pass**
- [ ] **Step 5: Commit**

```bash
git commit -m "feat(errors): add DataStreamError classification with Severity and Category"
```

---

### Task 13: Error Handling Phase 2 — Circuit Breaker (S4)

**Files:**
- Create: `pkg/errors/circuit_breaker.go`
- Create: `pkg/errors/circuit_breaker_test.go`

- [ ] **Step 1: Write failing test**

```go
func TestCircuitBreakerTripsAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		ResetTimeout:     time.Second,
	})

	// First 3 failures should be allowed
	for i := 0; i < 3; i++ {
		assert.True(t, cb.Allow())
		cb.RecordFailure()
	}

	// 4th call should be blocked (circuit open)
	assert.False(t, cb.Allow())
}

func TestCircuitBreakerResetsAfterTimeout(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 2,
		ResetTimeout:     100 * time.Millisecond,
	})

	cb.RecordFailure()
	cb.RecordFailure()
	assert.False(t, cb.Allow()) // open

	time.Sleep(150 * time.Millisecond)
	assert.True(t, cb.Allow()) // half-open

	cb.RecordSuccess()
	assert.True(t, cb.Allow()) // closed
}
```

- [ ] **Step 2: Verify fail**

- [ ] **Step 3: Implement CircuitBreaker per design doc**

States: Closed → Open → HalfOpen → Closed. Implement from `error-handling-design.md`.

- [ ] **Step 4: Verify pass**
- [ ] **Step 5: Commit**

```bash
git commit -m "feat(errors): add CircuitBreaker for fault tolerance"
```

---

### Task 14: Error Handling Phase 3 — Alerter interface (S4)

**Files:**
- Create: `pkg/errors/alerter.go`
- Create: `pkg/errors/alerter_test.go`

- [ ] **Step 1: Write failing test**

```go
func TestLogAlerterSendsAlert(t *testing.T) {
	var buf bytes.Buffer
	alerter := NewLogAlerter(&buf)
	err := alerter.Alert(context.Background(), &Alert{
		Level:   AlertLevelCritical,
		Title:   "Connection Lost",
		Message: "MySQL source connection failed",
	})
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "Connection Lost")
}
```

- [ ] **Step 2: Verify fail**

- [ ] **Step 3: Implement Alerter interface + LogAlerter**

```go
type AlertLevel string
const (
	AlertLevelWarning  AlertLevel = "warning"
	AlertLevelCritical AlertLevel = "critical"
	AlertLevelFatal    AlertLevel = "fatal"
)

type Alert struct {
	Level   AlertLevel
	Title   string
	Message string
	TaskID  string
	Error   error
}

type Alerter interface {
	Alert(ctx context.Context, alert *Alert) error
}
```

Implement `LogAlerter` and `WebhookAlerter` stub per design doc.

- [ ] **Step 4: Verify pass**
- [ ] **Step 5: Commit**

```bash
git commit -m "feat(errors): add Alerter interface with Log and Webhook implementations"
```

---

### Task 15: Missing API endpoints — batch 1 (S3)

**Files:**
- Modify: `internal/api/server.go` — add routes and handlers
- Modify: `internal/api/server_test.go`

Endpoints to add:
- `PUT /tasks/{id}` — update task config
- `POST /tasks/{id}/restart` — restart task
- `GET /tasks/{id}/progress` — get sync progress
- `GET /tasks/{id}/status` — get detailed status
- `GET /ready` — readiness probe

- [ ] **Step 1: Write failing tests for each endpoint**
- [ ] **Step 2: Verify all fail with 404**
- [ ] **Step 3: Implement handlers**
- [ ] **Step 4: Verify pass**
- [ ] **Step 5: Commit**

```bash
git commit -m "feat(api): add task update/restart/progress/status and readiness endpoints"
```

---

### Task 16: Missing API endpoints — batch 2 (S3)

**Files:**
- Modify: `internal/api/server.go`
- Modify: `internal/api/server_test.go`

Endpoints to add:
- `DELETE /nodes/{id}` — unregister node
- `POST /nodes/{id}/drain` — drain node tasks
- `GET /cluster/status` — cluster overview
- `GET /cluster/leader` — current leader
- `POST /cluster/rebalance` — rebalance tasks
- `GET /diagnose` — diagnostic info

- [ ] **Step 1-5: Same TDD pattern as Task 15**

```bash
git commit -m "feat(api): add cluster management and diagnostic endpoints"
```

---

### Task 17: 补充测试覆盖率 (S8)

**Files:**
- Multiple test files across `internal/pipeline/`, `internal/source/`, `pkg/event/`

Focus areas (lowest coverage):
1. `pkg/event` (27.4%) — ChangeEvent creation, Position Clone, RowData operations
2. `internal/pipeline` (27.6%) — Pipeline lifecycle, processEvent paths, error handling
3. `internal/source/mysql` (23.6%) — config validation, schema cache, binlog syncer
4. `internal/source/postgres` (15.3%) — config validation, replication connection
5. `internal/api` (27.9%) — all handler paths, error responses

- [ ] **Step 1-N: For each package, write missing tests following TDD**
- [ ] **Final: Verify coverage targets met**

Run: `go test ./pkg/event/ -coverprofile=cover.out && go tool cover -func=cover.out | tail -1`
Target: each package ≥ 80% (except connectors ≥ 60% with integration tests)

```bash
git commit -m "test: improve coverage for event, pipeline, source, and api packages"
```

---

### Task 18: Oracle Sink Connector

**Files:**
- Create: `internal/sink/oracle/config.go`
- Create: `internal/sink/oracle/connector.go`
- Create: `internal/sink/oracle/connector_test.go`

设计文档参考：`connector-design.md:1695-1699` + `:1757`（单事务写入，所有表支持事务）。

实现参考 MySQL/PostgreSQL Sink 模式：
- `database/sql` + `github.com/sijms/go-ora/v2` 驱动（Source 侧已使用）
- DSN 构建使用 `net.JoinHostPort`（IPv6 兼容）
- `ApplyDDL` 支持（直接执行 DDL SQL）
- INSERT/UPDATE/DELETE 生成（参考 PG Sink 的参数化查询）
- Oracle 特有：使用 `"SCHEMA"."TABLE"` 双引号引用，MERGE INTO 做 upsert

- [ ] **Step 1: Write failing tests** — config validation, connector interface compliance
- [ ] **Step 2: Implement config.go** — Config struct + Validate + DefaultConfig
- [ ] **Step 3: Implement connector.go** — Initialize/Start/Stop/Write/Flush/ApplyDDL
- [ ] **Step 4: Implement DML generation** — buildInsert/buildUpdate/buildDelete + WriteBatch 单事务
- [ ] **Step 5: Run tests, verify pass**
- [ ] **Step 6: Commit**

```bash
git commit -m "feat(sink): add Oracle sink connector with single-transaction write"
```

---

### Task 19: SQL Server Sink Connector

**Files:**
- Create: `internal/sink/sqlserver/config.go`
- Create: `internal/sink/sqlserver/connector.go`
- Create: `internal/sink/sqlserver/connector_test.go`

设计文档参考：`connector-design.md:1698-1699` + `:1758`（单事务写入，所有表支持事务）。

实现参考 MySQL/PostgreSQL Sink 模式：
- `database/sql` + `github.com/microsoft/go-mssqldb` 驱动（Source 侧已使用）
- DSN 构建使用 `net.JoinHostPort`（IPv6 兼容）
- `ApplyDDL` 支持
- INSERT/UPDATE/DELETE 生成（T-SQL 参数化查询 `@p1, @p2`）
- SQL Server 特有：使用 `[schema].[table]` 方括号引用，MERGE INTO 做 upsert

- [ ] **Step 1: Write failing tests** — config validation, connector interface compliance
- [ ] **Step 2: Implement config.go** — Config struct + Validate + DefaultConfig
- [ ] **Step 3: Implement connector.go** — Initialize/Start/Stop/Write/Flush/ApplyDDL
- [ ] **Step 4: Implement DML generation** — buildInsert/buildUpdate/buildDelete + WriteBatch 单事务
- [ ] **Step 5: Run tests, verify pass**
- [ ] **Step 6: Commit**

```bash
git commit -m "feat(sink): add SQL Server sink connector with single-transaction write"
```

---

## Summary

| Phase | Tasks | Items Fixed |
|-------|-------|------------|
| P0 | 1-2 | M4, A1 |
| P1 | 3-5 | M3, M5, A3 |
| P2 | 6-8 | M1, M2, S1b |
| P3 | 9-17 | S7, S1a, S6, S4 (3 phases), S3 (2 batches), S8 |
| P4 | 18-19 | Oracle Sink, SQL Server Sink (设计有、实现遗漏) |

Note: S5 (Memory Coordinator) was found to already exist at `internal/pipeline/coordinator.go:80-217`. No action needed.
