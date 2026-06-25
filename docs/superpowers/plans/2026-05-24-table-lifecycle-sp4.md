# Table-Level Independent Lifecycle — Sub-Plan 4: API & CLI

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add lifecycle-aware API endpoints and CLI commands per design doc §12, plus monitoring metrics per §11.

**Architecture:** New API handlers call SnapshotScheduler methods. CLI commands call the REST API. Metrics use existing Prometheus registry.

**Tech Stack:** Go 1.22+, gorilla/mux, cobra, prometheus

**Design Doc:** `docs/design/table-lifecycle-design.md` §11-12

**Depends on:** Sub-Plans 1-3

---

## File Structure

```
internal/api/
  lifecycle.go          — CREATE: lifecycle API handlers
  lifecycle_test.go     — CREATE: tests

internal/cli/
  lifecycle_cmd.go      — CREATE: CLI commands for lifecycle ops

pkg/metrics/
  lifecycle_metrics.go  — CREATE: table lifecycle Prometheus metrics
```

---

### Task 1: Lifecycle API endpoints

**Files:**
- Create: `internal/api/lifecycle.go`
- Create: `internal/api/lifecycle_test.go`
- Modify: `internal/api/server.go` — add routes + scheduler field

**Endpoints from design doc §12:**

| Method | Path | Handler | Description |
|--------|------|---------|-------------|
| GET | `/api/v1/tasks/{id}/detail` | `getTaskDetail` | Task + all table lifecycle states + summary |
| GET | `/api/v1/tasks/{id}/tables/errors` | `getTableErrors` | Error tables with details |
| POST | `/api/v1/tasks/{id}/tables/restart` | `restartTables` | Restart tables/schemas |
| GET | `/api/v1/tasks/{id}/tables/{table}/state` | `getTableState` | Single table lifecycle |
| POST | `/api/v1/tasks/{id}/tables/{table}/pause` | `pauseTableLifecycle` | Pause single table |
| POST | `/api/v1/tasks/{id}/tables/{table}/resume` | `resumeTableLifecycle` | Resume single table |
| POST | `/api/v1/tasks/{id}/tables/{table}/retry` | `retryTable` | Immediate retry |
| POST | `/api/v1/tasks/{id}/tables/{table}/skip-error` | `skipTableError` | Skip error, continue |

**Server modifications:**
- Add `scheduler *lifecycle.SnapshotScheduler` field to Server struct
- Add `SetScheduler(s *lifecycle.SnapshotScheduler)` method
- Register routes in `setupRoutes()`

**Tests:** For each endpoint, test happy path + nil scheduler (503) + not found.

- [ ] **Step 1: Write failing tests**
- [ ] **Step 2: Add routes to server.go**
- [ ] **Step 3: Implement handlers in lifecycle.go**
- [ ] **Step 4: Run tests**
- [ ] **Step 5: Commit**

```bash
git commit -m "feat(api): add table lifecycle management endpoints"
```

---

### Task 2: Lifecycle CLI commands

**Files:**
- Create: `internal/cli/lifecycle_cmd.go`
- Modify: `internal/cli/commands.go` — register lifecycle subcommands

**Commands from design doc §12.5:**

```bash
datastream-ctl task detail <task-id>
datastream-ctl task errors <task-id>
datastream-ctl task restart-table <task-id> <table1> [table2...] [--schema db] [--force]
datastream-ctl task pause-table <task-id> <table>
datastream-ctl task resume-table <task-id> <table>
datastream-ctl task skip-error <task-id> <table>
datastream-ctl task retry-table <task-id> <table>
```

Each command calls the corresponding REST API endpoint via HTTP client.

- [ ] **Step 1: Implement commands**
- [ ] **Step 2: Register in commands.go**
- [ ] **Step 3: Verify build**
- [ ] **Step 4: Commit**

```bash
git commit -m "feat(cli): add table lifecycle management commands"
```

---

### Task 3: Lifecycle monitoring metrics

**Files:**
- Create: `pkg/metrics/lifecycle_metrics.go`
- Create: `pkg/metrics/lifecycle_metrics_test.go`

**Metrics from design doc §11:**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `datastream_table_state` | Gauge | task, table, state | 1 if table is in this state |
| `datastream_snapshot_progress` | Gauge | task, table | 0-100% |
| `datastream_catching_up_lag_events` | Gauge | task, table | Events remaining |
| `datastream_binlog_cache_size_bytes` | Gauge | task, table | Per-table cache size |
| `datastream_global_min_position_lag_seconds` | Gauge | task | Lag of global min position |
| `datastream_snapshot_retries_total` | Counter | task, table | Retry count |

- [ ] **Step 1: Write failing tests**
- [ ] **Step 2: Implement metrics registration**
- [ ] **Step 3: Run tests**
- [ ] **Step 4: Commit**

```bash
git commit -m "feat(metrics): add table lifecycle Prometheus metrics"
```

---

### Task 4: Full verification

- [ ] **Step 1: Full build** — `go build ./...`
- [ ] **Step 2: Full tests** — `go test ./... -count=1`
- [ ] **Step 3: Verify all 43+ packages pass**

---

## Summary

| Task | What | Key Files |
|------|------|-----------|
| 1 | Lifecycle API endpoints (8 routes) | `internal/api/lifecycle.go` |
| 2 | Lifecycle CLI commands (7 commands) | `internal/cli/lifecycle_cmd.go` |
| 3 | Prometheus metrics (6 metrics) | `pkg/metrics/lifecycle_metrics.go` |
| 4 | Full verification | — |
