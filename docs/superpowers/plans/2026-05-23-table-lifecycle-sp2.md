# Table-Level Independent Lifecycle — Sub-Plan 2: BinlogCache

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the binlog event caching layer — the BinlogCacheBackend interface, LocalBackend (Badger-based), CacheEvent Protobuf format, and CLI decode tool.

**Architecture:** BinlogCacheBackend stores per-table incremental events as length-prefixed Protobuf records in per-table files. LocalBackend uses the filesystem with append-only writes. S3Backend is deferred to Sub-Plan 3.

**Tech Stack:** Go 1.22+, google.golang.org/protobuf, dgraph-io/badger/v4 (filesystem only, not KV), protoc

**Design Doc:** `docs/design/table-lifecycle-design.md` §3

**Depends on:** Sub-Plan 1 (event.Position GTID/ResumeToken, TableLifecycle types)

---

## File Structure

```
proto/
  cache_event.proto         — CREATE: CacheEvent message definition

internal/cache/
  cache_event.pb.go         — GENERATED: protoc output
  backend.go                — CREATE: BinlogCacheBackend interface
  local_backend.go          — CREATE: filesystem-based implementation
  local_backend_test.go     — CREATE: tests
  cache_size.go             — CREATE: ParseCacheSize (fixed/percentage), disk usage checker
  cache_size_test.go        — CREATE: tests

internal/cli/
  binlog_cmd.go             — CREATE: `datastream-ctl binlog decode/stat` commands
```

---

### Task 1: CacheEvent Protobuf definition + code generation

**Files:**
- Create: `proto/cache_event.proto`
- Create: `internal/cache/cache_event.pb.go` (generated)
- Create: `Makefile` target (or script)

- [ ] **Step 1: Write proto file**

```protobuf
syntax = "proto3";

package datastream.cache;

option go_package = "github.com/UFOXD/datastream/internal/cache";

message CacheEvent {
    string gtid = 1;
    int64  event_seq = 2;
    bool   is_begin = 3;
    bool   is_commit = 4;
    bytes  payload = 5;
    int64  timestamp_ms = 6;
}
```

- [ ] **Step 2: Generate Go code**

Run: `protoc --go_out=. --go_opt=paths=source_relative proto/cache_event.proto`

Or if the output path differs: `protoc --go_out=internal/cache --go_opt=paths=source_relative proto/cache_event.proto`

Verify: `internal/cache/cache_event.pb.go` exists and compiles.

- [ ] **Step 3: Verify build**

Run: `go build ./internal/cache/...`
Expected: clean

- [ ] **Step 4: Commit**

```bash
git add proto/cache_event.proto internal/cache/cache_event.pb.go
git commit -m "feat(cache): add CacheEvent protobuf definition"
```

---

### Task 2: BinlogCacheBackend interface

**Files:**
- Create: `internal/cache/backend.go`

- [ ] **Step 1: Write interface**

```go
package cache

import "context"

type BinlogCacheBackend interface {
    // Write appends a CacheEvent to the table's buffer
    Write(ctx context.Context, tableID string, event *CacheEvent) error

    // Read returns a channel of CacheEvents starting from the given GTID+EventSeq.
    // The returned channel is closed when all events are consumed or ctx is cancelled.
    Read(ctx context.Context, tableID string, fromGTID string, fromEventSeq int64) (<-chan *CacheEvent, error)

    // Delete removes all cached events for a table
    Delete(ctx context.Context, tableID string) error

    // Size returns the total cache size in bytes for a table
    Size(ctx context.Context, tableID string) (int64, error)

    // TotalSize returns the total cache size across all tables
    TotalSize(ctx context.Context) (int64, error)

    // Exists checks whether cache data exists for a table
    Exists(ctx context.Context, tableID string) (bool, error)

    // Close flushes and closes the backend
    Close() error
}
```

- [ ] **Step 2: Verify build**

Run: `go build ./internal/cache/...`

- [ ] **Step 3: Commit**

```bash
git add internal/cache/backend.go
git commit -m "feat(cache): add BinlogCacheBackend interface"
```

---

### Task 3: LocalBackend implementation (filesystem, length-prefixed protobuf)

**Files:**
- Create: `internal/cache/local_backend.go`
- Create: `internal/cache/local_backend_test.go`

- [ ] **Step 1: Write failing tests**

```go
func TestLocalBackendWriteAndRead(t *testing.T) {
    dir := t.TempDir()
    backend, err := NewLocalBackend(dir)
    require.NoError(t, err)
    defer backend.Close()

    ctx := context.Background()

    // Write 3 events
    for i := 0; i < 3; i++ {
        err := backend.Write(ctx, "db1.users", &CacheEvent{
            Gtid:        "uuid:100",
            EventSeq:    int64(i),
            Payload:     []byte(fmt.Sprintf("event-%d", i)),
            TimestampMs: time.Now().UnixMilli(),
        })
        require.NoError(t, err)
    }

    // Read all
    ch, err := backend.Read(ctx, "db1.users", "", 0)
    require.NoError(t, err)

    var events []*CacheEvent
    for ev := range ch {
        events = append(events, ev)
    }
    assert.Len(t, events, 3)
    assert.Equal(t, int64(0), events[0].EventSeq)
    assert.Equal(t, int64(2), events[2].EventSeq)
}

func TestLocalBackendReadFromOffset(t *testing.T) {
    dir := t.TempDir()
    backend, _ := NewLocalBackend(dir)
    defer backend.Close()

    ctx := context.Background()
    for i := 0; i < 5; i++ {
        backend.Write(ctx, "db1.t", &CacheEvent{
            Gtid:     "uuid:100",
            EventSeq: int64(i),
        })
    }

    // Read from GTID uuid:100, EventSeq 3
    ch, err := backend.Read(ctx, "db1.t", "uuid:100", 3)
    require.NoError(t, err)

    var events []*CacheEvent
    for ev := range ch {
        events = append(events, ev)
    }
    // Should get events 3 and 4
    assert.Len(t, events, 2)
    assert.Equal(t, int64(3), events[0].EventSeq)
}

func TestLocalBackendDelete(t *testing.T) {
    dir := t.TempDir()
    backend, _ := NewLocalBackend(dir)
    defer backend.Close()

    ctx := context.Background()
    backend.Write(ctx, "db1.t", &CacheEvent{Gtid: "uuid:1", EventSeq: 0})

    exists, _ := backend.Exists(ctx, "db1.t")
    assert.True(t, exists)

    err := backend.Delete(ctx, "db1.t")
    require.NoError(t, err)

    exists, _ = backend.Exists(ctx, "db1.t")
    assert.False(t, exists)
}

func TestLocalBackendSize(t *testing.T) {
    dir := t.TempDir()
    backend, _ := NewLocalBackend(dir)
    defer backend.Close()

    ctx := context.Background()
    backend.Write(ctx, "db1.t", &CacheEvent{
        Gtid:    "uuid:1",
        Payload: make([]byte, 1000),
    })

    size, err := backend.Size(ctx, "db1.t")
    require.NoError(t, err)
    assert.Greater(t, size, int64(1000)) // payload + protobuf overhead + length prefix
}

func TestLocalBackendTotalSize(t *testing.T) {
    dir := t.TempDir()
    backend, _ := NewLocalBackend(dir)
    defer backend.Close()

    ctx := context.Background()
    backend.Write(ctx, "db1.t1", &CacheEvent{Payload: make([]byte, 500)})
    backend.Write(ctx, "db1.t2", &CacheEvent{Payload: make([]byte, 500)})

    total, err := backend.TotalSize(ctx)
    require.NoError(t, err)
    assert.Greater(t, total, int64(1000))
}

func TestLocalBackendReadCancellation(t *testing.T) {
    dir := t.TempDir()
    backend, _ := NewLocalBackend(dir)
    defer backend.Close()

    ctx, cancel := context.WithCancel(context.Background())
    backend.Write(ctx, "db1.t", &CacheEvent{Gtid: "uuid:1", EventSeq: 0})

    cancel() // cancel before reading

    ch, err := backend.Read(ctx, "db1.t", "", 0)
    // Either returns error or channel closes immediately
    if err == nil {
        events := 0
        for range ch {
            events++
        }
        // Should get 0 or 1 events (may read one before noticing cancellation)
        assert.LessOrEqual(t, events, 1)
    }
}
```

- [ ] **Step 2: Run tests, verify fail**

- [ ] **Step 3: Implement LocalBackend**

Key implementation details:
- File path: `{baseDir}/{tableID}.binlog` (replace `.` in tableID with `_` for filesystem safety)
- Write: open file in append mode, marshal CacheEvent to protobuf, write [4-byte big-endian length][protobuf bytes]
- Read: open file, scan sequentially skipping events until matching GTID+EventSeq, then emit to channel
- Use `sync.Mutex` per table file to prevent concurrent write corruption
- Read spawns a goroutine that sends to channel and closes it when done or ctx cancelled
- Size: `os.Stat(filepath).Size()`
- TotalSize: sum of all `.binlog` files in baseDir
- Delete: `os.Remove(filepath)`
- Exists: `os.Stat` returns nil error

- [ ] **Step 4: Run tests, verify pass**

Run: `go test ./internal/cache/ -count=1 -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add internal/cache/local_backend.go internal/cache/local_backend_test.go
git commit -m "feat(cache): implement LocalBackend with length-prefixed protobuf files"
```

---

### Task 4: Cache size parser (fixed size + percentage)

**Files:**
- Create: `internal/cache/cache_size.go`
- Create: `internal/cache/cache_size_test.go`

- [ ] **Step 1: Write failing tests**

```go
func TestParseCacheSizeFixed(t *testing.T) {
    tests := []struct {
        input string
        want  int64
    }{
        {"50GB", 50 * 1024 * 1024 * 1024},
        {"100MB", 100 * 1024 * 1024},
        {"1TB", 1024 * 1024 * 1024 * 1024},
        {"500gb", 500 * 1024 * 1024 * 1024},
    }
    for _, tt := range tests {
        t.Run(tt.input, func(t *testing.T) {
            got, err := ParseCacheSize(tt.input, "/tmp")
            require.NoError(t, err)
            assert.Equal(t, tt.want, got)
        })
    }
}

func TestParseCacheSizePercentage(t *testing.T) {
    // Can't test exact percentage without knowing disk size,
    // but we can test the parsing logic
    got, err := ParseCacheSize("80%", "/tmp")
    require.NoError(t, err)
    assert.Greater(t, got, int64(0))
}

func TestParseCacheSizeInvalid(t *testing.T) {
    _, err := ParseCacheSize("abc", "/tmp")
    assert.Error(t, err)

    _, err = ParseCacheSize("", "/tmp")
    assert.Error(t, err)
}

func TestCacheSizeThresholds(t *testing.T) {
    maxSize := int64(100 * 1024 * 1024) // 100MB
    
    assert.Equal(t, CacheLevelNormal, CheckCacheLevel(50*1024*1024, maxSize))
    assert.Equal(t, CacheLevelWarning, CheckCacheLevel(82*1024*1024, maxSize))
    assert.Equal(t, CacheLevelPauseScheduling, CheckCacheLevel(92*1024*1024, maxSize))
    assert.Equal(t, CacheLevelFull, CheckCacheLevel(100*1024*1024, maxSize))
}
```

- [ ] **Step 2: Implement**

```go
package cache

import (
    "fmt"
    "strconv"
    "strings"
    "syscall"
)

type CacheLevel int

const (
    CacheLevelNormal          CacheLevel = iota // < 80%
    CacheLevelWarning                           // >= 80%
    CacheLevelPauseScheduling                   // >= 90%
    CacheLevelFull                              // >= 100%
)

func ParseCacheSize(value string, cacheDir string) (int64, error) {
    value = strings.TrimSpace(value)
    if value == "" {
        return 0, fmt.Errorf("empty cache size value")
    }

    if strings.HasSuffix(value, "%") {
        pctStr := strings.TrimSuffix(value, "%")
        pct, err := strconv.ParseFloat(pctStr, 64)
        if err != nil {
            return 0, fmt.Errorf("invalid percentage: %q", value)
        }
        diskTotal := getDiskTotal(cacheDir)
        return int64(float64(diskTotal) * pct / 100), nil
    }

    return parseByteSize(value)
}

func CheckCacheLevel(currentSize, maxSize int64) CacheLevel {
    if maxSize <= 0 {
        return CacheLevelNormal
    }
    ratio := float64(currentSize) / float64(maxSize)
    switch {
    case ratio >= 1.0:
        return CacheLevelFull
    case ratio >= 0.9:
        return CacheLevelPauseScheduling
    case ratio >= 0.8:
        return CacheLevelWarning
    default:
        return CacheLevelNormal
    }
}

func getDiskTotal(path string) int64 {
    var stat syscall.Statfs_t
    if err := syscall.Statfs(path, &stat); err != nil {
        return 0
    }
    return int64(stat.Blocks) * int64(stat.Bsize)
}

func parseByteSize(s string) (int64, error) {
    s = strings.ToUpper(strings.TrimSpace(s))
    multipliers := map[string]int64{
        "TB": 1024 * 1024 * 1024 * 1024,
        "GB": 1024 * 1024 * 1024,
        "MB": 1024 * 1024,
        "KB": 1024,
    }
    for suffix, mult := range multipliers {
        if strings.HasSuffix(s, suffix) {
            numStr := strings.TrimSuffix(s, suffix)
            num, err := strconv.ParseFloat(numStr, 64)
            if err != nil {
                return 0, fmt.Errorf("invalid size: %q", s)
            }
            return int64(num * float64(mult)), nil
        }
    }
    return 0, fmt.Errorf("invalid size format: %q (use TB/GB/MB/KB or %%)", s)
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./internal/cache/ -count=1`
Expected: All PASS

- [ ] **Step 4: Commit**

```bash
git add internal/cache/cache_size.go internal/cache/cache_size_test.go
git commit -m "feat(cache): add ParseCacheSize with fixed/percentage and threshold checker"
```

---

### Task 5: CLI binlog decode command

**Files:**
- Modify: `internal/cli/commands.go` — add `binlog` subcommand group
- Create: `internal/cli/binlog_cmd.go` — decode and stat commands

- [ ] **Step 1: Implement decode/stat commands**

The `decode` command reads a `.binlog` file and outputs JSON. The `stat` command shows statistics.

```go
// datastream-ctl binlog decode --file path/to/file.binlog --format json
// datastream-ctl binlog stat --file path/to/file.binlog
```

Implementation:
- Open file, read length-prefixed protobuf records
- For `decode`: unmarshal each CacheEvent, print as JSON to stdout
- For `stat`: count events, GTIDs, total size, time range

- [ ] **Step 2: Write a basic test**

Test that the decode command can read a file written by LocalBackend.

- [ ] **Step 3: Verify**

Run: `go build ./... && go test ./internal/cli/ -count=1`

- [ ] **Step 4: Commit**

```bash
git add internal/cli/binlog_cmd.go internal/cli/commands.go
git commit -m "feat(cli): add binlog decode and stat commands"
```

---

### Task 6: Full verification

- [ ] **Step 1: Full build**

Run: `go build ./...`

- [ ] **Step 2: Full tests**

Run: `go test ./... -count=1`
Expected: All packages pass

- [ ] **Step 3: Verify cache package independently**

Run: `go test ./internal/cache/ -count=1 -v`

---

## Summary

| Task | What | Key Files |
|------|------|-----------|
| 1 | CacheEvent Protobuf | `proto/cache_event.proto`, `internal/cache/cache_event.pb.go` |
| 2 | Backend interface | `internal/cache/backend.go` |
| 3 | LocalBackend (filesystem) | `internal/cache/local_backend.go` |
| 4 | Cache size parser + thresholds | `internal/cache/cache_size.go` |
| 5 | CLI decode/stat | `internal/cli/binlog_cmd.go` |
| 6 | Full verification | — |
