package lifecycle

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/UFOXD/datastream/internal/cache"
	"github.com/UFOXD/datastream/pkg/event"
)

// --- test cache backend ---

type testCacheBackend struct {
	events map[string][]*cache.CacheEvent
}

func newTestCacheBackend() *testCacheBackend {
	return &testCacheBackend{events: make(map[string][]*cache.CacheEvent)}
}

func (b *testCacheBackend) Read(ctx context.Context, tableID, fromTxID string, fromEventSeq int64) cache.ReadResult {
	ch := make(chan *cache.CacheEvent, 16)
	errCh := make(chan error, 1)

	go func() {
		defer close(ch)
		defer close(errCh)
		skip := fromTxID != ""
		for _, ev := range b.events[tableID] {
			if skip {
				if ev.TxID == fromTxID && ev.EventSeq >= fromEventSeq {
					skip = false
				} else {
					continue
				}
			}
			select {
			case ch <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	return cache.ReadResult{Events: ch, Err: errCh}
}

func (b *testCacheBackend) Write(_ context.Context, _ string, _ *cache.CacheEvent) error {
	return nil
}
func (b *testCacheBackend) WriteBatch(_ context.Context, _ string, _ []*cache.CacheEvent) error {
	return nil
}
func (b *testCacheBackend) Delete(_ context.Context, _ string) error        { return nil }
func (b *testCacheBackend) Size(_ context.Context, _ string) (int64, error) { return 0, nil }
func (b *testCacheBackend) TotalSize(_ context.Context) (int64, error)      { return 0, nil }
func (b *testCacheBackend) Exists(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (b *testCacheBackend) Sync(_ context.Context, _ string) error { return nil }
func (b *testCacheBackend) TruncateToLastComplete(_ context.Context, _ string) (*event.Position, error) {
	return nil, nil
}
func (b *testCacheBackend) Close() error { return nil }

// --- helpers ---

func makeCacheEvent(txID string, seq int64, ev *event.ChangeEvent) *cache.CacheEvent {
	payload, _ := json.Marshal(ev)
	return &cache.CacheEvent{
		TxID:        txID,
		EventSeq:    seq,
		Payload:     payload,
		TimestampMs: ev.Timestamp.UnixMilli(),
	}
}

// --- tests ---

func TestReplayerAppliesAllEvents(t *testing.T) {
	cb := newTestCacheBackend()
	sink := &mockEventSink{}

	tableID := "db.users"
	for i := int64(0); i < 3; i++ {
		ev := &event.ChangeEvent{
			Type:      event.EventTypeInsert,
			Table:     event.TableInfo{Database: "db", Table: "users"},
			Timestamp: time.Now(),
		}
		ce := makeCacheEvent("tx-1", i, ev)
		cb.events[tableID] = append(cb.events[tableID], ce)
	}

	replayer := NewCatchingUpReplayer(cb, sink, ReplayerConfig{
		UpsertDuration: 0, // no upsert window
	})

	result, err := replayer.Replay(context.Background(), tableID, "", 0)
	if err != nil {
		t.Fatalf("Replay error: %v", err)
	}
	if !result.CaughtUp {
		t.Error("expected CaughtUp=true")
	}
	if result.EventsApplied != 3 {
		t.Errorf("expected 3 events applied, got %d", result.EventsApplied)
	}
	if sink.count() != 3 {
		t.Errorf("expected 3 sink writes, got %d", sink.count())
	}
}

func TestReplayerSkipsToOffset(t *testing.T) {
	cb := newTestCacheBackend()
	sink := &mockEventSink{}

	tableID := "db.orders"
	for i := int64(0); i < 5; i++ {
		ev := &event.ChangeEvent{
			Type:      event.EventTypeInsert,
			Table:     event.TableInfo{Database: "db", Table: "orders"},
			Timestamp: time.Now(),
		}
		ce := makeCacheEvent("tx-A", i, ev)
		cb.events[tableID] = append(cb.events[tableID], ce)
	}

	replayer := NewCatchingUpReplayer(cb, sink, ReplayerConfig{})

	result, err := replayer.Replay(context.Background(), tableID, "tx-A", 3)
	if err != nil {
		t.Fatalf("Replay error: %v", err)
	}
	if !result.CaughtUp {
		t.Error("expected CaughtUp=true")
	}
	if result.EventsApplied != 2 {
		t.Errorf("expected 2 events (seq 3,4), got %d", result.EventsApplied)
	}
}
