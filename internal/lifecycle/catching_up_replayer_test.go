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

func (b *testCacheBackend) Read(ctx context.Context, tableID, fromGTID string, fromEventSeq int64) (<-chan *cache.CacheEvent, error) {
	ch := make(chan *cache.CacheEvent)
	go func() {
		defer close(ch)
		skip := fromGTID != ""
		for _, ev := range b.events[tableID] {
			if skip {
				if ev.Gtid == fromGTID && ev.EventSeq >= fromEventSeq {
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
	return ch, nil
}

func (b *testCacheBackend) Write(_ context.Context, _ string, _ *cache.CacheEvent) error {
	return nil
}
func (b *testCacheBackend) Delete(_ context.Context, _ string) error        { return nil }
func (b *testCacheBackend) Size(_ context.Context, _ string) (int64, error) { return 0, nil }
func (b *testCacheBackend) TotalSize(_ context.Context) (int64, error)      { return 0, nil }
func (b *testCacheBackend) Exists(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (b *testCacheBackend) Close() error { return nil }

// --- helpers ---

func makeCacheEvent(gtid string, seq int64, ev *event.ChangeEvent) *cache.CacheEvent {
	payload, _ := json.Marshal(ev)
	return &cache.CacheEvent{
		Gtid:        gtid,
		EventSeq:    seq,
		Payload:     payload,
		TimestampMs: ev.Timestamp.UnixMilli(),
	}
}

// --- tests ---

func TestReplayerAppliesAllEvents(t *testing.T) {
	cb := newTestCacheBackend()
	sink := &mockEventSink{}

	// Populate 3 events for table "db.users"
	tableID := "db.users"
	for i := int64(0); i < 3; i++ {
		ev := &event.ChangeEvent{
			Type:      event.EventTypeInsert,
			Table:     event.TableInfo{Database: "db", Table: "users"},
			Position:  event.Position{GTID: "gtid-1", SeqNo: int(i)},
			Timestamp: time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC),
		}
		cb.events[tableID] = append(cb.events[tableID], makeCacheEvent("gtid-1", i, ev))
	}

	replayer := NewCatchingUpReplayer(cb, sink, ReplayerConfig{
		BatchSize:      100,
		UpsertDuration: 0, // no upsert window
	})

	result, err := replayer.Replay(context.Background(), tableID, "", 0)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}

	if !result.CaughtUp {
		t.Error("expected CaughtUp=true")
	}
	if result.EventsApplied != 3 {
		t.Errorf("EventsApplied = %d, want 3", result.EventsApplied)
	}
	if result.LastGTID != "gtid-1" {
		t.Errorf("LastGTID = %q, want %q", result.LastGTID, "gtid-1")
	}
	if result.LastEventSeq != 2 {
		t.Errorf("LastEventSeq = %d, want 2", result.LastEventSeq)
	}
	if sink.count() != 3 {
		t.Errorf("sink received %d events, want 3", sink.count())
	}
}

func TestReplayerResumeFromPosition(t *testing.T) {
	cb := newTestCacheBackend()
	sink := &mockEventSink{}

	tableID := "db.orders"
	// 5 events: seq 0..4, all with gtid "gtid-A"
	for i := int64(0); i < 5; i++ {
		ev := &event.ChangeEvent{
			Type:      event.EventTypeInsert,
			Table:     event.TableInfo{Database: "db", Table: "orders"},
			Position:  event.Position{GTID: "gtid-A", SeqNo: int(i)},
			Timestamp: time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC),
		}
		cb.events[tableID] = append(cb.events[tableID], makeCacheEvent("gtid-A", i, ev))
	}

	replayer := NewCatchingUpReplayer(cb, sink, ReplayerConfig{
		BatchSize:      100,
		UpsertDuration: 0,
	})

	// Resume from gtid-A, event_seq=3 → should get seq 3 and 4
	result, err := replayer.Replay(context.Background(), tableID, "gtid-A", 3)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}

	if !result.CaughtUp {
		t.Error("expected CaughtUp=true")
	}
	if result.EventsApplied != 2 {
		t.Errorf("EventsApplied = %d, want 2", result.EventsApplied)
	}
	if result.LastEventSeq != 4 {
		t.Errorf("LastEventSeq = %d, want 4", result.LastEventSeq)
	}
}

func TestReplayerUpsertWindow(t *testing.T) {
	cb := newTestCacheBackend()
	sink := &mockEventSink{}

	tableID := "db.products"
	for i := int64(0); i < 3; i++ {
		ev := &event.ChangeEvent{
			Type:      event.EventTypeInsert,
			Table:     event.TableInfo{Database: "db", Table: "products"},
			Position:  event.Position{GTID: "gtid-U", SeqNo: int(i)},
			Timestamp: time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC),
		}
		cb.events[tableID] = append(cb.events[tableID], makeCacheEvent("gtid-U", i, ev))
	}

	replayer := NewCatchingUpReplayer(cb, sink, ReplayerConfig{
		BatchSize:      100,
		UpsertDuration: 5 * time.Second, // generous window; all events should be upsert
	})

	result, err := replayer.Replay(context.Background(), tableID, "", 0)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}

	if result.EventsApplied != 3 {
		t.Fatalf("EventsApplied = %d, want 3", result.EventsApplied)
	}

	// Verify all events have upsert metadata
	sink.mu.Lock()
	defer sink.mu.Unlock()
	for i, ev := range sink.events {
		mode, ok := ev.Metadata["_write_mode"]
		if !ok || mode != "upsert" {
			t.Errorf("event[%d] missing _write_mode=upsert, got metadata=%v", i, ev.Metadata)
		}
	}
}

func TestReplayerContextCancellation(t *testing.T) {
	cb := newTestCacheBackend()

	tableID := "db.slow"
	// Add many events so we can cancel mid-way
	for i := int64(0); i < 1000; i++ {
		ev := &event.ChangeEvent{
			Type:      event.EventTypeInsert,
			Table:     event.TableInfo{Database: "db", Table: "slow"},
			Position:  event.Position{GTID: "gtid-C", SeqNo: int(i)},
			Timestamp: time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC),
		}
		cb.events[tableID] = append(cb.events[tableID], makeCacheEvent("gtid-C", i, ev))
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Use a sink that cancels after receiving a few events
	cancelSink := &cancellingEventSink{
		cancelAfter: 5,
		cancelFunc:  cancel,
	}

	replayer := NewCatchingUpReplayer(cb, cancelSink, ReplayerConfig{
		BatchSize:      100,
		UpsertDuration: 0,
	})

	result, err := replayer.Replay(ctx, tableID, "", 0)
	if err == nil {
		t.Fatal("expected context cancellation error, got nil")
	}
	if result.CaughtUp {
		t.Error("expected CaughtUp=false on cancellation")
	}
	if result.EventsApplied == 0 {
		t.Error("expected some events to be applied before cancellation")
	}
	if result.EventsApplied >= 1000 {
		t.Error("expected partial application, got all events")
	}
}

func TestReplayerEmptyCache(t *testing.T) {
	cb := newTestCacheBackend()
	sink := &mockEventSink{}

	replayer := NewCatchingUpReplayer(cb, sink, ReplayerConfig{
		BatchSize:      100,
		UpsertDuration: time.Second,
	})

	result, err := replayer.Replay(context.Background(), "db.empty", "", 0)
	if err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}

	if !result.CaughtUp {
		t.Error("expected CaughtUp=true for empty cache")
	}
	if result.EventsApplied != 0 {
		t.Errorf("EventsApplied = %d, want 0", result.EventsApplied)
	}
	if result.LastGTID != "" {
		t.Errorf("LastGTID = %q, want empty", result.LastGTID)
	}
	if sink.count() != 0 {
		t.Errorf("sink received %d events, want 0", sink.count())
	}
}

// --- cancelling sink helper ---

// cancellingEventSink cancels the context after receiving a specified number of events.
type cancellingEventSink struct {
	count_      int
	cancelAfter int
	cancelFunc  context.CancelFunc
}

func (s *cancellingEventSink) Write(_ context.Context, events []*event.ChangeEvent) error {
	s.count_ += len(events)
	if s.count_ >= s.cancelAfter {
		s.cancelFunc()
	}
	return nil
}
