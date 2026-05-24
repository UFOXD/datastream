package lifecycle

import (
	"context"
	"encoding/json"
	"time"

	"github.com/UFOXD/datastream/internal/cache"
	"github.com/UFOXD/datastream/pkg/event"
)

// ReplayerConfig holds configuration for the CatchingUpReplayer.
type ReplayerConfig struct {
	// BatchSize is reserved for future batching optimisation.
	BatchSize int
	// UpsertDuration defines how long after replay start the replayer
	// should mark events with UPSERT write mode. This covers the window
	// where snapshot data and binlog events may overlap.
	UpsertDuration time.Duration
}

// ReplayResult contains the outcome of a replay operation.
type ReplayResult struct {
	// CaughtUp is true when the cache channel was fully drained.
	CaughtUp bool
	// LastGTID is the GTID of the last applied event.
	LastGTID string
	// LastEventSeq is the event sequence of the last applied event.
	LastEventSeq int64
	// EventsApplied is the total number of events written to the sink.
	EventsApplied int64
}

// CatchingUpReplayer reads cached binlog events and replays them through a sink.
// It is used during the catching-up phase of table lifecycle to bridge the gap
// between a completed snapshot and real-time streaming.
type CatchingUpReplayer struct {
	cache  cache.BinlogCacheBackend
	sink   EventSink
	config ReplayerConfig
}

// NewCatchingUpReplayer creates a new CatchingUpReplayer.
func NewCatchingUpReplayer(cacheBackend cache.BinlogCacheBackend, sink EventSink, config ReplayerConfig) *CatchingUpReplayer {
	return &CatchingUpReplayer{
		cache:  cacheBackend,
		sink:   sink,
		config: config,
	}
}

// Replay reads events from the cache starting at the given position and writes
// them to the sink. Events within the UpsertDuration window are marked with
// "_write_mode"="upsert" in their Metadata to signal the sink to use UPSERT
// semantics instead of plain INSERT.
//
// Returns a ReplayResult indicating progress. CaughtUp is true if the channel
// was fully consumed (all cached events applied). If the context is cancelled
// mid-replay, CaughtUp is false.
func (r *CatchingUpReplayer) Replay(ctx context.Context, tableID string, fromGTID string, fromEventSeq int64) (*ReplayResult, error) {
	ch, err := r.cache.Read(ctx, tableID, fromGTID, fromEventSeq)
	if err != nil {
		return nil, err
	}

	upsertUntil := time.Now().Add(r.config.UpsertDuration)

	result := &ReplayResult{}

	for {
		select {
		case <-ctx.Done():
			result.CaughtUp = false
			return result, ctx.Err()
		case ev, ok := <-ch:
			if !ok {
				// Channel closed — all events consumed.
				result.CaughtUp = true
				return result, nil
			}

			changeEvent, err := deserializePayload(ev.GetPayload())
			if err != nil {
				return result, err
			}

			// Apply UPSERT mode if within the upsert window.
			if time.Now().Before(upsertUntil) {
				if changeEvent.Metadata == nil {
					changeEvent.Metadata = make(map[string]string)
				}
				changeEvent.Metadata["_write_mode"] = "upsert"
			}

			if err := r.sink.Write(ctx, []*event.ChangeEvent{changeEvent}); err != nil {
				return result, err
			}

			result.LastGTID = ev.GetGtid()
			result.LastEventSeq = ev.GetEventSeq()
			result.EventsApplied++
		}
	}
}

// deserializePayload unmarshals a JSON-encoded ChangeEvent from cache payload.
func deserializePayload(payload []byte) (*event.ChangeEvent, error) {
	var ce event.ChangeEvent
	if err := json.Unmarshal(payload, &ce); err != nil {
		return nil, err
	}
	return &ce, nil
}
