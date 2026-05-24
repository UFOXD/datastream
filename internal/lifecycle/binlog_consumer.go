package lifecycle

import (
	"context"
	"encoding/json"

	"github.com/UFOXD/datastream/internal/cache"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
)

// EventSink writes processed change events downstream (e.g., to a target database or queue).
type EventSink interface {
	Write(ctx context.Context, events []*event.ChangeEvent) error
}

// BinlogConsumer routes incoming binlog ChangeEvents to the appropriate destination
// based on the per-table lifecycle state.
type BinlogConsumer struct {
	taskID string
	store  source.TableLifecycleStore
	cache  cache.BinlogCacheBackend
	sink   EventSink
}

// NewBinlogConsumer creates a new BinlogConsumer.
func NewBinlogConsumer(taskID string, store source.TableLifecycleStore, cacheBackend cache.BinlogCacheBackend, sink EventSink) *BinlogConsumer {
	return &BinlogConsumer{
		taskID: taskID,
		store:  store,
		cache:  cacheBackend,
		sink:   sink,
	}
}

// Route dispatches a single ChangeEvent based on the table's lifecycle state:
//   - snapshotting → write to cache (for later replay)
//   - catching_up, streaming → write to sink (real-time delivery)
//   - pending, error, paused → discard
//   - table not found → discard
func (c *BinlogConsumer) Route(ctx context.Context, ev *event.ChangeEvent) error {
	tableID := source.TableID{
		Database: ev.Table.Database,
		Schema:   ev.Table.Schema,
		Table:    ev.Table.Table,
	}

	lc, err := c.store.Get(ctx, c.taskID, tableID)
	if err != nil {
		// Table not found in store — discard.
		return nil
	}

	state := lc.GetState()
	switch state {
	case source.TableStateSnapshotting:
		ce := eventToCacheEvent(ev)
		return c.cache.Write(ctx, tableID.String(), ce)

	case source.TableStateCatchingUp, source.TableStateStreaming:
		return c.sink.Write(ctx, []*event.ChangeEvent{ev})

	default:
		// pending, error, paused — discard
		return nil
	}
}

// eventToCacheEvent converts a ChangeEvent to a CacheEvent for persistent caching.
func eventToCacheEvent(ev *event.ChangeEvent) *cache.CacheEvent {
	gtid := ev.Position.GTID
	if gtid == "" {
		gtid = ev.Position.TxID
	}

	payload, _ := json.Marshal(ev)

	return &cache.CacheEvent{
		Gtid:        gtid,
		EventSeq:    int64(ev.Position.SeqNo),
		TimestampMs: ev.Timestamp.UnixMilli(),
		Payload:     payload,
	}
}
