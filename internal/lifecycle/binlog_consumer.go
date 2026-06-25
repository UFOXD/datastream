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
	taskID     string
	store      source.TableLifecycleStore
	cache      cache.BinlogCacheBackend
	sink       EventSink
	sourceType cache.SourceType
}

// NewBinlogConsumer creates a new BinlogConsumer.
func NewBinlogConsumer(taskID string, store source.TableLifecycleStore, cacheBackend cache.BinlogCacheBackend, sink EventSink, sourceType cache.SourceType) *BinlogConsumer {
	return &BinlogConsumer{
		taskID:     taskID,
		store:      store,
		cache:      cacheBackend,
		sink:       sink,
		sourceType: sourceType,
	}
}

// Route dispatches a single ChangeEvent based on the table's lifecycle state.
func (c *BinlogConsumer) Route(ctx context.Context, ev *event.ChangeEvent) error {
	tableID := source.TableID{
		Database: ev.Table.Database,
		Schema:   ev.Table.Schema,
		Table:    ev.Table.Table,
	}

	lc, err := c.store.Get(ctx, c.taskID, tableID)
	if err != nil {
		return nil // table not found — discard
	}

	state := lc.GetState()
	switch state {
	case source.TableStateSnapshotting:
		ce := eventToCacheEvent(ev, c.sourceType)
		return c.cache.Write(ctx, tableID.String(), ce)

	case source.TableStateCatchingUp, source.TableStateStreaming:
		return c.sink.Write(ctx, []*event.ChangeEvent{ev})

	default:
		return nil
	}
}

// eventToCacheEvent converts a ChangeEvent to a CacheEvent for persistent caching.
func eventToCacheEvent(ev *event.ChangeEvent, sourceType cache.SourceType) *cache.CacheEvent {
	payload, _ := json.Marshal(ev)

	ce := &cache.CacheEvent{
		SourceType:  sourceType,
		EventSeq:    int64(ev.Position.SeqNo),
		TimestampMs: ev.Timestamp.UnixMilli(),
		Payload:     payload,
	}

	// Store position.
	ce.SetPosition(&ev.Position)

	// Set tx_id based on source type.
	ce.TxID = ev.Position.TxID

	return ce
}
