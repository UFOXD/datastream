package pipeline

import (
	"context"
	"strconv"
	"sync"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/sink"
	"github.com/UFOXD/datastream/pkg/utils"
)

// Dispatcher defines how events are dispatched to sinks.
type Dispatcher interface {
	// Dispatch dispatches an event to sinks.
	Dispatch(ctx context.Context, e *event.ChangeEvent, sinks []sink.Connector) error

	// Close closes the dispatcher.
	Close() error
}

// RoundRobinDispatcher dispatches events in round-robin fashion.
type RoundRobinDispatcher struct {
	counter uint64
	mu      sync.Mutex
}

// NewRoundRobinDispatcher creates a new round-robin dispatcher.
func NewRoundRobinDispatcher() *RoundRobinDispatcher {
	return &RoundRobinDispatcher{}
}

// Dispatch dispatches an event to the next sink in rotation.
func (d *RoundRobinDispatcher) Dispatch(ctx context.Context, e *event.ChangeEvent, sinks []sink.Connector) error {
	if len(sinks) == 0 {
		return ErrNoSink
	}

	d.mu.Lock()
	idx := d.counter % uint64(len(sinks))
	d.counter++
	d.mu.Unlock()

	return sinks[idx].Write(ctx, []*event.ChangeEvent{e})
}

// Close closes the dispatcher.
func (d *RoundRobinDispatcher) Close() error {
	return nil
}

// HashDispatcher dispatches events based on a hash of the key.
type HashDispatcher struct {
	hashKey string // Field name to hash on
}

// NewHashDispatcher creates a new hash-based dispatcher.
func NewHashDispatcher(hashKey string) *HashDispatcher {
	return &HashDispatcher{hashKey: hashKey}
}

// Dispatch dispatches an event based on hash of the key field.
func (d *HashDispatcher) Dispatch(ctx context.Context, e *event.ChangeEvent, sinks []sink.Connector) error {
	if len(sinks) == 0 {
		return ErrNoSink
	}

	// Get hash key value
	var keyVal string
	if e.IsDataEvent() {
		var row *event.RowData
		if !e.After.IsEmpty() {
			row = &e.After
		} else if !e.Before.IsEmpty() {
			row = &e.Before
		}

		if row != nil {
			if val, ok := row.Get(d.hashKey); ok {
				keyVal = strconv.FormatUint(uint64(utils.FNV32(val.(string))), 10)
			}
		}
	}

	// Use table name as fallback
	if keyVal == "" {
		keyVal = e.Table.Table
	}

	// Hash to determine sink
	hash := utils.FNV32(keyVal)
	idx := hash % uint32(len(sinks))

	return sinks[idx].Write(ctx, []*event.ChangeEvent{e})
}

// Close closes the dispatcher.
func (d *HashDispatcher) Close() error {
	return nil
}

// BroadcastDispatcher sends events to all sinks.
type BroadcastDispatcher struct{}

// NewBroadcastDispatcher creates a new broadcast dispatcher.
func NewBroadcastDispatcher() *BroadcastDispatcher {
	return &BroadcastDispatcher{}
}

// Dispatch broadcasts an event to all sinks.
func (d *BroadcastDispatcher) Dispatch(ctx context.Context, e *event.ChangeEvent, sinks []sink.Connector) error {
	if len(sinks) == 0 {
		return ErrNoSink
	}

	var lastErr error
	for _, s := range sinks {
		if err := s.Write(ctx, []*event.ChangeEvent{e}); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Close closes the dispatcher.
func (d *BroadcastDispatcher) Close() error {
	return nil
}

// NewDispatcher creates a dispatcher based on configuration.
func NewDispatcher(config DispatcherConfig) Dispatcher {
	switch config.Type {
	case "round-robin":
		return NewRoundRobinDispatcher()
	case "hash":
		return NewHashDispatcher(config.HashKey)
	case "broadcast":
		return NewBroadcastDispatcher()
	default:
		return NewRoundRobinDispatcher()
	}
}
