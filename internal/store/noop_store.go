package store

import (
	"context"

	"github.com/UFOXD/datastream/pkg/event"
)

// NoopStore implements TargetStore as a no-op.
// Used when the target store is not configured or for testing.
type NoopStore struct{}

// NewNoopStore creates a new NoopStore.
func NewNoopStore() *NoopStore {
	return &NoopStore{}
}

func (s *NoopStore) InitDatabase(_ context.Context) error                                       { return nil }
func (s *NoopStore) SaveFlushedPosition(_ context.Context, _ *event.Position) error             { return nil }
func (s *NoopStore) SaveCurrentPosition(_ context.Context, _ *event.Position) error             { return nil }
func (s *NoopStore) LoadPositions(_ context.Context) (flushed, current *event.Position, err error) {
	return nil, nil, nil
}
func (s *NoopStore) SaveTableLifecycle(_ context.Context, _, _, _ string, _ *event.Position, _ string) error {
	return nil
}
func (s *NoopStore) LoadTableLifecycles(_ context.Context) ([]*TableLifecycleRow, error) {
	return nil, nil
}
func (s *NoopStore) DeleteTableLifecycle(_ context.Context, _, _ string) error { return nil }
func (s *NoopStore) SaveSchemaHistory(_ context.Context, _ *SchemaHistoryRow) error {
	return nil
}
func (s *NoopStore) LoadSchemaHistory(_ context.Context) ([]*SchemaHistoryRow, error) {
	return nil, nil
}
func (s *NoopStore) SaveDDLState(_ context.Context, _ *DDLStateRow) error    { return nil }
func (s *NoopStore) LoadDDLState(_ context.Context, _, _ string) (*DDLStateRow, error) {
	return nil, nil
}
func (s *NoopStore) LoadPendingDDLStates(_ context.Context) ([]*DDLStateRow, error) {
	return nil, nil
}
func (s *NoopStore) DeleteDDLState(_ context.Context, _, _ string) error     { return nil }
func (s *NoopStore) SaveCommittedPosition(_ context.Context, _ string) error { return nil }
func (s *NoopStore) LoadCommittedPosition(_ context.Context) (string, error) { return "", nil }
func (s *NoopStore) Close() error                                            { return nil }
