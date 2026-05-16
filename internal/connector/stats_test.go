package connector

import (
	"context"
	"testing"
)

type mockProvider struct{}

func (m *mockProvider) Stats(ctx context.Context) Stats {
	return Stats{QueueSize: 1, Connected: true}
}

func TestStatsProvider_InterfaceSatisfied(t *testing.T) {
	var _ StatsProvider = (*mockProvider)(nil)
}

func TestStats_ZeroValueIsNotApplicable(t *testing.T) {
	var s Stats
	if s.QueueSize != 0 {
		t.Error("expected zero QueueSize")
	}
	if s.Connected {
		t.Error("expected Connected=false default")
	}
}
