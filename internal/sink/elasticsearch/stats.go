package elasticsearch

import (
	"context"
	"math"

	"github.com/UFOXD/datastream/internal/connector"
	"github.com/UFOXD/datastream/internal/sink"
)

var _ connector.StatsProvider = (*Connector)(nil)

func (c *Connector) Stats(ctx context.Context) connector.Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s := connector.Stats{
		Connected:  isConnected(c.status.State),
		LagSeconds: math.NaN(),
	}
	if c.position != nil {
		s.Position = c.position.String()
	}
	return s
}

func isConnected(st sink.State) bool {
	switch st {
	case sink.StateReady, sink.StateWriting, sink.StateFlushing:
		return true
	}
	return false
}
