package sqlserver

import (
	"context"
	"math"

	"github.com/UFOXD/datastream/internal/connector"
	"github.com/UFOXD/datastream/internal/source"
)

var _ connector.StatsProvider = (*Connector)(nil)

// Stats implements connector.StatsProvider. Position renders CDC LSN.
func (c *Connector) Stats(ctx context.Context) connector.Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s := connector.Stats{
		Connected:  c.status.State == source.StateRunning,
		LagSeconds: math.NaN(),
	}
	if c.position != nil {
		s.Position = c.position.String()
	}
	return s
}
