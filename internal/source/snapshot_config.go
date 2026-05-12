package source

import "errors"

// SnapshotConcurrencyConfig holds snapshot concurrency settings.
type SnapshotConcurrencyConfig struct {
	// MaxTableThreads is the max parallel tables during snapshot
	// Default: 4, Range: 1-16
	MaxTableThreads int `json:"max-table-threads" toml:"max-table-threads"`

	// EnableChunkParallel enables parallel chunk reading for large tables
	EnableChunkParallel bool `json:"enable-chunk-parallel" toml:"enable-chunk-parallel"`

	// MaxChunkThreads is the max parallel chunks per large table
	// Default: 4, Range: 1-8
	MaxChunkThreads int `json:"max-chunk-threads" toml:"max-chunk-threads"`

	// ChunkThreshold is the row count threshold for chunk parallelism
	ChunkThreshold int64 `json:"chunk-threshold" toml:"chunk-threshold"`

	// BatchSize is rows per read operation
	// Default: 1000, Range: 100-10000
	BatchSize int `json:"batch-size" toml:"batch-size"`

	// ChunkSize is rows per chunk for large tables
	ChunkSize int `json:"chunk-size" toml:"chunk-size"`

	// TaskQueueSize is the snapshot task queue size
	TaskQueueSize int `json:"task-queue-size" toml:"task-queue-size"`

	// EventBufferSize is the event buffer size
	EventBufferSize int `json:"event-buffer-size" toml:"event-buffer-size"`
}

// DefaultSnapshotConcurrencyConfig returns defaults.
func DefaultSnapshotConcurrencyConfig() *SnapshotConcurrencyConfig {
	return &SnapshotConcurrencyConfig{
		MaxTableThreads:     4,
		EnableChunkParallel: true,
		MaxChunkThreads:     4,
		ChunkThreshold:      100000,
		BatchSize:           1000,
		ChunkSize:           10000,
		TaskQueueSize:       1000,
		EventBufferSize:     10000,
	}
}

// Validate validates the configuration.
func (c *SnapshotConcurrencyConfig) Validate() error {
	if c.MaxTableThreads < 1 || c.MaxTableThreads > 16 {
		return errors.New("max-table-threads must be between 1 and 16")
	}
	if c.MaxChunkThreads < 1 || c.MaxChunkThreads > 8 {
		return errors.New("max-chunk-threads must be between 1 and 8")
	}
	if c.BatchSize < 100 || c.BatchSize > 10000 {
		return errors.New("batch-size must be between 100 and 10000")
	}
	if c.ChunkSize < 1000 {
		return errors.New("chunk-size must be at least 1000")
	}
	return nil
}

// ConcurrencyMode represents the concurrency mode for a table.
type ConcurrencyMode string

const (
	ConcurrencyModeSingle        ConcurrencyMode = "single"
	ConcurrencyModeChunkParallel ConcurrencyMode = "chunk-parallel"
)

// TableConcurrencyPlan holds the concurrency plan for a table.
type TableConcurrencyPlan struct {
	TableID      TableID
	Mode         ConcurrencyMode
	ChunkThreads int
	ChunkSize    int
	BatchSize    int
}

// ConcurrencyPlan holds the overall concurrency plan.
type ConcurrencyPlan struct {
	TablePlans map[string]*TableConcurrencyPlan
}

// SnapshotConcurrencyStrategy plans concurrency for tables.
type SnapshotConcurrencyStrategy struct {
	config *SnapshotConcurrencyConfig
}

// NewSnapshotConcurrencyStrategy creates a strategy.
func NewSnapshotConcurrencyStrategy(config *SnapshotConcurrencyConfig) *SnapshotConcurrencyStrategy {
	return &SnapshotConcurrencyStrategy{config: config}
}

// PlanConcurrency creates a concurrency plan for tables.
func (s *SnapshotConcurrencyStrategy) PlanConcurrency(tables []*TableInfo) *ConcurrencyPlan {
	plan := &ConcurrencyPlan{
		TablePlans: make(map[string]*TableConcurrencyPlan, len(tables)),
	}

	for _, t := range tables {
		tp := &TableConcurrencyPlan{
			TableID:   t.TableID,
			Mode:      ConcurrencyModeSingle,
			BatchSize: s.config.BatchSize,
			ChunkSize: s.config.ChunkSize,
		}

		if s.config.EnableChunkParallel && t.EstimatedRows >= s.config.ChunkThreshold {
			tp.Mode = ConcurrencyModeChunkParallel
			tp.ChunkThreads = s.config.MaxChunkThreads
		}

		plan.TablePlans[t.TableID.String()] = tp
	}

	return plan
}

// TableInfo holds table metadata for planning.
type TableInfo struct {
	TableID       TableID
	EstimatedRows int64
}
