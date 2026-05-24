package lifecycle

import "time"

type SchedulerConfig struct {
	MaxTableThreads int           `toml:"max-table-threads"`
	MaxChunkThreads int           `toml:"max-chunk-threads"`
	ChunkThreshold  int64         `toml:"chunk-threshold"`
	SmartOrder      bool          `toml:"smart-order"`
	MaxRetries      int           `toml:"max-retries"`
	RetryInterval   time.Duration `toml:"retry-interval"`
	BatchSize       int           `toml:"batch-size"`
	UpsertDuration  time.Duration `toml:"upsert-duration"`
	TargetMode      string        `toml:"target-mode"`
}

func DefaultSchedulerConfig() *SchedulerConfig {
	return &SchedulerConfig{
		MaxTableThreads: 4,
		MaxChunkThreads: 4,
		ChunkThreshold:  1000000,
		SmartOrder:      true,
		MaxRetries:      3,
		RetryInterval:   5 * time.Minute,
		BatchSize:       1000,
		UpsertDuration:  time.Minute,
		TargetMode:      "drop-create-insert",
	}
}
