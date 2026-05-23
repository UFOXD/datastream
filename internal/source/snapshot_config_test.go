package source

import (
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
)

func TestSnapshotConcurrencyConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *SnapshotConcurrencyConfig
		wantErr bool
		errMsg  string
	}{
		{
			name:    "default config is valid",
			config:  DefaultSnapshotConcurrencyConfig(),
			wantErr: false,
		},
		{
			name: "MaxTableThreads below minimum",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 0,
				MaxChunkThreads: 4,
				BatchSize:       1000,
				ChunkSize:       10000,
			},
			wantErr: true,
			errMsg:  "max-table-threads must be between 1 and 16",
		},
		{
			name: "MaxTableThreads above maximum",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 17,
				MaxChunkThreads: 4,
				BatchSize:       1000,
				ChunkSize:       10000,
			},
			wantErr: true,
			errMsg:  "max-table-threads must be between 1 and 16",
		},
		{
			name: "MaxTableThreads at boundary 1",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 1,
				MaxChunkThreads: 4,
				BatchSize:       1000,
				ChunkSize:       10000,
			},
			wantErr: false,
		},
		{
			name: "MaxTableThreads at boundary 16",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 16,
				MaxChunkThreads: 4,
				BatchSize:       1000,
				ChunkSize:       10000,
			},
			wantErr: false,
		},
		{
			name: "MaxChunkThreads below minimum",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 4,
				MaxChunkThreads: 0,
				BatchSize:       1000,
				ChunkSize:       10000,
			},
			wantErr: true,
			errMsg:  "max-chunk-threads must be between 1 and 8",
		},
		{
			name: "MaxChunkThreads above maximum",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 4,
				MaxChunkThreads: 9,
				BatchSize:       1000,
				ChunkSize:       10000,
			},
			wantErr: true,
			errMsg:  "max-chunk-threads must be between 1 and 8",
		},
		{
			name: "MaxChunkThreads at boundary 8",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 4,
				MaxChunkThreads: 8,
				BatchSize:       1000,
				ChunkSize:       10000,
			},
			wantErr: false,
		},
		{
			name: "BatchSize below minimum",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 4,
				MaxChunkThreads: 4,
				BatchSize:       99,
				ChunkSize:       10000,
			},
			wantErr: true,
			errMsg:  "batch-size must be between 100 and 10000",
		},
		{
			name: "BatchSize above maximum",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 4,
				MaxChunkThreads: 4,
				BatchSize:       10001,
				ChunkSize:       10000,
			},
			wantErr: true,
			errMsg:  "batch-size must be between 100 and 10000",
		},
		{
			name: "BatchSize at boundary 100",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 4,
				MaxChunkThreads: 4,
				BatchSize:       100,
				ChunkSize:       10000,
			},
			wantErr: false,
		},
		{
			name: "BatchSize at boundary 10000",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 4,
				MaxChunkThreads: 4,
				BatchSize:       10000,
				ChunkSize:       10000,
			},
			wantErr: false,
		},
		{
			name: "ChunkSize below minimum",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 4,
				MaxChunkThreads: 4,
				BatchSize:       1000,
				ChunkSize:       999,
			},
			wantErr: true,
			errMsg:  "chunk-size must be at least 1000",
		},
		{
			name: "ChunkSize at minimum 1000",
			config: &SnapshotConcurrencyConfig{
				MaxTableThreads: 4,
				MaxChunkThreads: 4,
				BatchSize:       1000,
				ChunkSize:       1000,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && err != nil && err.Error() != tt.errMsg {
				t.Errorf("Validate() error message = %q, want %q", err.Error(), tt.errMsg)
			}
		})
	}
}

func TestSnapshotConcurrencyStrategy_PlanConcurrency(t *testing.T) {
	cfg := DefaultSnapshotConcurrencyConfig()
	strategy := NewSnapshotConcurrencyStrategy(cfg)

	t.Run("empty tables", func(t *testing.T) {
		plan := strategy.PlanConcurrency(nil)
		if plan == nil {
			t.Fatal("PlanConcurrency() returned nil")
		}
		if len(plan.TablePlans) != 0 {
			t.Errorf("expected 0 table plans, got %d", len(plan.TablePlans))
		}
	})

	t.Run("small table uses single mode", func(t *testing.T) {
		tables := []*TableInfo{
			{
				TableID:       TableID{Database: "db1", Table: "small_table"},
				EstimatedRows: 1000, // below ChunkThreshold of 100000
			},
		}
		plan := strategy.PlanConcurrency(tables)
		key := "db1.small_table"
		tp, ok := plan.TablePlans[key]
		if !ok {
			t.Fatalf("plan missing entry for %q", key)
		}
		if tp.Mode != ConcurrencyModeSingle {
			t.Errorf("Mode = %q, want %q", tp.Mode, ConcurrencyModeSingle)
		}
		if tp.ChunkThreads != 0 {
			t.Errorf("ChunkThreads = %d, want 0 for single mode", tp.ChunkThreads)
		}
		if tp.BatchSize != cfg.BatchSize {
			t.Errorf("BatchSize = %d, want %d", tp.BatchSize, cfg.BatchSize)
		}
		if tp.ChunkSize != cfg.ChunkSize {
			t.Errorf("ChunkSize = %d, want %d", tp.ChunkSize, cfg.ChunkSize)
		}
	})

	t.Run("large table uses chunk-parallel mode", func(t *testing.T) {
		tables := []*TableInfo{
			{
				TableID:       TableID{Database: "db1", Table: "large_table"},
				EstimatedRows: 500000, // above ChunkThreshold of 100000
			},
		}
		plan := strategy.PlanConcurrency(tables)
		key := "db1.large_table"
		tp, ok := plan.TablePlans[key]
		if !ok {
			t.Fatalf("plan missing entry for %q", key)
		}
		if tp.Mode != ConcurrencyModeChunkParallel {
			t.Errorf("Mode = %q, want %q", tp.Mode, ConcurrencyModeChunkParallel)
		}
		if tp.ChunkThreads != cfg.MaxChunkThreads {
			t.Errorf("ChunkThreads = %d, want %d", tp.ChunkThreads, cfg.MaxChunkThreads)
		}
	})

	t.Run("table at threshold uses chunk-parallel mode", func(t *testing.T) {
		tables := []*TableInfo{
			{
				TableID:       TableID{Database: "db1", Table: "threshold_table"},
				EstimatedRows: cfg.ChunkThreshold, // exactly at threshold
			},
		}
		plan := strategy.PlanConcurrency(tables)
		key := "db1.threshold_table"
		tp, ok := plan.TablePlans[key]
		if !ok {
			t.Fatalf("plan missing entry for %q", key)
		}
		if tp.Mode != ConcurrencyModeChunkParallel {
			t.Errorf("Mode = %q, want %q", tp.Mode, ConcurrencyModeChunkParallel)
		}
	})

	t.Run("chunk parallel disabled forces single mode for large table", func(t *testing.T) {
		noCfg := DefaultSnapshotConcurrencyConfig()
		noCfg.EnableChunkParallel = false
		noParallelStrategy := NewSnapshotConcurrencyStrategy(noCfg)

		tables := []*TableInfo{
			{
				TableID:       TableID{Database: "db1", Table: "large_table"},
				EstimatedRows: 500000,
			},
		}
		plan := noParallelStrategy.PlanConcurrency(tables)
		key := "db1.large_table"
		tp, ok := plan.TablePlans[key]
		if !ok {
			t.Fatalf("plan missing entry for %q", key)
		}
		if tp.Mode != ConcurrencyModeSingle {
			t.Errorf("Mode = %q, want %q when chunk parallel disabled", tp.Mode, ConcurrencyModeSingle)
		}
		if tp.ChunkThreads != 0 {
			t.Errorf("ChunkThreads = %d, want 0 when chunk parallel disabled", tp.ChunkThreads)
		}
	})

	t.Run("multiple tables mixed sizes", func(t *testing.T) {
		tables := []*TableInfo{
			{TableID: TableID{Database: "db1", Table: "small"}, EstimatedRows: 500},
			{TableID: TableID{Database: "db1", Table: "large"}, EstimatedRows: 200000},
			{TableID: TableID{Database: "db2", Table: "medium"}, EstimatedRows: 50000},
		}
		plan := strategy.PlanConcurrency(tables)
		if len(plan.TablePlans) != 3 {
			t.Errorf("expected 3 table plans, got %d", len(plan.TablePlans))
		}

		if tp := plan.TablePlans["db1.small"]; tp == nil || tp.Mode != ConcurrencyModeSingle {
			t.Errorf("db1.small: expected single mode")
		}
		if tp := plan.TablePlans["db1.large"]; tp == nil || tp.Mode != ConcurrencyModeChunkParallel {
			t.Errorf("db1.large: expected chunk-parallel mode")
		}
		if tp := plan.TablePlans["db2.medium"]; tp == nil || tp.Mode != ConcurrencyModeSingle {
			t.Errorf("db2.medium: expected single mode (below threshold)")
		}
	})

	t.Run("table plan carries correct TableID", func(t *testing.T) {
		tables := []*TableInfo{
			{
				TableID:       TableID{Database: "mydb", Table: "mytable"},
				EstimatedRows: 0,
			},
		}
		plan := strategy.PlanConcurrency(tables)
		tp := plan.TablePlans["mydb.mytable"]
		if tp == nil {
			t.Fatal("plan missing entry")
		}
		if tp.TableID.Database != "mydb" || tp.TableID.Table != "mytable" {
			t.Errorf("TableID = %v, want mydb.mytable", tp.TableID)
		}
	})
}

func TestDefaultSnapshotConcurrencyConfig(t *testing.T) {
	cfg := DefaultSnapshotConcurrencyConfig()
	if cfg.MaxTableThreads != 4 {
		t.Errorf("MaxTableThreads = %d, want 4", cfg.MaxTableThreads)
	}
	if !cfg.EnableChunkParallel {
		t.Error("EnableChunkParallel = false, want true")
	}
	if cfg.MaxChunkThreads != 4 {
		t.Errorf("MaxChunkThreads = %d, want 4", cfg.MaxChunkThreads)
	}
	if cfg.ChunkThreshold != 100000 {
		t.Errorf("ChunkThreshold = %d, want 100000", cfg.ChunkThreshold)
	}
	if cfg.BatchSize != 1000 {
		t.Errorf("BatchSize = %d, want 1000", cfg.BatchSize)
	}
	if cfg.ChunkSize != 10000 {
		t.Errorf("ChunkSize = %d, want 10000", cfg.ChunkSize)
	}
	if cfg.TaskQueueSize != 1000 {
		t.Errorf("TaskQueueSize = %d, want 1000", cfg.TaskQueueSize)
	}
	if cfg.EventBufferSize != 10000 {
		t.Errorf("EventBufferSize = %d, want 10000", cfg.EventBufferSize)
	}

	// Default config must pass validation
	if err := cfg.Validate(); err != nil {
		t.Errorf("DefaultSnapshotConcurrencyConfig().Validate() = %v, want nil", err)
	}
}

func TestSnapshotModeWhenNeeded(t *testing.T) {
	// when_needed should behave like initial: snapshot when no position exists
	cfg := &SnapshotConfig{Mode: SnapshotModeWhenNeeded}

	// No saved position -> should snapshot
	if !cfg.ShouldSnapshot(nil) {
		t.Error("ShouldSnapshot(nil) = false, want true for when_needed with no saved position")
	}

	// Saved position exists -> should NOT snapshot
	pos := &event.Position{BinlogFile: "mysql-bin.000001"}
	if cfg.ShouldSnapshot(pos) {
		t.Error("ShouldSnapshot(pos) = true, want false for when_needed with saved position")
	}
}

func TestShouldSnapshot(t *testing.T) {
	pos := &event.Position{BinlogFile: "mysql-bin.000001"}

	tests := []struct {
		name     string
		mode     SnapshotMode
		savedPos *event.Position
		want     bool
	}{
		{name: "never with nil pos", mode: SnapshotModeNever, savedPos: nil, want: false},
		{name: "never with pos", mode: SnapshotModeNever, savedPos: pos, want: false},
		{name: "always with nil pos", mode: SnapshotModeAlways, savedPos: nil, want: true},
		{name: "always with pos", mode: SnapshotModeAlways, savedPos: pos, want: true},
		{name: "initial with nil pos", mode: SnapshotModeInitial, savedPos: nil, want: true},
		{name: "initial with pos", mode: SnapshotModeInitial, savedPos: pos, want: false},
		{name: "when_needed with nil pos", mode: SnapshotModeWhenNeeded, savedPos: nil, want: true},
		{name: "when_needed with pos", mode: SnapshotModeWhenNeeded, savedPos: pos, want: false},
		{name: "unknown mode with nil pos", mode: SnapshotMode("unknown"), savedPos: nil, want: false},
		{name: "unknown mode with pos", mode: SnapshotMode("unknown"), savedPos: pos, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &SnapshotConfig{Mode: tt.mode}
			got := cfg.ShouldSnapshot(tt.savedPos)
			if got != tt.want {
				t.Errorf("ShouldSnapshot() = %v, want %v", got, tt.want)
			}
		})
	}
}
