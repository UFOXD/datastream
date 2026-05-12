// Package source provides source connectors for DataStream.
package source

import (
	"context"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/logutil"
	"go.uber.org/zap"
)

// SnapshotTask represents a snapshot task for a table or chunk.
type SnapshotTask struct {
	TableID    TableID
	Schema     *event.TableInfo
	ChunkID    int
	ChunkRange *ChunkRange
	Priority   int
}

// ChunkRange defines a range for chunked snapshot.
type ChunkRange struct {
	StartKey interface{}
	EndKey   interface{}
}

// SnapshotResult represents a snapshot result.
type SnapshotResult struct {
	TableID TableID
	ChunkID int
	Events  []*event.ChangeEvent
	Error   error
}

// SnapshotReader reads snapshot data from a source.
type SnapshotReader interface {
	// ReadSnapshot reads snapshot data for a task.
	ReadSnapshot(ctx context.Context, task *SnapshotTask) ([]*event.ChangeEvent, error)

	// GetKeyRange returns the min and max key values for a table.
	// Returns error if the table has no suitable key for range queries.
	GetKeyRange(table *event.TableInfo) (minKey, maxKey interface{}, err error)
}

// SnapshotProgress tracks snapshot progress.
type SnapshotProgress struct {
	mu             sync.RWMutex
	TotalTables    int
	CompletedTable int
	TotalRows      int64
	ReadRows       int64
	StartTime      time.Time
}

// TotalChunks returns the total number of chunks.
func (p *SnapshotProgress) TotalChunks() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return int64(p.TotalTables)
}

// CompletedChunks returns the number of completed chunks.
func (p *SnapshotProgress) CompletedChunks() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return int64(p.CompletedTable)
}

// Progress returns the progress percentage.
func (p *SnapshotProgress) Progress() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.TotalTables == 0 {
		return 0
	}
	return float64(p.CompletedTable) / float64(p.TotalTables) * 100
}

// Elapsed returns the elapsed time.
func (p *SnapshotProgress) Elapsed() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return time.Since(p.StartTime)
}

// SnapshotWorker handles snapshot tasks.
type SnapshotWorker struct {
	id       int
	taskCh   <-chan *SnapshotTask
	resultCh chan<- *SnapshotResult
	reader   SnapshotReader
}

// Run runs the worker.
func (w *SnapshotWorker) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-w.taskCh:
			if !ok {
				return
			}

			events, err := w.reader.ReadSnapshot(ctx, task)
			w.resultCh <- &SnapshotResult{
				TableID: task.TableID,
				ChunkID: task.ChunkID,
				Events:  events,
				Error:   err,
			}
		}
	}
}

// SnapshotCoordinator coordinates parallel snapshot execution.
// Per connector-design.md §6, it manages multi-threaded snapshot processing
// with chunk-level parallelism for large tables.
type SnapshotCoordinator struct {
	config   *SnapshotConcurrencyConfig
	reader   SnapshotReader
	taskCh   chan *SnapshotTask
	resultCh chan *SnapshotResult
	workers  []*SnapshotWorker
	progress *SnapshotProgress

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *zap.Logger
}

// NewSnapshotCoordinator creates a new snapshot coordinator.
func NewSnapshotCoordinator(config *SnapshotConcurrencyConfig, reader SnapshotReader) *SnapshotCoordinator {
	ctx, cancel := context.WithCancel(context.Background())

	return &SnapshotCoordinator{
		config:   config,
		reader:   reader,
		taskCh:   make(chan *SnapshotTask, config.TaskQueueSize),
		resultCh: make(chan *SnapshotResult, config.EventBufferSize),
		progress: &SnapshotProgress{},
		ctx:      ctx,
		cancel:   cancel,
		logger:   logutil.L(),
	}
}

// Start starts the coordinator.
func (c *SnapshotCoordinator) Start(tables []*event.TableInfo) error {
	c.progress.StartTime = time.Now()
	c.progress.TotalTables = len(tables)

	// Create workers
	c.workers = make([]*SnapshotWorker, c.config.MaxTableThreads)
	for i := 0; i < c.config.MaxTableThreads; i++ {
		worker := &SnapshotWorker{
			id:       i,
			taskCh:   c.taskCh,
			resultCh: c.resultCh,
			reader:   c.reader,
		}
		c.workers[i] = worker
		c.wg.Add(1)
		go worker.Run(c.ctx, &c.wg)
	}

	// Generate tasks in background
	go c.generateTasks(tables)

	// Start result handler
	go c.handleResults()

	return nil
}

// generateTasks generates snapshot tasks for tables.
// Per connector-design.md:1856-1892, it splits large tables into chunks.
func (c *SnapshotCoordinator) generateTasks(tables []*event.TableInfo) {
	defer close(c.taskCh)

	for _, table := range tables {
		tasks := c.generateTableTasks(table)
		for _, task := range tasks {
			select {
			case <-c.ctx.Done():
				return
			case c.taskCh <- task:
			}
		}
	}
}

// generateTableTasks generates snapshot tasks for a single table.
// Implements chunk-based parallelism per connector-design.md:1856-1892.
func (c *SnapshotCoordinator) generateTableTasks(table *event.TableInfo) []*SnapshotTask {
	tableID := TableID{
		Database: table.Database,
		Schema:   table.Schema,
		Table:    table.Table,
	}

	// Try to get key range for chunking
	minKey, maxKey, err := c.reader.GetKeyRange(table)
	if err != nil {
		// Cannot chunk - return single task for the whole table
		c.logger.Debug("cannot chunk table, using single task",
			zap.String("table", tableID.String()),
			zap.Error(err),
		)
		return []*SnapshotTask{{
			TableID:  tableID,
			Schema:   table,
			ChunkID:  0,
			Priority: 0,
		}}
	}

	// Generate chunked tasks
	var tasks []*SnapshotTask
	chunkID := 0

	// Use numeric range iteration
	// This supports int-based primary keys
	switch min := minKey.(type) {
	case int:
		max := maxKey.(int)
		for start := min; start <= max; start += c.config.ChunkSize {
			end := start + c.config.ChunkSize - 1
			if end > max {
				end = max
			}
			tasks = append(tasks, &SnapshotTask{
				TableID: tableID,
				Schema:  table,
				ChunkID: chunkID,
				ChunkRange: &ChunkRange{
					StartKey: start,
					EndKey:   end,
				},
				Priority: chunkID,
			})
			chunkID++
		}
	case int64:
		max := maxKey.(int64)
		for start := min; start <= max; start += int64(c.config.ChunkSize) {
			end := start + int64(c.config.ChunkSize) - 1
			if end > max {
				end = max
			}
			tasks = append(tasks, &SnapshotTask{
				TableID: tableID,
				Schema:  table,
				ChunkID: chunkID,
				ChunkRange: &ChunkRange{
					StartKey: start,
					EndKey:   end,
				},
				Priority: chunkID,
			})
			chunkID++
		}
	default:
		// Unsupported key type - return single task
		c.logger.Debug("unsupported key type for chunking",
			zap.String("table", tableID.String()),
			zap.Any("keyType", minKey),
		)
		return []*SnapshotTask{{
			TableID:  tableID,
			Schema:   table,
			ChunkID:  0,
			Priority: 0,
		}}
	}

	c.logger.Info("generated chunked tasks for table",
		zap.String("table", tableID.String()),
		zap.Int("chunks", len(tasks)),
	)

	return tasks
}

// handleResults handles snapshot results.
func (c *SnapshotCoordinator) handleResults() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case result, ok := <-c.resultCh:
			if !ok {
				return
			}
			if result.Error != nil {
				c.logger.Error("snapshot task failed",
					zap.String("table", result.TableID.String()),
					zap.Int("chunk", result.ChunkID),
					zap.Error(result.Error),
				)
			} else {
				c.progress.mu.Lock()
				c.progress.CompletedTable++
				c.progress.mu.Unlock()
			}
		}
	}
}

// Stop stops the coordinator.
func (c *SnapshotCoordinator) Stop() {
	c.cancel()
	c.wg.Wait()
}

// Progress returns current progress.
func (c *SnapshotCoordinator) Progress() *SnapshotProgress {
	return c.progress
}
