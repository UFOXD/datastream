package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/UFOXD/datastream/internal/cache"
	"github.com/UFOXD/datastream/internal/schema"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/internal/store"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/parser"
)

// EventSink writes processed change events downstream (e.g., to a target database or queue).
type EventSink interface {
	Write(ctx context.Context, events []*event.ChangeEvent) error
}

// maxDDLRetries is the maximum number of automatic DDL retry attempts
// before the table enters error state.
const maxDDLRetries = 3

// BinlogConsumer routes incoming binlog ChangeEvents to the appropriate destination
// based on the per-table lifecycle state.
type BinlogConsumer struct {
	taskID     string
	store      source.TableLifecycleStore
	cache      cache.BinlogCacheBackend
	sink       EventSink
	sourceType cache.SourceType

	// DDL handling dependencies.
	parser      parser.DDLParser
	tables      *schema.Tables
	ddlRecords  *schema.DDLRecordManager
	history     schema.SchemaHistory
	ddlStore    store.TargetStore

	// Flush mechanism: tracks in-flight DML writes so DDL can wait for them.
	pendingWg    sync.WaitGroup
}

// NewBinlogConsumer creates a new BinlogConsumer.
// The ddlParser, tables, ddlRecords, history, and ddlStore parameters are optional;
// when nil, DDL handling is disabled (DDL events are discarded).
func NewBinlogConsumer(
	taskID string,
	store source.TableLifecycleStore,
	cacheBackend cache.BinlogCacheBackend,
	sink EventSink,
	sourceType cache.SourceType,
	ddlParser parser.DDLParser,
	tables *schema.Tables,
	ddlRecords *schema.DDLRecordManager,
	history schema.SchemaHistory,
	ddlStore store.TargetStore,
) *BinlogConsumer {
	return &BinlogConsumer{
		taskID:     taskID,
		store:      store,
		cache:      cacheBackend,
		sink:       sink,
		sourceType: sourceType,
		parser:     ddlParser,
		tables:     tables,
		ddlRecords: ddlRecords,
		history:    history,
		ddlStore:   ddlStore,
	}
}

// Route dispatches a single ChangeEvent based on the table's lifecycle state.
// DDL events are handled synchronously: pending DMLs are flushed first,
// then the DDL is applied, and the result is recorded.
func (c *BinlogConsumer) Route(ctx context.Context, ev *event.ChangeEvent) error {
	// DDL events take a separate path.
	if ev.IsDDL() {
		return c.handleDDL(ctx, ev)
	}

	tableID := source.TableID{
		Database: ev.Table.Database,
		Schema:   ev.Table.Schema,
		Table:    ev.Table.Table,
	}

	// Check if this table is blocked by a failed DDL (may trigger retry).
	if c.checkDDLState(ctx, tableID.Database, tableID.Table) {
		return nil // table blocked by DDL — discard this DML event
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
		// Track in-flight DML so flush can wait for completion.
		c.pendingWg.Add(1)
		err := c.sink.Write(ctx, []*event.ChangeEvent{ev})
		c.pendingWg.Done()
		return err

	default:
		return nil
	}
}

// handleDDL processes a DDL event synchronously:
//  1. Flush all pending DML writes.
//  2. Record DDL state as "applying".
//  3. Parse and apply the DDL via the parser.
//  4. Execute the DDL on the sink.
//  5. On success: update Tables, record schema history, delete DDL state.
//  6. On failure: record DDL state as "failed", pause the table.
func (c *BinlogConsumer) handleDDL(ctx context.Context, ev *event.ChangeEvent) error {
	// DDL handling requires all dependencies. If not configured, discard.
	if c.parser == nil || c.tables == nil || c.ddlRecords == nil || c.ddlStore == nil {
		return nil
	}

	db := ev.Table.Database
	tbl := ev.Table.Table
	ddl := ev.Metadata["ddl"]
	if ddl == "" {
		return fmt.Errorf("DDL event missing 'ddl' metadata")
	}

	// Step 1: Flush — wait for all in-flight DML writes to complete.
	c.flushPending()

	// Step 2: Record DDL state as "applying".
	lastSuccessInfo := c.tables.Get(db, tbl)
	if err := c.ddlStore.SaveDDLState(ctx, &store.DDLStateRow{
		DBName:          db,
		TableName:       tbl,
		DDL:             ddl,
		LastSuccessInfo: lastSuccessInfo,
		Status:          "applying",
	}); err != nil {
		return fmt.Errorf("save ddl state (applying): %w", err)
	}

	// Step 3: Parse and apply DDL to get new table structure.
	oldTable := c.tables.Get(db, tbl)
	result, err := c.parser.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		return c.handleDDLFailure(ctx, db, tbl, ddl, lastSuccessInfo, err)
	}

	// Step 4: Execute DDL on the sink.
	if err := c.sink.Write(ctx, []*event.ChangeEvent{ev}); err != nil {
		return c.handleDDLFailure(ctx, db, tbl, ddl, lastSuccessInfo, err)
	}

	// Step 5a: Success — update in-memory Tables and persist history.
	if result.NewTableInfo != nil {
		c.tables.Put(result.NewTableInfo)
	} else {
		// DROP table: remove from Tables.
		c.tables.Remove(db, tbl)
	}

	changeType := inferChangeType(result, oldTable)
	if err := c.ddlStore.SaveSchemaHistory(ctx, &store.SchemaHistoryRow{
		Position:   ev.Position,
		DBName:     db,
		TableName:  tbl,
		DDL:        ddl,
		TableInfo:  result.NewTableInfo,
		ChangeType: changeType,
	}); err != nil {
		return fmt.Errorf("save schema history: %w", err)
	}

	// Delete DDL state on success.
	if err := c.ddlStore.DeleteDDLState(ctx, db, tbl); err != nil {
		return fmt.Errorf("delete ddl state: %w", err)
	}

	return nil
}

// handleDDLFailure records a DDL failure in the persistent store and pauses the table.
func (c *BinlogConsumer) handleDDLFailure(ctx context.Context, db, tbl, ddl string, lastSuccessInfo *event.TableInfo, applyErr error) error {
	// Load existing DDL state to get current retry count.
	existing, _ := c.ddlStore.LoadDDLState(ctx, db, tbl)
	retryCount := 0
	if existing != nil {
		retryCount = existing.RetryCount
	}
	retryCount++

	// Save failed state.
	if err := c.ddlStore.SaveDDLState(ctx, &store.DDLStateRow{
		DBName:          db,
		TableName:       tbl,
		DDL:             ddl,
		LastSuccessInfo: lastSuccessInfo,
		Status:          "failed",
		ErrorMsg:        applyErr.Error(),
		RetryCount:      retryCount,
	}); err != nil {
		return fmt.Errorf("save ddl state (failed): %w", err)
	}

	// Pause the table in the lifecycle store so no more DML is routed.
	tableID := source.TableID{Database: db, Table: tbl}
	lc, err := c.store.Get(ctx, c.taskID, tableID)
	if err == nil {
		_ = lc.Pause()
		_ = c.store.Save(ctx, c.taskID, lc)
	}

	return fmt.Errorf("DDL failed for %s.%s: %w", db, tbl, applyErr)
}

// flushPending waits for all in-flight DML writes to complete.
func (c *BinlogConsumer) flushPending() {
	c.pendingWg.Wait()
}

// checkDDLState checks if a table has a failed DDL that should be retried.
// If retry_count < maxDDLRetries, it triggers a retry.
// If retry_count >= maxDDLRetries, the table stays paused (error state).
// Returns true if the table is blocked by a failed DDL (caller should discard the event).
func (c *BinlogConsumer) checkDDLState(ctx context.Context, db, tbl string) bool {
	if c.ddlStore == nil {
		return false
	}

	ddlState, err := c.ddlStore.LoadDDLState(ctx, db, tbl)
	if err != nil || ddlState == nil {
		return false // no DDL state — not blocked
	}

	if ddlState.Status != "failed" {
		return true // applying or other state — blocked
	}

	if ddlState.RetryCount >= maxDDLRetries {
		return true // exceeded retries — stays blocked
	}

	// Retry the failed DDL.
	tableID := source.TableID{Database: db, Table: tbl}
	lc, err := c.store.Get(ctx, c.taskID, tableID)
	if err != nil {
		return true
	}

	// Resume the table temporarily so handleDDL can process it.
	_ = lc.Resume()
	_ = c.store.Save(ctx, c.taskID, lc)

	// Build a synthetic DDL event for retry.
	retryEv := &event.ChangeEvent{
		Type: event.EventTypeDDL,
		Table: event.TableInfo{
			Database: db,
			Table:    tbl,
		},
		Metadata: map[string]string{
			"ddl": ddlState.DDL,
		},
	}

	// handleDDL will flush, apply, and record success/failure.
	_ = c.handleDDL(ctx, retryEv)
	return true
}

// inferChangeType determines the DDL change type from the parse result.
func inferChangeType(result *parser.DDLResult, oldTable *event.TableInfo) string {
	if result.NewTableInfo == nil {
		return "DROP"
	}
	if oldTable != nil {
		return "ALTER"
	}
	return "CREATE"
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
