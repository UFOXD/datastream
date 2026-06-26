// Package mysql provides a MySQL source connector for DataStream.
package mysql

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/internal/schema"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/parser"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// BinlogSyncer wraps the replication.BinlogSyncer and handles binlog streaming.
// This replaces the canal.Canal usage with direct replication package usage.
type BinlogSyncer struct {
	config      *Config
	syncer      *replication.BinlogSyncer
	streamer    *replication.BinlogStreamer
	parser      parser.DDLParser
	schemaCache *TableSchemaCache
	tables      *schema.Tables // in-memory table definitions from SchemaHistory

	// syncScope is the syncer's own deep copy of the scope.
	// Protected by syncScopeMu so the binlog goroutine and Connector
	// mutations (AddTables/RemoveTables) never race.
	syncScope   *source.SyncScope
	syncScopeMu sync.RWMutex

	// Event channels
	events chan *event.ChangeEvent
	errors chan error

	// Position tracking
	position   *event.Position
	positionMu sync.RWMutex
	currentFile string

	// Table column type cache (tableID -> column types)
	tableColumnTypes map[uint64][]byte
	tableColumnMetas map[uint64][]uint16
	tableMu          sync.RWMutex

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewBinlogSyncer creates a new binlog syncer.
// It stores a deep copy of syncScope so that mutations on the Connector's
// copy (e.g. AddTables/RemoveTables) do not race with the binlog goroutine.
func NewBinlogSyncer(config *Config, syncScope *source.SyncScope, schemaCache *TableSchemaCache, tables *schema.Tables, events chan *event.ChangeEvent, errors chan error) *BinlogSyncer {
	return &BinlogSyncer{
		config:           config,
		syncScope:        syncScope.Clone(),
		schemaCache:      schemaCache,
		tables:           tables,
		events:           events,
		errors:           errors,
		tableColumnTypes: make(map[uint64][]byte),
		tableColumnMetas: make(map[uint64][]uint16),
	}
}

// UpdateSyncScope replaces the syncer's internal scope with a deep copy of
// the provided scope. Safe to call from any goroutine while the binlog
// goroutine is running.
func (s *BinlogSyncer) UpdateSyncScope(newScope *source.SyncScope) {
	s.syncScopeMu.Lock()
	s.syncScope = newScope.Clone()
	s.syncScopeMu.Unlock()
}

// Start starts the binlog syncer.
func (s *BinlogSyncer) Start(ctx context.Context, startPos *event.Position) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Create binlog syncer config
	syncerCfg := replication.BinlogSyncerConfig{
		ServerID:         s.config.ServerID,
		Flavor:           "mysql",
		Host:             s.config.Host,
		Port:             uint16(s.config.Port),
		User:             s.config.User,
		Password:         s.config.Password,
		Charset:          "utf8mb4",
		RawModeEnabled:   false,
		SemiSyncEnabled:  false,
		ParseTime:        true,
	}

	// Create the syncer
	s.syncer = replication.NewBinlogSyncer(syncerCfg)

	// Get DDL parser from registry
	if p := parser.DefaultRegistry.Get("mysql"); p != nil {
		s.parser = p
	} else {
		log.Warn("MySQL DDL parser not found, DDL events will be passed as raw SQL")
	}

	// Start streaming from position
	var err error
	if startPos != nil && startPos.BinlogFile != "" {
		// Stream from specific position
		pos := mysql.Position{
			Name: startPos.BinlogFile,
			Pos:  startPos.BinlogPos,
		}
		s.streamer, err = s.syncer.StartSync(pos)
		s.currentFile = startPos.BinlogFile
		s.position = startPos.Clone()
		log.Info("starting binlog sync from position",
			zap.String("file", startPos.BinlogFile),
			zap.Uint32("pos", startPos.BinlogPos))
	} else {
		// Stream from latest position
		s.streamer, err = s.syncer.StartSync(mysql.Position{})
		log.Info("starting binlog sync from latest position")
	}

	if err != nil {
		return fmt.Errorf("failed to start binlog syncer: %w", err)
	}

	// Start event processing goroutine
	s.wg.Add(1)
	go s.run()

	return nil
}

// Stop stops the binlog syncer.
func (s *BinlogSyncer) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()

	if s.syncer != nil {
		s.syncer.Close()
	}
	return nil
}

// GetPosition returns the current position.
func (s *BinlogSyncer) GetPosition() *event.Position {
	s.positionMu.RLock()
	defer s.positionMu.RUnlock()
	if s.position == nil {
		return nil
	}
	return s.position.Clone()
}

// run is the main event processing loop.
func (s *BinlogSyncer) run() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			// Get next binlog event with timeout
			ev, err := s.streamer.GetEvent(s.ctx)
			if err != nil {
				if s.ctx.Err() != nil {
					// Context cancelled, normal exit
					return
				}
				log.Error("failed to get binlog event", zap.Error(err))
				select {
				case s.errors <- err:
				case <-time.After(5 * time.Second):
					log.Warn("error channel full, dropping error", zap.Error(err))
				}
				continue
			}

			// Process the event
			if err := s.processEvent(ev); err != nil {
				log.Error("failed to process binlog event", zap.Error(err))
				select {
				case s.errors <- err:
				case <-time.After(5 * time.Second):
					log.Warn("error channel full, dropping error", zap.Error(err))
				}
			}
		}
	}
}

// processEvent processes a single binlog event.
func (s *BinlogSyncer) processEvent(ev *replication.BinlogEvent) error {
	switch ev.Event.(type) {
	case *replication.RotateEvent:
		return s.handleRotateEvent(ev)
	case *replication.QueryEvent:
		return s.handleQueryEvent(ev)
	case *replication.TableMapEvent:
		return s.handleTableMapEvent(ev)
	case *replication.RowsEvent:
		return s.handleRowsEvent(ev)
	case *replication.XIDEvent:
		return s.handleXIDEvent(ev)
	case *replication.GTIDEvent:
		return s.handleGTIDEvent(ev)
	}
	return nil
}

// handleRotateEvent handles binlog file rotation.
func (s *BinlogSyncer) handleRotateEvent(ev *replication.BinlogEvent) error {
	rotateEvent, ok := ev.Event.(*replication.RotateEvent)
	if !ok {
		return nil
	}

	s.positionMu.Lock()
	s.currentFile = string(rotateEvent.NextLogName)
	s.positionMu.Unlock()

	log.Info("binlog rotation",
		zap.String("file", string(rotateEvent.NextLogName)),
		zap.Uint64("position", rotateEvent.Position))

	return nil
}

// handleTableMapEvent handles table map events to cache column type info.
func (s *BinlogSyncer) handleTableMapEvent(ev *replication.BinlogEvent) error {
	tableMapEvent, ok := ev.Event.(*replication.TableMapEvent)
	if !ok {
		return nil
	}

	// Cache column type information for this table
	s.tableMu.Lock()
	s.tableColumnTypes[tableMapEvent.TableID] = tableMapEvent.ColumnType
	s.tableColumnMetas[tableMapEvent.TableID] = tableMapEvent.ColumnMeta
	s.tableMu.Unlock()

	return nil
}

// handleQueryEvent handles DDL and other query events.
func (s *BinlogSyncer) handleQueryEvent(ev *replication.BinlogEvent) error {
	queryEvent, ok := ev.Event.(*replication.QueryEvent)
	if !ok {
		return nil
	}

	query := string(queryEvent.Query)
	if !isDDL(query) {
		return nil
	}

	database := string(queryEvent.Schema)

	log.Info("DDL event received",
		zap.String("query", query),
		zap.String("database", database))

	// Parse DDL using the parser
	var ddlResult *parser.DDLResult
	if s.parser != nil {
		results, err := s.parser.Parse(s.ctx, query)
		if err != nil {
			log.Warn("failed to parse DDL, passing as raw SQL",
				zap.String("query", query),
				zap.Error(err))
		} else if len(results) > 0 {
			ddlResult = results[0]
		}
	}

	// Build DDL change event
	changeEvent := &event.ChangeEvent{
		ID:   event.GenerateEventID(&event.SourceInfo{Connector: "mysql"}, time.Now(), int(ev.Header.LogPos)),
		Type: event.EventTypeDDL,
		Source: event.SourceInfo{
			Connector: "mysql",
			Database:  database,
		},
		Timestamp: time.Unix(int64(ev.Header.Timestamp), 0),
		Position: event.Position{
			BinlogFile: s.currentFile,
			BinlogPos:  ev.Header.LogPos,
			CommitTime: time.Unix(int64(ev.Header.Timestamp), 0),
		},
	}

	// Store DDL result in metadata
	if ddlResult != nil {
		changeEvent.Table = event.TableInfo{
			Database: ddlResult.Database,
			Table:    ddlResult.Table,
		}
		// Store parsed DDL info in metadata
		changeEvent.Metadata = map[string]string{
			"ddl":          query,
			"ddlType":      string(ddlResult.Type),
			"ddlDatabase":  ddlResult.Database,
			"ddlTable":     ddlResult.Table,
			"ddlStatement": ddlResult.Statement,
		}
	} else {
		changeEvent.Metadata = map[string]string{
			"ddl": query,
		}
	}

	// Invalidate schema cache for affected table
	if ddlResult != nil && ddlResult.Table != "" {
		s.schemaCache.Invalidate(ddlResult.Database, ddlResult.Table)
	} else {
		s.schemaCache.InvalidateAll()
	}

	// Apply DDL to update in-memory Tables
	if s.tables != nil && ddlResult != nil && ddlResult.Table != "" {
		oldTable := s.tables.Get(ddlResult.Database, ddlResult.Table)
		applyResult, err := s.parser.ApplyDDL(s.ctx, oldTable, query)
		if err == nil && applyResult != nil {
			if applyResult.NewTableInfo != nil {
				s.tables.Put(applyResult.NewTableInfo)
			} else if ddlResult.Type == parser.DDLTypeDropTable {
				s.tables.Remove(ddlResult.Database, ddlResult.Table)
			}
		}
	}

	// Update position
	s.positionMu.Lock()
	s.position = &changeEvent.Position
	s.positionMu.Unlock()

	// Send event
	select {
	case s.events <- changeEvent:
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout sending DDL event")
	}

	return nil
}

// handleRowsEvent handles row change events (INSERT/UPDATE/DELETE).
func (s *BinlogSyncer) handleRowsEvent(ev *replication.BinlogEvent) error {
	rowsEvent, ok := ev.Event.(*replication.RowsEvent)
	if !ok {
		return nil
	}

	database := string(rowsEvent.Table.Schema)
	table := string(rowsEvent.Table.Table)

	// Check if we should capture this table
	if !s.shouldCapture(database, table) {
		return nil
	}

	// Get table schema: prefer in-memory Tables (from SchemaHistory), fallback to schemaCache
	var tableInfo *event.TableInfo
	if s.tables != nil {
		tableInfo = s.tables.Get(database, table)
	}
	if tableInfo == nil {
		var err error
		tableInfo, err = s.schemaCache.Get(s.ctx, database, table)
		if err != nil {
			log.Warn("failed to get table schema, using minimal info",
				zap.String("database", database),
				zap.String("table", table),
				zap.Error(err))
			tableInfo = &event.TableInfo{
				Database: database,
				Table:    table,
			}
		}
	}

	// Determine event type
	var eventType event.EventType
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		eventType = event.EventTypeInsert
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		eventType = event.EventTypeUpdate
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		eventType = event.EventTypeDelete
	default:
		return nil
	}

	// Get column types from cache
	s.tableMu.RLock()
	colTypes := s.tableColumnTypes[rowsEvent.TableID]
	colMetas := s.tableColumnMetas[rowsEvent.TableID]
	s.tableMu.RUnlock()

	// Build change events from rows
	events := s.buildChangeEvents(eventType, ev.Header, tableInfo, rowsEvent, colTypes, colMetas)

	// Send events
	for _, changeEvent := range events {
		// Update position
		s.positionMu.Lock()
		s.position = &changeEvent.Position
		s.positionMu.Unlock()

		select {
		case s.events <- changeEvent:
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout sending change event")
		}
	}

	return nil
}

// handleXIDEvent handles transaction commit events.
func (s *BinlogSyncer) handleXIDEvent(ev *replication.BinlogEvent) error {
	// Update position on transaction commit
	s.positionMu.Lock()
	s.position = &event.Position{
		BinlogFile: s.currentFile,
		BinlogPos:  ev.Header.LogPos,
		CommitTime: time.Unix(int64(ev.Header.Timestamp), 0),
	}
	s.positionMu.Unlock()

	return nil
}

// handleGTIDEvent captures the GTID from the GTID event into the current position.
func (s *BinlogSyncer) handleGTIDEvent(ev *replication.BinlogEvent) error {
	gtidEv, ok := ev.Event.(*replication.GTIDEvent)
	if !ok {
		return nil
	}

	// SID is 16 bytes (UUID). Format as standard UUID string.
	sid := gtidEv.SID
	uuid := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		sid[0:4], sid[4:6], sid[6:8], sid[8:10], sid[10:16])
	gtid := fmt.Sprintf("%s:%d", uuid, gtidEv.GNO)

	s.positionMu.Lock()
	if s.position == nil {
		s.position = &event.Position{}
	}
	s.position.TxID = gtid
	s.positionMu.Unlock()

	return nil
}

// shouldCapture checks if a table should be captured.
// It prefers SyncScope when set, falling back to legacy config.Databases/Tables.
func (s *BinlogSyncer) shouldCapture(database, table string) bool {
	// Use SyncScope when available. Take a read lock so this is safe
	// to call concurrently with UpdateSyncScope.
	s.syncScopeMu.RLock()
	scope := s.syncScope
	s.syncScopeMu.RUnlock()

	if scope != nil {
		switch scope.Level {
		case source.SyncLevelDatabase:
			return scope.Databases.ShouldSyncTable(database, table)
		case source.SyncLevelTable:
			return scope.Tables.ShouldSyncTable(database, table)
		}
	}

	// Fallback: legacy config.Databases / config.Tables
	if len(s.config.Databases) == 0 {
		return true
	}

	for _, db := range s.config.Databases {
		if db == database || db == "*" {
			return true
		}
	}

	// Check table patterns
	for db, pattern := range s.config.Tables {
		if db == database || db == "*" {
			if pattern == "*" || pattern == "" {
				return true
			}
			if matchPattern(pattern, table) {
				return true
			}
		}
	}

	return false
}

// buildChangeEvents builds change events from rows event.
func (s *BinlogSyncer) buildChangeEvents(eventType event.EventType, header *replication.EventHeader, tableInfo *event.TableInfo, rowsEvent *replication.RowsEvent, colTypes []byte, colMetas []uint16) []*event.ChangeEvent {
	var events []*event.ChangeEvent

	switch eventType {
	case event.EventTypeInsert:
		for _, row := range rowsEvent.Rows {
			afterData := s.buildRowData(tableInfo.Columns, row)
			events = append(events, &event.ChangeEvent{
				ID:        event.GenerateEventID(&event.SourceInfo{Connector: "mysql"}, time.Now(), int(header.LogPos)),
				Type:      eventType,
				Source:    event.SourceInfo{Connector: "mysql", Database: tableInfo.Database},
				Table:     *tableInfo,
				Timestamp: time.Unix(int64(header.Timestamp), 0),
				After:     afterData,
				Position: event.Position{
					BinlogFile: s.currentFile,
					BinlogPos:  header.LogPos,
					CommitTime: time.Unix(int64(header.Timestamp), 0),
				},
			})
		}

	case event.EventTypeUpdate:
		// Update events have pairs of rows: [before, after, before, after, ...]
		for i := 0; i < len(rowsEvent.Rows); i += 2 {
			if i+1 >= len(rowsEvent.Rows) {
				break
			}
			beforeRow := rowsEvent.Rows[i]
			afterRow := rowsEvent.Rows[i+1]

			beforeData := s.buildRowData(tableInfo.Columns, beforeRow)
			afterData := s.buildRowData(tableInfo.Columns, afterRow)

			events = append(events, &event.ChangeEvent{
				ID:        event.GenerateEventID(&event.SourceInfo{Connector: "mysql"}, time.Now(), int(header.LogPos)),
				Type:      eventType,
				Source:    event.SourceInfo{Connector: "mysql", Database: tableInfo.Database},
				Table:     *tableInfo,
				Timestamp: time.Unix(int64(header.Timestamp), 0),
				Before:    beforeData,
				After:     afterData,
				Position: event.Position{
					BinlogFile: s.currentFile,
					BinlogPos:  header.LogPos,
					CommitTime: time.Unix(int64(header.Timestamp), 0),
				},
			})
		}

	case event.EventTypeDelete:
		for _, row := range rowsEvent.Rows {
			beforeData := s.buildRowData(tableInfo.Columns, row)
			events = append(events, &event.ChangeEvent{
				ID:        event.GenerateEventID(&event.SourceInfo{Connector: "mysql"}, time.Now(), int(header.LogPos)),
				Type:      eventType,
				Source:    event.SourceInfo{Connector: "mysql", Database: tableInfo.Database},
				Table:     *tableInfo,
				Timestamp: time.Unix(int64(header.Timestamp), 0),
				Before:    beforeData,
				Position: event.Position{
					BinlogFile: s.currentFile,
					BinlogPos:  header.LogPos,
					CommitTime: time.Unix(int64(header.Timestamp), 0),
				},
			})
		}
	}

	return events
}

// buildRowData builds RowData from column definitions and values.
func (s *BinlogSyncer) buildRowData(columns []event.ColumnInfo, values []interface{}) event.RowData {
	fields := make(map[string]event.Field)

	for i, col := range columns {
		if i >= len(values) {
			break
		}

		value := values[i]
		fieldType := "unknown"
		if col.Type != "" {
			fieldType = s.mapColumnType(col.Type)
		}

		fields[col.Name] = event.Field{
			Name:  col.Name,
			Value: value,
			Type:  fieldType,
		}
	}

	return event.RowData{Fields: fields}
}

// mapColumnType maps MySQL column type to our field type.
func (s *BinlogSyncer) mapColumnType(rawType string) string {
	// Convert to lowercase for comparison
	rawType = strings.ToLower(rawType)

	// Numeric types
	if strings.Contains(rawType, "int") ||
		strings.Contains(rawType, "float") ||
		strings.Contains(rawType, "double") ||
		strings.Contains(rawType, "decimal") ||
		strings.Contains(rawType, "numeric") {
		return "number"
	}

	// Date/time types
	if strings.Contains(rawType, "date") ||
		strings.Contains(rawType, "time") ||
		strings.Contains(rawType, "timestamp") ||
		strings.Contains(rawType, "year") {
		return "datetime"
	}

	// String types
	if strings.Contains(rawType, "char") ||
		strings.Contains(rawType, "varchar") ||
		strings.Contains(rawType, "text") ||
		strings.Contains(rawType, "blob") ||
		strings.Contains(rawType, "binary") ||
		strings.Contains(rawType, "enum") ||
		strings.Contains(rawType, "set") ||
		strings.Contains(rawType, "json") {
		return "string"
	}

	// Bit type
	if strings.Contains(rawType, "bit") {
		return "bit"
	}

	// Geometry types
	if strings.Contains(rawType, "geometry") ||
		strings.Contains(rawType, "point") ||
		strings.Contains(rawType, "linestring") ||
		strings.Contains(rawType, "polygon") {
		return "geometry"
	}

	return "unknown"
}

// isDDL checks if a query is a DDL statement.
func isDDL(query string) bool {
	upper := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(upper, "CREATE ") ||
		strings.HasPrefix(upper, "ALTER ") ||
		strings.HasPrefix(upper, "DROP ") ||
		strings.HasPrefix(upper, "TRUNCATE ") ||
		strings.HasPrefix(upper, "RENAME ")
}

// matchPattern performs pattern matching with wildcards.
// Supports:
//   - * matches any sequence of characters
//   - ? matches any single character
//   - literal characters match themselves
func matchPattern(pattern, s string) bool {
	if pattern == "*" {
		return true
	}

	// Use simple wildcard matching algorithm
	return wildcardMatch(pattern, s)
}

// wildcardMatch implements * and ? wildcard matching.
func wildcardMatch(pattern, s string) bool {
	pLen, sLen := len(pattern), len(s)

	// dp[i][j] = true if pattern[0:i] matches s[0:j]
	dp := make([][]bool, pLen+1)
	for i := range dp {
		dp[i] = make([]bool, sLen+1)
	}

	// Empty pattern matches empty string
	dp[0][0] = true

	// Pattern with only * can match empty string
	for i := 1; i <= pLen; i++ {
		if pattern[i-1] == '*' {
			dp[i][0] = dp[i-1][0]
		}
	}

	// Fill the DP table
	for i := 1; i <= pLen; i++ {
		for j := 1; j <= sLen; j++ {
			switch pattern[i-1] {
			case '*':
				// * matches zero or more characters
				dp[i][j] = dp[i-1][j] || dp[i][j-1]
			case '?':
				// ? matches exactly one character
				dp[i][j] = dp[i-1][j-1]
			default:
				// Exact character match
				dp[i][j] = dp[i-1][j-1] && pattern[i-1] == s[j-1]
			}
		}
	}

	return dp[pLen][sLen]
}
