package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/UFOXD/datastream/pkg/event"
)

// MySQLStore implements TargetStore using a MySQL-compatible target database.
type MySQLStore struct {
	db     *sql.DB
	taskID string
	dbName string // ds_{taskID}
}

// NewMySQLStore creates a new MySQLStore.
func NewMySQLStore(db *sql.DB, taskID string) *MySQLStore {
	return &MySQLStore{
		db:     db,
		taskID: taskID,
		dbName: "ds_" + sanitizeTaskID(taskID),
	}
}

func sanitizeTaskID(id string) string {
	// Only allow alphanumeric and underscores
	var b strings.Builder
	for _, c := range id {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			b.WriteRune(c)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

// InitDatabase creates the database and all tables.
func (s *MySQLStore) InitDatabase(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", s.dbName)); err != nil {
		return fmt.Errorf("create database: %w", err)
	}

	tables := []string{
		`CREATE TABLE IF NOT EXISTS task_position (
			id INT PRIMARY KEY DEFAULT 1,
			flushed_position JSON NOT NULL,
			current_position JSON NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS table_lifecycle (
			db_name VARCHAR(255) NOT NULL,
			tbl_name VARCHAR(255) NOT NULL,
			state VARCHAR(32) NOT NULL,
			snapshot_position JSON,
			error_msg TEXT,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (db_name, tbl_name)
		)`,
		`CREATE TABLE IF NOT EXISTS schema_history (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			position JSON NOT NULL,
			db_name VARCHAR(255) NOT NULL,
			tbl_name VARCHAR(255) NOT NULL,
			ddl TEXT NOT NULL,
			table_info JSON NOT NULL,
			change_type VARCHAR(32) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			INDEX idx_table (db_name, tbl_name)
		)`,
		`CREATE TABLE IF NOT EXISTS ddl_state (
			db_name VARCHAR(255) NOT NULL,
			tbl_name VARCHAR(255) NOT NULL,
			ddl TEXT NOT NULL,
			last_success_info JSON,
			status VARCHAR(32) NOT NULL,
			error_msg TEXT,
			retry_count INT DEFAULT 0,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (db_name, tbl_name)
		)`,
		`CREATE TABLE IF NOT EXISTS committed_position (
			id INT PRIMARY KEY DEFAULT 1,
			gtid_set TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		)`,
	}

	for _, ddl := range tables {
		if _, err := s.db.ExecContext(ctx, fmt.Sprintf("USE `%s`", s.dbName)); err != nil {
			return fmt.Errorf("use database: %w", err)
		}
		if _, err := s.db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("create table: %w", err)
		}
	}
	return nil
}

func (s *MySQLStore) useDB(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("USE `%s`", s.dbName))
	return err
}

func marshalJSON(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func unmarshalPosition(data []byte) (*event.Position, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var p event.Position
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func unmarshalTableInfo(data []byte) (*event.TableInfo, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var t event.TableInfo
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// --- Task Position ---

func (s *MySQLStore) SaveFlushedPosition(ctx context.Context, pos *event.Position) error {
	if err := s.useDB(ctx); err != nil {
		return err
	}
	data, err := marshalJSON(pos)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO task_position (id, flushed_position, current_position) VALUES (1, ?, ?)
		 ON DUPLICATE KEY UPDATE flushed_position = ?`,
		data, data, data)
	return err
}

func (s *MySQLStore) SaveCurrentPosition(ctx context.Context, pos *event.Position) error {
	if err := s.useDB(ctx); err != nil {
		return err
	}
	data, err := marshalJSON(pos)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`UPDATE task_position SET current_position = ? WHERE id = 1`, data)
	return err
}

func (s *MySQLStore) LoadPositions(ctx context.Context) (flushed, current *event.Position, err error) {
	if err := s.useDB(ctx); err != nil {
		return nil, nil, err
	}
	var flushedData, currentData []byte
	err = s.db.QueryRowContext(ctx,
		`SELECT flushed_position, current_position FROM task_position WHERE id = 1`).Scan(&flushedData, &currentData)
	if err == sql.ErrNoRows {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	flushed, err = unmarshalPosition(flushedData)
	if err != nil {
		return nil, nil, err
	}
	current, err = unmarshalPosition(currentData)
	return flushed, current, err
}

// --- Table Lifecycle ---

func (s *MySQLStore) SaveTableLifecycle(ctx context.Context, db, tbl, state string, snapshotPos *event.Position, errMsg string) error {
	if err := s.useDB(ctx); err != nil {
		return err
	}
	var posData []byte
	if snapshotPos != nil {
		var err error
		posData, err = marshalJSON(snapshotPos)
		if err != nil {
			return err
		}
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO table_lifecycle (db_name, tbl_name, state, snapshot_position, error_msg)
		 VALUES (?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE state = VALUES(state), snapshot_position = VALUES(snapshot_position), error_msg = VALUES(error_msg)`,
		db, tbl, state, posData, errMsg)
	return err
}

func (s *MySQLStore) LoadTableLifecycles(ctx context.Context) ([]*TableLifecycleRow, error) {
	if err := s.useDB(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT db_name, tbl_name, state, snapshot_position, error_msg, created_at, updated_at FROM table_lifecycle`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*TableLifecycleRow
	for rows.Next() {
		r := &TableLifecycleRow{}
		var posData []byte
		if err := rows.Scan(&r.DBName, &r.TableName, &r.State, &posData, &r.ErrorMsg, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		r.SnapshotPosition, _ = unmarshalPosition(posData)
		result = append(result, r)
	}
	return result, rows.Err()
}

func (s *MySQLStore) DeleteTableLifecycle(ctx context.Context, db, tbl string) error {
	if err := s.useDB(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM table_lifecycle WHERE db_name = ? AND tbl_name = ?`, db, tbl)
	return err
}

// --- Schema History ---

func (s *MySQLStore) SaveSchemaHistory(ctx context.Context, rec *SchemaHistoryRow) error {
	if err := s.useDB(ctx); err != nil {
		return err
	}
	posData, err := marshalJSON(&rec.Position)
	if err != nil {
		return err
	}
	tiData, err := marshalJSON(rec.TableInfo)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO schema_history (position, db_name, tbl_name, ddl, table_info, change_type)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		posData, rec.DBName, rec.TableName, rec.DDL, tiData, rec.ChangeType)
	return err
}

func (s *MySQLStore) LoadSchemaHistory(ctx context.Context) ([]*SchemaHistoryRow, error) {
	if err := s.useDB(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, position, db_name, tbl_name, ddl, table_info, change_type, created_at
		 FROM schema_history ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*SchemaHistoryRow
	for rows.Next() {
		r := &SchemaHistoryRow{}
		var posData, tiData []byte
		if err := rows.Scan(&r.ID, &posData, &r.DBName, &r.TableName, &r.DDL, &tiData, &r.ChangeType, &r.CreatedAt); err != nil {
			return nil, err
		}
		p, _ := unmarshalPosition(posData)
		if p != nil {
			r.Position = *p
		}
		r.TableInfo, _ = unmarshalTableInfo(tiData)
		result = append(result, r)
	}
	return result, rows.Err()
}

// --- DDL State ---

func (s *MySQLStore) SaveDDLState(ctx context.Context, rec *DDLStateRow) error {
	if err := s.useDB(ctx); err != nil {
		return err
	}
	var lastData []byte
	if rec.LastSuccessInfo != nil {
		var err error
		lastData, err = marshalJSON(rec.LastSuccessInfo)
		if err != nil {
			return err
		}
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO ddl_state (db_name, tbl_name, ddl, last_success_info, status, error_msg, retry_count)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE ddl = VALUES(ddl), last_success_info = VALUES(last_success_info),
		 status = VALUES(status), error_msg = VALUES(error_msg), retry_count = VALUES(retry_count)`,
		rec.DBName, rec.TableName, rec.DDL, lastData, rec.Status, rec.ErrorMsg, rec.RetryCount)
	return err
}

func (s *MySQLStore) LoadDDLState(ctx context.Context, db, tbl string) (*DDLStateRow, error) {
	if err := s.useDB(ctx); err != nil {
		return nil, err
	}
	r := &DDLStateRow{}
	var lastData []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT db_name, tbl_name, ddl, last_success_info, status, error_msg, retry_count, created_at, updated_at
		 FROM ddl_state WHERE db_name = ? AND tbl_name = ?`, db, tbl).Scan(
		&r.DBName, &r.TableName, &r.DDL, &lastData, &r.Status, &r.ErrorMsg, &r.RetryCount, &r.CreatedAt, &r.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.LastSuccessInfo, _ = unmarshalTableInfo(lastData)
	return r, nil
}

func (s *MySQLStore) LoadPendingDDLStates(ctx context.Context) ([]*DDLStateRow, error) {
	if err := s.useDB(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT db_name, tbl_name, ddl, last_success_info, status, error_msg, retry_count, created_at, updated_at
		 FROM ddl_state WHERE status = 'applying'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*DDLStateRow
	for rows.Next() {
		r := &DDLStateRow{}
		var lastData []byte
		if err := rows.Scan(&r.DBName, &r.TableName, &r.DDL, &lastData, &r.Status, &r.ErrorMsg, &r.RetryCount, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		r.LastSuccessInfo, _ = unmarshalTableInfo(lastData)
		result = append(result, r)
	}
	return result, rows.Err()
}

func (s *MySQLStore) DeleteDDLState(ctx context.Context, db, tbl string) error {
	if err := s.useDB(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM ddl_state WHERE db_name = ? AND tbl_name = ?`, db, tbl)
	return err
}

// --- Committed Position ---

func (s *MySQLStore) SaveCommittedPosition(ctx context.Context, gtidSet string) error {
	if err := s.useDB(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO committed_position (id, gtid_set) VALUES (1, ?)
		 ON DUPLICATE KEY UPDATE gtid_set = ?`, gtidSet, gtidSet)
	return err
}

func (s *MySQLStore) LoadCommittedPosition(ctx context.Context) (string, error) {
	if err := s.useDB(ctx); err != nil {
		return "", err
	}
	var gtidSet string
	err := s.db.QueryRowContext(ctx,
		`SELECT gtid_set FROM committed_position WHERE id = 1`).Scan(&gtidSet)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return gtidSet, err
}

// Close releases resources.
func (s *MySQLStore) Close() error {
	return nil // db is managed externally
}
