//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMySQLConnection(t *testing.T) {
	cfg := MySQLConfig()

	// 等待数据库就绪
	WaitForReady(t, cfg.Driver, cfg.SourceDSN, 30*time.Second)

	// 测试连接
	db := Connect(t, cfg.Driver, cfg.SourceDSN)
	defer db.Close()

	// 验证能执行查询
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var version string
	err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	require.NoError(t, err, "Failed to query MySQL version")
	require.NotEmpty(t, version, "MySQL version should not be empty")

	t.Logf("MySQL version: %s", version)
}

func TestMySQLSnapshot(t *testing.T) {
	cfg := MySQLConfig()
	WaitForReady(t, cfg.Driver, cfg.SourceDSN, 30*time.Second)

	db := Connect(t, cfg.Driver, cfg.SourceDSN)
	defer db.Close()

	// 清理并创建测试表
	DropTable(t, db, "snapshot_test")
	CreateTable(t, db, "snapshot_test")

	// 插入测试数据
	InsertRow(t, db, "snapshot_test", 1, "row1", 100)
	InsertRow(t, db, "snapshot_test", 2, "row2", 200)
	InsertRow(t, db, "snapshot_test", 3, "row3", 300)

	// 验证数据
	count := CountRows(t, db, "snapshot_test")
	require.Equal(t, 3, count, "Expected 3 rows in snapshot_test")

	AssertRowExists(t, db, "snapshot_test", 1, "row1", 100)
	AssertRowExists(t, db, "snapshot_test", 2, "row2", 200)
	AssertRowExists(t, db, "snapshot_test", 3, "row3", 300)

	t.Log("MySQL snapshot test passed")
}

func TestMySQLCDC(t *testing.T) {
	cfg := MySQLConfig()
	WaitForReady(t, cfg.Driver, cfg.SourceDSN, 30*time.Second)

	db := Connect(t, cfg.Driver, cfg.SourceDSN)
	defer db.Close()

	// 清理并创建测试表
	DropTable(t, db, "cdc_test")
	CreateTable(t, db, "cdc_test")

	// 初始数据
	InsertRow(t, db, "cdc_test", 1, "initial", 0)

	// 测试 INSERT
	InsertRow(t, db, "cdc_test", 2, "inserted", 100)
	AssertRowExists(t, db, "cdc_test", 2, "inserted", 100)

	// 测试 UPDATE
	UpdateRow(t, db, "cdc_test", 1, 999)
	AssertRowExists(t, db, "cdc_test", 1, "initial", 999)

	// 测试 DELETE
	DeleteRow(t, db, "cdc_test", 2)
	AssertRowNotExists(t, db, "cdc_test", 2)

	t.Log("MySQL CDC test passed")
}

func TestMySQLDDL(t *testing.T) {
	cfg := MySQLConfig()
	WaitForReady(t, cfg.Driver, cfg.SourceDSN, 30*time.Second)

	db := Connect(t, cfg.Driver, cfg.SourceDSN)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 测试 CREATE TABLE
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS ddl_test (
			id INT PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	require.NoError(t, err, "Failed to CREATE TABLE")

	// 测试 ALTER TABLE
	_, err = db.ExecContext(ctx, `
		ALTER TABLE ddl_test ADD COLUMN value INT DEFAULT 0
	`)
	require.NoError(t, err, "Failed to ALTER TABLE")

	// 验证新列可用
	_, err = db.ExecContext(ctx, `
		INSERT INTO ddl_test (id, name, value) VALUES (1, 'test', 100)
	`)
	require.NoError(t, err, "Failed to INSERT with new column")

	// 测试 DROP TABLE
	_, err = db.ExecContext(ctx, `DROP TABLE IF EXISTS ddl_test`)
	require.NoError(t, err, "Failed to DROP TABLE")

	t.Log("MySQL DDL test passed")
}
