//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "github.com/denisenkom/go-mssqldb"
)

func TestSQLServerConnection(t *testing.T) {
	cfg := SQLServerConfig()

	// 等待数据库就绪
	WaitForReady(t, "sqlserver", cfg.SourceDSN, 60*time.Second)

	// 测试连接
	db, err := sql.Open("sqlserver", cfg.SourceDSN)
	require.NoError(t, err, "Failed to open SQL Server connection")
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	require.NoError(t, err, "Failed to ping SQL Server")

	// 验证能执行查询
	var version string
	err = db.QueryRowContext(ctx, "SELECT @@VERSION").Scan(&version)
	require.NoError(t, err, "Failed to query SQL Server version")
	require.NotEmpty(t, version, "SQL Server version should not be empty")

	t.Logf("SQL Server version: %s", version)
}

func TestSQLServerSnapshot(t *testing.T) {
	cfg := SQLServerConfig()
	WaitForReady(t, "sqlserver", cfg.SourceDSN, 60*time.Second)

	db, err := sql.Open("sqlserver", cfg.SourceDSN)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// 创建测试表
	_, err = db.ExecContext(ctx, `
		IF OBJECT_ID('snapshot_test', 'U') IS NOT NULL DROP TABLE snapshot_test;
		CREATE TABLE snapshot_test (
			id INT PRIMARY KEY,
			name NVARCHAR(100) NOT NULL,
			value INT DEFAULT 0
		)
	`)
	require.NoError(t, err, "Failed to create table")

	// 插入测试数据
	_, err = db.ExecContext(ctx, `
		INSERT INTO snapshot_test (id, name, value) VALUES
		(1, 'row1', 100),
		(2, 'row2', 200),
		(3, 'row3', 300)
	`)
	require.NoError(t, err, "Failed to insert data")

	// 验证数据
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM snapshot_test").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count, "Expected 3 rows")

	t.Log("SQL Server snapshot test passed")
}

func TestSQLServerCDC(t *testing.T) {
	cfg := SQLServerConfig()
	WaitForReady(t, "sqlserver", cfg.SourceDSN, 60*time.Second)

	db, err := sql.Open("sqlserver", cfg.SourceDSN)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// 创建测试表
	_, err = db.ExecContext(ctx, `
		IF OBJECT_ID('cdc_test', 'U') IS NOT NULL DROP TABLE cdc_test;
		CREATE TABLE cdc_test (
			id INT PRIMARY KEY,
			name NVARCHAR(100) NOT NULL,
			value INT DEFAULT 0
		)
	`)
	require.NoError(t, err, "Failed to create table")

	// 测试 INSERT
	_, err = db.ExecContext(ctx, `INSERT INTO cdc_test (id, name, value) VALUES (1, 'test', 100)`)
	require.NoError(t, err)

	// 测试 UPDATE
	_, err = db.ExecContext(ctx, `UPDATE cdc_test SET value = 200 WHERE id = 1`)
	require.NoError(t, err)

	// 验证更新
	var value int
	err = db.QueryRowContext(ctx, `SELECT value FROM cdc_test WHERE id = 1`).Scan(&value)
	require.NoError(t, err)
	require.Equal(t, 200, value)

	// 测试 DELETE
	_, err = db.ExecContext(ctx, `DELETE FROM cdc_test WHERE id = 1`)
	require.NoError(t, err)

	t.Log("SQL Server CDC test passed")
}

func TestSQLServerDDL(t *testing.T) {
	cfg := SQLServerConfig()
	WaitForReady(t, "sqlserver", cfg.SourceDSN, 60*time.Second)

	db, err := sql.Open("sqlserver", cfg.SourceDSN)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// 测试 CREATE TABLE
	_, err = db.ExecContext(ctx, `
		IF OBJECT_ID('ddl_test', 'U') IS NOT NULL DROP TABLE ddl_test;
		CREATE TABLE ddl_test (
			id INT PRIMARY KEY,
			name NVARCHAR(100)
		)
	`)
	require.NoError(t, err, "Failed to CREATE TABLE")

	// 测试 ALTER TABLE
	_, err = db.ExecContext(ctx, `ALTER TABLE ddl_test ADD value INT DEFAULT 0`)
	require.NoError(t, err, "Failed to ALTER TABLE")

	// 验证新列可用
	_, err = db.ExecContext(ctx, `INSERT INTO ddl_test (id, name, value) VALUES (1, 'test', 100)`)
	require.NoError(t, err, "Failed to INSERT with new column")

	// 测试 DROP TABLE
	_, err = db.ExecContext(ctx, `DROP TABLE ddl_test`)
	require.NoError(t, err, "Failed to DROP TABLE")

	t.Log("SQL Server DDL test passed")
}
