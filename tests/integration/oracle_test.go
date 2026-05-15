//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "github.com/sijms/go-ora/v2"
)

func TestOracleConnection(t *testing.T) {
	cfg := OracleConfig()

	// 等待数据库就绪 (Oracle 启动较慢)
	WaitForReady(t, "oracle", cfg.SourceDSN, 120*time.Second)

	// 测试连接
	db, err := sql.Open("oracle", cfg.SourceDSN)
	require.NoError(t, err, "Failed to open Oracle connection")
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	require.NoError(t, err, "Failed to ping Oracle")

	// 验证能执行查询
	var dummy int
	err = db.QueryRowContext(ctx, "SELECT 1 FROM DUAL").Scan(&dummy)
	require.NoError(t, err, "Failed to query Oracle")
	require.Equal(t, 1, dummy)

	t.Log("Oracle connection test passed")
}

func TestOracleSnapshot(t *testing.T) {
	cfg := OracleConfig()
	WaitForReady(t, "oracle", cfg.SourceDSN, 120*time.Second)

	db, err := sql.Open("oracle", cfg.SourceDSN)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// 创建测试表
	_, err = db.ExecContext(ctx, `
		BEGIN
			EXECUTE IMMEDIATE 'DROP TABLE snapshot_test';
		EXCEPTION
			WHEN OTHERS THEN NULL;
		END;
	`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE snapshot_test (
			id NUMBER PRIMARY KEY,
			name VARCHAR2(100) NOT NULL,
			value NUMBER DEFAULT 0
		)
	`)
	require.NoError(t, err, "Failed to create table")

	// 插入测试数据
	_, err = db.ExecContext(ctx, `INSERT INTO snapshot_test (id, name, value) VALUES (1, 'row1', 100)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO snapshot_test (id, name, value) VALUES (2, 'row2', 200)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO snapshot_test (id, name, value) VALUES (3, 'row3', 300)`)
	require.NoError(t, err)

	// 验证数据
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM snapshot_test").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count, "Expected 3 rows")

	t.Log("Oracle snapshot test passed")
}

func TestOracleCDC(t *testing.T) {
	cfg := OracleConfig()
	WaitForReady(t, "oracle", cfg.SourceDSN, 120*time.Second)

	db, err := sql.Open("oracle", cfg.SourceDSN)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// 创建测试表
	_, err = db.ExecContext(ctx, `
		BEGIN
			EXECUTE IMMEDIATE 'DROP TABLE cdc_test';
		EXCEPTION
			WHEN OTHERS THEN NULL;
		END;
	`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE cdc_test (
			id NUMBER PRIMARY KEY,
			name VARCHAR2(100) NOT NULL,
			value NUMBER DEFAULT 0
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

	t.Log("Oracle CDC test passed")
}

func TestOracleDDL(t *testing.T) {
	cfg := OracleConfig()
	WaitForReady(t, "oracle", cfg.SourceDSN, 120*time.Second)

	db, err := sql.Open("oracle", cfg.SourceDSN)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// 测试 CREATE TABLE
	_, err = db.ExecContext(ctx, `
		BEGIN
			EXECUTE IMMEDIATE 'DROP TABLE ddl_test';
		EXCEPTION
			WHEN OTHERS THEN NULL;
		END;
	`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE ddl_test (
			id NUMBER PRIMARY KEY,
			name VARCHAR2(100)
		)
	`)
	require.NoError(t, err, "Failed to CREATE TABLE")

	// 测试 ALTER TABLE
	_, err = db.ExecContext(ctx, `ALTER TABLE ddl_test ADD value NUMBER DEFAULT 0`)
	require.NoError(t, err, "Failed to ALTER TABLE")

	// 验证新列可用
	_, err = db.ExecContext(ctx, `INSERT INTO ddl_test (id, name, value) VALUES (1, 'test', 100)`)
	require.NoError(t, err, "Failed to INSERT with new column")

	// 测试 DROP TABLE
	_, err = db.ExecContext(ctx, `DROP TABLE ddl_test`)
	require.NoError(t, err, "Failed to DROP TABLE")

	t.Log("Oracle DDL test passed")
}
