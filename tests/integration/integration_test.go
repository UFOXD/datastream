//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	Driver   string
	SourceDSN string
	SinkDSN   string
}

// MySQLConfig 返回 MySQL 测试配置
func MySQLConfig() *DatabaseConfig {
	return &DatabaseConfig{
		Driver:    "mysql",
		SourceDSN: "root:testpass@tcp(localhost:13306)/testdb?parseTime=true",
		SinkDSN:   "root:testpass@tcp(localhost:13306)/testdb_sink?parseTime=true",
	}
}

// PostgresConfig 返回 PostgreSQL 测试配置
func PostgresConfig() *DatabaseConfig {
	return &DatabaseConfig{
		Driver:    "postgres",
		SourceDSN: "postgres://postgres:testpass@localhost:15432/testdb?sslmode=disable",
		SinkDSN:   "postgres://postgres:testpass@localhost:15432/testdb_sink?sslmode=disable",
	}
}

// SQLServerConfig 返回 SQL Server 测试配置
func SQLServerConfig() *DatabaseConfig {
	return &DatabaseConfig{
		Driver:    "sqlserver",
		SourceDSN: "sqlserver://sa:TestPass123!@localhost:11433?database=testdb",
		SinkDSN:   "sqlserver://sa:TestPass123!@localhost:11433?database=testdb_sink",
	}
}

// OracleConfig 返回 Oracle 测试配置
func OracleConfig() *DatabaseConfig {
	return &DatabaseConfig{
		Driver:    "oracle",
		SourceDSN: "system/testpass@localhost:11521/XEPDB1",
		SinkDSN:   "system/testpass@localhost:11521/XEPDB1",
	}
}

// Connect 连接数据库
func Connect(t *testing.T, driver, dsn string) *sql.DB {
	db, err := sql.Open(driver, dsn)
	require.NoError(t, err, "Failed to open database connection")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	require.NoError(t, err, "Failed to ping database")

	return db
}

// WaitForReady 等待数据库就绪
func WaitForReady(t *testing.T, driver, dsn string, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for database %s to be ready", driver)
		case <-ticker.C:
			db, err := sql.Open(driver, dsn)
			if err != nil {
				continue
			}
			if db.PingContext(ctx) == nil {
				db.Close()
				return
			}
			db.Close()
		}
	}
}

// CreateTable 创建测试表
func CreateTable(t *testing.T, db *sql.DB, table string) {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			value INT DEFAULT 0
		)
	`, table)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to create table %s", table)
}

// DropTable 删除测试表
func DropTable(t *testing.T, db *sql.DB, table string) {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to drop table %s", table)
}

// InsertRow 插入一行数据
func InsertRow(t *testing.T, db *sql.DB, table string, id int, name string, value int) {
	query := fmt.Sprintf("INSERT INTO %s (id, name, value) VALUES (?, ?, ?)", table)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, query, id, name, value)
	require.NoError(t, err, "Failed to insert row into %s", table)
}

// UpdateRow 更新一行数据
func UpdateRow(t *testing.T, db *sql.DB, table string, id int, value int) {
	query := fmt.Sprintf("UPDATE %s SET value = ? WHERE id = ?", table)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, query, value, id)
	require.NoError(t, err, "Failed to update row in %s", table)
}

// DeleteRow 删除一行数据
func DeleteRow(t *testing.T, db *sql.DB, table string, id int) {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = ?", table)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, query, id)
	require.NoError(t, err, "Failed to delete row from %s", table)
}

// CountRows 统计表行数
func CountRows(t *testing.T, db *sql.DB, table string) int {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	err := db.QueryRowContext(ctx, query).Scan(&count)
	require.NoError(t, err, "Failed to count rows in %s", table)

	return count
}

// AssertRowExists 验证行存在
func AssertRowExists(t *testing.T, db *sql.DB, table string, id int, expectedName string, expectedValue int) {
	query := fmt.Sprintf("SELECT name, value FROM %s WHERE id = ?", table)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var name string
	var value int
	err := db.QueryRowContext(ctx, query, id).Scan(&name, &value)
	require.NoError(t, err, "Row %d not found in %s", id, table)
	require.Equal(t, expectedName, name, "Name mismatch for row %d", id)
	require.Equal(t, expectedValue, value, "Value mismatch for row %d", id)
}

// AssertRowNotExists 验证行不存在
func AssertRowNotExists(t *testing.T, db *sql.DB, table string, id int) {
	query := fmt.Sprintf("SELECT 1 FROM %s WHERE id = ?", table)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var dummy int
	err := db.QueryRowContext(ctx, query, id).Scan(&dummy)
	require.Error(t, err, "Row %d should not exist in %s", id, table)
	require.Equal(t, sql.ErrNoRows, err)
}
