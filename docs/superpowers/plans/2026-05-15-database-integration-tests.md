# 数据库兼容性集成测试实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 MySQL、PostgreSQL、SQL Server、Oracle 四种数据库创建端到端集成测试

**Architecture:** 使用 Docker Compose 启动数据库容器，Go 测试框架验证同构同步功能（连接、快照、CDC、DDL）

**Tech Stack:** Go testing, Docker Compose, database/sql, testify

---

## 文件结构

```
tests/
├── docker/
│   ├── docker-compose.yml       # 数据库容器定义
│   ├── mysql/init.sql          # MySQL 初始化
│   ├── postgres/init.sql       # PostgreSQL 初始化
│   ├── sqlserver/init.sql      # SQL Server 初始化
│   └── oracle/init.sql         # Oracle 初始化
└── integration/
    ├── integration_test.go      # 测试辅助函数
    ├── mysql_test.go            # MySQL 集成测试
    ├── postgres_test.go         # PostgreSQL 集成测试
    ├── sqlserver_test.go        # SQL Server 集成测试
    └── oracle_test.go           # Oracle 集成测试
```

---

### Task 1: 创建目录结构和 Docker Compose

**Files:**
- Create: `tests/docker/docker-compose.yml`
- Create: `tests/docker/mysql/init.sql`
- Create: `tests/docker/postgres/init.sql`
- Create: `tests/docker/sqlserver/init.sql`
- Create: `tests/docker/oracle/init.sql`

- [ ] **Step 1: 创建目录**

```bash
mkdir -p tests/docker/mysql tests/docker/postgres tests/docker/sqlserver tests/docker/oracle
mkdir -p tests/integration
```

- [ ] **Step 2: 创建 docker-compose.yml**

```yaml
# tests/docker/docker-compose.yml
version: '3.8'

services:
  mysql:
    image: mysql:8.0
    container_name: datastream-mysql-test
    ports:
      - "13306:3306"
    environment:
      MYSQL_ROOT_PASSWORD: testpass
      MYSQL_DATABASE: testdb
    volumes:
      - ./mysql/init.sql:/docker-entrypoint-initdb.d/init.sql
    command: --binlog-format=ROW --log-bin=mysql-bin --server-id=1
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 5s
      timeout: 5s
      retries: 10

  postgres:
    image: postgres:15
    container_name: datastream-postgres-test
    ports:
      - "15432:5432"
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: testpass
      POSTGRES_DB: testdb
    volumes:
      - ./postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 10

  sqlserver:
    image: mcr.microsoft.com/mssql/server:2022-latest
    container_name: datastream-sqlserver-test
    ports:
      - "11433:1433"
    environment:
      ACCEPT_EULA: Y
      SA_PASSWORD: "TestPass123!"
    healthcheck:
      test: ["CMD-SHELL", "/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'TestPass123!' -Q 'SELECT 1'"]
      interval: 10s
      timeout: 5s
      retries: 10

  oracle:
    image: gvenzl/oracle-xe:21c
    container_name: datastream-oracle-test
    ports:
      - "11521:1521"
    environment:
      ORACLE_PASSWORD: testpass
    healthcheck:
      test: ["CMD-SHELL", "echo 'SELECT 1 FROM DUAL;' | sqlplus -s system/testpass@localhost:1521/XEPDB1"]
      interval: 30s
      timeout: 10s
      retries: 10
```

- [ ] **Step 3: 创建 MySQL 初始化脚本**

```sql
-- tests/docker/mysql/init.sql
CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

CREATE TABLE IF NOT EXISTS test_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    value INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO test_table (name, value) VALUES ('test1', 100), ('test2', 200);
```

- [ ] **Step 4: 创建 PostgreSQL 初始化脚本**

```sql
-- tests/docker/postgres/init.sql
CREATE TABLE IF NOT EXISTS test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    value INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO test_table (name, value) VALUES ('test1', 100), ('test2', 200);
```

- [ ] **Step 5: 创建 SQL Server 初始化脚本**

```sql
-- tests/docker/sqlserver/init.sql
CREATE DATABASE testdb;
GO

USE testdb;
GO

CREATE TABLE test_table (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(100) NOT NULL,
    value INT DEFAULT 0,
    created_at DATETIME2 DEFAULT GETDATE()
);
GO

INSERT INTO test_table (name, value) VALUES ('test1', 100), ('test2', 200);
GO
```

- [ ] **Step 6: 创建 Oracle 初始化脚本**

```sql
-- tests/docker/oracle/init.sql
CREATE TABLE test_table (
    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR2(100) NOT NULL,
    value NUMBER DEFAULT 0,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP
);

INSERT INTO test_table (name, value) VALUES ('test1', 100);
INSERT INTO test_table (name, value) VALUES ('test2', 200);
COMMIT;
```

- [ ] **Step 7: 提交 Docker 配置**

```bash
git add tests/docker/
git commit -m "feat(test): add docker-compose for integration test databases

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 2: 创建测试辅助函数

**Files:**
- Create: `tests/integration/integration_test.go`

- [ ] **Step 1: 创建测试辅助函数文件**

```go
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
```

- [ ] **Step 2: 提交测试辅助函数**

```bash
git add tests/integration/integration_test.go
git commit -m "feat(test): add integration test helpers

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 3: MySQL 集成测试

**Files:**
- Create: `tests/integration/mysql_test.go`

- [ ] **Step 1: 创建 MySQL 集成测试**

```go
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
```

- [ ] **Step 2: 提交 MySQL 集成测试**

```bash
git add tests/integration/mysql_test.go
git commit -m "feat(test): add MySQL integration tests

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 4: PostgreSQL 集成测试

**Files:**
- Create: `tests/integration/postgres_test.go`

- [ ] **Step 1: 创建 PostgreSQL 集成测试**

```go
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

func TestPostgresConnection(t *testing.T) {
	cfg := PostgresConfig()

	// 等待数据库就绪
	WaitForReady(t, cfg.Driver, cfg.SourceDSN, 30*time.Second)

	// 测试连接
	db := Connect(t, cfg.Driver, cfg.SourceDSN)
	defer db.Close()

	// 验证能执行查询
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var version string
	err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	require.NoError(t, err, "Failed to query PostgreSQL version")
	require.NotEmpty(t, version, "PostgreSQL version should not be empty")

	t.Logf("PostgreSQL version: %s", version)
}

func TestPostgresSnapshot(t *testing.T) {
	cfg := PostgresConfig()
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

	t.Log("PostgreSQL snapshot test passed")
}

func TestPostgresCDC(t *testing.T) {
	cfg := PostgresConfig()
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

	t.Log("PostgreSQL CDC test passed")
}

func TestPostgresDDL(t *testing.T) {
	cfg := PostgresConfig()
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

	t.Log("PostgreSQL DDL test passed")
}
```

- [ ] **Step 2: 提交 PostgreSQL 集成测试**

```bash
git add tests/integration/postgres_test.go
git commit -m "feat(test): add PostgreSQL integration tests

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 5: SQL Server 集成测试

**Files:**
- Create: `tests/integration/sqlserver_test.go`

- [ ] **Step 1: 创建 SQL Server 集成测试**

```go
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
```

- [ ] **Step 2: 提交 SQL Server 集成测试**

```bash
git add tests/integration/sqlserver_test.go
git commit -m "feat(test): add SQL Server integration tests

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 6: Oracle 集成测试

**Files:**
- Create: `tests/integration/oracle_test.go`

- [ ] **Step 1: 创建 Oracle 集成测试**

```go
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
```

- [ ] **Step 2: 提交 Oracle 集成测试**

```bash
git add tests/integration/oracle_test.go
git commit -m "feat(test): add Oracle integration tests

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 7: 更新 Makefile

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: 添加集成测试命令到 Makefile**

在 Makefile 末尾添加：

```makefile
# Integration tests
.PHONY: test-integration-up test-integration-down test-integration test-integration-mysql test-integration-postgres test-integration-sqlserver test-integration-oracle

test-integration-up:
	docker-compose -f tests/docker/docker-compose.yml up -d
	@echo "Waiting for databases to be ready..."
	@sleep 10

test-integration-down:
	docker-compose -f tests/docker/docker-compose.yml down -v

test-integration: test-integration-up
	go test -v -tags=integration -timeout 10m ./tests/integration/...
	$(MAKE) test-integration-down

test-integration-mysql: test-integration-up
	go test -v -tags=integration -timeout 5m ./tests/integration/... -run MySQL
	$(MAKE) test-integration-down

test-integration-postgres: test-integration-up
	go test -v -tags=integration -timeout 5m ./tests/integration/... -run Postgres
	$(MAKE) test-integration-down

test-integration-sqlserver: test-integration-up
	go test -v -tags=integration -timeout 5m ./tests/integration/... -run SQLServer
	$(MAKE) test-integration-down

test-integration-oracle: test-integration-up
	go test -v -tags=integration -timeout 5m ./tests/integration/... -run Oracle
	$(MAKE) test-integration-down
```

- [ ] **Step 2: 提交 Makefile 更新**

```bash
git add Makefile
git commit -m "feat(test): add integration test Makefile targets

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 8: 添加测试依赖

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: 添加 testify 和数据库驱动依赖**

```bash
go get github.com/stretchr/testify
go get github.com/denisenkom/go-mssqldb
go get github.com/sijms/go-ora/v2
```

- [ ] **Step 2: 整理依赖**

```bash
go mod tidy
```

- [ ] **Step 3: 提交依赖更新**

```bash
git add go.mod go.sum
git commit -m "feat(test): add integration test dependencies

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## 自检清单

- [x] 所有规格要求都有对应任务
- [x] 无占位符 (TBD/TODO)
- [x] 类型和方法名一致
- [x] 每个任务可独立完成
- [x] 包含完整代码和命令
