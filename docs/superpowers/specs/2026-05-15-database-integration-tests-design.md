# 数据库兼容性集成测试设计规格

> **Created:** 2026-05-15
> **Status:** Approved

---

## 概述

使用 Docker Compose 启动测试数据库容器，为 MySQL、PostgreSQL、SQL Server、Oracle 四种数据库创建端到端集成测试，验证同构同步的核心功能。

---

## 测试矩阵

| 数据库 | Source 连接器 | Sink 连接器 | 测试文件 |
|--------|--------------|-------------|----------|
| MySQL | ✅ | ✅ | `tests/integration/mysql_test.go` |
| PostgreSQL | ✅ | ✅ | `tests/integration/postgres_test.go` |
| SQL Server | ✅ | ✅ | `tests/integration/sqlserver_test.go` |
| Oracle | ✅ | ✅ | `tests/integration/oracle_test.go` |

---

## 测试场景

每个数据库测试以下四个核心场景：

### 1. TestConnection — 连接建立与断开

- 测试连接配置正确时能成功连接
- 测试连接失败时返回正确错误
- 测试断开连接能正常清理资源

### 2. TestSnapshot — 全量快照读取

- 创建测试表并插入初始数据
- 启动 Source 连接器读取快照
- 验证读取的数据与源表一致
- 验证快照完成后位点正确记录

### 3. TestCDC — 增量变更捕获

- 完成快照后插入新数据
- 更新已有数据
- 删除数据
- 验证 Source 捕获所有变更事件
- 验证 Sink 正确写入目标

### 4. TestDDL — DDL 语句处理

- 执行 CREATE TABLE
- 执行 ALTER TABLE (添加列)
- 执行 DROP TABLE
- 验证 DDL 事件正确解析和传递

---

## 目录结构

```
tests/
├── integration/
│   ├── integration_test.go      # 测试辅助函数、Fixture
│   ├── mysql_test.go            # MySQL 集成测试
│   ├── postgres_test.go         # PostgreSQL 集成测试
│   ├── sqlserver_test.go        # SQL Server 集成测试
│   └── oracle_test.go           # Oracle 集成测试
├── docker/
│   ├── docker-compose.yml       # 所有数据库容器定义
│   ├── mysql/
│   │   └── init.sql            # MySQL 初始化脚本
│   ├── postgres/
│   │   └── init.sql            # PostgreSQL 初始化脚本
│   ├── sqlserver/
│   │   └── init.sql            # SQL Server 初始化脚本
│   └── oracle/
│       └── init.sql            # Oracle 初始化脚本
└── Makefile                     # 测试命令（添加到根目录 Makefile）
```

---

## Docker Compose 配置

### 数据库容器规格

| 数据库 | 镜像 | 端口 | 用户/密码 |
|--------|------|------|----------|
| MySQL 8.0 | mysql:8.0 | 3306 | root/testpass |
| PostgreSQL 15 | postgres:15 | 5432 | postgres/testpass |
| SQL Server 2022 | mcr.microsoft.com/mssql/server:2022-latest | 1433 | sa/TestPass123! |
| Oracle 21c | gvenzl/oracle-xe:21c | 1521 | system/testpass |

### docker-compose.yml 结构

```yaml
version: '3.8'
services:
  mysql:
    image: mysql:8.0
    ports: ["3306:3306"]
    environment:
      MYSQL_ROOT_PASSWORD: testpass
    volumes:
      - ./mysql/init.sql:/docker-entrypoint-initdb.d/init.sql

  postgres:
    image: postgres:15
    ports: ["5432:5432"]
    environment:
      POSTGRES_PASSWORD: testpass
    volumes:
      - ./postgres/init.sql:/docker-entrypoint-initdb.d/init.sql

  sqlserver:
    image: mcr.microsoft.com/mssql/server:2022-latest
    ports: ["1433:1433"]
    environment:
      ACCEPT_EULA: Y
      SA_PASSWORD: TestPass123!

  oracle:
    image: gvenzl/oracle-xe:21c
    ports: ["1521:1521"]
    environment:
      ORACLE_PASSWORD: testpass
```

---

## 测试辅助函数

### integration_test.go

```go
package integration

import (
    "context"
    "testing"
    "time"
)

// TestConfig 测试配置
type TestConfig struct {
    SourceDSN string
    SinkDSN   string
    Timeout   time.Duration
}

// WaitForReady 等待数据库就绪
func WaitForReady(ctx context.Context, dsn string) error

// CreateTable 创建测试表
func CreateTable(ctx context.Context, db *sql.DB, table string) error

// InsertData 插入测试数据
func InsertData(ctx context.Context, db *sql.DB, table string, rows []map[string]interface{}) error

// AssertDataEquals 验证源和目标数据一致
func AssertDataEquals(t *testing.T, source, sink *sql.DB, table string)

// RunIntegrationTest 运行集成测试的通用框架
func RunIntegrationTest(t *testing.T, cfg *TestConfig, fn func(ctx context.Context, source, sink *sql.DB))
```

---

## Makefile 目标

```makefile
# 集成测试
test-integration-up:
	docker-compose -f tests/docker/docker-compose.yml up -d

test-integration-down:
	docker-compose -f tests/docker/docker-compose.yml down -v

test-integration: test-integration-up
	go test -v -tags=integration ./tests/integration/...
	$(MAKE) test-integration-down

test-integration-mysql:
	go test -v -tags=integration ./tests/integration/... -run MySQL

test-integration-postgres:
	go test -v -tags=integration ./tests/integration/... -run PostgreSQL

test-integration-sqlserver:
	go test -v -tags=integration ./tests/integration/... -run SQLServer

test-integration-oracle:
	go test -v -tags=integration ./tests/integration/... -run Oracle
```

---

## 实现优先级

| 优先级 | 任务 | 预计工作量 |
|--------|------|-----------|
| P0 | 创建目录结构和 Docker Compose | 0.5 天 |
| P0 | 实现测试辅助函数 | 0.5 天 |
| P1 | MySQL 集成测试 | 1 天 |
| P1 | PostgreSQL 集成测试 | 1 天 |
| P1 | SQL Server 集成测试 | 1 天 |
| P1 | Oracle 集成测试 | 1 天 |

---

## 跳过条件

集成测试需要 `-tags=integration` 才会运行，避免在普通 `go test ./...` 中执行：

```go
// +build integration

package integration
```

CI 环境可以单独配置运行集成测试。

---

*文档版本：v1.0*
*创建时间：2026-05-15*
