# 测试策略设计

## 概述

本文档定义 DataStream 项目的测试策略，包括测试层级、测试工具、测试覆盖率和测试流程，确保代码质量和系统稳定性。

---

## 测试金字塔

```
                    ┌─────────┐
                    │   E2E   │  端到端测试（少量）
                    │  Tests  │  - 完整链路验证
                    └─────────┘  - 关键场景覆盖
                    ┌─────────────┐
                    │ Integration │  集成测试（适量）
                    │    Tests    │  - 组件间交互
                    └─────────────┘  - 真实环境验证
            ┌─────────────────────────────┐
            │       Unit Tests            │  单元测试（大量）
            │                             │  - 函数级别验证
            │    快速、隔离、覆盖率高      │  - Mock 依赖
            └─────────────────────────────┘
```

---

## 测试层级

### 1. 单元测试

**目标**：验证单个函数/方法的行为

**原则**：
- 快速执行（毫秒级）
- 隔离依赖（使用 Mock）
- 高覆盖率（>80%）
- 表格驱动测试

**工具**：
- `testing` - Go 标准测试框架
- `testify` - 断言和 Mock
- `gomock` - 接口 Mock
- `go-cmp` - 深度比较

**示例**：

```go
package parser

import (
    "context"
    "testing"

    "github.com/stretchr/testify/require"
)

func TestMySQLDDLParser_Parse(t *testing.T) {
    tests := []struct {
        name        string
        ddl         string
        expected    *DDLResult
        expectError bool
    }{
        {
            name: "create table",
            ddl: `CREATE TABLE users (
                id INT PRIMARY KEY,
                name VARCHAR(100)
            )`,
            expected: &DDLResult{
                Type:      DDLTypeCreateTable,
                Database:  "",
                Table:     "users",
            },
        },
        {
            name: "drop table",
            ddl:  "DROP TABLE users",
            expected: &DDLResult{
                Type:  DDLTypeDropTable,
                Table: "users",
            },
        },
        {
            name: "alter table add column",
            ddl:  "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
            expected: &DDLResult{
                Type:  DDLTypeAlterTable,
                Table: "users",
                TableChanges: &TableChanges{
                    Operation: TableOpAlter,
                    AddedColumns: []ColumnInfo{
                        {Name: "email", Type: "varchar(255)"},
                    },
                },
            },
        },
        {
            name:        "invalid ddl",
            ddl:         "INVALID SQL",
            expectError: true,
        },
    }

    parser := NewMySQLDDLParser()

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := parser.Parse(context.Background(), tt.ddl)

            if tt.expectError {
                require.Error(t, err)
                return
            }

            require.NoError(t, err)
            require.Equal(t, tt.expected.Type, result.Type)
            require.Equal(t, tt.expected.Table, result.Table)
        })
    }
}
```

**Mock 示例**：

```go
// 使用 testify mock
type MockSourceConnector struct {
    mock.Mock
}

func (m *MockSourceConnector) Start(ctx context.Context) error {
    args := m.Called(ctx)
    return args.Error(0)
}

func (m *MockSourceConnector) Stop(ctx context.Context) error {
    args := m.Called(ctx)
    return args.Error(0)
}

func (m *MockSourceConnector) Events() <-chan *ChangeEvent {
    args := m.Called()
    return args.Get(0).(<-chan *ChangeEvent)
}

// 测试中使用 Mock
func TestPipeline_Start(t *testing.T) {
    mockSource := new(MockSourceConnector)
    mockSink := new(MockSinkConnector)

    eventCh := make(chan *ChangeEvent, 1)
    mockSource.On("Events").Return((<-chan *ChangeEvent)(eventCh))
    mockSource.On("Start", mock.Anything).Return(nil)
    mockSink.On("Emit", mock.Anything, mock.Anything).Return(nil)

    pipeline := NewPipeline(mockSource, mockSink)

    ctx := context.Background()
    err := pipeline.Start(ctx)
    require.NoError(t, err)

    // 发送测试事件
    eventCh <- &ChangeEvent{
        Type: EventTypeInsert,
        Table: TableInfo{Table: "users"},
    }

    // 验证
    mockSink.AssertCalled(t, "Emit", mock.Anything, mock.Anything)
}
```

---

### 2. 集成测试

**目标**：验证组件间的交互

**原则**：
- 使用真实依赖（数据库、消息队列）
- 测试关键路径
- 可重复执行
- 使用 Docker 容器

**工具**：
- `testcontainers-go` - Docker 容器管理
- `dockertest` - Docker 测试工具
- `go-sqlmock` - 数据库 Mock（可选）

**示例**：

```go
// +build integration

package integration

import (
    "context"
    "testing"
    "time"

    "github.com/testcontainers/testcontainers-go"
    "github.com/testcontainers/testcontainers-go/wait"
)

func TestMySQLToMySQLSync(t *testing.T) {
    ctx := context.Background()

    // 启动源数据库
    sourceContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
        ContainerRequest: testcontainers.ContainerRequest{
            Image:        "mysql:8.0",
            ExposedPorts: []string{"3306/tcp"},
            Env: map[string]string{
                "MYSQL_ROOT_PASSWORD": "root",
                "MYSQL_DATABASE":      "test",
            },
            WaitingFor: wait.ForLog("port: 3306  MySQL Community Server"),
        },
        Started: true,
    })
    require.NoError(t, err)
    defer sourceContainer.Terminate(ctx)

    // 启动目标数据库
    sinkContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
        ContainerRequest: testcontainers.ContainerRequest{
            Image:        "mysql:8.0",
            ExposedPorts: []string{"3306/tcp"},
            Env: map[string]string{
                "MYSQL_ROOT_PASSWORD": "root",
                "MYSQL_DATABASE":      "test",
            },
            WaitingFor: wait.ForLog("port: 3306  MySQL Community Server"),
        },
        Started: true,
    })
    require.NoError(t, err)
    defer sinkContainer.Terminate(ctx)

    // 获取连接信息
    sourceHost, err := sourceContainer.Host(ctx)
    require.NoError(t, err)
    sourcePort, err := sourceContainer.MappedPort(ctx, "3306")
    require.NoError(t, err)

    sinkHost, err := sinkContainer.Host(ctx)
    require.NoError(t, err)
    sinkPort, err := sinkContainer.MappedPort(ctx, "3306")
    require.NoError(t, err)

    // 创建同步任务
    task := createSyncTask(t, sourceHost, sourcePort, sinkHost, sinkPort)

    // 在源库插入数据
    insertTestData(t, sourceHost, sourcePort, "test", "users", map[string]interface{}{
        "id":   1,
        "name": "test",
    })

    // 等待同步
    time.Sleep(2 * time.Second)

    // 验证目标库数据
    data := queryData(t, sinkHost, sinkPort, "test", "users", 1)
    require.Equal(t, "test", data["name"])
}
```

**集成测试配置**：

```go
// tests/integration/setup.go
package integration

import (
    "os"
    "testing"
)

func TestMain(m *testing.M) {
    // 检查是否运行集成测试
    if os.Getenv("INTEGRATION_TEST") == "" {
        return
    }

    // 设置测试环境
    setupTestEnvironment()

    // 运行测试
    code := m.Run()

    // 清理测试环境
    teardownTestEnvironment()

    os.Exit(code)
}
```

---

### 3. 端到端测试

**目标**：验证完整链路

**原则**：
- 模拟真实场景
- 覆盖关键业务流程
- 自动化执行

**测试场景**：

| 场景 | 说明 |
|------|------|
| 全量同步 | 快照阶段数据同步 |
| 增量同步 | 实时变更捕获 |
| DDL 同步 | Schema 变更传播 |
| 故障恢复 | 任务重启后恢复 |
| 表级动态 | 动态添加/删除表 |

**示例**：

```go
// tests/e2e/sync_test.go
// +build e2e

package e2e

import (
    "context"
    "testing"
    "time"

    "github.com/stretchr/testify/require"
)

// TestFullSync 全量同步测试
func TestFullSync(t *testing.T) {
    env := SetupEnvironment(t)
    defer env.Teardown()

    // 1. 在源库创建表并插入数据
    env.SourceDB.Exec(`
        CREATE TABLE products (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            price DECIMAL(10,2)
        )
    `)

    for i := 1; i <= 1000; i++ {
        env.SourceDB.Exec(
            "INSERT INTO products VALUES (?, ?, ?)",
            i, fmt.Sprintf("Product-%d", i), float64(i)*1.5,
        )
    }

    // 2. 创建同步任务
    task := env.CreateTask(TaskConfig{
        Name:   "full-sync-test",
        Source: env.SourceConfig,
        Sink:   env.SinkConfig,
    })

    // 3. 等待同步完成
    env.WaitForTaskStatus(task.ID, TaskStatusRunning, 30*time.Second)
    env.WaitForSyncComplete(task.ID, 60*time.Second)

    // 4. 验证数据一致性
    count := env.CountTable(env.SinkDB, "products")
    require.Equal(t, 1000, count)
}

// TestIncrementalSync 增量同步测试
func TestIncrementalSync(t *testing.T) {
    env := SetupEnvironment(t)
    defer env.Teardown()

    // 1. 创建表并启动同步
    env.SourceDB.Exec(`CREATE TABLE orders (id INT PRIMARY KEY, status VARCHAR(20))`)
    task := env.CreateTask(DefaultTaskConfig)

    env.WaitForTaskStatus(task.ID, TaskStatusRunning, 30*time.Second)

    // 2. 执行 DML 操作
    env.SourceDB.Exec("INSERT INTO orders VALUES (1, 'pending')")
    env.SourceDB.Exec("INSERT INTO orders VALUES (2, 'pending')")
    env.SourceDB.Exec("UPDATE orders SET status = 'completed' WHERE id = 1")
    env.SourceDB.Exec("DELETE FROM orders WHERE id = 2")

    // 3. 等待同步
    time.Sleep(2 * time.Second)

    // 4. 验证结果
    var status string
    env.SinkDB.QueryRow("SELECT status FROM orders WHERE id = 1").Scan(&status)
    require.Equal(t, "completed", status)

    var count int
    env.SinkDB.QueryRow("SELECT COUNT(*) FROM orders").Scan(&count)
    require.Equal(t, 1, count)
}

// TestDDLSync DDL 同步测试
func TestDDLSync(t *testing.T) {
    env := SetupEnvironment(t)
    defer env.Teardown()

    task := env.CreateTask(DefaultTaskConfig)
    env.WaitForTaskStatus(task.ID, TaskStatusRunning, 30*time.Second)

    // 1. CREATE TABLE
    env.SourceDB.Exec(`CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))`)
    time.Sleep(1 * time.Second)
    env.AssertTableExists(env.SinkDB, "users")

    // 2. ALTER TABLE
    env.SourceDB.Exec(`ALTER TABLE users ADD COLUMN email VARCHAR(255)`)
    time.Sleep(1 * time.Second)
    env.AssertColumnExists(env.SinkDB, "users", "email")

    // 3. DROP TABLE
    env.SourceDB.Exec(`DROP TABLE users`)
    time.Sleep(1 * time.Second)
    env.AssertTableNotExists(env.SinkDB, "users")
}

// TestFailover 故障恢复测试
func TestFailover(t *testing.T) {
    env := SetupEnvironment(t)
    defer env.Teardown()

    // 1. 创建任务并插入数据
    task := env.CreateTask(DefaultTaskConfig)
    env.WaitForTaskStatus(task.ID, TaskStatusRunning, 30*time.Second)

    env.SourceDB.Exec("CREATE TABLE test (id INT PRIMARY KEY)")
    env.SourceDB.Exec("INSERT INTO test VALUES (1)")

    // 2. 记录当前位置
    position := env.GetTaskPosition(task.ID)

    // 3. 停止任务
    env.StopTask(task.ID)
    env.WaitForTaskStatus(task.ID, TaskStatusStopped, 10*time.Second)

    // 4. 插入更多数据（任务停止期间）
    env.SourceDB.Exec("INSERT INTO test VALUES (2)")
    env.SourceDB.Exec("INSERT INTO test VALUES (3)")

    // 5. 重启任务
    env.StartTask(task.ID)
    env.WaitForTaskStatus(task.ID, TaskStatusRunning, 10*time.Second)

    // 6. 等待同步完成
    env.WaitForSyncComplete(task.ID, 30*time.Second)

    // 7. 验证所有数据已同步
    count := env.CountTable(env.SinkDB, "test")
    require.Equal(t, 3, count)
}

// TestDynamicTableAdd 动态添加表测试
func TestDynamicTableAdd(t *testing.T) {
    env := SetupEnvironment(t)
    defer env.Teardown()

    // 1. 创建任务（初始不包含 test 表）
    task := env.CreateTask(TaskConfig{
        ExcludeTables: []string{"test"},
    })
    env.WaitForTaskStatus(task.ID, TaskStatusRunning, 30*time.Second)

    // 2. 创建 test 表并插入数据
    env.SourceDB.Exec("CREATE TABLE test (id INT PRIMARY KEY)")
    env.SourceDB.Exec("INSERT INTO test VALUES (1)")

    // 3. 验证数据未同步
    time.Sleep(2 * time.Second)
    env.AssertTableNotExists(env.SinkDB, "test")

    // 4. 动态添加表
    env.AddTables(task.ID, []string{"test"}, true)

    // 5. 等待快照完成
    time.Sleep(3 * time.Second)

    // 6. 验证数据已同步
    count := env.CountTable(env.SinkDB, "test")
    require.Equal(t, 1, count)
}
```

---

## 测试覆盖率

### 覆盖率目标

| 层级 | 目标覆盖率 |
|------|-----------|
| Core Layer | 90% |
| Connector Layer | 80% |
| Pipeline Layer | 85% |
| Coordinator Layer | 80% |
| API Layer | 75% |

### 覆盖率报告

```bash
# 生成覆盖率报告
go test -coverprofile=coverage.out ./...

# 查看 HTML 报告
go tool cover -html=coverage.out

# 查看函数覆盖率
go tool cover -func=coverage.out
```

### Makefile 集成

```makefile
.PHONY: test test-coverage test-integration test-e2e

# 单元测试
test:
	go test -v -race ./...

# 覆盖率测试
test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# 集成测试
test-integration:
	INTEGRATION_TEST=1 go test -v -tags=integration ./tests/integration/...

# 端到端测试
test-e2e:
	go test -v -tags=e2e ./tests/e2e/...

# 所有测试
test-all: test test-integration test-e2e
```

---

## 测试数据管理

### 测试数据生成

```go
// tests/fixtures/generator.go
package fixtures

import (
    "math/rand"
    "time"
)

// User 用户测试数据
type User struct {
    ID    int
    Name  string
    Email string
}

// GenerateUsers 生成用户测试数据
func GenerateUsers(count int) []*User {
    users := make([]*User, count)
    for i := 0; i < count; i++ {
        users[i] = &User{
            ID:    i + 1,
            Name:  randomName(),
            Email: randomEmail(),
        }
    }
    return users
}

func randomName() string {
    names := []string{"Alice", "Bob", "Charlie", "David", "Eve"}
    return names[rand.Intn(len(names))]
}

func randomEmail() string {
    return fmt.Sprintf("user%d@example.com", rand.Intn(10000))
}
```

### 测试数据加载

```go
// tests/fixtures/loader.go
package fixtures

import (
    "encoding/json"
    "os"
)

// LoadFromFile 从文件加载测试数据
func LoadFromFile(path string, v interface{}) error {
    data, err := os.ReadFile(path)
    if err != nil {
        return err
    }
    return json.Unmarshal(data, v)
}

// LoadUsers 加载用户测试数据
func LoadUsers() ([]*User, error) {
    var users []*User
    err := LoadFromFile("tests/fixtures/users.json", &users)
    return users, err
}
```

---

## 性能测试

### 基准测试

```go
// internal/parser/mysql/mysql_parser_bench_test.go
package mysql

import (
    "context"
    "testing"
)

func BenchmarkDDLParser_Parse(b *testing.B) {
    parser := NewMySQLDDLParser()
    ddl := `CREATE TABLE users (
        id INT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(255) UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_name (name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        parser.Parse(context.Background(), ddl)
    }
}

func BenchmarkEventProcessing(b *testing.B) {
    processor := NewEventProcessor()
    event := &ChangeEvent{
        Type: EventTypeInsert,
        Table: TableInfo{Table: "users"},
        After: RowData{
            Fields: map[string]Field{
                "id":    {Value: 1},
                "name":  {Value: "test"},
                "email": {Value: "test@example.com"},
            },
        },
    }

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        processor.Process(event)
    }
}
```

### 负载测试

```go
// tests/load/sync_load_test.go
// +build load

package load

import (
    "context"
    "testing"
    "time"
)

// TestSyncLoad 同步负载测试
func TestSyncLoad(t *testing.T) {
    env := SetupEnvironment(t)
    defer env.Teardown()

    task := env.CreateTask(DefaultTaskConfig)
    env.WaitForTaskStatus(task.ID, TaskStatusRunning, 30*time.Second)

    // 创建表
    env.SourceDB.Exec(`
        CREATE TABLE load_test (
            id INT PRIMARY KEY,
            data VARCHAR(1000)
        )
    `)

    // 并发插入数据
    recordCount := 100000
    concurrency := 10

    start := time.Now()
    done := make(chan int, concurrency)

    for i := 0; i < concurrency; i++ {
        go func(workerID int) {
            for j := 0; j < recordCount/concurrency; j++ {
                id := workerID*(recordCount/concurrency) + j
                env.SourceDB.Exec(
                    "INSERT INTO load_test VALUES (?, ?)",
                    id, randomString(1000),
                )
            }
            done <- workerID
        }(i)
    }

    // 等待插入完成
    for i := 0; i < concurrency; i++ {
        <-done
    }
    insertDuration := time.Since(start)

    // 等待同步完成
    env.WaitForTableSync(task.ID, "load_test", recordCount, 5*time.Minute)

    // 计算吞吐量
    syncDuration := time.Since(start)
    tps := float64(recordCount) / syncDuration.Seconds()

    t.Logf("Insert: %v, Total: %v, TPS: %.2f", insertDuration, syncDuration, tps)
}
```

---

## CI/CD 集成

### GitHub Actions

```yaml
# .github/workflows/test.yml
name: Test

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  unit-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Go
        uses: actions/setup-go@v4
        with:
          go-version: '1.21'

      - name: Run unit tests
        run: go test -v -race -coverprofile=coverage.out ./...

      - name: Upload coverage
        uses: codecov/codecov-action@v3
        with:
          file: ./coverage.out

  integration-test:
    runs-on: ubuntu-latest
    services:
      mysql:
        image: mysql:8.0
        env:
          MYSQL_ROOT_PASSWORD: root
        ports:
          - 3306:3306
        options: >-
          --health-cmd "mysqladmin ping"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: postgres
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    steps:
      - uses: actions/checkout@v3

      - name: Set up Go
        uses: actions/setup-go@v4
        with:
          go-version: '1.21'

      - name: Run integration tests
        run: INTEGRATION_TEST=1 go test -v -tags=integration ./tests/integration/...
        env:
          MYSQL_HOST: localhost
          MYSQL_PORT: 3306
          POSTGRES_HOST: localhost
          POSTGRES_PORT: 5432

  e2e-test:
    runs-on: ubuntu-latest
    needs: [unit-test, integration-test]
    steps:
      - uses: actions/checkout@v3

      - name: Set up Go
        uses: actions/setup-go@v4
        with:
          go-version: '1.21'

      - name: Run E2E tests
        run: go test -v -tags=e2e ./tests/e2e/...
```

---

## 测试检查清单

### 代码提交前

- [ ] 单元测试通过
- [ ] 覆盖率满足要求
- [ ] 无竞态条件（`-race`）
- [ ] 代码格式化（`gofmt`）
- [ ] 静态检查（`golangci-lint`）

### PR 合并前

- [ ] 所有 CI 检查通过
- [ ] 集成测试通过
- [ ] 代码审查通过
- [ ] 文档更新

### 发布前

- [ ] E2E 测试通过
- [ ] 性能测试通过
- [ ] 回归测试通过
- [ ] 发布说明更新

---

*文档版本：v1.0*
*创建时间：2026-05-07*
*更新时间：2026-05-07*
