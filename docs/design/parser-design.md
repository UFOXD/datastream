# SQL Parser 设计

## 概述

DataStream 的 SQL Parser 模块负责解析 DDL 语句和过滤表达式。所有 SQL 数据库使用 ANTLR 构建完整 DDL 解析器，MongoDB 使用 noop parser。

---

## 设计原则

### 核心思路

1. **MySQL/MariaDB**: 使用 ANTLR 构建完整 DDL 解析器
2. **PostgreSQL**: 使用 ANTLR 构建完整 DDL 解析器
3. **Oracle**: 使用 ANTLR 构建完整 DDL 解析器
4. **SQL Server**: 使用 ANTLR 构建完整 DDL 解析器
5. **MongoDB**: 无需 SQL Parser，直接处理 BSON/JSON

### Parser 角色定位

| 场景 | 需要解析 | 说明 |
|------|---------|------|
| DDL 变更检测 | 所有 SQL 数据库需要解析 DDL | 统一使用 ANTLR 解析 |
| 同步范围过滤 | 解析表名、库名匹配规则 | 简单的通配符匹配 |
| 路由规则 | 解析路由表达式 | 可选，高级功能 |

---

## 接口定义

### DDLParser 接口

```go
package parser

import "context"

// DDLParser DDL 解析器接口
type DDLParser interface {
    // Parse 解析 DDL 语句，支持解析多条 DDL（以分号分隔）
    // 返回所有解析结果的切片
    Parse(ctx context.Context, ddl string) ([]*DDLResult, error)

    // SupportedTypes 返回支持的 DDL 类型
    SupportedTypes() []DDLType
}

// DDLResults 用于在 Visitor 中收集多个 DDL 解析结果
type DDLResults struct {
    Results []*DDLResult
}

// Add 添加一个结果到集合
func (r *DDLResults) Add(result *DDLResult) {
    if result != nil {
        r.Results = append(r.Results, result)
    }
}

// DDLResult DDL 解析结果
type DDLResult struct {
    // DDL 类型
    Type DDLType

    // 数据库名
    Database string

    // 表名（可选）
    Table string

    // 原 DDL 语句
    Statement string

    // 表结构变更（针对 CREATE/ALTER TABLE）
    TableChanges *TableChanges

    // 索引变更（针对 CREATE/DROP INDEX）
    IndexChanges *IndexChanges

    // 其他元数据
    Metadata map[string]interface{}
}

// DDLType DDL 类型
type DDLType string

const (
    DDLTypeCreateDatabase DDLType = "create_database"
    DDLTypeDropDatabase   DDLType = "drop_database"
    DDLTypeAlterDatabase  DDLType = "alter_database"

    DDLTypeCreateTable DDLType = "create_table"
    DDLTypeDropTable   DDLType = "drop_table"
    DDLTypeAlterTable  DDLType = "alter_table"
    DDLTypeRenameTable DDLType = "rename_table"
    DDLTypeTruncate    DDLType = "truncate_table"

    DDLTypeCreateIndex DDLType = "create_index"
    DDLTypeDropIndex   DDLType = "drop_index"

    DDLTypeCreateView DDLType = "create_view"
    DDLTypeDropView   DDLType = "drop_view"

    DDLTypeUnknown DDLType = "unknown"
)

// TableChanges 表结构变更
type TableChanges struct {
    // 操作类型
    Operation TableOperation

    // 表信息
    Table *TableInfo

    // 新增列
    AddedColumns []ColumnInfo

    // 删除列
    DroppedColumns []string

    // 修改列
    ModifiedColumns []ColumnModification

    // 主键变更
    PrimaryKeyChange *PrimaryKeyChange
}

// TableOperation 表操作类型
type TableOperation string

const (
    TableOpCreate TableOperation = "create"
    TableOpAlter  TableOperation = "alter"
    TableOpDrop   TableOperation = "drop"
    TableOpRename TableOperation = "rename"
)

// ColumnModification 列修改
type ColumnModification struct {
    Old ColumnInfo
    New ColumnInfo
}

// PrimaryKeyChange 主键变更
type PrimaryKeyChange struct {
    OldColumns []string
    NewColumns []string
}
```

---

## MySQL ANTLR Parser

### 架构设计

```
MySQL DDL 语句
      │
      ▼
┌─────────────────────┐
│   ANTLR Lexer       │  ← MySQLLexer.g4
└─────────────────────┘
      │
      ▼
┌─────────────────────┐
│   ANTLR Parser      │  ← MySQLParser.g4
└─────────────────────┘
      │
      ▼
┌─────────────────────┐
│   Parse Tree        │
└─────────────────────┘
      │
      ▼
┌─────────────────────┐
│   DDL Visitor       │  ← 自定义 Visitor 提取结构化信息
└─────────────────────┘
      │
      ▼
┌─────────────────────┐
│   DDLResult         │
└─────────────────────┘
```

### 项目结构

```
datastream/
├── parser/
│   ├── parser.go           # Parser 接口定义
│   ├── mysql/
│   │   ├── mysql_parser.go # MySQL Parser 实现
│   │   ├── visitor.go      # DDL Visitor
│   │   ├── listener.go     # DDL Listener (可选)
│   │   └── generated/      # ANTLR 生成的代码
│   │       ├── mysql_lexer.go
│   │       ├── mysql_parser.go
│   │       └── ...
│   ├── postgres/
│   │   ├── postgres_parser.go # PostgreSQL Parser 实现
│   │   ├── visitor.go         # DDL Visitor
│   │   └── generated/         # ANTLR 生成的代码
│   ├── oracle/
│   │   ├── oracle_parser.go   # Oracle Parser 实现
│   │   ├── visitor.go         # DDL Visitor
│   │   └── generated/         # ANTLR 生成的代码
│   ├── sqlserver/
│   │   ├── sqlserver_parser.go # SQL Server Parser 实现
│   │   ├── visitor.go          # DDL Visitor
│   │   └── generated/          # ANTLR 生成的代码
│   └── noop/
│       └── noop_parser.go  # 空实现（仅用于 MongoDB）
```

### ANTLR Grammar

复用 Debezium 的 MySQL grammar 文件，或使用官方 MySQL grammar：

```bash
# 下载 MySQL ANTLR grammar
# 来源: https://github.com/antlr/grammars-v4/tree/master/sql/mysql

antlr4 -Dlanguage=Go -visitor -o generated MySQLLexer.g4
antlr4 -Dlanguage=Go -visitor -o generated MySQLParser.g4
```

**重要：** 必须添加 `-visitor` 参数以生成 visitor 接口，用于 DDL 结构信息提取。

### MySQL Parser 实现

```go
package mysql

import (
    "context"

    "datastream/parser"
    "datastream/parser/mysql/generated"
)

// MySQLDDLParser MySQL DDL 解析器
type MySQLDDLParser struct {
    visitor *DDLVisitor
}

// NewMySQLDDLParser 创建 MySQL DDL 解析器
func NewMySQLDDLParser() *MySQLDDLParser {
    return &MySQLDDLParser{
        visitor: NewDDLVisitor(),
    }
}

// Parse 解析 DDL 语句，支持解析多条 DDL（以分号分隔）
func (p *MySQLDDLParser) Parse(ctx context.Context, ddl string) ([]*parser.DDLResult, error) {
    // 1. 创建 Lexer
    input := antlr.NewInputStream(ddl)
    lexer := generated.NewMySQLLexer(input)
    tokenStream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

    // 2. 创建 Parser
    antlrParser := generated.NewMySQLParser(tokenStream)

    // 3. 解析
    tree := antlrParser.Root()

    // 4. 遍历语法树，提取结构化信息
    result := tree.Accept(p.visitor)
    if results, ok := result.(*parser.DDLResults); ok {
        return results.Results, nil
    }

    // Fallback for unexpected result type
    return []*parser.DDLResult{{
        Type:      parser.DDLTypeUnknown,
        Statement: ddl,
    }}, nil
}

// SupportedTypes 返回支持的 DDL 类型
func (p *MySQLDDLParser) SupportedTypes() []parser.DDLType {
    return []parser.DDLType{
        parser.DDLTypeCreateDatabase,
        parser.DDLTypeDropDatabase,
        parser.DDLTypeCreateTable,
        parser.DDLTypeDropTable,
        parser.DDLTypeAlterTable,
        parser.DDLTypeRenameTable,
        parser.DDLTypeTruncate,
        parser.DDLTypeCreateIndex,
        parser.DDLTypeDropIndex,
    }
}
```

### DDL Visitor

```go
package mysql

import (
    "datastream/parser"
    "datastream/parser/mysql/generated"

    "github.com/antlr/antlr4/runtime/Go/antlr/v4"
)

// DDLVisitor DDL 语法树访问器
type DDLVisitor struct {
    *generated.BaseMySQLParserVisitor
}

// NewDDLVisitor 创建 Visitor
func NewDDLVisitor() *DDLVisitor {
    return &DDLVisitor{
        BaseMySQLParserVisitor: &generated.BaseMySQLParserVisitor{},
    }
}

// VisitRoot 访问根节点，收集所有 DDL 语句的结果
func (v *DDLVisitor) VisitRoot(ctx *generated.RootContext) interface{} {
    v.ddl = ctx.GetText()
    results := &parser.DDLResults{}

    if sqlStmts := ctx.SqlStatements(); sqlStmts != nil {
        stmtResults := v.VisitSqlStatements(sqlStmts.(*generated.SqlStatementsContext))
        if sr, ok := stmtResults.(*parser.DDLResults); ok {
            results.Results = append(results.Results, sr.Results...)
        }
    }

    return results
}

// VisitSqlStatements 访问 SQL 语句集合
func (v *DDLVisitor) VisitSqlStatements(ctx *generated.SqlStatementsContext) interface{} {
    results := &parser.DDLResults{}

    for _, stmt := range ctx.AllSqlStatement() {
        result := v.VisitSqlStatement(stmt.(*generated.SqlStatementContext))
        if ddlResult, ok := result.(*parser.DDLResult); ok && ddlResult != nil {
            results.Add(ddlResult)
        }
    }

    return results
}

// VisitStatement 访问语句节点
func (v *DDLVisitor) VisitStatement(ctx *generated.StatementContext) interface{} {
    if createTable := ctx.CreateTable(); createTable != nil {
        return v.visitCreateTable(createTable.(*generated.CreateTableContext))
    }
    if alterTable := ctx.AlterTable(); alterTable != nil {
        return v.visitAlterTable(alterTable.(*generated.AlterTableContext))
    }
    if dropTable := ctx.DropTable(); dropTable != nil {
        return v.visitDropTable(dropTable.(*generated.DropTableContext))
    }
    // ... 其他 DDL 类型
    return nil
}

// visitCreateTable 处理 CREATE TABLE
func (v *DDLVisitor) visitCreateTable(ctx *generated.CreateTableContext) *parser.DDLResult {
    result := &parser.DDLResult{
        Type:      parser.DDLTypeCreateTable,
        Database:  v.extractDatabaseName(ctx),
        Table:     v.extractTableName(ctx),
        Statement: v.ddl,
    }

    // 解析列定义
    result.TableChanges = &parser.TableChanges{
        Operation:     parser.TableOpCreate,
        Table:         v.extractTableInfo(ctx),
        AddedColumns:  v.extractColumns(ctx),
    }

    return result
}

// visitAlterTable 处理 ALTER TABLE
func (v *DDLVisitor) visitAlterTable(ctx *generated.AlterTableContext) *parser.DDLResult {
    result := &parser.DDLResult{
        Type:      parser.DDLTypeAlterTable,
        Database:  v.extractDatabaseName(ctx),
        Table:     v.extractTableName(ctx),
        Statement: v.ddl,
    }

    // 解析 ALTER 子句
    tableChanges := &parser.TableChanges{
        Operation: parser.TableOpAlter,
    }

    for _, option := range ctx.AllAlterOption() {
        v.processAlterOption(option, tableChanges)
    }

    result.TableChanges = tableChanges
    return result
}
```

---

## Noop Parser

对于不需要完整 SQL 解析的数据库（仅 MongoDB），提供空实现：

```go
package noop

import (
    "context"

    "datastream/parser"
)

// NoopParser 空解析器
type NoopParser struct{}

// NewNoopParser 创建空解析器
func NewNoopParser() *NoopParser {
    return &NoopParser{}
}

// Parse 返回空结果（MongoDB 从 Change Stream 获取结构化数据）
func (p *NoopParser) Parse(ctx context.Context, ddl string) ([]*parser.DDLResult, error) {
    // MongoDB 的 Change Stream 机制已提供结构化的 DDL 事件
    // 不需要解析 SQL
    return []*parser.DDLResult{{
        Type:      parser.DDLTypeUnknown,
        Statement: ddl,
    }}, nil
}

// SupportedTypes 返回空列表
func (p *NoopParser) SupportedTypes() []parser.DDLType {
    return []parser.DDLType{}
}
```

---

## PostgreSQL ANTLR Parser

### ANTLR Grammar

使用 ANTLR grammars-v4 官方 PostgreSQL grammar：

```bash
# 下载 PostgreSQL ANTLR grammar
# 来源: https://github.com/antlr/grammars-v4/tree/master/sql/postgresql

antlr4 -Dlanguage=Go -visitor -o generated PostgreSQLLexer.g4
antlr4 -Dlanguage=Go -visitor -o generated PostgreSQLParser.g4
```

**重要：** 必须添加 `-visitor` 参数以生成 visitor 接口。

### 注意事项

PostgreSQL grammar 需要自定义 `PostgreSQLLexerBase` 和 `PostgreSQLParserBase` 基类来处理：
- 动态 SQL 语义
- dollar-quoted 字符串
- 嵌套注释

---

## Oracle ANTLR Parser

### ANTLR Grammar

使用 ANTLR grammars-v4 官方 PL/SQL grammar：

```bash
# 下载 Oracle PL/SQL ANTLR grammar
# 来源: https://github.com/antlr/grammars-v4/tree/master/sql/plsql

antlr4 -Dlanguage=Go -visitor -o generated PlSqlLexer.g4
antlr4 -Dlanguage=Go -visitor -o generated PlSqlParser.g4
```

**重要：** 必须添加 `-visitor` 参数以生成 visitor 接口。

### 注意事项

PL/SQL grammar 需要自定义 `PlSqlLexerBase` 和 `PlSqlParserBase` 基类来处理：
- PL/SQL 特有语法
- 绑定变量
- 条件编译

---

## SQL Server ANTLR Parser

### ANTLR Grammar

使用 ANTLR grammars-v4 官方 T-SQL grammar：

```bash
# 下载 SQL Server T-SQL ANTLR grammar
# 来源: https://github.com/antlr/grammars-v4/tree/master/sql/tsql

antlr4 -Dlanguage=Go -visitor -o generated TSqlLexer.g4
antlr4 -Dlanguage=Go -visitor -o generated TSqlParser.g4
```

**重要：** 必须添加 `-visitor` 参数以生成 visitor 接口。

---

## Parser Registry

```go
package parser

import "sync"

// Registry Parser 注册表
type Registry struct {
    mu      sync.RWMutex
    parsers map[string]DDLParser
}

// NewRegistry 创建注册表
func NewRegistry() *Registry {
    return &Registry{
        parsers: make(map[string]DDLParser),
    }
}

// Register 注册 Parser
func (r *Registry) Register(connectorType string, parser DDLParser) {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.parsers[connectorType] = parser
}

// Get 获取 Parser
func (r *Registry) Get(connectorType string) DDLParser {
    r.mu.RLock()
    defer r.mu.RUnlock()
    return r.parsers[connectorType]
}

// DefaultRegistry 全局默认注册表
var DefaultRegistry = NewRegistry()

func init() {
    // 注册 MySQL Parser
    DefaultRegistry.Register("mysql", mysql.NewMySQLDDLParser())
    DefaultRegistry.Register("mariadb", mysql.NewMySQLDDLParser())

    // 注册 PostgreSQL Parser
    DefaultRegistry.Register("postgresql", postgres.NewPostgreSQLDDLParser())

    // 注册 Oracle Parser
    DefaultRegistry.Register("oracle", oracle.NewOracleDDLParser())

    // 注册 SQL Server Parser
    DefaultRegistry.Register("sqlserver", sqlserver.NewSQLServerDDLParser())

    // MongoDB 使用 Noop Parser
    DefaultRegistry.Register("mongodb", noop.NewNoopParser())
}
```

---

## 使用示例

### Source Connector 中使用

```go
func (s *MySQLSourceConnector) processDDL(binlogEvent *BinlogEvent) error {
    // 获取 Parser
    parser := parser.DefaultRegistry.Get("mysql")

    // 解析 DDL（支持多条 DDL 语句）
    results, err := parser.Parse(context.Background(), binlogEvent.DDL)
    if err != nil {
        return err
    }

    // 遍历所有解析结果
    for _, result := range results {
        // 根据 DDL 类型处理
        switch result.Type {
        case parser.DDLTypeCreateTable:
            if err := s.handleCreateTable(result); err != nil {
                return err
            }
        case parser.DDLTypeAlterTable:
            if err := s.handleAlterTable(result); err != nil {
                return err
            }
        case parser.DDLTypeDropTable:
            if err := s.handleDropTable(result); err != nil {
                return err
            }
        }
    }

    return nil
}
```

### PostgreSQL 使用 Parser

```go
func (s *PostgreSQLSourceConnector) processDDL(pgEvent *PgEvent) error {
    // 获取 Parser
    parser := parser.DefaultRegistry.Get("postgresql")

    // 解析 DDL（支持多条 DDL 语句）
    results, err := parser.Parse(context.Background(), pgEvent.DDL)
    if err != nil {
        return err
    }

    // 遍历所有解析结果
    for _, result := range results {
        // 根据 DDL 类型处理
        switch result.Type {
        case parser.DDLTypeCreateTable:
            if err := s.handleCreateTable(result); err != nil {
                return err
            }
        case parser.DDLTypeAlterTable:
            if err := s.handleAlterTable(result); err != nil {
                return err
            }
        case parser.DDLTypeDropTable:
            if err := s.handleDropTable(result); err != nil {
                return err
            }
        }
    }

    return nil
}
```

### 解析多条 DDL 示例

```go
func ExampleParseMultipleDDL() {
    p := mysql.NewParser()

    // 输入包含多条 DDL 语句（以分号分隔）
    ddl := "CREATE TABLE users (id INT); DROP TABLE temp;"

    results, err := p.Parse(context.Background(), ddl)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Parsed %d DDL statements\n", len(results))
    // Output: Parsed 2 DDL statements

    for i, result := range results {
        fmt.Printf("Statement %d: %s\n", i+1, result.Type)
    }
    // Output:
    // Statement 1: create_table
    // Statement 2: drop_table
}
```

---

## ANTLR 开发流程

### 1. 准备 Grammar 文件

```bash
# 从 Debezium 或 ANTLR grammars 仓库获取
# https://github.com/debezium/debezium/tree/main/debezium-ddl-parser
# 或
# https://github.com/antlr/grammars-v4/tree/master/sql/mysql
```

### 2. 生成 Go 代码

```bash
# 安装 ANTLR
brew install antlr

# 生成 Go 代码（必须添加 -visitor 参数）
antlr4 -Dlanguage=Go -visitor \
    -o parser/mysql/generated \
    -package generated \
    MySQLLexer.g4 MySQLParser.g4
```

### 3. 实现 Visitor

```bash
# 实现 BaseMySQLParserVisitor 接口
# 提取 DDL 中的结构化信息
```

---

## 测试策略

### 单元测试

```go
func TestMySQLDDLParser_CreateTable(t *testing.T) {
    p := mysql.NewParser()

    ddl := `CREATE TABLE users (
        id INT PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(255) UNIQUE
    )`

    results, err := p.Parse(context.Background(), ddl)
    require.NoError(t, err)
    require.Len(t, results, 1)

    result := results[0]
    assert.Equal(t, parser.DDLTypeCreateTable, result.Type)
    assert.Equal(t, "users", result.Table)
    assert.Len(t, result.TableChanges.AddedColumns, 3)
}

func TestMySQLDDLParser_MultipleStatements(t *testing.T) {
    p := mysql.NewParser()

    // 测试解析多条 DDL 语句
    ddl := "CREATE TABLE users (id INT); DROP TABLE users;"

    results, err := p.Parse(context.Background(), ddl)
    require.NoError(t, err)
    require.Len(t, results, 2)

    // 验证第一条语句
    assert.Equal(t, parser.DDLTypeCreateTable, results[0].Type)
    assert.Equal(t, "users", results[0].Table)

    // 验证第二条语句
    assert.Equal(t, parser.DDLTypeDropTable, results[1].Type)
    assert.Equal(t, "users", results[1].Table)
}
```

### 兼容性测试

```go
// 使用 Debezium 的 DDL 测试用例验证解析结果
func TestMySQLDDLParser_DebeziumCompatibility(t *testing.T) {
    testCases := []struct {
        name string
        ddl  string
        // ...
    }{
        // 从 Debezium 测试集导入
    }
}
```

---

## 设计决策记录

| 决策项 | 选择 | 原因 |
|--------|------|------|
| MySQL Parser | ANTLR | 与 Debezium 一致，完整支持 MySQL 方言 |
| PostgreSQL Parser | ANTLR | 完整支持 PostgreSQL 方言，统一解析架构 |
| Oracle Parser | ANTLR | 完整支持 PL/SQL 方言，统一解析架构 |
| SQL Server Parser | ANTLR | 完整支持 T-SQL 方言，统一解析架构 |
| MongoDB | noop | Change Stream 提供结构化输出 |
| Grammar 来源 | grammars-v4 官方 | 可复用成熟的 grammar 定义 |
| 多条 DDL 支持 | `[]*DDLResult` | 支持解析以分号分隔的多条 DDL 语句，提高实用性 |

---

*文档版本：v2.1*
*创建时间：2026-05-07*
*更新时间：2026-05-10*

### 更新历史

**v2.1 (2026-05-10)**
- 修改 `Parse()` 接口返回类型为 `[]*DDLResult`，支持解析多条 DDL 语句
- 添加 `DDLResults` 辅助类型用于 Visitor 收集结果
- 更新所有 Parser 实现和测试用例

**v2.0 (2026-05-09)**
- 初始 ANTLR Parser 架构设计
- 支持 MySQL、PostgreSQL、Oracle、SQL Server 四种数据库
