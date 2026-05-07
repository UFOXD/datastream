# SQL Parser 设计

## 概述

DataStream 的 SQL Parser 模块负责解析 DDL 语句和过滤表达式。根据 Debezium 的实践，只有 MySQL/MariaDB 需要完整的 SQL 解析器，其他数据库的 CDC 机制已提供结构化输出。

---

## 设计原则

### 核心思路

1. **MySQL/MariaDB**: 使用 ANTLR 构建完整 DDL 解析器
2. **PostgreSQL**: 利用 pgoutput 逻辑解码的结构化输出
3. **MongoDB**: 无需 SQL Parser，直接处理 BSON/JSON
4. **Oracle**: LogMiner + 数据字典查询
5. **SQL Server**: CDC API 结构化输出

### Parser 角色定位

| 场景 | 需要解析 | 说明 |
|------|---------|------|
| DDL 变更检测 | MySQL 需要解析 binlog 中的 DDL | 其他数据库 CDC 已结构化 |
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
    // Parse 解析 DDL 语句
    Parse(ctx context.Context, ddl string) (*DDLResult, error)

    // SupportedTypes 返回支持的 DDL 类型
    SupportedTypes() []DDLType
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
│   └── noop/
│       └── noop_parser.go  # 空实现（用于不需要解析的数据库）
```

### ANTLR Grammar

复用 Debezium 的 MySQL grammar 文件，或使用官方 MySQL grammar：

```bash
# 下载 MySQL ANTLR grammar
# 来源: https://github.com/antlr/grammars-v4/tree/master/sql/mysql

antlr4 -Dlanguage=Go -o generated MySQLLexer.g4
antlr4 -Dlanguage=Go -o generated MySQLParser.g4
```

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

// Parse 解析 DDL 语句
func (p *MySQLDDLParser) Parse(ctx context.Context, ddl string) (*parser.DDLResult, error) {
    // 1. 创建 Lexer
    input := antlr.NewInputStream(ddl)
    lexer := generated.NewMySQLLexer(input)
    tokenStream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

    // 2. 创建 Parser
    antlrParser := generated.NewMySQLParser(tokenStream)

    // 3. 解析
    tree := antlrParser.Root()

    // 4. 遍历语法树，提取结构化信息
    return p.visitor.Visit(tree).(*parser.DDLResult), nil
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

// VisitRoot 访问根节点
func (v *DDLVisitor) VisitRoot(ctx *generated.RootContext) interface{} {
    for _, stmt := range ctx.AllStatement() {
        result := v.VisitStatement(stmt.(*generated.StatementContext))
        if result != nil {
            return result
        }
    }
    return &parser.DDLResult{Type: parser.DDLTypeUnknown}
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
        Statement: ctx.GetText(),
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
        Statement: ctx.GetText(),
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

对于不需要完整 SQL 解析的数据库（PostgreSQL、MongoDB、SQL Server），提供空实现：

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

// Parse 返回空结果（这些数据库从 CDC 流中获取结构化数据）
func (p *NoopParser) Parse(ctx context.Context, ddl string) (*parser.DDLResult, error) {
    // 这些数据库的 CDC 机制已提供结构化的 DDL 事件
    // 不需要解析 SQL
    return &parser.DDLResult{
        Type:      parser.DDLTypeUnknown,
        Statement: ddl,
    }, nil
}

// SupportedTypes 返回空列表
func (p *NoopParser) SupportedTypes() []parser.DDLType {
    return []parser.DDLType{}
}
```

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

    // 其他数据库使用 Noop Parser
    noop := noop.NewNoopParser()
    DefaultRegistry.Register("postgresql", noop)
    DefaultRegistry.Register("mongodb", noop)
    DefaultRegistry.Register("oracle", noop)
    DefaultRegistry.Register("sqlserver", noop)
}
```

---

## 使用示例

### Source Connector 中使用

```go
func (s *MySQLSourceConnector) processDDL(binlogEvent *BinlogEvent) error {
    // 获取 Parser
    parser := parser.DefaultRegistry.Get("mysql")

    // 解析 DDL
    result, err := parser.Parse(context.Background(), binlogEvent.DDL)
    if err != nil {
        return err
    }

    // 根据 DDL 类型处理
    switch result.Type {
    case parser.DDLTypeCreateTable:
        return s.handleCreateTable(result)
    case parser.DDLTypeAlterTable:
        return s.handleAlterTable(result)
    case parser.DDLTypeDropTable:
        return s.handleDropTable(result)
    }

    return nil
}
```

### PostgreSQL 不需要解析

```go
func (s *PostgreSQLSourceConnector) processReplicationMessage(msg *pgoutput.Message) error {
    switch m := msg.(type) {
    case *pgoutput.RelationMessage:
        // pgoutput 已经提供了结构化的表结构信息
        return s.handleRelationMessage(m)
    case *pgoutput.InsertMessage:
        return s.handleInsert(m)
    }
    return nil
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

# 生成 Go 代码
antlr4 -Dlanguage=Go \
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
    parser := mysql.NewMySQLDDLParser()

    ddl := `CREATE TABLE users (
        id INT PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(255) UNIQUE
    )`

    result, err := parser.Parse(context.Background(), ddl)
    require.NoError(t, err)

    assert.Equal(t, parser.DDLTypeCreateTable, result.Type)
    assert.Equal(t, "users", result.Table)
    assert.Len(t, result.TableChanges.AddedColumns, 3)
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
| PostgreSQL | 不需要 | pgoutput 提供结构化输出 |
| MongoDB | 不需要 | Change Stream 提供结构化输出 |
| Grammar 来源 | Debezium/官方 | 可复用成熟的 grammar 定义 |

---

*文档版本：v1.0*
*创建时间：2026-05-07*
*更新时间：2026-05-07*
