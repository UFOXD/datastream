# 事件模型设计

## 概述

DataStream 的核心是统一的事件模型，用于抽象不同数据库的变更事件。所有 Source Connector 产生的事件都转换为统一格式，Pipeline 和 Sink Connector 基于统一格式进行处理。

---

## 核心数据结构

### ChangeEvent

变更事件的核心结构，代表一条数据变更：

```go
// ChangeEvent 代表一条数据变更事件
type ChangeEvent struct {
    // 唯一标识
    ID string `json:"id"` // 格式: {source}:{timestamp}:{sequence}

    // 事件来源
    Source SourceInfo `json:"source"`

    // 事件类型
    Type EventType `json:"type"`

    // 数据位置
    Position Position `json:"position"`

    // 表信息
    Table TableInfo `json:"table"`

    // 变更数据
    Before RowData `json:"before,omitempty"` // 变更前数据（UPDATE/DELETE）
    After  RowData `json:"after,omitempty"`  // 变更后数据（INSERT/UPDATE）

    // 时间戳
    Timestamp time.Time `json:"timestamp"`

    // 事务信息
    Transaction *TransactionInfo `json:"transaction,omitempty"`

    // Schema 信息
    Schema *SchemaInfo `json:"schema,omitempty"`

    // 元数据
    Metadata map[string]string `json:"metadata,omitempty"`
}
```

### SourceInfo

数据源信息：

```go
// SourceInfo 描述事件来源
type SourceInfo struct {
    // Connector 类型
    Connector string `json:"connector"` // mysql, postgresql, mongodb, oracle, sqlserver, mariadb

    // 数据库连接信息
    Host     string `json:"host"`
    Port     int    `json:"port"`
    Database string `json:"database"`

    // 集群信息（如果适用）
    ClusterName string `json:"clusterName,omitempty"`
    ServerName  string `json:"serverName,omitempty"`

    // 采集信息
    Snapshot bool `json:"snapshot"` // 是否来自快照
}
```

### EventType

事件类型枚举：

```go
// EventType 事件类型
type EventType string

const (
    EventTypeInsert    EventType = "insert"    // 插入
    EventTypeUpdate    EventType = "update"    // 更新
    EventTypeDelete    EventType = "delete"    // 删除
    EventTypeTruncate  EventType = "truncate"  // 截断表
    EventTypeDDL       EventType = "ddl"       // DDL 语句
    EventTypeHeartbeat EventType = "heartbeat" // 心跳
    EventTypeTombstone EventType = "tombstone" // 墓碑消息（用于清理）
)
```

### Position

数据位置，用于记录同步进度和断点恢复：

```go
// Position 记录数据位置
type Position struct {
    // 二进制日志位置（MySQL/MariaDB）
    BinlogFile string `json:"binlogFile,omitempty"`
    BinlogPos  uint32 `json:"binlogPos,omitempty"`

    // LSN 位置（PostgreSQL）
    LSN uint64 `json:"lsn,omitempty"`

    // SCN 位置（Oracle）
    SCN uint64 `json:"scn,omitempty"`

    // Oplog/Timestamp 位置（MongoDB）
    Timestamp uint64 `json:"timestamp,omitempty"`
    Order     int    `json:"order,omitempty"`

    // SQL Server 位置
    ChangeLsn string `json:"changeLsn,omitempty"`

    // 通用时间戳（用于比较）
    CommitTime time.Time `json:"commitTime"`

    // 事务序列号
    TxID    string `json:"txId,omitempty"`
    SeqNo   int    `json:"seqNo,omitempty"`   // 事务内序号
    Total   int    `json:"total,omitempty"`   // 事务内总事件数
}
```

### TableInfo

表信息：

```go
// TableInfo 表元数据
type TableInfo struct {
    // 数据库名
    Database string `json:"database"`

    // Schema 名（PostgreSQL/Oracle）
    Schema string `json:"schema,omitempty"`

    // 表名
    Table string `json:"table"`

    // 主键列
    PrimaryKeyColumns []string `json:"primaryKeyColumns,omitempty"`

    // 唯一索引列（用于无主键表）
    UniqueKeyColumns []string `json:"uniqueKeyColumns,omitempty"`

    // 列信息
    Columns []ColumnInfo `json:"columns,omitempty"`
}

// ColumnInfo 列元数据
type ColumnInfo struct {
    Name     string `json:"name"`
    Type     string `json:"type"`     // 数据库原始类型
    Nullable bool   `json:"nullable"`
    Length   int    `json:"length,omitempty"`
    Scale    int    `json:"scale,omitempty"`
}
```

### RowData

行数据，表示一条记录：

```go
// RowData 表示一行数据
type RowData struct {
    // 字段值映射（列名 -> 值）
    Fields map[string]Field `json:"fields"`
}

// Field 表示一个字段值
type Field struct {
    // 字段名
    Name string `json:"name"`

    // 字段值
    Value interface{} `json:"value"`

    // 字段类型
    Type string `json:"type"`

    // 是否为 NULL
    Null bool `json:"null,omitempty"`
}
```

### SchemaInfo

Schema 信息：

```go
// SchemaInfo 描述数据的 Schema
type SchemaInfo struct {
    // 版本号
    Version int64 `json:"version"`

    // 列定义
    Columns []ColumnSchema `json:"columns"`

    // 主键
    PrimaryKey []string `json:"primaryKey,omitempty"`
}

// ColumnSchema 列 Schema 定义
type ColumnSchema struct {
    Name     string      `json:"name"`
    Type     string      `json:"type"`
    Optional bool        `json:"optional"`
    Default  interface{} `json:"default,omitempty"`
}
```

### TransactionInfo

事务信息：

```go
// TransactionInfo 事务信息
type TransactionInfo struct {
    // 事务 ID
    ID string `json:"id"`

    // 事务内事件总数
    TotalEvents int `json:"totalEvents"`

    // 当前事件在事务内的序号
    EventIndex int `json:"eventIndex"`

    // 事务开始时间
    BeginTime time.Time `json:"beginTime"`

    // 事务提交时间
    CommitTime time.Time `json:"commitTime"`
}
```

---

## 位置比较与序列化

### Position 比较

```go
// Compare 比较两个 Position
// 返回: -1 p < other, 0 p == other, 1 p > other
func (p *Position) Compare(other *Position) (int, error) {
    // 按提交时间比较
    if p.CommitTime.Before(other.CommitTime) {
        return -1, nil
    }
    if p.CommitTime.After(other.CommitTime) {
        return 1, nil
    }

    // 时间相同，比较事务内序号
    if p.SeqNo < other.SeqNo {
        return -1, nil
    }
    if p.SeqNo > other.SeqNo {
        return 1, nil
    }

    return 0, nil
}
```

### Position 序列化

```go
// MarshalBinary 将 Position 序列化为字节
func (p *Position) MarshalBinary() ([]byte, error) {
    return json.Marshal(p)
}

// UnmarshalBinary 从字节反序列化 Position
func (p *Position) UnmarshalBinary(data []byte) error {
    return json.Unmarshal(data, p)
}

// String 返回 Position 的字符串表示
func (p *Position) String() string {
    return fmt.Sprintf("%s:%d:%d", p.TxID, p.SeqNo, p.Total)
}
```

---

## 事件 ID 生成

```go
// GenerateID 生成事件唯一 ID
func GenerateEventID(source *SourceInfo, timestamp time.Time, seqNo int) string {
    return fmt.Sprintf("%s:%s:%s:%d",
        source.Connector,
        source.Database,
        timestamp.Format("20060102150405.000"),
        seqNo,
    )
}
```

---

## 不同数据库的适配

### MySQL/MariaDB

```go
// MySQL 事件转换
type MySQLEventConverter struct{}

func (c *MySQLEventConverter) Convert(mysqlEvent *MySQLBinlogEvent) (*ChangeEvent, error) {
    return &ChangeEvent{
        ID:        GenerateEventID(source, mysqlEvent.Timestamp, mysqlEvent.SeqNo),
        Source:    source,
        Type:      convertEventType(mysqlEvent.Type),
        Position: Position{
            BinlogFile:  mysqlEvent.BinlogFile,
            BinlogPos:   mysqlEvent.BinlogPos,
            CommitTime:  mysqlEvent.Timestamp,
            TxID:        mysqlEvent.TxID,
            SeqNo:       mysqlEvent.SeqNo,
        },
        Table:     convertTableInfo(mysqlEvent.Table),
        Before:    convertRowData(mysqlEvent.Before),
        After:     convertRowData(mysqlEvent.After),
        Timestamp: mysqlEvent.Timestamp,
    }, nil
}
```

### PostgreSQL

```go
// PostgreSQL 事件转换
type PostgresEventConverter struct{}

func (c *PostgresEventConverter) Convert(pgEvent *PostgresWALEvent) (*ChangeEvent, error) {
    return &ChangeEvent{
        ID:        GenerateEventID(source, pgEvent.Timestamp, pgEvent.SeqNo),
        Source:    source,
        Type:      convertEventType(pgEvent.Type),
        Position: Position{
            LSN:        pgEvent.LSN,
            CommitTime: pgEvent.Timestamp,
            TxID:       pgEvent.TxID,
            SeqNo:      pgEvent.SeqNo,
        },
        Table:     convertTableInfo(pgEvent.Table),
        Before:    convertRowData(pgEvent.Before),
        After:     convertRowData(pgEvent.After),
        Timestamp: pgEvent.Timestamp,
    }, nil
}
```

### MongoDB

```go
// MongoDB 事件转换
type MongoDBEventConverter struct{}

func (c *MongoDBEventConverter) Convert(mongoEvent *MongoOplogEvent) (*ChangeEvent, error) {
    return &ChangeEvent{
        ID:        GenerateEventID(source, mongoEvent.Timestamp, 0),
        Source:    source,
        Type:      convertEventType(mongoEvent.Operation),
        Position: Position{
            Timestamp:  mongoEvent.Timestamp,
            Order:      mongoEvent.Order,
            CommitTime: time.Unix(int64(mongoEvent.Timestamp), 0),
        },
        Table: TableInfo{
            Database: mongoEvent.Ns.Database,
            Table:    mongoEvent.Ns.Collection,
        },
        After:     convertDocumentToRowData(mongoEvent.Document),
        Timestamp: time.Unix(int64(mongoEvent.Timestamp), 0),
    }, nil
}
```

---

## 心跳事件

```go
// HeartbeatEvent 心跳事件
type HeartbeatEvent struct {
    Source    SourceInfo `json:"source"`
    Timestamp time.Time  `json:"timestamp"`
    Position  Position   `json:"position"`
}

// ToChangeEvent 转换为 ChangeEvent
func (h *HeartbeatEvent) ToChangeEvent() *ChangeEvent {
    return &ChangeEvent{
        ID:        fmt.Sprintf("%s:heartbeat:%d", h.Source.Connector, h.Timestamp.UnixNano()),
        Source:    h.Source,
        Type:      EventTypeHeartbeat,
        Position:  h.Position,
        Timestamp: h.Timestamp,
    }
}
```

---

## DDL 事件

```go
// DDLEvent DDL 变更事件
type DDLEvent struct {
    ChangeEvent
    DDL       DDLInfo `json:"ddl"`
}

// DDLInfo DDL 信息
type DDLInfo struct {
    Type      DDLType `json:"type"`      // CREATE, ALTER, DROP, RENAME, TRUNCATE
    Statement string  `json:"statement"` // 原始 DDL 语句
    Database  string  `json:"database"`
    Table     string  `json:"table,omitempty"`
}

// DDLType DDL 类型
type DDLType string

const (
    DDLTypeCreate    DDLType = "create"
    DDLTypeAlter     DDLType = "alter"
    DDLTypeDrop      DDLType = "drop"
    DDLTypeRename    DDLType = "rename"
    DDLTypeTruncate  DDLType = "truncate"
)
```

---

## 设计原则

### 1. 统一抽象

- 所有数据库的变更事件统一转换为 `ChangeEvent`
- Source Connector 负责数据库特定格式到统一格式的转换
- Sink Connector 只需要处理统一格式的事件

### 2. 位置可恢复

- `Position` 记录数据位置，用于断点恢复
- 不同数据库使用不同的位置字段
- 位置可序列化，持久化到目标数据库

### 3. Schema 演进

- `SchemaInfo` 记录数据结构版本
- 支持向后兼容的 Schema 变更
- 不兼容变更时暂停同步并告警

### 4. 事务边界

- `TransactionInfo` 记录事务信息
- 支持事务内事件的顺序保证
- 支持事务完整性校验

---

## 使用示例

### 创建事件

```go
event := &ChangeEvent{
    ID: "mysql:inventory:20260507120000.000:1",
    Source: SourceInfo{
        Connector: "mysql",
        Host:      "localhost",
        Port:      3306,
        Database:  "inventory",
    },
    Type: EventTypeInsert,
    Position: Position{
        BinlogFile:  "mysql-bin.000001",
        BinlogPos:   12345,
        CommitTime:  time.Now(),
        TxID:        "tx-001",
        SeqNo:       1,
        Total:       1,
    },
    Table: TableInfo{
        Database:         "inventory",
        Table:            "products",
        PrimaryKeyColumns: []string{"id"},
    },
    After: RowData{
        Fields: map[string]Field{
            "id":    {Name: "id", Value: 101, Type: "int"},
            "name":  {Name: "name", Value: "Widget", Type: "varchar"},
            "price": {Name: "price", Value: 9.99, Type: "decimal"},
        },
    },
    Timestamp: time.Now(),
}
```

### 处理事件

```go
func (p *Pipeline) processEvent(event *ChangeEvent) error {
    // 过滤
    if !p.filter.Match(event) {
        return nil
    }

    // 转换
    transformed, err := p.transformer.Transform(event)
    if err != nil {
        return err
    }

    // 路由
    route := p.router.Route(transformed)

    // 发送到 Sink
    return p.sink.Emit(transformed, route)
}
```

---

*文档版本：v1.0*
*创建时间：2026-05-07*
*更新时间：2026-05-07*
