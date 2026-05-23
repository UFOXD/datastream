# 表级独立生命周期设计（全量/增量解耦）

> 创建时间：2026-05-23
> 版本：v1.0

---

## 1. 背景与问题

### 1.1 现状

当前 DataStream 的全量/增量流程是：**所有表全量完成 → 开始增量**。这是一个单一生命周期模型。

### 1.2 问题

10T+ 数据量场景下：

1. **大表全量耗时过长**（72 小时+），期间源端连接断开/网络抖动导致全量失败
2. **全量失败只能从头重跑**，无表级隔离——一张大表失败拖累所有表
3. **全量期间 binlog/redo log 被源库清理**，增量起点丢失
4. **任务永远无法进入增量**：大表反复失败 → 重跑 → 又失败 → 死循环

### 1.3 设计目标

- 每张表独立推进全量→增量，完成一张增量一张
- 大表全量失败只重跑该表，不影响已进入增量的表
- binlog/oplog 不丢失，即使全量耗时超过源库日志保留时间
- 大表支持分块并发全量 + 可选 S3 中转

---

## 2. 核心模型

### 2.1 表级状态机

每张表独立维护 6 个状态：

```
pending ──────▶ snapshotting ──────▶ catching_up ──────▶ streaming
   ▲                │                    │                  │
   │                ▼                    ▼                  ▼
   │             error ◀─────────────  error ◀──────────  error
   │                │                    │                  │
   │                ▼                    ▼                  ▼
   └──── (重试) ────┘                 paused ◀──────────  paused
```

状态说明：

| 状态 | 含义 |
|------|------|
| `pending` | 等待开始全量 |
| `snapshotting` | 全量进行中（DROP+CREATE+INSERT） |
| `catching_up` | 全量完成，正在回放缓冲的增量事件 |
| `streaming` | 实时增量同步中 |
| `error` | 出错，等待重试或人工干预 |
| `paused` | 手动暂停 |

### 2.2 状态转换规则

| 转换 | 触发条件 | 动作 |
|------|---------|------|
| `pending → snapshotting` | 调度器分配 | 记录 snapshotPosition，目标端 DROP+CREATE TABLE |
| `snapshotting → catching_up` | 全量完成 | 检查缓冲完整性，开始回放 |
| `snapshotting → error` | 全量失败 / 缓冲总量达到 hard limit | 记录错误信息 |
| `catching_up → streaming` | 回放追平实时位点 | 原子切换协议（见 §6.5），删除缓冲文件 |
| `catching_up → error` | 回放失败且缓冲和源库日志都不可用 | 记录错误 |
| `catching_up → catching_up` | 回放失败但缓冲完好或源库日志可重建 | 在 catching_up 内重试，不回 pending |
| `error → pending` | 重试触发（仅当无法在原阶段恢复时） | **先记录新 snapshotPosition，再切 pending，最后清缓冲** |
| `streaming → error` | 增量消费失败 | 记录错误 |
| `catching_up/streaming → paused` | 手动操作 | 暂停该表处理 |
| `paused` → 原状态 | 手动恢复 | 继续处理 |

**约束：**
- `pending` 和 `snapshotting` 状态不允许 pause（取消进行中的 SELECT 太复杂）
- `error → pending` 的操作顺序必须是：先记 snapshotPosition → 再切状态 → 最后清缓冲（防止事件丢失窗口，见 §6.6）
- 跨表事务原子性在 snapshotting/catching_up 阶段不保证（同一 GTID 的事件可能分发到不同目的地），进入 streaming 后恢复原子性

### 2.3 每张表持久化的状态

```go
type TableLifecycle struct {
    TableID           TableID
    State             TableState
    SnapshotPosition  *event.Position   // 全量开始时的增量位点
    StreamPosition    *event.Position   // 当前增量消费位点
    CatchingUpProgress CatchingUpProgress // 回放进度
    RetryCount        int
    MaxRetries        int
    LastError         string
    LastStateChange   time.Time
}

// 位点标识复用 event.Position（扩展 GTID 和 ResumeToken 字段）
// 不引入新的 ProgressPosition 类型，避免平行类型转换
// 需要给 event.Position 新增:
//   GTID        string `json:"gtid,omitempty"`        // MySQL GTID Set
//   ResumeToken []byte `json:"resumeToken,omitempty"` // MongoDB Change Stream resume token

// CatchingUpProgress 回放进度（精确到事件级别）
type CatchingUpProgress struct {
    CurrentGTID  string  // 当前正在回放的 GTID/事务标识
    EventSeq     int64   // 该事务内的事件序号
    FileOffset   int64   // 本地缓冲文件的字节偏移（性能优化，非恢复依据）
    UpsertUntil  time.Time // UPSERT 模式截止时间
}
```

---

## 3. 增量事件缓冲

### 3.1 架构

全量期间，增量事件（binlog/oplog/redo）同步消费并缓冲到本地或 S3。

```
┌──────────┐     ┌──────────────────────────────────────┐
│ 源库      │────▶│ BinlogConsumer (全程运行)             │
│ binlog流  │     │                                      │
└──────────┘     │  按表分发:                            │
                 │   表在 snapshotting → BinlogCache     │
                 │   表在 catching_up  → Pipeline/Sink   │
                 │   表在 streaming    → Pipeline/Sink   │
                 │   表在 pending/error → 丢弃           │
                 └──────────────────────────────────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │ BinlogCacheBackend│
                 │  ├── LocalBackend │  (默认, Badger 全新实现)
                 │  └── S3Backend    │  (可选, 大数据量)
                 └──────────────────┘
```

### 3.2 缓冲存储格式

**按表分文件，length-prefixed Protobuf 格式：**

```
本地路径: {data_dir}/binlog_cache/{task_id}/{db}.{table}.binlog
S3 路径:  s3://{bucket}/{prefix}/binlog_cache/{task_id}/{db}.{table}/seg_{NNN}.binlog

文件内部格式:
  [4 bytes: payload length (big-endian)]
  [N bytes: Protobuf 编码的 CacheEvent]
  [4 bytes: payload length]
  [N bytes: Protobuf 编码的 CacheEvent]
  ...
```

**CacheEvent Protobuf 定义：**

```protobuf
message CacheEvent {
    string gtid = 1;           // GTID 或事务标识
    int64  event_seq = 2;      // 该事务内的事件序号（从 0 开始）
    bool   is_begin = 3;       // 事务开始标记
    bool   is_commit = 4;      // 事务提交标记
    bytes  payload = 5;        // 序列化的 ChangeEvent
    int64  timestamp_ms = 6;   // 事件时间戳
}
```

### 3.3 BinlogCacheBackend 接口

```go
type BinlogCacheBackend interface {
    // Write 追加写入一个事件到表的缓冲
    Write(ctx context.Context, tableID string, event *CacheEvent) error
    
    // Read 顺序读取表的缓冲事件（从指定位置开始）
    Read(ctx context.Context, tableID string, fromGTID string, fromEventSeq int64) (<-chan *CacheEvent, error)
    
    // Delete 删除表的所有缓冲数据
    Delete(ctx context.Context, tableID string) error
    
    // Size 返回表的缓冲大小（字节）
    Size(ctx context.Context, tableID string) (int64, error)
    
    // Exists 检查表的缓冲是否存在
    Exists(ctx context.Context, tableID string) (bool, error)
}
```

### 3.4 CLI 调试工具

```bash
# 将 binlog 缓冲文件转为 JSON 可读格式
datastream-ctl binlog decode --file /data/binlog_cache/task-1/db.table.binlog --format json > output.json

# 查看缓冲文件统计信息
datastream-ctl binlog stat --file /data/binlog_cache/task-1/db.table.binlog
# Output: events=1234567, gtids=456, size=2.3GB, time_range=2026-05-23T10:00:00~2026-05-23T22:00:00
```

---

## 4. 全量调度器（SnapshotScheduler）

### 4.1 职责

编排所有表的全量/增量生命周期，核心组件。

```
SnapshotScheduler
  │
  ├── 路径决策 (SnapshotPathResolver)
  │     ├── 用户手动指定 → 按配置走
  │     ├── 未指定 + 行数 ≥ 阈值 → S3 中转
  │     └── 未指定 + 行数 < 阈值 → 直连
  │
  ├── 全量调度
  │     ├── 表间并发: 可配置并发数 (MaxTableThreads)
  │     ├── 表内并发: 大表 hash 取模分片 (MaxChunkThreads)
  │     └── 智能排序: 小表优先完成，尽早进入增量
  │
  ├── BinlogConsumer (全程运行)
  │     ├── 全局消费增量流
  │     ├── 按表分发到缓冲或 Sink
  │     └── 维护全局最小位点 (GlobalMinPosition)
  │
  ├── catching_up 协调
  │     ├── 缓冲完整性检查 + 恢复
  │     ├── 回放缓冲事件
  │     ├── 写入模式切换 (UPSERT → 正常 DML)
  │     └── 追平检测 → 切换到 streaming
  │
  └── 失败处理
        ├── 可配置重试次数和间隔
        ├── 重试时清除缓冲并重新记录位点
        └── 超过重试次数 → error 等待人工干预
```

### 4.2 全局最小位点（GlobalMinPosition）

**绝对不能丢失的信息：所有表中已同步完成的最小增量位点。**

```
表 A: streaming, 位点 GTID:1000
表 B: catching_up, 位点 GTID:500
表 C: snapshotting, snapshotPosition = GTID:300
表 D: streaming, 位点 GTID:800

GlobalMinPosition = min(300, 500, 800, 1000) = GTID:300
```

用途：
- 缓冲文件丢失时，只要源库日志覆盖 GlobalMinPosition，就能重建任何表的缓冲
- 这是整个系统的恢复安全点

持久化要求：
- 存储在 offset storage（etcd/本地文件），不依赖缓冲文件
- 每次任何表的位点更新时，重新计算并持久化

---

## 5. 全量执行

### 5.1 两条路径

| 路径 | 适用场景 | 流程 |
|------|---------|------|
| 直连 | 小表 / 网络稳定 | Source → hash 分片并发 → Sink (DROP+CREATE+INSERT) |
| S3 中转 | 大表 / 网络不稳定 | Source → S3 → Sink (断点续传) |

### 5.2 直连路径

```
1. DROP TABLE IF EXISTS target.table
2. CREATE TABLE target.table (复制源表结构)
3. 并发 INSERT:
   Worker 0: SELECT * FROM source.table WHERE MOD(CRC32(CONCAT(pk_cols)), N) = 0
   Worker 1: SELECT * FROM source.table WHERE MOD(CRC32(CONCAT(pk_cols)), N) = 1
   ...
   Worker N-1: SELECT * FROM source.table WHERE MOD(CRC32(CONCAT(pk_cols)), N) = N-1
4. 所有 Worker 完成 → 表进入 catching_up
```

Hash 取模分片特点：
- 不依赖主键类型（整数、UUID、复合主键均可）
- 分片均匀
- 各分片完全独立
- 不需要查询 min/max key

数据库 Hash 函数映射：

| 数据库 | Hash 函数 |
|--------|----------|
| MySQL/MariaDB | `CRC32(CONCAT(pk_cols))` |
| PostgreSQL | `hashtext(pk_cols::text)` |
| Oracle | `ORA_HASH(pk_cols)` |
| SQL Server | `CHECKSUM(pk_cols)` |
| MongoDB | `$mod` aggregation |

### 5.3 S3 中转路径

```
阶段 1 (导出): Source → hash 分片并发 → S3
  s3://{bucket}/{prefix}/snapshot/{task_id}/{db}.{table}/
    ├── _schema.json          # 表结构
    ├── chunk_000.parquet     # 分片 0
    ├── chunk_001.parquet     # 分片 1
    └── ...
    └── _complete             # 导出完成标记

阶段 2 (导入): S3 → Sink (断点续传)
  - 目标端 DROP+CREATE TABLE
  - 逐文件导入，每完成一个文件记录进度
  - 失败重启从上次未完成的文件继续

阶段 3 (清理): 导入完成后删除 S3 临时文件（可配置保留）
```

### 5.4 路径决策

```go
type SnapshotPathResolver struct {
    config *SnapshotS3Config
}

func (r *SnapshotPathResolver) Resolve(table *event.TableInfo, rowCount int64) SnapshotPath {
    // 1. 用户手动指定
    for _, rule := range r.config.Tables {
        if matchPattern(rule.Pattern, table) {
            return rule.Mode // "s3" 或 "direct"
        }
    }
    
    // 2. 自动判断
    if r.config.Enabled && rowCount >= r.config.ThresholdRows {
        return SnapshotPathS3
    }
    
    return SnapshotPathDirect
}
```

---

## 6. Catching-Up 回放

### 6.1 流程

```
全量完成 → 进入 catching_up:
  1. 缓冲完整性检查 (validateCache)
  2. 如缓冲丢失但源库日志仍在 → 重建缓冲
  3. 从缓冲中按序回放该表的增量事件
  4. 写入模式: 前 1 分钟 UPSERT，之后切正常 DML
  5. 追平实时位点 → 切换到 streaming
```

### 6.2 写入模式切换

```
catching_up 开始:
  upsertUntil = time.Now().Add(config.CatchingUpUpsertDuration)  // 默认 1 分钟

回放每条事件:
  if time.Now().Before(upsertUntil):
      使用 UPSERT/REPLACE 模式 (幂等，覆盖断点续传的重叠区)
  else:
      使用正常 INSERT/UPDATE/DELETE 模式 (高效，避免锁竞争)
```

配置项：`catching_up_upsert_duration = "1m"`

### 6.3 断点续传

正常回放时持久化的进度：

```go
type CatchingUpProgress struct {
    CurrentGTID  string  // 当前 GTID/事务标识
    EventSeq     int64   // 该事务内的事件序号
    FileOffset   int64   // 缓冲文件字节偏移（本地快速 seek 用）
    UpsertUntil  time.Time
}
```

**关键设计决策：**
- `CurrentGTID + EventSeq` 是恢复依据——和源数据绑定，不依赖缓冲文件
- `FileOffset` 是性能优化——本地回放时快速 seek，非恢复用
- 进度持久化到 offset storage，不存在缓冲文件中

**断点恢复流程：**

```
重启后:
  1. 读取 CatchingUpProgress (从 offset storage)
  2. 缓冲文件存在?
     ├── 存在 → seek to FileOffset → 继续回放
     └── 不存在 → 从源库重建缓冲 → 跳过前 EventSeq 个事件 → 继续回放
  3. 设置 upsertUntil = time.Now().Add(1m) (重新进入 UPSERT 窗口)
```

### 6.4 缓冲恢复

```
缓冲完整性检查 (catching_up 开始前):
  1. 缓冲文件存在?
     ├── 存在且完整 → 正常回放
     └── 不存在/损坏 → 进入恢复流程
  
  2. 恢复流程:
     a. 读取该表的 SnapshotPosition
     b. 检查源库日志是否覆盖 SnapshotPosition:
        ├── MySQL: SHOW BINARY LOGS 最早 ≤ SnapshotPosition
        ├── Oracle: SELECT MIN(FIRST_CHANGE#) FROM V$ARCHIVED_LOG
        ├── PostgreSQL: pg_available_wal_lsn()
        └── MongoDB: oplog 最早 ts ≤ SnapshotPosition.ClusterTime
     c. 覆盖 → 从源库重新消费 → 重建缓冲文件 → 正常 catching_up
     d. 不覆盖 → 无法恢复 → 表回到 error (需重做全量)
```

### 6.5 追平检测算法（catching_up → streaming 切换）

缓冲是临时的，只在 snapshotting 阶段写入。追平后缓冲文件删除，streaming 阶段零缓冲。

```
事件路由生命周期:
  pending:       事件丢弃
  snapshotting:  事件 → 写入缓冲文件
  catching_up:   缓冲回放 → Sink（同时 Consumer 继续写缓冲尾部）
  追平:          Consumer 切换路由 → 事件直接发 Sink，缓冲文件删除
  streaming:     事件 → 直接发 Sink（不经过缓冲）
```

**追平检测算法：**

```
1. Replayer 从缓冲文件顺序读取并应用事件
2. 当 Replayer 读到文件尾部（没有更多事件），等待 100ms
3. 再次检查是否有新事件写入
4. 如果连续 3 次检查都没有新事件 → 认为追平
5. 原子切换协议:
   a. Replayer 通知 BinlogConsumer: "准备切换 table_X"
   b. BinlogConsumer 获取 table_X 的路由锁
   c. BinlogConsumer flush 最后一批事件到缓冲文件（持锁期间该表新到事件排队等待）
   d. BinlogConsumer 将 table_X 的路由从"写缓冲"切换为"直接发 Sink"
   e. BinlogConsumer 释放路由锁（排队的事件按新路由直发 Sink）
   f. Replayer 消费完缓冲文件最后的事件（含 flush 的部分）
   g. 删除缓冲文件，表状态变为 streaming
```

**无 gap 保证：** 步骤 b-e 持锁，flush 和路由切换之间不会有事件被漏处理。Replayer 消费完缓冲全部内容后，Consumer 已经在直发。

**无重复保证：** Consumer 持锁切换，Replayer 消费到 flush 点后退出。没有事件同时写入缓冲和直发 Sink。

### 6.6 catching_up 阶段错误处理

catching_up 出错不一定回 pending，先尝试在当前阶段恢复：

```
catching_up 出错:
  ├── 缓冲文件完好 + 目标端临时不可用 → 在 catching_up 内重试
  ├── 缓冲文件损坏 + 源库日志还在 → 重建缓冲，继续 catching_up
  └── 缓冲文件损坏 + 源库日志也没了 → 回 pending 重做全量
```

### 6.7 大事务处理

大事务（单个 GTID 包含百万行变更）的处理：

- **缓冲阶段**：逐条写入文件，事务边界用 `is_begin`/`is_commit` 标记
- **回放阶段**：拆分为小批次提交（每 `batch_size` 行一次 COMMIT）
- **进度记录**：`GTID + EventSeq` 精确定位到事务内的具体位置
- **恢复时**：从源库重新拉取该 GTID 完整事务 → 跳过前 `EventSeq` 个事件 → 从断点继续

---

## 7. 数据库适配

### 7.1 MySQL/MariaDB

| 维度 | 方案 |
|------|------|
| 增量流 | binlog（全局一条流） |
| 位点标识 | GTID Set（推荐）/ binlog file+pos |
| 全量一致性点 | `SHOW MASTER STATUS` 获取 GTID 或 pos |
| 分片 Hash | `CRC32(CONCAT(pk_cols))` |
| 主从切换 | GTID 模式透明支持；file+pos 模式是已知风险 |
| 前置要求 | 推荐启用 GTID（`gtid_mode=ON`） |
| 非事务表 | MyISAM DML 也产生 binlog event 并分配 GTID，不影响 |

### 7.2 Oracle

| 维度 | 方案 |
|------|------|
| 增量流 | LogMiner (`V$LOGMNR_CONTENTS`) |
| 位点标识 | SCN (System Change Number，全局唯一递增) |
| 全量一致性点 | `SELECT CURRENT_SCN FROM V$DATABASE` |
| 分片 Hash | `ORA_HASH(pk_cols)` |
| 主从切换 | Data Guard 切换后 SCN 连续，透明支持 |
| 前置要求 | 补充日志 `ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` |

### 7.3 PostgreSQL

| 维度 | 方案 |
|------|------|
| 增量流 | Logical Replication (WAL) |
| 位点标识 | LSN (Log Sequence Number) |
| 全量一致性点 | `pg_current_wal_lsn()` |
| 分片 Hash | `hashtext(pk_cols::text)` |
| 主从切换 | LSN 在 timeline 切换后可能不连续——已知限制 |
| 前置要求 | `wal_level = logical` |

### 7.4 SQL Server

| 维度 | 方案 |
|------|------|
| 增量流 | CDC (`cdc.fn_cdc_get_all_changes_*`) |
| 位点标识 | LSN |
| 全量一致性点 | `sys.fn_cdc_get_max_lsn()` |
| 分片 Hash | `CHECKSUM(pk_cols)` |
| 主从切换 | Always On AG 切换后 LSN 连续 |
| 前置要求 | CDC 已启用 |

### 7.5 MongoDB

| 维度 | 方案 |
|------|------|
| 增量流 | Change Stream（基于 oplog） |
| 位点标识 | resume token + cluster time |
| 全量一致性点 | 当前 cluster time |
| 分片 Hash | `$mod` aggregation on `_id` |
| 主从切换 | Replica Set 切换后 Change Stream 透明恢复 |
| oplog 窗口 | 等价于 binlog 保留时间，必须缓冲（和关系型一样） |
| 前置要求 | 必须是 Replica Set 或 Sharded Cluster（Standalone 不支持） |

**关键：MongoDB 也必须缓冲增量事件到本地/S3，和关系型数据库统一架构。** 原因：大表全量时间可能超过 oplog 窗口，Change Stream 无法从过期的 resume token 恢复。

---

## 8. 配置

```toml
[snapshot]
mode = "initial"                      # never, initial, always, when_needed

[snapshot.scheduler]
max-table-threads = 4                 # 表间并发数
max-chunk-threads = 4                 # 单表内分片并发数
chunk-threshold = 1000000             # 超过此行数启用分片
smart-order = true                    # 小表优先

[snapshot.retry]
max-retries = 3                       # 最大重试次数
retry-interval = "5m"                 # 重试间隔

[snapshot.write]
target-mode = "drop-create-insert"    # 全量写入模式
batch-size = 1000                     # 每批写入行数

# S3 中转
[snapshot.s3]
enabled = false
bucket = "my-bucket"
prefix = "datastream"
region = "us-east-1"
threshold-rows = 100000000            # 1 亿行以上自动走 S3
format = "parquet"

# 按表覆盖路径
[[snapshot.s3.tables]]
pattern = "db1.huge_table*"
mode = "s3"

[[snapshot.s3.tables]]
pattern = "db1.small_*"
mode = "direct"

# 增量缓冲
[snapshot.binlog-cache]
backend = "local"                     # local 或 s3
local-dir = "/data/datastream/binlog_cache"
s3-bucket = ""
s3-prefix = ""

# 总缓冲上限（所有表共享），支持两种格式:
#   固定大小: "50GB"
#   目录所在磁盘容量百分比: "80%" (扩容磁盘后自动生效，无需改配置)
# 百分比模式下每次检查时实时计算磁盘总量
# 支持通过 API 动态调整，不需要重启
max-cache-size = "80%"

# S3 后端不限制大小（S3 容量无限）
#
# 阶梯策略:
#   80% of max-cache-size: 报警 (soft limit)
#   90% of max-cache-size: 暂停新表进入 snapshotting (pending 不再调度)
#   100% of max-cache-size: 所有 snapshotting 的表进入 error
#   已在 catching_up 的表不受影响（只读不写，缓冲在缩小）
#   已在 streaming 的表不受影响（不走缓冲）

# Catching-up
[snapshot.catching-up]
upsert-duration = "1m"                # 重启后 UPSERT 模式持续时间
batch-size = 1000                     # 回放批次大小
```

---

## 9. 已知限制与约束

### 9.1 前置要求

| 数据库 | 要求 |
|--------|------|
| MySQL | 推荐 GTID 模式；binlog 保留时间建议 ≥ 预估全量时间 |
| Oracle | 补充日志 `(ALL) COLUMNS`；archived log 保留时间充足 |
| PostgreSQL | `wal_level = logical`；WAL 保留时间充足 |
| SQL Server | CDC 已启用 |
| MongoDB | Replica Set 或 Sharded Cluster；oplog 大小建议 ≥ 50GB |

### 9.2 已知限制

| 限制 | 说明 | 影响 |
|------|------|------|
| 跨表事务原子性 | snapshotting/catching_up 阶段，同一 GTID 的跨表事务可能被拆到不同目的地（一张表在缓冲，一张在 Sink）。进入 streaming 后恢复原子性。 | 最终一致性保证，过程中目标库可见"半事务" |
| MySQL file+pos 主从切换 | 无 GTID 时主从切换后位点可能失效 | 建议启用 GTID |
| PostgreSQL timeline 分叉 | Failover 后 LSN 可能不连续 | 后续迭代解决 |
| 非 Replica Set MongoDB | Standalone 不支持 Change Stream | 不支持此功能 |
| 目标库临时不一致 | catching_up 阶段目标表数据不完整 | 业务可见性窗口 |
| GlobalMinPosition 被大表钉住 | 最慢大表的 snapshotPosition 决定了 GlobalMinPosition，所有缓冲的恢复依赖源库日志覆盖该位点 | smart-order 小表优先 + 定期检查源库日志覆盖范围并报警 |
| 缓冲磁盘空间 | 全量期间缓冲可能达到上限 | 阶梯策略：80% 报警 → 90% 暂停调度 → 100% error；或溢出到 S3 |
| pause 限制 | pending 和 snapshotting 状态不允许 pause | 如需停止全量，使用 restart-table 回 pending |

### 9.3 恢复能力矩阵

| 故障场景 | 恢复方式 |
|---------|---------|
| DataStream 节点重启 | 从 offset storage 读取所有表状态，继续原流程 |
| 缓冲文件丢失 + 源库日志还在 | 从 GlobalMinPosition 重建缓冲 |
| 缓冲文件丢失 + 源库日志已清理 | 受影响的表回到 pending 重做全量 |
| 源端连接断开 | 自动重连，binlog consumer 从断点继续 |
| 目标端连接断开 | 暂停写入，重连后从断点继续（UPSERT 窗口覆盖） |
| 节点迁移 | 新节点从共享 offset storage 恢复，重建缓冲 |

---

## 10. 与现有组件的关系

| 现有组件 | 变化 |
|---------|------|
| `SnapshotCoordinator` | 降级为纯并发执行器，不再负责生命周期编排 |
| `PersistentBuffer` (Badger) | 不复用代码。LocalBackend 基于同一技术（Badger）全新实现，API 完全不同 |
| `BinlogSyncer` / `CDCReader` / `LogMinerReader` | **不修改**。BinlogConsumer 从 `source.Events()` channel 读取事件做二次分发，不侵入 Syncer 内部 |
| `Pipeline.processEvent` | 不变，streaming 阶段的事件处理路径不变 |
| `TableManager` / `TableSyncState` | `TableLifecycle` 替换 `TableSyncState`，状态名统一为 6 状态（pending/snapshotting/catching_up/streaming/error/paused） |
| `source.Connector` 接口 | 可能需要扩展 `SnapshotTable()` / `StreamTable()` 方法 |
| `event.Position` | 扩展 GTID 和 ResumeToken 字段；`Compare()` 增强为基于 CommitTime 跨数据库比较 |

**BinlogConsumer 与 BinlogSyncer 的集成方式：**

```
BinlogSyncer (现有，不修改)
  └── events chan ← 所有表的 binlog 事件（连接管理/解析/过滤由 Syncer 负责）

BinlogConsumer (新增，从 events chan 消费)
  └── 按表的 TableLifecycle.State 路由:
      ├── snapshotting → BinlogCacheBackend.Write()
      ├── catching_up/streaming → Pipeline/Sink
      └── pending/error/paused → 丢弃
```

BinlogConsumer 插在 Pipeline 和 Source 之间，是一个纯路由层。

### 新增组件

| 组件 | 职责 |
|------|------|
| `SnapshotScheduler` | 核心编排器，管理所有表的生命周期 |
| `BinlogConsumer` | 全程消费增量流，按表分发 |
| `BinlogCacheBackend` | 缓冲接口 (Local / S3) |
| `SnapshotPathResolver` | 路径决策 (直连 / S3 中转) |
| `CatchingUpReplayer` | catching_up 回放器 |
| `TableLifecycleStore` | 表状态持久化 |

---

## 11. 监控指标

| 指标 | 类型 | 说明 |
|------|------|------|
| `datastream_table_state` | Gauge | 每张表当前状态 |
| `datastream_snapshot_progress` | Gauge | 全量进度 (0-100%) |
| `datastream_catching_up_lag_events` | Gauge | catching_up 剩余事件数 |
| `datastream_binlog_cache_size_bytes` | Gauge | 每张表的缓冲大小 |
| `datastream_global_min_position_lag_seconds` | Gauge | 全局最小位点滞后 |
| `datastream_snapshot_retries_total` | Counter | 全量重试次数 |

---

## 12. 运维 API

表级独立生命周期需要表级别的可观测性和操控能力。

### 12.1 获取任务详情（含表级状态）

```
GET /api/v1/tasks/{id}/detail
```

返回任务信息 + 所有表的生命周期状态：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "id": "task-1",
    "name": "mysql-to-mysql",
    "status": "running",
    "globalMinPosition": {"gtid": "uuid:300"},
    "tables": [
      {
        "tableId": "db1.users",
        "state": "streaming",
        "snapshotPosition": {"gtid": "uuid:100"},
        "streamPosition": {"gtid": "uuid:1000"},
        "snapshotProgress": 100.0,
        "lastStateChange": "2026-05-23T10:00:00Z"
      },
      {
        "tableId": "db1.orders",
        "state": "catching_up",
        "snapshotPosition": {"gtid": "uuid:200"},
        "streamPosition": {"gtid": "uuid:500"},
        "catchingUpLag": 3500,
        "snapshotProgress": 100.0,
        "lastStateChange": "2026-05-23T11:30:00Z"
      },
      {
        "tableId": "db1.huge_table",
        "state": "snapshotting",
        "snapshotPosition": {"gtid": "uuid:300"},
        "snapshotProgress": 45.2,
        "snapshotPath": "s3",
        "lastStateChange": "2026-05-23T08:00:00Z"
      }
    ],
    "summary": {
      "total": 15,
      "pending": 2,
      "snapshotting": 1,
      "catchingUp": 1,
      "streaming": 10,
      "error": 1,
      "paused": 0
    }
  }
}
```

### 12.2 获取任务中处于 error 的表

```
GET /api/v1/tasks/{id}/tables/errors
```

返回所有处于 error 状态的表及其错误详情：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "tables": [
      {
        "tableId": "db1.huge_table",
        "state": "error",
        "errorMessage": "source connection timeout after 30s",
        "errorTime": "2026-05-23T12:45:00Z",
        "retryCount": 2,
        "maxRetries": 3,
        "lastSnapshotPosition": {"gtid": "uuid:300"},
        "snapshotProgress": 67.3,
        "cacheSize": "12.5GB",
        "suggestion": "check source network connectivity; next auto-retry in 5m"
      }
    ],
    "count": 1
  }
}
```

### 12.3 重新运行指定表（从全量开始）

```
POST /api/v1/tasks/{id}/tables/restart
```

请求体：

```json
{
  "tables": ["db1.huge_table", "db1.another_table"],
  "schemas": ["db2"],
  "force": false
}
```

`tables` 和 `schemas` 可同时使用，取并集。`schemas` 指定后，该 schema 下所有表都会被重跑。

展开使用当前任务正在同步的表列表（即 SnapshotScheduler 管理的 TableLifecycle 集合），不查询源库 catalog。如需添加新表，应先通过 AddTables API 注册。

行为：
- 如果指定了 `schemas`，从当前任务的 TableLifecycle 列表中筛选属于该 schema 的表，合并到 `tables` 列表
- 将指定表的状态重置为 `pending`
- 清除该表的 binlog 缓冲
- 重新记录 snapshotPosition
- 由调度器重新安排全量执行
- `force=true` 时即使表在 streaming 状态也强制重跑

响应：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "restarted": ["db2.users", "db2.orders", "db2.products", "db1.huge_table", "db1.another_table"],
    "skipped": [],
    "expandedFromSchemas": {"db2": ["db2.users", "db2.orders", "db2.products"]},
    "newSnapshotPosition": {"gtid": "uuid:1200"}
  }
}
```

错误场景：
- 表不存在 → 400 + 错误列表
- 表在 streaming 且 `force=false` → 409 Conflict
- 任务未运行 → 503

### 12.4 其他表级操控端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `GET /api/v1/tasks/{id}/tables` | GET | 列出所有表及其状态（简要版） |
| `GET /api/v1/tasks/{id}/tables/{table}/state` | GET | 获取单表详细状态 |
| `POST /api/v1/tasks/{id}/tables/{table}/pause` | POST | 暂停单表 |
| `POST /api/v1/tasks/{id}/tables/{table}/resume` | POST | 恢复单表 |
| `POST /api/v1/tasks/{id}/tables/{table}/skip-error` | POST | 跳过当前错误继续（危险操作） |
| `POST /api/v1/tasks/{id}/tables/{table}/retry` | POST | 立即重试（不等自动重试间隔） |

### 12.5 CLI 命令

```bash
# 查看任务详情（含表级状态）
datastream-ctl task detail task-1

# 查看 error 表
datastream-ctl task errors task-1

# 重跑指定表
datastream-ctl task restart-table task-1 db1.huge_table db1.another_table

# 重跑整个 schema 下的所有表
datastream-ctl task restart-table task-1 --schema db2

# 混合：指定表 + 指定 schema
datastream-ctl task restart-table task-1 db1.huge_table --schema db2

# 强制重跑（即使在 streaming）
datastream-ctl task restart-table task-1 db1.huge_table --force
datastream-ctl task restart-table task-1 --schema db2 --force

# 暂停/恢复单表
datastream-ctl task pause-table task-1 db1.huge_table
datastream-ctl task resume-table task-1 db1.huge_table

# 跳过错误
datastream-ctl task skip-error task-1 db1.huge_table

# 立即重试
datastream-ctl task retry-table task-1 db1.huge_table
```

---

*返回 [设计文档总览](./Design.md)*
