AegisDB 是一个高性能、分布式的键值数据库系统，采用 Rust 语言实现。它提供了完整的分布式存储、事务支持和自动负载均衡功能。

## 目录

- [特性](#特性)
- [架构设计](#架构设计)
- [核心功能](#核心功能)
- [快速开始](#快速开始)
- [API 文档](#api-文档)
- [分布式设计](#分布式设计)
- [开发指南](#开发指南)

## 特性

### 存储引擎
- ✅ 基于 RocksDB 的高性能存储引擎
- ✅ 支持 Column Families (CF_DEFAULT, CF_WRITE, CF_LOCK)
- ✅ Write-Ahead Log (WAL) 支持，确保数据持久化
- ✅ 支持 Standalone 和分布式两种模式
- ✅ 批量写入优化

### RawKV API
- ✅ `RawGet` - 获取键值对
- ✅ `RawPut` - 写入键值对
- ✅ `RawDelete` - 删除键值对
- ✅ `RawScan` - 范围扫描

### 事务 API (MVCC)
- ✅ `KvGet` - 事务读取（支持版本控制）
- ✅ `KvScan` - 事务范围扫描
- ✅ `KvPrewrite` - 两阶段提交第一阶段（预写）
- ✅ `KvCommit` - 两阶段提交第二阶段（提交）
- ✅ `KvCheckTxnStatus` - 检查事务状态
- ✅ `KvBatchRollback` - 批量回滚
- ✅ `KvResolveLock` - 解决锁冲突

### 分布式特性
- ✅ Raft 共识算法实现
- ✅ Region 分片管理
- ✅ 自动 Region 分割和合并
- ✅ Leader 选举和故障转移
- ✅ 多副本数据复制
- ✅ 调度器自动负载均衡

### 工具
- ✅ 交互式 CLI 客户端
- ✅ gRPC 服务接口
- ✅ 完整的测试套件

## 架构设计

### 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                    Client Layer                         │
│  (CLI / gRPC Client / Application)                     │
└────────────────────┬──────────────────────────────────┘
                     │
┌────────────────────▼──────────────────────────────────┐
│                  Server Layer                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐│
│  │  RawKV API   │  │ Transaction  │  │  Multi-Level  ││
│  │              │  │     API     │  │     API       ││
│  └──────────────┘  └──────────────┘  └──────────────┘│
└────────────────────┬──────────────────────────────────┘
                     │
┌────────────────────▼──────────────────────────────────┐
│              Storage Abstraction Layer                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐│
│  │  Standalone  │  │   RaftStore  │  │   Reader     ││
│  │   Storage    │  │              │  │              ││
│  └──────────────┘  └──────────────┘  └──────────────┘│
└────────────────────┬──────────────────────────────────┘
                     │
┌────────────────────▼──────────────────────────────────┐
│              Engine Layer (RocksDB)                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐│
│  │  KV Engine   │  │ Raft Engine  │  │   WAL        ││
│  └──────────────┘  └──────────────┘  └──────────────┘│
└───────────────────────────────────────────────────────┘
```

### 数据模型

AegisDB 使用三个 Column Families 来组织数据：

1. **CF_DEFAULT**: 存储实际的数据值
   - Key: `encode_key(user_key, start_ts)`
   - Value: 用户数据

2. **CF_WRITE**: 存储事务的 Write 记录
   - Key: `encode_key(user_key, commit_ts)`
   - Value: `Write { start_ts, kind }`
   - 用于 MVCC 版本控制和事务可见性判断

3. **CF_LOCK**: 存储事务锁信息
   - Key: `user_key`
   - Value: `Lock { primary, ts, ttl, kind }`
   - 用于两阶段提交的锁管理

### MVCC 事务模型

AegisDB 实现了基于多版本并发控制 (MVCC) 的事务系统：

1. **时间戳分配**: 每个事务分配唯一的开始时间戳 (start_ts) 和提交时间戳 (commit_ts)

2. **读取流程**:
   - 检查 CF_LOCK 中是否有锁冲突
   - 在 CF_WRITE 中查找 `commit_ts <= start_ts` 的最新 Write
   - 根据 Write 类型从 CF_DEFAULT 读取值或返回空

3. **写入流程** (两阶段提交):
   - **Prewrite 阶段**: 
     - 检查锁冲突和写冲突
     - 在 CF_DEFAULT 写入数据
     - 在 CF_LOCK 写入锁记录
   - **Commit 阶段**:
     - 验证锁属于当前事务
     - 在 CF_WRITE 写入 Write 记录
     - 删除 CF_LOCK 中的锁

## 核心功能

### 1. 存储引擎

#### StandaloneStorage
单机模式存储引擎，直接使用 RocksDB：

```rust
let config = Config::new_default();
let storage = StandaloneStorage::new(&config)?;
storage.start().await?;

// 写入数据
let batch = vec![
    Modify::Put(Put {
        key: b"key1".to_vec(),
        value: b"value1".to_vec(),
        cf: "default".to_string(),
    })
];
storage.write(batch).await?;

// 读取数据
let reader = storage.reader().await?;
let value = reader.get_cf("default", b"key1").await?;
```

#### 特性
- 支持多个 Column Families
- 批量写入优化
- 快照隔离的读取器
- WAL 持久化保证

### 2. RawKV API

提供基本的键值操作，不涉及事务：

```rust
// RawGet
let req = RawGetRequest {
    context: Context::new(1),
    key: b"key1".to_vec(),
    cf: "default".to_string(),
};
let resp = RawKvServer::raw_get(&server, req).await?;

// RawPut
let req = RawPutRequest {
    context: Context::new(1),
    key: b"key1".to_vec(),
    value: b"value1".to_vec(),
    cf: "default".to_string(),
};
RawKvServer::raw_put(&server, req).await?;

// RawScan
let req = RawScanRequest {
    context: Context::new(1),
    start_key: b"key1".to_vec(),
    limit: 10,
    cf: "default".to_string(),
};
let resp = RawKvServer::raw_scan(&server, req).await?;
```

### 3. 事务 API

#### 基本事务操作

```rust
// Prewrite (第一阶段)
let req = PrewriteRequest {
    context: Context::new(1),
    mutations: vec![
        Mutation {
            op: Op::Put,
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        }
    ],
    primary_lock: b"primary".to_vec(),
    start_version: 1000,
    lock_ttl: 3000,
};
TransactionKvServer::kv_prewrite(&server, server.latches(), req).await?;

// Commit (第二阶段)
let req = CommitRequest {
    context: Context::new(1),
    start_version: 1000,
    keys: vec![b"key1".to_vec()],
    commit_version: 2000,
};
TransactionKvServer::kv_commit(&server, server.latches(), req).await?;

// Get (事务读取)
let req = GetRequest {
    context: Context::new(1),
    key: b"key1".to_vec(),
    version: 2000,
};
let resp = TransactionKvServer::kv_get(&server, req).await?;
```

#### 锁管理

- **Latches**: 内存中的锁存器，用于防止并发事务冲突
- **Lock TTL**: 锁的生存时间，防止死锁
- **Lock Resolution**: 自动检测和解决过期锁

### 4. Raft 共识

AegisDB 实现了完整的 Raft 共识算法：

#### 核心组件

- **RaftLog**: Raft 日志管理
- **Progress**: 每个 Peer 的复制进度跟踪
- **State Machine**: Leader/Follower/Candidate 状态转换

#### 特性

- Leader 选举
- 日志复制和一致性保证
- 心跳机制
- 日志压缩 (Log GC)
- 快照支持

### 5. Region 管理

Region 是数据分片的基本单位：

#### Region 结构

```rust
pub struct Region {
    pub id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub region_epoch: RegionEpoch,
    pub peers: Vec<Peer>,
}
```

#### Region 操作

- **Split**: 当 Region 大小超过阈值时自动分割
- **Merge**: 合并相邻的小 Region
- **ChangePeer**: 添加或删除 Peer
- **TransferLeader**: 转移 Leader 角色

### 6. 调度器系统

调度器负责集群的自动负载均衡：

#### 调度器类型

1. **BalanceRegionScheduler**: 平衡 Region 分布
   - 检测 Region 数量不平衡
   - 从负载高的 Store 迁移 Region 到负载低的 Store

2. **BalanceLeaderScheduler**: 平衡 Leader 分布
   - 确保每个 Store 的 Leader 数量均衡
   - 通过 TransferLeader 操作调整

3. **ScaleScheduler**: 集群扩缩容
   - 检测新加入的 Store
   - 自动分配 Region 到新 Store

#### Operator 系统

调度器生成 Operator，由 OperatorController 执行：

```rust
pub enum OpKind {
    AddPeer { peer: Peer, to_store: u64 },
    RemovePeer { peer: Peer },
    TransferLeader { peer: Peer },
}
```

## 快速开始

### 安装

```bash
# 克隆仓库
git clone <repository-url>
cd aegisdb

# 构建项目
cargo build --release
```

### 启动服务器

```bash
# Standalone 模式
cargo run --bin aegisdb -- run --addr 127.0.0.1:20160 --db-path /tmp/aegisdb

# 分布式模式（需要配置 Raft）
cargo run --bin aegisdb_server -- --store-addr 127.0.0.1:20160 \
    --scheduler-addr 127.0.0.1:2379 \
    --db-path /tmp/aegisdb
```

### 使用 CLI

```bash
# 启动 CLI 客户端
cargo run --bin aegisdb -- cli --addr 127.0.0.1:20160

# RawKV 命令
aegisdb> SET key1 value1
aegisdb> GET key1
aegisdb> SCAN "" 10
aegisdb> DEL key1

# 事务命令
aegisdb> KVPREWRITE primary_key 1000 3000 key1 value1 key2 value2
aegisdb> KVCOMMIT 1000 2000 key1 key2
aegisdb> KVGET key1 2000
aegisdb> KVSCAN "" 10 2000
```

## API 文档

### gRPC 服务

AegisDB 提供 gRPC 服务接口，定义在 `proto/tinykvpb.proto`：

```protobuf
service TinyKv {
    // RawKV commands
    rpc RawGet(kvrpcpb.RawGetRequest) returns (kvrpcpb.RawGetResponse);
    rpc RawPut(kvrpcpb.RawPutRequest) returns (kvrpcpb.RawPutResponse);
    rpc RawDelete(kvrpcpb.RawDeleteRequest) returns (kvrpcpb.RawDeleteResponse);
    rpc RawScan(kvrpcpb.RawScanRequest) returns (kvrpcpb.RawScanResponse);
    
    // Transactional commands
    rpc KvGet(kvrpcpb.GetRequest) returns (kvrpcpb.GetResponse);
    rpc KvScan(kvrpcpb.ScanRequest) returns (kvrpcpb.ScanResponse);
    rpc KvPrewrite(kvrpcpb.PrewriteRequest) returns (kvrpcpb.PrewriteResponse);
    rpc KvCommit(kvrpcpb.CommitRequest) returns (kvrpcpb.CommitResponse);
}
```

### 使用 gRPC 客户端

可以使用任何支持 gRPC 的客户端，例如使用 `grpcurl`:

```bash
# 安装 grpcurl
# Windows: choco install grpcurl
# Linux/Mac: brew install grpcurl

# 测试 RawGet
grpcurl -plaintext -d '{"key":"dGVzdF9rZXk=","cf":"default"}' \
  localhost:20160 tinykvpb.TinyKv/RawGet
```

## 分布式设计

### 数据分片 (Sharding)

AegisDB 使用 Region 作为数据分片的基本单位：

1. **键空间划分**: 整个键空间被划分为多个连续的 Region
2. **Region 范围**: 每个 Region 负责 `[start_key, end_key)` 范围的键
3. **自动分割**: 当 Region 大小超过 `region_max_size` 时自动分割
4. **负载均衡**: 调度器确保 Region 在 Store 之间均匀分布

### 复制和一致性

1. **Raft 复制**: 每个 Region 的多个副本通过 Raft 协议保持一致性
2. **多数派提交**: 写入需要多数派确认才能提交
3. **Leader 读写**: 所有读写请求都发送到 Leader
4. **Follower 只读**: Follower 可以提供只读快照查询（可选）

### 故障处理

1. **Leader 故障**: 自动选举新的 Leader
2. **Follower 故障**: 不影响读写，但影响可用性
3. **网络分区**: Raft 保证分区后多数派一侧可用
4. **数据恢复**: 通过 Raft 日志重放恢复数据

### 调度策略

调度器定期检查集群状态并生成调度操作：

1. **Region 均衡**: 确保每个 Store 的 Region 数量相近
2. **Leader 均衡**: 确保每个 Store 的 Leader 数量相近
3. **容量均衡**: 考虑 Store 的存储容量和负载
4. **故障恢复**: 自动检测故障并迁移数据

### 扩展性

1. **水平扩展**: 通过添加新 Store 扩展集群
2. **自动重分布**: 新 Store 加入后自动分配 Region
3. **动态调整**: 支持在线添加/删除节点
4. **Region 分割**: 大 Region 自动分割以分散负载

## 开发指南

### 项目结构

```
aegisdb/
├── src/
│   ├── bin/              # 可执行文件入口
│   │   ├── aegisdb.rs    # CLI 工具
│   │   ├── aegisdb_server.rs  # 服务器
│   │   └── aegisdb_cli.rs     # CLI 客户端
│   ├── config.rs         # 配置管理
│   ├── storage/          # 存储抽象层
│   │   ├── mod.rs
│   │   ├── standalone_storage.rs
│   │   └── reader.rs
│   ├── engine_util/      # 存储引擎工具
│   │   ├── engines.rs
│   │   ├── write_batch.rs
│   │   └── iterator.rs
│   ├── server/           # 服务器层
│   │   ├── raw_api.rs    # RawKV API
│   │   ├── transaction_api.rs  # 事务 API
│   │   └── grpc.rs       # gRPC 服务
│   ├── transaction/      # 事务实现
│   │   ├── mvcc/         # MVCC 实现
│   │   └── latches/      # 锁存器
│   ├── raft/             # Raft 共识
│   ├── raftstore/        # Raft 存储层
│   └── scheduler/        # 调度器
├── proto/                # Protocol Buffers 定义
└── tests/                # 测试文件
```

### 运行测试

```bash
# 运行所有测试
cargo test

# 运行特定测试
cargo test test_raw_kv

# 运行集成测试
cargo test --test integration_test
```

## 致谢
项目的部分代码参考以下项目，感谢开发者的贡献
- [TiKV](https://github.com/tikv/tikv)
- [TinyKV](https://github.com/tidb-incubator/tinykv)
- [RocksDB](https://github.com/facebook/rocksdb) 
- [tonic](https://github.com/hyperium/tonic) 