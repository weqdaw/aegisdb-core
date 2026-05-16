AegisDB is a high-performance distributed key-value database system implemented in Rust.

The project focuses on:

- distributed storage architecture,
- MVCC transaction processing,
- Raft-based consensus,
- automatic region scheduling,
- and scalable distributed system engineering.

Inspired by modern cloud-native distributed databases such as TiKV, AegisDB was developed as a research-oriented distributed database engineering project exploring fault-tolerant storage systems and distributed transaction management.

---

## Design Goals

AegisDB was designed with the following goals:

- Provide scalable distributed key-value storage
- Support transactional consistency using MVCC
- Ensure fault tolerance through the Raft consensus protocol
- Enable automatic data sharding and scheduling
- Explore modern distributed database architecture in Rust
- Simulate realistic distributed storage cluster behavior

---

## System Architecture

The architecture of AegisDB follows a layered distributed database design consisting of:

- Client Layer
- API Service Layer
- Storage Abstraction Layer
- Distributed Consensus Layer
- Persistent Storage Engine

### Architecture Overview

## Key Features

### Distributed Storage

- Region-based data sharding
  
- Multi-replica replication
  
- Automatic region split and merge
  
- Dynamic load balancing
  
- Horizontal scalability
  

### Transaction System

- MVCC-based transaction model
  
- Snapshot isolation
  
- Two-phase commit protocol
  
- Lock conflict detection
  
- Transaction rollback and recovery
  

### Consensus & Fault Tolerance

- Raft consensus implementation
  
- Leader election and failover
  
- Log replication
  
- Majority commit guarantee
  
- Heartbeat synchronization
  

### Engineering Features

- RocksDB storage engine integration
  
- WAL persistence
  
- gRPC communication interface
  
- Interactive CLI client
  
- Multi-node cluster simulation
  
- Comprehensive test suite
  

---

## Storage Engine Design

AegisDB uses RocksDB as the persistent storage backend and organizes data using three Column Families.

### Column Families

#### CF_DEFAULT

Stores actual user data.

- Key: `encode_key(user_key, start_ts)`
  
- Value: user value
  

#### CF_WRITE

Stores MVCC write metadata.

- Key: `encode_key(user_key, commit_ts)`
  
- Value: `Write { start_ts, kind }`
  

Used for:

- version visibility,
  
- snapshot isolation,
  
- transaction commit tracking.
  

#### CF_LOCK

Stores transaction lock information.

- Key: `user_key`
  
- Value: `Lock { primary, ts, ttl, kind }`
  

Used for:

- lock conflict detection,
  
- two-phase commit coordination,
  
- deadlock prevention.
  

---

## MVCC Transaction Model

AegisDB implements Multi-Version Concurrency Control (MVCC) to provide transactional consistency.

### Transaction Lifecycle

#### 1. Timestamp Allocation

Each transaction receives:

- `start_ts`
  
- `commit_ts`
  

to maintain version ordering.

---

#### 2. Read Flow

The transaction read process includes:

1. Checking lock conflicts in `CF_LOCK`
  
2. Searching visible committed versions in `CF_WRITE`
  
3. Loading actual values from `CF_DEFAULT`
  

This guarantees snapshot isolation.

---

#### 3. Write Flow (Two-Phase Commit)

### Prewrite Phase

- Detect write conflicts
  
- Detect lock conflicts
  
- Write data into `CF_DEFAULT`
  
- Create lock entries in `CF_LOCK`
  

### Commit Phase

- Validate transaction ownership
  
- Write commit metadata into `CF_WRITE`
  
- Remove locks from `CF_LOCK`
  

---

## Distributed Consensus

AegisDB implements the Raft consensus algorithm for distributed consistency.

### Core Components

- RaftLog
  
- Progress Tracker
  
- State Machine
  
- Peer Replication
  
- Heartbeat Synchronization
  

### Supported Features

- Leader election
  
- Log replication
  
- Failover recovery
  
- Snapshot support
  
- Log compaction
  

---

## Region-Based Sharding

AegisDB uses Regions as the fundamental data sharding unit.

```rust
pub struct Region {
    pub id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub region_epoch: RegionEpoch,
    pub peers: Vec<Peer>,
}
```

### Region Operations

- Region Split
  
- Region Merge
  
- ChangePeer
  
- TransferLeader
  

### Scheduling Goals

- Region balance
  
- Leader balance
  
- Storage utilization balance
  
- Failure recovery
  

---

## Scheduler System

The scheduler subsystem automatically balances cluster workloads.

### Scheduler Types

#### BalanceRegionScheduler

- Detects uneven Region distribution
  
- Migrates Regions between Stores
  

#### BalanceLeaderScheduler

- Balances Leader distribution
  
- Reduces hotspot concentration
  

#### ScaleScheduler

- Supports dynamic cluster scaling
  
- Redistributes Regions to new Stores
  

---

## Multi-Node Cluster Simulation

AegisDB supports single-process multi-node distributed cluster simulation.

### Cluster Design

- One PD server maintains cluster metadata
  
- Multiple Store nodes communicate through gRPC
  
- Heartbeat synchronization maintains cluster topology
  
- Region statistics are updated dynamically
  

### Features

- Simulated 10-node cluster
  
- Persistent heartbeat streams
  
- Region topology management
  
- Dynamic scheduling visualization
  

This design enables realistic distributed system behavior without requiring multiple physical machines.

---

## Engineering Challenges

During development, several distributed systems challenges were explored:

- MVCC visibility and version management
  
- Lock conflict handling
  
- Distributed transaction coordination
  
- Raft log synchronization
  
- Region scheduling and balancing
  
- Cluster heartbeat management
  
- Multi-node distributed simulation
  
- Fault-tolerant storage design
  

---

## Tech Stack

### Programming Language

- Rust

### Storage Engine

- RocksDB

### Communication

- gRPC
  
- tonic
  

### Distributed Systems

- Raft Consensus
  
- MVCC
  
- Two-Phase Commit
  

### Tooling

- Cargo
  
- Protocol Buffers
  

---

## Project Structure

```text
aegisdb/
├── src/
│   ├── bin/
│   ├── storage/
│   ├── engine_util/
│   ├── server/
│   ├── transaction/
│   ├── raft/
│   ├── raftstore/
│   └── scheduler/
├── proto/
├── tests/
└── docs/
```

---

## Quick Start

### Build

```bash
cargo build --release
```

### Run Standalone Mode

```bash
cargo run --bin aegisdb -- run \
    --addr 127.0.0.1:20160 \
    --db-path /tmp/aegisdb
```

### Run Distributed Mode

```bash
cargo run --bin aegisdb_server \
    -- --store-addr 127.0.0.1:20160 \
    --scheduler-addr 127.0.0.1:2379 \
    --db-path /tmp/aegisdb
```

---

## CLI Example

```bash
aegisdb> SET key1 value1
aegisdb> GET key1
aegisdb> SCAN "" 10
aegisdb> DEL key1
```

### Transaction Example

```bash
aegisdb> KVPREWRITE primary_key 1000 3000 key1 value1
aegisdb> KVCOMMIT 1000 2000 key1
aegisdb> KVGET key1 2000
```

---

## API Interface

AegisDB provides gRPC-based APIs for distributed communication.

### RawKV API

- RawGet
  
- RawPut
  
- RawDelete
  
- RawScan
  

### Transaction API

- KvGet
  
- KvScan
  
- KvPrewrite
  
- KvCommit
  
- KvCheckTxnStatus
  
- KvBatchRollback
  
- KvResolveLock
  

---

## Testing

Run all tests:

```bash
cargo test
```

Run integration tests:

```bash
cargo test --test integration_test
```

---

## Future Work

Planned future improvements include:

- distributed transaction optimization
  
- snapshot-based recovery
  
- distributed monitoring dashboard
  
- adaptive scheduling strategies
  
- performance benchmarking
  
- follower read optimization
  
- cloud-native deployment support
  

---

## References

Part of the implementation was inspired by the following open-source projects:

- TiKV
  
- TinyKV
  
- RocksDB
  
- tonic
  

---

## Note

This project was developed for distributed systems research and engineering exploration purposes.

Due to ongoing development and experimental features, some components may still be under active refinement.

---

## Author

Computer Science undergraduate focusing on:

- Distributed Systems
  
- AI Systems Engineering
  
- Database Systems
  
- Retrieval-Augmented Generation
  
- Scalable Software Architecture
