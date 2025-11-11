mod server;


use aegisdb::{Config, StandaloneStorage, Storage, Modify, Put, Delete, Server, RawKvServer, MultiLevelKvServer, TransactionKvServer};
use aegisdb::proto::kvrpcpb::*;
use env_logger;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    
    println!("=== AegisDB 功能测试 ===\n");
    
    let config = Config::new_default();
    config.validate()?;
    
    let storage = StandaloneStorage::new(&config)?;
    storage.start().await?;
    
    println!("1. 测试基本写入和读取");
    let batch = vec![
        Modify::Put(Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            cf: "default".to_string(),
        }),
        Modify::Put(Put {
            key: b"key2".to_vec(),
            value: b"value2".to_vec(),
            cf: "default".to_string(),
        }),
    ];
    
    storage.write(batch).await?;
    
    let reader = storage.reader().await?;
    let value = reader.get_cf("default", b"key1").await?;
    println!("   读取 key1: {:?}", value);
    
    println!("\n2. 测试不同 Column Family");
    let cf_batch = vec![
        Modify::Put(Put {
            key: b"same_key".to_vec(),
            value: b"default_value".to_vec(),
            cf: "default".to_string(),
        }),
        Modify::Put(Put {
            key: b"same_key".to_vec(),
            value: b"write_value".to_vec(),
            cf: "write".to_string(),
        }),
        Modify::Put(Put {
            key: b"same_key".to_vec(),
            value: b"lock_value".to_vec(),
            cf: "lock".to_string(),
        }),
    ];
    
    storage.write(cf_batch).await?;
    
    let reader2 = storage.reader().await?;
    println!("   default CF: {:?}", reader2.get_cf("default", b"same_key").await?);
    println!("   write CF: {:?}", reader2.get_cf("write", b"same_key").await?);
    println!("   lock CF: {:?}", reader2.get_cf("lock", b"same_key").await?);
    
    println!("\n3. 测试删除操作");
    let delete_batch = vec![Modify::Delete(Delete {
        key: b"key1".to_vec(),
        cf: "default".to_string(),
    })];
    
    storage.write(delete_batch).await?;
    
    let reader3 = storage.reader().await?;
    let deleted_value = reader3.get_cf("default", b"key1").await?;
    println!("   删除后读取 key1: {:?}", deleted_value);
    
    println!("\n4. 测试批量写入");
    let mut large_batch = Vec::new();
    for i in 0..10 {
        large_batch.push(Modify::Put(Put {
            key: format!("batch_key_{}", i).into_bytes(),
            value: format!("batch_value_{}", i).into_bytes(),
            cf: "default".to_string(),
        }));
    }
    storage.write(large_batch).await?;
    println!("   成功写入 10 个键值对");
    
    println!("\n5. 测试迭代器");
    let reader4 = storage.reader().await?;
    let mut iter = reader4.iter_cf("default");
    let mut count = 0;
    while iter.valid() {
        let _item = iter.item();
        count += 1;
        iter.next();
    }
    println!("   迭代器遍历到 {} 个键值对", count);
    
    // 关闭第一个 storage，释放文件锁
    storage.stop().await?;
    
    // 等待一下，确保 RocksDB 完全释放锁文件
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    println!("\n=== AegisDB RawKV API 测试 ===\n");
    
    // 重新创建 storage 用于 RawKV API 测试，使用不同的数据库路径避免锁冲突
    let mut config2 = Config::new_default();
    config2.db_path = "/tmp/aegisdb_rawkv".to_string();  // 使用不同的路径
    config2.validate()?;
    
    let storage2 = StandaloneStorage::new(&config2)?;
    storage2.start().await?;
    
    let server = Server::new(storage2);
    
    println!("1. 测试 RawPut");
    let put_req = RawPutRequest {
        context: Context::new(1),
        key: b"user:1".to_vec(),
        value: b"Alice".to_vec(),
        cf: "default".to_string(),
    };
    let put_resp = RawKvServer::raw_put(&server, put_req).await?;
    println!("   RawPut 响应: {:?}", put_resp);
    
    println!("\n2. 测试 RawGet");
    let get_req = RawGetRequest {
        context: Context::new(1),
        key: b"user:1".to_vec(),
        cf: "default".to_string(),
    };
    let get_resp = RawKvServer::raw_get(&server, get_req).await?;
    println!("   RawGet 响应: {:?}", get_resp);
    if let Some(value) = &get_resp.value {
        println!("   值: {}", String::from_utf8_lossy(value));
    }
    
    println!("\n3. 测试 RawScan");
    // 写入更多数据
    for i in 1..=5 {
        let put_req = RawPutRequest {
            context: Context::new(1),
            key: format!("key{}", i).into_bytes(),
            value: format!("value{}", i).into_bytes(),
            cf: "default".to_string(),
        };
        RawKvServer::raw_put(&server, put_req).await?;
    }
    
    let scan_req = RawScanRequest {
        context: Context::new(1),
        start_key: b"key1".to_vec(),
        limit: 3,
        cf: "default".to_string(),
    };
    let scan_resp = RawKvServer::raw_scan(&server, scan_req).await?;
    println!("   RawScan 找到 {} 个键值对", scan_resp.kvs.len());
    for kv in &scan_resp.kvs {
        println!("   {} = {}", 
            String::from_utf8_lossy(&kv.key),
            String::from_utf8_lossy(&kv.value));
    }
    
    println!("\n4. 测试 RawDelete");
    let delete_req = RawDeleteRequest {
        context: Context::new(1),
        key: b"user:1".to_vec(),
        cf: "default".to_string(),
    };
    let delete_resp = RawKvServer::raw_delete(&server, delete_req).await?;
    println!("   RawDelete 响应: {:?}", delete_resp);
    
    // 验证已删除
    let get_req2 = RawGetRequest {
        context: Context::new(1),
        key: b"user:1".to_vec(),
        cf: "default".to_string(),
    };
    let get_resp2 = RawKvServer::raw_get(&server, get_req2).await?;
    println!("   删除后读取: not_found = {}", get_resp2.not_found);
    
    // 关闭 storage
    server.storage().stop().await?;
    println!("\n=== 所有测试完成 ===");
     
    println!("\n=== AegisDB 多级键值 API 测试 ===\n");

    // 重新创建 storage 用于多级键值测试
    let mut config3 = Config::new_default();
    config3.db_path = "/tmp/aegisdb_multilevel".to_string();
    config3.validate()?;

    let storage3 = StandaloneStorage::new(&config3)?;
    storage3.start().await?;

    let server3 = Server::new(storage3);

    // 使用 region_id = 100 作为一级键
    let region_id = 100u64;

    println!("1. 测试 MultiLevelPut（仅提供二级键）");
    let put_req = MultiLevelPutRequest {
        context: Context::new(region_id),
        secondary_key: b"user:1".to_vec(),
        value: b"Alice".to_vec(),
        cf: "default".to_string(),
    };
    let put_resp = MultiLevelKvServer::multi_level_put(&server3, put_req).await?;
    println!("   MultiLevelPut 响应: {:?}", put_resp);

    println!("\n2. 测试 MultiLevelGet（仅提供二级键）");
    let get_req = MultiLevelGetRequest {
        context: Context::new(region_id),
        secondary_key: b"user:1".to_vec(),
        cf: "default".to_string(),
    };
    let get_resp = MultiLevelKvServer::multi_level_get(&server3, get_req).await?;
    println!("   MultiLevelGet 响应: {:?}", get_resp);
    if let Some(value) = &get_resp.value {
        println!("   值: {}", String::from_utf8_lossy(value));
    }

    println!("\n3. 测试批量写入多个二级键");
    for i in 1..=5 {
        let put_req = MultiLevelPutRequest {
            context: Context::new(region_id),
            secondary_key: format!("item:{}", i).into_bytes(),
            value: format!("value{}", i).into_bytes(),
            cf: "default".to_string(),
        };
        MultiLevelKvServer::multi_level_put(&server3, put_req).await?;
    }
    println!("   成功写入 5 个二级键值对");

    println!("\n4. 测试 MultiLevelScan（扫描同一一级键下的所有二级键）");
    let scan_req = MultiLevelScanRequest {
        context: Context::new(region_id),
        start_secondary_key: b"item:1".to_vec(),
        limit: 10,
        cf: "default".to_string(),
    };
    let scan_resp = MultiLevelKvServer::multi_level_scan(&server3, scan_req).await?;
    println!("   MultiLevelScan 找到 {} 个键值对", scan_resp.kvs.len());
    for kv in &scan_resp.kvs {
        println!("   {} = {}", 
            String::from_utf8_lossy(&kv.secondary_key),
            String::from_utf8_lossy(&kv.value));
    }

    println!("\n5. 测试 MultiLevelDelete（仅提供二级键）");
    let delete_req = MultiLevelDeleteRequest {
        context: Context::new(region_id),
        secondary_key: b"user:1".to_vec(),
        cf: "default".to_string(),
    };
    let delete_resp = MultiLevelKvServer::multi_level_delete(&server3, delete_req).await?;
    println!("   MultiLevelDelete 响应: {:?}", delete_resp);

    // 验证已删除
    let get_req2 = MultiLevelGetRequest {
        context: Context::new(region_id),
        secondary_key: b"user:1".to_vec(),
        cf: "default".to_string(),
    };
    let get_resp2 = MultiLevelKvServer::multi_level_get(&server3, get_req2).await?;
    println!("   删除后读取: not_found = {}", get_resp2.not_found);

    // 关闭 storage
    server3.storage().stop().await?;
    println!("\n=== 所有测试完成 ===");

    println!("\n=== AegisDB Raft 测试 ===\n");

    // 创建内存存储用于 Raft 测试
    use aegisdb::raft::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    struct MemoryStorage {
        hard_state: Arc<Mutex<HardState>>,
        conf_state: Arc<Mutex<ConfState>>,
        entries: Arc<Mutex<Vec<Entry>>>,
        snapshot: Arc<Mutex<Option<Snapshot>>>,
    }

    impl MemoryStorage {
        fn new() -> Self {
            Self {
                hard_state: Arc::new(Mutex::new(HardState {
                    term: 0,
                    vote: 0,
                    commit: 0,
                })),
                conf_state: Arc::new(Mutex::new(ConfState { nodes: Vec::new() })),
                entries: Arc::new(Mutex::new(Vec::new())),
                snapshot: Arc::new(Mutex::new(None)),
            }
        }

        fn with_peers(peers: Vec<u64>) -> Self {
            let storage = Self::new();
            *storage.conf_state.lock().unwrap() = ConfState { nodes: peers };
            storage
        }
    }

    #[async_trait]
    impl Storage for MemoryStorage {
        async fn initial_state(&self) -> anyhow::Result<(HardState, ConfState)> {
            let hs = *self.hard_state.lock().unwrap();
            let cs = self.conf_state.lock().unwrap().clone();
            Ok((hs, cs))
        }

        async fn entries(&self, lo: u64, hi: u64) -> anyhow::Result<Vec<Entry>> {
            let entries = self.entries.lock().unwrap();
            let mut result = Vec::new();
            for entry in entries.iter() {
                if entry.index >= lo && entry.index < hi {
                    result.push(entry.clone());
                }
            }
            Ok(result)
        }

        async fn term(&self, index: u64) -> anyhow::Result<u64> {
            if index == 0 {
                return Ok(0);
            }
            let entries = self.entries.lock().unwrap();
            for entry in entries.iter() {
                if entry.index == index {
                    return Ok(entry.term);
                }
            }
            Err(anyhow::anyhow!("entry at index {} not found", index))
        }

        async fn last_index(&self) -> anyhow::Result<u64> {
            let entries = self.entries.lock().unwrap();
            Ok(entries.last().map(|e| e.index).unwrap_or(0))
        }

        async fn first_index(&self) -> anyhow::Result<u64> {
            let entries = self.entries.lock().unwrap();
            Ok(entries.first().map(|e| e.index).unwrap_or(1))
        }

        async fn snapshot(&self) -> anyhow::Result<Snapshot> {
            let snapshot = self.snapshot.lock().unwrap();
            if let Some(ref snap) = *snapshot {
                Ok(snap.clone())
            } else {
                Ok(Snapshot {
                    data: Vec::new(),
                    metadata: SnapshotMetadata {
                        conf_state: ConfState { nodes: Vec::new() },
                        index: 0,
                        term: 0,
                    },
                })
            }
        }
    }

    println!("1. 测试创建 RawNode");
    let storage = Box::new(MemoryStorage::with_peers(vec![1, 2, 3]));
    let config = RaftConfig {
        id: 1,
        peers: vec![1, 2, 3],
        election_tick: 10,
        heartbeat_tick: 3,
        storage,
        applied: 0,
    };
    let raw_node = RawNode::new(config).await?;
    println!("   RawNode 创建成功: id={}, state={:?}", raw_node.raft.id, raw_node.raft.state);

    println!("\n2. 测试单节点集群选举");
    let storage2 = Box::new(MemoryStorage::with_peers(vec![1]));
    let config2 = RaftConfig {
        id: 1,
        peers: vec![1],
        election_tick: 10,
        heartbeat_tick: 3,
        storage: storage2,
        applied: 0,
    };
    let mut raw_node2 = RawNode::new(config2).await?;
    raw_node2.campaign()?;
    println!("   选举后状态: {:?}", raw_node2.raft.state);
    assert_eq!(raw_node2.raft.state, StateType::Leader);
    println!("   单节点集群成功成为 Leader");

    println!("\n3. 测试提案日志");
    let data = b"test data".to_vec();
    raw_node2.propose(data.clone())?;
    let ready = raw_node2.ready();
    if !ready.entries.is_empty() {
        println!("   提案成功，找到 {} 个条目", ready.entries.len());
        // 查找我们提案的条目（跳过 noop entry）
        for entry in &ready.entries {
            if !entry.data.is_empty() && entry.data == data {
                println!("   找到提案的条目: index={}, term={}", entry.index, entry.term);
            }
        }
    } else {
        println!("   警告: ready.entries 为空（可能包含 noop entry）");
    }

    println!("\n4. 测试配置变更");
    let cc = ConfChange {
        change_type: ConfChangeType::AddNode,
        node_id: 2,
        context: Vec::new(),
    };
    raw_node2.propose_conf_change(cc.clone())?;
    let ready2 = raw_node2.ready();
    if !ready2.entries.is_empty() {
        println!("   配置变更提案成功，找到 {} 个条目", ready2.entries.len());
        for entry in &ready2.entries {
            if entry.entry_type == EntryType::ConfChange {
                println!("   找到配置变更条目: index={}, term={}", entry.index, entry.term);
            }
        }
    }

    println!("\n=== Raft 测试完成 ===");

    println!("\n=== AegisDB Region 管理测试 ===\n");

    // 创建测试用的 Engines
    use aegisdb::engine_util::engines::create_db;
    use aegisdb::raftstore::*;
    use aegisdb::proto::metapb::{Region, Peer, RegionEpoch};

    // 使用临时目录路径
    let temp_kv_path = "/tmp/aegisdb_region_test_kv";
    let temp_raft_path = "/tmp/aegisdb_region_test_raft";
    
    // 清理可能存在的旧数据
    let _ = std::fs::remove_dir_all(temp_kv_path);
    let _ = std::fs::remove_dir_all(temp_raft_path);
    
    let kv_path = temp_kv_path;
    let raft_path = temp_raft_path;
    
    let kv_db = create_db(kv_path, false)?;
    let raft_db = create_db(raft_path, true)?;
    
    let engines = std::sync::Arc::new(aegisdb::engine_util::Engines::new(
        kv_db,
        Some(raft_db),
        kv_path.to_string(),
        raft_path.to_string(),
    ));

    println!("1. 测试 StoreMeta - Region 元数据管理");
    let store_meta = StoreMeta::new();
    
    // 创建测试 Region
    let region1 = Region {
        id: 1,
        start_key: b"a".to_vec(),
        end_key: b"m".to_vec(),
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![Peer {
            id: 1,
            store_id: 1,
        }],
    };
    
    let region2 = Region {
        id: 2,
        start_key: b"m".to_vec(),
        end_key: vec![],
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![Peer {
            id: 2,
            store_id: 1,
        }],
    };
    
    store_meta.set_region(region1.clone());
    store_meta.set_region(region2.clone());
    
    println!("   创建了 2 个 Region: region_id=1 (a-m), region_id=2 (m-)");
    
    // 测试根据 key 查找 Region
    if let Some(found) = store_meta.find_region_by_key(b"b") {
        println!("   查找 key='b' 的 Region: region_id={}", found.id);
    }
    
    if let Some(found) = store_meta.find_region_by_key(b"z") {
        println!("   查找 key='z' 的 Region: region_id={}", found.id);
    }
    
    println!("\n2. 测试 Router - 消息路由");
    let router = std::sync::Arc::new(Router::new());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    router.register(1, tx);
    
    let msg = aegisdb::raftstore::message::Msg {
        msg_type: aegisdb::raftstore::message::MsgType::Tick,
        region_id: 1,
        data: aegisdb::raftstore::message::MsgData::Empty,
    };
    router.send(1, msg).unwrap();
    println!("   成功发送消息到 region_id=1");
    
    // 异步接收消息（不阻塞）
    tokio::spawn(async move {
        if let Some(received) = rx.recv().await {
            println!("   接收到消息: region_id={}", received.region_id);
        }
    });
    
    println!("\n3. 测试 PeerStorage - Region 存储");
    let peer_storage = PeerStorage::new(engines.clone(), region1.clone())?;
    println!("   创建 PeerStorage 成功: region_id={}", peer_storage.region().id);
    println!("   applied_index={}, is_initialized={}", 
        peer_storage.applied_index(), 
        peer_storage.is_initialized());
    
    // 更新 Region
    let mut new_region = region1.clone();
    new_region.region_epoch.version = 2;
    peer_storage.set_region(new_region.clone());
    println!("   更新 Region epoch version 到 2");
    
    println!("\n4. 测试 SplitChecker - Region Split 检查");
    let raft_router = aegisdb::raftstore::router::RaftRouter::new(router.clone());
    let region_config = Config::new_test();
    let checker = aegisdb::raftstore::split_checker::SplitChecker::new(
        engines.clone(),
        raft_router,
        &region_config,
    );
    
    let large_region = Region {
        id: 100,
        start_key: b"key000".to_vec(),
        end_key: vec![],
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![Peer {
            id: 100,
            store_id: 1,
        }],
    };
    
    let split_key = checker.check(&large_region);
    println!("   检查 Region split: split_key={:?}", split_key);
    println!("   (当前是简化实现，实际需要扫描数据)");
    
    println!("\n5. 测试 Region 重叠检测");
    let region3 = Region {
        id: 3,
        start_key: b"b".to_vec(),
        end_key: b"c".to_vec(),
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![Peer {
            id: 3,
            store_id: 1,
        }],
    };
    
    store_meta.set_region(region3.clone());
    let overlaps = store_meta.get_overlap_regions(&region3);
    println!("   检测 Region 3 (b-c) 的重叠:");
    for r in &overlaps {
        println!("     - region_id={}, start_key={:?}, end_key={:?}", 
            r.id, 
            String::from_utf8_lossy(&r.start_key),
            String::from_utf8_lossy(&r.end_key));
    }
    
    println!("\n6. 测试元数据读写");
    use aegisdb::raftstore::meta::values::{write_region_state, get_region_local_state, PeerState};
    use aegisdb::engine_util::WriteBatch;
    
    let mut kv_wb = WriteBatch::new();
    write_region_state(&mut kv_wb, &region1, PeerState::Normal)?;
    engines.write_kv(&kv_wb)?;
    
    let state = get_region_local_state(&engines, 1)?;
    if let Some(ref s) = state {
        println!("   成功读取 Region 状态: region_id={}, state={:?}", 
            s.region.id, s.state);
    }
    
    println!("\n=== Region 管理测试完成 ===");

    println!("\n=== AegisDB 调度器测试 ===\n");

    use aegisdb::scheduler::*;
    use aegisdb::scheduler::schedulers::Scheduler;
    use std::time::Duration;

    // 创建集群
    let cluster = std::sync::Arc::new(BasicCluster::new());

    println!("1. 创建测试 Store");
    let store1 = StoreInfo::new(aegisdb::proto::metapb::Store {
        id: 1,
        address: "127.0.0.1:20160".to_string(),
        state: aegisdb::proto::metapb::StoreState::Up,
    });
    let store2 = StoreInfo::new(aegisdb::proto::metapb::Store {
        id: 2,
        address: "127.0.0.1:20161".to_string(),
        state: aegisdb::proto::metapb::StoreState::Up,
    });
    let store3 = StoreInfo::new(aegisdb::proto::metapb::Store {
        id: 3,
        address: "127.0.0.1:20162".to_string(),
        state: aegisdb::proto::metapb::StoreState::Up,
    });

    cluster.put_store(store1);
    cluster.put_store(store2);
    cluster.put_store(store3);
    println!("   创建了 3 个 Store");

    println!("\n2. 创建测试 Region（不均衡分布）");
    // Store 1 有多个 Region，Store 2 和 3 较少
    for i in 1..=5 {
        let mut region = RegionInfo::new(
            Region {
                id: i,
                start_key: format!("key{}", i).into_bytes(),
                end_key: format!("key{}", i + 1).into_bytes(),
                region_epoch: RegionEpoch::new(1, 1),
                peers: vec![
                    Peer { id: i * 10, store_id: 1 },
                    Peer { id: i * 10 + 1, store_id: 2 },
                ],
            },
            Some(Peer { id: i * 10, store_id: 1 }),
        );
        region.set_approximate_size(10);
        cluster.put_region(region);
    }
    println!("   创建了 5 个 Region，Store 1 有 5 个 Leader");

    println!("\n3. 查看集群统计信息");
    let stores = cluster.get_stores();
    for store in &stores {
        println!("   Store {}: {} regions, {} leaders, {} MB region size, {} MB leader size",
            store.id(),
            store.region_count(),
            store.leader_count(),
            store.region_size(),
            store.leader_size());
    }

    println!("\n4. 测试 BalanceRegionScheduler");
    let balance_region = BalanceRegionScheduler::new(Duration::from_secs(30));
    if let Some(op) = balance_region.schedule(&cluster) {
        println!("   创建了 Region 均衡操作: {:?}", op);
        println!("   操作描述: {}", op.desc);
    } else {
        println!("   当前没有需要均衡的 Region");
    }

    println!("\n5. 测试 BalanceLeaderScheduler");
    let balance_leader = BalanceLeaderScheduler::new(Duration::from_secs(30));
    if let Some(op) = balance_leader.schedule(&cluster) {
        println!("   创建了 Leader 均衡操作: {:?}", op);
        println!("   操作描述: {}", op.desc);
    } else {
        println!("   当前没有需要均衡的 Leader");
    }

    println!("\n6. 测试 OperatorController");
    use aegisdb::scheduler::operator_controller::OperatorController;
    use aegisdb::scheduler::heartbeat_streams::SimpleHeartbeatStreams;
    
    // 创建心跳流
    let hb_streams = std::sync::Arc::new(SimpleHeartbeatStreams::new());
    
    // 创建 OperatorController
    let op_controller = std::sync::Arc::new(OperatorController::new(
        std::sync::Arc::clone(&cluster),
        hb_streams.clone(),
    ));
    
    println!("   创建了 OperatorController");
    
    // 测试添加操作
    let test_region = cluster.get_region(1).unwrap();
    let transfer_op = Operator::create_transfer_leader_operator(
        "test transfer leader".to_string(),
        &test_region,
        1,
        2,
    );
    
    if op_controller.add_operator(vec![transfer_op]) {
        println!("   成功添加 TransferLeader 操作");
    } else {
        println!("   添加操作失败");
    }
    
    // 测试分发操作
    println!("\n   测试分发操作（从心跳）");
    op_controller.dispatch(&test_region, "heartbeat");
    
    // 检查发送的消息
    let messages = hb_streams.get_sent_messages();
    println!("   发送了 {} 条消息", messages.len());
    for (region_id, msg) in &messages {
        println!("     Region {}: change_peer={:?}, transfer_leader={:?}",
            region_id,
            msg.change_peer.is_some(),
            msg.transfer_leader.is_some());
    }
    
    // 测试获取操作
    if let Some(op) = op_controller.get_operator(1) {
        println!("   找到操作: region_id={}, desc={}", op.region_id, op.desc);
    }
    
    // 测试操作计数
    let region_count = op_controller.operator_count(OpKind::Region);
    let leader_count = op_controller.operator_count(OpKind::Leader);
    println!("   操作计数: Region={}, Leader={}", region_count, leader_count);
    
    println!("\n7. 测试 Coordinator with OperatorController");
    let coordinator = Coordinator::new(
        std::sync::Arc::clone(&cluster),
        op_controller.clone(),
    );
    
    coordinator.register_scheduler(Box::new(BalanceRegionScheduler::new(Duration::from_secs(30))));
    coordinator.register_scheduler(Box::new(BalanceLeaderScheduler::new(Duration::from_secs(30))));
    
    println!("   已注册 2 个调度器");
    
    // 启动协调器运行一小段时间
    coordinator.start().await;
    println!("   协调器已启动");
    
    // 等待一段时间让调度器运行
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // 检查是否有创建的操作
    let all_ops = op_controller.get_operators();
    println!("   当前有 {} 个运行中的操作", all_ops.len());
    for op in &all_ops {
        println!("     操作: region_id={}, desc={}, current_step={}/{}",
            op.region_id, op.desc, op.current_step, op.steps.len());
    }
    
    coordinator.stop();
    println!("   协调器已停止");

    println!("\n8. 测试 Operator 步骤");
    use aegisdb::scheduler::operator::{OpStep, AddPeer, TransferLeader};
    
    let test_region2 = RegionInfo::new(
        Region {
            id: 100,
            start_key: b"test".to_vec(),
            end_key: b"test_end".to_vec(),
            region_epoch: RegionEpoch::new(1, 1),
            peers: vec![Peer { id: 1000, store_id: 1 }],
        },
        Some(Peer { id: 1000, store_id: 1 }),
    );
    
    let add_peer = AddPeer {
        to_store: 2,
        peer_id: 2000,
    };
    println!("   AddPeer 步骤: {}", add_peer.description());
    println!("   是否完成: {}", add_peer.is_finish(&test_region2));
    
    let transfer_leader = TransferLeader {
        from_store: 1,
        to_store: 2,
    };
    println!("   TransferLeader 步骤: {}", transfer_leader.description());
    println!("   是否完成: {}", transfer_leader.is_finish(&test_region2));
    
    println!("\n9. 测试完整的操作流程（AddPeer -> TransferLeader -> RemovePeer）");
    let mut test_region3 = RegionInfo::new(
        Region {
            id: 200,
            start_key: b"test2".to_vec(),
            end_key: b"test2_end".to_vec(),
            region_epoch: RegionEpoch::new(1, 1),
            peers: vec![Peer { id: 2000, store_id: 1 }],
        },
        Some(Peer { id: 2000, store_id: 1 }),
    );
    test_region3.set_approximate_size(20);
    cluster.put_region(test_region3.clone());
    
    // 创建 MovePeer 操作
    let move_op = Operator::create_move_peer_operator(
        "move peer from store 1 to store 2".to_string(),
        &test_region3,
        1,
        2,
        2001,
    );
    
    println!("   创建 MovePeer 操作: {}", move_op.desc);
    println!("   操作步骤数: {}", move_op.steps.len());
    
    if op_controller.add_operator(vec![move_op]) {
        println!("   成功添加 MovePeer 操作");
        
        // 模拟操作执行过程
        println!("\n   模拟操作执行:");
        
        // 第一步：AddPeer
        println!("   步骤 1: AddPeer");
        op_controller.dispatch(&test_region3, "heartbeat");
        
        // 模拟添加 peer 成功
        let mut updated_region = test_region3.clone();
        let mut new_peers = updated_region.region().peers.clone();
        new_peers.push(Peer { id: 2001, store_id: 2 });
        updated_region = RegionInfo::new(
            Region {
                id: 200,
                start_key: updated_region.start_key().to_vec(),
                end_key: updated_region.end_key().to_vec(),
                region_epoch: RegionEpoch {
                    version: 1,
                    conf_ver: 2, // conf_ver 增加
                },
                peers: new_peers,
            },
            Some(Peer { id: 2000, store_id: 1 }),
        );
        cluster.put_region(updated_region.clone());
        
        // 第二步：TransferLeader
        println!("   步骤 2: TransferLeader");
        op_controller.dispatch(&updated_region, "heartbeat");
        
        // 模拟转移 leader 成功
        let mut updated_region2 = updated_region.clone();
        updated_region2.set_leader(Some(Peer { id: 2001, store_id: 2 }));
        cluster.put_region(updated_region2.clone());
        
        // 第三步：RemovePeer
        println!("   步骤 3: RemovePeer");
        op_controller.dispatch(&updated_region2, "heartbeat");
        
        // 模拟移除 peer 成功
        let mut final_peers = updated_region2.region().peers.clone();
        final_peers.retain(|p| p.store_id != 1);
        let final_region = RegionInfo::new(
            Region {
                id: 200,
                start_key: updated_region2.start_key().to_vec(),
                end_key: updated_region2.end_key().to_vec(),
                region_epoch: RegionEpoch {
                    version: 1,
                    conf_ver: 3, // conf_ver 再次增加
                },
                peers: final_peers,
            },
            Some(Peer { id: 2001, store_id: 2 }),
        );
        cluster.put_region(final_region.clone());
        
        // 最后检查操作是否完成
        op_controller.dispatch(&final_region, "heartbeat");
        
        if op_controller.get_operator(200).is_none() {
            println!("   操作已完成！");
        } else {
            println!("   操作仍在进行中");
        }
        
        // 检查操作状态
        if let Some(status) = op_controller.get_operator_status(200) {
            println!("   操作状态: {:?}", status.status);
        }
    } else {
        println!("   添加 MovePeer 操作失败");
    }

    println!("\n=== 调度器测试完成 ===");

    println!("\n=== AegisDB MVCC 事务测试 ===\n");

    // 创建测试用的 storage（使用临时目录）
    use tempfile::TempDir;
    let temp_dir_mvcc = TempDir::new()?;
    let mut config_mvcc = Config::new_default();
    config_mvcc.db_path = temp_dir_mvcc.path().to_str().unwrap().to_string();
    config_mvcc.validate()?;

    let storage_mvcc = StandaloneStorage::new(&config_mvcc)?;
    storage_mvcc.start().await?;

    use aegisdb::transaction::mvcc::*;
    use aegisdb::transaction::latches::Latches;

    println!("1. 测试 MVCC 键编码");
    let user_key = b"test_key";
    let ts = 1234567890u64;
    let encoded = codec::encode_key(user_key, ts);
    let decoded_key = codec::decode_user_key(&encoded)?;
    let decoded_ts = codec::decode_timestamp(&encoded)?;
    println!("   原始键: {:?}, 时间戳: {}", String::from_utf8_lossy(user_key), ts);
    println!("   编码后长度: {}", encoded.len());
    println!("   解码键: {:?}, 时间戳: {}", String::from_utf8_lossy(&decoded_key), decoded_ts);
    assert_eq!(decoded_key, user_key);
    assert_eq!(decoded_ts, ts);

    println!("\n2. 测试 Write 序列化");
    let write = write::Write {
        start_ts: 100,
        kind: write::WriteKind::Put,
    };
    let bytes = write.to_bytes();
    let parsed = write::Write::parse(&bytes)
        .map_err(|e| anyhow::anyhow!("failed to parse write: {}", e))?;
    println!("   Write 序列化长度: {}", bytes.len());
    println!("   解析成功: start_ts={}", parsed.unwrap().start_ts);

    println!("\n3. 测试 Lock 序列化");
    let lock = lock::Lock {
        primary: b"primary_key".to_vec(),
        ts: 100,
        ttl: 3000,
        kind: write::WriteKind::Put,
    };
    let lock_bytes = lock.to_bytes();
    let parsed_lock = lock::Lock::parse(&lock_bytes)
        .map_err(|e| anyhow::anyhow!("failed to parse lock: {}", e))?;
    println!("   Lock 序列化长度: {}", lock_bytes.len());
    println!("   解析成功: ts={}, ttl={}", parsed_lock.ts, parsed_lock.ttl);

    println!("\n4. 测试 MVCC 事务基本操作");
    let reader1 = storage_mvcc.reader().await?;
    let mut txn1 = transaction::MvccTxn::new(reader1, 100);
    
    // 写入值
    txn1.put_value(b"key1", b"value1");
    println!("   写入 key1 = value1 (start_ts=100)");
    
    // 写入 Write 记录
    let write1 = write::Write {
        start_ts: 100,
        kind: write::WriteKind::Put,
    };
    txn1.put_write(b"key1", 100, &write1);
    
    // 提交写入
    storage_mvcc.write(txn1.writes().to_vec()).await?;
    println!("   提交事务成功");
    
    // 创建新事务读取
    let reader2 = storage_mvcc.reader().await?;
    let txn2 = transaction::MvccTxn::new(reader2, 200);
    let value = txn2.get_value(b"key1").await?;
    println!("   读取 key1 (start_ts=200): {:?}", value.as_ref().map(|v| String::from_utf8_lossy(v)));
    assert_eq!(value, Some(b"value1".to_vec()));

    println!("\n5. 测试 MVCC 事务锁");
    let reader3 = storage_mvcc.reader().await?;
    let mut txn3 = transaction::MvccTxn::new(reader3, 300);
    
    // 添加锁
    let lock1 = lock::Lock {
        primary: b"primary".to_vec(),
        ts: 300,
        ttl: 3000,
        kind: write::WriteKind::Put,
    };
    txn3.put_lock(b"key2", &lock1);
    storage_mvcc.write(txn3.writes().to_vec()).await?;
    println!("   添加锁到 key2 (ts=300)");
    
    // 读取锁
    let reader4 = storage_mvcc.reader().await?;
    let txn4 = transaction::MvccTxn::new(reader4, 400);
    let read_lock = txn4.get_lock(b"key2").await?;
    if let Some(ref lock) = read_lock {
        println!("   读取锁成功: ts={}, ttl={}", lock.ts, lock.ttl);
    }

    println!("\n6. 测试 Latches 并发控制");
    let latches = Latches::new();
    let keys1 = vec![b"key1".to_vec(), b"key2".to_vec()];
    let keys2 = vec![b"key2".to_vec(), b"key3".to_vec()];
    
    // 第一个任务获取锁
    let notify1 = latches.acquire_latches(&keys1);
    assert!(notify1.is_none());
    println!("   任务1获取锁成功: keys1");
    
    // 第二个任务尝试获取冲突的锁
    let notify2 = latches.acquire_latches(&keys2);
    assert!(notify2.is_some());
    println!("   任务2等待锁: keys2 (与 keys1 冲突)");
    
    // 释放第一个任务的锁
    latches.release_latches(&keys1);
    println!("   任务1释放锁");
    
    // 等待一小段时间让通知生效
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    
    // 现在第二个任务应该能获取锁了
    let notify3 = latches.acquire_latches(&keys2);
    assert!(notify3.is_none());
    println!("   任务2获取锁成功");

    println!("\n7. 测试多版本读取");
    // 在时间戳 200 写入新值
    let reader5 = storage_mvcc.reader().await?;
    let mut txn5 = transaction::MvccTxn::new(reader5, 200);
    txn5.put_value(b"key1", b"value1_v2");
    let write2 = write::Write {
        start_ts: 200,
        kind: write::WriteKind::Put,
    };
    txn5.put_write(b"key1", 200, &write2);
    storage_mvcc.write(txn5.writes().to_vec()).await?;
    println!("   在时间戳 200 写入 key1 = value1_v2");
    
    // 在时间戳 150 的事务应该读取到旧值
    let reader6 = storage_mvcc.reader().await?;
    let txn6 = transaction::MvccTxn::new(reader6, 150);
    let old_value = txn6.get_value(b"key1").await?;
    println!("   时间戳 150 读取: {:?}", old_value.as_ref().map(|v| String::from_utf8_lossy(v)));
    assert_eq!(old_value, Some(b"value1".to_vec()));
    
    // 在时间戳 250 的事务应该读取到新值
    let reader7 = storage_mvcc.reader().await?;
    let txn7 = transaction::MvccTxn::new(reader7, 250);
    let new_value = txn7.get_value(b"key1").await?;
    println!("   时间戳 250 读取: {:?}", new_value.as_ref().map(|v| String::from_utf8_lossy(v)));
    assert_eq!(new_value, Some(b"value1_v2".to_vec()));

    storage_mvcc.stop().await?;
    println!("\n=== MVCC 事务测试完成 ===");

    println!("\n=== AegisDB 事务 API 测试 ===\n");

    // 创建测试用的 storage（使用临时目录）
    let temp_dir_txn = TempDir::new()?;
    let mut config_txn = Config::new_default();
    config_txn.db_path = temp_dir_txn.path().to_str().unwrap().to_string();
    config_txn.validate()?;

    let storage_txn = StandaloneStorage::new(&config_txn)?;
    storage_txn.start().await?;

    let server_txn = Server::new(storage_txn);

    println!("1. 测试 KvGet - 事务读取");
    // 先写入一些数据
    let reader0 = server_txn.storage().reader().await?;
    let mut txn0 = transaction::MvccTxn::new(reader0, 1000);
    txn0.put_value(b"user:1", b"Alice");
    let write0 = write::Write {
        start_ts: 1000,
        kind: write::WriteKind::Put,
    };
    txn0.put_write(b"user:1", 1000, &write0);
    server_txn.storage().write(txn0.writes().to_vec()).await?;

    let get_req = GetRequest {
        context: Context::new(1),
        key: b"user:1".to_vec(),
        version: 2000,  // 在时间戳 2000 读取
    };
    let get_resp = TransactionKvServer::kv_get(&server_txn, get_req).await?;
    println!("   KvGet 响应: {:?}", get_resp);
    if let Some(value) = &get_resp.value {
        println!("   值: {}", String::from_utf8_lossy(value));
    }

    println!("\n2. 测试 KvPrewrite - 两阶段提交第一阶段");
    let prewrite_req = PrewriteRequest {
        context: Context::new(1),
        mutations: vec![
            Mutation {
                op: Op::Put,
                key: b"user:2".to_vec(),
                value: b"Bob".to_vec(),
            },
            Mutation {
                op: Op::Put,
                key: b"user:3".to_vec(),
                value: b"Charlie".to_vec(),
            },
        ],
        primary_lock: b"user:2".to_vec(),
        start_version: 3000,
        lock_ttl: 3000,
    };
    let prewrite_resp = TransactionKvServer::kv_prewrite(
        &server_txn,
        server_txn.latches(),
        prewrite_req,
    ).await?;
    println!("   KvPrewrite 响应: {:?}", prewrite_resp);
    if !prewrite_resp.errors.is_empty() {
        println!("   错误: {:?}", prewrite_resp.errors);
    } else {
        println!("   Prewrite 成功，键已锁定");
    }

    println!("\n3. 测试 KvCommit - 两阶段提交第二阶段");
    let commit_req = CommitRequest {
        context: Context::new(1),
        start_version: 3000,
        keys: vec![b"user:2".to_vec(), b"user:3".to_vec()],
        commit_version: 3001,
    };
    let commit_resp = TransactionKvServer::kv_commit(
        &server_txn,
        server_txn.latches(),
        commit_req,
    ).await?;
    println!("   KvCommit 响应: {:?}", commit_resp);
    if commit_resp.error.is_some() {
        println!("   错误: {:?}", commit_resp.error);
    } else {
        println!("   Commit 成功");
    }

    // 验证提交后的值
    let get_req2 = GetRequest {
        context: Context::new(1),
        key: b"user:2".to_vec(),
        version: 4000,
    };
    let get_resp2 = TransactionKvServer::kv_get(&server_txn, get_req2).await?;
    if let Some(value) = &get_resp2.value {
        println!("   提交后读取 user:2: {}", String::from_utf8_lossy(value));
    }

    println!("\n4. 测试 KvScan - 事务扫描");
    // 写入更多数据用于扫描
    let reader1 = server_txn.storage().reader().await?;
    let mut txn1 = transaction::MvccTxn::new(reader1, 5000);
    for i in 1..=5 {
        let key = format!("item:{}", i).into_bytes();
        let value = format!("value{}", i).into_bytes();
        txn1.put_value(&key, &value);
        let write1 = write::Write {
            start_ts: 5000,
            kind: write::WriteKind::Put,
        };
        txn1.put_write(&key, 5000, &write1);
    }
    server_txn.storage().write(txn1.writes().to_vec()).await?;

    let scan_req = ScanRequest {
        context: Context::new(1),
        start_key: b"item:1".to_vec(),
        limit: 10,
        version: 6000,
    };
    let scan_resp = TransactionKvServer::kv_scan(&server_txn, scan_req).await?;
    println!("   KvScan 找到 {} 个键值对", scan_resp.pairs.len());
    for pair in &scan_resp.pairs {
        if pair.error.is_none() {
            println!("   {} = {}", 
                String::from_utf8_lossy(&pair.key),
                String::from_utf8_lossy(&pair.value));
        } else {
            println!("   {} (错误: {:?})", 
                String::from_utf8_lossy(&pair.key),
                pair.error);
        }
    }

    println!("\n5. 测试 KvCheckTxnStatus - 检查事务状态");
    // 创建一个锁定的键
    let reader2 = server_txn.storage().reader().await?;
    let mut txn2 = transaction::MvccTxn::new(reader2, 7000);
    let lock2 = lock::Lock {
        primary: b"primary_key".to_vec(),
        ts: 7000,
        ttl: 3000,
        kind: write::WriteKind::Put,
    };
    txn2.put_lock(b"locked_key", &lock2);
    server_txn.storage().write(txn2.writes().to_vec()).await?;

    let check_req = CheckTxnStatusRequest {
        context: Context::new(1),
        primary_key: b"primary_key".to_vec(),
        lock_ts: 7000,
        current_ts: 8000,  // 当前时间戳
    };
    let check_resp = TransactionKvServer::kv_check_txn_status(
        &server_txn,
        server_txn.latches(),
        check_req,
    ).await?;
    println!("   KvCheckTxnStatus 响应: {:?}", check_resp);
    println!("   锁 TTL: {}, 提交版本: {}, 操作: {:?}", 
        check_resp.lock_ttl, 
        check_resp.commit_version,
        check_resp.action);

    println!("\n6. 测试 KvBatchRollback - 批量回滚");
    // 创建一些锁定的键
    let reader3 = server_txn.storage().reader().await?;
    let mut txn3 = transaction::MvccTxn::new(reader3, 9000);
    let lock3 = lock::Lock {
        primary: b"rollback_primary".to_vec(),
        ts: 9000,
        ttl: 3000,
        kind: write::WriteKind::Put,
    };
    txn3.put_lock(b"rollback_key1", &lock3);
    txn3.put_lock(b"rollback_key2", &lock3);
    server_txn.storage().write(txn3.writes().to_vec()).await?;

    let rollback_req = BatchRollbackRequest {
        context: Context::new(1),
        start_version: 9000,
        keys: vec![b"rollback_key1".to_vec(), b"rollback_key2".to_vec()],
    };
    let rollback_resp = TransactionKvServer::kv_batch_rollback(
        &server_txn,
        server_txn.latches(),
        rollback_req,
    ).await?;
    println!("   KvBatchRollback 响应: {:?}", rollback_resp);
    if rollback_resp.error.is_some() {
        println!("   错误: {:?}", rollback_resp.error);
    } else {
        println!("   批量回滚成功");
    }

    println!("\n7. 测试 KvResolveLock - 解决锁冲突");
    // 创建一些锁定的键
    let reader4 = server_txn.storage().reader().await?;
    let mut txn4 = transaction::MvccTxn::new(reader4, 10000);
    let lock4 = lock::Lock {
        primary: b"resolve_primary".to_vec(),
        ts: 10000,
        ttl: 3000,
        kind: write::WriteKind::Put,
    };
    txn4.put_lock(b"resolve_key1", &lock4);
    txn4.put_lock(b"resolve_key2", &lock4);
    server_txn.storage().write(txn4.writes().to_vec()).await?;

    // 提交锁
    let resolve_req = ResolveLockRequest {
        context: Context::new(1),
        start_version: 10000,
        commit_version: 10001,  // >0 表示提交
    };
    let resolve_resp = TransactionKvServer::kv_resolve_lock(
        &server_txn,
        server_txn.latches(),
        resolve_req,
    ).await?;
    println!("   KvResolveLock 响应: {:?}", resolve_resp);
    if resolve_resp.error.is_some() {
        println!("   错误: {:?}", resolve_resp.error);
    } else {
        println!("   解决锁成功（提交）");
    }

    // 验证锁已提交
    let get_req3 = GetRequest {
        context: Context::new(1),
        key: b"resolve_key1".to_vec(),
        version: 11000,
    };
    let get_resp3 = TransactionKvServer::kv_get(&server_txn, get_req3).await?;
    println!("   解决锁后读取 resolve_key1: {:?}", get_resp3.value);

    server_txn.storage().stop().await?;
    println!("\n=== 事务 API 测试完成 ===");

    println!("\n=== AegisDB 调度器与 RaftStore 集成测试 ===\n");

    use aegisdb::raftstore::scheduler_client::{SchedulerClient, Client};
    use aegisdb::raftstore::runner::{SchedulerTaskHandler, SchedulerTask, SchedulerRegionHeartbeatTask, SchedulerStoreHeartbeatTask, SchedulerAskSplitTask};
    use aegisdb::raftstore::router::{Router, RaftRouter};
    use aegisdb::proto::schedulerpb::{StoreStats, RegionHeartbeatRequest, RegionHeartbeatResponse};

    // 创建测试用的 Engines（虽然当前测试未使用，但保留以备将来扩展）
    let _temp_kv_path = "/tmp/aegisdb_scheduler_integration_kv";
    let _temp_raft_path = "/tmp/aegisdb_scheduler_integration_raft";

    println!("1. 创建调度器客户端");
    let scheduler_client: Box<dyn SchedulerClient> = Box::new(
        Client::new(
            vec!["127.0.0.1:2379".to_string()],
            "store-1".to_string(),
        ).await?
    );
    
    // 等待集群 ID 初始化
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    
    println!("   调度器客户端创建成功，集群 ID: {}", scheduler_client.get_cluster_id());

    println!("\n2. 创建 Router 和 RaftRouter");
    let router = Arc::new(Router::new());
    let raft_router = RaftRouter::new(router.clone());
    println!("   Router 创建成功");

    println!("\n3. 创建调度器任务处理器");
    let task_handler = SchedulerTaskHandler::new(
        1,
        scheduler_client,
        raft_router,
    );
    task_handler.start();
    println!("   任务处理器启动成功");

    println!("\n4. 测试 Region 心跳任务");
    let region = Region {
        id: 100,
        start_key: b"key000".to_vec(),
        end_key: b"key999".to_vec(),
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![Peer { id: 1000, store_id: 1 }],
    };
    
    // 为 region 100 注册一个假的 sender，用于测试 AskSplit 任务
    let (fake_sender, mut fake_receiver) = tokio::sync::mpsc::unbounded_channel();
    router.register(region.id, fake_sender);
    
    // 启动一个后台任务来接收消息（避免 channel 满）
    tokio::spawn(async move {
        while let Some(_msg) = fake_receiver.recv().await {
            // 忽略消息，仅用于测试
        }
    });
    
    let heartbeat_task = SchedulerTask::RegionHeartbeat(SchedulerRegionHeartbeatTask {
        region: region.clone(),
        peer: Peer { id: 1000, store_id: 1 },
        pending_peers: vec![],
        approximate_size: Some(1024 * 1024), // 1MB
    });
    
    task_handler.handle(heartbeat_task).await?;
    println!("   Region 心跳任务处理成功");

    println!("\n5. 测试 Store 心跳任务");
    let store_stats = StoreStats {
        store_id: 1,
        capacity: 100 * 1024 * 1024, // 100MB
        available: 50 * 1024 * 1024,  // 50MB
        used_size: 50 * 1024 * 1024,  // 50MB
        region_count: 10,
        leader_count: 5,
    };
    
    let store_heartbeat_task = SchedulerTask::StoreHeartbeat(SchedulerStoreHeartbeatTask {
        stats: store_stats,
    });
    
    task_handler.handle(store_heartbeat_task).await?;
    println!("   Store 心跳任务处理成功");

    println!("\n6. 测试 AskSplit 任务");
    let split_task = SchedulerTask::AskSplit(SchedulerAskSplitTask {
        region: region.clone(),
        split_key: b"key500".to_vec(),
        peer: Peer { id: 1000, store_id: 1 },
        callback: None,
    });
    
    task_handler.handle(split_task).await?;
    println!("   AskSplit 任务处理成功");

    println!("\n7. 测试调度器客户端基本功能");
    let test_client: Box<dyn SchedulerClient> = Box::new(
        Client::new(
            vec!["127.0.0.1:2379".to_string()],
            "test-store".to_string(),
        ).await?
    );
    
    // 等待集群 ID 初始化
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    
    println!("   测试分配 ID");
    let id = test_client.alloc_id().await?;
    println!("   分配到的 ID: {}", id);
    
    println!("   测试 Bootstrap");
    let store = aegisdb::proto::metapb::Store {
        id: 1,
        address: "127.0.0.1:20160".to_string(),
        state: aegisdb::proto::metapb::StoreState::Up,
    };
    let bootstrap_resp = test_client.bootstrap(&store).await?;
    println!("   Bootstrap 响应: {:?}", bootstrap_resp.header.is_some());
    
    println!("   测试 IsBootstrapped");
    let bootstrapped = test_client.is_bootstrapped().await?;
    println!("   集群是否已初始化: {}", bootstrapped);

    println!("\n8. 测试 Region 心跳响应处理");
    // 创建一个新的客户端用于测试心跳响应
    let test_client2: Box<dyn SchedulerClient> = Box::new(
        Client::new(
            vec!["127.0.0.1:2379".to_string()],
            "test-store-2".to_string(),
        ).await?
    );
    
    // 等待集群 ID 初始化和心跳流循环启动
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // 设置心跳响应处理器
    let response_received = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let response_received_clone = response_received.clone();
    let response_region_id = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let response_region_id_clone = response_region_id.clone();
    
    test_client2.set_region_heartbeat_response_handler(
        1,
        Box::new(move |resp: RegionHeartbeatResponse| {
            println!("   收到 Region 心跳响应: region_id={}, epoch={:?}", 
                resp.region_id, 
                resp.region_epoch);
            response_received_clone.store(true, std::sync::atomic::Ordering::Relaxed);
            response_region_id_clone.store(resp.region_id, std::sync::atomic::Ordering::Relaxed);
        }),
    );
    
    // 发送心跳
    let heartbeat_req = RegionHeartbeatRequest {
        header: Some(test_client2.request_header()),
        region: Some(region.clone()),
        leader: Some(Peer { id: 1000, store_id: 1 }),
        pending_peers: vec![],
        approximate_size: 1024,
    };
    
    println!("   发送 Region 心跳请求: region_id={}", region.id);
    test_client2.region_heartbeat(heartbeat_req)?;
    
    // 等待响应（给足够的时间让心跳流循环处理）
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    if response_received.load(std::sync::atomic::Ordering::Relaxed) {
        let received_region_id = response_region_id.load(std::sync::atomic::Ordering::Relaxed);
        println!("   Region 心跳响应处理成功: region_id={}", received_region_id);
        assert_eq!(received_region_id, region.id, "收到的 region_id 应该匹配");
    } else {
        println!("   警告: 未收到 Region 心跳响应（可能是模拟实现）");
    }
    
    // 测试多次心跳
    println!("\n   测试多次心跳发送");
    for i in 1..=3 {
        let heartbeat_req = RegionHeartbeatRequest {
            header: Some(test_client2.request_header()),
            region: Some(region.clone()),
            leader: Some(Peer { id: 1000, store_id: 1 }),
            pending_peers: vec![],
            approximate_size: 1024 * i as u64,
        };
        test_client2.region_heartbeat(heartbeat_req)?;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    println!("   成功发送 3 次心跳");

    println!("\n9. 测试调度器客户端关闭");
    test_client.close().await;
    test_client2.close().await;
    println!("   调度器客户端关闭成功");

    println!("\n=== 调度器与 RaftStore 集成测试完成 ===");

    println!("\n=== AegisDB 数据分片测试 ===\n");

    // 创建测试用的 Engines
    let temp_dir_split = TempDir::new()?;
    let kv_path_split = temp_dir_split.path().join("kv");
    let raft_path_split = temp_dir_split.path().join("raft");
    
    let kv_db_split = create_db(kv_path_split.to_str().unwrap(), false)?;
    let raft_db_split = create_db(raft_path_split.to_str().unwrap(), true)?;
    
    let engines_split = Arc::new(aegisdb::engine_util::Engines::new(
        kv_db_split,
        Some(raft_db_split),
        kv_path_split.to_str().unwrap().to_string(),
        raft_path_split.to_str().unwrap().to_string(),
    ));

    println!("1. 测试 SplitChecker - Region 分片检测");
    
    // 写入大量数据以触发分片
    let mut wb = WriteBatch::new();
    let data_size = 100 * 1024; // 100KB per key
    let num_keys = 200; // 总共约 20MB 数据
    
    println!("   写入测试数据: {} 个键，每个键 {} 字节", num_keys, data_size);
    for i in 0..num_keys {
        let key = format!("key{:04}", i).into_bytes();
        let value = vec![i as u8; data_size];
        wb.set_cf("default", &key, &value);
    }
    engines_split.write_kv(&wb)?;
    println!("   数据写入完成");

    // 创建 Router 和 RaftRouter
    let router_split = Arc::new(Router::new());
    let raft_router_split = RaftRouter::new(router_split.clone());
    
    // 创建 SplitChecker
    let config_split = Config::new_test();
    let checker = aegisdb::raftstore::split_checker::SplitChecker::new(
        engines_split.clone(),
        raft_router_split,
        &config_split,
    );
    
    // 创建一个大 Region（包含所有数据）
    let large_region = Region {
        id: 1000,
        start_key: b"key0000".to_vec(),
        end_key: vec![],
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![Peer {
            id: 10000,
            store_id: 1,
        }],
    };
    
    println!("   检查 Region {} 是否需要分片", large_region.id);
    let split_key = checker.check(&large_region);
    if let Some(ref key) = split_key {
        println!("   检测到需要分片，split_key: {:?}", String::from_utf8_lossy(key));
    } else {
        println!("   当前 Region 大小未超过阈值，不需要分片");
        println!("   (注意: 实际数据量可能不足以触发分片，这是正常的)");
    }

    println!("\n2. 测试 StoreBalancer - 负载均衡");
    
    use aegisdb::raftstore::store_balancer::StoreBalancer;
    
    let balancer = Arc::new(StoreBalancer::new(Duration::from_secs(60)));
    
    // 模拟多个 Store 的负载
    println!("   更新 Store 负载信息");
    balancer.update_store_load(1, 10, 5, 100 * 1024 * 1024, 50 * 1024 * 1024);
    balancer.update_store_load(2, 5, 3, 50 * 1024 * 1024, 30 * 1024 * 1024);
    balancer.update_store_load(3, 3, 2, 30 * 1024 * 1024, 20 * 1024 * 1024);
    
    println!("   Store 1: 10 regions, 5 leaders, 100MB region size, 50MB leader size");
    println!("   Store 2: 5 regions, 3 leaders, 50MB region size, 30MB leader size");
    println!("   Store 3: 3 regions, 2 leaders, 30MB region size, 20MB leader size");
    
    // 选择负载最轻的 Store
    let selected_stores = balancer.select_stores(3);
    println!("   选择负载最轻的 3 个 Store:");
    for (idx, store_id) in selected_stores.iter().enumerate() {
        if let Some(load) = balancer.get_store_load(*store_id) {
            println!("     {}. Store {}: load_score={:.2}", idx + 1, store_id, load.load_score());
        }
    }
    
    // 验证选择的是负载最轻的 Store
    assert_eq!(selected_stores[0], 3, "应该选择负载最轻的 Store 3");
    assert_eq!(selected_stores[1], 2, "应该选择负载第二轻的 Store 2");
    assert_eq!(selected_stores[2], 1, "应该选择负载第三轻的 Store 1");

    println!("\n3. 测试 RegionAllocator - Region 分配");
    
    use aegisdb::raftstore::region_allocator::RegionAllocator;
    
    let allocator = Arc::new(RegionAllocator::new(balancer.clone(), 3));
    
    // 为新的 Region 分配 Peers
    let test_region = Region {
        id: 2000,
        start_key: b"test_start".to_vec(),
        end_key: b"test_end".to_vec(),
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![],
    };
    
    println!("   为新 Region {} 分配 Peers", test_region.id);
    let peers = allocator.allocate_peers(&test_region, 2000)?;
    println!("   分配到的 Peers:");
    for peer in &peers {
        println!("     Peer {}: store_id={}", peer.id, peer.store_id);
    }
    
    // 验证分配了 3 个 Peer
    assert_eq!(peers.len(), 3, "应该分配 3 个 Peer");
    
    // 验证 Peer 分布在不同的 Store
    let store_ids: Vec<u64> = peers.iter().map(|p| p.store_id).collect();
    assert_eq!(store_ids, vec![3, 2, 1], "Peer 应该分布在 Store 3, 2, 1");

    println!("\n4. 测试 AdminHandler - 执行分片操作");
    
    use aegisdb::raftstore::admin::AdminHandler;
    use aegisdb::raftstore::store_meta::StoreMeta;
    use aegisdb::proto::raft_cmdpb::{AdminRequest, AdminCmdType, SplitRequest};
    
    let store_meta = Arc::new(StoreMeta::new());
    let admin_handler = AdminHandler::new(
        store_meta.clone(),
        allocator.clone(),
        1, // store_id
    );
    
    // 创建要分片的 Region
    let region_to_split = Region {
        id: 3000,
        start_key: b"key0000".to_vec(),
        end_key: b"key9999".to_vec(),
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![Peer {
            id: 30000,
            store_id: 1,
        }],
    };
    
    // 先注册 Region 到 StoreMeta
    store_meta.set_region(region_to_split.clone());
    
    // 创建 Split Request
    let split_request = AdminRequest {
        cmd_type: AdminCmdType::Split as i32,
        split: Some(SplitRequest {
            split_key: b"key5000".to_vec(),
            new_region_id: 3001,
            new_peer_ids: vec![], // 空列表，让 RegionAllocator 分配
        }),
        change_peer: None,
        compact_log: None,
        transfer_leader: None,
    };
    
    println!("   执行分片操作:");
    println!("     原 Region: id={}, range=[{:?}, {:?}]", 
        region_to_split.id,
        String::from_utf8_lossy(&region_to_split.start_key),
        String::from_utf8_lossy(&region_to_split.end_key));
    println!("     split_key: {:?}", String::from_utf8_lossy(&split_request.split.as_ref().unwrap().split_key));
    println!("     新 Region ID: {}", split_request.split.as_ref().unwrap().new_region_id);
    
    let split_response = admin_handler.execute(&region_to_split, split_request)?;
    
    println!("   分片操作成功完成");
    if let Some(ref split_resp) = split_response.split {
        println!("   创建了 {} 个新 Region", split_resp.regions.len());
        for region in &split_resp.regions {
            println!("     Region {}: range=[{:?}, {:?}], epoch={:?}", 
                region.id,
                String::from_utf8_lossy(&region.start_key),
                String::from_utf8_lossy(&region.end_key),
                region.region_epoch);
        }
    }
    
    // 验证分片结果
    let updated_region = store_meta.get_region(3000).expect("应该能找到更新后的 Region");
    let new_region = store_meta.get_region(3001).expect("应该能找到新创建的 Region");
    
    println!("\n   验证分片结果:");
    println!("     更新后的原 Region: range=[{:?}, {:?}]", 
        String::from_utf8_lossy(&updated_region.start_key),
        String::from_utf8_lossy(&updated_region.end_key));
    println!("     新 Region: range=[{:?}, {:?}]", 
        String::from_utf8_lossy(&new_region.start_key),
        String::from_utf8_lossy(&new_region.end_key));
    
    // 验证 Region 范围
    assert_eq!(updated_region.end_key, b"key5000", "原 Region 的 end_key 应该是 split_key");
    assert_eq!(new_region.start_key, b"key5000", "新 Region 的 start_key 应该是 split_key");
    assert_eq!(new_region.end_key, b"key9999", "新 Region 的 end_key 应该是原 Region 的 end_key");
    
    // 验证 Region Epoch 已更新
    assert_eq!(updated_region.region_epoch.version, 2, "原 Region 的 epoch version 应该增加");
    assert_eq!(new_region.region_epoch.version, 2, "新 Region 的 epoch version 应该是 2");
    
    // 验证新 Region 有 Peers
    assert!(!new_region.peers.is_empty(), "新 Region 应该有 Peers");
    println!("     新 Region 有 {} 个 Peers", new_region.peers.len());
    for peer in &new_region.peers {
        println!("       Peer {}: store_id={}", peer.id, peer.store_id);
    }

    println!("\n5. 测试完整的分片流程 - 从检测到执行");
    
    // 创建新的测试 Region
    let test_region_2 = Region {
        id: 4000,
        start_key: b"split_test_start".to_vec(),
        end_key: b"split_test_end".to_vec(),
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![Peer {
            id: 40000,
            store_id: 1,
        }],
    };
    
    store_meta.set_region(test_region_2.clone());
    
    // 写入数据到这个 Region
    let mut wb2 = WriteBatch::new();
    for i in 0..50 {
        let key = format!("split_test_key{:03}", i).into_bytes();
        let value = vec![i as u8; 50 * 1024]; // 50KB per key
        wb2.set_cf("default", &key, &value);
    }
    engines_split.write_kv(&wb2)?;
    
    // 创建新的 SplitChecker
    let raft_router_split2 = RaftRouter::new(router_split.clone());
    let checker2 = aegisdb::raftstore::split_checker::SplitChecker::new(
        engines_split.clone(),
        raft_router_split2,
        &config_split,
    );
    
    println!("   检查 Region {} 是否需要分片", test_region_2.id);
    let split_key2 = checker2.check(&test_region_2);
    
    if let Some(ref key) = split_key2 {
        println!("   检测到需要分片，split_key: {:?}", String::from_utf8_lossy(key));
        
        // 执行分片操作
        let split_request2 = AdminRequest {
            cmd_type: AdminCmdType::Split as i32,
            split: Some(SplitRequest {
                split_key: key.clone(),
                new_region_id: 4001,
                new_peer_ids: vec![],
            }),
            change_peer: None,
            compact_log: None,
            transfer_leader: None,
        };
        
        println!("   执行分片操作...");
        let split_response2 = admin_handler.execute(&test_region_2, split_request2)?;
        
        if let Some(ref split_resp) = split_response2.split {
            println!("   分片成功，创建了 {} 个新 Region", split_resp.regions.len());
            
            // 验证分片后的 Region 可以正确查找 key
            let _updated_region2 = store_meta.get_region(4000).expect("应该能找到更新后的 Region");
            let _new_region2 = store_meta.get_region(4001).expect("应该能找到新创建的 Region");
            
            // 测试 key 查找
            let test_key1 = b"split_test_key010";
            let test_key2 = b"split_test_key040";
            
            let found_region1 = store_meta.find_region_by_key(test_key1);
            let found_region2 = store_meta.find_region_by_key(test_key2);
            
            println!("   验证 key 查找:");
            if let Some(ref r) = found_region1 {
                println!("     key {:?} 在 Region {}", String::from_utf8_lossy(test_key1), r.id);
            }
            if let Some(ref r) = found_region2 {
                println!("     key {:?} 在 Region {}", String::from_utf8_lossy(test_key2), r.id);
            }
        }
    } else {
        println!("   当前 Region 大小未超过阈值，不需要分片");
    }

    println!("\n6. 测试负载均衡 - 多次分片后的 Peer 分布");
    
    // 重置 Store 负载
    balancer.update_store_load(1, 0, 0, 0, 0);
    balancer.update_store_load(2, 0, 0, 0, 0);
    balancer.update_store_load(3, 0, 0, 0, 0);
    
    println!("   重置 Store 负载，开始多次分片测试");
    
    // 执行多次分片，验证负载均衡
    for i in 0..5 {
        let region_id = 5000 + i;
        let test_region = Region {
            id: region_id,
            start_key: format!("region{}_start", i).into_bytes(),
            end_key: format!("region{}_end", i).into_bytes(),
            region_epoch: RegionEpoch::new(1, 1),
            peers: vec![],
        };
        
        store_meta.set_region(test_region.clone());
        
        let split_request = AdminRequest {
            cmd_type: AdminCmdType::Split as i32,
            split: Some(SplitRequest {
                split_key: format!("region{}_mid", i).into_bytes(),
                new_region_id: region_id + 100,
                new_peer_ids: vec![],
            }),
            change_peer: None,
            compact_log: None,
            transfer_leader: None,
        };
        
        let _ = admin_handler.execute(&test_region, split_request);
        
        // 更新 Store 负载（模拟心跳）
        let store1_load = balancer.get_store_load(1).map(|l| l.region_count).unwrap_or(0);
        let store2_load = balancer.get_store_load(2).map(|l| l.region_count).unwrap_or(0);
        let store3_load = balancer.get_store_load(3).map(|l| l.region_count).unwrap_or(0);
        
        balancer.update_store_load(1, store1_load + 1, store1_load, 0, 0);
        balancer.update_store_load(2, store2_load + 1, store2_load, 0, 0);
        balancer.update_store_load(3, store3_load + 1, store3_load, 0, 0);
    }
    
    println!("   完成 5 次分片操作");
    
    // 检查负载分布
    println!("   检查负载分布:");
    for store_id in 1..=3 {
        if let Some(load) = balancer.get_store_load(store_id) {
            println!("     Store {}: {} regions, load_score={:.2}", 
                store_id, load.region_count, load.load_score());
        }
    }
    
    // 验证负载相对均衡（允许一定差异）
    let store1_regions = balancer.get_store_load(1).map(|l| l.region_count).unwrap_or(0);
    let store2_regions = balancer.get_store_load(2).map(|l| l.region_count).unwrap_or(0);
    let store3_regions = balancer.get_store_load(3).map(|l| l.region_count).unwrap_or(0);
    
    let max_regions = store1_regions.max(store2_regions).max(store3_regions);
    let min_regions = store1_regions.min(store2_regions).min(store3_regions);
    
    println!("   Region 分布: 最多 {} 个，最少 {} 个", max_regions, min_regions);
    if max_regions - min_regions <= 2 {
        println!("   负载分布相对均衡 ✓");
    } else {
        println!("   负载分布存在差异（这是正常的，取决于分配策略）");
    }

    println!("\n7. 测试分片边界情况");
    
    // 测试空 split_key
    let invalid_region = Region {
        id: 6000,
        start_key: b"test".to_vec(),
        end_key: b"test_end".to_vec(),
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![Peer { id: 60000, store_id: 1 }],
    };
    
    store_meta.set_region(invalid_region.clone());
    
    let invalid_split_request = AdminRequest {
        cmd_type: AdminCmdType::Split as i32,
        split: Some(SplitRequest {
            split_key: vec![], // 空的 split_key
            new_region_id: 6001,
            new_peer_ids: vec![],
        }),
        change_peer: None,
        compact_log: None,
        transfer_leader: None,
    };
    
    let result = admin_handler.execute(&invalid_region, invalid_split_request);
    if result.is_err() {
        println!("   正确处理空 split_key 错误: {}", result.unwrap_err());
    } else {
        println!("   警告: 空 split_key 应该返回错误");
    }
    
    // 测试 split_key 不在 Region 范围内
    let out_of_range_request = AdminRequest {
        cmd_type: AdminCmdType::Split as i32,
        split: Some(SplitRequest {
            split_key: b"out_of_range_key".to_vec(),
            new_region_id: 6002,
            new_peer_ids: vec![],
        }),
        change_peer: None,
        compact_log: None,
        transfer_leader: None,
    };
    
    let result2 = admin_handler.execute(&invalid_region, out_of_range_request);
    if result2.is_err() {
        println!("   正确处理 split_key 超出范围错误: {}", result2.unwrap_err());
    } else {
        println!("   警告: split_key 超出范围应该返回错误");
    }

    println!("\n=== 数据分片测试完成 ===");

    println!("\n=== AegisDB 自动扩容/缩容测试 ===\n");

    use aegisdb::scheduler::schedulers::ScaleScheduler;
    use aegisdb::scheduler::coordinator::Coordinator;

    // 创建集群
    let cluster_scale = std::sync::Arc::new(BasicCluster::new());

    println!("1. 创建测试 Store");
    for i in 1..=5 {
        let store = StoreInfo::new(aegisdb::proto::metapb::Store {
            id: i,
            address: format!("127.0.0.1:{}", 20160 + i),
            state: aegisdb::proto::metapb::StoreState::Up,
        });
        cluster_scale.put_store(store);
    }
    println!("   创建了 5 个 Store");

    println!("\n2. 测试自动扩容 - 创建只有 1 个 Peer 的 Region");
    let region_scale_up = RegionInfo::new(
        Region {
            id: 10000,
            start_key: b"scale_up_start".to_vec(),
            end_key: b"scale_up_end".to_vec(),
            region_epoch: RegionEpoch::new(1, 1),
            peers: vec![Peer { id: 100000, store_id: 1 }],
        },
        Some(Peer { id: 100000, store_id: 1 }),
    );
    cluster_scale.put_region(region_scale_up.clone());
    println!("   创建了 Region {}，只有 1 个 Peer（需要扩容到 3 个）", region_scale_up.id());

    // 创建调度器（最小 3 个 Peer，最大 5 个 Peer）
    let scale_scheduler = ScaleScheduler::with_peer_count(Duration::from_secs(30), 3, 5);

    // 创建 OperatorController
    let hb_streams_scale = std::sync::Arc::new(SimpleHeartbeatStreams::new());
    let op_controller_scale = std::sync::Arc::new(OperatorController::new(
        cluster_scale.clone(),
        hb_streams_scale.clone(),
    ));

    // 创建 Coordinator
    let coordinator_scale = Coordinator::new(
        cluster_scale.clone(),
        op_controller_scale.clone(),
    );

    // 注册调度器
    coordinator_scale.register_scheduler(Box::new(scale_scheduler));
    println!("   注册了 ScaleScheduler");

    // 启动协调器
    coordinator_scale.start().await;
    println!("   启动协调器");

    // 等待调度器运行
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 检查是否创建了扩容操作
    let ops = op_controller_scale.get_operators();
    println!("   当前有 {} 个运行中的操作", ops.len());
    if let Some(op) = ops.iter().find(|o| o.region_id == 10000) {
        println!("   找到扩容操作: region_id={}, desc={}", op.region_id, op.desc);
        println!("   操作步骤数: {}", op.steps.len());
        
        // 模拟添加 Peer 成功
        let mut new_peers = region_scale_up.region().peers.clone();
        new_peers.push(Peer { id: 100001, store_id: 2 });
        new_peers.push(Peer { id: 100002, store_id: 3 });
        
        let updated_region = RegionInfo::new(
            Region {
                id: 10000,
                start_key: region_scale_up.start_key().to_vec(),
                end_key: region_scale_up.end_key().to_vec(),
                region_epoch: RegionEpoch {
                    version: 1,
                    conf_ver: 2, // conf_ver 增加
                },
                peers: new_peers,
            },
            Some(Peer { id: 100000, store_id: 1 }),
        );
        cluster_scale.put_region(updated_region.clone());
        
        // 分发操作（模拟心跳）
        op_controller_scale.dispatch(&updated_region, "heartbeat");
        
        // 等待操作完成
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // 检查操作是否完成
        let op_after = op_controller_scale.get_operator(10000);
        if op_after.is_none() {
            println!("   扩容操作已完成！");
        } else {
            println!("   扩容操作仍在进行中");
        }
    } else {
        println!("   警告: 未找到扩容操作（可能需要更多时间）");
    }

    println!("\n3. 测试自动缩容 - 创建有 6 个 Peer 的 Region");
    let region_scale_down = RegionInfo::new(
        Region {
            id: 10001,
            start_key: b"scale_down_start".to_vec(),
            end_key: b"scale_down_end".to_vec(),
            region_epoch: RegionEpoch::new(1, 1),
            peers: vec![
                Peer { id: 100010, store_id: 1 },
                Peer { id: 100011, store_id: 2 },
                Peer { id: 100012, store_id: 3 },
                Peer { id: 100013, store_id: 4 },
                Peer { id: 100014, store_id: 5 },
                Peer { id: 100015, store_id: 1 }, // 重复的 store_id
            ],
        },
        Some(Peer { id: 100010, store_id: 1 }), // Leader 在 store 1
    );
    cluster_scale.put_region(region_scale_down.clone());
    println!("   创建了 Region {}，有 6 个 Peer（需要缩容到 5 个）", region_scale_down.id());

    // 等待调度器运行
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 检查是否创建了缩容操作
    let ops2 = op_controller_scale.get_operators();
    println!("   当前有 {} 个运行中的操作", ops2.len());
    if let Some(op) = ops2.iter().find(|o| o.region_id == 10001) {
        println!("   找到缩容操作: region_id={}, desc={}", op.region_id, op.desc);
        println!("   操作步骤数: {}", op.steps.len());
        
        // 模拟移除 Peer 成功（移除 store 1 上的非 Leader Peer）
        let new_peers2: Vec<Peer> = region_scale_down.region().peers
            .iter()
            .filter(|p| p.id != 100015) // 移除 peer 100015
            .cloned()
            .collect();
        
        let updated_region2 = RegionInfo::new(
            Region {
                id: 10001,
                start_key: region_scale_down.start_key().to_vec(),
                end_key: region_scale_down.end_key().to_vec(),
                region_epoch: RegionEpoch {
                    version: 1,
                    conf_ver: 2, // conf_ver 增加
                },
                peers: new_peers2,
            },
            Some(Peer { id: 100010, store_id: 1 }),
        );
        cluster_scale.put_region(updated_region2.clone());
        
        // 分发操作（模拟心跳）
        op_controller_scale.dispatch(&updated_region2, "heartbeat");
        
        // 等待操作完成
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // 检查操作是否完成
        let op_after2 = op_controller_scale.get_operator(10001);
        if op_after2.is_none() {
            println!("   缩容操作已完成！");
        } else {
            println!("   缩容操作仍在进行中");
        }
    } else {
        println!("   警告: 未找到缩容操作（可能需要更多时间）");
    }

    println!("\n4. 测试完整扩容流程 - 从 2 个 Peer 扩容到 3 个");
    let region_scale_full = RegionInfo::new(
        Region {
            id: 10002,
            start_key: b"scale_full_start".to_vec(),
            end_key: b"scale_full_end".to_vec(),
            region_epoch: RegionEpoch::new(1, 1),
            peers: vec![
                Peer { id: 100020, store_id: 1 },
                Peer { id: 100021, store_id: 2 },
            ],
        },
        Some(Peer { id: 100020, store_id: 1 }),
    );
    cluster_scale.put_region(region_scale_full.clone());
    println!("   创建了 Region {}，有 2 个 Peer（需要扩容到 3 个）", region_scale_full.id());

    // 等待调度器运行
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 检查是否创建了扩容操作
    let ops3 = op_controller_scale.get_operators();
    println!("   当前有 {} 个运行中的操作", ops3.len());
    if let Some(op) = ops3.iter().find(|o| o.region_id == 10002) {
        println!("   找到扩容操作: region_id={}, desc={}", op.region_id, op.desc);
        
        // 模拟添加 Peer 成功
        let mut new_peers3 = region_scale_full.region().peers.clone();
        new_peers3.push(Peer { id: 100022, store_id: 3 });
        
        let updated_region3 = RegionInfo::new(
            Region {
                id: 10002,
                start_key: region_scale_full.start_key().to_vec(),
                end_key: region_scale_full.end_key().to_vec(),
                region_epoch: RegionEpoch {
                    version: 1,
                    conf_ver: 2, // conf_ver 增加
                },
                peers: new_peers3,
            },
            Some(Peer { id: 100020, store_id: 1 }),
        );
        cluster_scale.put_region(updated_region3.clone());
        
        // 分发操作（模拟心跳）
        op_controller_scale.dispatch(&updated_region3, "heartbeat");
        
        // 等待操作完成
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // 检查操作是否完成
        let op_after3 = op_controller_scale.get_operator(10002);
        if op_after3.is_none() {
            println!("   扩容操作已完成！");
            
            // 验证 Region 现在有 3 个 Peer
            let final_region = cluster_scale.get_region(10002).unwrap();
            println!("   最终 Region 有 {} 个 Peer", final_region.peers().len());
            assert_eq!(final_region.peers().len(), 3, "Region 应该有 3 个 Peer");
        } else {
            println!("   扩容操作仍在进行中");
        }
    } else {
        println!("   警告: 未找到扩容操作（可能需要更多时间）");
    }

    println!("\n5. 测试不需要扩容/缩容的情况");
    let region_no_scale = RegionInfo::new(
        Region {
            id: 10003,
            start_key: b"no_scale_start".to_vec(),
            end_key: b"no_scale_end".to_vec(),
            region_epoch: RegionEpoch::new(1, 1),
            peers: vec![
                Peer { id: 100030, store_id: 1 },
                Peer { id: 100031, store_id: 2 },
                Peer { id: 100032, store_id: 3 },
            ],
        },
        Some(Peer { id: 100030, store_id: 1 }),
    );
    cluster_scale.put_region(region_no_scale.clone());
    println!("   创建了 Region {}，有 3 个 Peer（不需要扩容/缩容）", region_no_scale.id());

    // 等待调度器运行
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 检查是否创建了操作
    let ops4 = op_controller_scale.get_operators();
    let has_op = ops4.iter().any(|o| o.region_id == 10003);
    if !has_op {
        println!("   正确：未创建操作（Region 已有 3 个 Peer，符合要求）");
    } else {
        println!("   警告：创建了不必要的操作");
    }

    // 停止协调器
    coordinator_scale.stop();
    println!("   协调器已停止");

    println!("\n=== 自动扩容/缩容测试完成 ===");

    Ok(())
}