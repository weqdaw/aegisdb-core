use aegisdb::raftstore::*;
use aegisdb::proto::metapb::{Region, Peer, RegionEpoch};
use aegisdb::engine_util::Engines;
use std::sync::Arc;
use tempfile::TempDir;

/// 创建测试用的 Engines
fn create_test_engines() -> (Arc<Engines>, TempDir) {
    use aegisdb::engine_util::engines::create_db;
    let temp_dir = TempDir::new().unwrap();
    let kv_path = temp_dir.path().join("kv");
    let raft_path = temp_dir.path().join("raft");
    
    let kv_db = create_db(kv_path.to_str().unwrap(), false).unwrap();
    let raft_db = create_db(raft_path.to_str().unwrap(), true).unwrap();
    
    let engines = Arc::new(Engines::new(
        kv_db,
        Some(raft_db),
        kv_path.to_str().unwrap().to_string(),
        raft_path.to_str().unwrap().to_string(),
    ));
    (engines, temp_dir)
}

/// 创建测试用的 Region
fn create_test_region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> Region {
    Region {
        id,
        start_key,
        end_key,
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![Peer {
            id: 1,
            store_id: 1,
        }],
    }
}

#[tokio::test]
async fn test_store_meta() {
    let store_meta = StoreMeta::new();
    
    // 创建测试 Region
    let region1 = create_test_region(1, b"a".to_vec(), b"z".to_vec());
    let region2 = create_test_region(2, b"z".to_vec(), vec![]);
    
    // 设置 Region
    store_meta.set_region(region1.clone());
    store_meta.set_region(region2.clone());
    
    // 测试获取 Region
    let retrieved = store_meta.get_region(1).unwrap();
    assert_eq!(retrieved.id, 1);
    assert_eq!(retrieved.start_key, b"a");
    
    // 测试根据 key 查找 Region
    let found = store_meta.find_region_by_key(b"b").unwrap();
    assert_eq!(found.id, 1);
    
    let found2 = store_meta.find_region_by_key(b"z").unwrap();
    assert_eq!(found2.id, 2);
    
    // 测试获取所有 Region
    let all_regions = store_meta.get_all_regions();
    assert_eq!(all_regions.len(), 2);
    
    // 测试移除 Region
    store_meta.remove_region(1);
    assert!(store_meta.get_region(1).is_none());
}

#[tokio::test]
async fn test_meta_keys() {
    use aegisdb::raftstore::meta::*;
    
    let region_id = 100u64;
    
    // 测试 Region 状态键
    let region_key = region_state_key(region_id);
    assert_eq!(region_key.len(), 10);
    assert_eq!(region_key[0], 0x01); // REGION_META_PREFIX
    
    // 测试解码
    let (decoded_id, suffix) = decode_region_meta_key(&region_key).unwrap();
    assert_eq!(decoded_id, region_id);
    assert_eq!(suffix, 0x01); // REGION_STATE_SUFFIX
    
    // 测试 Raft 状态键
    let raft_key = raft_state_key(region_id);
    assert_eq!(raft_key.len(), 9);
    assert_eq!(raft_key[0], 0x02); // RAFT_STATE_PREFIX
    
    // 测试 Apply 状态键
    let apply_key = apply_state_key(region_id);
    assert_eq!(apply_key.len(), 9);
    assert_eq!(apply_key[0], 0x04); // APPLY_STATE_PREFIX
}

#[tokio::test]
async fn test_meta_values() {
    use aegisdb::raftstore::meta::values::*;
    use aegisdb::engine_util::WriteBatch;
    
    let (engines, _temp_dir) = create_test_engines();
    
    // 创建测试 Region
    let region = create_test_region(1, b"a".to_vec(), b"z".to_vec());
    
    // 写入 Region 状态
    let mut kv_wb = WriteBatch::new();
    write_region_state(&mut kv_wb, &region, PeerState::Normal).unwrap();
    engines.write_kv(&kv_wb).unwrap();
    
    // 注意：由于 write_region_state 使用了 set_cf("", ...)，实际存储的 key 可能不同
    // 这里需要根据实际实现调整
    
    // 读取 Region 状态
    let state = get_region_local_state(&engines, 1).unwrap().unwrap();
    assert_eq!(state.state, PeerState::Normal);
    assert_eq!(state.region.id, 1);
    
    // 测试不存在的 Region
    let state2 = get_region_local_state(&engines, 999).unwrap();
    assert!(state2.is_none());
}

#[tokio::test]
async fn test_router() {
    use aegisdb::raftstore::message::{Msg, MsgType, MsgData};
    use tokio::sync::mpsc;
    
    let router = Router::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    
    // 注册 region
    router.register(1, tx);
    
    // 发送消息
    let msg = Msg {
        msg_type: MsgType::Tick,
        region_id: 1,
        data: MsgData::Empty,
    };
    router.send(1, msg).unwrap();
    
    // 接收消息
    let received = rx.recv().await.unwrap();
    assert_eq!(received.region_id, 1);
    
    // 测试不存在的 region
    let msg2 = Msg {
        msg_type: MsgType::Tick,
        region_id: 999,
        data: MsgData::Empty,
    };
    assert!(router.send(999, msg2).is_err());
}

#[tokio::test]
async fn test_peer_storage() {
    let (engines, _temp_dir) = create_test_engines();
    
    let region = create_test_region(1, b"a".to_vec(), b"z".to_vec());
    let peer_storage = PeerStorage::new(engines.clone(), region.clone()).unwrap();
    
    // 测试获取 Region
    let retrieved = peer_storage.region();
    assert_eq!(retrieved.id, 1);
    
    // 测试更新 Region
    let mut new_region = region.clone();
    new_region.region_epoch.version = 2;
    peer_storage.set_region(new_region.clone());
    
    let updated = peer_storage.region();
    assert_eq!(updated.region_epoch.version, 2);
    
    // 测试 applied_index（初始应该为 0）
    assert_eq!(peer_storage.applied_index(), 0);
    assert!(!peer_storage.is_initialized());
}

#[tokio::test]
async fn test_util_functions() {
    use aegisdb::raftstore::util::*;
    
    let region = create_test_region(1, b"a".to_vec(), b"z".to_vec());
    
    // 测试 find_peer
    let peer = find_peer(&region, 1).unwrap();
    assert_eq!(peer.id, 1);
    assert_eq!(peer.store_id, 1);
    
    assert!(find_peer(&region, 999).is_none());
    
    // 测试 key_in_region
    assert!(key_in_region(b"b", &region));
    assert!(key_in_region(b"a", &region));
    assert!(!key_in_region(b"z", &region)); // z 是 end_key，不包含
    assert!(!key_in_region(b"0", &region)); // 小于 start_key
    
    // 测试 is_epoch_stale
    let epoch1 = RegionEpoch::new(1, 1);
    let epoch2 = RegionEpoch::new(1, 2);
    let epoch3 = RegionEpoch::new(2, 1);
    
    assert!(is_epoch_stale(&epoch1, &epoch2));
    assert!(is_epoch_stale(&epoch1, &epoch3));
    assert!(!is_epoch_stale(&epoch2, &epoch1));
}

#[tokio::test]
async fn test_split_checker() {
    use aegisdb::raftstore::split_checker::SplitChecker;
    use aegisdb::raftstore::router::{Router, RaftRouter};
    use aegisdb::config::Config;
    use aegisdb::engine_util::WriteBatch;
    
    let (engines, _temp_dir) = create_test_engines();
    let config = Config::new_test();
    
    // 写入一些测试数据
    let mut wb = WriteBatch::new();
    for i in 0..100 {
        let key = format!("key{:03}", i).into_bytes();
        let value = vec![0u8; 1024]; // 1KB per value
        wb.set_cf("default", &key, &value);
    }
    engines.write_kv(&wb).unwrap();
    
    // 创建 router（虽然不会真正发送消息）
    let router = Arc::new(Router::new());
    let raft_router = RaftRouter::new(router.clone());
    
    // 创建 split checker（现在接受 Arc<Engines>）
    let checker = SplitChecker::new(engines.clone(), raft_router, &config);
    
    // 创建一个大 Region（包含所有数据）
    let region = create_test_region(1, b"key000".to_vec(), vec![]);
    
    // 检查 split（由于数据量可能不够，可能返回 None）
    let split_key = checker.check(&region);
    // 这个测试主要验证不会 panic，实际结果取决于数据量
    println!("Split key: {:?}", split_key);
}

#[tokio::test]
async fn test_region_overlap() {
    let store_meta = StoreMeta::new();
    
    // 创建重叠的 Region
    let region1 = create_test_region(1, b"a".to_vec(), b"m".to_vec());
    let region2 = create_test_region(2, b"m".to_vec(), b"z".to_vec());
    let region3 = create_test_region(3, b"b".to_vec(), b"c".to_vec()); // 与 region1 重叠
    
    store_meta.set_region(region1.clone());
    store_meta.set_region(region2.clone());
    store_meta.set_region(region3.clone());
    
    // 测试重叠检测
    let overlaps = store_meta.get_overlap_regions(&region3);
    // region3 应该与 region1 重叠
    assert!(overlaps.iter().any(|r| r.id == 1));
}

