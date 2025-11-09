/// 持久化测试
/// 测试数据持久化、崩溃恢复和一致性
use aegisdb::engine_util::{
    Engines, WriteBatch,
    WalManager, WalEntryType, verify_data_consistency, check_wal_integrity,
};
use aegisdb::engine_util::engines::create_db;
use aegisdb::raftstore::meta::{write_region_state, PeerState};
use aegisdb::proto::metapb::{Region, Peer, RegionEpoch};
use tempfile::TempDir;

fn create_test_engines() -> (Engines, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let kv_path = temp_dir.path().join("kv");
    let raft_path = temp_dir.path().join("raft");
    
    std::fs::create_dir_all(&kv_path).unwrap();
    std::fs::create_dir_all(&raft_path).unwrap();
    
    let kv_db = create_db(kv_path.to_str().unwrap(), false).unwrap();
    let raft_db = create_db(raft_path.to_str().unwrap(), true).unwrap();
    
    let engines = Engines::new(
        kv_db,
        Some(raft_db),
        kv_path.to_str().unwrap().to_string(),
        raft_path.to_str().unwrap().to_string(),
    );
    
    (engines, temp_dir)
}

/// 测试基本持久化功能
#[test]
fn test_basic_persistence() {
    let (engines, _temp_dir) = create_test_engines();
    
    // 写入一些数据
    let mut wb = WriteBatch::new();
    wb.set_cf("default", b"key1", b"value1");
    wb.set_cf("default", b"key2", b"value2");
    engines.write_kv(&wb).unwrap();
    
    // 验证数据已写入
    assert_eq!(engines.kv.get(b"default_key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(engines.kv.get(b"default_key2").unwrap(), Some(b"value2".to_vec()));
    
    // 获取路径
    let kv_path = engines.kv_path.clone();
    let raft_path = engines.raft_path.clone();
    
    // 关闭数据库（模拟崩溃）
    drop(engines);
    
    // 重新打开数据库
    let kv_db = create_db(&kv_path, false).unwrap();
    let raft_db = create_db(&raft_path, true).unwrap();
    let engines2 = Engines::new(
        kv_db,
        Some(raft_db),
        kv_path.clone(),
        raft_path.clone(),
    );
    
    // 验证数据仍然存在（持久化成功）
    assert_eq!(engines2.kv.get(b"default_key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(engines2.kv.get(b"default_key2").unwrap(), Some(b"value2".to_vec()));
}

/// 测试崩溃恢复
#[test]
fn test_crash_recovery() {
    let (engines, _temp_dir) = create_test_engines();
    
    // 写入一些数据
    let mut wb = WriteBatch::new();
    wb.set_cf("default", b"key1", b"value1");
    wb.set_cf("default", b"key2", b"value2");
    wb.set_cf("default", b"key3", b"value3");
    engines.write_kv(&wb).unwrap();
    
    // 强制刷新 WAL
    engines.flush_wal().unwrap();
    
    // 获取路径
    let kv_path = engines.kv_path.clone();
    let raft_path = engines.raft_path.clone();
    
    // 关闭数据库（模拟崩溃）
    drop(engines);
    
    // 重新打开数据库（模拟恢复）
    let kv_db = create_db(&kv_path, false).unwrap();
    let raft_db = create_db(&raft_path, true).unwrap();
    let engines2 = Engines::new(
        kv_db,
        Some(raft_db),
        kv_path.clone(),
        raft_path.clone(),
    );
    
    // 验证所有数据都已恢复
    assert_eq!(engines2.kv.get(b"default_key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(engines2.kv.get(b"default_key2").unwrap(), Some(b"value2".to_vec()));
    assert_eq!(engines2.kv.get(b"default_key3").unwrap(), Some(b"value3".to_vec()));
}

/// 测试 Region 状态持久化
#[test]
fn test_region_state_persistence() {
    let (engines, _temp_dir) = create_test_engines();
    
    // 创建测试 Region
    let region = Region {
        id: 1,
        start_key: b"a".to_vec(),
        end_key: b"m".to_vec(),
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![Peer {
            id: 1,
            store_id: 1,
        }],
    };
    
    // 写入 Region 状态
    let mut wb = WriteBatch::new();
    write_region_state(&mut wb, &region, PeerState::Normal).unwrap();
    engines.write_kv(&wb).unwrap();
    
    // 强制刷新 WAL
    engines.flush_wal().unwrap();
    
    // 获取路径
    let kv_path = engines.kv_path.clone();
    let raft_path = engines.raft_path.clone();
    
    // 关闭数据库（模拟崩溃）
    drop(engines);
    
    // 重新打开数据库（模拟恢复）
    let kv_db = create_db(&kv_path, false).unwrap();
    let raft_db = create_db(&raft_path, true).unwrap();
    let engines2 = Engines::new(
        kv_db,
        Some(raft_db),
        kv_path.clone(),
        raft_path.clone(),
    );
    
    // 验证 Region 状态已恢复
    use aegisdb::raftstore::meta::get_region_local_state;
    let recovered_state = get_region_local_state(&engines2, 1).unwrap();
    assert!(recovered_state.is_some());
    let state = recovered_state.unwrap();
    assert_eq!(state.region.id, 1);
    assert_eq!(state.state, PeerState::Normal);
}

/// 测试数据一致性
#[test]
fn test_data_consistency() {
    let (engines, _temp_dir) = create_test_engines();
    
    // 写入一些数据
    let mut wb = WriteBatch::new();
    wb.set_cf("default", b"key1", b"value1");
    wb.set_cf("default", b"key2", b"value2");
    engines.write_kv(&wb).unwrap();
    
    // 验证数据一致性
    assert!(verify_data_consistency(&engines).unwrap());
    
    // 强制刷新 WAL
    engines.flush_wal().unwrap();
    
    // 再次验证数据一致性
    assert!(verify_data_consistency(&engines).unwrap());
}

/// 测试 WAL 完整性
#[test]
fn test_wal_integrity() {
    let (engines, _temp_dir) = create_test_engines();
    
    // 写入一些数据
    let mut wb = WriteBatch::new();
    wb.set_cf("default", b"key1", b"value1");
    engines.write_kv(&wb).unwrap();
    
    // 检查 WAL 完整性
    assert!(check_wal_integrity(&engines).unwrap());
    
    // 强制刷新 WAL
    engines.flush_wal().unwrap();
    
    // 再次检查 WAL 完整性
    assert!(check_wal_integrity(&engines).unwrap());
}

/// 测试 WAL 管理器
#[test]
fn test_wal_manager() {
    let (engines, _temp_dir) = create_test_engines();
    let mut wal = WalManager::new();
    
    // 添加一些 WAL 条目
    wal.add_entry(WalEntryType::KvWrite, b"data1".to_vec());
    wal.add_entry(WalEntryType::RaftLog, b"data2".to_vec());
    wal.add_entry(WalEntryType::RegionState, b"data3".to_vec());
    
    assert_eq!(wal.pending_count(), 3);
    
    // 刷新 WAL
    wal.flush(&engines).unwrap();
    
    assert_eq!(wal.pending_count(), 0);
}

/// 测试批量写入持久化
#[test]
fn test_batch_write_persistence() {
    let (engines, _temp_dir) = create_test_engines();
    
    // 批量写入大量数据
    let mut wb = WriteBatch::new();
    for i in 0..100 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        wb.set_cf("default", key.as_bytes(), value.as_bytes());
    }
    engines.write_kv(&wb).unwrap();
    
    // 强制刷新 WAL
    engines.flush_wal().unwrap();
    
    // 获取路径
    let kv_path = engines.kv_path.clone();
    let raft_path = engines.raft_path.clone();
    
    // 关闭数据库（模拟崩溃）
    drop(engines);
    
    // 重新打开数据库（模拟恢复）
    let kv_db = create_db(&kv_path, false).unwrap();
    let raft_db = create_db(&raft_path, true).unwrap();
    let engines2 = Engines::new(
        kv_db,
        Some(raft_db),
        kv_path.clone(),
        raft_path.clone(),
    );
    
    // 验证所有数据都已恢复
    for i in 0..100 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        let encoded_key = format!("default_{}", key);
        assert_eq!(
            engines2.kv.get(encoded_key.as_bytes()).unwrap(),
            Some(value.as_bytes().to_vec())
        );
    }
}

/// 测试写入和删除的持久化
#[test]
fn test_write_delete_persistence() {
    let (engines, _temp_dir) = create_test_engines();
    
    // 写入数据
    let mut wb = WriteBatch::new();
    wb.set_cf("default", b"key1", b"value1");
    wb.set_cf("default", b"key2", b"value2");
    engines.write_kv(&wb).unwrap();
    
    // 删除一个 key
    let mut wb2 = WriteBatch::new();
    wb2.delete_cf("default", b"key1");
    engines.write_kv(&wb2).unwrap();
    
    // 强制刷新 WAL
    engines.flush_wal().unwrap();
    
    // 获取路径
    let kv_path = engines.kv_path.clone();
    let raft_path = engines.raft_path.clone();
    
    // 关闭数据库（模拟崩溃）
    drop(engines);
    
    // 重新打开数据库（模拟恢复）
    let kv_db = create_db(&kv_path, false).unwrap();
    let raft_db = create_db(&raft_path, true).unwrap();
    let engines2 = Engines::new(
        kv_db,
        Some(raft_db),
        kv_path.clone(),
        raft_path.clone(),
    );
    
    // 验证删除已持久化
    assert_eq!(engines2.kv.get(b"default_key1").unwrap(), None);
    assert_eq!(engines2.kv.get(b"default_key2").unwrap(), Some(b"value2".to_vec()));
}

/// 测试异步写入的持久化
#[test]
fn test_async_write_persistence() {
    let (engines, _temp_dir) = create_test_engines();
    
    // 使用异步写入
    let mut wb = WriteBatch::new();
    wb.set_cf("default", b"key1", b"value1");
    engines.write_kv_async(&wb).unwrap();
    
    // 强制刷新 WAL 以确保数据持久化
    engines.flush_wal().unwrap();
    
    // 获取路径
    let kv_path = engines.kv_path.clone();
    let raft_path = engines.raft_path.clone();
    
    // 关闭数据库（模拟崩溃）
    drop(engines);
    
    // 重新打开数据库（模拟恢复）
    let kv_db = create_db(&kv_path, false).unwrap();
    let raft_db = create_db(&raft_path, true).unwrap();
    let engines2 = Engines::new(
        kv_db,
        Some(raft_db),
        kv_path.clone(),
        raft_path.clone(),
    );
    
    // 验证数据已持久化
    assert_eq!(engines2.kv.get(b"default_key1").unwrap(), Some(b"value1".to_vec()));
}

/// 测试 Raft 日志持久化
#[test]
fn test_raft_log_persistence() {
    let (engines, _temp_dir) = create_test_engines();
    
    // 写入 Raft 日志
    let mut wb = WriteBatch::new();
    wb.set_cf("default", b"raft_log_1", b"log_data_1");
    wb.set_cf("default", b"raft_log_2", b"log_data_2");
    engines.write_raft(&wb).unwrap();
    
    // 强制刷新 WAL
    engines.flush_wal().unwrap();
    
    // 获取路径
    let kv_path = engines.kv_path.clone();
    let raft_path = engines.raft_path.clone();
    
    // 关闭数据库（模拟崩溃）
    drop(engines);
    
    // 重新打开数据库（模拟恢复）
    let kv_db = create_db(&kv_path, false).unwrap();
    let raft_db = create_db(&raft_path, true).unwrap();
    let engines2 = Engines::new(
        kv_db,
        Some(raft_db),
        kv_path.clone(),
        raft_path.clone(),
    );
    
    // 验证 Raft 日志已恢复
    if let Some(ref raft_db) = engines2.raft {
        assert_eq!(raft_db.get(b"default_raft_log_1").unwrap(), Some(b"log_data_1".to_vec()));
        assert_eq!(raft_db.get(b"default_raft_log_2").unwrap(), Some(b"log_data_2".to_vec()));
    }
}

