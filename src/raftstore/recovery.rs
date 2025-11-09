use crate::engine_util::Engines;
use crate::raftstore::meta::{get_region_local_state, decode_region_meta_key, REGION_META_PREFIX, PeerState};
use crate::proto::metapb::Region;
use anyhow::Result;

/// 从持久化存储恢复所有 Region
pub fn recover_regions(
    engines: &Engines,
    store_id: u64,
) -> Result<Vec<Region>> {
    let mut regions = Vec::new();
    let db = &engines.kv;
    let mut iter = db.raw_iterator();
    
    // Seek 到 REGION_META_PREFIX
    // 注意：由于使用了 key_with_cf，实际的键是 "_{REGION_META_PREFIX}..."
    // 所以我们需要 seek 到 "_" + REGION_META_PREFIX
    let seek_key = format!("_{}", REGION_META_PREFIX as char);
    iter.seek(seek_key.as_bytes());
    
    // 扫描所有 region_state_key
    while iter.valid() {
        if let Some(key_bytes) = iter.key() {
            // 检查是否超出范围（跳过非 region meta 键）
            if key_bytes.len() < 2 || key_bytes[0] != b'_' || key_bytes[1] != REGION_META_PREFIX {
                // 如果已经超出范围，停止扫描
                if key_bytes.len() > 0 && key_bytes[0] != b'_' {
                    break;
                }
                iter.next();
                continue;
            }
            
            // 提取实际的 key（去掉 "_" 前缀）
            let actual_key = &key_bytes[1..];
            
            // 尝试解码 region_meta_key
            if let Ok((region_id, _)) = decode_region_meta_key(actual_key) {
                // 读取 Region 状态
                if let Ok(Some(region_state)) = get_region_local_state(engines, region_id) {
                    // 检查 PeerState（如果是 Tombstone，跳过）
                    if region_state.state == PeerState::Tombstone {
                        iter.next();
                        continue;
                    }
                    
                    // 检查该 region 是否属于当前 store（检查 peers）
                    let region = &region_state.region;
                    let belongs_to_store = region.peers.iter().any(|p| p.store_id == store_id);
                    
                    if belongs_to_store {
                        regions.push(region.clone());
                    }
                }
            }
        }
        
        iter.next();
    }
    
    Ok(regions)
}

/// 验证恢复的 Region 数据完整性
pub fn validate_region(engines: &Engines, region_id: u64) -> Result<bool> {
    // 检查 region_state 是否存在
    let region_state = get_region_local_state(engines, region_id)?;
    if region_state.is_none() {
        return Ok(false);
    }
    
    // 检查 apply_state 是否存在（可选，新创建的 region 可能没有）
    // 这里只检查 region_state 存在即可
    
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine_util::engines::create_db;
    use crate::engine_util::Engines;
    use crate::raftstore::meta::{write_region_state, PeerState};
    use crate::engine_util::WriteBatch;
    use crate::proto::metapb::{Region, Peer, RegionEpoch};
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

    #[test]
    fn test_recover_regions() {
        let (engines, _temp_dir) = create_test_engines();
        
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
        
        // 写入 Region 状态
        let mut wb = WriteBatch::new();
        write_region_state(&mut wb, &region1, PeerState::Normal).unwrap();
        write_region_state(&mut wb, &region2, PeerState::Normal).unwrap();
        engines.write_kv(&wb).unwrap();
        
        // 恢复 Region
        let recovered = recover_regions(&engines, 1).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].id, 1);
        assert_eq!(recovered[1].id, 2);
    }

    #[test]
    fn test_recover_regions_with_tombstone() {
        let (engines, _temp_dir) = create_test_engines();
        
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
        
        // 写入 Region 状态（一个正常，一个 Tombstone）
        let mut wb = WriteBatch::new();
        write_region_state(&mut wb, &region1, PeerState::Normal).unwrap();
        write_region_state(&mut wb, &region2, PeerState::Tombstone).unwrap();
        engines.write_kv(&wb).unwrap();
        
        // 恢复 Region（应该只恢复 Normal 的）
        let recovered = recover_regions(&engines, 1).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].id, 1);
    }

    #[test]
    fn test_validate_region() {
        let (engines, _temp_dir) = create_test_engines();
        
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
        
        // 验证 Region
        assert!(validate_region(&engines, 1).unwrap());
        assert!(!validate_region(&engines, 999).unwrap());
    }
}

