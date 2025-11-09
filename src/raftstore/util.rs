use crate::proto::metapb::{Region, Peer, RegionEpoch};
use crate::engine_util::util::exceed_end_key;

/// 在 Region 中查找 Peer
pub fn find_peer(region: &Region, store_id: u64) -> Option<&Peer> {
    region.peers.iter().find(|p| p.store_id == store_id)
}

/// 检查 key 是否在 Region 范围内
pub fn key_in_region(key: &[u8], region: &Region) -> bool {
    let start_key = &region.start_key;
    let end_key = &region.end_key;
    
    // 检查 start_key
    if !start_key.is_empty() && key < start_key.as_slice() {
        return false;
    }
    
    // 检查 end_key
    if !end_key.is_empty() && exceed_end_key(key, end_key) {
        return false;
    }
    
    true
}

/// 检查 Region Epoch 是否过期
pub fn is_epoch_stale(epoch: &RegionEpoch, other: &RegionEpoch) -> bool {
    epoch.version < other.version 
        || (epoch.version == other.version && epoch.conf_ver < other.conf_ver)
}

/// 安全复制字节
pub fn safe_copy(data: &[u8]) -> Vec<u8> {
    data.to_vec()
}

/// 无效 ID
pub const INVALID_ID: u64 = 0;