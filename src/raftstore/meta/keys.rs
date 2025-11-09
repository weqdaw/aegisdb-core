use bytes::{BytesMut, BufMut};

/// Region 元数据键的前缀
pub const REGION_META_PREFIX: u8 = 0x01;
pub const REGION_META_MIN_KEY: &[u8] = &[REGION_META_PREFIX];
pub const REGION_META_MAX_KEY: &[u8] = &[REGION_META_PREFIX + 1];

/// Raft 状态键的前缀
pub const RAFT_STATE_PREFIX: u8 = 0x02;
pub const RAFT_LOG_PREFIX: u8 = 0x03;
pub const APPLY_STATE_PREFIX: u8 = 0x04;

/// Region 状态键后缀
pub const REGION_STATE_SUFFIX: u8 = 0x01;

/// 编码 Region 状态键: [REGION_META_PREFIX][region_id][REGION_STATE_SUFFIX]
pub fn region_state_key(region_id: u64) -> Vec<u8> {
    let mut key = BytesMut::with_capacity(1 + 8 + 1);
    key.put_u8(REGION_META_PREFIX);
    key.put_u64(region_id);
    key.put_u8(REGION_STATE_SUFFIX);
    key.to_vec()
}

/// 解码 Region 元数据键
pub fn decode_region_meta_key(key: &[u8]) -> anyhow::Result<(u64, u8)> {
    if key.len() < 10 || key[0] != REGION_META_PREFIX {
        return Err(anyhow::anyhow!("invalid region meta key"));
    }
    let region_id = u64::from_be_bytes([
        key[1], key[2], key[3], key[4],
        key[5], key[6], key[7], key[8],
    ]);
    let suffix = key[9];
    Ok((region_id, suffix))
}

/// Raft 状态键: [RAFT_STATE_PREFIX][region_id]
pub fn raft_state_key(region_id: u64) -> Vec<u8> {
    let mut key = BytesMut::with_capacity(1 + 8);
    key.put_u8(RAFT_STATE_PREFIX);
    key.put_u64(region_id);
    key.to_vec()
}

/// Raft 日志键: [RAFT_LOG_PREFIX][region_id][index]
pub fn raft_log_key(region_id: u64, index: u64) -> Vec<u8> {
    let mut key = BytesMut::with_capacity(1 + 8 + 8);
    key.put_u8(RAFT_LOG_PREFIX);
    key.put_u64(region_id);
    key.put_u64(index);
    key.to_vec()
}

/// Apply 状态键: [APPLY_STATE_PREFIX][region_id]
pub fn apply_state_key(region_id: u64) -> Vec<u8> {
    let mut key = BytesMut::with_capacity(1 + 8);
    key.put_u8(APPLY_STATE_PREFIX);
    key.put_u64(region_id);
    key.to_vec()
}