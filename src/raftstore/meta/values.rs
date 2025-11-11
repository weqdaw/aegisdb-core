use crate::proto::metapb::Region;
use crate::raftstore::meta::{region_state_key, raft_state_key, apply_state_key, raft_log_key};
use crate::engine_util::{Engines, WriteBatch, get_cf};
use crate::raft::types::Entry;
use serde::{Serialize, Deserialize};
use anyhow::Result;
use prost::Message;

/// Peer 状态
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerState {
    Normal = 0,
    Tombstone = 1,
}

/// Region 本地状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionLocalState {
    pub state: PeerState,
    pub region_bytes: Vec<u8>,
}

/// Raft 本地状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftLocalState {
    pub hard_state: HardState,
    pub last_index: u64,
    pub last_term: u64,
}

/// Hard State
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    pub term: u64,
    pub vote: u64,
    pub commit: u64,
}

/// Raft Apply 状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftApplyState {
    pub applied_index: u64,
    pub truncated_state: RaftTruncatedState,
}

/// Raft 截断状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftTruncatedState {
    pub index: u64,
    pub term: u64,
}

impl RegionLocalState {
    pub fn new(region: Region, state: PeerState) -> Self {
        let mut buf = Vec::new();
        region.encode(&mut buf).unwrap();
        Self { state, region_bytes: buf }
    }
}

/// 写入 Region 状态
pub fn write_region_state(
    kv_wb: &mut WriteBatch,
    region: &Region,
    state: PeerState,
) -> Result<()> {
    let mut buf = Vec::new();
    region.encode(&mut buf)?;
    let region_state = RegionLocalState { state, region_bytes: buf };
    let key = region_state_key(region.id);
    let value = bincode::serialize(&region_state)?;
    // 直接使用原始 key，因为这是元数据，不需要 CF 前缀
    kv_wb.set_cf("", &key, &value);
    Ok(())
}

/// 读取 Region 状态
pub fn get_region_local_state(
    engines: &Engines,
    region_id: u64,
) -> Result<Option<RegionLocalState>> {
    let key = region_state_key(region_id);
    // 使用 get_cf("", ...) 因为 write_region_state 使用 set_cf("", ...)
    match get_cf(&engines.kv, "", &key)? {
        Some(value) => {
            let state: RegionLocalState = bincode::deserialize(&value)?;
            Ok(Some(state))
        }
        None => Ok(None),
    }
}

/// 读取 Raft 本地状态
pub fn get_raft_local_state(
    engines: &Engines,
    region_id: u64,
) -> Result<Option<RaftLocalState>> {
    let key = raft_state_key(region_id);
    if let Some(ref raft_db) = engines.raft {
        match raft_db.get(&key)? {
            Some(value) => {
                let state: RaftLocalState = bincode::deserialize(&value)?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    } else {
        Ok(None)
    }
}

/// 读取 Apply 状态
pub fn get_apply_state(
    engines: &Engines,
    region_id: u64,
) -> Result<Option<RaftApplyState>> {
    let key = apply_state_key(region_id);
    // 使用 get_cf("", ...) 因为 apply state 也使用空 CF
    match get_cf(&engines.kv, "", &key)? {
        Some(value) => {
            let state: RaftApplyState = bincode::deserialize(&value)?;
            Ok(Some(state))
        }
        None => Ok(None),
    }
}

/// 写入 Raft 状态
pub fn write_raft_state(
    engines: &Engines,
    region_id: u64,
    state: &RaftLocalState,
) -> Result<()> {
    let key = raft_state_key(region_id);
    let value = bincode::serialize(state)?;
    if let Some(ref raft_db) = engines.raft {
        raft_db.put(&key, &value)?;
    }
    Ok(())
}

/// 写入 Apply 状态
pub fn write_apply_state(
    kv_wb: &mut WriteBatch,
    region_id: u64,
    state: &RaftApplyState,
) -> Result<()> {
    let key = apply_state_key(region_id);
    let value = bincode::serialize(state)?;
    kv_wb.set_cf("", &key, &value);
    Ok(())
}

/// 写入 Raft 日志条目
pub fn write_raft_log(
    engines: &Engines,
    region_id: u64,
    entries: &[Entry],
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    
    if let Some(ref raft_db) = engines.raft {
        for entry in entries {
            let key = raft_log_key(region_id, entry.index);
            let value = bincode::serialize(entry)?;
            raft_db.put(&key, &value)?;
        }
    }
    Ok(())
}

/// 读取 Raft 日志条目
pub fn read_raft_log(
    engines: &Engines,
    region_id: u64,
    lo: u64,
    hi: u64,
) -> Result<Vec<Entry>> {
    if lo >= hi {
        return Ok(Vec::new());
    }
    
    let mut entries = Vec::new();
    if let Some(ref raft_db) = engines.raft {
        for index in lo..hi {
            let key = raft_log_key(region_id, index);
            match raft_db.get(&key)? {
                Some(value) => {
                    let entry: Entry = bincode::deserialize(&value)?;
                    entries.push(entry);
                }
                None => {
                    // 如果某个索引的日志不存在，停止读取
                    break;
                }
            }
        }
    }
    Ok(entries)
}

/// 读取单个 Raft 日志条目
pub fn read_raft_log_entry(
    engines: &Engines,
    region_id: u64,
    index: u64,
) -> Result<Option<Entry>> {
    if let Some(ref raft_db) = engines.raft {
        let key = raft_log_key(region_id, index);
        match raft_db.get(&key)? {
            Some(value) => {
                let entry: Entry = bincode::deserialize(&value)?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    } else {
        Ok(None)
    }
}

/// 删除 Raft 日志条目（用于日志压缩）
pub fn delete_raft_log(
    engines: &Engines,
    region_id: u64,
    lo: u64,
    hi: u64,
) -> Result<()> {
    if lo >= hi {
        return Ok(());
    }
    
    if let Some(ref raft_db) = engines.raft {
        use rocksdb::WriteBatch;
        let mut wb = WriteBatch::default();
        for index in lo..hi {
            let key = raft_log_key(region_id, index);
            wb.delete(&key);
        }
        raft_db.write(wb)?;
    }
    Ok(())
}