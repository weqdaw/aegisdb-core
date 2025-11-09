use crate::proto::metapb::Region;
use crate::engine_util::Engines;
use crate::raftstore::meta::{
    get_apply_state, get_raft_local_state, read_raft_log, read_raft_log_entry,
};
use crate::raft::storage::Storage;
use crate::raft::types::{HardState, ConfState, Entry, Snapshot};
use async_trait::async_trait;
use std::sync::Arc;
use anyhow::Result;

/// Peer 存储
pub struct PeerStorage {
    engines: Arc<Engines>,
    region: Arc<parking_lot::RwLock<Region>>,
}

impl PeerStorage {
    pub fn new(engines: Arc<Engines>, region: Region) -> anyhow::Result<Self> {
        Ok(Self {
            engines,
            region: Arc::new(parking_lot::RwLock::new(region)),
        })
    }

    pub fn region(&self) -> Region {
        self.region.read().clone()
    }

    pub fn set_region(&self, region: Region) {
        *self.region.write() = region;
    }

    pub fn applied_index(&self) -> u64 {
        if let Ok(Some(apply_state)) = get_apply_state(&self.engines, self.region().id) {
            apply_state.applied_index
        } else {
            0
        }
    }

    pub fn truncated_index(&self) -> u64 {
        if let Ok(Some(apply_state)) = get_apply_state(&self.engines, self.region().id) {
            apply_state.truncated_state.index
        } else {
            0
        }
    }

    pub fn truncated_term(&self) -> u64 {
        if let Ok(Some(apply_state)) = get_apply_state(&self.engines, self.region().id) {
            apply_state.truncated_state.term
        } else {
            0
        }
    }

    pub fn is_initialized(&self) -> bool {
        self.applied_index() > 0
    }
}

#[async_trait]
impl Storage for PeerStorage {
    async fn initial_state(&self) -> Result<(HardState, ConfState)> {
        let region = self.region();
        let region_id = region.id;
        
        // 从 engines 读取 HardState
        let hard_state = if let Some(raft_state) = get_raft_local_state(&self.engines, region_id)? {
            // 转换 values::HardState 到 types::HardState
            HardState {
                term: raft_state.hard_state.term,
                vote: raft_state.hard_state.vote,
                commit: raft_state.hard_state.commit,
            }
        } else {
            HardState {
                term: 0,
                vote: 0,
                commit: 0,
            }
        };
        
        // 从 region 获取 ConfState
        let conf_state = ConfState {
            nodes: region.peers.iter().map(|p| p.id).collect(),
        };
        
        Ok((hard_state, conf_state))
    }

    async fn entries(&self, lo: u64, hi: u64) -> Result<Vec<Entry>> {
        let region_id = self.region().id;
        read_raft_log(&self.engines, region_id, lo, hi)
    }

    async fn term(&self, index: u64) -> Result<u64> {
        if index == 0 {
            return Ok(0);
        }
        
        let region_id = self.region().id;
        
        // 先检查 truncated_state
        if let Ok(Some(apply_state)) = get_apply_state(&self.engines, region_id) {
            if index < apply_state.truncated_state.index {
                return Err(anyhow::anyhow!(
                    "entry index {} is before truncated index {}",
                    index,
                    apply_state.truncated_state.index
                ));
            }
            if index == apply_state.truncated_state.index {
                return Ok(apply_state.truncated_state.term);
            }
        }
        
        // 从日志中读取
        match read_raft_log_entry(&self.engines, region_id, index)? {
            Some(entry) => Ok(entry.term),
            None => {
                // 如果日志不存在，检查是否是 last_index
                if let Some(raft_state) = get_raft_local_state(&self.engines, region_id)? {
                    if index == raft_state.last_index {
                        Ok(raft_state.last_term)
                    } else {
                        Err(anyhow::anyhow!("entry at index {} not found", index))
                    }
                } else {
                    Err(anyhow::anyhow!("entry at index {} not found", index))
                }
            }
        }
    }

    async fn last_index(&self) -> Result<u64> {
        let region_id = self.region().id;
        
        if let Some(raft_state) = get_raft_local_state(&self.engines, region_id)? {
            Ok(raft_state.last_index)
        } else {
            // 如果没有 raft_state，返回 applied_index
            Ok(self.applied_index())
        }
    }

    async fn first_index(&self) -> Result<u64> {
        Ok(self.truncated_index() + 1)
    }

    async fn snapshot(&self) -> Result<Snapshot> {
        let region = self.region();
        let applied_index = self.applied_index();
        let truncated_term = self.truncated_term();
        
        Ok(Snapshot {
            data: Vec::new(), // 快照数据由 RegionTaskHandler 生成
            metadata: crate::raft::types::SnapshotMetadata {
                conf_state: ConfState {
                    nodes: region.peers.iter().map(|p| p.id).collect(),
                },
                index: applied_index,
                term: truncated_term,
            },
        })
    }
}