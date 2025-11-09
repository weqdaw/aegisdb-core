use crate::proto::metapb::{Region, Peer as MetaPeer};
use crate::raftstore::peer_storage::PeerStorage;
use crate::raft::RawNode;
use crate::config::Config;
use crate::engine_util::Engines;
use std::sync::Arc;

/// Peer 结构
pub struct Peer {
    pub meta: MetaPeer,
    pub region_id: u64,
    pub raft_group: Arc<RawNode>,
    pub peer_storage: Arc<PeerStorage>,
    pub tag: String,
    
    // Split 相关
    pub size_diff_hint: u64,
    pub approximate_size: Option<u64>,
    
    // 停止标志
    pub stopped: bool,
}

impl Peer {
    pub async fn new(
        store_id: u64,
        cfg: &Config,
        engines: Arc<Engines>,
        region: Region,
        meta_peer: MetaPeer,
    ) -> anyhow::Result<Self> {
        let tag = format!("[region {}] {}", region.id, meta_peer.id);
        
        let peer_storage = PeerStorage::new(engines.clone(), region.clone())?;
        let applied_index = peer_storage.applied_index();
        let peer_storage_arc = Arc::new(peer_storage);
        
        // 创建 Raft 配置
        // 注意：由于 Storage trait 需要 &self，而我们需要共享所有权
        // 这里使用一个包装器来让 Arc<PeerStorage> 实现 Storage
        struct StorageWrapper(Arc<PeerStorage>);
        
        #[async_trait::async_trait]
        impl crate::raft::storage::Storage for StorageWrapper {
            async fn initial_state(&self) -> anyhow::Result<(crate::raft::types::HardState, crate::raft::types::ConfState)> {
                self.0.initial_state().await
            }
            async fn entries(&self, lo: u64, hi: u64) -> anyhow::Result<Vec<crate::raft::types::Entry>> {
                self.0.entries(lo, hi).await
            }
            async fn term(&self, index: u64) -> anyhow::Result<u64> {
                self.0.term(index).await
            }
            async fn last_index(&self) -> anyhow::Result<u64> {
                self.0.last_index().await
            }
            async fn first_index(&self) -> anyhow::Result<u64> {
                self.0.first_index().await
            }
            async fn snapshot(&self) -> anyhow::Result<crate::raft::types::Snapshot> {
                self.0.snapshot().await
            }
        }
        
        let storage: Box<dyn crate::raft::storage::Storage> = Box::new(StorageWrapper(peer_storage_arc.clone()));
        
        let raft_config = crate::raft::RaftConfig {
            id: meta_peer.id,
            peers: region.peers.iter().map(|p| p.id).collect(),
            election_tick: cfg.raft_election_timeout_ticks as usize,
            heartbeat_tick: cfg.raft_heartbeat_ticks as usize,
            storage,
            applied: applied_index,
        };
        
        let mut raw_node = RawNode::new(raft_config).await?;
        
        // 如果只有一个 peer 且是当前 store，直接成为 leader
        if region.peers.len() == 1 && region.peers[0].store_id == store_id {
            raw_node.campaign()?;
        }
        
        let raft_group = Arc::new(raw_node);
        
        Ok(Self {
            meta: meta_peer,
            region_id: region.id,
            raft_group,
            peer_storage: peer_storage_arc,
            tag,
            size_diff_hint: 0,
            approximate_size: None,
            stopped: false,
        })
    }

    pub fn region(&self) -> Region {
        self.peer_storage.region()
    }

    pub fn set_region(&self, region: Region) {
        self.peer_storage.set_region(region);
    }

    pub fn is_leader(&self) -> bool {
        self.raft_group.raft.state == crate::raft::StateType::Leader
    }

    pub fn leader_id(&self) -> u64 {
        self.raft_group.raft.lead
    }

    pub fn term(&self) -> u64 {
        self.raft_group.raft.term
    }
}