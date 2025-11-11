use crate::proto::metapb::{Region, Peer, RegionEpoch};
use std::collections::HashSet;

/// Region 信息，包含元数据和运行时信息
#[derive(Clone, Debug)]
pub struct RegionInfo {
    region: Region,
    leader: Option<Peer>,
    pending_peers: Vec<Peer>,
    approximate_size: u64,
}

impl RegionInfo {
    pub fn new(region: Region, leader: Option<Peer>) -> Self {
        Self {
            region,
            leader,
            pending_peers: Vec::new(),
            approximate_size: 1, // 默认 1MB
        }
    }

    pub fn id(&self) -> u64 {
        self.region.id
    }

    pub fn start_key(&self) -> &[u8] {
        &self.region.start_key
    }

    pub fn end_key(&self) -> &[u8] {
        &self.region.end_key
    }

    pub fn peers(&self) -> &[Peer] {
        &self.region.peers
    }

    pub fn leader(&self) -> Option<&Peer> {
        self.leader.as_ref()
    }

    pub fn set_leader(&mut self, leader: Option<Peer>) {
        self.leader = leader;
    }

    pub fn pending_peers(&self) -> &[Peer] {
        &self.pending_peers
    }

    pub fn add_pending_peer(&mut self, peer: Peer) {
        self.pending_peers.push(peer);
    }

    pub fn remove_pending_peer(&mut self, peer_id: u64) {
        self.pending_peers.retain(|p| p.id != peer_id);
    }

    pub fn approximate_size(&self) -> u64 {
        self.approximate_size
    }

    pub fn set_approximate_size(&mut self, size: u64) {
        self.approximate_size = size;
    }

    pub fn epoch(&self) -> RegionEpoch {
        self.region.region_epoch.clone().unwrap_or_default()
    }

    pub fn region(&self) -> &Region {
        &self.region
    }

    pub fn get_store_ids(&self) -> HashSet<u64> {
        self.region.peers.iter().map(|p| p.store_id).collect()
    }

    pub fn get_leader_store_id(&self) -> Option<u64> {
        self.leader.as_ref().map(|p| p.store_id)
    }

    pub fn get_follower_store_ids(&self) -> Vec<u64> {
        let leader_store_id = self.get_leader_store_id();
        self.region
            .peers
            .iter()
            .filter_map(|p| {
                if Some(p.store_id) != leader_store_id {
                    Some(p.store_id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn has_pending_peers(&self) -> bool {
        !self.pending_peers.is_empty()
    }

    pub fn is_healthy(&self) -> bool {
        self.pending_peers.is_empty()
    }

    pub fn get_peer_by_store_id(&self, store_id: u64) -> Option<Peer> {
        self.peers().iter().find(|p| p.store_id == store_id).cloned()
    }
}