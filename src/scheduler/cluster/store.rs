use crate::proto::metapb::{Store, StoreState};
use std::time::{Duration, SystemTime};

/// Store 信息，包含元数据和统计信息
#[derive(Clone, Debug)]
pub struct StoreInfo {
    meta: Store,
    leader_count: usize,
    region_count: usize,
    leader_size: u64,
    region_size: u64,
    pending_peer_count: usize,
    last_heartbeat: SystemTime,
    leader_weight: f64,
    region_weight: f64,
    blocked: bool,
}

impl StoreInfo {
    pub fn new(store: Store) -> Self {
        Self {
            meta: store,
            leader_count: 0,
            region_count: 0,
            leader_size: 0,
            region_size: 0,
            pending_peer_count: 0,
            last_heartbeat: SystemTime::now(),
            leader_weight: 1.0,
            region_weight: 1.0,
            blocked: false,
        }
    }

    pub fn id(&self) -> u64 {
        self.meta.id
    }

    pub fn address(&self) -> &str {
        &self.meta.address
    }

    pub fn state(&self) -> StoreState {
        self.meta.state
    }

    pub fn is_up(&self) -> bool {
        self.meta.state == StoreState::Up
    }

    pub fn leader_count(&self) -> usize {
        self.leader_count
    }

    pub fn region_count(&self) -> usize {
        self.region_count
    }

    pub fn leader_size(&self) -> u64 {
        self.leader_size
    }

    pub fn region_size(&self) -> u64 {
        self.region_size
    }

    pub fn pending_peer_count(&self) -> usize {
        self.pending_peer_count
    }

    pub fn is_blocked(&self) -> bool {
        self.blocked
    }

    pub fn set_blocked(&mut self, blocked: bool) {
        self.blocked = blocked;
    }

    pub fn update_leader_count(&mut self, count: usize) {
        self.leader_count = count;
    }

    pub fn update_region_count(&mut self, count: usize) {
        self.region_count = count;
    }

    pub fn update_leader_size(&mut self, size: u64) {
        self.leader_size = size;
    }

    pub fn update_region_size(&mut self, size: u64) {
        self.region_size = size;
    }

    pub fn update_pending_peer_count(&mut self, count: usize) {
        self.pending_peer_count = count;
    }

    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = SystemTime::now();
    }

    pub fn down_time(&self) -> Duration {
        SystemTime::now()
            .duration_since(self.last_heartbeat)
            .unwrap_or(Duration::ZERO)
    }

    pub fn meta(&self) -> &Store {
        &self.meta
    }
}