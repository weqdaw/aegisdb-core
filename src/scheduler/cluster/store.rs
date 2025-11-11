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
    // extended metrics
    healthy: bool,
    avg_resp_ms: u64,
    error_count: u64,
    mem_total: u64,
    mem_used: u64,
    disk_total: u64,
    disk_used: u64,
    network_state: String,
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
            healthy: true,
            avg_resp_ms: 0,
            error_count: 0,
            mem_total: 0,
            mem_used: 0,
            disk_total: 0,
            disk_used: 0,
            network_state: "normal".to_string(),
        }
    }

    pub fn id(&self) -> u64 {
        self.meta.id
    }

    pub fn address(&self) -> &str {
        &self.meta.address
    }

    pub fn state(&self) -> StoreState {
        StoreState::from_i32(self.meta.state).unwrap_or(StoreState::Up)
    }

    pub fn is_up(&self) -> bool {
        self.state() == StoreState::Up
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

    // getters
    pub fn healthy(&self) -> bool {
        self.healthy
    }
    pub fn avg_resp_ms(&self) -> u64 {
        self.avg_resp_ms
    }
    pub fn error_count(&self) -> u64 {
        self.error_count
    }
    pub fn mem_total(&self) -> u64 {
        self.mem_total
    }
    pub fn mem_used(&self) -> u64 {
        self.mem_used
    }
    pub fn disk_total(&self) -> u64 {
        self.disk_total
    }
    pub fn disk_used(&self) -> u64 {
        self.disk_used
    }
    pub fn network_state(&self) -> &str {
        &self.network_state
    }

    // setters
    pub fn update_healthy(&mut self, healthy: bool) {
        self.healthy = healthy;
    }
    pub fn update_avg_resp_ms(&mut self, ms: u64) {
        self.avg_resp_ms = ms;
    }
    pub fn update_error_count(&mut self, c: u64) {
        self.error_count = c;
    }
    pub fn update_mem(&mut self, total: u64, used: u64) {
        self.mem_total = total;
        self.mem_used = used;
    }
    pub fn update_disk(&mut self, total: u64, used: u64) {
        self.disk_total = total;
        self.disk_used = used;
    }
    pub fn update_network_state(&mut self, s: String) {
        self.network_state = s;
    }
}