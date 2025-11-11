use std::time::Duration;
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub store_addr: String,
    pub raft: bool,
    pub scheduler_addr: String,
    pub log_level: String,
    pub db_path: String,
    pub pd_endpoints: Vec<String>,
    
    // Raft configuration
    pub raft_base_tick_interval: Duration,
    pub raft_heartbeat_ticks: i32,
    pub raft_election_timeout_ticks: i32,
    pub raft_log_gc_tick_interval: Duration,
    pub raft_log_gc_count_limit: u64,
    
    // Region configuration
    pub split_region_check_tick_interval: Duration,
    pub scheduler_heartbeat_tick_interval: Duration,
    pub scheduler_store_heartbeat_tick_interval: Duration,
    pub region_max_size: u64,
    pub region_split_size: u64,
}

impl Config {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.raft_heartbeat_ticks == 0 {
            return Err(anyhow::anyhow!("heartbeat tick must greater than 0"));
        }
        
        if self.raft_election_timeout_ticks <= self.raft_heartbeat_ticks {
            return Err(anyhow::anyhow!("election tick must be greater than heartbeat tick"));
        }
        
        Ok(())
    }
    
    pub fn new_default() -> Self {
        Self {
            store_addr: "127.0.0.1:20160".to_string(),
            raft: true,
            scheduler_addr: "127.0.0.1:2379".to_string(),
            log_level: Self::get_log_level(),
            db_path: "/tmp/aegisdb".to_string(),
            pd_endpoints: vec!["http://127.0.0.1:2379".to_string()],
            raft_base_tick_interval: Duration::from_secs(1),
            raft_heartbeat_ticks: 2,
            raft_election_timeout_ticks: 10,
            raft_log_gc_tick_interval: Duration::from_secs(10),
            raft_log_gc_count_limit: 128000,
            split_region_check_tick_interval: Duration::from_secs(10),
            scheduler_heartbeat_tick_interval: Duration::from_secs(10),
            scheduler_store_heartbeat_tick_interval: Duration::from_secs(10),
            region_max_size: 144 * 1024 * 1024, // 144 MB
            region_split_size: 96 * 1024 * 1024, // 96 MB
        }
    }
    
    pub fn new_test() -> Self {
        Self {
            store_addr: "127.0.0.1:20160".to_string(),
            raft: true,
            scheduler_addr: "127.0.0.1:2379".to_string(),
            log_level: Self::get_log_level(),
            db_path: "/tmp/aegisdb_test".to_string(),
            pd_endpoints: vec!["http://127.0.0.1:2379".to_string()],
            raft_base_tick_interval: Duration::from_millis(50),
            raft_heartbeat_ticks: 2,
            raft_election_timeout_ticks: 10,
            raft_log_gc_tick_interval: Duration::from_millis(50),
            raft_log_gc_count_limit: 128000,
            split_region_check_tick_interval: Duration::from_millis(100),
            scheduler_heartbeat_tick_interval: Duration::from_millis(100),
            scheduler_store_heartbeat_tick_interval: Duration::from_millis(500),
            region_max_size: 144 * 1024 * 1024,
            region_split_size: 96 * 1024 * 1024,
        }
    }
    
    fn get_log_level() -> String {
        env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string())
    }
}

pub const KB: u64 = 1024;
pub const MB: u64 = 1024 * 1024;