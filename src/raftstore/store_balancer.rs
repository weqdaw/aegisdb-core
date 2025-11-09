use crate::proto::metapb::Store;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

/// Store 负载信息
#[derive(Clone, Debug)]
pub struct StoreLoad {
    pub store_id: u64,
    pub region_count: usize,
    pub leader_count: usize,
    pub region_size: u64,
    pub leader_size: u64,
    pub last_update: SystemTime,
    pub is_up: bool,
}

impl StoreLoad {
    pub fn new(store_id: u64) -> Self {
        Self {
            store_id,
            region_count: 0,
            leader_count: 0,
            region_size: 0,
            leader_size: 0,
            last_update: SystemTime::now(),
            is_up: true,
        }
    }

    /// 更新负载信息
    pub fn update(&mut self, region_count: usize, leader_count: usize, region_size: u64, leader_size: u64) {
        self.region_count = region_count;
        self.leader_count = leader_count;
        self.region_size = region_size;
        self.leader_size = leader_size;
        self.last_update = SystemTime::now();
    }

    /// 检查是否过期（超过一定时间未更新）
    pub fn is_stale(&self, max_age: Duration) -> bool {
        SystemTime::now()
            .duration_since(self.last_update)
            .unwrap_or(Duration::MAX) > max_age
    }

    /// 计算负载分数（用于排序）
    /// 分数越低，负载越轻，越适合分配新的 Region
    pub fn load_score(&self) -> f64 {
        // 综合考虑 Region 数量和大小
        // 权重可以根据实际情况调整
        let count_weight = 1.0;
        let size_weight = 0.000001; // 将字节转换为 MB 级别的权重
        
        (self.region_count as f64) * count_weight + (self.region_size as f64) * size_weight
    }
}

/// Store 负载均衡器
/// 维护所有 Store 的负载信息，用于选择最优的 Store 来分配新的 Region
pub struct StoreBalancer {
    /// store_id -> StoreLoad
    loads: Arc<RwLock<HashMap<u64, StoreLoad>>>,
    /// 负载信息过期时间
    max_load_age: Duration,
}

impl StoreBalancer {
    pub fn new(max_load_age: Duration) -> Self {
        Self {
            loads: Arc::new(RwLock::new(HashMap::new())),
            max_load_age,
        }
    }

    /// 更新 Store 负载信息
    pub fn update_store_load(
        &self,
        store_id: u64,
        region_count: usize,
        leader_count: usize,
        region_size: u64,
        leader_size: u64,
    ) {
        let mut loads = self.loads.write().unwrap();
        let load = loads.entry(store_id).or_insert_with(|| StoreLoad::new(store_id));
        load.update(region_count, leader_count, region_size, leader_size);
    }

    /// 从 Store 心跳更新负载信息
    pub fn update_from_store_heartbeat(&self, store: &Store, region_count: u64, leader_count: u64) {
        // 注意：这里简化处理，实际应该从 StoreStats 获取更详细的信息
        self.update_store_load(
            store.id,
            region_count as usize,
            leader_count as usize,
            0, // region_size 需要从其他地方获取
            0, // leader_size 需要从其他地方获取
        );
    }

    /// 设置 Store 状态
    pub fn set_store_state(&self, store_id: u64, is_up: bool) {
        let mut loads = self.loads.write().unwrap();
        if let Some(load) = loads.get_mut(&store_id) {
            load.is_up = is_up;
        }
    }

    /// 获取所有可用的 Store（按负载排序，负载最轻的在前）
    pub fn get_available_stores(&self) -> Vec<StoreLoad> {
        let loads = self.loads.read().unwrap();
        let mut stores: Vec<StoreLoad> = loads
            .values()
            .filter(|load| {
                load.is_up && !load.is_stale(self.max_load_age)
            })
            .cloned()
            .collect();
        
        // 按负载分数排序，负载最轻的在前
        stores.sort_by(|a, b| {
            a.load_score().partial_cmp(&b.load_score()).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        stores
    }

    /// 选择最优的 Store 来分配新的 Region
    /// 返回负载最轻的 Store ID
    pub fn select_best_store(&self) -> Option<u64> {
        let stores = self.get_available_stores();
        stores.first().map(|load| load.store_id)
    }

    /// 选择多个 Store 来分配新的 Region（用于创建多个 Peer）
    /// 返回按负载排序的 Store ID 列表
    pub fn select_stores(&self, count: usize) -> Vec<u64> {
        let stores = self.get_available_stores();
        stores
            .iter()
            .take(count)
            .map(|load| load.store_id)
            .collect()
    }

    /// 获取 Store 负载信息
    pub fn get_store_load(&self, store_id: u64) -> Option<StoreLoad> {
        let loads = self.loads.read().unwrap();
        loads.get(&store_id).cloned()
    }

    /// 获取平均 Region 数量
    pub fn get_average_region_count(&self) -> f64 {
        let stores = self.get_available_stores();
        if stores.is_empty() {
            return 0.0;
        }
        
        let total: usize = stores.iter().map(|s| s.region_count).sum();
        total as f64 / stores.len() as f64
    }

    /// 检查是否需要负载均衡
    /// 如果某个 Store 的 Region 数量超过平均值的 1.2 倍，则认为需要均衡
    pub fn needs_balance(&self) -> bool {
        let stores = self.get_available_stores();
        if stores.is_empty() {
            return false;
        }

        let avg = self.get_average_region_count();
        if avg == 0.0 {
            return false;
        }

        let threshold = avg * 1.2;
        stores.iter().any(|s| s.region_count as f64 > threshold)
    }

    /// 清理过期的负载信息
    pub fn cleanup_stale_loads(&self) {
        let mut loads = self.loads.write().unwrap();
        loads.retain(|_, load| !load.is_stale(self.max_load_age));
    }
}