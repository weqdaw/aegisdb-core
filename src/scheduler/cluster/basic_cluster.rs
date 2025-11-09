use super::{StoreInfo, RegionInfo};
use std::collections::HashMap;
use parking_lot::RwLock;

/// 基础集群信息管理
#[derive(Clone)]
pub struct BasicCluster {
    stores: Arc<RwLock<HashMap<u64, StoreInfo>>>,
    regions: Arc<RwLock<HashMap<u64, RegionInfo>>>,
    // store_id -> region_ids
    store_regions: Arc<RwLock<HashMap<u64, HashSet<u64>>>>,
    // store_id -> leader_region_ids
    store_leaders: Arc<RwLock<HashMap<u64, HashSet<u64>>>>,
}

use std::sync::Arc;
use std::collections::HashSet;

impl BasicCluster {
    pub fn new() -> Self {
        Self {
            stores: Arc::new(RwLock::new(HashMap::new())),
            regions: Arc::new(RwLock::new(HashMap::new())),
            store_regions: Arc::new(RwLock::new(HashMap::new())),
            store_leaders: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 获取所有 Store
    pub fn get_stores(&self) -> Vec<StoreInfo> {
        let stores = self.stores.read();
        stores.values().cloned().collect()
    }

    /// 获取 Store
    pub fn get_store(&self, store_id: u64) -> Option<StoreInfo> {
        let stores = self.stores.read();
        stores.get(&store_id).cloned()
    }

    /// 添加或更新 Store
    pub fn put_store(&self, store: StoreInfo) {
        let mut stores = self.stores.write();
        stores.insert(store.id(), store);
    }

    /// 获取所有 Region
    pub fn get_regions(&self) -> Vec<RegionInfo> {
        let regions = self.regions.read();
        regions.values().cloned().collect()
    }

    /// 获取 Region
    pub fn get_region(&self, region_id: u64) -> Option<RegionInfo> {
        let regions = self.regions.read();
        regions.get(&region_id).cloned()
    }

    /// 添加或更新 Region
    pub fn put_region(&self, region: RegionInfo) {
        let region_id = region.id();
        let old_region = {
            let regions = self.regions.read();
            regions.get(&region_id).cloned()
        };

        // 移除旧的映射
        if let Some(old) = &old_region {
            let mut store_regions = self.store_regions.write();
            let mut store_leaders = self.store_leaders.write();
            
            for peer in old.peers() {
                if let Some(regions) = store_regions.get_mut(&peer.store_id) {
                    regions.remove(&region_id);
                }
            }
            if let Some(leader_store_id) = old.get_leader_store_id() {
                if let Some(leaders) = store_leaders.get_mut(&leader_store_id) {
                    leaders.remove(&region_id);
                }
            }
        }

        // 添加新的映射
        {
            let mut regions = self.regions.write();
            let mut store_regions = self.store_regions.write();
            let mut store_leaders = self.store_leaders.write();

            regions.insert(region_id, region.clone());
            for peer in region.peers() {
                store_regions
                    .entry(peer.store_id)
                    .or_insert_with(HashSet::new)
                    .insert(region_id);
            }
            if let Some(leader_store_id) = region.get_leader_store_id() {
                store_leaders
                    .entry(leader_store_id)
                    .or_insert_with(HashSet::new)
                    .insert(region_id);
            }
        }

        // 更新 Store 统计信息（在释放所有锁之后）
        self.update_store_stats();
    }

    /// 更新 Store 统计信息
    fn update_store_stats(&self) {
        let regions = self.regions.read();
        let mut stores = self.stores.write();

        // 重置计数
        for store in stores.values_mut() {
            store.update_region_count(0);
            store.update_leader_count(0);
            store.update_region_size(0);
            store.update_leader_size(0);
        }

        // 重新计算
        for region in regions.values() {
            let size = region.approximate_size();

            // 更新 region_count 和 region_size
            // 直接从 region 的 peers 中获取 store_ids
            for peer in region.peers() {
                if let Some(store) = stores.get_mut(&peer.store_id) {
                    store.update_region_count(store.region_count() + 1);
                    store.update_region_size(store.region_size() + size);
                }
            }

            // 更新 leader_count 和 leader_size
            if let Some(leader_store_id) = region.get_leader_store_id() {
                if let Some(store) = stores.get_mut(&leader_store_id) {
                    store.update_leader_count(store.leader_count() + 1);
                    store.update_leader_size(store.leader_size() + size);
                }
            }
        }
    }

    /// 获取 Store 上的 Region 数量
    pub fn get_store_region_count(&self, store_id: u64) -> usize {
        let store_regions = self.store_regions.read();
        store_regions
            .get(&store_id)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    /// 获取 Store 上的 Leader 数量
    pub fn get_store_leader_count(&self, store_id: u64) -> usize {
        let store_leaders = self.store_leaders.read();
        store_leaders
            .get(&store_id)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    /// 随机获取 Store 上的一个 Pending Region
    pub fn rand_pending_region(&self, store_id: u64) -> Option<RegionInfo> {
        let store_regions = self.store_regions.read();
        let regions = self.regions.read();
        
        if let Some(region_ids) = store_regions.get(&store_id) {
            let pending_regions: Vec<_> = region_ids
                .iter()
                .filter_map(|&id| {
                    regions.get(&id).and_then(|r| {
                        if r.has_pending_peers() {
                            Some(r.clone())
                        } else {
                            None
                        }
                    })
                })
                .collect();
            
            if !pending_regions.is_empty() {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                let idx = rng.gen_range(0..pending_regions.len());
                return Some(pending_regions[idx].clone());
            }
        }
        None
    }

    /// 随机获取 Store 上的一个 Follower Region
    pub fn rand_follower_region(&self, store_id: u64) -> Option<RegionInfo> {
        let store_regions = self.store_regions.read();
        let regions = self.regions.read();
        
        if let Some(region_ids) = store_regions.get(&store_id) {
            let follower_regions: Vec<_> = region_ids
                .iter()
                .filter_map(|&id| {
                    regions.get(&id).and_then(|r| {
                        if r.get_leader_store_id() != Some(store_id) && r.is_healthy() {
                            Some(r.clone())
                        } else {
                            None
                        }
                    })
                })
                .collect();
            
            if !follower_regions.is_empty() {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                let idx = rng.gen_range(0..follower_regions.len());
                return Some(follower_regions[idx].clone());
            }
        }
        None
    }

    /// 随机获取 Store 上的一个 Leader Region
    pub fn rand_leader_region(&self, store_id: u64) -> Option<RegionInfo> {
        let store_leaders = self.store_leaders.read();
        let regions = self.regions.read();
        
        if let Some(region_ids) = store_leaders.get(&store_id) {
            let leader_regions: Vec<_> = region_ids
                .iter()
                .filter_map(|&id| {
                    regions.get(&id).and_then(|r| {
                        if r.is_healthy() {
                            Some(r.clone())
                        } else {
                            None
                        }
                    })
                })
                .collect();
            
            if !leader_regions.is_empty() {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                let idx = rng.gen_range(0..leader_regions.len());
                return Some(leader_regions[idx].clone());
            }
        }
        None
    }

    /// 获取 Region 的 Follower Store IDs
    pub fn get_follower_stores(&self, region: &RegionInfo) -> Vec<u64> {
        region.get_follower_store_ids()
    }

    /// 获取 Region 的 Leader Store ID
    pub fn get_leader_store(&self, region: &RegionInfo) -> Option<u64> {
        region.get_leader_store_id()
    }
}