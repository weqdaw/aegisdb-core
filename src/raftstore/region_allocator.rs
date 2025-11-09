use crate::raftstore::store_balancer::StoreBalancer;
use crate::proto::metapb::{Region, Peer};
use anyhow::Result;
use std::sync::Arc;

/// Region 分配器
/// 负责为新创建的 Region 选择最优的 Store 来分配 Peer
pub struct RegionAllocator {
    balancer: Arc<StoreBalancer>,
    /// 每个 Region 的副本数（Replication Factor）
    replication_factor: usize,
}

impl RegionAllocator {
    pub fn new(balancer: Arc<StoreBalancer>, replication_factor: usize) -> Self {
        Self {
            balancer,
            replication_factor,
        }
    }

    /// 为新的 Region 分配 Store
    /// 返回选中的 Store ID 列表
    pub fn allocate_stores(&self, _region: &Region) -> Result<Vec<u64>> {
        // 选择负载最轻的 Store
        let stores = self.balancer.select_stores(self.replication_factor);
        
        if stores.len() < self.replication_factor {
            return Err(anyhow::anyhow!(
                "not enough available stores: need {}, got {}",
                self.replication_factor,
                stores.len()
            ));
        }

        Ok(stores)
    }

    /// 为新的 Region 创建 Peer 列表
    pub fn allocate_peers(&self, region: &Region, _new_region_id: u64) -> Result<Vec<Peer>> {
        let store_ids = self.allocate_stores(region)?;
        
        // 生成 Peer ID（实际应该从 ID Allocator 获取）
        let mut peers = Vec::new();
        for (idx, store_id) in store_ids.iter().enumerate() {
            // 简化处理：使用 store_id * 10000 + idx 作为 peer_id
            // 实际应该从 ID Allocator 获取
            let peer_id = store_id * 10000 + idx as u64;
            peers.push(Peer {
                id: peer_id,
                store_id: *store_id,
            });
        }

        Ok(peers)
    }

    /// 选择单个 Store（用于添加副本等场景）
    pub fn select_single_store(&self, exclude_stores: &[u64]) -> Option<u64> {
        let stores = self.balancer.get_available_stores();
        
        // 排除已经在使用的 Store
        let candidate = stores
            .iter()
            .find(|load| !exclude_stores.contains(&load.store_id));
        
        candidate.map(|load| load.store_id)
    }
}