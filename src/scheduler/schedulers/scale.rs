use super::Scheduler;
use crate::scheduler::cluster::{BasicCluster, StoreInfo, RegionInfo};
use crate::scheduler::operator::Operator;
use crate::scheduler::filter::{Filter, StoreStateFilter, select_target_stores};
use std::time::Duration;
use rand::Rng;

const MIN_PEER_COUNT: usize = 3;  // 最小 Peer 数量（推荐值）
const MAX_PEER_COUNT: usize = 5;   // 最大 Peer 数量（推荐值）

/// 自动扩容/缩容调度器
pub struct ScaleScheduler {
    name: String,
    filters: Vec<Box<dyn Filter>>,
    max_down_time: Duration,
    min_peer_count: usize,
    max_peer_count: usize,
}

impl ScaleScheduler {
    pub fn new(max_down_time: Duration) -> Self {
        Self::with_peer_count(max_down_time, MIN_PEER_COUNT, MAX_PEER_COUNT)
    }

    pub fn with_peer_count(max_down_time: Duration, min_peer_count: usize, max_peer_count: usize) -> Self {
        let mut filters: Vec<Box<dyn Filter>> = Vec::new();
        filters.push(Box::new(StoreStateFilter {
            max_down_time,
            action_scope: "scale".to_string(),
        }));

        Self {
            name: "scale-scheduler".to_string(),
            filters,
            max_down_time,  // 保留用于过滤器
            min_peer_count,
            max_peer_count,
        }
    }
}

impl Scheduler for ScaleScheduler {
    fn name(&self) -> &str {
        &self.name
    }

    fn scheduler_type(&self) -> &str {
        "scale"
    }

    fn is_schedule_allowed(&self, cluster: &BasicCluster) -> bool {
        // 检查是否有足够的 Store 来执行扩容/缩容
        let stores = cluster.get_stores();
        stores.len() >= self.min_peer_count
    }

    fn schedule(&self, cluster: &BasicCluster) -> Option<Operator> {
        // 1. 获取所有 Region
        let regions = cluster.get_regions();
        
        // 2. 检查需要扩容的 Region（peer 数量 < min_peer_count）
        for region in &regions {
            let peer_count = region.peers().len();
            
            if peer_count < self.min_peer_count {
                // 需要扩容
                if let Some(op) = self.try_create_add_peer_operator(cluster, region) {
                    log::info!(
                        "scale up: region_id={}, current_peers={}, target_peers={}",
                        region.id(),
                        peer_count,
                        self.min_peer_count
                    );
                    return Some(op);
                }
            }
        }
        
        // 3. 检查需要缩容的 Region（peer 数量 > max_peer_count）
        for region in &regions {
            let peer_count = region.peers().len();
            
            if peer_count > self.max_peer_count {
                // 需要缩容
                if let Some(op) = self.try_create_remove_peer_operator(cluster, region) {
                    log::info!(
                        "scale down: region_id={}, current_peers={}, target_peers={}",
                        region.id(),
                        peer_count,
                        self.max_peer_count
                    );
                    return Some(op);
                }
            }
        }
        
        None
    }
}

impl ScaleScheduler {
    /// 尝试创建 AddPeer Operator（扩容）
    fn try_create_add_peer_operator(
        &self,
        cluster: &BasicCluster,
        region: &RegionInfo,
    ) -> Option<Operator> {
        // 1. 获取所有合适的 Store
        let stores = cluster.get_stores();
        let targets = select_target_stores(&stores, &self.filters);
        
        if targets.is_empty() {
            return None;
        }
        
        // 2. 找到不在 Region 中的 Store
        let region_store_ids: std::collections::HashSet<u64> = 
            region.peers().iter().map(|p| p.store_id).collect();
        
        let available_stores: Vec<&StoreInfo> = targets
            .iter()
            .filter(|store| !region_store_ids.contains(&store.id()))
            .collect();
        
        if available_stores.is_empty() {
            log::debug!(
                "no available store for scale up: region_id={}",
                region.id()
            );
            return None;
        }
        
        // 3. 选择负载最轻的 Store
        let target_store = available_stores
            .iter()
            .min_by_key(|store| (store.region_count(), store.region_size()))
            .unwrap();
        
        // 4. 生成新的 Peer ID（简化处理，实际应该从 ID Allocator 获取）
        let new_peer_id = rand::thread_rng().gen_range(10000..99999);
        
        // 5. 创建 AddPeer Operator
        let desc = format!(
            "scale up: add peer {} on store {} for region {}",
            new_peer_id,
            target_store.id(),
            region.id()
        );
        
        Some(Operator::create_add_peer_operator(
            desc,
            region,
            target_store.id(),
            new_peer_id,
        ))
    }
    
    /// 尝试创建 RemovePeer Operator（缩容）
    fn try_create_remove_peer_operator(
        &self,
        cluster: &BasicCluster,
        region: &RegionInfo,
    ) -> Option<Operator> {
        // 1. 获取 Region 的所有 Peer
        let peers = region.peers();
        
        if peers.len() <= self.min_peer_count {
            // 不能缩容到低于最小数量
            return None;
        }
        
        // 2. 找到 Leader Peer（不能移除 Leader）
        let leader = region.leader();
        let leader_store_id = leader.map(|p| p.store_id);
        
        // 3. 找到可以移除的 Peer（非 Leader，且负载最重）
        let stores = cluster.get_stores();
        let mut candidate_peers: Vec<&crate::proto::metapb::Peer> = peers
            .iter()
            .filter(|peer| {
                // 不能移除 Leader
                if let Some(leader_store) = leader_store_id {
                    if peer.store_id == leader_store {
                        return false;
                    }
                }
                true
            })
            .collect();
        
        if candidate_peers.is_empty() {
            log::debug!(
                "no candidate peer for scale down: region_id={}",
                region.id()
            );
            return None;
        }
        
        // 4. 选择负载最重的 Store 上的 Peer（优先移除）
        candidate_peers.sort_by(|a, b| {
            let store_a = stores.iter().find(|s| s.id() == a.store_id);
            let store_b = stores.iter().find(|s| s.id() == b.store_id);
            
            match (store_a, store_b) {
                (Some(sa), Some(sb)) => {
                    sb.region_count()
                        .cmp(&sa.region_count())
                        .then_with(|| sb.region_size().cmp(&sa.region_size()))
                }
                _ => std::cmp::Ordering::Equal,
            }
        });
        
        let peer_to_remove = candidate_peers[0];
        
        // 5. 创建 RemovePeer Operator
        let desc = format!(
            "scale down: remove peer {} on store {} for region {}",
            peer_to_remove.id,
            peer_to_remove.store_id,
            region.id()
        );
        
        Some(Operator::create_remove_peer_operator(
            desc,
            region,
            peer_to_remove.store_id,
        ))
    }
}

