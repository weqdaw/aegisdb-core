use super::Scheduler;
use crate::scheduler::cluster::{BasicCluster, StoreInfo, RegionInfo};
use crate::scheduler::operator::Operator;
use crate::scheduler::filter::{Filter, StoreStateFilter, select_source_stores, select_target_stores};
use std::time::Duration;

const BALANCE_REGION_RETRY_LIMIT: usize = 10;
const REGION_TOLERANT_SIZE_RATIO: f64 = 0.05;

/// Region 均衡调度器
pub struct BalanceRegionScheduler {
    name: String,
    filters: Vec<Box<dyn Filter>>,
    max_down_time: Duration,
}

impl BalanceRegionScheduler {
    pub fn new(max_down_time: Duration) -> Self {
        let mut filters: Vec<Box<dyn Filter>> = Vec::new();
        filters.push(Box::new(StoreStateFilter {
            max_down_time,
            action_scope: "balance-region".to_string(),
        }));

        Self {
            name: "balance-region-scheduler".to_string(),
            filters,
            max_down_time,
        }
    }
}

impl Scheduler for BalanceRegionScheduler {
    fn name(&self) -> &str {
        &self.name
    }

    fn scheduler_type(&self) -> &str {
        "balance-region"
    }

    fn is_schedule_allowed(&self, _cluster: &BasicCluster) -> bool {
        // 检查当前正在执行的 Region 操作数量
        // 这里简化处理，实际应该从 OperatorController 获取
        true
    }

    fn schedule(&self, cluster: &BasicCluster) -> Option<Operator> {
        // 1. 获取所有合适的 Store
        let stores = cluster.get_stores();
        let sources = select_source_stores(&stores, &self.filters);
        let targets = select_target_stores(&stores, &self.filters);

        if sources.is_empty() || targets.is_empty() {
            return None;
        }

        // 2. 按 Region 数量排序：源 Store 从大到小，目标 Store 从小到大
        let mut sorted_sources: Vec<StoreInfo> = sources;
        sorted_sources.sort_by(|a, b| {
            b.region_count().cmp(&a.region_count())
                .then_with(|| b.region_size().cmp(&a.region_size()))
        });

        let mut sorted_targets: Vec<StoreInfo> = targets;
        sorted_targets.sort_by(|a, b| {
            a.region_count().cmp(&b.region_count())
                .then_with(|| a.region_size().cmp(&b.region_size()))
        });

        // 3. 尝试从源 Store 迁移 Region 到目标 Store
        for source in &sorted_sources {
            for _ in 0..BALANCE_REGION_RETRY_LIMIT {
                // 3.1 优先选择 Pending Region
                if let Some(region) = cluster.rand_pending_region(source.id()) {
                    if let Some(op) = self.try_create_move_peer_operator(
                        cluster,
                        &region,
                        source,
                        &sorted_targets,
                    ) {
                        return Some(op);
                    }
                }

                // 3.2 其次选择 Follower Region
                if let Some(region) = cluster.rand_follower_region(source.id()) {
                    if let Some(op) = self.try_create_move_peer_operator(
                        cluster,
                        &region,
                        source,
                        &sorted_targets,
                    ) {
                        return Some(op);
                    }
                }

                // 3.3 最后选择 Leader Region
                if let Some(region) = cluster.rand_leader_region(source.id()) {
                    if let Some(op) = self.try_create_move_peer_operator(
                        cluster,
                        &region,
                        source,
                        &sorted_targets,
                    ) {
                        return Some(op);
                    }
                }
            }
        }

        None
    }
}

impl BalanceRegionScheduler {
    /// 尝试创建 MovePeer Operator
    fn try_create_move_peer_operator(
        &self,
        _cluster: &BasicCluster,
        region: &RegionInfo,
        source: &StoreInfo,
        targets: &[StoreInfo],
    ) -> Option<Operator> {
        // 找到合适的目标 Store
        for target in targets {
            // 检查差异是否足够大
            let source_size = source.region_size();
            let target_size = target.region_size();
            let region_size = region.approximate_size();

            // 确保迁移后，目标 Store 的 Region 大小仍然小于源 Store
            if source_size <= target_size + 2 * region_size {
                continue;
            }

            // 检查目标 Store 是否已经在 Region 的 Peers 中
            if region.get_store_ids().contains(&target.id()) {
                continue;
            }

            // 生成新的 Peer ID（简化处理，实际应该从 ID Allocator 获取）
            use rand::Rng;
            let new_peer_id = rand::thread_rng().gen_range(10000..99999);

            // 创建 MovePeer Operator
            let desc = format!(
                "move peer from store {} to store {} for region {}",
                source.id(),
                target.id(),
                region.id()
            );

            return Some(Operator::create_move_peer_operator(
                desc,
                region,
                source.id(),
                target.id(),
                new_peer_id,
            ));
        }

        None
    }
}