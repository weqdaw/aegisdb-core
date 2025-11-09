use super::Scheduler;
use crate::scheduler::cluster::{BasicCluster, StoreInfo};
use crate::scheduler::operator::Operator;
use crate::scheduler::filter::{Filter, StoreStateFilter, select_source_stores, select_target_stores};
use std::time::Duration;

const BALANCE_LEADER_RETRY_LIMIT: usize = 10;
const LEADER_TOLERANT_SIZE_RATIO: f64 = 0.05;

/// Leader 均衡调度器
pub struct BalanceLeaderScheduler {
    name: String,
    filters: Vec<Box<dyn Filter>>,
    max_down_time: Duration,
}

impl BalanceLeaderScheduler {
    pub fn new(max_down_time: Duration) -> Self {
        let mut filters: Vec<Box<dyn Filter>> = Vec::new();
        filters.push(Box::new(StoreStateFilter {
            max_down_time,
            action_scope: "balance-leader".to_string(),
        }));

        Self {
            name: "balance-leader-scheduler".to_string(),
            filters,
            max_down_time,
        }
    }
}

impl Scheduler for BalanceLeaderScheduler {
    fn name(&self) -> &str {
        &self.name
    }

    fn scheduler_type(&self) -> &str {
        "balance-leader"
    }

    fn is_schedule_allowed(&self, _cluster: &BasicCluster) -> bool {
        true
    }

    fn schedule(&self, cluster: &BasicCluster) -> Option<Operator> {
        let stores = cluster.get_stores();
        let sources = select_source_stores(&stores, &self.filters);
        let targets = select_target_stores(&stores, &self.filters);

        if sources.is_empty() || targets.is_empty() {
            return None;
        }

        // 按 Leader 数量排序
        let mut sorted_sources: Vec<StoreInfo> = sources;
        sorted_sources.sort_by(|a, b| b.leader_count().cmp(&a.leader_count()));

        let mut sorted_targets: Vec<StoreInfo> = targets;
        sorted_targets.sort_by(|a, b| a.leader_count().cmp(&b.leader_count()));

        // 尝试转移 Leader
        for source in &sorted_sources {
            for _ in 0..BALANCE_LEADER_RETRY_LIMIT {
                if let Some(region) = cluster.rand_leader_region(source.id()) {
                    let follower_stores = cluster.get_follower_stores(&region);
                    let valid_targets: Vec<_> = sorted_targets
                        .iter()
                        .filter(|t| follower_stores.contains(&t.id()))
                        .collect();

                    for target in valid_targets {
                        // 检查差异是否足够大
                        let diff = source.leader_count() as i32
                            - target.leader_count() as i32;
                        if diff < 2 {
                            continue;
                        }

                        let desc = format!(
                            "transfer leader from store {} to store {} for region {}",
                            source.id(),
                            target.id(),
                            region.id()
                        );

                        return Some(Operator::create_transfer_leader_operator(
                            desc,
                            &region,
                            source.id(),
                            target.id(),
                        ));
                    }
                }
            }
        }

        None
    }
}