use crate::scheduler::cluster::StoreInfo;
use std::time::Duration;

/// Store 过滤器接口
pub trait Filter: Send + Sync {
    /// 检查 Store 是否可以作为源 Store
    fn source(&self, store: &StoreInfo) -> bool;
    
    /// 检查 Store 是否可以作为目标 Store
    fn target(&self, store: &StoreInfo) -> bool;
    
    /// 过滤器名称
    fn name(&self) -> &str;
}

/// Store 状态过滤器
pub struct StoreStateFilter {
    pub max_down_time: Duration,
    pub action_scope: String,
}

impl Filter for StoreStateFilter {
    fn source(&self, store: &StoreInfo) -> bool {
        !store.is_up() || store.is_blocked() || store.down_time() > self.max_down_time
    }

    fn target(&self, store: &StoreInfo) -> bool {
        !store.is_up() || store.is_blocked() || store.down_time() > self.max_down_time
    }

    fn name(&self) -> &str {
        "store-state-filter"
    }
}

/// 选择可以作为源 Store 的列表
pub fn select_source_stores(stores: &[StoreInfo], filters: &[Box<dyn Filter>]) -> Vec<StoreInfo> {
    stores
        .iter()
        .filter(|store| {
            filters.iter().all(|filter| !filter.source(store))
        })
        .cloned()
        .collect()
}

/// 选择可以作为目标 Store 的列表
pub fn select_target_stores(stores: &[StoreInfo], filters: &[Box<dyn Filter>]) -> Vec<StoreInfo> {
    stores
        .iter()
        .filter(|store| {
            filters.iter().all(|filter| !filter.target(store))
        })
        .cloned()
        .collect()
}