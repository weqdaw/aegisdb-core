use crate::proto::metapb::Region;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use crate::engine_util::util::exceed_end_key;

/// Store 级别的 Region 元数据
#[derive(Clone)]
pub struct StoreMeta {
    /// region_id -> region
    regions: Arc<RwLock<BTreeMap<u64, Region>>>,
    /// 按 start_key 排序的 Region 列表（用于范围查找）
    region_ranges: Arc<RwLock<Vec<(Vec<u8>, u64)>>>, // (start_key, region_id)
}

impl StoreMeta {
    pub fn new() -> Self {
        Self {
            regions: Arc::new(RwLock::new(BTreeMap::new())),
            region_ranges: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// 设置 Region
    pub fn set_region(&self, region: Region) {
        let mut regions = self.regions.write().unwrap();
        let mut ranges = self.region_ranges.write().unwrap();
        
        regions.insert(region.id, region.clone());
        
        // 更新范围列表
        ranges.retain(|(_, id)| *id != region.id);
        ranges.push((region.start_key.clone(), region.id));
        ranges.sort_by(|a, b| a.0.cmp(&b.0));
    }

    /// 获取 Region
    pub fn get_region(&self, region_id: u64) -> Option<Region> {
        let regions = self.regions.read().unwrap();
        regions.get(&region_id).cloned()
    }

    /// 根据 key 查找 Region
    pub fn find_region_by_key(&self, key: &[u8]) -> Option<Region> {
        let ranges = self.region_ranges.read().unwrap();
        
        // 二分查找：找到最后一个 start_key <= key 的 Region
        let mut result: Option<u64> = None;
        for (start_key, region_id) in ranges.iter().rev() {
            if key >= start_key.as_slice() {
                result = Some(*region_id);
                break;
            }
        }
        
        if let Some(region_id) = result {
            let regions = self.regions.read().unwrap();
            if let Some(region) = regions.get(&region_id) {
                // 检查 key 是否在 Region 范围内
                if key_in_region(key, region) {
                    return Some(region.clone());
                }
            }
        }
        
        None
    }

    /// 获取所有 Region
    pub fn get_all_regions(&self) -> Vec<Region> {
        let regions = self.regions.read().unwrap();
        regions.values().cloned().collect()
    }

    /// 移除 Region
    pub fn remove_region(&self, region_id: u64) {
        let mut regions = self.regions.write().unwrap();
        let mut ranges = self.region_ranges.write().unwrap();
        
        regions.remove(&region_id);
        ranges.retain(|(_, id)| *id != region_id);
    }

    /// 获取重叠的 Region
    pub fn get_overlap_regions(&self, region: &Region) -> Vec<Region> {
        let ranges = self.region_ranges.read().unwrap();
        let regions = self.regions.read().unwrap();
        let mut overlaps = Vec::new();
        
        for (_start_key, region_id) in ranges.iter() {
            if let Some(existing_region) = regions.get(region_id) {
                // 检查是否重叠
                if regions_overlap(region, existing_region) {
                    overlaps.push(existing_region.clone());
                }
            }
        }
        
        overlaps
    }
}

fn key_in_region(key: &[u8], region: &Region) -> bool {
    let start_key = &region.start_key;
    let end_key = &region.end_key;
    
    if !start_key.is_empty() && key < start_key.as_slice() {
        return false;
    }
    
    if !end_key.is_empty() && exceed_end_key(key, end_key) {
        return false;
    }
    
    true
}

fn regions_overlap(r1: &Region, r2: &Region) -> bool {
    // 检查两个 Region 是否重叠
    let r1_start = &r1.start_key;
    let _r1_end = &r1.end_key;
    let r2_start = &r2.start_key;
    let _r2_end = &r2.end_key;
    
    // r1 的 start 在 r2 范围内
    if key_in_region(r1_start, r2) {
        return true;
    }
    
    // r2 的 start 在 r1 范围内
    if key_in_region(r2_start, r1) {
        return true;
    }
    
    false
}