use crate::proto::metapb::Region;
use crate::engine_util::Engines;
use crate::engine_util::{CFS, util::exceed_end_key};
use crate::config::Config;
use crate::raftstore::router::RaftRouter;
use crate::raftstore::message::{Msg, MsgType, MsgData};
use std::sync::Arc;
use log::debug;

/// Split 检查器
pub struct SplitChecker {
    engine: Arc<Engines>,
    router: RaftRouter,
    max_size: u64,
    split_size: u64,
}

impl SplitChecker {
    pub fn new(engine: Arc<Engines>, router: RaftRouter, cfg: &Config) -> Self {
        Self {
            engine,
            router,
            max_size: cfg.region_max_size,
            split_size: cfg.region_split_size,
        }
    }

    /// 检查 Region 是否需要 Split
    /// 返回 split_key（如果 Region 需要分片）
    pub fn check(&self, region: &Region) -> Option<Vec<u8>> {
        let start_key = &region.start_key;
        let end_key = &region.end_key;
        
        // 扫描所有 Column Family 的数据
        let mut total_size = 0u64;
        let mut split_key: Option<Vec<u8>> = None;
        
        // 遍历所有 CF
        for cf in CFS {
            if let Some((size, key)) = self.scan_cf(cf, start_key, end_key) {
                total_size += size;
                
                // 如果超过 split_size 且还没有设置 split_key，设置它
                if total_size > self.split_size && split_key.is_none() {
                    split_key = Some(key);
                }
                
                // 如果超过 max_size，停止扫描
                if total_size > self.max_size {
                    break;
                }
            }
        }
        
        debug!(
            "[region {}] split check: size={}, max_size={}, split_size={}, split_key={:?}",
            region.id, total_size, self.max_size, self.split_size, split_key
        );
        
        // 更新 Region 的近似大小
        self.router.send(region.id, Msg {
            msg_type: MsgType::RegionApproximateSize,
            region_id: region.id,
            data: MsgData::ApproximateSize(total_size),
        }).ok();
        
        // 如果当前大小小于 max_size，不进行 split
        if total_size < self.max_size {
            return None;
        }
        
        split_key
    }
    
    /// 扫描单个 Column Family
    fn scan_cf(&self, cf: &str, start_key: &[u8], end_key: &[u8]) -> Option<(u64, Vec<u8>)> {
        // 使用 RocksDB 的 raw_iterator 直接访问
        use rocksdb::DBRawIterator;
        
        let prefix = format!("{}_", cf);
        let db = &self.engine.kv;
        let mut iter = db.raw_iterator();
        
        // Seek 到 start_key
        if !start_key.is_empty() {
            let seek_key = format!("{}{}", prefix, String::from_utf8_lossy(start_key));
            iter.seek(seek_key.as_bytes());
        } else {
            let seek_key = prefix.as_bytes();
            iter.seek(seek_key);
        }
        
        let mut current_size = 0u64;
        let mut split_key: Option<Vec<u8>> = None;
        let prefix_bytes = prefix.as_bytes();
        
        while iter.valid() {
            if let Some(key_bytes) = iter.key() {
                // 检查 key 是否以 prefix 开头
                if !key_bytes.starts_with(prefix_bytes) {
                    break;
                }
                
                // 提取实际的 key（去掉 prefix）
                let actual_key = &key_bytes[prefix_bytes.len()..];
                
                // 检查是否超出范围
                if !end_key.is_empty() && exceed_end_key(actual_key, end_key) {
                    break;
                }
                
                let key_size = actual_key.len() as u64;
                let value_size = iter.value().map(|v| v.len()).unwrap_or(0) as u64;
                current_size += key_size + value_size;
                
                // 如果超过 split_size 且还没有设置 split_key，设置它
                if current_size > self.split_size && split_key.is_none() {
                    split_key = Some(actual_key.to_vec());
                }
                
                // 如果超过 max_size，停止扫描
                if current_size > self.max_size {
                    break;
                }
            }
            
            iter.next();
        }
        
        if current_size > 0 {
            Some((current_size, split_key.unwrap_or_default()))
        } else {
            None
        }
    }
}