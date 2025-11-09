use async_trait::async_trait;
use rocksdb::DB;
use std::sync::Arc;
use lru::LruCache;
use tokio::sync::RwLock;
use crate::storage::StorageReader;
use crate::storage::standalone_storage::StandaloneStorage;
use crate::engine_util::{DBIterator, RocksDBIterator};

pub struct StandaloneReader {
    pub db: Arc<DB>,
    pub cache: Arc<RwLock<LruCache<String, Vec<u8>>>>,
}

#[async_trait]
impl StorageReader for StandaloneReader {
    async fn get_cf(&self, cf: &str, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let cache_key = format!("{}:{}", cf, String::from_utf8_lossy(key));
        
        // 先查缓存
        {
            let cache = self.cache.read().await;
            if let Some(value) = cache.peek(&cache_key) {
                return Ok(Some(value.clone()));
            }
        }
        
        // 查数据库
        let encoded_key = StandaloneStorage::key_with_cf(cf, key);
        match self.db.get(&encoded_key)? {
            Some(value) => {
                let value_vec = value.to_vec();
                
                // 更新缓存
                {
                    let mut cache = self.cache.write().await;
                    cache.put(cache_key, value_vec.clone());
                }
                
                Ok(Some(value_vec))
            }
            None => Ok(None),
        }
    }
    
    fn iter_cf(&self, cf: &str) -> Box<dyn DBIterator> {
        let prefix = format!("{}_", cf);
        Box::new(RocksDBIterator::new(self.db.clone(), prefix))
    }
    
    fn close(&self) {
        // RocksDB 迭代器会自动清理
    }
}