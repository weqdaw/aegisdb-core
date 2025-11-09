use async_trait::async_trait;
use rocksdb::{DB, Options};
use lru::LruCache;
use std::sync::Arc;
use std::num::NonZeroUsize;
use tokio::sync::RwLock;
use crate::config::Config;
use crate::storage::{Storage, StorageReader, Modify};
use crate::storage::reader::StandaloneReader;

pub struct StandaloneStorage {
    db: Arc<DB>,
    cache: Arc<RwLock<LruCache<String, Vec<u8>>>>,
    #[allow(dead_code)]
    cache_capacity: usize,
}

impl StandaloneStorage {
    pub fn new(conf: &Config) -> anyhow::Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        
        // 确保目录存在
        std::fs::create_dir_all(&conf.db_path)?;
        
        let db = DB::open(&opts, &conf.db_path)?;
        
        // 默认缓存容量：1000 个条目
        let cache_capacity = 1000;
        
        Ok(Self {
            db: Arc::new(db),
            cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(cache_capacity).unwrap()
            ))),
            cache_capacity,
        })
    }
    
    pub fn key_with_cf(cf: &str, key: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(cf.len() + 1 + key.len());
        encoded.extend_from_slice(cf.as_bytes());
        encoded.push(b'_');
        encoded.extend_from_slice(key);
        encoded
    }
    
    #[allow(dead_code)]
    async fn get_cf_internal(&self, cf: &str, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let cache_key = format!("{}:{}", cf, String::from_utf8_lossy(key));
        
        // 先查缓存
        {
            let cache = self.cache.read().await;
            if let Some(value) = cache.peek(&cache_key) {
                return Ok(Some(value.clone()));
            }
        }
        
        // 查数据库
        let encoded_key = Self::key_with_cf(cf, key);
        match self.db.get(&encoded_key)? {
            Some(value) => {
                let value_vec = value.to_vec();
                
                // 写入缓存
                {
                    let mut cache = self.cache.write().await;
                    cache.put(cache_key, value_vec.clone());
                }
                
                Ok(Some(value_vec))
            }
            None => Ok(None),
        }
    }
}

#[async_trait]
impl Storage for StandaloneStorage {
    async fn start(&self) -> anyhow::Result<()> {
        log::info!("StandaloneStorage started");
        Ok(())
    }
    
    async fn stop(&self) -> anyhow::Result<()> {
        log::info!("StandaloneStorage stopped");
        Ok(())
    }
    
    async fn write(&self, batch: Vec<Modify>) -> anyhow::Result<()> {
        use rocksdb::WriteBatch as RocksDBWriteBatch;
        
        let mut wb = RocksDBWriteBatch::default();
        let mut cache_updates = Vec::new();
        
        for modify in batch {
            match modify {
                Modify::Put(put) => {
                    let encoded_key = Self::key_with_cf(&put.cf, &put.key);
                    wb.put(&encoded_key, &put.value);
                    
                    let cache_key = format!("{}:{}", put.cf, String::from_utf8_lossy(&put.key));
                    cache_updates.push((cache_key, Some(put.value)));
                }
                Modify::Delete(delete) => {
                    let encoded_key = Self::key_with_cf(&delete.cf, &delete.key);
                    wb.delete(&encoded_key);
                    
                    let cache_key = format!("{}:{}", delete.cf, String::from_utf8_lossy(&delete.key));
                    cache_updates.push((cache_key, None));
                }
            }
        }
        
        // 批量写入数据库
        self.db.write(wb)?;
        
        // 更新缓存
        {
            let mut cache = self.cache.write().await;
            for (key, value) in cache_updates {
                if let Some(v) = value {
                    cache.put(key, v);
                } else {
                    cache.pop(&key);
                }
            }
        }
        
        Ok(())
    }
    
    async fn reader(&self) -> anyhow::Result<Box<dyn StorageReader>> {
        Ok(Box::new(StandaloneReader {
            db: self.db.clone(),
            cache: self.cache.clone(),
        }))
    }
}