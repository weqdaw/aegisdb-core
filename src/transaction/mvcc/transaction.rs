/// MvccTxn 是 MVCC 事务的核心结构
/// 
/// 它提供了基于用户键和时间戳的读写操作抽象

use crate::storage::{StorageReader, Modify, Put, Delete};
use crate::engine_util::{CF_DEFAULT, CF_WRITE, CF_LOCK};
use crate::transaction::mvcc::codec::*;
use crate::transaction::mvcc::write::Write;
use crate::transaction::mvcc::lock::Lock;
use anyhow::Result;

/// MVCC 事务
pub struct MvccTxn {
    /// 事务的开始时间戳
    pub start_ts: u64,
    /// 存储读取器
    pub reader: Box<dyn StorageReader>,
    /// 待写入的修改
    pub writes: Vec<Modify>,
}

impl MvccTxn {
    /// 创建新的 MVCC 事务
    pub fn new(reader: Box<dyn StorageReader>, start_ts: u64) -> Self {
        Self {
            start_ts,
            reader,
            writes: Vec::new(),
        }
    }

    /// 获取所有待写入的修改
    pub fn writes(&self) -> &[Modify] {
        &self.writes
    }

    /// 记录一个 Write 到 write CF
    pub fn put_write(&mut self, key: &[u8], ts: u64, write: &Write) {
        let encoded_key = encode_key(key, ts);
        self.writes.push(Modify::Put(Put {
            key: encoded_key,
            value: write.to_bytes(),
            cf: CF_WRITE.to_string(),
        }));
    }

    /// 获取指定键的锁
    pub async fn get_lock(&self, key: &[u8]) -> Result<Option<Lock>> {
        match self.reader.get_cf(CF_LOCK, key).await? {
            Some(value) => {
                let lock = Lock::parse(&value)
                    .map_err(|e| anyhow::anyhow!("failed to parse lock: {}", e))?;
                Ok(Some(lock))
            }
            None => Ok(None),
        }
    }

    /// 添加一个锁
    pub fn put_lock(&mut self, key: &[u8], lock: &Lock) {
        self.writes.push(Modify::Put(Put {
            key: key.to_vec(),
            value: lock.to_bytes(),
            cf: CF_LOCK.to_string(),
        }));
    }

    /// 删除一个锁
    pub fn delete_lock(&mut self, key: &[u8]) {
        self.writes.push(Modify::Delete(Delete {
            key: key.to_vec(),
            cf: CF_LOCK.to_string(),
        }));
    }

    /// 获取指定键在事务开始时间戳时的值
    /// 
    /// 查找逻辑：
    /// 1. 检查是否有锁（如果有且不是当前事务的锁，返回错误）
    /// 2. 在 write CF 中查找 commit_ts <= start_ts 的最新 Write
    /// 3. 如果找到 Put，从 default CF 中读取对应的值
    /// 4. 如果找到 Delete 或 Rollback，返回 None
    pub async fn get_value(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 检查锁
        if let Some(lock) = self.get_lock(key).await? {
            if lock.ts != self.start_ts {
                return Err(anyhow::anyhow!("key is locked by another transaction"));
            }
        }

        // 查找最新的 Write
        let (write, _commit_ts) = self.most_recent_write(key).await?;
        
        match write {
            Some(w) => {
                match w.kind {
                    crate::transaction::mvcc::write::WriteKind::Put => {
                        // 从 default CF 读取值
                        let encoded_key = encode_key(key, w.start_ts);
                        self.reader.get_cf(CF_DEFAULT, &encoded_key).await
                    }
                    crate::transaction::mvcc::write::WriteKind::Delete => Ok(None),
                    crate::transaction::mvcc::write::WriteKind::Rollback => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    /// 添加一个值到 default CF
    pub fn put_value(&mut self, key: &[u8], value: &[u8]) {
        let encoded_key = encode_key(key, self.start_ts);
        self.writes.push(Modify::Put(Put {
            key: encoded_key,
            value: value.to_vec(),
            cf: CF_DEFAULT.to_string(),
        }));
    }

    /// 删除一个值
    pub fn delete_value(&mut self, key: &[u8]) {
        let encoded_key = encode_key(key, self.start_ts);
        self.writes.push(Modify::Delete(Delete {
            key: encoded_key,
            cf: CF_DEFAULT.to_string(),
        }));
    }

    /// 查找当前事务的 Write
    pub async fn current_write(&self, key: &[u8]) -> Result<(Option<Write>, u64)> {
        let mut iter = self.reader.iter_cf(CF_WRITE);
        let search_key = encode_key(key, self.start_ts);
        
        // 查找以 key 开头且时间戳 >= start_ts 的第一个 Write
        iter.seek(&search_key);
        
        if !iter.valid() {
            return Ok((None, 0));
        }
        
        let item = iter.item();
        let encoded_key = item.key();
        
        // 检查是否是同一个用户键
        let user_key = decode_user_key(encoded_key)?;
        if user_key != key {
            return Ok((None, 0));
        }
        
        let commit_ts = decode_timestamp(encoded_key)?;
        if commit_ts != self.start_ts {
            return Ok((None, 0));
        }
        
        let value = item.value()?;
        let write = Write::parse(&value)
            .map_err(|e| anyhow::anyhow!("failed to parse write: {}", e))?;
        
        Ok((write, commit_ts))
    }

    /// 查找最新的 Write
    pub async fn most_recent_write(&self, key: &[u8]) -> Result<(Option<Write>, u64)> {
        let mut iter = self.reader.iter_cf(CF_WRITE);
        
        // 从该键的最大时间戳开始查找
        let search_key = encode_key(key, u64::MAX);
        iter.seek(&search_key);
        
        // 如果没找到，尝试查找该键的任何 Write
        if !iter.valid() {
            let prefix_key = encode_bytes(key);
            iter.seek(&prefix_key);
        }
        
        // 遍历找到第一个匹配的 Write（commit_ts <= start_ts）
        while iter.valid() {
            let item = iter.item();
            let encoded_key = item.key();
            
            // 解码用户键
            let user_key = match decode_user_key(encoded_key) {
                Ok(k) => k,
                Err(_) => {
                    iter.next();
                    continue;
                }
            };
            
            // 检查是否是同一个用户键
            if user_key != key {
                break;
            }
            
            // 解码时间戳
            let commit_ts = match decode_timestamp(encoded_key) {
                Ok(ts) => ts,
                Err(_) => {
                    iter.next();
                    continue;
                }
            };
            
            // 检查时间戳是否 <= start_ts
            if commit_ts <= self.start_ts {
                // 解析 Write
                let value = item.value()?;
                let write = Write::parse(&value)
                    .map_err(|e| anyhow::anyhow!("failed to parse write: {}", e))?;
                
                if let Some(w) = write {
                    return Ok((Some(w), commit_ts));
                }
            }
            
            iter.next();
        }
        
        Ok((None, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{StandaloneStorage, Storage};
    use crate::config::Config;
    use tempfile::TempDir;

    async fn create_test_txn(start_ts: u64) -> (MvccTxn, StandaloneStorage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::new_test();
        config.db_path = temp_dir.path().to_str().unwrap().to_string();
        let storage = StandaloneStorage::new(&config).unwrap();
        storage.start().await.unwrap();
        let reader = storage.reader().await.unwrap();
        let txn = MvccTxn::new(reader, start_ts);
        (txn, storage, temp_dir)
    }

    #[tokio::test]
    async fn test_mvcc_txn_put_and_get() {
        let (mut txn, storage, _temp_dir) = create_test_txn(100).await;
        
        // 写入值
        txn.put_value(b"key1", b"value1");
        
        // 写入 Write 记录
        let write = Write {
            start_ts: 100,
            kind: crate::transaction::mvcc::write::WriteKind::Put,
        };
        txn.put_write(b"key1", 100, &write);
        
        // 提交写入
        storage.write(txn.writes().to_vec()).await.unwrap();
        
        // 创建新事务读取
        let reader2 = storage.reader().await.unwrap();
        let txn2 = MvccTxn::new(reader2, 200);
        let value = txn2.get_value(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
        
        storage.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_mvcc_txn_lock() {
        let (mut txn, storage, _temp_dir) = create_test_txn(100).await;
        
        // 添加锁
        let lock = Lock {
            primary: b"primary".to_vec(),
            ts: 100,
            ttl: 3000,
            kind: crate::transaction::mvcc::write::WriteKind::Put,
        };
        txn.put_lock(b"key1", &lock);
        
        // 提交
        storage.write(txn.writes().to_vec()).await.unwrap();
        
        // 读取锁
        let reader2 = storage.reader().await.unwrap();
        let txn2 = MvccTxn::new(reader2, 200);
        let read_lock = txn2.get_lock(b"key1").await.unwrap();
        assert!(read_lock.is_some());
        assert_eq!(read_lock.unwrap().ts, 100);
        
        storage.stop().await.unwrap();
    }
}