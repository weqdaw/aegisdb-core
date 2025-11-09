/// Latches 提供命令级的原子性
/// 
/// 通过为每个键提供锁存器，确保并发命令不会冲突

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

pub struct Latches {
    latch_map: Arc<Mutex<HashMap<Vec<u8>, Arc<Notify>>>>,
}

impl Latches {
    /// 创建新的 Latches
    pub fn new() -> Self {
        Self {
            latch_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 尝试获取所有键的锁存器
    /// 
    /// 如果成功，返回 None
    /// 如果任何键被锁定，返回需要等待的 Notify
    pub fn acquire_latches(&self, keys: &[Vec<u8>]) -> Option<Arc<Notify>> {
        let mut map = self.latch_map.lock().unwrap();
        
        // 检查是否有任何键被锁定
        for key in keys {
            if let Some(notify) = map.get(key) {
                return Some(notify.clone());
            }
        }
        
        // 所有键都可用，锁定它们
        let notify = Arc::new(Notify::new());
        for key in keys {
            map.insert(key.clone(), notify.clone());
        }
        
        None
    }

    /// 释放所有键的锁存器
    pub fn release_latches(&self, keys: &[Vec<u8>]) {
        let mut map = self.latch_map.lock().unwrap();
        
        if let Some(first_key) = keys.first() {
            if let Some(notify) = map.remove(first_key) {
                notify.notify_waiters();
            }
        }
        
        for key in keys.iter().skip(1) {
            map.remove(key);
        }
    }

    /// 等待获取所有键的锁存器
    /// 
    /// 这是一个阻塞操作，直到所有键都可用
    pub async fn wait_for_latches(&self, keys: &[Vec<u8>]) {
        loop {
            if let Some(notify) = self.acquire_latches(keys) {
                notify.notified().await;
            } else {
                return;
            }
        }
    }
}

impl Default for Latches {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_latches() {
        let latches = Latches::new();
        let keys1 = vec![b"key1".to_vec(), b"key2".to_vec()];
        let keys2 = vec![b"key2".to_vec(), b"key3".to_vec()];
        
        // 第一个任务获取锁
        let notify1 = latches.acquire_latches(&keys1);
        assert!(notify1.is_none());
        
        // 第二个任务尝试获取冲突的锁
        let notify2 = latches.acquire_latches(&keys2);
        assert!(notify2.is_some());
        
        // 释放第一个任务的锁
        latches.release_latches(&keys1);
        
        // 等待一小段时间让通知生效
        sleep(Duration::from_millis(10)).await;
        
        // 现在第二个任务应该能获取锁了
        let notify3 = latches.acquire_latches(&keys2);
        assert!(notify3.is_none());
    }
}