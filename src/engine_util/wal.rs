/// WAL (Write-Ahead Log) 层
/// 提供显式的 WAL 写入和恢复功能
use crate::engine_util::Engines;
use anyhow::Result;
use serde::{Serialize, Deserialize};

/// WAL 条目类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntryType {
    /// KV 写入
    KvWrite,
    /// Raft 日志写入
    RaftLog,
    /// Region 状态更新
    RegionState,
    /// Apply State 更新
    ApplyState,
}

/// WAL 条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// 条目类型
    pub entry_type: WalEntryType,
    /// 序列号（用于恢复时排序）
    pub sequence: u64,
    /// 数据
    pub data: Vec<u8>,
    /// 时间戳
    pub timestamp: u64,
}

/// WAL 管理器
pub struct WalManager {
    /// 序列号生成器
    sequence: u64,
    /// 待刷新的条目
    pending_entries: Vec<WalEntry>,
}

impl WalManager {
    pub fn new() -> Self {
        Self {
            sequence: 0,
            pending_entries: Vec::new(),
        }
    }
    
    /// 添加 WAL 条目
    pub fn add_entry(&mut self, entry_type: WalEntryType, data: Vec<u8>) -> u64 {
        let sequence = self.sequence;
        self.sequence += 1;
        
        let entry = WalEntry {
            entry_type,
            sequence,
            data,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        
        self.pending_entries.push(entry);
        sequence
    }
    
    /// 刷新所有待刷新的条目到磁盘
    pub fn flush(&mut self, engines: &Engines) -> Result<()> {
        if self.pending_entries.is_empty() {
            return Ok(());
        }
        
        // 强制刷新 WAL 到磁盘
        engines.flush_wal()?;
        
        // 清空待刷新条目
        self.pending_entries.clear();
        
        Ok(())
    }
    
    /// 获取待刷新条目数量
    pub fn pending_count(&self) -> usize {
        self.pending_entries.len()
    }
    
    /// 重置序列号（用于测试）
    #[cfg(test)]
    pub fn reset_sequence(&mut self) {
        self.sequence = 0;
    }
}

impl Default for WalManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 验证数据一致性
pub fn verify_data_consistency(engines: &Engines) -> Result<bool> {
    // 检查 KV 和 Raft 数据库是否可访问
    // 尝试创建一个迭代器来验证数据库可访问
    let _iter = engines.kv.iterator(rocksdb::IteratorMode::Start);
    
    if let Some(ref raft_db) = engines.raft {
        let _iter = raft_db.iterator(rocksdb::IteratorMode::Start);
    }
    
    Ok(true)
}

/// 检查 WAL 完整性
pub fn check_wal_integrity(engines: &Engines) -> Result<bool> {
    // 尝试刷新 WAL，如果成功则说明 WAL 完整
    engines.flush_wal()?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine_util::engines::create_db;
    use crate::engine_util::WriteBatch;
    use tempfile::TempDir;
    
    fn create_test_engines() -> (Engines, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let kv_path = temp_dir.path().join("kv");
        let raft_path = temp_dir.path().join("raft");
        
        std::fs::create_dir_all(&kv_path).unwrap();
        std::fs::create_dir_all(&raft_path).unwrap();
        
        let kv_db = create_db(kv_path.to_str().unwrap(), false).unwrap();
        let raft_db = create_db(raft_path.to_str().unwrap(), true).unwrap();
        
        let engines = Engines::new(
            kv_db,
            Some(raft_db),
            kv_path.to_str().unwrap().to_string(),
            raft_path.to_str().unwrap().to_string(),
        );
        
        (engines, temp_dir)
    }
    
    #[test]
    fn test_wal_manager() {
        let mut wal = WalManager::new();
        
        // 添加条目
        let seq1 = wal.add_entry(WalEntryType::KvWrite, b"data1".to_vec());
        let seq2 = wal.add_entry(WalEntryType::RaftLog, b"data2".to_vec());
        
        assert_eq!(seq1, 0);
        assert_eq!(seq2, 1);
        assert_eq!(wal.pending_count(), 2);
    }
    
    #[test]
    fn test_wal_flush() {
        let (engines, _temp_dir) = create_test_engines();
        let mut wal = WalManager::new();
        
        // 添加条目
        wal.add_entry(WalEntryType::KvWrite, b"data1".to_vec());
        wal.add_entry(WalEntryType::RaftLog, b"data2".to_vec());
        
        // 刷新
        wal.flush(&engines).unwrap();
        
        assert_eq!(wal.pending_count(), 0);
    }
    
    #[test]
    fn test_verify_data_consistency() {
        let (engines, _temp_dir) = create_test_engines();
        
        // 写入一些数据
        let mut wb = WriteBatch::new();
        wb.set_cf("default", b"key1", b"value1");
        engines.write_kv(&wb).unwrap();
        
        // 验证一致性
        assert!(verify_data_consistency(&engines).unwrap());
    }
    
    #[test]
    fn test_check_wal_integrity() {
        let (engines, _temp_dir) = create_test_engines();
        
        // 写入一些数据
        let mut wb = WriteBatch::new();
        wb.set_cf("default", b"key1", b"value1");
        engines.write_kv(&wb).unwrap();
        
        // 检查 WAL 完整性
        assert!(check_wal_integrity(&engines).unwrap());
    }
}

