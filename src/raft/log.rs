use crate::raft::storage::Storage;
use crate::raft::types::{Entry, Snapshot};
use anyhow::Result;

/// RaftLog 管理日志条目
/// 
/// 日志布局：
/// snapshot/first.....applied....committed....stabled.....last
/// --------|------------------------------------------------|
///                      log entries
pub struct RaftLog {
    /// 存储接口
    storage: Box<dyn Storage>,
    
    /// committed: 已提交的最高索引（在大多数节点上已持久化）
    pub committed: u64,
    
    /// applied: 已应用到状态机的最高索引
    /// 不变式: applied <= committed
    pub applied: u64,
    
    /// stabled: 已持久化到存储的最高索引
    /// 用于记录尚未持久化的日志
    pub stabled: u64,
    
    /// entries: 所有尚未压缩的日志条目（内存中）
    entries: Vec<Entry>,
    
    /// pending_snapshot: 待应用的快照（如果有）
    pending_snapshot: Option<Snapshot>,
}

impl RaftLog {
    /// 创建新的 RaftLog
    pub async fn new(storage: Box<dyn Storage>) -> Result<Self> {
        let (hard_state, _conf_state) = storage.initial_state().await?;
        let last_index = storage.last_index().await?;
        let first_index = storage.first_index().await?;
        
        // 加载未压缩的日志条目
        let entries = if last_index >= first_index {
            storage.entries(first_index, last_index + 1).await?
        } else {
            Vec::new()
        };

        Ok(Self {
            storage,
            committed: hard_state.commit,
            applied: 0, // 初始时从配置中获取
            stabled: last_index,
            entries,
            pending_snapshot: None,
        })
    }

    /// 获取所有未压缩的日志条目
    pub fn all_entries(&self) -> Vec<Entry> {
        self.entries.clone()
    }

    /// 获取所有未稳定的日志条目
    pub fn unstable_entries(&self) -> Vec<Entry> {
        let start = if self.stabled >= self.first_index() {
            (self.stabled - self.first_index() + 1) as usize
        } else {
            0
        };
        self.entries[start..].to_vec()
    }

    /// 获取所有已提交但未应用的日志条目
    pub fn next_entries(&self) -> Vec<Entry> {
        let applied_offset = if self.applied >= self.first_index() {
            (self.applied - self.first_index() + 1) as usize
        } else {
            0
        };
        let committed_offset = if self.committed >= self.first_index() {
            (self.committed - self.first_index() + 1) as usize
        } else {
            0
        };
        self.entries[applied_offset..=committed_offset.min(self.entries.len())].to_vec()
    }

    /// 获取最后一条日志的索引
    pub fn last_index(&self) -> u64 {
        if let Some(ref snap) = self.pending_snapshot {
            if !snap.is_empty() {
                return snap.metadata.index;
            }
        }
        if !self.entries.is_empty() {
            self.entries.last().unwrap().index
        } else {
            self.stabled
        }
    }

    /// 获取第一条日志的索引
    pub fn first_index(&self) -> u64 {
        if let Some(ref snap) = self.pending_snapshot {
            if !snap.is_empty() {
                return snap.metadata.index + 1;
            }
        }
        if !self.entries.is_empty() {
            self.entries[0].index
        } else {
            self.stabled + 1
        }
    }

    /// 获取指定索引的 term
    pub async fn term(&self, index: u64) -> Result<u64> {
        let first = self.first_index();
        let last = self.last_index();
        
        if index < first {
            return Err(anyhow::anyhow!("index {} < first {}", index, first));
        }
        
        if index == first - 1 {
            // 返回快照的 term
            if let Some(ref snap) = self.pending_snapshot {
                if !snap.is_empty() && snap.metadata.index == first - 1 {
                    return Ok(snap.metadata.term);
                }
            }
        }
        
        if index <= last {
            let offset = (index - first) as usize;
            if offset < self.entries.len() {
                return Ok(self.entries[offset].term);
            }
        }
        
        // 从存储中获取
        self.storage.term(index).await
    }

    /// 追加日志条目
    pub fn append(&mut self, entries: &[Entry]) {
        if entries.is_empty() {
            return;
        }

        let after = entries[0].index;
        if after <= self.committed {
            // 覆盖已提交的日志（不应该发生）
            panic!("append after committed index {} <= {}", after, self.committed);
        }

        // 如果新条目覆盖了现有条目，需要截断
        let first = self.first_index();
        if after > first {
            let offset = (after - first) as usize;
            if offset < self.entries.len() {
                self.entries.truncate(offset);
            }
        }

        self.entries.extend_from_slice(entries);
    }

    /// 尝试压缩日志（当快照应用后）
    pub fn maybe_compact(&mut self, compact_index: u64) -> Result<()> {
        if compact_index <= self.first_index() {
            return Ok(());
        }

        let first = self.first_index();
        if compact_index > self.last_index() {
            return Err(anyhow::anyhow!(
                "compact_index {} > last_index {}",
                compact_index,
                self.last_index()
            ));
        }

        let offset = (compact_index - first + 1) as usize;
        if offset < self.entries.len() {
            self.entries.drain(..offset);
        }

        Ok(())
    }

    /// 应用快照
    pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        if snapshot.is_empty() {
            return Ok(());
        }

        let snap_index = snapshot.metadata.index;
        let _snap_term = snapshot.metadata.term;

        if snap_index <= self.committed {
            return Err(anyhow::anyhow!(
                "snapshot index {} <= committed {}",
                snap_index,
                self.committed
            ));
        }

        self.pending_snapshot = Some(snapshot);
        self.committed = snap_index;
        self.applied = snap_index;
        self.stabled = snap_index;
        self.entries.clear();

        Ok(())
    }

    /// 获取指定索引的 term（同步版本，仅从内存中获取）
    /// 如果索引不在内存中，返回 None
    pub fn term_sync(&self, index: u64) -> Option<u64> {
        // 索引 0 是特殊值，返回 term 0
        if index == 0 {
            return Some(0);
        }
        
        let first = self.first_index();
        let last = self.last_index();
        
        if index < first {
            return None;
        }
        
        if index == first - 1 {
            // 返回快照的 term
            if let Some(ref snap) = self.pending_snapshot {
                if !snap.is_empty() && snap.metadata.index == first - 1 {
                    return Some(snap.metadata.term);
                }
            }
            return None;
        }
        
        if index <= last {
            let offset = (index - first) as usize;
            if offset < self.entries.len() {
                return Some(self.entries[offset].term);
            }
        }
        
        None
    }

    pub fn pending_snapshot(&self) -> Option<&Snapshot> {
        self.pending_snapshot.as_ref()
    }


    pub fn take_pending_snapshot(&mut self) -> Option<Snapshot> {
        self.pending_snapshot.take()
    }
    
}