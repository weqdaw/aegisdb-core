use crate::raft::types::{HardState, ConfState, Entry, Snapshot};
use anyhow::Result;

/// Storage trait：Raft 存储接口
/// 由上层应用实现，用于持久化 Raft 状态和日志
#[async_trait::async_trait]
pub trait Storage: Send + Sync {
    /// 获取初始状态（HardState 和 ConfState）
    async fn initial_state(&self) -> Result<(HardState, ConfState)>;

    /// 获取日志条目 [lo, hi)
    async fn entries(&self, lo: u64, hi: u64) -> Result<Vec<Entry>>;

    /// 获取指定索引的 term
    async fn term(&self, index: u64) -> Result<u64>;

    /// 获取最后一条日志的索引
    async fn last_index(&self) -> Result<u64>;

    /// 获取第一条可用日志的索引
    async fn first_index(&self) -> Result<u64>;

    /// 获取最新的快照
    async fn snapshot(&self) -> Result<Snapshot>;
}

/// 错误类型
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("requested index is unavailable due to compaction")]
    Compacted,
    #[error("requested index is older than the existing snapshot")]
    SnapOutOfDate,
    #[error("requested entry at index is unavailable")]
    Unavailable,
    #[error("snapshot is temporarily unavailable")]
    SnapshotTemporarilyUnavailable,
}