// aegisdb/src/proto/eraftpb.rs
// 简化的 eraftpb 定义，用于 Raft 配置变更

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfChangeType {
    AddNode = 0,
    RemoveNode = 1,
}

