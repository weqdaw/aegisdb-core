use serde::{Deserialize, Serialize};

/// Node ID 常量：None 表示没有 leader
pub const NONE: u64 = 0;

/// Raft 节点状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateType {
    Follower,
    Candidate,
    Leader,
}

impl StateType {
    pub fn as_str(&self) -> &'static str {
        match self {
            StateType::Follower => "StateFollower",
            StateType::Candidate => "StateCandidate",
            StateType::Leader => "StateLeader",
        }
    }
}

/// Entry 类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    Normal = 0,
    ConfChange = 1,
}

/// Raft 日志条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub entry_type: EntryType,
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
}

impl Entry {
    pub fn new_normal(term: u64, index: u64, data: Vec<u8>) -> Self {
        Self {
            entry_type: EntryType::Normal,
            term,
            index,
            data,
        }
    }

    pub fn new_conf_change(term: u64, index: u64, data: Vec<u8>) -> Self {
        Self {
            entry_type: EntryType::ConfChange,
            term,
            index,
            data,
        }
    }
}

/// 消息类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    // 本地消息
    MsgHup = 0,              // 选举超时，开始选举
    MsgBeat = 1,              // Leader 发送心跳信号
    MsgPropose = 2,           // 提案新日志
    
    // 网络消息
    MsgAppend = 3,            // 追加日志条目
    MsgAppendResponse = 4,    // 追加日志响应
    MsgRequestVote = 5,        // 请求投票
    MsgRequestVoteResponse = 6, // 投票响应
    MsgSnapshot = 7,          // 快照
    MsgHeartbeat = 8,         // 心跳
    MsgHeartbeatResponse = 9,  // 心跳响应
    MsgTransferLeader = 11,   // 转移领导权
    MsgTimeoutNow = 12,       // 立即超时
}

impl MessageType {
    /// 判断是否为本地消息
    pub fn is_local(&self) -> bool {
        matches!(self, MessageType::MsgHup | MessageType::MsgBeat | MessageType::MsgPropose)
    }

    /// 判断是否为响应消息
    pub fn is_response(&self) -> bool {
        matches!(
            self,
            MessageType::MsgAppendResponse
                | MessageType::MsgRequestVoteResponse
                | MessageType::MsgHeartbeatResponse
        )
    }
}

/// Raft 消息
#[derive(Debug, Clone)]
pub struct Message {
    pub msg_type: MessageType,
    pub to: u64,
    pub from: u64,
    pub term: u64,
    pub log_term: u64,      // 最后一条日志的 term
    pub index: u64,         // 最后一条日志的 index
    pub entries: Vec<Entry>,
    pub commit: u64,        // commit index
    pub snapshot: Option<Snapshot>,
    pub reject: bool,       // 是否拒绝（用于响应消息）
}

impl Message {
    pub fn new() -> Self {
        Self {
            msg_type: MessageType::MsgHup,
            to: NONE,
            from: NONE,
            term: 0,
            log_term: 0,
            index: 0,
            entries: Vec::new(),
            commit: 0,
            snapshot: None,
            reject: false,
        }
    }
}

/// 快照元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub conf_state: ConfState,
    pub index: u64,
    pub term: u64,
}

/// 快照
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub data: Vec<u8>,
    pub metadata: SnapshotMetadata,
}

impl Snapshot {
    pub fn is_empty(&self) -> bool {
        self.metadata.index == 0
    }
}

/// 硬状态（需要持久化）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    pub term: u64,
    pub vote: u64,
    pub commit: u64,
}

impl HardState {
    pub fn is_empty(&self) -> bool {
        self.term == 0 && self.vote == 0 && self.commit == 0
    }
}

/// 配置状态（集群成员信息）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfState {
    pub nodes: Vec<u64>,
}

/// 配置变更
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfChangeType {
    AddNode = 0,
    RemoveNode = 1,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfChange {
    pub change_type: ConfChangeType,
    pub node_id: u64,
    pub context: Vec<u8>,
}

/// raft 配置
pub struct RaftConfig {
    /// 节点 ID（不能为 0）
    pub id: u64,
    
    /// 所有节点的 ID（包括自己），仅在启动新集群时设置
    pub peers: Vec<u64>,
    
    /// 选举超时（tick 数）
    /// 如果 follower 在 ElectionTick 个 tick 内没有收到 leader 的消息，将变为 candidate
    pub election_tick: usize,
    
    /// 心跳超时（tick 数）
    /// Leader 每 HeartbeatTick 个 tick 发送一次心跳
    pub heartbeat_tick: usize,
    
    /// 存储接口（需要转换为 Box<dyn Storage>）
    pub storage: Box<dyn crate::raft::storage::Storage>,
    
    /// 已应用的最后一个索引（重启时设置）
    pub applied: u64,
}

impl std::fmt::Debug for RaftConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftConfig")
            .field("id", &self.id)
            .field("peers", &self.peers)
            .field("election_tick", &self.election_tick)
            .field("heartbeat_tick", &self.heartbeat_tick)
            .field("storage", &"<dyn Storage>")
            .field("applied", &self.applied)
            .finish()
    }
}

impl RaftConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.id == NONE {
            return Err(anyhow::anyhow!("cannot use none as id"));
        }
        if self.heartbeat_tick <= 0 {
            return Err(anyhow::anyhow!("heartbeat tick must be greater than 0"));
        }
        if self.election_tick <= self.heartbeat_tick {
            return Err(anyhow::anyhow!("election tick must be greater than heartbeat tick"));
        }
        Ok(())
    }
}