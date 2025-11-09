use crate::raft::types::*;
use crate::raft::raft::Raft;
use crate::raft::Progress;
use anyhow::Result;

/// 软状态（不需要持久化）
/// 包含当前 Leader 和状态信息
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoftState {
    /// 当前 Leader ID（如果没有 leader 则为 NONE）
    pub lead: u64,
    /// 当前节点状态
    pub raft_state: StateType,
}

impl SoftState {
    pub fn new(lead: u64, raft_state: StateType) -> Self {
        Self { lead, raft_state }
    }

    /// 判断是否有更新
    pub fn is_updated(&self, other: &SoftState) -> bool {
        self.lead != other.lead || self.raft_state != other.raft_state
    }
}

/// Ready 包含需要由上层应用处理的所有更新
/// 这些更新必须在消息发送之前持久化
#[derive(Debug, Clone)]
pub struct Ready {
    /// 软状态更新（如果有）
    /// 如果为 None，表示没有软状态更新
    pub soft_state: Option<SoftState>,

    /// 硬状态更新（需要持久化）
    /// 如果为空（is_empty），表示没有硬状态更新
    pub hard_state: HardState,

    /// 需要持久化的日志条目（在发送消息之前）
    pub entries: Vec<Entry>,

    /// 需要持久化的快照（如果有）
    pub snapshot: Option<Snapshot>,

    /// 已提交但未应用的日志条目（需要应用到状态机）
    pub committed_entries: Vec<Entry>,

    /// 需要发送的消息（在持久化之后）
    pub messages: Vec<Message>,

    /// 是否需要持久化
    pub must_sync: bool,
}

impl Ready {
    /// 创建空的 Ready
    pub fn new() -> Self {
        Self {
            soft_state: None,
            hard_state: HardState {
                term: 0,
                vote: 0,
                commit: 0,
            },
            entries: Vec::new(),
            snapshot: None,
            committed_entries: Vec::new(),
            messages: Vec::new(),
            must_sync: false,
        }
    }

    /// 判断是否有任何更新
    pub fn has_ready(&self) -> bool {
        self.soft_state.is_some()
            || !self.hard_state.is_empty()
            || !self.entries.is_empty()
            || self.snapshot.is_some()
            || !self.committed_entries.is_empty()
            || !self.messages.is_empty()
    }
}

impl Default for Ready {
    fn default() -> Self {
        Self::new()
    }
}

/// RawNode 是 Raft 的包装，提供更高级的接口
/// 它管理 Ready 的生成和状态跟踪
pub struct RawNode {
    /// 内部的 Raft 实例
    pub raft: Raft,

    /// 上次的软状态（用于检测变化）
    prev_soft_state: SoftState,

    /// 上次的硬状态（用于检测变化）
    prev_hard_state: HardState,

    /// 上次的 stabled 索引（用于检测需要持久化的条目）
    prev_stabled: u64,

    /// 上次的 applied 索引（用于检测需要应用的条目）
    prev_applied: u64,
}

impl RawNode {
    /// 创建新的 RawNode
    pub async fn new(config: RaftConfig) -> Result<Self> {
        let raft = Raft::new(config).await?;
        
        let prev_soft_state = SoftState::new(raft.lead, raft.state);
        let prev_hard_state = HardState {
            term: raft.term,
            vote: raft.vote,
            commit: raft.raft_log.committed,
        };
        let prev_stabled = raft.raft_log.stabled;
        let prev_applied = raft.raft_log.applied;

        Ok(Self {
            raft,
            prev_soft_state,
            prev_hard_state,
            prev_stabled,
            prev_applied,
        })
    }

    /// Tick 推进逻辑时钟
    pub fn tick(&mut self) {
        self.raft.tick();
    }

    /// Campaign 开始选举
    pub fn campaign(&mut self) -> Result<()> {
        self.raft.step(Message {
            msg_type: MessageType::MsgHup,
            from: self.raft.id,
            ..Message::new()
        })
    }

    /// Propose 提案新日志
    pub fn propose(&mut self, data: Vec<u8>) -> Result<()> {
        let entry = Entry::new_normal(self.raft.term, 0, data);
        self.raft.step(Message {
            msg_type: MessageType::MsgPropose,
            from: self.raft.id,
            entries: vec![entry],
            ..Message::new()
        })
    }

    /// ProposeConfChange 提案配置变更
    pub fn propose_conf_change(&mut self, cc: ConfChange) -> Result<()> {
        let data = bincode::serialize(&cc)
            .map_err(|e| anyhow::anyhow!("failed to serialize conf change: {}", e))?;
        let entry = Entry::new_conf_change(self.raft.term, 0, data);
        self.raft.step(Message {
            msg_type: MessageType::MsgPropose,
            from: self.raft.id,
            entries: vec![entry],
            ..Message::new()
        })
    }

    /// Step 处理收到的消息
    pub fn step(&mut self, m: Message) -> Result<()> {
        // 忽略本地消息（不应该通过网络接收）
        if m.msg_type.is_local() {
            return Err(anyhow::anyhow!("cannot step local message"));
        }

        // 如果是响应消息，检查 peer 是否存在
        if m.msg_type.is_response() {
            if !self.raft.prs.contains_key(&m.from) {
                return Err(anyhow::anyhow!("peer {} not found", m.from));
            }
        }

        self.raft.step(m)
    }

    /// Ready 返回当前的 Ready 状态
    pub fn ready(&mut self) -> Ready {
        let mut ready = Ready::new();

        // 检查软状态更新
        let current_soft_state = SoftState::new(self.raft.lead, self.raft.state);
        if current_soft_state.is_updated(&self.prev_soft_state) {
            ready.soft_state = Some(current_soft_state.clone());
        }

        // 检查硬状态更新
        let current_hard_state = HardState {
            term: self.raft.term,
            vote: self.raft.vote,
            commit: self.raft.raft_log.committed,
        };
        if !current_hard_state.is_empty() && current_hard_state != self.prev_hard_state {
            ready.hard_state = current_hard_state;
            ready.must_sync = true;
        }

        // 检查需要持久化的日志条目（所有未稳定的条目）
        ready.entries = self.raft.raft_log.unstable_entries();

        // 检查快照
        if let Some(snapshot) = self.raft.raft_log.pending_snapshot() {
            if !snapshot.is_empty() {
                ready.snapshot = Some(snapshot.clone());
                ready.must_sync = true;
            }
        }

        // 检查需要应用的日志条目
        let current_applied = self.raft.raft_log.applied;
        if current_applied < self.raft.raft_log.committed {
            ready.committed_entries = self.raft.raft_log.next_entries();
        }

        // 获取待发送的消息
        ready.messages = self.raft.msgs.clone();
        self.raft.msgs.clear();

        ready
    }

    /// HasReady 检查是否有 Ready 状态
    pub fn has_ready(&self) -> bool {
        // 检查软状态
        let current_soft_state = SoftState::new(self.raft.lead, self.raft.state);
        if current_soft_state.is_updated(&self.prev_soft_state) {
            return true;
        }

        // 检查硬状态
        let current_hard_state = HardState {
            term: self.raft.term,
            vote: self.raft.vote,
            commit: self.raft.raft_log.committed,
        };
        if !current_hard_state.is_empty() && current_hard_state != self.prev_hard_state {
            return true;
        }

        // 检查需要持久化的条目（所有未稳定的条目）
        let last_index = self.raft.raft_log.last_index();
        if last_index > self.raft.raft_log.stabled {
            return true;
        }

        // 检查快照
        if let Some(snapshot) = self.raft.raft_log.pending_snapshot() {
            if !snapshot.is_empty() {
                return true;
            }
        }

        // 检查需要应用的条目
        if self.raft.raft_log.applied < self.raft.raft_log.committed {
            return true;
        }

        // 检查待发送的消息
        if !self.raft.msgs.is_empty() {
            return true;
        }

        false
    }

    /// Advance 通知 RawNode 已经处理完 Ready
    /// 需要更新内部状态跟踪
    pub fn advance(&mut self, rd: Ready) {
        // 更新软状态
        if let Some(ref soft_state) = rd.soft_state {
            self.prev_soft_state = soft_state.clone();
        }

        // 更新硬状态
        if !rd.hard_state.is_empty() {
            self.prev_hard_state = rd.hard_state;
        }

        // 更新 stabled 索引
        if !rd.entries.is_empty() {
            // 找到最后一条条目的索引
            if let Some(last_entry) = rd.entries.last() {
                self.prev_stabled = last_entry.index;
                self.raft.raft_log.stabled = last_entry.index;
            }
        }

        // 更新 applied 索引
        if !rd.committed_entries.is_empty() {
            if let Some(last_entry) = rd.committed_entries.last() {
                self.prev_applied = last_entry.index;
                self.raft.raft_log.applied = last_entry.index;
            }
        }

        // 如果应用了快照，需要更新相关状态
        if let Some(ref snapshot) = rd.snapshot {
            if !snapshot.is_empty() {
                self.prev_stabled = snapshot.metadata.index;
                self.prev_applied = snapshot.metadata.index;
                self.raft.raft_log.stabled = snapshot.metadata.index;
                self.raft.raft_log.applied = snapshot.metadata.index;
                // 清除 pending_snapshot
                self.raft.raft_log.take_pending_snapshot();
            }
        }
    }

    /// ApplyConfChange 应用配置变更
    pub fn apply_conf_change(&mut self, cc: &ConfChange) -> ConfState {
        match cc.change_type {
            ConfChangeType::AddNode => {
                self.raft.add_node(cc.node_id);
            }
            ConfChangeType::RemoveNode => {
                self.raft.remove_node(cc.node_id);
            }
        }

        // 返回当前的配置状态
        let mut nodes = vec![self.raft.id];
        nodes.extend(self.raft.prs.keys().cloned());
        nodes.sort();
        ConfState { nodes }
    }

    /// GetProgress 获取所有 peer 的进度（仅在 leader 时有效）
    pub fn get_progress(&self) -> Option<std::collections::HashMap<u64, Progress>> {
        if self.raft.state != StateType::Leader {
            return None;
        }

        Some(self.raft.prs.clone())
    }

    /// TransferLeader 转移领导权
    pub fn transfer_leader(&mut self, transferee: u64) -> Result<()> {
        if self.raft.state != StateType::Leader {
            return Err(anyhow::anyhow!("not leader"));
        }

        if transferee == self.raft.id {
            return Ok(());
        }

        if !self.raft.prs.contains_key(&transferee) {
            return Err(anyhow::anyhow!("transferee {} not found", transferee));
        }

        self.raft.step(Message {
            msg_type: MessageType::MsgTransferLeader,
            from: self.raft.id,
            to: transferee,
            ..Message::new()
        })
    }

    /// GetRaft 获取内部的 Raft 实例（用于测试）
    pub fn get_raft(&self) -> &Raft {
        &self.raft
    }

    /// GetRaftMut 获取可变的 Raft 实例（用于测试）
    pub fn get_raft_mut(&mut self) -> &mut Raft {
        &mut self.raft
    }
}