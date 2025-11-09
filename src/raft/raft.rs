use crate::raft::types::*;
use crate::raft::log::RaftLog;
use crate::raft::progress::Progress;
use std::collections::HashMap;
use rand::Rng;
use anyhow::Result;

pub struct Raft {
    pub id: u64,
    
    pub term: u64,
    pub vote: u64,
    
    pub raft_log: RaftLog,
    
    /// 每个 peer 的复制进度（leader 使用）
    pub prs: HashMap<u64, Progress>,
    
    /// 当前角色
    pub state: StateType,
    
    /// 投票记录（candidate 使用）
    votes: HashMap<u64, bool>,
    
    /// 待发送的消息
    pub msgs: Vec<Message>,
    
    /// Leader ID
    pub lead: u64,
    
    /// 心跳超时间隔（tick 数）
    heartbeat_timeout: usize,
    /// 选举超时基准（tick 数）
    election_timeout: usize,
    /// 自上次心跳超时后的 tick 数
    heartbeat_elapsed: usize,
    /// 自上次选举超时后的 tick 数
    election_elapsed: usize,
    /// 随机选举超时（避免同时选举）
    random_election_timeout: usize,
}

impl Raft {
    /// 创建新的 Raft 实例
    pub async fn new(config: RaftConfig) -> Result<Self> {
        config.validate()?;
        
        let storage = config.storage;
        let (hard_state, conf_state) = storage.initial_state().await?;
        
        let mut raft_log = RaftLog::new(storage).await?;
        raft_log.applied = config.applied;
        
        // 初始化 peer 进度
        let mut prs = HashMap::new();
        let last_index = raft_log.last_index();
        for &peer_id in &config.peers {
            if peer_id != config.id {
                prs.insert(peer_id, Progress::new(last_index + 1));
            }
        }
        
        // 如果没有 peers，从 conf_state 中获取
        if prs.is_empty() && !conf_state.nodes.is_empty() {
            for &peer_id in &conf_state.nodes {
                if peer_id != config.id {
                    prs.insert(peer_id, Progress::new(last_index + 1));
                }
            }
        }
        
        // 生成随机选举超时
        let mut rng = rand::thread_rng();
        let random_election_timeout = rng.gen_range(config.election_tick..=config.election_tick * 2);
        
        Ok(Self {
            id: config.id,
            term: hard_state.term,
            vote: hard_state.vote,
            raft_log,
            prs,
            state: StateType::Follower,
            votes: HashMap::new(),
            msgs: Vec::new(),
            lead: NONE,
            heartbeat_timeout: config.heartbeat_tick,
            election_timeout: config.election_tick,
            heartbeat_elapsed: 0,
            election_elapsed: 0,
            random_election_timeout,
        })
    }

    /// Tick：推进逻辑时钟
    pub fn tick(&mut self) {
        match self.state {
            StateType::Leader => {
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= self.heartbeat_timeout {
                    self.heartbeat_elapsed = 0;
                    self.step(Message {
                        msg_type: MessageType::MsgBeat,
                        from: self.id,
                        ..Message::new()
                    }).ok();
                }
            }
            StateType::Follower | StateType::Candidate => {
                self.election_elapsed += 1;
                if self.election_elapsed >= self.random_election_timeout {
                    self.election_elapsed = 0;
                    self.step(Message {
                        msg_type: MessageType::MsgHup,
                        from: self.id,
                        ..Message::new()
                    }).ok();
                }
            }
        }
    }

    /// Step：消息处理入口（同步版本，使用同步的 term 方法）
    pub fn step(&mut self, m: Message) -> Result<()> {
        // 如果消息的 term 更大，转为 follower
        if m.term > self.term {
            if m.msg_type == MessageType::MsgRequestVote || m.msg_type == MessageType::MsgAppend {
                self.become_follower(m.term, NONE);
            } else {
                self.become_follower(m.term, m.from);
            }
        }
        
        // 根据当前状态处理消息
        match self.state {
            StateType::Follower => self.step_follower(m)?,
            StateType::Candidate => self.step_candidate(m)?,
            StateType::Leader => self.step_leader(m)?,
        }
        
        Ok(())
    }

    /// Step 异步版本：用于需要访问存储的场景
    pub async fn step_async(&mut self, m: Message) -> Result<()> {
        // 如果消息的 term 更大，转为 follower
        if m.term > self.term {
            if m.msg_type == MessageType::MsgRequestVote || m.msg_type == MessageType::MsgAppend {
                self.become_follower(m.term, NONE);
            } else {
                self.become_follower(m.term, m.from);
            }
        }
        
        // 根据当前状态处理消息
        match self.state {
            StateType::Follower => self.step_follower_async(m).await?,
            StateType::Candidate => self.step_candidate_async(m).await?,
            StateType::Leader => self.step_leader_async(m).await?,
        }
        
        Ok(())
    }

    /// Follower 状态的消息处理
    fn step_follower(&mut self, m: Message) -> Result<()> {
        match m.msg_type {
            MessageType::MsgHup => {
                self.campaign()?;
            }
            MessageType::MsgBeat | MessageType::MsgPropose => {
                // 转发给 leader
                if self.lead != NONE {
                    let mut msg = m.clone();
                    msg.to = self.lead;
                    self.msgs.push(msg);
                }
            }
            MessageType::MsgAppend => {
                self.election_elapsed = 0;
                self.handle_append_entries(m)?;
            }
            MessageType::MsgHeartbeat => {
                self.election_elapsed = 0;
                self.handle_heartbeat(m)?;
            }
            MessageType::MsgSnapshot => {
                self.election_elapsed = 0;
                self.handle_snapshot(m)?;
            }
            MessageType::MsgRequestVote => {
                self.handle_request_vote(m)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Follower 状态的消息处理（异步版本）
    async fn step_follower_async(&mut self, m: Message) -> Result<()> {
        match m.msg_type {
            MessageType::MsgHup => {
                self.campaign_async().await?;
            }
            MessageType::MsgBeat | MessageType::MsgPropose => {
                if self.lead != NONE {
                    let mut msg = m.clone();
                    msg.to = self.lead;
                    self.msgs.push(msg);
                }
            }
            MessageType::MsgAppend => {
                self.election_elapsed = 0;
                self.handle_append_entries_async(m).await?;
            }
            MessageType::MsgHeartbeat => {
                self.election_elapsed = 0;
                self.handle_heartbeat(m)?;
            }
            MessageType::MsgSnapshot => {
                self.election_elapsed = 0;
                self.handle_snapshot(m)?;
            }
            MessageType::MsgRequestVote => {
                self.handle_request_vote_async(m).await?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Candidate 状态的消息处理
    fn step_candidate(&mut self, m: Message) -> Result<()> {
        match m.msg_type {
            MessageType::MsgHup => {
                self.campaign()?;
            }
            MessageType::MsgPropose => {
                // Candidate 忽略提案
            }
            MessageType::MsgAppend => {
                // 收到 AppendEntries，说明有合法的 leader，转为 follower
                self.become_follower(m.term, m.from);
                self.handle_append_entries(m)?;
            }
            MessageType::MsgHeartbeat => {
                self.become_follower(m.term, m.from);
                self.handle_heartbeat(m)?;
            }
            MessageType::MsgSnapshot => {
                self.become_follower(m.term, m.from);
                self.handle_snapshot(m)?;
            }
            MessageType::MsgRequestVote => {
                self.handle_request_vote(m)?;
            }
            MessageType::MsgRequestVoteResponse => {
                self.handle_request_vote_response(m)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Candidate 状态的消息处理（异步版本）
    async fn step_candidate_async(&mut self, m: Message) -> Result<()> {
        match m.msg_type {
            MessageType::MsgHup => {
                self.campaign_async().await?;
            }
            MessageType::MsgPropose => {}
            MessageType::MsgAppend => {
                self.become_follower(m.term, m.from);
                self.handle_append_entries_async(m).await?;
            }
            MessageType::MsgHeartbeat => {
                self.become_follower(m.term, m.from);
                self.handle_heartbeat(m)?;
            }
            MessageType::MsgSnapshot => {
                self.become_follower(m.term, m.from);
                self.handle_snapshot(m)?;
            }
            MessageType::MsgRequestVote => {
                self.handle_request_vote_async(m).await?;
            }
            MessageType::MsgRequestVoteResponse => {
                self.handle_request_vote_response(m)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Leader 状态的消息处理
    fn step_leader(&mut self, m: Message) -> Result<()> {
        match m.msg_type {
            MessageType::MsgBeat => {
                self.bcast_heartbeat()?;
            }
            MessageType::MsgPropose => {
                // Leader 总是可以处理提案（包括单节点集群）
                // 追加到本地日志
                let last_index = self.raft_log.last_index();
                let mut entries = Vec::new();
                for (i, entry) in m.entries.iter().enumerate() {
                    // 保留原始 entry 的类型（Normal 或 ConfChange）
                    let new_entry = if entry.entry_type == EntryType::ConfChange {
                        Entry::new_conf_change(
                            self.term,
                            last_index + 1 + i as u64,
                            entry.data.clone(),
                        )
                    } else {
                        Entry::new_normal(
                            self.term,
                            last_index + 1 + i as u64,
                            entry.data.clone(),
                        )
                    };
                    entries.push(new_entry);
                }
                self.raft_log.append(&entries);
                self.maybe_commit()?;
                self.bcast_append()?;
            }
            MessageType::MsgAppendResponse => {
                self.handle_append_response(m)?;
            }
            MessageType::MsgHeartbeatResponse => {
                self.handle_heartbeat_response(m)?;
            }
            MessageType::MsgRequestVote => {
                self.handle_request_vote(m)?;
            }
            MessageType::MsgSnapshot => {
                self.become_follower(m.term, m.from);
                self.handle_snapshot(m)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Leader 状态的消息处理（异步版本）
    async fn step_leader_async(&mut self, m: Message) -> Result<()> {
        match m.msg_type {
            MessageType::MsgBeat => {
                self.bcast_heartbeat()?;
            }
            MessageType::MsgPropose => {
                // Leader 总是可以处理提案（包括单节点集群）
                let last_index = self.raft_log.last_index();
                let mut entries = Vec::new();
                for (i, entry) in m.entries.iter().enumerate() {
                    // 保留原始 entry 的类型（Normal 或 ConfChange）
                    let new_entry = if entry.entry_type == EntryType::ConfChange {
                        Entry::new_conf_change(
                            self.term,
                            last_index + 1 + i as u64,
                            entry.data.clone(),
                        )
                    } else {
                        Entry::new_normal(
                            self.term,
                            last_index + 1 + i as u64,
                            entry.data.clone(),
                        )
                    };
                    entries.push(new_entry);
                }
                self.raft_log.append(&entries);
                self.maybe_commit_async().await?;
                self.bcast_append_async().await?;
            }
            MessageType::MsgAppendResponse => {
                self.handle_append_response_async(m).await?;
            }
            MessageType::MsgHeartbeatResponse => {
                self.handle_heartbeat_response_async(m).await?;
            }
            MessageType::MsgRequestVote => {
                self.handle_request_vote_async(m).await?;
            }
            MessageType::MsgSnapshot => {
                self.become_follower(m.term, m.from);
                self.handle_snapshot(m)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// 成为 Follower
    pub fn become_follower(&mut self, term: u64, lead: u64) {
        self.term = term;
        self.lead = lead;
        self.state = StateType::Follower;
        self.vote = NONE;
        self.votes.clear();
        self.election_elapsed = 0;
        // 重新生成随机选举超时
        let mut rng = rand::thread_rng();
        self.random_election_timeout = rng.gen_range(self.election_timeout..=self.election_timeout * 2);
    }

    /// 成为 Candidate
    pub fn become_candidate(&mut self) {
        self.term += 1;
        self.state = StateType::Candidate;
        self.vote = self.id;
        self.votes.clear();
        self.votes.insert(self.id, true);
        self.lead = NONE;
        self.election_elapsed = 0;
        // 重新生成随机选举超时
        let mut rng = rand::thread_rng();
        self.random_election_timeout = rng.gen_range(self.election_timeout..=self.election_timeout * 2);
    }

    /// 成为 Leader
    pub fn become_leader(&mut self) {
        self.state = StateType::Leader;
        self.lead = self.id;
        self.heartbeat_elapsed = 0;
        
        // 初始化所有 peer 的进度
        let last_index = self.raft_log.last_index();
        for (_, progress) in self.prs.iter_mut() {
            progress.next_index = last_index + 1;
            progress.match_index = 0;
        }
        
        // 追加一个 noop 条目，确保 leader 的日志至少有一条当前 term 的条目
        let noop_entry = Entry::new_normal(self.term, last_index + 1, Vec::new());
        self.raft_log.append(&[noop_entry.clone()]);
        self.bcast_append().ok();
    }

    /// 开始选举（同步版本，使用同步 term）
    fn campaign(&mut self) -> Result<()> {
        self.become_candidate();
        
        if self.quorum() == 1 {
            // 只有自己，直接成为 leader
            self.become_leader();
            return Ok(());
        }
        
        let last_index = self.raft_log.last_index();
        let last_term = self.raft_log.term_sync(last_index)
            .ok_or_else(|| anyhow::anyhow!("cannot get term for index {}", last_index))?;
        
        // 向所有 peer 发送投票请求
        for &peer_id in self.prs.keys() {
            if peer_id == self.id {
                continue;
            }
            
            self.msgs.push(Message {
                msg_type: MessageType::MsgRequestVote,
                to: peer_id,
                from: self.id,
                term: self.term,
                log_term: last_term,
                index: last_index,
                ..Message::new()
            });
        }
        
        Ok(())
    }

    /// 开始选举（异步版本）
    async fn campaign_async(&mut self) -> Result<()> {
        self.become_candidate();
        
        if self.quorum() == 1 {
            self.become_leader();
            return Ok(());
        }
        
        let last_index = self.raft_log.last_index();
        let last_term = self.raft_log.term(last_index).await?;
        
        for &peer_id in self.prs.keys() {
            if peer_id == self.id {
                continue;
            }
            
            self.msgs.push(Message {
                msg_type: MessageType::MsgRequestVote,
                to: peer_id,
                from: self.id,
                term: self.term,
                log_term: last_term,
                index: last_index,
                ..Message::new()
            });
        }
        
        Ok(())
    }

    /// 处理 AppendEntries 请求（同步版本）
    fn handle_append_entries(&mut self, m: Message) -> Result<()> {
        if m.term < self.term {
            self.msgs.push(Message {
                msg_type: MessageType::MsgAppendResponse,
                to: m.from,
                from: self.id,
                term: self.term,
                reject: true,
                ..Message::new()
            });
            return Ok(());
        }
        
        if m.term > self.term {
            self.term = m.term;
        }
        
        self.lead = m.from;
        
        // 检查日志匹配
        if m.index > self.raft_log.last_index() {
            self.msgs.push(Message {
                msg_type: MessageType::MsgAppendResponse,
                to: m.from,
                from: self.id,
                term: self.term,
                reject: true,
                index: self.raft_log.last_index(),
                ..Message::new()
            });
            return Ok(());
        }
        
        // 检查 prev_log_term 是否匹配
        if m.index > 0 {
            let term = self.raft_log.term_sync(m.index)
                .ok_or_else(|| anyhow::anyhow!("cannot get term for index {}", m.index))?;
            if term != m.log_term {
                self.msgs.push(Message {
                    msg_type: MessageType::MsgAppendResponse,
                    to: m.from,
                    from: self.id,
                    term: self.term,
                    reject: true,
                    index: m.index - 1,
                    ..Message::new()
                });
                return Ok(());
            }
        }
        
        // 追加新条目
        if !m.entries.is_empty() {
            self.raft_log.append(&m.entries);
        }
        
        // 更新 commit index
        if m.commit > self.raft_log.committed {
            self.raft_log.committed = m.commit.min(self.raft_log.last_index());
        }
        
        // 发送成功响应
        self.msgs.push(Message {
            msg_type: MessageType::MsgAppendResponse,
            to: m.from,
            from: self.id,
            term: self.term,
            reject: false,
            index: self.raft_log.last_index(),
            ..Message::new()
        });
        
        Ok(())
    }

    /// 处理 AppendEntries 请求（异步版本）
    async fn handle_append_entries_async(&mut self, m: Message) -> Result<()> {
        if m.term < self.term {
            self.msgs.push(Message {
                msg_type: MessageType::MsgAppendResponse,
                to: m.from,
                from: self.id,
                term: self.term,
                reject: true,
                ..Message::new()
            });
            return Ok(());
        }
        
        if m.term > self.term {
            self.term = m.term;
        }
        
        self.lead = m.from;
        
        if m.index > self.raft_log.last_index() {
            self.msgs.push(Message {
                msg_type: MessageType::MsgAppendResponse,
                to: m.from,
                from: self.id,
                term: self.term,
                reject: true,
                index: self.raft_log.last_index(),
                ..Message::new()
            });
            return Ok(());
        }
        
        if m.index > 0 {
            let term = self.raft_log.term(m.index).await?;
            if term != m.log_term {
                self.msgs.push(Message {
                    msg_type: MessageType::MsgAppendResponse,
                    to: m.from,
                    from: self.id,
                    term: self.term,
                    reject: true,
                    index: m.index - 1,
                    ..Message::new()
                });
                return Ok(());
            }
        }
        
        if !m.entries.is_empty() {
            self.raft_log.append(&m.entries);
        }
        
        if m.commit > self.raft_log.committed {
            self.raft_log.committed = m.commit.min(self.raft_log.last_index());
        }
        
        self.msgs.push(Message {
            msg_type: MessageType::MsgAppendResponse,
            to: m.from,
            from: self.id,
            term: self.term,
            reject: false,
            index: self.raft_log.last_index(),
            ..Message::new()
        });
        
        Ok(())
    }

    /// 处理 Heartbeat 请求
    fn handle_heartbeat(&mut self, m: Message) -> Result<()> {
        if m.term < self.term {
            return Ok(());
        }
        
        if m.term > self.term {
            self.term = m.term;
        }
        
        self.lead = m.from;
        
        if m.commit > self.raft_log.committed {
            self.raft_log.committed = m.commit.min(self.raft_log.last_index());
        }
        
        self.msgs.push(Message {
            msg_type: MessageType::MsgHeartbeatResponse,
            to: m.from,
            from: self.id,
            term: self.term,
            ..Message::new()
        });
        
        Ok(())
    }

    /// 处理 Snapshot
    fn handle_snapshot(&mut self, m: Message) -> Result<()> {
        if let Some(snapshot) = m.snapshot {
            if snapshot.metadata.index <= self.raft_log.committed {
                return Ok(());
            }
            
            self.raft_log.apply_snapshot(snapshot)?;
            
            self.msgs.push(Message {
                msg_type: MessageType::MsgAppendResponse,
                to: m.from,
                from: self.id,
                term: self.term,
                reject: false,
                index: self.raft_log.last_index(),
                ..Message::new()
            });
        }
        
        Ok(())
    }

    /// 处理 RequestVote 请求（同步版本）
    fn handle_request_vote(&mut self, m: Message) -> Result<()> {
        if m.term < self.term {
            self.msgs.push(Message {
                msg_type: MessageType::MsgRequestVoteResponse,
                to: m.from,
                from: self.id,
                term: self.term,
                reject: true,
                ..Message::new()
            });
            return Ok(());
        }
        
        if m.term > self.term {
            self.become_follower(m.term, NONE);
        }
        
        let can_vote = self.vote == NONE || self.vote == m.from;
        let last_index = self.raft_log.last_index();
        let last_term = self.raft_log.term_sync(last_index)
            .unwrap_or(0); // 如果获取不到，使用 0
        
        let log_ok = m.log_term > last_term || (m.log_term == last_term && m.index >= last_index);
        
        if can_vote && log_ok {
            self.vote = m.from;
            self.election_elapsed = 0;
            self.msgs.push(Message {
                msg_type: MessageType::MsgRequestVoteResponse,
                to: m.from,
                from: self.id,
                term: self.term,
                reject: false,
                ..Message::new()
            });
        } else {
            self.msgs.push(Message {
                msg_type: MessageType::MsgRequestVoteResponse,
                to: m.from,
                from: self.id,
                term: self.term,
                reject: true,
                ..Message::new()
            });
        }
        
        Ok(())
    }

    /// 处理 RequestVote 请求（异步版本）
    async fn handle_request_vote_async(&mut self, m: Message) -> Result<()> {
        if m.term < self.term {
            self.msgs.push(Message {
                msg_type: MessageType::MsgRequestVoteResponse,
                to: m.from,
                from: self.id,
                term: self.term,
                reject: true,
                ..Message::new()
            });
            return Ok(());
        }
        
        if m.term > self.term {
            self.become_follower(m.term, NONE);
        }
        
        let can_vote = self.vote == NONE || self.vote == m.from;
        let last_index = self.raft_log.last_index();
        let last_term = self.raft_log.term(last_index).await?;
        
        let log_ok = m.log_term > last_term || (m.log_term == last_term && m.index >= last_index);
        
        if can_vote && log_ok {
            self.vote = m.from;
            self.election_elapsed = 0;
            self.msgs.push(Message {
                msg_type: MessageType::MsgRequestVoteResponse,
                to: m.from,
                from: self.id,
                term: self.term,
                reject: false,
                ..Message::new()
            });
        } else {
            self.msgs.push(Message {
                msg_type: MessageType::MsgRequestVoteResponse,
                to: m.from,
                from: self.id,
                term: self.term,
                reject: true,
                ..Message::new()
            });
        }
        
        Ok(())
    }

    /// 处理 RequestVote 响应
    fn handle_request_vote_response(&mut self, m: Message) -> Result<()> {
        if m.term != self.term || self.state != StateType::Candidate {
            return Ok(());
        }
        
        self.votes.insert(m.from, !m.reject);
        
        let mut granted = 0;
        let mut rejected = 0;
        for (_, &voted) in &self.votes {
            if voted {
                granted += 1;
            } else {
                rejected += 1;
            }
        }
        
        let quorum = self.quorum();
        if granted >= quorum {
            self.become_leader();
        } else if rejected >= quorum {
            self.become_follower(self.term, NONE);
        }
        
        Ok(())
    }

    /// 处理 AppendResponse（同步版本）
    fn handle_append_response(&mut self, m: Message) -> Result<()> {
        if m.term < self.term {
            return Ok(());
        }
        
        if let Some(progress) = self.prs.get_mut(&m.from) {
            if m.reject {
                progress.decrease_next(m.index);
            } else {
                progress.update_match(m.index);
            }
        }
        
        // 在释放 progress 借用后调用其他方法
        if !m.reject {
            self.maybe_commit()?;
        }
        
        if let Some(progress) = self.prs.get(&m.from) {
            if m.reject {
                self.send_append(m.from)?;
            } else if progress.next_index <= self.raft_log.last_index() {
                self.send_append(m.from)?;
            }
        }
        Ok(())
    }

    /// 处理 AppendResponse（异步版本）
    async fn handle_append_response_async(&mut self, m: Message) -> Result<()> {
        if m.term < self.term {
            return Ok(());
        }
        
        if let Some(progress) = self.prs.get_mut(&m.from) {
            if m.reject {
                progress.decrease_next(m.index);
            } else {
                progress.update_match(m.index);
            }
        }
        
        // 在释放 progress 借用后调用其他方法
        if !m.reject {
            self.maybe_commit_async().await?;
        }
        
        if let Some(progress) = self.prs.get(&m.from) {
            if m.reject {
                self.send_append_async(m.from).await?;
            } else if progress.next_index <= self.raft_log.last_index() {
                self.send_append_async(m.from).await?;
            }
        }
        Ok(())
    }

    /// 处理 HeartbeatResponse（同步版本）
    fn handle_heartbeat_response(&mut self, m: Message) -> Result<()> {
        if m.term < self.term {
            return Ok(());
        }
        
        if let Some(progress) = self.prs.get_mut(&m.from) {
            if progress.next_index <= self.raft_log.last_index() {
                self.send_append(m.from)?;
            }
        }
        
        Ok(())
    }

    /// 处理 HeartbeatResponse（异步版本）
    async fn handle_heartbeat_response_async(&mut self, m: Message) -> Result<()> {
        if m.term < self.term {
            return Ok(());
        }
        
        if let Some(progress) = self.prs.get_mut(&m.from) {
            if progress.next_index <= self.raft_log.last_index() {
                self.send_append_async(m.from).await?;
            }
        }
        
        Ok(())
    }

    /// 发送 AppendEntries 到指定 peer（同步版本）
    fn send_append(&mut self, to: u64) -> Result<()> {
        let progress = match self.prs.get(&to) {
            Some(p) => p.clone(),
            None => return Ok(()),
        };
        
        let next_index = progress.next_index;
        let last_index = self.raft_log.last_index();
        
        if next_index > last_index + 1 {
            return Ok(());
        }
        
        let mut entries = Vec::new();
        if next_index <= last_index {
            let first = self.raft_log.first_index();
            let start = (next_index - first) as usize;
            let end = (last_index - first + 1) as usize;
            entries = self.raft_log.all_entries()[start..end.min(self.raft_log.all_entries().len())].to_vec();
        }
        
        let prev_log_term = if next_index > 0 {
            self.raft_log.term_sync(next_index - 1).unwrap_or(0)
        } else {
            0
        };
        
        self.msgs.push(Message {
            msg_type: MessageType::MsgAppend,
            to,
            from: self.id,
            term: self.term,
            log_term: prev_log_term,
            index: next_index - 1,
            entries,
            commit: self.raft_log.committed,
            ..Message::new()
        });
        
        Ok(())
    }

    /// 发送 AppendEntries 到指定 peer（异步版本）
    async fn send_append_async(&mut self, to: u64) -> Result<()> {
        let progress = match self.prs.get(&to) {
            Some(p) => p.clone(),
            None => return Ok(()),
        };
        
        let next_index = progress.next_index;
        let last_index = self.raft_log.last_index();
        
        if next_index > last_index + 1 {
            return Ok(());
        }
        
        let mut entries = Vec::new();
        if next_index <= last_index {
            let first = self.raft_log.first_index();
            let start = (next_index - first) as usize;
            let end = (last_index - first + 1) as usize;
            entries = self.raft_log.all_entries()[start..end.min(self.raft_log.all_entries().len())].to_vec();
        }
        
        let prev_log_term = if next_index > 0 {
            self.raft_log.term(next_index - 1).await.unwrap_or(0)
        } else {
            0
        };
        
        self.msgs.push(Message {
            msg_type: MessageType::MsgAppend,
            to,
            from: self.id,
            term: self.term,
            log_term: prev_log_term,
            index: next_index - 1,
            entries,
            commit: self.raft_log.committed,
            ..Message::new()
        });
        
        Ok(())
    }

    /// 发送 Heartbeat 到指定 peer
    fn send_heartbeat(&mut self, to: u64) {
        let commit = self.raft_log.committed;
        self.msgs.push(Message {
            msg_type: MessageType::MsgHeartbeat,
            to,
            from: self.id,
            term: self.term,
            commit,
            ..Message::new()
        });
    }

    /// 广播 AppendEntries（同步版本）
    fn bcast_append(&mut self) -> Result<()> {
        let peer_ids: Vec<u64> = self.prs.keys().cloned().collect();
        for peer_id in peer_ids {
            if peer_id != self.id {
                self.send_append(peer_id)?;
            }
        }
        Ok(())
    }

    /// 广播 AppendEntries（异步版本）
    async fn bcast_append_async(&mut self) -> Result<()> {
        let peer_ids: Vec<u64> = self.prs.keys().cloned().collect();
        for peer_id in peer_ids {
            if peer_id != self.id {
                self.send_append_async(peer_id).await?;
            }
        }
        Ok(())
    }

    /// 广播 Heartbeat
    fn bcast_heartbeat(&mut self) -> Result<()> {
        let peer_ids: Vec<u64> = self.prs.keys().cloned().collect();
        for peer_id in peer_ids {
            if peer_id != self.id {
                self.send_heartbeat(peer_id);
            }
        }
        Ok(())
    }

    /// 尝试提交日志（同步版本）
    fn maybe_commit(&mut self) -> Result<()> {
        if self.state != StateType::Leader {
            return Ok(());
        }
        
        let mut matches: Vec<u64> = self.prs.values().map(|p| p.match_index).collect();
        matches.sort();
        matches.reverse();
        
        let quorum = self.quorum();
        if matches.len() >= quorum {
            let commit_index = matches[quorum - 1];
            if commit_index > self.raft_log.committed {
                let term = self.raft_log.term_sync(commit_index).unwrap_or(0);
                if term == self.term {
                    self.raft_log.committed = commit_index;
                }
            }
        }
        
        Ok(())
    }

    /// 尝试提交日志（异步版本）
    async fn maybe_commit_async(&mut self) -> Result<()> {
        if self.state != StateType::Leader {
            return Ok(());
        }
        
        let mut matches: Vec<u64> = self.prs.values().map(|p| p.match_index).collect();
        matches.sort();
        matches.reverse();
        
        let quorum = self.quorum();
        if matches.len() >= quorum {
            let commit_index = matches[quorum - 1];
            if commit_index > self.raft_log.committed {
                let term = self.raft_log.term(commit_index).await?;
                if term == self.term {
                    self.raft_log.committed = commit_index;
                }
            }
        }
        
        Ok(())
    }

    /// 计算法定人数
    fn quorum(&self) -> usize {
        (self.prs.len() + 1) / 2 + 1
    }

    /// 添加节点
    pub fn add_node(&mut self, id: u64) {
        if id == self.id {
            return;
        }
        
        if !self.prs.contains_key(&id) {
            let next_index = self.raft_log.last_index() + 1;
            self.prs.insert(id, Progress::new(next_index));
        }
    }

    /// 删除节点
    pub fn remove_node(&mut self, id: u64) {
        if id == self.id {
            return;
        }
        
        self.prs.remove(&id);
        
        if self.state == StateType::Leader {
            self.maybe_commit().ok();
        }
    }
}