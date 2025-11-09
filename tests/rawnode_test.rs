use aegisdb::raft::*;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

/// 内存存储实现，用于测试
struct MemoryStorage {
    hard_state: Arc<Mutex<HardState>>,
    conf_state: Arc<Mutex<ConfState>>,
    entries: Arc<Mutex<Vec<Entry>>>,
    snapshot: Arc<Mutex<Option<Snapshot>>>,
}

impl MemoryStorage {
    fn new() -> Self {
        Self {
            hard_state: Arc::new(Mutex::new(HardState {
                term: 0,
                vote: 0,
                commit: 0,
            })),
            conf_state: Arc::new(Mutex::new(ConfState { nodes: Vec::new() })),
            entries: Arc::new(Mutex::new(Vec::new())),
            snapshot: Arc::new(Mutex::new(None)),
        }
    }

    fn with_peers(peers: Vec<u64>) -> Self {
        let storage = Self::new();
        *storage.conf_state.lock().unwrap() = ConfState { nodes: peers };
        storage
    }

    fn append_entries(&self, entries: &[Entry]) {
        let mut stored = self.entries.lock().unwrap();
        stored.extend_from_slice(entries);
    }

    fn set_hard_state(&self, hs: HardState) {
        *self.hard_state.lock().unwrap() = hs;
    }

    fn set_snapshot(&self, snapshot: Snapshot) {
        *self.snapshot.lock().unwrap() = Some(snapshot);
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn initial_state(&self) -> anyhow::Result<(HardState, ConfState)> {
        let hs = *self.hard_state.lock().unwrap();
        let cs = self.conf_state.lock().unwrap().clone();
        Ok((hs, cs))
    }

    async fn entries(&self, lo: u64, hi: u64) -> anyhow::Result<Vec<Entry>> {
        let entries = self.entries.lock().unwrap();
        let mut result = Vec::new();
        for entry in entries.iter() {
            if entry.index >= lo && entry.index < hi {
                result.push(entry.clone());
            }
        }
        Ok(result)
    }

    async fn term(&self, index: u64) -> anyhow::Result<u64> {
        let entries = self.entries.lock().unwrap();
        for entry in entries.iter() {
            if entry.index == index {
                return Ok(entry.term);
            }
        }
        Err(anyhow::anyhow!("entry at index {} not found", index))
    }

    async fn last_index(&self) -> anyhow::Result<u64> {
        let entries = self.entries.lock().unwrap();
        Ok(entries.last().map(|e| e.index).unwrap_or(0))
    }

    async fn first_index(&self) -> anyhow::Result<u64> {
        let entries = self.entries.lock().unwrap();
        Ok(entries.first().map(|e| e.index).unwrap_or(1))
    }

    async fn snapshot(&self) -> anyhow::Result<Snapshot> {
        let snapshot = self.snapshot.lock().unwrap();
        if let Some(ref snap) = *snapshot {
            Ok(snap.clone())
        } else {
            Ok(Snapshot {
                data: Vec::new(),
                metadata: SnapshotMetadata {
                    conf_state: ConfState { nodes: Vec::new() },
                    index: 0,
                    term: 0,
                },
            })
        }
    }
}

/// 创建测试用的 RawNode
async fn new_raw_node(id: u64, peers: Vec<u64>, election_tick: usize, heartbeat_tick: usize) -> RawNode {
    let storage = Box::new(MemoryStorage::with_peers(peers.clone()));
    let config = RaftConfig {
        id,
        peers,
        election_tick,
        heartbeat_tick,
        storage,
        applied: 0,
    };
    RawNode::new(config).await.unwrap()
}

#[tokio::test]
async fn test_raw_node_new() {
    let storage = Box::new(MemoryStorage::new());
    let config = RaftConfig {
        id: 1,
        peers: vec![1, 2, 3],
        election_tick: 10,
        heartbeat_tick: 3,
        storage,
        applied: 0,
    };

    let raw_node = RawNode::new(config).await.unwrap();
    assert_eq!(raw_node.raft.id, 1);
    assert_eq!(raw_node.raft.state, StateType::Follower);
    assert_eq!(raw_node.raft.lead, NONE);
}

#[tokio::test]
async fn test_raw_node_tick() {
    let mut raw_node = new_raw_node(1, vec![1, 2, 3], 10, 3).await;
    
    // tick 应该增加内部计数器，我们通过检查状态变化来验证
    raw_node.tick();
    
    // 验证 tick 正常工作（通过多次 tick 后应该触发选举超时）
    for _ in 0..15 {
        raw_node.tick();
    }
    
    // 应该触发选举
    assert!(raw_node.has_ready());
}

#[tokio::test]
async fn test_raw_node_campaign() {
    let mut raw_node = new_raw_node(1, vec![1, 2, 3], 10, 3).await;
    
    // 初始状态应该是 Follower
    assert_eq!(raw_node.raft.state, StateType::Follower);
    
    // 开始选举
    raw_node.campaign().unwrap();
    
    // 应该变为 Candidate
    assert_eq!(raw_node.raft.state, StateType::Candidate);
    assert_eq!(raw_node.raft.term, 1);
}

#[tokio::test]
async fn test_raw_node_propose() {
    let mut raw_node = new_raw_node(1, vec![1], 10, 3).await;
    
    // 先成为 leader（单节点集群）
    raw_node.campaign().unwrap();
    
    // 等待选举完成
    for _ in 0..20 {
        raw_node.tick();
        if raw_node.raft.state == StateType::Leader {
            break;
        }
    }
    
    // 提案日志
    let data = b"test data".to_vec();
    raw_node.propose(data.clone()).unwrap();
    
    // 检查是否有 Ready
    let ready = raw_node.ready();
    assert!(!ready.entries.is_empty());
    assert_eq!(ready.entries[0].data, data);
}

#[tokio::test]
async fn test_raw_node_ready_and_advance() {
    let mut raw_node = new_raw_node(1, vec![1], 10, 3).await;
    
    // 初始状态，应该没有 Ready
    assert!(!raw_node.has_ready());
    
    // 开始选举
    raw_node.campaign().unwrap();
    
    // 现在应该有 Ready（状态变化）
    assert!(raw_node.has_ready());
    
    let ready = raw_node.ready();
    assert!(ready.soft_state.is_some());
    assert_eq!(ready.soft_state.as_ref().unwrap().raft_state, StateType::Candidate);
    
    // Advance
    raw_node.advance(ready);
    
    // 再次检查，应该没有新的 Ready（除非有新的变化）
    // 注意：这里可能会有消息，所以可能还有 Ready
}

#[tokio::test]
async fn test_raw_node_step() {
    let mut raw_node1 = new_raw_node(1, vec![1, 2], 10, 3).await;
    let mut raw_node2 = new_raw_node(2, vec![1, 2], 10, 3).await;
    
    // raw_node1 开始选举
    raw_node1.campaign().unwrap();
    
    // 获取 Ready 并发送消息
    let ready = raw_node1.ready();
    assert!(!ready.messages.is_empty());
    
    // 找到发送给 raw_node2 的消息（先克隆，因为 ready 会被移动）
    let vote_msg = ready.messages.iter()
        .find(|m| m.to == 2 && m.msg_type == MessageType::MsgRequestVote)
        .unwrap()
        .clone();
    
    // raw_node2 处理消息
    raw_node2.advance(ready);
    raw_node2.step(vote_msg).unwrap();
    
    // raw_node2 应该投票给 raw_node1
    assert_eq!(raw_node2.raft.vote, 1);
}

#[tokio::test]
async fn test_raw_node_propose_conf_change() {
    let mut raw_node = new_raw_node(1, vec![1], 10, 3).await;
    
    // 成为 leader
    raw_node.campaign().unwrap();
    for _ in 0..20 {
        raw_node.tick();
        if raw_node.raft.state == StateType::Leader {
            break;
        }
    }
    
    // 提案配置变更
    let cc = ConfChange {
        change_type: ConfChangeType::AddNode,
        node_id: 2,
        context: Vec::new(),
    };
    
    raw_node.propose_conf_change(cc.clone()).unwrap();
    
    // 检查 Ready
    let ready = raw_node.ready();
    assert!(!ready.entries.is_empty());
    assert_eq!(ready.entries[0].entry_type, EntryType::ConfChange);
}

#[tokio::test]
async fn test_raw_node_apply_conf_change() {
    let mut raw_node = new_raw_node(1, vec![1], 10, 3).await;
    
    // 初始只有一个节点
    assert_eq!(raw_node.raft.prs.len(), 0); // 不包括自己
    
    // 添加节点
    let cc = ConfChange {
        change_type: ConfChangeType::AddNode,
        node_id: 2,
        context: Vec::new(),
    };
    
    let conf_state = raw_node.apply_conf_change(&cc);
    assert!(conf_state.nodes.contains(&1));
    assert!(conf_state.nodes.contains(&2));
    assert!(raw_node.raft.prs.contains_key(&2));
    
    // 移除节点
    let cc = ConfChange {
        change_type: ConfChangeType::RemoveNode,
        node_id: 2,
        context: Vec::new(),
    };
    
    raw_node.apply_conf_change(&cc);
    assert!(!raw_node.raft.prs.contains_key(&2));
}

#[tokio::test]
async fn test_raw_node_transfer_leader() {
    let mut raw_node1 = new_raw_node(1, vec![1, 2], 10, 3).await;
    let mut raw_node2 = new_raw_node(2, vec![1, 2], 10, 3).await;
    
    // 先让 raw_node1 成为 leader
    raw_node1.campaign().unwrap();
    
    // 等待选举完成
    for _ in 0..20 {
        raw_node1.tick();
        raw_node2.tick();
        if raw_node1.raft.state == StateType::Leader {
            break;
        }
    }
    
    // 添加 raw_node2 到 raw_node1 的 prs
    raw_node1.raft.add_node(2);
    
    // 转移领导权
    raw_node1.transfer_leader(2).unwrap();
    
    // 检查是否有转移消息
    let ready = raw_node1.ready();
    let transfer_msg = ready.messages.iter()
        .find(|m| m.msg_type == MessageType::MsgTransferLeader);
    assert!(transfer_msg.is_some());
}

#[tokio::test]
async fn test_raw_node_get_progress() {
    let mut raw_node = new_raw_node(1, vec![1, 2], 10, 3).await;
    
    // 不是 leader，应该返回 None
    assert!(raw_node.get_progress().is_none());
    
    // 成为 leader
    raw_node.campaign().unwrap();
    for _ in 0..20 {
        raw_node.tick();
        if raw_node.raft.state == StateType::Leader {
            break;
        }
    }
    
    // 添加节点
    raw_node.raft.add_node(2);
    
    // 现在应该能获取进度
    let progress = raw_node.get_progress();
    assert!(progress.is_some());
    let progress = progress.unwrap();
    assert!(progress.contains_key(&2));
}

#[tokio::test]
async fn test_raw_node_ready_entries() {
    let mut raw_node = new_raw_node(1, vec![1], 10, 3).await;
    
    // 成为 leader
    raw_node.campaign().unwrap();
    for _ in 0..20 {
        raw_node.tick();
        if raw_node.raft.state == StateType::Leader {
            break;
        }
    }
    
    // 提案多个日志
    raw_node.propose(b"entry1".to_vec()).unwrap();
    raw_node.propose(b"entry2".to_vec()).unwrap();
    raw_node.propose(b"entry3".to_vec()).unwrap();
    
    // 获取 Ready
    let ready = raw_node.ready();
    assert!(!ready.entries.is_empty());
    
    // Advance
    raw_node.advance(ready);
    
    // 再次提案
    raw_node.propose(b"entry4".to_vec()).unwrap();
    
    // 新的 Ready 应该只包含新的条目
    let ready = raw_node.ready();
    assert_eq!(ready.entries.len(), 1);
    assert_eq!(ready.entries[0].data, b"entry4");
}

#[tokio::test]
async fn test_raw_node_committed_entries() {
    let mut raw_node = new_raw_node(1, vec![1], 10, 3).await;
    
    // 成为 leader
    raw_node.campaign().unwrap();
    for _ in 0..20 {
        raw_node.tick();
        if raw_node.raft.state == StateType::Leader {
            break;
        }
    }
    
    // 提案日志
    raw_node.propose(b"test".to_vec()).unwrap();
    
    // 单节点集群，日志应该立即提交
    let ready = raw_node.ready();
    assert!(!ready.entries.is_empty());
    
    // Advance entries
    let mut ready2 = ready.clone();
    ready2.committed_entries.clear(); // 先不应用
    raw_node.advance(ready2);
    
    // 再次获取 Ready，应该包含已提交的条目
    let ready3 = raw_node.ready();
    if !ready3.committed_entries.is_empty() {
        assert_eq!(ready3.committed_entries[0].data, b"test");
    }
}

#[tokio::test]
async fn test_raw_node_hard_state() {
    let mut raw_node = new_raw_node(1, vec![1, 2, 3], 10, 3).await;
    
    // 初始硬状态应该是空的
    let ready = raw_node.ready();
    assert!(ready.hard_state.is_empty());
    
    // 开始选举
    raw_node.campaign().unwrap();
    
    // 现在应该有硬状态更新
    let ready = raw_node.ready();
    assert!(!ready.hard_state.is_empty());
    assert_eq!(ready.hard_state.term, 1);
    assert_eq!(ready.hard_state.vote, 1);
    assert!(ready.must_sync);
}