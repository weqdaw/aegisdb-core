use crate::proto::metapb::RegionEpoch;
use crate::scheduler::cluster::RegionInfo;
use std::time::{SystemTime, Duration};
use std::any::Any;
use std::hash::{Hash, Hasher};

/// 操作类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Region,
    Leader,
    Admin,
}

impl Hash for OpKind {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (*self as u8).hash(state);
    }
}

/// 操作步骤接口
pub trait OpStep: Send + Sync {
    /// 检查配置版本是否改变
    fn conf_ver_changed(&self, region: &RegionInfo) -> bool;
    
    /// 检查步骤是否完成
    fn is_finish(&self, region: &RegionInfo) -> bool;
    
    /// 步骤描述
    fn description(&self) -> String;

    /// 转化为Any 
    fn as_any(&self) -> &dyn Any;
}

/// 添加 Peer 步骤
#[derive(Debug, Clone)]
pub struct AddPeer {
    pub to_store: u64,
    pub peer_id: u64,
}

impl AddPeer {
    pub fn boxed(self) -> Box<dyn OpStep> {
        Box::new(self)
    }
}

impl OpStep for AddPeer {
    fn conf_ver_changed(&self, region: &RegionInfo) -> bool {
        region.peers().iter().any(|p| p.id == self.peer_id && p.store_id == self.to_store)
    }

    fn is_finish(&self, region: &RegionInfo) -> bool {
        region.peers().iter().any(|p| p.id == self.peer_id && p.store_id == self.to_store)
            && !region.pending_peers().iter().any(|p| p.id == self.peer_id)
    }

    fn description(&self) -> String {
        format!("add peer {} on store {}", self.peer_id, self.to_store)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// 移除 Peer 步骤
#[derive(Debug, Clone)]
pub struct RemovePeer {
    pub from_store: u64,
}

impl RemovePeer {
    pub fn boxed(self) -> Box<dyn OpStep> {
        Box::new(self)
    }
}

impl OpStep for RemovePeer {
    fn conf_ver_changed(&self, region: &RegionInfo) -> bool {
        !region.peers().iter().any(|p| p.store_id == self.from_store)
    }

    fn is_finish(&self, region: &RegionInfo) -> bool {
        !region.peers().iter().any(|p| p.store_id == self.from_store)
    }

    fn description(&self) -> String {
        format!("remove peer on store {}", self.from_store)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// 转移 Leader 步骤
#[derive(Debug, Clone)]
pub struct TransferLeader {
    pub from_store: u64,
    pub to_store: u64,
}

impl TransferLeader {
    pub fn boxed(self) -> Box<dyn OpStep> {
        Box::new(self)
    }
}

impl OpStep for TransferLeader {
    fn conf_ver_changed(&self, _region: &RegionInfo) -> bool {
        false // transfer leader 不改变配置版本
    }

    fn is_finish(&self, region: &RegionInfo) -> bool {
        region.get_leader_store_id() == Some(self.to_store)
    }

    fn description(&self) -> String {
        format!("transfer leader from store {} to store {}", self.from_store, self.to_store)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// 调度操作
pub struct Operator {
    pub desc: String,
    pub region_id: u64,
    pub region_epoch: RegionEpoch,
    pub kind: OpKind,
    pub steps: Vec<Box<dyn OpStep>>,
    pub current_step: usize,
    pub create_time: SystemTime,
    pub start_time: Option<SystemTime>,
}

impl std::fmt::Debug for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Operator")
            .field("desc", &self.desc)
            .field("region_id", &self.region_id)
            .field("region_epoch", &self.region_epoch)
            .field("kind", &self.kind)
            .field("steps_count", &self.steps.len())
            .field("current_step", &self.current_step)
            .field("create_time", &self.create_time)
            .field("start_time", &self.start_time)
            .finish()
    }
}

impl Operator {
    pub fn new(
        desc: String,
        region_id: u64,
        region_epoch: RegionEpoch,
        kind: OpKind,
        steps: Vec<Box<dyn OpStep>>,
    ) -> Self {
        Self {
            desc,
            region_id,
            region_epoch,
            kind,
            steps,
            current_step: 0,
            create_time: SystemTime::now(),
            start_time: None,
        }
    }

    /// 创建 MovePeer 操作
    pub fn create_move_peer_operator(
        desc: String,
        region: &RegionInfo,
        from_store: u64,
        to_store: u64,
        new_peer_id: u64,
    ) -> Self {
        let mut steps: Vec<Box<dyn OpStep>> = Vec::new();
        
        // 1. 添加新 Peer
        steps.push(Box::new(AddPeer {
            to_store,
            peer_id: new_peer_id,
        }));
        
        // 2. 如果需要，转移 Leader
        if region.get_leader_store_id() == Some(from_store) {
            steps.push(Box::new(TransferLeader {
                from_store,
                to_store,
            }));
        }
        
        // 3. 移除旧 Peer
        steps.push(Box::new(RemovePeer {
            from_store,
        }));

        Self::new(
            desc,
            region.id(),
            region.epoch().clone(),
            OpKind::Region,
            steps,
        )
    }

    /// 创建 TransferLeader 操作
    pub fn create_transfer_leader_operator(
        desc: String,
        region: &RegionInfo,
        from_store: u64,
        to_store: u64,
    ) -> Self {
        let steps: Vec<Box<dyn OpStep>> = vec![Box::new(TransferLeader {
            from_store,
            to_store,
        })];

        Self::new(
            desc,
            region.id(),
            region.epoch().clone(),
            OpKind::Leader,
            steps,
        )
    }

    /// 创建 AddPeer 操作（扩容）
    pub fn create_add_peer_operator(
        desc: String,
        region: &RegionInfo,
        to_store: u64,
        peer_id: u64,
    ) -> Self {
        let steps: Vec<Box<dyn OpStep>> = vec![Box::new(AddPeer {
            to_store,
            peer_id,
        })];

        Self::new(
            desc,
            region.id(),
            region.epoch().clone(),
            OpKind::Region,
            steps,
        )
    }

    /// 创建 RemovePeer 操作（缩容）
    pub fn create_remove_peer_operator(
        desc: String,
        region: &RegionInfo,
        from_store: u64,
    ) -> Self {
        let steps: Vec<Box<dyn OpStep>> = vec![Box::new(RemovePeer {
            from_store,
        })];

        Self::new(
            desc,
            region.id(),
            region.epoch().clone(),
            OpKind::Region,
            steps,
        )
    }

    /// 检查操作是否完成
    pub fn check(&self, region: &RegionInfo) -> Option<&dyn OpStep> {
        if self.current_step >= self.steps.len() {
            return None;
        }

        let step = &self.steps[self.current_step];
        if step.is_finish(region) {
            None // 当前步骤已完成
        } else {
            Some(step.as_ref())
        }
    }

    /// 检查是否超时
    pub fn is_timeout(&self) -> bool {
        let timeout = match self.kind {
            OpKind::Leader => Duration::from_secs(10),
            OpKind::Region => Duration::from_secs(600),
            OpKind::Admin => Duration::from_secs(600),
        };

        if let Some(start) = self.start_time {
            start.elapsed().unwrap_or(Duration::ZERO) > timeout
        } else {
            self.create_time.elapsed().unwrap_or(Duration::ZERO) > timeout
        }
    }

    pub fn start(&mut self) {
        self.start_time = Some(SystemTime::now());
    }

    pub fn next_step(&mut self) {
        if self.current_step < self.steps.len() {
            self.current_step += 1;
        }
    }
}

impl Clone for Operator {
    fn clone(&self) -> Self {
        // 克隆步骤：通过 as_any 和 downcast 来克隆具体类型
        let cloned_steps: Vec<Box<dyn OpStep>> = self.steps.iter().map(|step| {
            if let Some(add_peer) = step.as_any().downcast_ref::<AddPeer>() {
                Box::new(add_peer.clone()) as Box<dyn OpStep>
            } else if let Some(remove_peer) = step.as_any().downcast_ref::<RemovePeer>() {
                Box::new(remove_peer.clone()) as Box<dyn OpStep>
            } else if let Some(transfer_leader) = step.as_any().downcast_ref::<TransferLeader>() {
                Box::new(transfer_leader.clone()) as Box<dyn OpStep>
            } else {
                panic!("Unknown OpStep type")
            }
        }).collect();

        Self {
            desc: self.desc.clone(),
            region_id: self.region_id,
            region_epoch: self.region_epoch.clone(),
            kind: self.kind,
            steps: cloned_steps,
            current_step: self.current_step,
            create_time: self.create_time,
            start_time: self.start_time,
        }
    }
}