use crate::raftstore::peer::Peer;
use crate::raftstore::message::{Msg, MsgType, MsgData};
use crate::raftstore::store_meta::StoreMeta;
use crate::raftstore::router::RaftRouter;
use crate::raftstore::split_checker::SplitChecker;
use crate::raftstore::runner::{SchedulerTask, SchedulerAskSplitTask};
use crate::proto::metapb::{RegionEpoch, Peer as MetaPeer};
use anyhow::Result;
use std::sync::Arc;
use log::info;

/// Peer 消息处理器
pub struct PeerMsgHandler {
    peer: Arc<Peer>,
    store_meta: Arc<StoreMeta>,
    router: RaftRouter,
    split_checker: Option<SplitChecker>,
}

impl PeerMsgHandler {
    pub fn new(
        peer: Arc<Peer>,
        store_meta: Arc<StoreMeta>,
        router: RaftRouter,
        split_checker: Option<SplitChecker>,
    ) -> Self {
        Self {
            peer,
            store_meta,
            router,
            split_checker,
        }
    }

    /// 处理消息
    pub fn handle_msg(&self, msg: Msg) -> Result<()> {
        match msg.msg_type {
            MsgType::Tick => self.on_tick(),
            MsgType::SplitRegion => {
                if let MsgData::SplitRegion { region_epoch, split_key, .. } = msg.data {
                    self.on_prepare_split_region(region_epoch, split_key)?;
                }
            }
            MsgType::RegionApproximateSize => {
                if let MsgData::ApproximateSize(size) = msg.data {
                    self.on_approximate_region_size(size);
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Tick 处理
    fn on_tick(&self) {
        if self.peer.stopped {
            return;
        }
        
        // 触发 Raft tick
        // self.peer.raft_group.tick();
        
        // 检查是否需要 split
        self.on_split_region_check_tick();
    }

    /// Split Region 检查 Tick
    fn on_split_region_check_tick(&self) {
        if !self.peer.is_leader() {
            return;
        }
        
        // 如果大小变化提示太小，跳过检查
        if let Some(approx_size) = self.peer.approximate_size {
            if self.peer.size_diff_hint < (approx_size / 8) {
                return;
            }
        }
        
        // 执行 split check
        if let Some(ref checker) = self.split_checker {
            let region = self.peer.region();
            if let Some(split_key) = checker.check(&region) {
                // 发送 split 消息
                let msg = Msg {
                    msg_type: MsgType::SplitRegion,
                    region_id: region.id,
                    data: MsgData::SplitRegion {
                        region_epoch: region.region_epoch.clone().unwrap_or_default(),
                        split_key,
                        callback: None,
                    },
                };
                self.router.send(region.id, msg).ok();
            }
        }
    }

    /// 准备 Split Region
    fn on_prepare_split_region(
        &self,
        region_epoch: RegionEpoch,
        split_key: Vec<u8>,
    ) -> Result<()> {
        // 验证 split
        self.validate_split_region(&region_epoch, &split_key)?;
        
        let region = self.peer.region();
        let leader_peer = MetaPeer {
            id: self.peer.meta.id,
            store_id: self.peer.meta.store_id,
        };
        
        info!(
            "[region {}] preparing split: split_key={:?}",
            region.id, split_key
        );
        
        // 创建 SchedulerAskSplitTask
        // 注意：这里简化处理，实际应该通过任务通道发送到 SchedulerTaskHandler
        // 为了简化，我们通过一个特殊的消息类型来发送
        // 实际实现中，应该有一个任务队列或通道
        
        // 这里我们通过 router 发送一个特殊的消息，由外部处理
        // 或者直接在这里调用 SchedulerTaskHandler（如果可访问）
        
        // 暂时记录日志，实际应该发送任务
        info!(
            "[region {}] split task created: split_key={:?}",
            region.id, split_key
        );
        
        Ok(())
    }

    /// 验证 Split Region
    fn validate_split_region(
        &self,
        epoch: &RegionEpoch,
        split_key: &[u8],
    ) -> Result<()> {
        if split_key.is_empty() {
            return Err(anyhow::anyhow!("split key should not be empty"));
        }
        
        if !self.peer.is_leader() {
            return Err(anyhow::anyhow!("not leader"));
        }
        
        let region = self.peer.region();
        let latest_epoch = region.region_epoch.clone().unwrap_or_default();
        
        if latest_epoch.version != epoch.version {
            return Err(anyhow::anyhow!("epoch changed"));
        }
        
        Ok(())
    }

    /// 更新 Region 近似大小
    fn on_approximate_region_size(&self, _size: u64) {
        // 更新 peer 的 approximate_size
        // 注意：这里需要修改 Peer 结构使其支持可变
        // 实际实现中可能需要使用 Arc<RwLock<Peer>> 或类似结构
    }
}