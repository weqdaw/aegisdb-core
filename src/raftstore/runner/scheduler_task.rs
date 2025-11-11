use crate::proto::metapb::{Region, Peer, RegionEpoch};
use crate::proto::schedulerpb::*;
use crate::proto::raft_cmdpb::{AdminRequest, AdminCmdType, SplitRequest, RaftCmdRequest, RaftRequestHeader};
use crate::raftstore::scheduler_client::SchedulerClient;
use crate::raftstore::router::RaftRouter;
use crate::raftstore::message::{Callback, Msg, MsgType, MsgData};
use anyhow::Result;
use log::info;

/// 调度器 AskSplit 任务
#[derive(Debug)]
pub struct SchedulerAskSplitTask {
    pub region: Region,
    pub split_key: Vec<u8>,
    pub peer: Peer,
    pub callback: Option<Callback>,
}

/// 调度器 Region 心跳任务
#[derive(Debug)]
pub struct SchedulerRegionHeartbeatTask {
    pub region: Region,
    pub peer: Peer,
    pub pending_peers: Vec<Peer>,
    pub approximate_size: Option<u64>,
}

/// 调度器 Store 心跳任务
#[derive(Debug)]
pub struct SchedulerStoreHeartbeatTask {
    pub stats: StoreStats,
}

/// 调度器任务处理器
pub struct SchedulerTaskHandler {
    store_id: u64,
    scheduler_client: Box<dyn SchedulerClient>,
    router: RaftRouter,
}

impl SchedulerTaskHandler {
    pub fn new(
        store_id: u64,
        scheduler_client: Box<dyn SchedulerClient>,
        router: RaftRouter,
    ) -> Self {
        Self {
            store_id,
            scheduler_client,
            router,
        }
    }
    
    /// 启动处理器
    pub fn start(&self) {
        let client = self.scheduler_client.as_ref();
        let store_id = self.store_id;
        let handler = Box::new(move |resp: RegionHeartbeatResponse| {
            Self::on_region_heartbeat_response(store_id, resp);
        });
        client.set_region_heartbeat_response_handler(handler);
    }
    
    /// 处理 Region 心跳响应
    fn on_region_heartbeat_response(store_id: u64, resp: RegionHeartbeatResponse) {
        if let Some(_change_peer) = resp.change_peer {
            info!("[store {}] received change peer response for region {}", store_id, resp.region_id);
            // TODO: 发送 ChangePeer 请求到 Raft
        } else if let Some(_transfer_leader) = resp.transfer_leader {
            info!("[store {}] received transfer leader response for region {}", store_id, resp.region_id);
            // TODO: 发送 TransferLeader 请求到 Raft
        }
    }
    
    /// 处理任务
    pub async fn handle(&self, task: SchedulerTask) -> Result<()> {
        match task {
            SchedulerTask::AskSplit(t) => self.on_ask_split(t).await,
            SchedulerTask::RegionHeartbeat(t) => self.on_heartbeat(t).await,
            SchedulerTask::StoreHeartbeat(t) => self.on_store_heartbeat(t).await,
        }
    }
    
    /// 处理 AskSplit 任务
    async fn on_ask_split(&self, task: SchedulerAskSplitTask) -> Result<()> {
        let resp = self.scheduler_client.ask_split(&task.region).await?;
        
        if let Some(header) = resp.header {
            if let Some(err) = header.error {
                return Err(anyhow::anyhow!("scheduler error: {:?}", err));
            }
        }
        
        // 构建 Split AdminRequest
        let admin_request = AdminRequest {
            cmd_type: AdminCmdType::Split as i32,
            split: Some(SplitRequest {
                split_key: task.split_key,
                new_region_id: resp.new_region_id,
                new_peer_ids: resp.new_peer_ids,
            }),
            change_peer: None,
            compact_log: None,
            transfer_leader: None,
        };
        
        // 发送到 Raft
        self.send_admin_request(
            task.region.id,
            task.region.region_epoch.clone().unwrap_or_default(),
            task.peer,
            admin_request,
            task.callback,
        ).await?;
        
        Ok(())
    }
    
    /// 处理 Region 心跳任务
    async fn on_heartbeat(&self, task: SchedulerRegionHeartbeatTask) -> Result<()> {
        let size = task.approximate_size.unwrap_or(0);
        
        let request = RegionHeartbeatRequest {
            header: Some(self.scheduler_client.request_header()),
            region: Some(task.region),
            leader: Some(task.peer),
            pending_peers: task.pending_peers,
            approximate_size: size,
        };
        
        self.scheduler_client.region_heartbeat(request)?;
        Ok(())
    }
    
    /// 处理 Store 心跳任务
    async fn on_store_heartbeat(&self, task: SchedulerStoreHeartbeatTask) -> Result<()> {
        self.scheduler_client.store_heartbeat(&task.stats).await?;
        Ok(())
    }
    
    /// 发送 Admin 请求
    async fn send_admin_request(
        &self,
        region_id: u64,
        epoch: RegionEpoch,
        peer: Peer,
        req: AdminRequest,
        callback: Option<Callback>,
    ) -> Result<()> {
        info!(
            "[store {}] sending admin request for region {}: cmd_type={}",
            self.store_id, region_id, req.cmd_type
        );

        // 简化处理：直接序列化 AdminRequest
        // 注意：实际实现中应该使用 protobuf 序列化整个 RaftCmdRequest
        // 这里我们使用一个简化的方式：将 AdminRequest 包装在 Vec<u8> 中
        // 实际实现中，应该使用 protobuf 的 encode/decode
        // 
        // 构建完整的 RaftCmdRequest（用于日志记录）
        let _raft_cmd_request = RaftCmdRequest {
            header: Some(RaftRequestHeader {
                region_id,
                peer: Some(peer),
                region_epoch: Some(epoch),
                term: 0, // TODO: 从 Peer 获取当前 term
            }),
            requests: vec![],
            admin_request: Some(req.clone()),
        };

        // 简化处理：使用 region_id 作为标识
        // 实际实现中，应该序列化整个 RaftCmdRequest
        let request_data = format!("ADMIN_REQUEST:{}", region_id).into_bytes();

        // 通过 Msg 发送
        // 注意：实际实现中，应该使用 RaftCmdRequest 的 protobuf 序列化
        let msg = Msg {
            msg_type: MsgType::RaftCmd,
            region_id,
            data: MsgData::RaftCmd {
                request: request_data,
                callback,
            },
        };

        // 通过 router 发送
        self.router.send(region_id, msg)?;
        
        Ok(())
    }
    
}

/// 调度器任务枚举
#[derive(Debug)]
pub enum SchedulerTask {
    AskSplit(SchedulerAskSplitTask),
    RegionHeartbeat(SchedulerRegionHeartbeatTask),
    StoreHeartbeat(SchedulerStoreHeartbeatTask),
}