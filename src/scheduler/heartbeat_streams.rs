use crate::scheduler::operator_controller::HeartbeatStreams;
use crate::scheduler::cluster::RegionInfo;
use crate::proto::schedulerpb::RegionHeartbeatResponse;
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// 简单的心跳流实现（用于测试和单机模式）
pub struct SimpleHeartbeatStreams {
    // store_id -> sender
    streams: Arc<RwLock<HashMap<u64, mpsc::UnboundedSender<RegionHeartbeatResponse>>>>,
    // 用于测试：记录发送的消息
    sent_messages: Arc<RwLock<Vec<(u64, RegionHeartbeatResponse)>>>,
}

impl SimpleHeartbeatStreams {
    pub fn new() -> Self {
        Self {
            streams: Arc::new(RwLock::new(HashMap::new())),
            sent_messages: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// 注册 store 的心跳流
    pub fn bind_stream(&self, store_id: u64, sender: mpsc::UnboundedSender<RegionHeartbeatResponse>) {
        let mut streams = self.streams.write();
        streams.insert(store_id, sender);
    }

    /// 获取发送的消息（用于测试）
    pub fn get_sent_messages(&self) -> Vec<(u64, RegionHeartbeatResponse)> {
        let messages = self.sent_messages.read();
        messages.clone()
    }

    /// 清空发送的消息（用于测试）
    pub fn clear_sent_messages(&self) {
        let mut messages = self.sent_messages.write();
        messages.clear();
    }
}

impl HeartbeatStreams for SimpleHeartbeatStreams {
    fn send_msg(&self, region: &RegionInfo, msg: RegionHeartbeatResponse) {
        // 获取 Leader Store ID
        if let Some(leader_store_id) = region.get_leader_store_id() {
            // 记录消息（用于测试）
            {
                let mut messages = self.sent_messages.write();
                // 手动克隆消息
                let msg_clone = RegionHeartbeatResponse {
                    header: msg.header.clone(),
                    change_peer: msg.change_peer.clone(),
                    transfer_leader: msg.transfer_leader.clone(),
                    region_id: msg.region_id,
                    region_epoch: msg.region_epoch.clone(),
                    target_peer: msg.target_peer.clone(),
                };
                messages.push((region.id(), msg_clone));
            }
            
            let streams = self.streams.read();
            if let Some(sender) = streams.get(&leader_store_id) {
                // 克隆消息用于发送
                let msg_clone = RegionHeartbeatResponse {
                    header: msg.header.clone(),
                    change_peer: msg.change_peer.clone(),
                    transfer_leader: msg.transfer_leader.clone(),
                    region_id: msg.region_id,
                    region_epoch: msg.region_epoch.clone(),
                    target_peer: msg.target_peer.clone(),
                };
                if let Err(e) = sender.send(msg_clone) {
                    log::warn!("failed to send heartbeat message to store {}: {}", leader_store_id, e);
                } else {
                    log::info!("sent heartbeat message to store {} for region {}", leader_store_id, region.id());
                }
            } else {
                log::warn!("heartbeat stream not found for store {}, region {}", leader_store_id, region.id());
            }
        } else {
            log::warn!("region {} has no leader", region.id());
        }
    }
}