use crate::raftstore::message::Msg;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use tokio::sync::mpsc;

/// 路由表：region_id -> peer sender
pub struct Router {
    peers: Arc<RwLock<HashMap<u64, mpsc::UnboundedSender<Msg>>>>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 注册 Peer
    pub fn register(&self, region_id: u64, sender: mpsc::UnboundedSender<Msg>) {
        let mut peers = self.peers.write().unwrap();
        peers.insert(region_id, sender);
    }

    /// 取消注册
    pub fn unregister(&self, region_id: u64) {
        let mut peers = self.peers.write().unwrap();
        peers.remove(&region_id);
    }

    /// 发送消息
    pub fn send(&self, region_id: u64, msg: Msg) -> anyhow::Result<()> {
        let peers = self.peers.read().unwrap();
        if let Some(sender) = peers.get(&region_id) {
            sender.send(msg).map_err(|e| anyhow::anyhow!("failed to send message: {:?}", e))?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("region {} not found", region_id))
        }
    }

    /// 获取所有 region_id
    pub fn get_all_region_ids(&self) -> Vec<u64> {
        let peers = self.peers.read().unwrap();
        peers.keys().cloned().collect()
    }
}

/// Raft Router 包装
pub struct RaftRouter {
    router: Arc<Router>,
}

impl RaftRouter {
    pub fn new(router: Arc<Router>) -> Self {
        Self { router }
    }

    pub fn send(&self, region_id: u64, msg: Msg) -> anyhow::Result<()> {
        self.router.send(region_id, msg)
    }
}