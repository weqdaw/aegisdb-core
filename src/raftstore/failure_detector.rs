use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio::time::sleep;
use log::{info, warn, error};

/// 节点状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    /// 正常
    Healthy,
    /// 可疑（心跳超时但未确认故障）
    Suspect,
    /// 故障
    Failed,
}

/// 节点健康信息
#[derive(Debug, Clone)]
struct NodeHealth {
    status: NodeStatus,
    last_heartbeat: Option<Instant>,
    consecutive_failures: u32,
    last_check: Instant,
}

impl NodeHealth {
    fn new() -> Self {
        Self {
            status: NodeStatus::Healthy,
            last_heartbeat: None,
            consecutive_failures: 0,
            last_check: Instant::now(),
        }
    }
    
    fn update_heartbeat(&mut self) {
        self.last_heartbeat = Some(Instant::now());
        self.consecutive_failures = 0;
        if self.status != NodeStatus::Healthy {
            self.status = NodeStatus::Healthy;
            info!("Node health status changed to Healthy");
        }
    }
    
    fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        self.last_check = Instant::now();
    }
    
    fn check_health(&mut self, heartbeat_timeout: Duration, failure_threshold: u32) -> bool {
        let now = Instant::now();
        let time_since_heartbeat = self.last_heartbeat
            .map(|t| now.duration_since(t))
            .unwrap_or(Duration::from_secs(3600));
        
        if time_since_heartbeat > heartbeat_timeout {
            if self.status == NodeStatus::Healthy {
                self.status = NodeStatus::Suspect;
                warn!("Node health status changed to Suspect (heartbeat timeout)");
            }
            
            if self.consecutive_failures >= failure_threshold {
                if self.status != NodeStatus::Failed {
                    self.status = NodeStatus::Failed;
                    error!("Node health status changed to Failed");
                    return true; // 状态变化
                }
            }
        }
        
        false
    }
}

/// 故障检测器
pub struct FailureDetector {
    nodes: Arc<RwLock<HashMap<u64, NodeHealth>>>,
    heartbeat_timeout: Duration,
    failure_threshold: u32,
    check_interval: Duration,
}

impl FailureDetector {
    pub fn new(
        heartbeat_timeout: Duration,
        failure_threshold: u32,
        check_interval: Duration,
    ) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_timeout,
            failure_threshold,
            check_interval,
        }
    }
    
    /// 注册节点
    pub async fn register_node(&self, node_id: u64) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node_id, NodeHealth::new());
        info!("Registered node {} for failure detection", node_id);
    }
    
    /// 更新节点心跳
    pub async fn update_heartbeat(&self, node_id: u64) {
        let mut nodes = self.nodes.write().await;
        if let Some(health) = nodes.get_mut(&node_id) {
            health.update_heartbeat();
        } else {
            // 如果节点不存在，自动注册
            let mut health = NodeHealth::new();
            health.update_heartbeat();
            nodes.insert(node_id, health);
        }
    }
    
    /// 记录节点故障
    pub async fn record_failure(&self, node_id: u64) {
        let mut nodes = self.nodes.write().await;
        if let Some(health) = nodes.get_mut(&node_id) {
            health.record_failure();
        }
    }
    
    /// 获取节点状态
    pub async fn get_status(&self, node_id: u64) -> Option<NodeStatus> {
        let nodes = self.nodes.read().await;
        nodes.get(&node_id).map(|h| h.status)
    }
    
    /// 检查所有节点健康状态
    pub async fn check_all_nodes(&self) -> Vec<u64> {
        let mut failed_nodes = Vec::new();
        let mut nodes = self.nodes.write().await;
        
        for (node_id, health) in nodes.iter_mut() {
            if health.check_health(self.heartbeat_timeout, self.failure_threshold) {
                failed_nodes.push(*node_id);
            }
        }
        
        failed_nodes
    }
    
    /// 启动故障检测循环
    pub async fn start_detection_loop(&self, mut on_failure: impl FnMut(u64) + Send + 'static) {
        let nodes = Arc::clone(&self.nodes);
        let heartbeat_timeout = self.heartbeat_timeout;
        let failure_threshold = self.failure_threshold;
        let check_interval = self.check_interval;
        
        tokio::spawn(async move {
            loop {
                sleep(check_interval).await;
                
                let mut failed_nodes = Vec::new();
                {
                    let mut nodes_guard = nodes.write().await;
                    for (node_id, health) in nodes_guard.iter_mut() {
                        if health.check_health(heartbeat_timeout, failure_threshold) {
                            failed_nodes.push(*node_id);
                        }
                    }
                }
                
                // 通知故障节点
                for node_id in failed_nodes {
                    on_failure(node_id);
                }
            }
        });
    }
    
    /// 移除节点
    pub async fn remove_node(&self, node_id: u64) {
        let mut nodes = self.nodes.write().await;
        nodes.remove(&node_id);
        info!("Removed node {} from failure detection", node_id);
    }
    
    /// 获取所有故障节点
    pub async fn get_failed_nodes(&self) -> Vec<u64> {
        let nodes = self.nodes.read().await;
        nodes.iter()
            .filter(|(_, health)| health.status == NodeStatus::Failed)
            .map(|(node_id, _)| *node_id)
            .collect()
    }
    
    /// 获取所有可疑节点
    pub async fn get_suspect_nodes(&self) -> Vec<u64> {
        let nodes = self.nodes.read().await;
        nodes.iter()
            .filter(|(_, health)| health.status == NodeStatus::Suspect)
            .map(|(node_id, _)| *node_id)
            .collect()
    }
}

/// Leader 切换管理器
pub struct LeaderSwitchManager {
    failure_detector: Arc<FailureDetector>,
    region_leaders: Arc<RwLock<HashMap<u64, u64>>>, // region_id -> leader_id
}

impl LeaderSwitchManager {
    pub fn new(failure_detector: Arc<FailureDetector>) -> Self {
        Self {
            failure_detector,
            region_leaders: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// 设置 Region 的 Leader
    pub async fn set_leader(&self, region_id: u64, leader_id: u64) {
        let mut leaders = self.region_leaders.write().await;
        leaders.insert(region_id, leader_id);
    }
    
    /// 获取 Region 的 Leader
    pub async fn get_leader(&self, region_id: u64) -> Option<u64> {
        let leaders = self.region_leaders.read().await;
        leaders.get(&region_id).copied()
    }
    
    /// 检查并处理 Leader 故障
    pub async fn check_and_switch_leader(&self, region_id: u64) -> Option<u64> {
        if let Some(leader_id) = self.get_leader(region_id).await {
            if let Some(status) = self.failure_detector.get_status(leader_id).await {
                if status == NodeStatus::Failed {
                    warn!("Leader {} for region {} has failed, need to switch", leader_id, region_id);
                    // 返回 None 表示需要重新选举
                    let mut leaders = self.region_leaders.write().await;
                    leaders.remove(&region_id);
                    return None;
                }
            }
        }
        
        self.get_leader(region_id).await
    }
    
    /// 启动 Leader 切换监控
    pub async fn start_monitoring(&self, mut on_leader_failure: impl FnMut(u64, u64) + Send + 'static) {
        let failure_detector = Arc::clone(&self.failure_detector);
        let region_leaders = Arc::clone(&self.region_leaders);
        
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(1)).await;
                
                let failed_nodes = failure_detector.get_failed_nodes().await;
                if !failed_nodes.is_empty() {
                    let leaders = region_leaders.read().await;
                    for (region_id, leader_id) in leaders.iter() {
                        if failed_nodes.contains(leader_id) {
                            on_leader_failure(*region_id, *leader_id);
                        }
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_failure_detector_basic() {
        let detector = FailureDetector::new(
            Duration::from_secs(2),
            3,
            Duration::from_millis(100),
        );
        
        // 注册节点
        detector.register_node(1).await;
        detector.register_node(2).await;
        
        // 更新心跳
        detector.update_heartbeat(1).await;
        detector.update_heartbeat(2).await;
        
        // 检查状态（应该是 Healthy）
        assert_eq!(detector.get_status(1).await, Some(NodeStatus::Healthy));
        assert_eq!(detector.get_status(2).await, Some(NodeStatus::Healthy));
    }

    #[tokio::test]
    async fn test_failure_detector_timeout() {
        let detector = FailureDetector::new(
            Duration::from_millis(500),
            2,
            Duration::from_millis(100),
        );
        
        detector.register_node(1).await;
        detector.update_heartbeat(1).await;
        
        // 等待超时
        sleep(Duration::from_millis(600)).await;
        
        // 记录几次故障并检查健康状态
        detector.record_failure(1).await;
        detector.record_failure(1).await;
        
        // 手动触发健康检查
        let _ = detector.check_all_nodes().await;
        
        // 检查状态（应该变为 Suspect 或 Failed）
        let status = detector.get_status(1).await;
        assert!(status == Some(NodeStatus::Suspect) || status == Some(NodeStatus::Failed));
    }

    #[tokio::test]
    async fn test_failure_detector_recovery() {
        let detector = FailureDetector::new(
            Duration::from_millis(500),
            2,
            Duration::from_millis(100),
        );
        
        detector.register_node(1).await;
        detector.update_heartbeat(1).await;
        
        // 等待超时
        sleep(Duration::from_millis(600)).await;
        detector.record_failure(1).await;
        
        // 更新心跳（应该恢复）
        detector.update_heartbeat(1).await;
        
        // 检查状态（应该恢复为 Healthy）
        assert_eq!(detector.get_status(1).await, Some(NodeStatus::Healthy));
    }

    #[tokio::test]
    async fn test_leader_switch_manager() {
        let detector = Arc::new(FailureDetector::new(
            Duration::from_millis(500),
            2,
            Duration::from_millis(100),
        ));
        
        let manager = LeaderSwitchManager::new(Arc::clone(&detector));
        
        // 设置 Leader
        manager.set_leader(1, 100).await;
        manager.set_leader(2, 200).await;
        
        // 检查 Leader
        assert_eq!(manager.get_leader(1).await, Some(100));
        assert_eq!(manager.get_leader(2).await, Some(200));
        
        // 注册节点并更新心跳
        detector.register_node(100).await;
        detector.register_node(200).await;
        detector.update_heartbeat(100).await;
        detector.update_heartbeat(200).await;
        
        // 等待超时并记录故障
        sleep(Duration::from_millis(600)).await;
        for _ in 0..3 {
            detector.record_failure(100).await;
        }
        
        // 手动触发健康检查
        let _ = detector.check_all_nodes().await;
        
        // 检查并切换 Leader
        let leader = manager.check_and_switch_leader(1).await;
        assert_eq!(leader, None); // Leader 已故障，需要重新选举
    }
}

