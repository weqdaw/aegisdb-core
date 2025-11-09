use crate::scheduler::cluster::{BasicCluster, RegionInfo};
use crate::scheduler::operator::{Operator, OpStep, OpKind, AddPeer, RemovePeer, TransferLeader};
use crate::proto::schedulerpb::{RegionHeartbeatResponse, ChangePeer, TransferLeader as PbTransferLeader};
use crate::proto::metapb::Peer;
use crate::proto::eraftpb::ConfChangeType;
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::Duration;

/// 心跳流接口，用于发送调度命令到 raftstore
pub trait HeartbeatStreams: Send + Sync {
    /// 发送消息到指定 Region
    fn send_msg(&self, region: &RegionInfo, msg: RegionHeartbeatResponse);
}

/// 操作状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorStatus {
    Running,
    Success,
    Timeout,
    Cancel,
    Replace,
}

/// 操作记录（带状态）
#[derive(Clone)]
pub struct OperatorWithStatus {
    pub operator: Operator,
    pub status: OperatorStatus,
}

/// 操作控制器，用于限制调度速度和管理操作
pub struct OperatorController {
    cluster: Arc<BasicCluster>,
    operators: Arc<RwLock<HashMap<u64, Operator>>>, // region_id -> operator
    hb_streams: Arc<dyn HeartbeatStreams>,
    counts: Arc<RwLock<HashMap<OpKind, u64>>>, // 操作类型计数
    op_records: Arc<RwLock<HashMap<u64, OperatorWithStatus>>>, // 操作记录（TTL）
}

impl OperatorController {
    /// 创建新的 OperatorController
    pub fn new(cluster: Arc<BasicCluster>, hb_streams: Arc<dyn HeartbeatStreams>) -> Self {
        Self {
            cluster,
            operators: Arc::new(RwLock::new(HashMap::new())),
            hb_streams,
            counts: Arc::new(RwLock::new(HashMap::new())),
            op_records: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 分发操作（核心方法）
    /// source: "heartbeat" | "active push" | "create"
    pub fn dispatch(&self, region: &RegionInfo, source: &str) {
        let region_id = region.id();
        
        // 检查是否存在操作
        let mut operators = self.operators.write();
        let op = operators.get_mut(&region_id);

        if let Some(op) = op {
            let timeout = op.is_timeout();
            
            // 检查当前步骤是否已完成，如果完成则进入下一步
            if op.current_step < op.steps.len() {
                let step = &op.steps[op.current_step];
                if step.is_finish(region) {
                    // 步骤已完成，进入下一步
                    log::debug!("step finished, moving to next step: region_id={}, step={}", region_id, step.description());
                    op.next_step();
                }
            }
            
            // 检查操作是否完成
            if op.current_step >= op.steps.len() {
                // 操作完成
                let op_clone = op.clone();
                operators.remove(&region_id);
                drop(operators);
                self.update_counts();
                log::info!(
                    "operator finish: region_id={}, takes={:?}",
                    region_id,
                    op_clone.start_time
                        .unwrap_or(op_clone.create_time)
                        .elapsed()
                        .unwrap_or(Duration::ZERO)
                );
                self.put_op_record(op_clone, OperatorStatus::Success);
                return;
            }
            
            // 检查超时
            if timeout {
                let op_clone = op.clone();
                operators.remove(&region_id);
                drop(operators);
                self.update_counts();
                log::info!(
                    "operator timeout: region_id={}, takes={:?}",
                    region_id,
                    op_clone.start_time
                        .unwrap_or(op_clone.create_time)
                        .elapsed()
                        .unwrap_or(Duration::ZERO)
                );
                self.put_op_record(op_clone, OperatorStatus::Timeout);
                return;
            }
            
            // 检查是否有待执行的步骤
            if let Some(step) = op.check(region) {
                // 检查 Region Epoch 是否匹配（防止过期操作）
                let origin_epoch = &op.region_epoch;
                let latest_epoch = region.epoch();
                
                // 如果是从心跳来的，且 conf_ver 变化超过允许范围，取消操作
                if source == "heartbeat" {
                    let conf_ver_changed = step.conf_ver_changed(region);
                    let changes = latest_epoch.conf_ver.saturating_sub(origin_epoch.conf_ver);
                    
                    if changes > if conf_ver_changed { 1 } else { 0 } {
                        // 操作已过期，移除
                        let op_clone = op.clone();
                        operators.remove(&region_id);
                        drop(operators);
                        self.update_counts();
                        log::info!(
                            "stale operator: region_id={}, takes={:?}, changes={}",
                            region_id,
                            op_clone.start_time
                                .unwrap_or(op_clone.create_time)
                                .elapsed()
                                .unwrap_or(Duration::ZERO),
                            changes
                        );
                        self.put_op_record(op_clone, OperatorStatus::Cancel);
                        return;
                    }
                }

                // 发送调度命令（需要先释放锁）
                drop(operators);
                // 重新获取 step 引用（这里简化处理，实际应该保存 step 信息）
                // 由于 step 是 trait object，无法直接克隆，我们需要重新获取
                if let Some(op) = self.get_operator(region_id) {
                    if let Some(step_ref) = op.check(region) {
                        self.send_schedule_command(region, step_ref, source);
                    }
                }
            }
        }
    }

    /// 添加操作
    pub fn add_operator(&self, ops: Vec<Operator>) -> bool {
        let mut operators = self.operators.write();
        
        if !self.check_add_operator(&ops, &operators) {
            // 取消操作
            for op in ops {
                self.put_op_record(op, OperatorStatus::Cancel);
            }
            return false;
        }

        for mut op in ops {
            self.add_operator_locked(&mut op, &mut operators);
        }
        true
    }

    /// 检查操作是否可以添加
    fn check_add_operator(&self, ops: &[Operator], operators: &HashMap<u64, Operator>) -> bool {
        for op in ops {
            let region = self.cluster.get_region(op.region_id);
            if region.is_none() {
                log::debug!("region not found, cancel add operator: region_id={}", op.region_id);
                return false;
            }

            let region = region.unwrap();
            let region_epoch = region.epoch();
            
            // 检查 Region Epoch 是否匹配
            if region_epoch.version != op.region_epoch.version 
                || region_epoch.conf_ver != op.region_epoch.conf_ver {
                log::debug!(
                    "region epoch not match, cancel add operator: region_id={}",
                    op.region_id
                );
                return false;
            }

            // 检查是否已有操作
            if operators.contains_key(&op.region_id) {
                // 简化：如果已有操作，不允许添加新操作
                // 实际应该检查优先级
                log::debug!(
                    "already have operator, cancel add operator: region_id={}",
                    op.region_id
                );
                return false;
            }
        }
        true
    }

    /// 添加操作（已加锁）
    fn add_operator_locked(&self, op: &mut Operator, operators: &mut HashMap<u64, Operator>) {
        let region_id = op.region_id;

        log::info!("add operator: region_id={}, desc={}", region_id, op.desc);

        // 如果已有旧操作，替换它
        let old_op = operators.remove(&region_id);
        if let Some(old_op) = old_op {
            let old_op_clone = old_op.clone();
            log::info!(
                "replace old operator: region_id={}, takes={:?}",
                region_id,
                old_op_clone.start_time
                    .unwrap_or(old_op_clone.create_time)
                    .elapsed()
                    .unwrap_or(Duration::ZERO)
            );
            // 先释放锁，再记录
            self.put_op_record(old_op_clone, OperatorStatus::Replace);
            // 重新获取锁
            let mut operators = self.operators.write();
            
            // 设置开始时间
            op.start();

            // 添加到操作列表
            let op_clone = op.clone();
            operators.insert(region_id, op_clone);
            drop(operators); // 释放锁

            // 更新计数
            self.update_counts();

            // 立即检查并发送命令
            if let Some(region) = self.cluster.get_region(region_id) {
                if let Some(op_ref) = self.get_operator(region_id) {
                    if let Some(step) = op_ref.check(&region) {
                        self.send_schedule_command(&region, step, "create");
                    }
                }
            }
        } else {
            // 设置开始时间
            op.start();

            // 添加到操作列表
            let op_clone = op.clone();
            operators.insert(region_id, op_clone);
        }
    }

    /// 移除操作
    pub fn remove_operator(&self, op: &Operator) -> bool {
        let mut operators = self.operators.write();
        let region_id = op.region_id;
        
        if let Some(current_op) = operators.get(&region_id) {
            // 确保移除的是同一个操作（通过比较 region_id 和 create_time）
            if current_op.region_id == op.region_id 
                && current_op.create_time == op.create_time {
                operators.remove(&region_id);
                self.update_counts();
                return true;
            }
        }
        false
    }

    /// 获取操作
    pub fn get_operator(&self, region_id: u64) -> Option<Operator> {
        let operators = self.operators.read();
        operators.get(&region_id).cloned()
    }

    /// 获取所有操作
    pub fn get_operators(&self) -> Vec<Operator> {
        let operators = self.operators.read();
        operators.values().cloned().collect()
    }

    /// 发送调度命令
    fn send_schedule_command(&self, region: &RegionInfo, step: &dyn OpStep, source: &str) {
        log::info!(
            "send schedule command: region_id={}, step={}, source={}",
            region.id(),
            step.description(),
            source
        );

        let mut response = RegionHeartbeatResponse {
            header: None,
            change_peer: None,
            transfer_leader: None,
            region_id: region.id(),
            region_epoch: Some(region.epoch().clone()),
            target_peer: None,
        };

        // 根据步骤类型构建响应
        if let Some(transfer_leader) = step.as_any().downcast_ref::<TransferLeader>() {
            // TransferLeader 命令
            if let Some(peer) = region.get_peer_by_store_id(transfer_leader.to_store) {
                response.transfer_leader = Some(PbTransferLeader {
                    peer: Some(peer.clone()),
                });
            }
        } else if let Some(add_peer) = step.as_any().downcast_ref::<AddPeer>() {
            // AddPeer 命令
            if region.get_peer_by_store_id(add_peer.to_store).is_none() {
                // Peer 还未添加
                response.change_peer = Some(ChangePeer {
                    peer: Some(Peer {
                        id: add_peer.peer_id,
                        store_id: add_peer.to_store,
                    }),
                    change_type: ConfChangeType::AddNode as i32,
                });
            }
        } else if let Some(remove_peer) = step.as_any().downcast_ref::<RemovePeer>() {
            // RemovePeer 命令
            if let Some(peer) = region.get_peer_by_store_id(remove_peer.from_store) {
                response.change_peer = Some(ChangePeer {
                    peer: Some(peer.clone()),
                    change_type: ConfChangeType::RemoveNode as i32,
                });
            }
        }

        // 发送命令
        self.hb_streams.send_msg(region, response);
    }

    /// 更新操作计数
    fn update_counts(&self) {
        let operators = self.operators.read();
        let mut counts = self.counts.write();
        
        // 重置计数
        counts.clear();
        
        // 重新计算
        for op in operators.values() {
            *counts.entry(op.kind).or_insert(0) += 1;
        }
    }

    /// 获取操作数量
    pub fn operator_count(&self, mask: OpKind) -> u64 {
        let counts = self.counts.read();
        counts.get(&mask).copied().unwrap_or(0)
    }

    /// 获取操作状态
    pub fn get_operator_status(&self, region_id: u64) -> Option<OperatorWithStatus> {
        // 先检查运行中的操作
        {
            let operators = self.operators.read();
            if let Some(op) = operators.get(&region_id) {
                return Some(OperatorWithStatus {
                    operator: op.clone(),
                    status: OperatorStatus::Running,
                });
            }
        }

        // 检查记录中的操作
        let op_records = self.op_records.read();
        op_records.get(&region_id).cloned()
    }

    /// 添加操作记录
    fn put_op_record(&self, op: Operator, status: OperatorStatus) {
        let mut op_records = self.op_records.write();
        op_records.insert(
            op.region_id,
            OperatorWithStatus {
                operator: op,
                status,
            },
        );
    }
}