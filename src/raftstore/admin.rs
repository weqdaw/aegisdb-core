use crate::proto::metapb::{Region, Peer};
use crate::proto::raft_cmdpb::{AdminRequest, AdminResponse, AdminCmdType, SplitRequest, SplitResponse};
use crate::raftstore::store_meta::StoreMeta;
use crate::raftstore::region_allocator::RegionAllocator;
use anyhow::Result;
use std::sync::Arc;
use log::info;

/// Admin Command 处理器
/// 处理 Raft Admin Commands，如 Split、ChangePeer 等
pub struct AdminHandler {
    store_meta: Arc<StoreMeta>,
    region_allocator: Arc<RegionAllocator>,
    store_id: u64,
}

impl AdminHandler {
    pub fn new(
        store_meta: Arc<StoreMeta>,
        region_allocator: Arc<RegionAllocator>,
        store_id: u64,
    ) -> Self {
        Self {
            store_meta,
            region_allocator,
            store_id,
        }
    }

    /// 执行 Admin Request
    pub fn execute(&self, region: &Region, request: AdminRequest) -> Result<AdminResponse> {
        let cmd_type = AdminCmdType::from(request.cmd_type);
        
        match cmd_type {
            AdminCmdType::Split => {
                if let Some(split_req) = request.split {
                    self.execute_split(region, split_req)
                } else {
                    Err(anyhow::anyhow!("split request is missing"))
                }
            }
            AdminCmdType::ChangePeer => {
                // TODO: 实现 ChangePeer
                Err(anyhow::anyhow!("ChangePeer not implemented"))
            }
            AdminCmdType::TransferLeader => {
                // TODO: 实现 TransferLeader
                Err(anyhow::anyhow!("TransferLeader not implemented"))
            }
            AdminCmdType::CompactLog => {
                // TODO: 实现 CompactLog
                Err(anyhow::anyhow!("CompactLog not implemented"))
            }
            AdminCmdType::InvalidAdmin => {
                Err(anyhow::anyhow!("invalid admin command"))
            }
        }
    }

    /// 执行 Split 操作
    fn execute_split(&self, region: &Region, request: SplitRequest) -> Result<AdminResponse> {
        info!(
            "[store {}] executing split: region_id={}, split_key={:?}, new_region_id={}",
            self.store_id, region.id, request.split_key, request.new_region_id
        );

        // 验证 split_key
        if request.split_key.is_empty() {
            return Err(anyhow::anyhow!("split key is empty"));
        }

        // 验证 split_key 在 region 范围内
        if !self.is_key_in_range(&request.split_key, region) {
            return Err(anyhow::anyhow!(
                "split key {:?} is not in region range [{:?}, {:?}]",
                request.split_key,
                region.start_key,
                region.end_key
            ));
        }

        // 创建新的 Region
        let new_region = self.create_new_region(region, &request)?;
        
        // 更新原 Region 的范围
        let updated_region = self.update_region_range(region, &request.split_key)?;

        // 更新 StoreMeta
        self.store_meta.set_region(updated_region.clone());
        self.store_meta.set_region(new_region.clone());

        info!(
            "[store {}] split completed: old_region_id={}, new_region_id={}",
            self.store_id, region.id, request.new_region_id
        );

        Ok(AdminResponse {
            cmd_type: AdminCmdType::Split as i32,
            change_peer: None,
            compact_log: None,
            transfer_leader: None,
            split: Some(SplitResponse {
                regions: vec![updated_region, new_region],
            }),
        })
    }

    /// 创建新的 Region
    fn create_new_region(&self, old_region: &Region, request: &SplitRequest) -> Result<Region> {
        // 使用 RegionAllocator 分配 Peers（如果 request 中没有提供）
        let peers = if !request.new_peer_ids.is_empty() {
            // 使用请求中提供的 peer_ids
            request.new_peer_ids
                .iter()
                .map(|&peer_id| {
                    // 这里简化处理，实际应该从 StoreMeta 或其他地方获取 store_id
                    // 为了负载均衡，我们应该使用 RegionAllocator
                    Peer {
                        id: peer_id,
                        store_id: self.store_id, // 简化：使用当前 store_id
                    }
                })
                .collect()
        } else {
            // 使用 RegionAllocator 分配 Peers（考虑负载均衡）
            self.region_allocator.allocate_peers(old_region, request.new_region_id)?
        };

        // 创建新的 Region Epoch
        let mut new_epoch = old_region.region_epoch.clone().unwrap_or_default();
        new_epoch.version += 1;
        new_epoch.conf_ver += 1;

        // 创建新 Region
        let new_region = Region {
            id: request.new_region_id,
            start_key: request.split_key.clone(),
            end_key: old_region.end_key.clone(),
            region_epoch: Some(new_epoch),
            peers,
        };

        Ok(new_region)
    }

    /// 更新原 Region 的范围
    fn update_region_range(&self, region: &Region, split_key: &[u8]) -> Result<Region> {
        // 创建新的 Region Epoch
        let mut new_epoch = region.region_epoch.clone().unwrap_or_default();
        new_epoch.version += 1;

        // 更新 Region 的 end_key
        let mut updated_region = region.clone();
        updated_region.end_key = split_key.to_vec();
        updated_region.region_epoch = Some(new_epoch);

        // 更新 PeerStorage 中的 Region
        // 注意：这里简化处理，实际应该通过 PeerStorage 来更新
        // 因为 Region 信息存储在 Raft 状态中

        Ok(updated_region)
    }

    /// 检查 key 是否在 region 范围内
    fn is_key_in_range(&self, key: &[u8], region: &Region) -> bool {
        // 检查 start_key
        if !region.start_key.is_empty() && key < region.start_key.as_slice() {
            return false;
        }

        // 检查 end_key
        if !region.end_key.is_empty() && key >= region.end_key.as_slice() {
            return false;
        }

        true
    }
}

impl AdminCmdType {
    fn from(value: i32) -> Self {
        match value {
            1 => AdminCmdType::ChangePeer,
            3 => AdminCmdType::CompactLog,
            4 => AdminCmdType::TransferLeader,
            10 => AdminCmdType::Split,
            _ => AdminCmdType::InvalidAdmin,
        }
    }
}

