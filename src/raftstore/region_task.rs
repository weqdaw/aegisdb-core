use crate::engine_util::{Engines, util::exceed_end_key};
use crate::raftstore::meta::{get_region_local_state, write_region_state, write_apply_state, PeerState};
use crate::engine_util::WriteBatch;
use serde::{Serialize, Deserialize};
use anyhow::Result;

/// Region 任务类型
#[derive(Debug)]
pub enum RegionTask {
    /// 生成快照
    GenSnapshot {
        region_id: u64,
        notifier: tokio::sync::oneshot::Sender<Option<Vec<u8>>>,
    },
    /// 应用快照
    ApplySnapshot {
        region_id: u64,
        snapshot_data: Vec<u8>,
        notifier: tokio::sync::oneshot::Sender<bool>,
    },
    /// 销毁 Region 数据
    Destroy {
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
}

/// Region 任务处理器
pub struct RegionTaskHandler {
    engines: Engines,
}

impl RegionTaskHandler {
    pub fn new(engines: Engines) -> Self {
        Self { engines }
    }

    pub async fn handle(&self, task: RegionTask) -> anyhow::Result<()> {
        match task {
            RegionTask::GenSnapshot { region_id, notifier } => {
                let snapshot = self.gen_snapshot(region_id).await?;
                notifier.send(Some(snapshot)).ok();
            }
            RegionTask::ApplySnapshot { region_id, snapshot_data, notifier } => {
                let success = self.apply_snapshot(region_id, snapshot_data).await?;
                notifier.send(success).ok();
            }
            RegionTask::Destroy { region_id, start_key, end_key } => {
                self.destroy_range(region_id, start_key, end_key).await?;
            }
        }
        Ok(())
    }

    async fn gen_snapshot(&self, region_id: u64) -> anyhow::Result<Vec<u8>> {
        // 读取 region 的元数据
        let region_state = get_region_local_state(&self.engines, region_id)?
            .ok_or_else(|| anyhow::anyhow!("region {} not found", region_id))?;
        
        let region = &region_state.region;
        let start_key = &region.start_key;
        let end_key = &region.end_key;
        
        // 快照数据结构
        #[derive(Serialize, Deserialize)]
        struct SnapshotData {
            region: crate::proto::metapb::Region,
            kvs: Vec<(Vec<u8>, Vec<u8>)>,
        }
        
        // 扫描 KV 数据库，收集该 region 范围内的所有 KV 对
        let mut kvs = Vec::new();
        let db = &self.engines.kv;
        let mut iter = db.raw_iterator();
        
        // Seek 到 start_key
        if !start_key.is_empty() {
            iter.seek(start_key);
        } else {
            iter.seek_to_first();
        }
        
        // 扫描范围内的所有 KV 对
        while iter.valid() {
            if let Some(key_bytes) = iter.key() {
                // 检查是否超出范围
                if !end_key.is_empty() && exceed_end_key(key_bytes, end_key) {
                    break;
                }
                
                // 跳过元数据键（以特殊前缀开头的键）
                if key_bytes.len() > 0 && key_bytes[0] < 0x10 {
                    iter.next();
                    continue;
                }
                
                if let Some(value_bytes) = iter.value() {
                    kvs.push((key_bytes.to_vec(), value_bytes.to_vec()));
                }
            }
            
            iter.next();
        }
        
        // 序列化快照数据
        let snapshot_data = SnapshotData {
            region: region.clone(),
            kvs,
        };
        
        Ok(bincode::serialize(&snapshot_data)?)
    }

    async fn apply_snapshot(&self, region_id: u64, snapshot_data: Vec<u8>) -> anyhow::Result<bool> {
        // 反序列化快照数据
        #[derive(Serialize, Deserialize)]
        struct SnapshotData {
            region: crate::proto::metapb::Region,
            kvs: Vec<(Vec<u8>, Vec<u8>)>,
        }
        
        let snapshot: SnapshotData = bincode::deserialize(&snapshot_data)?;
        let region = &snapshot.region;
        
        // 先调用 destroy_range 清理旧数据
        self.destroy_range(
            region_id,
            region.start_key.clone(),
            region.end_key.clone(),
        ).await?;
        
        // 批量写入 KV 对
        let mut wb = WriteBatch::new();
        for (key, value) in snapshot.kvs {
            wb.set_cf("default", &key, &value);
        }
        
        // 更新 region 状态
        write_region_state(&mut wb, region, PeerState::Normal)?;
        
        // 更新 apply_state（使用快照的 index）
        if let Some(apply_state) = crate::raftstore::meta::get_apply_state(&self.engines, region_id)? {
            write_apply_state(&mut wb, region_id, &apply_state)?;
        }
        
        self.engines.write_kv(&wb)?;
        
        Ok(true)
    }

    async fn destroy_range(
        &self,
        _region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> anyhow::Result<()> {
        let db = &self.engines.kv;
        let mut iter = db.raw_iterator();
        
        // Seek 到 start_key
        if !start_key.is_empty() {
            iter.seek(&start_key);
        } else {
            iter.seek_to_first();
        }
        
        // 收集要删除的键
        let mut keys_to_delete = Vec::new();
        while iter.valid() {
            if let Some(key_bytes) = iter.key() {
                // 检查是否超出范围
                if !end_key.is_empty() && exceed_end_key(key_bytes, &end_key) {
                    break;
                }
                
                // 跳过元数据键（以特殊前缀开头的键）
                if key_bytes.len() > 0 && key_bytes[0] < 0x10 {
                    iter.next();
                    continue;
                }
                
                keys_to_delete.push(key_bytes.to_vec());
            }
            
            iter.next();
        }
        
        // 批量删除
        if !keys_to_delete.is_empty() {
            use rocksdb::WriteBatch;
            let mut wb = WriteBatch::default();
            for key in keys_to_delete {
                wb.delete(&key);
            }
            db.write(wb)?;
        }
        
        Ok(())
    }
}