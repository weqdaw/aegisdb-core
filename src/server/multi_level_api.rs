use crate::server::Server;
use crate::storage::Storage;
use crate::storage::{Modify, Put, Delete};
use crate::proto::kvrpcpb::*;
use crate::util::{
    encode_multi_level_key,
    decode_multi_level_key,
    primary_key_from_region_id,
};

/// MultiLevelKvServer 提供多级键值存储 API
/// 
/// 特点：
/// - 用户只需提供二级键（secondary_key）
/// - 一级键（primary_key）自动从 Context 的 region_id 获取
/// - 内部自动编码为完整键进行存储
pub struct MultiLevelKvServer;

impl MultiLevelKvServer {
    /// MultiLevelPut - 使用二级键存储数据
    /// 
    /// 一级键从 context.region_id 自动获取
    pub async fn multi_level_put<S: Storage>(
        server: &Server<S>,
        req: MultiLevelPutRequest,
    ) -> anyhow::Result<MultiLevelPutResponse> {
        // 从 context 获取 region_id 作为一级键
        let primary_key = primary_key_from_region_id(req.context.region_id);
        
        // 编码完整键
        let full_key = encode_multi_level_key(&primary_key, &req.secondary_key);
        
        // 构造 Modify 批次
        let batch = vec![Modify::Put(Put {
            key: full_key,
            value: req.value,
            cf: req.cf,
        })];

        // 写入存储
        server.storage().write(batch).await?;

        Ok(MultiLevelPutResponse::ok())
    }

    /// MultiLevelGet - 使用二级键查询数据
    /// 
    /// 一级键从 context.region_id 自动获取
    pub async fn multi_level_get<S: Storage>(
        server: &Server<S>,
        req: MultiLevelGetRequest,
    ) -> anyhow::Result<MultiLevelGetResponse> {
        // 从 context 获取 region_id 作为一级键
        let primary_key = primary_key_from_region_id(req.context.region_id);
        
        // 编码完整键
        let full_key = encode_multi_level_key(&primary_key, &req.secondary_key);
        
        // 获取 reader
        let reader = server.storage().reader().await?;
        
        // 读取值
        match reader.get_cf(&req.cf, &full_key).await? {
            Some(value) => Ok(MultiLevelGetResponse::ok(Some(value))),
            None => Ok(MultiLevelGetResponse::ok(None)),
        }
    }

    /// MultiLevelDelete - 使用二级键删除数据
    /// 
    /// 一级键从 context.region_id 自动获取
    pub async fn multi_level_delete<S: Storage>(
        server: &Server<S>,
        req: MultiLevelDeleteRequest,
    ) -> anyhow::Result<MultiLevelDeleteResponse> {
        // 从 context 获取 region_id 作为一级键
        let primary_key = primary_key_from_region_id(req.context.region_id);
        
        // 编码完整键
        let full_key = encode_multi_level_key(&primary_key, &req.secondary_key);
        
        // 构造 Delete Modify
        let batch = vec![Modify::Delete(Delete {
            key: full_key,
            cf: req.cf,
        })];

        // 执行删除
        server.storage().write(batch).await?;

        Ok(MultiLevelDeleteResponse::ok())
    }

    /// MultiLevelScan - 扫描指定一级键下的所有二级键
    /// 
    /// 返回所有匹配的二级键值对
    pub async fn multi_level_scan<S: Storage>(
        server: &Server<S>,
        req: MultiLevelScanRequest,
    ) -> anyhow::Result<MultiLevelScanResponse> {
        // 从 context 获取 region_id 作为一级键
        let primary_key = primary_key_from_region_id(req.context.region_id);
        
        // 编码起始完整键
        let start_full_key = encode_multi_level_key(&primary_key, &req.start_secondary_key);
        
        // 编码结束键（用于限制扫描范围）
        // 结束键 = primary_key + 0xFF + 0xFF... (确保只扫描当前一级键下的数据)
        let mut end_key = primary_key.clone();
        end_key.push(0xFF);
        end_key.push(0xFF);  // 添加额外的分隔符，确保扫描到一级键的末尾
        
        // 获取 reader
        let reader = server.storage().reader().await?;
        
        // 创建迭代器
        let mut iter = reader.iter_cf(&req.cf);
        
        // 定位到 start_key
        iter.seek(&start_full_key);
        
        // 收集结果
        let mut kvs = Vec::new();
        let mut count = 0;
        
        while iter.valid() && count < req.limit as usize {
            let item = iter.item();
            let full_key = item.key();
            
            // 检查是否超出当前一级键的范围
            if full_key.len() > end_key.len() || 
               (full_key.len() == end_key.len() && full_key > end_key.as_slice()) {
                break;
            }
            
            // 解码完整键，提取二级键
            match decode_multi_level_key(full_key) {
                Ok((decoded_primary, secondary_key)) => {
                    // 验证一级键是否匹配
                    if decoded_primary == primary_key {
                        let value = item.value()?;
                        kvs.push(MultiLevelKvPair {
                            secondary_key,
                            value,
                        });
                        count += 1;
                    } else {
                        // 一级键不匹配，说明已经超出范围
                        break;
                    }
                }
                Err(_) => {
                    // 无法解码，跳过
                }
            }
            
            iter.next();
        }
        
        Ok(MultiLevelScanResponse::ok(kvs))
    }
}