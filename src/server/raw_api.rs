use crate::server::Server;
use crate::storage::Storage;
use crate::storage::{Modify, Put, Delete};
use crate::proto::kvrpcpb::*;

/// RawKV API 实现
/// 
/// 注意：在 standalone 模式下，我们简化了版本校验
/// 因为 standalone 模式没有 region 的概念
pub struct RawKvServer;

impl RawKvServer {
    /// RawGet 从存储中获取指定 CF 和 Key 的值
    pub async fn raw_get<S: Storage>(
        server: &Server<S>,
        req: RawGetRequest,
    ) -> anyhow::Result<RawGetResponse> {
        // 在 standalone 模式下，可以跳过版本校验
        // 如果需要，可以在这里添加版本校验逻辑
        
        // 获取 reader
        let reader = server.storage().reader().await?;
        
        // 读取值
        match reader.get_cf(&req.cf, &req.key).await? {
            Some(value) => Ok(RawGetResponse::ok(Some(value))),
            None => Ok(RawGetResponse::ok(None)),
        }
    }

    /// RawPut 将数据写入存储
    pub async fn raw_put<S: Storage>(
        server: &Server<S>,
        req: RawPutRequest,
    ) -> anyhow::Result<RawPutResponse> {
        // 构造 Modify 批次
        let batch = vec![Modify::Put(Put {
            key: req.key,
            value: req.value,
            cf: req.cf,
        })];

        // 写入存储
        server.storage().write(batch).await?;

        Ok(RawPutResponse::ok())
    }

    /// RawDelete 从存储中删除指定 CF 和 Key
    pub async fn raw_delete<S: Storage>(
        server: &Server<S>,
        req: RawDeleteRequest,
    ) -> anyhow::Result<RawDeleteResponse> {
        // 构造 Delete Modify
        let batch = vec![Modify::Delete(Delete {
            key: req.key,
            cf: req.cf,
        })];

        // 执行删除
        server.storage().write(batch).await?;

        Ok(RawDeleteResponse::ok())
    }

    /// RawScan 扫描从 start_key 开始，最多 limit 个键值对
    pub async fn raw_scan<S: Storage>(
        server: &Server<S>,
        req: RawScanRequest,
    ) -> anyhow::Result<RawScanResponse> {
        // 获取 reader
        let reader = server.storage().reader().await?;
        
        // 创建迭代器
        let mut iter = reader.iter_cf(&req.cf);
        
        // 定位到 start_key
        iter.seek(&req.start_key);
        
        // 收集结果
        let mut kvs = Vec::new();
        let mut count = 0;
        
        while iter.valid() && count < req.limit as usize {
            let item = iter.item();
            // item.value() 返回 Result<Vec<u8>, anyhow::Error>，需要使用 ? 处理
            let value = item.value()?;
            kvs.push(KvPair::new(
                item.key().to_vec(),
                value,  // 直接使用 value，已经是 Vec<u8>
            ));
            iter.next();
            count += 1;
        }
        
        Ok(RawScanResponse::ok(kvs))
    }
}