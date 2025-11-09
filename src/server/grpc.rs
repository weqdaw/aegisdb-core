use crate::server::Server;
use crate::storage::Storage;
use crate::server::RawKvServer;
use crate::proto::kvrpcpb::*;
use tonic::{Request, Response, Status};

// 生成的 proto 代码会在这里
pub mod metapb {
    tonic::include_proto!("metapb");
}

pub mod errorpb {
    tonic::include_proto!("errorpb");
}

pub mod tinykvpb {
    tonic::include_proto!("tinykvpb");
}

pub mod kvrpcpb {
    tonic::include_proto!("kvrpcpb");
}

pub use tinykvpb::tiny_kv_server::TinyKv;

/// gRPC 服务实现
pub struct TinyKvService<S: Storage> {
    server: Server<S>,
}

impl<S: Storage> TinyKvService<S> {
    pub fn new(server: Server<S>) -> Self {
        Self { server }
    }
}

#[tonic::async_trait]
impl<S: Storage + Send + Sync + 'static> TinyKv for TinyKvService<S> {
    async fn raw_get(
        &self,
        request: Request<kvrpcpb::RawGetRequest>,
    ) -> Result<Response<kvrpcpb::RawGetResponse>, Status> {
        let req = request.into_inner();
        
        // 转换 proto 类型到内部类型
        let context = req.context.map(|c| Context::new(c.region_id)).unwrap_or_else(|| Context::new(0));
        let internal_req = RawGetRequest {
            context,
            key: req.key,
            cf: req.cf,
        };
        
        match RawKvServer::raw_get(&self.server, internal_req).await {
            Ok(resp) => {
                let proto_resp = kvrpcpb::RawGetResponse {
                    region_error: resp.region_error.map(|e| convert_error(e)),
                    error: resp.error.unwrap_or_default(),
                    value: resp.value.unwrap_or_default(),
                    not_found: resp.not_found,
                };
                Ok(Response::new(proto_resp))
            }
            Err(e) => Err(Status::internal(format!("Internal error: {}", e))),
        }
    }

    async fn raw_put(
        &self,
        request: Request<kvrpcpb::RawPutRequest>,
    ) -> Result<Response<kvrpcpb::RawPutResponse>, Status> {
        let req = request.into_inner();
        
        let context = req.context.map(|c| Context::new(c.region_id)).unwrap_or_else(|| Context::new(0));
        let internal_req = RawPutRequest {
            context,
            key: req.key,
            value: req.value,
            cf: req.cf,
        };
        
        match RawKvServer::raw_put(&self.server, internal_req).await {
            Ok(resp) => {
                let proto_resp = kvrpcpb::RawPutResponse {
                    region_error: resp.region_error.map(|e| convert_error(e)),
                    error: resp.error.unwrap_or_default(),
                };
                Ok(Response::new(proto_resp))
            }
            Err(e) => Err(Status::internal(format!("Internal error: {}", e))),
        }
    }

    async fn raw_delete(
        &self,
        request: Request<kvrpcpb::RawDeleteRequest>,
    ) -> Result<Response<kvrpcpb::RawDeleteResponse>, Status> {
        let req = request.into_inner();
        
        let context = req.context.map(|c| Context::new(c.region_id)).unwrap_or_else(|| Context::new(0));
        let internal_req = RawDeleteRequest {
            context,
            key: req.key,
            cf: req.cf,
        };
        
        match RawKvServer::raw_delete(&self.server, internal_req).await {
            Ok(resp) => {
                let proto_resp = kvrpcpb::RawDeleteResponse {
                    region_error: resp.region_error.map(|e| convert_error(e)),
                    error: resp.error.unwrap_or_default(),
                };
                Ok(Response::new(proto_resp))
            }
            Err(e) => Err(Status::internal(format!("Internal error: {}", e))),
        }
    }

    async fn raw_scan(
        &self,
        request: Request<kvrpcpb::RawScanRequest>,
    ) -> Result<Response<kvrpcpb::RawScanResponse>, Status> {
        let req = request.into_inner();
        
        let context = req.context.map(|c| Context::new(c.region_id)).unwrap_or_else(|| Context::new(0));
        let internal_req = RawScanRequest {
            context,
            start_key: req.start_key,
            limit: req.limit,
            cf: req.cf,
        };
        
        match RawKvServer::raw_scan(&self.server, internal_req).await {
            Ok(resp) => {
                let proto_kvs: Vec<kvrpcpb::KvPair> = resp.kvs
                    .into_iter()
                    .map(|kv| kvrpcpb::KvPair {
                        error: kv.error.map(|e| convert_key_error(e)),
                        key: kv.key,
                        value: kv.value,
                    })
                    .collect();
                
                let proto_resp = kvrpcpb::RawScanResponse {
                    region_error: resp.region_error.map(|e| convert_error(e)),
                    error: resp.error.unwrap_or_default(),
                    kvs: proto_kvs,
                };
                Ok(Response::new(proto_resp))
            }
            Err(e) => Err(Status::internal(format!("Internal error: {}", e))),
        }
    }

    // 事务相关方法（占位符，后续实现）
    async fn kv_get(
        &self,
        _request: Request<kvrpcpb::GetRequest>,
    ) -> Result<Response<kvrpcpb::GetResponse>, Status> {
        Err(Status::unimplemented("Not implemented yet"))
    }

    async fn kv_scan(
        &self,
        _request: Request<kvrpcpb::ScanRequest>,
    ) -> Result<Response<kvrpcpb::ScanResponse>, Status> {
        Err(Status::unimplemented("Not implemented yet"))
    }

    async fn kv_prewrite(
        &self,
        _request: Request<kvrpcpb::PrewriteRequest>,
    ) -> Result<Response<kvrpcpb::PrewriteResponse>, Status> {
        Err(Status::unimplemented("Not implemented yet"))
    }

    async fn kv_commit(
        &self,
        _request: Request<kvrpcpb::CommitRequest>,
    ) -> Result<Response<kvrpcpb::CommitResponse>, Status> {
        Err(Status::unimplemented("Not implemented yet"))
    }
}

// 辅助函数：转换错误类型
fn convert_error(e: crate::proto::errorpb::Error) -> errorpb::Error {
    // 内部 Error 是枚举类型，proto Error 是结构体类型
    // 简化处理：只设置 message 字段，其他字段为 None
    let message = match &e {
        crate::proto::errorpb::Error::NotLeader { region_id, .. } => {
            format!("NotLeader: region_id={}", region_id)
        }
        crate::proto::errorpb::Error::RegionNotFound { region_id } => {
            format!("RegionNotFound: region_id={}", region_id)
        }
        crate::proto::errorpb::Error::KeyNotInRegion { key, region_id, .. } => {
            format!("KeyNotInRegion: key={:?}, region_id={}", key, region_id)
        }
        crate::proto::errorpb::Error::EpochNotMatch { .. } => {
            "EpochNotMatch".to_string()
        }
        crate::proto::errorpb::Error::ServerIsBusy { reason } => {
            format!("ServerIsBusy: {}", reason)
        }
        crate::proto::errorpb::Error::StaleCommand => {
            "StaleCommand".to_string()
        }
        crate::proto::errorpb::Error::StoreNotMatch { request_store_id, actual_store_id } => {
            format!("StoreNotMatch: request={}, actual={}", request_store_id, actual_store_id)
        }
        crate::proto::errorpb::Error::RaftEntryTooLarge { region_id, entry_size } => {
            format!("RaftEntryTooLarge: region_id={}, size={}", region_id, entry_size)
        }
    };
    
    errorpb::Error {
        message,
        not_leader: None,
        region_not_found: None,
        key_not_in_region: None,
        stale_epoch: None,
    }
}

fn convert_key_error(e: crate::proto::kvrpcpb::KeyError) -> kvrpcpb::KeyError {
    let mut proto_key_error = kvrpcpb::KeyError {
        locked: None,
        retryable: String::new(),
        abort: String::new(),
        conflict_ts: 0,
        conflict_key: Vec::new(),
        conflict_commit_ts: 0,
        already_exist: 0,
    };
    
    if let Some(locked) = e.locked {
        proto_key_error.locked = Some(kvrpcpb::LockInfo {
            primary_lock: locked.primary_lock,
            lock_version: locked.lock_version,
            key: locked.key,
            lock_ttl: locked.lock_ttl,
        });
    }
    
    if let Some(retryable) = e.retryable {
        proto_key_error.retryable = retryable;
    }
    
    if let Some(abort) = e.abort {
        proto_key_error.abort = abort;
    }
    
    if let Some(conflict) = e.conflict {
        proto_key_error.conflict_ts = conflict.conflict_ts;
        proto_key_error.conflict_key = conflict.key;
        // 注意：proto 中可能没有 conflict_commit_ts 和 already_exist 字段
        // 使用默认值 0
    }
    
    proto_key_error
}