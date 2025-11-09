// aegisdb/src/proto/kvrpcpb.rs
use crate::proto::metapb::{RegionEpoch, Peer};
use crate::proto::errorpb::Error;

/// 操作类型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Put = 0,
    Del = 1,
    Rollback = 2,
}

// Context 包含请求的元数据信息
#[derive(Debug, Clone)]
pub struct Context {
    pub region_id: u64,
    pub region_epoch: Option<RegionEpoch>,
    pub peer: Option<Peer>,
    pub term: u64,
}

impl Context {
    pub fn new(region_id: u64) -> Self {
        Self {
            region_id,
            region_epoch: None,
            peer: None,
            term: 0,
        }
    }

    pub fn with_epoch(mut self, epoch: RegionEpoch) -> Self {
        self.region_epoch = Some(epoch);
        self
    }
}

// RawGet 请求和响应
#[derive(Debug, Clone)]
pub struct RawGetRequest {
    pub context: Context,
    pub key: Vec<u8>,
    pub cf: String,
}

#[derive(Debug, Clone)]
pub struct RawGetResponse {
    pub region_error: Option<Error>,
    pub error: Option<String>,
    pub value: Option<Vec<u8>>,
    pub not_found: bool,
}

impl RawGetResponse {
    pub fn ok(value: Option<Vec<u8>>) -> Self {
        Self {
            region_error: None,
            error: None,
            value: value.clone(),
            not_found: value.is_none(),
        }
    }

    pub fn with_error(error: Error) -> Self {
        Self {
            region_error: Some(error),
            error: None,
            value: None,
            not_found: false,
        }
    }
}

// RawPut 请求和响应
#[derive(Debug, Clone)]
pub struct RawPutRequest {
    pub context: Context,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub cf: String,
}

#[derive(Debug, Clone)]
pub struct RawPutResponse {
    pub region_error: Option<Error>,
    pub error: Option<String>,
}

impl RawPutResponse {
    pub fn ok() -> Self {
        Self {
            region_error: None,
            error: None,
        }
    }

    pub fn with_error(error: Error) -> Self {
        Self {
            region_error: Some(error),
            error: None,
        }
    }
}

// RawDelete 请求和响应
#[derive(Debug, Clone)]
pub struct RawDeleteRequest {
    pub context: Context,
    pub key: Vec<u8>,
    pub cf: String,
}

#[derive(Debug, Clone)]
pub struct RawDeleteResponse {
    pub region_error: Option<Error>,
    pub error: Option<String>,
}

impl RawDeleteResponse {
    pub fn ok() -> Self {
        Self {
            region_error: None,
            error: None,
        }
    }

    pub fn with_error(error: Error) -> Self {
        Self {
            region_error: Some(error),
            error: None,
        }
    }
}

// RawScan 请求和响应
#[derive(Debug, Clone)]
pub struct RawScanRequest {
    pub context: Context,
    pub start_key: Vec<u8>,
    pub limit: u32,
    pub cf: String,
}

#[derive(Debug, Clone)]
pub struct KvPair {
    pub error: Option<KeyError>,  // 事务扫描时可能包含错误
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl KvPair {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            error: None,
            key,
            value,
        }
    }

    pub fn with_error(key: Vec<u8>, error: KeyError) -> Self {
        Self {
            error: Some(error),
            key,
            value: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct RawScanResponse {
    pub region_error: Option<Error>,
    pub error: Option<String>,
    pub kvs: Vec<KvPair>,
}

impl RawScanResponse {
    pub fn ok(kvs: Vec<KvPair>) -> Self {
        Self {
            region_error: None,
            error: None,
            kvs,
        }
    }

    pub fn with_error(error: Error) -> Self {
        Self {
            region_error: Some(error),
            error: None,
            kvs: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct MultiLevelPutRequest {
    pub context: Context,
    pub secondary_key: Vec<u8>,  // 仅需提供二级键
    pub value: Vec<u8>,
    pub cf: String,
}

/// MultiLevelPut 响应
#[derive(Debug, Clone)]
pub struct MultiLevelPutResponse {
    pub region_error: Option<Error>,
    pub error: Option<String>,
}

impl MultiLevelPutResponse {
    pub fn ok() -> Self {
        Self {
            region_error: None,
            error: None,
        }
    }

    pub fn with_error(error: Error) -> Self {
        Self {
            region_error: Some(error),
            error: None,
        }
    }
}

/// MultiLevelGet 请求 - 仅提供二级键进行查询
#[derive(Debug, Clone)]
pub struct MultiLevelGetRequest {
    pub context: Context,
    pub secondary_key: Vec<u8>,  // 仅需提供二级键
    pub cf: String,
}

/// MultiLevelGet 响应
#[derive(Debug, Clone)]
pub struct MultiLevelGetResponse {
    pub region_error: Option<Error>,
    pub error: Option<String>,
    pub value: Option<Vec<u8>>,
    pub not_found: bool,
}

impl MultiLevelGetResponse {
    pub fn ok(value: Option<Vec<u8>>) -> Self {
        Self {
            region_error: None,
            error: None,
            value: value.clone(),
            not_found: value.is_none(),
        }
    }

    pub fn with_error(error: Error) -> Self {
        Self {
            region_error: Some(error),
            error: None,
            value: None,
            not_found: false,
        }
    }
}

/// MultiLevelDelete 请求 - 仅提供二级键进行删除
#[derive(Debug, Clone)]
pub struct MultiLevelDeleteRequest {
    pub context: Context,
    pub secondary_key: Vec<u8>,  // 仅需提供二级键
    pub cf: String,
}

/// MultiLevelDelete 响应
#[derive(Debug, Clone)]
pub struct MultiLevelDeleteResponse {
    pub region_error: Option<Error>,
    pub error: Option<String>,
}

impl MultiLevelDeleteResponse {
    pub fn ok() -> Self {
        Self {
            region_error: None,
            error: None,
        }
    }

    pub fn with_error(error: Error) -> Self {
        Self {
            region_error: Some(error),
            error: None,
        }
    }
}

/// MultiLevelScan 请求 - 扫描指定一级键下的所有二级键
#[derive(Debug, Clone)]
pub struct MultiLevelScanRequest {
    pub context: Context,
    pub start_secondary_key: Vec<u8>,  // 起始二级键
    pub limit: u32,
    pub cf: String,
}

/// MultiLevelScan 响应
#[derive(Debug, Clone)]
pub struct MultiLevelScanResponse {
    pub region_error: Option<Error>,
    pub error: Option<String>,
    pub kvs: Vec<MultiLevelKvPair>,  // 返回的键值对（仅包含二级键）
}

/// 多级键值对（仅包含二级键和值）
#[derive(Debug, Clone)]
pub struct MultiLevelKvPair {
    pub secondary_key: Vec<u8>,
    pub value: Vec<u8>,
}

impl MultiLevelScanResponse {
    pub fn ok(kvs: Vec<MultiLevelKvPair>) -> Self {
        Self {
            region_error: None,
            error: None,
            kvs,
        }
    }

    pub fn with_error(error: Error) -> Self {
        Self {
            region_error: Some(error),
            error: None,
            kvs: vec![],
        }
    }
}

// ========== 事务相关请求和响应 ==========

/// GetRequest - 事务读取请求
#[derive(Debug, Clone)]
pub struct GetRequest {
    pub context: Context,
    pub key: Vec<u8>,
    pub version: u64,  // 事务的开始时间戳
}

/// GetResponse - 事务读取响应
#[derive(Debug, Clone)]
pub struct GetResponse {
    pub region_error: Option<Error>,
    pub error: Option<KeyError>,
    pub value: Option<Vec<u8>>,
    pub not_found: bool,
}

impl GetResponse {
    pub fn ok(value: Option<Vec<u8>>) -> Self {
        Self {
            region_error: None,
            error: None,
            value: value.clone(),
            not_found: value.is_none(),
        }
    }

    pub fn with_key_error(error: KeyError) -> Self {
        Self {
            region_error: None,
            error: Some(error),
            value: None,
            not_found: false,
        }
    }

    pub fn with_region_error(error: Error) -> Self {
        Self {
            region_error: Some(error),
            error: None,
            value: None,
            not_found: false,
        }
    }
}

/// Mutation - 事务中的写操作
#[derive(Debug, Clone)]
pub struct Mutation {
    pub op: Op,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// PrewriteRequest - 两阶段提交第一阶段
#[derive(Debug, Clone)]
pub struct PrewriteRequest {
    pub context: Context,
    pub mutations: Vec<Mutation>,
    pub primary_lock: Vec<u8>,  // 主键
    pub start_version: u64,      // 事务开始时间戳
    pub lock_ttl: u64,           // 锁的生存时间
}

/// PrewriteResponse - Prewrite 响应
#[derive(Debug, Clone)]
pub struct PrewriteResponse {
    pub region_error: Option<Error>,
    pub errors: Vec<KeyError>,
}

impl PrewriteResponse {
    pub fn ok() -> Self {
        Self {
            region_error: None,
            errors: vec![],
        }
    }

    pub fn with_errors(errors: Vec<KeyError>) -> Self {
        Self {
            region_error: None,
            errors,
        }
    }

    pub fn with_region_error(error: Error) -> Self {
        Self {
            region_error: Some(error),
            errors: vec![],
        }
    }
}

/// CommitRequest - 两阶段提交第二阶段
#[derive(Debug, Clone)]
pub struct CommitRequest {
    pub context: Context,
    pub start_version: u64,   // 事务开始时间戳
    pub keys: Vec<Vec<u8>>,   // 要提交的键列表
    pub commit_version: u64,  // 提交时间戳
}

/// CommitResponse - Commit 响应
#[derive(Debug, Clone)]
pub struct CommitResponse {
    pub region_error: Option<Error>,
    pub error: Option<KeyError>,
}

impl CommitResponse {
    pub fn ok() -> Self {
        Self {
            region_error: None,
            error: None,
        }
    }

    pub fn with_key_error(error: KeyError) -> Self {
        Self {
            region_error: None,
            error: Some(error),
        }
    }

    pub fn with_region_error(error: Error) -> Self {
        Self {
            region_error: Some(error),
            error: None,
        }
    }
}

/// ScanRequest - 事务扫描请求
#[derive(Debug, Clone)]
pub struct ScanRequest {
    pub context: Context,
    pub start_key: Vec<u8>,
    pub limit: u32,
    pub version: u64,  // 事务开始时间戳
}

/// ScanResponse - 扫描响应
#[derive(Debug, Clone)]
pub struct ScanResponse {
    pub region_error: Option<Error>,
    pub pairs: Vec<KvPair>,  // 可能包含错误信息
}

impl ScanResponse {
    pub fn ok(pairs: Vec<KvPair>) -> Self {
        Self {
            region_error: None,
            pairs,
        }
    }

    pub fn with_region_error(error: Error) -> Self {
        Self {
            region_error: Some(error),
            pairs: vec![],
        }
    }
}

/// CheckTxnStatusRequest - 检查事务状态请求
#[derive(Debug, Clone)]
pub struct CheckTxnStatusRequest {
    pub context: Context,
    pub primary_key: Vec<u8>,
    pub lock_ts: u64,      // 锁的时间戳（事务开始时间戳）
    pub current_ts: u64,   // 当前时间戳（用于检查 TTL）
}

/// CheckTxnStatusResponse - 检查事务状态响应
#[derive(Debug, Clone)]
pub struct CheckTxnStatusResponse {
    pub region_error: Option<Error>,
    pub lock_ttl: u64,        // 锁的 TTL（如果已锁定）
    pub commit_version: u64,  // 提交时间戳（如果已提交）
    pub action: Action,        // 执行的操作
}

impl CheckTxnStatusResponse {
    pub fn locked(lock_ttl: u64) -> Self {
        Self {
            region_error: None,
            lock_ttl,
            commit_version: 0,
            action: Action::NoAction,
        }
    }

    pub fn committed(commit_version: u64) -> Self {
        Self {
            region_error: None,
            lock_ttl: 0,
            commit_version,
            action: Action::NoAction,
        }
    }

    pub fn rolled_back(action: Action) -> Self {
        Self {
            region_error: None,
            lock_ttl: 0,
            commit_version: 0,
            action,
        }
    }

    pub fn with_region_error(error: Error) -> Self {
        Self {
            region_error: Some(error),
            lock_ttl: 0,
            commit_version: 0,
            action: Action::NoAction,
        }
    }
}

/// BatchRollbackRequest - 批量回滚请求
#[derive(Debug, Clone)]
pub struct BatchRollbackRequest {
    pub context: Context,
    pub start_version: u64,
    pub keys: Vec<Vec<u8>>,
}

/// BatchRollbackResponse - 批量回滚响应
#[derive(Debug, Clone)]
pub struct BatchRollbackResponse {
    pub region_error: Option<Error>,
    pub error: Option<KeyError>,
}

impl BatchRollbackResponse {
    pub fn ok() -> Self {
        Self {
            region_error: None,
            error: None,
        }
    }

    pub fn with_key_error(error: KeyError) -> Self {
        Self {
            region_error: None,
            error: Some(error),
        }
    }

    pub fn with_region_error(error: Error) -> Self {
        Self {
            region_error: Some(error),
            error: None,
        }
    }
}

/// ResolveLockRequest - 解决锁请求
#[derive(Debug, Clone)]
pub struct ResolveLockRequest {
    pub context: Context,
    pub start_version: u64,
    pub commit_version: u64,  // 0 表示回滚，>0 表示提交
}

/// ResolveLockResponse - 解决锁响应
#[derive(Debug, Clone)]
pub struct ResolveLockResponse {
    pub region_error: Option<Error>,
    pub error: Option<KeyError>,
}

impl ResolveLockResponse {
    pub fn ok() -> Self {
        Self {
            region_error: None,
            error: None,
        }
    }

    pub fn with_key_error(error: KeyError) -> Self {
        Self {
            region_error: None,
            error: Some(error),
        }
    }

    pub fn with_region_error(error: Error) -> Self {
        Self {
            region_error: Some(error),
            error: None,
        }
    }
}

/// Action - CheckTxnStatus 执行的操作
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    NoAction = 0,
    TtlExpireRollback = 1,      // 因 TTL 过期而回滚
    LockNotExistRollback = 2,   // 锁不存在，记录回滚
}

/// KeyError - 键错误
#[derive(Debug, Clone)]
pub struct KeyError {
    pub locked: Option<LockInfo>,        // 键被锁定
    pub retryable: Option<String>,       // 可重试错误
    pub abort: Option<String>,           // 应中止事务
    pub conflict: Option<WriteConflict>, // 写冲突
}

impl KeyError {
    pub fn locked(lock_info: LockInfo) -> Self {
        Self {
            locked: Some(lock_info),
            retryable: None,
            abort: None,
            conflict: None,
        }
    }

    pub fn retryable(msg: String) -> Self {
        Self {
            locked: None,
            retryable: Some(msg),
            abort: None,
            conflict: None,
        }
    }

    pub fn abort(msg: String) -> Self {
        Self {
            locked: None,
            retryable: None,
            abort: Some(msg),
            conflict: None,
        }
    }

    pub fn conflict(conflict: WriteConflict) -> Self {
        Self {
            locked: None,
            retryable: None,
            abort: None,
            conflict: Some(conflict),
        }
    }
}

/// LockInfo - 锁信息
#[derive(Debug, Clone)]
pub struct LockInfo {
    pub primary_lock: Vec<u8>,
    pub lock_version: u64,
    pub key: Vec<u8>,
    pub lock_ttl: u64,
}

/// WriteConflict - 写冲突
#[derive(Debug, Clone)]
pub struct WriteConflict {
    pub start_ts: u64,
    pub conflict_ts: u64,
    pub key: Vec<u8>,
    pub primary: Vec<u8>,
}



