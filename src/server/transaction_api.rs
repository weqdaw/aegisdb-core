use crate::proto::kvrpcpb::*;
use crate::server::Server;
use crate::storage::Storage;
use crate::transaction::mvcc::*;
use crate::transaction::latches::Latches;
use crate::engine_util::CF_LOCK;
use anyhow::Result;

/// 事务 API 服务器
pub struct TransactionKvServer;

impl TransactionKvServer {
    /// KvGet - 事务读取
    /// 
    /// 在指定时间戳读取键的值
    pub async fn kv_get<S: Storage>(
        server: &Server<S>,
        req: GetRequest,
    ) -> Result<GetResponse> {
        let reader = server.storage().reader().await?;
        let txn = transaction::MvccTxn::new(reader, req.version);

        // 检查锁
        if let Some(lock) = txn.get_lock(&req.key).await? {
            if lock.ts != req.version {
                // 键被其他事务锁定
                let lock_info = LockInfo {
                    primary_lock: lock.primary.clone(),
                    lock_version: lock.ts,
                    key: req.key.clone(),
                    lock_ttl: lock.ttl,
                };
                return Ok(GetResponse::with_key_error(KeyError::locked(lock_info)));
            }
        }

        // 读取值
        match txn.get_value(&req.key).await {
            Ok(Some(value)) => Ok(GetResponse::ok(Some(value))),
            Ok(None) => Ok(GetResponse::ok(None)),
            Err(e) => {
                // 处理锁错误
                if e.to_string().contains("locked") {
                    if let Some(lock) = txn.get_lock(&req.key).await? {
                        let lock_info = LockInfo {
                            primary_lock: lock.primary.clone(),
                            lock_version: lock.ts,
                            key: req.key.clone(),
                            lock_ttl: lock.ttl,
                        };
                        Ok(GetResponse::with_key_error(KeyError::locked(lock_info)))
                    } else {
                        Ok(GetResponse::ok(None))
                    }
                } else {
                    Err(e)
                }
            }
        }
    }

    /// KvPrewrite - 两阶段提交第一阶段
    /// 
    /// 锁定所有键并写入值
    pub async fn kv_prewrite<S: Storage>(
        server: &Server<S>,
        latches: &Latches,
        req: PrewriteRequest,
    ) -> Result<PrewriteResponse> {
        // 收集所有要锁定的键
        let keys: Vec<Vec<u8>> = req.mutations.iter().map(|m| m.key.clone()).collect();
        
        // 获取锁存器
        latches.wait_for_latches(&keys).await;

        let reader = server.storage().reader().await?;
        let mut txn = transaction::MvccTxn::new(reader, req.start_version);
        let mut errors = Vec::new();

        // 处理每个 mutation
        for mutation in &req.mutations {
            let key = &mutation.key;

            // 检查是否已有锁
            if let Some(lock) = txn.get_lock(key).await? {
                if lock.ts != req.start_version {
                    // 被其他事务锁定
                    let lock_info = LockInfo {
                        primary_lock: lock.primary.clone(),
                        lock_version: lock.ts,
                        key: key.clone(),
                        lock_ttl: lock.ttl,
                    };
                    errors.push(KeyError::locked(lock_info));
                    continue;
                }
            }

            // 检查是否有写冲突（检查是否有 commit_ts > start_version 的 Write）
            if let Ok((Some(_write), commit_ts)) = txn.most_recent_write(key).await {
                if commit_ts > req.start_version {
                    // 写冲突
                    let conflict = WriteConflict {
                        start_ts: req.start_version,
                        conflict_ts: commit_ts,
                        key: key.clone(),
                        primary: req.primary_lock.clone(),
                    };
                    errors.push(KeyError::conflict(conflict));
                    continue;
                }
            }

            // 写入值
            match mutation.op {
                Op::Put => {
                    txn.put_value(key, &mutation.value);
                }
                Op::Del => {
                    txn.delete_value(key);
                }
                _ => {
                    errors.push(KeyError::abort(format!("unsupported operation: {:?}", mutation.op)));
                    continue;
                }
            }

            // 添加锁
            let lock = lock::Lock {
                primary: req.primary_lock.clone(),
                ts: req.start_version,
                ttl: req.lock_ttl,
                kind: match mutation.op {
                    Op::Put => write::WriteKind::Put,
                    Op::Del => write::WriteKind::Delete,
                    _ => write::WriteKind::Put,
                },
            };
            txn.put_lock(key, &lock);
        }

        // 如果有错误，释放锁存器并返回
        if !errors.is_empty() {
            latches.release_latches(&keys);
            return Ok(PrewriteResponse::with_errors(errors));
        }

        // 提交所有写入
        server.storage().write(txn.writes().to_vec()).await?;
        latches.release_latches(&keys);

        Ok(PrewriteResponse::ok())
    }

    /// KvCommit - 两阶段提交第二阶段
    /// 
    /// 提交事务，将锁转换为 Write 记录
    pub async fn kv_commit<S: Storage>(
        server: &Server<S>,
        latches: &Latches,
        req: CommitRequest,
    ) -> Result<CommitResponse> {
        // 获取锁存器
        latches.wait_for_latches(&req.keys).await;

        let reader = server.storage().reader().await?;
        let mut txn = transaction::MvccTxn::new(reader, req.start_version);

        // 检查所有键是否都被当前事务锁定
        for key in &req.keys {
            if let Some(lock) = txn.get_lock(key).await? {
                if lock.ts != req.start_version {
                    // 锁不属于当前事务
                    let lock_info = LockInfo {
                        primary_lock: lock.primary.clone(),
                        lock_version: lock.ts,
                        key: key.clone(),
                        lock_ttl: lock.ttl,
                    };
                    latches.release_latches(&req.keys);
                    return Ok(CommitResponse::with_key_error(KeyError::locked(lock_info)));
                }
            } else {
                // 键未被锁定，可能已被回滚或提交
                // 检查是否已提交
                if let Ok((Some(_write), commit_ts)) = txn.current_write(key).await {
                    if commit_ts == req.commit_version {
                        // 已经提交，继续
                        continue;
                    }
                }
                // 未找到锁或 Write，返回错误
                latches.release_latches(&req.keys);
                return Ok(CommitResponse::with_key_error(KeyError::abort(
                    format!("key {} is not locked", String::from_utf8_lossy(key))
                )));
            }
        }

        // 为每个键创建 Write 记录并删除锁
        for key in &req.keys {
            // 获取锁以确定 Write 类型
            if let Some(lock) = txn.get_lock(key).await? {
                if lock.ts == req.start_version {
                    // 创建 Write 记录
                    let write = write::Write {
                        start_ts: req.start_version,
                        kind: lock.kind.clone(),
                    };
                    txn.put_write(key, req.commit_version, &write);
                    
                    // 删除锁
                    txn.delete_lock(key);
                }
            }
        }

        // 提交所有写入
        server.storage().write(txn.writes().to_vec()).await?;
        latches.release_latches(&req.keys);

        Ok(CommitResponse::ok())
    }

    /// KvScan - 事务扫描
    /// 
    /// 在指定时间戳扫描多个键值对
    pub async fn kv_scan<S: Storage>(
        server: &Server<S>,
        req: ScanRequest,
    ) -> Result<ScanResponse> {
        let reader = server.storage().reader().await?;
        let txn = transaction::MvccTxn::new(reader, req.version);
        let mut scanner = scanner::Scanner::new(&req.start_key, txn);
        scanner.set_limit(req.limit);

        let mut pairs = Vec::new();
        let mut count = 0;

        while count < req.limit {
            match scanner.next().await? {
                Some((key, value)) => {
                    // 检查锁
                    let reader2 = server.storage().reader().await?;
                    let txn2 = transaction::MvccTxn::new(reader2, req.version);
                    if let Some(lock) = txn2.get_lock(&key).await? {
                        if lock.ts != req.version {
                            // 键被锁定，记录错误但继续扫描
                            let lock_info = LockInfo {
                                primary_lock: lock.primary.clone(),
                                lock_version: lock.ts,
                                key: key.clone(),
                                lock_ttl: lock.ttl,
                            };
                            pairs.push(KvPair::with_error(key, KeyError::locked(lock_info)));
                            count += 1;
                            continue;
                        }
                    }
                    pairs.push(KvPair::new(key, value));
                    count += 1;
                }
                None => break,
            }
        }

        Ok(ScanResponse::ok(pairs))
    }

    /// KvCheckTxnStatus - 检查事务状态
    /// 
    /// 检查主键的锁状态，如果过期则回滚
    pub async fn kv_check_txn_status<S: Storage>(
        server: &Server<S>,
        latches: &Latches,
        req: CheckTxnStatusRequest,
    ) -> Result<CheckTxnStatusResponse> {
        let keys = vec![req.primary_key.clone()];
        latches.wait_for_latches(&keys).await;

        let reader = server.storage().reader().await?;
        let mut txn = transaction::MvccTxn::new(reader, req.lock_ts);

        // 检查锁是否存在
        if let Some(lock) = txn.get_lock(&req.primary_key).await? {
            if lock.ts != req.lock_ts {
                // 锁不属于当前事务
                latches.release_latches(&keys);
                return Ok(CheckTxnStatusResponse::locked(lock.ttl));
            }

            // 检查 TTL 是否过期
            let physical_time = codec::physical_time(req.current_ts);
            let lock_physical_time = codec::physical_time(lock.ts);
            let ttl_ms = lock.ttl;

            if physical_time > lock_physical_time + ttl_ms {
                // TTL 过期，回滚
                txn.delete_lock(&req.primary_key);
                
                // 创建 Rollback Write 记录
                let write = write::Write {
                    start_ts: req.lock_ts,
                    kind: write::WriteKind::Rollback,
                };
                txn.put_write(&req.primary_key, req.lock_ts, &write);

                // 删除预写的值
                txn.delete_value(&req.primary_key);

                server.storage().write(txn.writes().to_vec()).await?;
                latches.release_latches(&keys);
                return Ok(CheckTxnStatusResponse::rolled_back(Action::TtlExpireRollback));
            }

            // 锁仍然有效
            latches.release_latches(&keys);
            return Ok(CheckTxnStatusResponse::locked(lock.ttl));
        }

        // 锁不存在，检查是否已提交
        if let Ok((Some(_write), commit_ts)) = txn.current_write(&req.primary_key).await {
            if commit_ts > 0 {
                latches.release_latches(&keys);
                return Ok(CheckTxnStatusResponse::committed(commit_ts));
            }
        }

        // 锁不存在且未提交，记录回滚
        let write = write::Write {
            start_ts: req.lock_ts,
            kind: write::WriteKind::Rollback,
        };
        txn.put_write(&req.primary_key, req.lock_ts, &write);
        server.storage().write(txn.writes().to_vec()).await?;
        latches.release_latches(&keys);

        Ok(CheckTxnStatusResponse::rolled_back(Action::LockNotExistRollback))
    }

    /// KvBatchRollback - 批量回滚
    /// 
    /// 回滚事务的所有键
    pub async fn kv_batch_rollback<S: Storage>(
        server: &Server<S>,
        latches: &Latches,
        req: BatchRollbackRequest,
    ) -> Result<BatchRollbackResponse> {
        // 获取锁存器
        latches.wait_for_latches(&req.keys).await;

        let reader = server.storage().reader().await?;
        let mut txn = transaction::MvccTxn::new(reader, req.start_version);

        // 处理每个键
        for key in &req.keys {
            // 检查锁是否属于当前事务
            if let Some(lock) = txn.get_lock(key).await? {
                if lock.ts != req.start_version {
                    // 锁不属于当前事务
                    let lock_info = LockInfo {
                        primary_lock: lock.primary.clone(),
                        lock_version: lock.ts,
                        key: key.clone(),
                        lock_ttl: lock.ttl,
                    };
                    latches.release_latches(&req.keys);
                    return Ok(BatchRollbackResponse::with_key_error(KeyError::locked(lock_info)));
                }

                // 删除锁
                txn.delete_lock(key);
            }

            // 检查是否已提交
            if let Ok((Some(_write), commit_ts)) = txn.current_write(key).await {
                if commit_ts > req.start_version {
                    // 已提交，不能回滚
                    latches.release_latches(&req.keys);
                    return Ok(BatchRollbackResponse::with_key_error(KeyError::abort(
                        format!("key {} is already committed", String::from_utf8_lossy(key))
                    )));
                }
            }

            // 创建 Rollback Write 记录
            let write = write::Write {
                start_ts: req.start_version,
                kind: write::WriteKind::Rollback,
            };
            txn.put_write(key, req.start_version, &write);

            // 删除预写的值
            txn.delete_value(key);
        }

        // 提交所有写入
        server.storage().write(txn.writes().to_vec()).await?;
        latches.release_latches(&req.keys);

        Ok(BatchRollbackResponse::ok())
    }

    /// KvResolveLock - 解决锁冲突
    /// 
    /// 根据 commit_version 决定提交或回滚所有锁
    pub async fn kv_resolve_lock<S: Storage>(
        server: &Server<S>,
        latches: &Latches,
        req: ResolveLockRequest,
    ) -> Result<ResolveLockResponse> {
        let reader = server.storage().reader().await?;
        
        // 查找所有属于该事务的锁
        let mut iter = reader.iter_cf(CF_LOCK);
        iter.seek(&[]);
        
        let mut keys_to_resolve = Vec::new();
        while iter.valid() {
            let item = iter.item();
            let key = item.key();
            let value = item.value()?;
            
            if let Ok(lock) = lock::Lock::parse(&value) {
                if lock.ts == req.start_version {
                    keys_to_resolve.push(key.to_vec());
                }
            }
            
            iter.next();
        }

        if keys_to_resolve.is_empty() {
            return Ok(ResolveLockResponse::ok());
        }

        // 获取锁存器
        latches.wait_for_latches(&keys_to_resolve).await;

        let reader2 = server.storage().reader().await?;
        let mut txn = transaction::MvccTxn::new(reader2, req.start_version);

        if req.commit_version > 0 {
            // 提交所有锁
            for key in &keys_to_resolve {
                if let Some(lock) = txn.get_lock(key).await? {
                    if lock.ts == req.start_version {
                        // 创建 Write 记录
                        let write = write::Write {
                            start_ts: req.start_version,
                            kind: lock.kind.clone(),
                        };
                        txn.put_write(key, req.commit_version, &write);
                        
                        // 删除锁
                        txn.delete_lock(key);
                    }
                }
            }
        } else {
            // 回滚所有锁
            for key in &keys_to_resolve {
                if let Some(lock) = txn.get_lock(key).await? {
                    if lock.ts == req.start_version {
                        // 删除锁
                        txn.delete_lock(key);
                        
                        // 创建 Rollback Write 记录
                        let write = write::Write {
                            start_ts: req.start_version,
                            kind: write::WriteKind::Rollback,
                        };
                        txn.put_write(key, req.start_version, &write);
                        
                        // 删除预写的值
                        txn.delete_value(key);
                    }
                }
            }
        }

        // 提交所有写入
        server.storage().write(txn.writes().to_vec()).await?;
        latches.release_latches(&keys_to_resolve);

        Ok(ResolveLockResponse::ok())
    }
}