use rocksdb::{DB, Options, WriteOptions};
use std::path::Path;
use crate::engine_util::WriteBatch;

pub struct Engines {
    pub kv: DB,
    pub kv_path: String,
    pub raft: Option<DB>,
    pub raft_path: String,
    // 写选项，用于控制 WAL 行为
    write_opts: WriteOptions,
}

impl Engines {
    pub fn new(kv_engine: DB, raft_engine: Option<DB>, kv_path: String, raft_path: String) -> Self {
        // 创建写选项，确保 WAL 启用并同步写入
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(true); // 同步写入，确保数据持久化
        write_opts.disable_wal(false); // 启用 WAL
        
        Self {
            kv: kv_engine,
            kv_path,
            raft: raft_engine,
            raft_path,
            write_opts,
        }
    }
    
    /// 写入 KV 数据，使用同步写入确保持久化
    pub fn write_kv(&self, wb: &WriteBatch) -> anyhow::Result<()> {
        wb.write_to_db_with_options(&self.kv, &self.write_opts)
    }
    
    /// 写入 Raft 数据，使用同步写入确保持久化
    pub fn write_raft(&self, wb: &WriteBatch) -> anyhow::Result<()> {
        if let Some(ref raft_db) = self.raft {
            wb.write_to_db_with_options(raft_db, &self.write_opts)
        } else {
            Ok(())
        }
    }
    
    /// 写入 KV 数据（异步，不等待同步）
    pub fn write_kv_async(&self, wb: &WriteBatch) -> anyhow::Result<()> {
        wb.write_to_db(&self.kv)
    }
    
    /// 写入 Raft 数据（异步，不等待同步）
    pub fn write_raft_async(&self, wb: &WriteBatch) -> anyhow::Result<()> {
        if let Some(ref raft_db) = self.raft {
            wb.write_to_db(raft_db)
        } else {
            Ok(())
        }
    }
    
    /// 强制刷新 WAL 到磁盘
    pub fn flush_wal(&self) -> anyhow::Result<()> {
        self.kv.flush_wal(true)?;
        if let Some(ref raft_db) = self.raft {
            raft_db.flush_wal(true)?;
        }
        Ok(())
    }
    
    pub fn close(&self) -> anyhow::Result<()> {
        // RocksDB 会在 Drop 时自动关闭
        Ok(())
    }
    
    pub fn destroy(&self) -> anyhow::Result<()> {
        self.close()?;
        std::fs::remove_dir_all(&self.kv_path)?;
        if Path::new(&self.raft_path).exists() {
            std::fs::remove_dir_all(&self.raft_path)?;
        }
        Ok(())
    }
}

pub fn create_db(path: &str, raft: bool) -> anyhow::Result<DB> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    
    // 启用 WAL（默认启用，但显式设置以确保）
    opts.set_wal_recovery_mode(rocksdb::DBRecoveryMode::PointInTime);
    
    // 设置 WAL 目录（可选，默认与数据目录相同）
    // opts.set_wal_dir(wal_path);
    
    if raft {
        // Raft engine 不需要写 blob，因为很快会被删除
        // RocksDB 中通过设置来优化
        // 但 WAL 仍然需要，以确保 Raft 日志的持久化
    } else {
        // KV engine 需要确保数据持久化
        // 设置合理的 WAL 大小限制
        opts.set_max_total_wal_size(1024 * 1024 * 1024); // 1GB
    }
    
    std::fs::create_dir_all(path)?;
    let db = DB::open(&opts, path)?;
    Ok(db)
}