/// Scanner 用于扫描多个键值对
/// 
/// 它处理 MVCC 的多版本特性，只返回逻辑上有效的键值对

use crate::engine_util::{CF_DEFAULT, CF_WRITE};
use crate::transaction::mvcc::codec::*;
use crate::transaction::mvcc::write::Write;
use crate::transaction::mvcc::transaction::MvccTxn;
use anyhow::Result;

pub struct Scanner {
    txn: MvccTxn,
    #[allow(dead_code)]
    start_key: Vec<u8>,
    end_key: Option<Vec<u8>>,
    limit: u32,
    count: u32,
    current_key: Option<Vec<u8>>,
    write_iter: Box<dyn crate::engine_util::DBIterator>,
    exhausted: bool,
}

impl Scanner {
    /// 创建新的 Scanner
    pub fn new(start_key: &[u8], txn: MvccTxn) -> Self {
        let mut write_iter = txn.reader.iter_cf(CF_WRITE);
        let search_key = encode_key(start_key, u64::MAX);
        write_iter.seek(&search_key);
        
        Self {
            txn,
            start_key: start_key.to_vec(),
            end_key: None,
            limit: u32::MAX,
            count: 0,
            current_key: None,
            write_iter,
            exhausted: false,
        }
    }

    /// 设置结束键
    pub fn set_end_key(&mut self, end_key: Option<Vec<u8>>) {
        self.end_key = end_key;
    }

    /// 设置限制
    pub fn set_limit(&mut self, limit: u32) {
        self.limit = limit;
    }

    /// 获取下一个键值对
    pub async fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.exhausted || self.count >= self.limit {
            return Ok(None);
        }

        while self.write_iter.valid() {
            let item = self.write_iter.item();
            let encoded_key = item.key();
            
            // 解码用户键
            let user_key = match decode_user_key(encoded_key) {
                Ok(k) => k,
                Err(_) => {
                    self.write_iter.next();
                    continue;
                }
            };
            
            // 检查是否超过结束键
            if let Some(ref end_key) = self.end_key {
                if &user_key >= end_key {
                    self.exhausted = true;
                    return Ok(None);
                }
            }
            
            // 跳过已处理过的键
            if let Some(ref current) = self.current_key {
                if &user_key == current {
                    self.write_iter.next();
                    continue;
                }
            }
            
            // 解码时间戳
            let commit_ts = match decode_timestamp(encoded_key) {
                Ok(ts) => ts,
                Err(_) => {
                    self.write_iter.next();
                    continue;
                }
            };
            
            // 检查时间戳是否有效
            if commit_ts > self.txn.start_ts {
                self.write_iter.next();
                continue;
            }
            
            // 解析 Write
            let value = match item.value() {
                Ok(v) => v,
                Err(_) => {
                    self.write_iter.next();
                    continue;
                }
            };
            let write = match Write::parse(&value) {
                Ok(Some(w)) => w,
                _ => {
                    self.write_iter.next();
                    continue;
                }
            };
            
            // 处理不同类型的 Write
            match write.kind {
                crate::transaction::mvcc::write::WriteKind::Put => {
                    // 读取值
                    let encoded_value_key = encode_key(&user_key, write.start_ts);
                    if let Some(value) = self.txn.reader.get_cf(CF_DEFAULT, &encoded_value_key).await? {
                        self.current_key = Some(user_key.clone());
                        self.count += 1;
                        return Ok(Some((user_key, value)));
                    }
                }
                crate::transaction::mvcc::write::WriteKind::Delete => {
                    // 跳过已删除的键
                    self.current_key = Some(user_key.clone());
                    self.write_iter.next();
                    continue;
                }
                crate::transaction::mvcc::write::WriteKind::Rollback => {
                    // 跳过回滚的键
                    self.current_key = Some(user_key.clone());
                    self.write_iter.next();
                    continue;
                }
            }
            
            self.write_iter.next();
        }
        
        self.exhausted = true;
        Ok(None)
    }

    /// 关闭 Scanner
    pub fn close(&self) {
        self.txn.reader.close();
    }
}