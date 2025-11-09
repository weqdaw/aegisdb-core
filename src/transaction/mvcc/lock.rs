/// Lock 结构体表示事务锁
/// 
/// 存储在 "lock" CF 中，用于两阶段提交协议

use crate::transaction::mvcc::write::WriteKind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lock {
    /// 主键（primary key），用于标识事务
    pub primary: Vec<u8>,
    /// 锁的时间戳（事务的 start timestamp）
    pub ts: u64,
    /// 生存时间（TTL）
    pub ttl: u64,
    /// 锁类型
    pub kind: WriteKind,
}

impl Lock {
    /// 序列化为字节数组
    /// 
    /// 格式: [primary][kind(1 byte)][ts(8 bytes)][ttl(8 bytes)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.primary.len() + 17);
        buf.extend_from_slice(&self.primary);
        buf.push(self.kind.clone() as u8);
        buf.extend_from_slice(&self.ts.to_be_bytes());
        buf.extend_from_slice(&self.ttl.to_be_bytes());
        buf
    }

    /// 从字节数组解析 Lock
    pub fn parse(input: &[u8]) -> Result<Self, String> {
        if input.len() <= 17 {
            return Err(format!(
                "mvcc: error parsing lock, not enough input, found {} bytes",
                input.len()
            ));
        }
        
        let primary_len = input.len() - 17;
        let primary = input[..primary_len].to_vec();
        let kind = WriteKind::from(input[primary_len]);
        let ts = u64::from_be_bytes([
            input[primary_len + 1],
            input[primary_len + 2],
            input[primary_len + 3],
            input[primary_len + 4],
            input[primary_len + 5],
            input[primary_len + 6],
            input[primary_len + 7],
            input[primary_len + 8],
        ]);
        let ttl = u64::from_be_bytes([
            input[primary_len + 9],
            input[primary_len + 10],
            input[primary_len + 11],
            input[primary_len + 12],
            input[primary_len + 13],
            input[primary_len + 14],
            input[primary_len + 15],
            input[primary_len + 16],
        ]);
        
        Ok(Lock { primary, ts, ttl, kind })
    }

    /// 检查锁是否对给定的 key 和 txn_start_ts 有效
    pub fn is_locked_for(&self, _key: &[u8], txn_start_ts: u64) -> bool {
        if self.ts <= txn_start_ts {
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_serialization() {
        let lock = Lock {
            primary: b"primary_key".to_vec(),
            ts: 1234567890,
            ttl: 3000,
            kind: WriteKind::Put,
        };
        let bytes = lock.to_bytes();
        let parsed = Lock::parse(&bytes).unwrap();
        assert_eq!(lock, parsed);
    }

    #[test]
    fn test_lock_is_locked_for() {
        let lock = Lock {
            primary: b"primary".to_vec(),
            ts: 100,
            ttl: 3000,
            kind: WriteKind::Put,
        };
        assert!(lock.is_locked_for(b"key", 200)); // ts <= txn_start_ts
        assert!(!lock.is_locked_for(b"key", 50)); // ts > txn_start_ts
    }
}