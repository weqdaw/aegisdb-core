/// Write 结构体表示已提交的写操作
/// 
/// 序列化后存储在 "write" CF 中，用于 MVCC 查找

use crate::proto::kvrpcpb::Op;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteKind {
    Put = 1,
    Delete = 2,
    Rollback = 3,
}

impl WriteKind {
    pub fn to_proto(&self) -> Op {
        match self {
            WriteKind::Put => Op::Put,
            WriteKind::Delete => Op::Del,
            WriteKind::Rollback => Op::Rollback,
        }
    }

    pub fn from_proto(op: Op) -> Self {
        match op {
            Op::Put => WriteKind::Put,
            Op::Del => WriteKind::Delete,
            Op::Rollback => WriteKind::Rollback,
        }
    }
}

impl From<u8> for WriteKind {
    fn from(byte: u8) -> Self {
        match byte {
            1 => WriteKind::Put,
            2 => WriteKind::Delete,
            3 => WriteKind::Rollback,
            _ => panic!("invalid write kind: {}", byte),
        }
    }
}

/// Write 表示一个已提交的写操作
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write {
    /// 写操作所在事务的 start timestamp
    pub start_ts: u64,
    /// 写操作类型
    pub kind: WriteKind,
}

impl Write {
    /// 序列化为字节数组
    /// 
    /// 格式: [kind(1 byte)][start_ts(8 bytes)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; 9];
        buf[0] = self.kind.clone() as u8;
        buf[1..9].copy_from_slice(&self.start_ts.to_be_bytes());
        buf
    }

    /// 从字节数组解析 Write
    pub fn parse(value: &[u8]) -> Result<Option<Self>, String> {
        if value.is_empty() {
            return Ok(None);
        }
        if value.len() != 9 {
            return Err(format!(
                "mvcc/write/ParseWrite: value is incorrect length, expected 9, found {}",
                value.len()
            ));
        }
        let kind = WriteKind::from(value[0]);
        let start_ts = u64::from_be_bytes([
            value[1], value[2], value[3], value[4],
            value[5], value[6], value[7], value[8],
        ]);
        Ok(Some(Write { start_ts, kind }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_serialization() {
        let write = Write {
            start_ts: 1234567890,
            kind: WriteKind::Put,
        };
        let bytes = write.to_bytes();
        let parsed = Write::parse(&bytes).unwrap().unwrap();
        assert_eq!(write, parsed);
    }

    #[test]
    fn test_write_kinds() {
        let put = Write {
            start_ts: 100,
            kind: WriteKind::Put,
        };
        let delete = Write {
            start_ts: 100,
            kind: WriteKind::Delete,
        };
        let rollback = Write {
            start_ts: 100,
            kind: WriteKind::Rollback,
        };
        
        assert_eq!(put.kind.to_proto(), Op::Put);
        assert_eq!(delete.kind.to_proto(), Op::Del);
        assert_eq!(rollback.kind.to_proto(), Op::Rollback);
    }
}