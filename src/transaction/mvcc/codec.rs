/// MVCC 键编码解码工具
/// 
/// 基于 memcomparable 格式，确保键按用户键（升序）和时间戳（降序）排序

use anyhow::Result;

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = 0xFF;
const ENC_PAD: u8 = 0x00;

/// 编码字节数组（memcomparable 格式）
/// 
/// 格式: [group1][marker1]...[groupN][markerN]
/// group 是 8 字节的切片，不足部分用 0 填充
/// marker 是 `0xFF - padding 0 count`
pub fn encode_bytes(data: &[u8]) -> Vec<u8> {
    let d_len = data.len();
    let mut result = Vec::with_capacity((d_len / ENC_GROUP_SIZE + 1) * (ENC_GROUP_SIZE + 1) + 8);
    
    let mut idx = 0;
    while idx <= d_len {
        let remain = d_len - idx;
        let pad_count = if remain >= ENC_GROUP_SIZE {
            result.extend_from_slice(&data[idx..idx + ENC_GROUP_SIZE]);
            0
        } else {
            result.extend_from_slice(&data[idx..]);
            let pad = ENC_GROUP_SIZE - remain;
            result.extend_from_slice(&vec![ENC_PAD; pad]);
            pad
        };
        
        let marker = ENC_MARKER - pad_count as u8;
        result.push(marker);
        
        idx += ENC_GROUP_SIZE;
    }
    
    result
}

/// 解码字节数组（memcomparable 格式）
pub fn decode_bytes(b: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut data = Vec::new();
    let mut offset = 0;
    
    loop {
        if b.len() - offset < ENC_GROUP_SIZE + 1 {
            return Err(anyhow::anyhow!("insufficient bytes to decode value"));
        }
        
        let group_bytes = &b[offset..offset + ENC_GROUP_SIZE + 1];
        let group = &group_bytes[..ENC_GROUP_SIZE];
        let marker = group_bytes[ENC_GROUP_SIZE];
        
        let pad_count = (ENC_MARKER - marker) as usize;
        if pad_count > ENC_GROUP_SIZE {
            return Err(anyhow::anyhow!("invalid marker byte"));
        }
        
        let real_group_size = ENC_GROUP_SIZE - pad_count;
        data.extend_from_slice(&group[..real_group_size]);
        offset += ENC_GROUP_SIZE + 1;
        
        if pad_count != 0 {
            // 验证填充字节
            for v in &group[real_group_size..] {
                if *v != ENC_PAD {
                    return Err(anyhow::anyhow!("invalid padding byte"));
                }
            }
            break;
        }
    }
    
    Ok((data, offset))
}

/// 编码用户键和时间戳为 MVCC 键
/// 
/// 格式: [encoded_user_key][timestamp(8 bytes, inverted)]
/// 时间戳使用按位取反，确保降序排列
pub fn encode_key(user_key: &[u8], ts: u64) -> Vec<u8> {
    let mut encoded_key = encode_bytes(user_key);
    encoded_key.extend_from_slice(&(!ts).to_be_bytes());
    encoded_key
}

/// 从 MVCC 键解码出用户键
pub fn decode_user_key(encoded_key: &[u8]) -> Result<Vec<u8>> {
    let (user_key, _) = decode_bytes(encoded_key)?;
    Ok(user_key)
}

/// 从 MVCC 键解码出时间戳
pub fn decode_timestamp(encoded_key: &[u8]) -> Result<u64> {
    let (_, offset) = decode_bytes(encoded_key)?;
    if encoded_key.len() < offset + 8 {
        return Err(anyhow::anyhow!("insufficient bytes for timestamp"));
    }
    let ts_bytes = &encoded_key[offset..offset + 8];
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(ts_bytes);
    Ok(!u64::from_be_bytes(bytes))
}

/// 提取时间戳的物理时间部分
/// 
/// 时间戳通常由物理时间和逻辑时间组成
/// 物理时间用于超时检查
pub fn physical_time(ts: u64) -> u64 {
    // 假设物理时间在高 48 位，逻辑时间在低 16 位
    // 可以根据实际需求调整
    ts >> 16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_bytes() {
        let data = b"hello";
        let encoded = encode_bytes(data);
        let (decoded, _) = decode_bytes(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_encode_decode_key() {
        let user_key = b"test_key";
        let ts = 1234567890u64;
        let encoded = encode_key(user_key, ts);
        let decoded_key = decode_user_key(&encoded).unwrap();
        let decoded_ts = decode_timestamp(&encoded).unwrap();
        assert_eq!(decoded_key, user_key);
        assert_eq!(decoded_ts, ts);
    }

    #[test]
    fn test_key_ordering() {
        let key1 = encode_key(b"key", 100);
        let key2 = encode_key(b"key", 200);
        // 时间戳大的应该排在前面
        assert!(key1 > key2);
        
        let key3 = encode_key(b"key1", 100);
        let key4 = encode_key(b"key2", 100);
        // 用户键小的应该排在前面
        assert!(key3 < key4);
    }
}