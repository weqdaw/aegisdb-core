/// 多级键编码工具
/// 
/// 用于将一级键和二级键编码为单个字节数组
/// 格式: [primary_key_len(4 bytes)][primary_key][secondary_key]
/// 这样可以保证键的字典序正确，并且可以轻松解码

const SEPARATOR: u8 = 0xFF; // 使用 0xFF 作为分隔符，因为它是最大的单字节值

/// 编码多级键
/// 
/// # Arguments
/// * `primary_key` - 一级键（如 region_id 的字节表示）
/// * `secondary_key` - 二级键（用户提供的键）
/// 
/// # Returns
/// 编码后的完整键
pub fn encode_multi_level_key(primary_key: &[u8], secondary_key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(4 + primary_key.len() + 1 + secondary_key.len());
    
    // 写入 primary_key 长度（4字节，大端序）
    let pk_len = primary_key.len() as u32;
    encoded.extend_from_slice(&pk_len.to_be_bytes());
    
    // 写入 primary_key
    encoded.extend_from_slice(primary_key);
    
    // 写入分隔符
    encoded.push(SEPARATOR);
    
    // 写入 secondary_key
    encoded.extend_from_slice(secondary_key);
    
    encoded
}

/// 解码多级键
/// 
/// # Arguments
/// * `encoded_key` - 编码后的完整键
/// 
/// # Returns
/// (primary_key, secondary_key) 的元组
pub fn decode_multi_level_key(encoded_key: &[u8]) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    if encoded_key.len() < 5 {
        return Err(anyhow::anyhow!("Encoded key too short"));
    }
    
    // 读取 primary_key 长度
    let pk_len = u32::from_be_bytes([
        encoded_key[0],
        encoded_key[1],
        encoded_key[2],
        encoded_key[3],
    ]) as usize;
    
    if encoded_key.len() < 5 + pk_len {
        return Err(anyhow::anyhow!("Invalid encoded key format"));
    }
    
    // 提取 primary_key
    let primary_key = encoded_key[4..4 + pk_len].to_vec();
    
    // 检查分隔符
    if encoded_key.len() < 5 + pk_len + 1 || encoded_key[4 + pk_len] != SEPARATOR {
        return Err(anyhow::anyhow!("Invalid separator in encoded key"));
    }
    
    // 提取 secondary_key
    let secondary_key = encoded_key[5 + pk_len..].to_vec();
    
    Ok((primary_key, secondary_key))
}

/// 从 region_id 生成 primary_key 字节
pub fn primary_key_from_region_id(region_id: u64) -> Vec<u8> {
    region_id.to_be_bytes().to_vec()
}

/// 从 primary_key 字节恢复 region_id
pub fn region_id_from_primary_key(primary_key: &[u8]) -> anyhow::Result<u64> {
    if primary_key.len() != 8 {
        return Err(anyhow::anyhow!("Invalid primary key length for region_id"));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(primary_key);
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode() {
        let primary = b"region_1";
        let secondary = b"key1";
        
        let encoded = encode_multi_level_key(primary, secondary);
        let (decoded_primary, decoded_secondary) = decode_multi_level_key(&encoded).unwrap();
        
        assert_eq!(decoded_primary, primary);
        assert_eq!(decoded_secondary, secondary);
    }

    #[test]
    fn test_region_id_encoding() {
        let region_id = 12345u64;
        let primary = primary_key_from_region_id(region_id);
        let secondary = b"user:1";
        
        let encoded = encode_multi_level_key(&primary, secondary);
        let (decoded_primary, _) = decode_multi_level_key(&encoded).unwrap();
        let decoded_region_id = region_id_from_primary_key(&decoded_primary).unwrap();
        
        assert_eq!(decoded_region_id, region_id);
    }

    #[test]
    fn test_key_ordering() {
        // 测试键的字典序
        let primary = b"region_1";
        let secondary1 = b"key1";
        let secondary2 = b"key2";
        
        let encoded1 = encode_multi_level_key(primary, secondary1);
        let encoded2 = encode_multi_level_key(primary, secondary2);
        
        assert!(encoded1 < encoded2); // 保证字典序正确
    }
}