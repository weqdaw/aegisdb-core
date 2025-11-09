pub mod modify;
pub mod standalone_storage;
pub mod reader;

pub use modify::{Modify, Put, Delete};
pub use standalone_storage::StandaloneStorage;
pub use reader::StandaloneReader;

use async_trait::async_trait;
use crate::engine_util::DBIterator;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn start(&self) -> anyhow::Result<()>;
    async fn stop(&self) -> anyhow::Result<()>;
    async fn write(&self, batch: Vec<Modify>) -> anyhow::Result<()>;
    async fn reader(&self) -> anyhow::Result<Box<dyn StorageReader>>;
}

#[async_trait]
pub trait StorageReader: Send + Sync {
    async fn get_cf(&self, cf: &str, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>>;
    fn iter_cf(&self, cf: &str) -> Box<dyn DBIterator>;
    fn close(&self);
}

// 测试代码
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::standalone_storage::StandaloneStorage;
    use crate::config::Config;
    use tempfile::TempDir;

    async fn create_test_storage() -> (StandaloneStorage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::new_test();
        config.db_path = temp_dir.path().to_str().unwrap().to_string();
        let storage = StandaloneStorage::new(&config).unwrap();
        (storage, temp_dir)
    }

    #[tokio::test]
    async fn test_storage_write_and_read() {
        let (storage, _temp_dir) = create_test_storage().await;
        storage.start().await.unwrap();

        // 测试写入
        let batch = vec![
            Modify::Put(Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
                cf: "default".to_string(),
            }),
            Modify::Put(Put {
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
                cf: "default".to_string(),
            }),
        ];

        storage.write(batch).await.unwrap();

        // 测试读取
        let reader = storage.reader().await.unwrap();
        let value1 = reader.get_cf("default", b"key1").await.unwrap();
        assert_eq!(value1, Some(b"value1".to_vec()));

        let value2 = reader.get_cf("default", b"key2").await.unwrap();
        assert_eq!(value2, Some(b"value2".to_vec()));

        // 测试不存在的键
        let value3 = reader.get_cf("default", b"key3").await.unwrap();
        assert_eq!(value3, None);

        storage.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_storage_delete() {
        let (storage, _temp_dir) = create_test_storage().await;
        storage.start().await.unwrap();

        // 先写入
        let batch = vec![Modify::Put(Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            cf: "default".to_string(),
        })];
        storage.write(batch).await.unwrap();

        // 验证存在
        let reader = storage.reader().await.unwrap();
        assert_eq!(
            reader.get_cf("default", b"key1").await.unwrap(),
            Some(b"value1".to_vec())
        );

        // 删除
        let delete_batch = vec![Modify::Delete(Delete {
            key: b"key1".to_vec(),
            cf: "default".to_string(),
        })];
        storage.write(delete_batch).await.unwrap();

        // 验证已删除
        let reader2 = storage.reader().await.unwrap();
        assert_eq!(reader2.get_cf("default", b"key1").await.unwrap(), None);

        storage.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_storage_column_families() {
        let (storage, _temp_dir) = create_test_storage().await;
        storage.start().await.unwrap();

        let batch = vec![
            Modify::Put(Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
                cf: "default".to_string(),
            }),
            Modify::Put(Put {
                key: b"key1".to_vec(),
                value: b"value2".to_vec(),
                cf: "write".to_string(),
            }),
            Modify::Put(Put {
                key: b"key1".to_vec(),
                value: b"value3".to_vec(),
                cf: "lock".to_string(),
            }),
        ];

        storage.write(batch).await.unwrap();

        let reader = storage.reader().await.unwrap();
        assert_eq!(
            reader.get_cf("default", b"key1").await.unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            reader.get_cf("write", b"key1").await.unwrap(),
            Some(b"value2".to_vec())
        );
        assert_eq!(
            reader.get_cf("lock", b"key1").await.unwrap(),
            Some(b"value3".to_vec())
        );

        storage.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_storage_batch_write() {
        let (storage, _temp_dir) = create_test_storage().await;
        storage.start().await.unwrap();

        // 批量写入 100 个键值对
        let mut batch = Vec::new();
        for i in 0..100 {
            batch.push(Modify::Put(Put {
                key: format!("key{}", i).into_bytes(),
                value: format!("value{}", i).into_bytes(),
                cf: "default".to_string(),
            }));
        }

        storage.write(batch).await.unwrap();

        // 验证所有键值对
        let reader = storage.reader().await.unwrap();
        for i in 0..100 {
            let key = format!("key{}", i).into_bytes();
            let expected_value = format!("value{}", i).into_bytes();
            let value = reader.get_cf("default", &key).await.unwrap();
            assert_eq!(value, Some(expected_value));
        }

        storage.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_storage_cache() {
        let (storage, _temp_dir) = create_test_storage().await;
        storage.start().await.unwrap();

        // 写入数据
        let batch = vec![Modify::Put(Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            cf: "default".to_string(),
        })];
        storage.write(batch).await.unwrap();

        // 第一次读取（应该从数据库）
        let reader1 = storage.reader().await.unwrap();
        let value1 = reader1.get_cf("default", b"key1").await.unwrap();
        assert_eq!(value1, Some(b"value1".to_vec()));

        // 第二次读取（应该从缓存）
        let reader2 = storage.reader().await.unwrap();
        let value2 = reader2.get_cf("default", b"key1").await.unwrap();
        assert_eq!(value2, Some(b"value1".to_vec()));

        storage.stop().await.unwrap();
    }
}