use aegisdb::{Config, StandaloneStorage, Storage, Modify, Put, Delete};
use tempfile::TempDir;

async fn setup_storage() -> (StandaloneStorage, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::new_test();
    config.db_path = temp_dir.path().to_str().unwrap().to_string();
    let storage = StandaloneStorage::new(&config).unwrap();
    (storage, temp_dir)
}

#[tokio::test]
async fn test_integration_basic_operations() {
    let (storage, _temp_dir) = setup_storage().await;
    storage.start().await.unwrap();

    // 写入多个键值对
    let batch = vec![
        Modify::Put(Put {
            key: b"user:1".to_vec(),
            value: b"Alice".to_vec(),
            cf: "default".to_string(),
        }),
        Modify::Put(Put {
            key: b"user:2".to_vec(),
            value: b"Bob".to_vec(),
            cf: "default".to_string(),
        }),
        Modify::Put(Put {
            key: b"user:3".to_vec(),
            value: b"Charlie".to_vec(),
            cf: "default".to_string(),
        }),
    ];

    storage.write(batch).await.unwrap();

    // 读取验证
    let reader = storage.reader().await.unwrap();
    assert_eq!(
        reader.get_cf("default", b"user:1").await.unwrap(),
        Some(b"Alice".to_vec())
    );
    assert_eq!(
        reader.get_cf("default", b"user:2").await.unwrap(),
        Some(b"Bob".to_vec())
    );

    // 更新值
    let update_batch = vec![Modify::Put(Put {
        key: b"user:1".to_vec(),
        value: b"Alice Updated".to_vec(),
        cf: "default".to_string(),
    })];
    storage.write(update_batch).await.unwrap();

    assert_eq!(
        reader.get_cf("default", b"user:1").await.unwrap(),
        Some(b"Alice Updated".to_vec())
    );

    // 删除
    let delete_batch = vec![Modify::Delete(Delete {
        key: b"user:2".to_vec(),
        cf: "default".to_string(),
    })];
    storage.write(delete_batch).await.unwrap();

    assert_eq!(reader.get_cf("default", b"user:2").await.unwrap(), None);

    storage.stop().await.unwrap();
}

#[tokio::test]
async fn test_integration_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap().to_string();

    // 第一次写入
    {
        let mut config = Config::new_test();
        config.db_path = db_path.clone();
        let storage = StandaloneStorage::new(&config).unwrap();
        storage.start().await.unwrap();

        let batch = vec![Modify::Put(Put {
            key: b"persistent_key".to_vec(),
            value: b"persistent_value".to_vec(),
            cf: "default".to_string(),
        })];
        storage.write(batch).await.unwrap();
        storage.stop().await.unwrap();
    }

    // 重新打开，验证数据持久化
    {
        let mut config = Config::new_test();
        config.db_path = db_path;
        let storage = StandaloneStorage::new(&config).unwrap();
        storage.start().await.unwrap();

        let reader = storage.reader().await.unwrap();
        assert_eq!(
            reader.get_cf("default", b"persistent_key").await.unwrap(),
            Some(b"persistent_value".to_vec())
        );

        storage.stop().await.unwrap();
    }
}

#[tokio::test]
async fn test_integration_multiple_column_families() {
    let (storage, _temp_dir) = setup_storage().await;
    storage.start().await.unwrap();

    let batch = vec![
        Modify::Put(Put {
            key: b"data".to_vec(),
            value: b"data_value".to_vec(),
            cf: "default".to_string(),
        }),
        Modify::Put(Put {
            key: b"data".to_vec(),
            value: b"write_value".to_vec(),
            cf: "write".to_string(),
        }),
        Modify::Put(Put {
            key: b"data".to_vec(),
            value: b"lock_value".to_vec(),
            cf: "lock".to_string(),
        }),
    ];

    storage.write(batch).await.unwrap();

    let reader = storage.reader().await.unwrap();
    assert_eq!(
        reader.get_cf("default", b"data").await.unwrap(),
        Some(b"data_value".to_vec())
    );
    assert_eq!(
        reader.get_cf("write", b"data").await.unwrap(),
        Some(b"write_value".to_vec())
    );
    assert_eq!(
        reader.get_cf("lock", b"data").await.unwrap(),
        Some(b"lock_value".to_vec())
    );

    storage.stop().await.unwrap();
}