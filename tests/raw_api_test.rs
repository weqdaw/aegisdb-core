use aegisdb::{Config, StandaloneStorage, Server, RawKvServer, Storage};
use aegisdb::proto::kvrpcpb::*;
use tempfile::TempDir;

async fn setup_server() -> (Server<StandaloneStorage>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::new_test();
    config.db_path = temp_dir.path().to_str().unwrap().to_string();
    let storage = StandaloneStorage::new(&config).unwrap();
    storage.start().await.unwrap();
    (Server::new(storage), temp_dir)
}

#[tokio::test]
async fn test_raw_get() {
    let (server, _temp_dir) = setup_server().await;
    
    // 先写入数据
    let put_req = RawPutRequest {
        context: Context::new(1),
        key: b"key1".to_vec(),
        value: b"value1".to_vec(),
        cf: "default".to_string(),
    };
    RawKvServer::raw_put(&server, put_req).await.unwrap();
    
    // 读取数据
    let get_req = RawGetRequest {
        context: Context::new(1),
        key: b"key1".to_vec(),
        cf: "default".to_string(),
    };
    let resp = RawKvServer::raw_get(&server, get_req).await.unwrap();
    
    assert!(!resp.not_found);
    assert_eq!(resp.value, Some(b"value1".to_vec()));
}

#[tokio::test]
async fn test_raw_get_not_found() {
    let (server, _temp_dir) = setup_server().await;
    
    let get_req = RawGetRequest {
        context: Context::new(1),
        key: b"nonexistent".to_vec(),
        cf: "default".to_string(),
    };
    let resp = RawKvServer::raw_get(&server, get_req).await.unwrap();
    
    assert!(resp.not_found);
    assert_eq!(resp.value, None);
}

#[tokio::test]
async fn test_raw_put() {
    let (server, _temp_dir) = setup_server().await;
    
    let put_req = RawPutRequest {
        context: Context::new(1),
        key: b"key1".to_vec(),
        value: b"value1".to_vec(),
        cf: "default".to_string(),
    };
    let resp = RawKvServer::raw_put(&server, put_req).await.unwrap();
    
    assert!(resp.region_error.is_none());
    assert!(resp.error.is_none());
    
    // 验证写入成功
    let get_req = RawGetRequest {
        context: Context::new(1),
        key: b"key1".to_vec(),
        cf: "default".to_string(),
    };
    let get_resp = RawKvServer::raw_get(&server, get_req).await.unwrap();
    assert_eq!(get_resp.value, Some(b"value1".to_vec()));
}

#[tokio::test]
async fn test_raw_delete() {
    let (server, _temp_dir) = setup_server().await;
    
    // 先写入
    let put_req = RawPutRequest {
        context: Context::new(1),
        key: b"key1".to_vec(),
        value: b"value1".to_vec(),
        cf: "default".to_string(),
    };
    RawKvServer::raw_put(&server, put_req).await.unwrap();
    
    // 删除
    let delete_req = RawDeleteRequest {
        context: Context::new(1),
        key: b"key1".to_vec(),
        cf: "default".to_string(),
    };
    let resp = RawKvServer::raw_delete(&server, delete_req).await.unwrap();
    assert!(resp.region_error.is_none());
    
    // 验证已删除
    let get_req = RawGetRequest {
        context: Context::new(1),
        key: b"key1".to_vec(),
        cf: "default".to_string(),
    };
    let get_resp = RawKvServer::raw_get(&server, get_req).await.unwrap();
    assert!(get_resp.not_found);
}

#[tokio::test]
async fn test_raw_scan() {
    let (server, _temp_dir) = setup_server().await;
    
    // 写入多个键值对
    for i in 1..=5 {
        let put_req = RawPutRequest {
            context: Context::new(1),
            key: vec![i],
            value: vec![233, i],
            cf: "default".to_string(),
        };
        RawKvServer::raw_put(&server, put_req).await.unwrap();
    }
    
    // 扫描
    let scan_req = RawScanRequest {
        context: Context::new(1),
        start_key: vec![1],
        limit: 3,
        cf: "default".to_string(),
    };
    let resp = RawKvServer::raw_scan(&server, scan_req).await.unwrap();
    
    assert_eq!(resp.kvs.len(), 3);
    assert_eq!(resp.kvs[0].key, vec![1]);
    assert_eq!(resp.kvs[0].value, vec![233, 1]);
    assert_eq!(resp.kvs[1].key, vec![2]);
    assert_eq!(resp.kvs[2].key, vec![3]);
}

#[tokio::test]
async fn test_raw_scan_after_delete() {
    let (server, _temp_dir) = setup_server().await;
    
    // 写入数据
    for i in 1..=4 {
        let put_req = RawPutRequest {
            context: Context::new(1),
            key: vec![i],
            value: vec![233, i],
            cf: "default".to_string(),
        };
        RawKvServer::raw_put(&server, put_req).await.unwrap();
    }
    
    // 删除 key 3
    let delete_req = RawDeleteRequest {
        context: Context::new(1),
        key: vec![3],
        cf: "default".to_string(),
    };
    RawKvServer::raw_delete(&server, delete_req).await.unwrap();
    
    // 扫描应该只返回 1, 2, 4
    let scan_req = RawScanRequest {
        context: Context::new(1),
        start_key: vec![1],
        limit: 10,
        cf: "default".to_string(),
    };
    let resp = RawKvServer::raw_scan(&server, scan_req).await.unwrap();
    
    assert_eq!(resp.kvs.len(), 3);
    assert_eq!(resp.kvs[0].key, vec![1]);
    assert_eq!(resp.kvs[1].key, vec![2]);
    assert_eq!(resp.kvs[2].key, vec![4]);
}