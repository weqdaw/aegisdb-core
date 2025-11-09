// aegisdb/tests/scheduler_client_test.rs
// 调度器客户端测试

use aegisdb::raftstore::scheduler_client::{SchedulerClient, Client};
use aegisdb::proto::metapb::{Region, Peer, RegionEpoch, Store, StoreState};
use aegisdb::proto::schedulerpb::*;
use std::time::Duration;

#[tokio::test]
async fn test_scheduler_client_creation() {
    let client = Client::new(
        vec!["127.0.0.1:2379".to_string()],
        "test-store".to_string(),
    ).await;
    
    assert!(client.is_ok());
    let client = client.unwrap();
    assert_eq!(client.get_cluster_id(), 0); // 初始为 0，异步更新
}

#[tokio::test]
async fn test_scheduler_client_alloc_id() {
    let client = Client::new(
        vec!["127.0.0.1:2379".to_string()],
        "test-store".to_string(),
    ).await.unwrap();
    
    // 等待集群 ID 初始化
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let id = client.alloc_id().await;
    assert!(id.is_ok());
    let id = id.unwrap();
    assert!(id > 0);
}

#[tokio::test]
async fn test_scheduler_client_bootstrap() {
    let client = Client::new(
        vec!["127.0.0.1:2379".to_string()],
        "test-store".to_string(),
    ).await.unwrap();
    
    // 等待集群 ID 初始化
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let store = Store {
        id: 1,
        address: "127.0.0.1:20160".to_string(),
        state: StoreState::Up,
    };
    
    let resp = client.bootstrap(&store).await;
    assert!(resp.is_ok());
}

#[tokio::test]
async fn test_scheduler_client_region_heartbeat() {
    let client = Client::new(
        vec!["127.0.0.1:2379".to_string()],
        "test-store".to_string(),
    ).await.unwrap();
    
    // 等待集群 ID 初始化
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let region = Region {
        id: 1,
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![Peer { id: 1, store_id: 1 }],
    };
    
    let request = RegionHeartbeatRequest {
        header: Some(client.request_header()),
        region: Some(region),
        leader: Some(Peer { id: 1, store_id: 1 }),
        pending_peers: vec![],
        approximate_size: 100,
    };
    
    let result = client.region_heartbeat(request);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_scheduler_client_store_heartbeat() {
    let client = Client::new(
        vec!["127.0.0.1:2379".to_string()],
        "test-store".to_string(),
    ).await.unwrap();
    
    // 等待集群 ID 初始化
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let stats = StoreStats {
        store_id: 1,
        capacity: 1000,
        available: 500,
        used_size: 500,
        region_count: 10,
        leader_count: 5,
    };
    
    let result = client.store_heartbeat(&stats).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_scheduler_client_ask_split() {
    let client = Client::new(
        vec!["127.0.0.1:2379".to_string()],
        "test-store".to_string(),
    ).await.unwrap();
    
    // 等待集群 ID 初始化
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let region = Region {
        id: 1,
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![Peer { id: 1, store_id: 1 }],
    };
    
    let resp = client.ask_split(&region).await;
    assert!(resp.is_ok());
    let resp = resp.unwrap();
    assert!(resp.new_region_id > 0);
    assert!(!resp.new_peer_ids.is_empty());
}

