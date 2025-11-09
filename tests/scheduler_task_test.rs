// aegisdb/tests/scheduler_task_test.rs
// 调度器任务处理测试

use aegisdb::raftstore::runner::*;
use aegisdb::raftstore::scheduler_client::{SchedulerClient, Client};
use aegisdb::raftstore::router::RaftRouter;
use aegisdb::raftstore::router::Router;
use aegisdb::proto::metapb::{Region, Peer, RegionEpoch};
use aegisdb::proto::schedulerpb::StoreStats;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_scheduler_task_handler_creation() {
    let router = Arc::new(Router::new());
    let raft_router = RaftRouter::new(router);
    
    let scheduler_client: Box<dyn SchedulerClient> = Box::new(
        Client::new(
            vec!["127.0.0.1:2379".to_string()],
            "test-store".to_string(),
        ).await.unwrap()
    );
    
    let handler = SchedulerTaskHandler::new(
        1,
        scheduler_client,
        raft_router,
    );
    
    handler.start();
}

#[tokio::test]
async fn test_scheduler_task_region_heartbeat() {
    let router = Arc::new(Router::new());
    let raft_router = RaftRouter::new(router);
    
    let scheduler_client: Box<dyn SchedulerClient> = Box::new(
        Client::new(
            vec!["127.0.0.1:2379".to_string()],
            "test-store".to_string(),
        ).await.unwrap()
    );
    
    // 等待集群 ID 初始化
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let handler = SchedulerTaskHandler::new(
        1,
        scheduler_client,
        raft_router,
    );
    
    handler.start();
    
    let region = Region {
        id: 1,
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![Peer { id: 1, store_id: 1 }],
    };
    
    let task = SchedulerTask::RegionHeartbeat(SchedulerRegionHeartbeatTask {
        region,
        peer: Peer { id: 1, store_id: 1 },
        pending_peers: vec![],
        approximate_size: Some(100),
    });
    
    let result = handler.handle(task).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_scheduler_task_store_heartbeat() {
    let router = Arc::new(Router::new());
    let raft_router = RaftRouter::new(router);
    
    let scheduler_client: Box<dyn SchedulerClient> = Box::new(
        Client::new(
            vec!["127.0.0.1:2379".to_string()],
            "test-store".to_string(),
        ).await.unwrap()
    );
    
    // 等待集群 ID 初始化
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let handler = SchedulerTaskHandler::new(
        1,
        scheduler_client,
        raft_router,
    );
    
    handler.start();
    
    let stats = StoreStats {
        store_id: 1,
        capacity: 1000,
        available: 500,
        used_size: 500,
        region_count: 10,
        leader_count: 5,
    };
    
    let task = SchedulerTask::StoreHeartbeat(SchedulerStoreHeartbeatTask {
        stats,
    });
    
    let result = handler.handle(task).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_scheduler_task_ask_split() {
    let router = Arc::new(Router::new());
    let raft_router = RaftRouter::new(router);
    
    let scheduler_client: Box<dyn SchedulerClient> = Box::new(
        Client::new(
            vec!["127.0.0.1:2379".to_string()],
            "test-store".to_string(),
        ).await.unwrap()
    );
    
    // 等待集群 ID 初始化
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let handler = SchedulerTaskHandler::new(
        1,
        scheduler_client,
        raft_router,
    );
    
    handler.start();
    
    let region = Region {
        id: 1,
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
        region_epoch: RegionEpoch::new(1, 1),
        peers: vec![Peer { id: 1, store_id: 1 }],
    };
    
    let task = SchedulerTask::AskSplit(SchedulerAskSplitTask {
        region,
        split_key: b"m".to_vec(),
        peer: Peer { id: 1, store_id: 1 },
        callback: None,
    });
    
    let result = handler.handle(task).await;
    assert!(result.is_ok());
}

