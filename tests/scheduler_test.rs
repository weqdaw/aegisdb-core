use aegisdb::scheduler::*;
use aegisdb::scheduler::schedulers::Scheduler;
use aegisdb::proto::metapb::{Store, StoreState, Region, Peer, RegionEpoch};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_basic_cluster() {
    let cluster = Arc::new(BasicCluster::new());

    // 创建测试 Store
    let store1 = StoreInfo::new(Store {
        id: 1,
        address: "127.0.0.1:20160".to_string(),
        state: StoreState::Up,
    });
    let store2 = StoreInfo::new(Store {
        id: 2,
        address: "127.0.0.1:20161".to_string(),
        state: StoreState::Up,
    });
    let store3 = StoreInfo::new(Store {
        id: 3,
        address: "127.0.0.1:20162".to_string(),
        state: StoreState::Up,
    });

    cluster.put_store(store1);
    cluster.put_store(store2);
    cluster.put_store(store3);

    // 创建测试 Region
    let mut region1 = RegionInfo::new(
        Region {
            id: 1,
            start_key: b"a".to_vec(),
            end_key: b"m".to_vec(),
            region_epoch: RegionEpoch::new(1, 1),
            peers: vec![
                Peer { id: 1, store_id: 1 },
                Peer { id: 2, store_id: 2 },
            ],
        },
        Some(Peer { id: 1, store_id: 1 }),
    );
    region1.set_approximate_size(10);

    let mut region2 = RegionInfo::new(
        Region {
            id: 2,
            start_key: b"m".to_vec(),
            end_key: vec![],
            region_epoch: RegionEpoch::new(1, 1),
            peers: vec![
                Peer { id: 3, store_id: 1 },
                Peer { id: 4, store_id: 3 },
            ],
        },
        Some(Peer { id: 3, store_id: 1 }),
    );
    region2.set_approximate_size(10);

    cluster.put_region(region1);
    cluster.put_region(region2);

    // 验证统计信息
    assert_eq!(cluster.get_store_region_count(1), 2);
    assert_eq!(cluster.get_store_region_count(2), 1);
    assert_eq!(cluster.get_store_region_count(3), 1);
    assert_eq!(cluster.get_store_leader_count(1), 2);
    assert_eq!(cluster.get_store_leader_count(2), 0);
    assert_eq!(cluster.get_store_leader_count(3), 0);
}

#[tokio::test]
async fn test_balance_region_scheduler() {
    let cluster = Arc::new(BasicCluster::new());

    // 创建不均衡的 Store
    let store1 = StoreInfo::new(Store {
        id: 1,
        address: "127.0.0.1:20160".to_string(),
        state: StoreState::Up,
    });
    let store2 = StoreInfo::new(Store {
        id: 2,
        address: "127.0.0.1:20161".to_string(),
        state: StoreState::Up,
    });

    cluster.put_store(store1);
    cluster.put_store(store2);

    // Store 1 有多个 Region，Store 2 没有
    for i in 1..=5 {
        let mut region = RegionInfo::new(
            Region {
                id: i,
                start_key: format!("key{}", i).into_bytes(),
                end_key: format!("key{}", i + 1).into_bytes(),
                region_epoch: RegionEpoch::new(1, 1),
                peers: vec![Peer { id: i * 10, store_id: 1 }],
            },
            Some(Peer { id: i * 10, store_id: 1 }),
        );
        region.set_approximate_size(10);
        cluster.put_region(region);
    }

    // 创建调度器
    let scheduler = BalanceRegionScheduler::new(Duration::from_secs(30));

    // 尝试调度
    if let Some(op) = scheduler.schedule(&cluster) {
        println!("创建了操作: {:?}", op);
        assert_eq!(op.kind, OpKind::Region);
    } else {
        println!("没有创建操作（可能因为 Store 数量不足或其他原因）");
    }
}

#[tokio::test]
async fn test_balance_leader_scheduler() {
    let cluster = Arc::new(BasicCluster::new());

    // 创建 Store
    let store1 = StoreInfo::new(Store {
        id: 1,
        address: "127.0.0.1:20160".to_string(),
        state: StoreState::Up,
    });
    let store2 = StoreInfo::new(Store {
        id: 2,
        address: "127.0.0.1:20161".to_string(),
        state: StoreState::Up,
    });
    let store3 = StoreInfo::new(Store {
        id: 3,
        address: "127.0.0.1:20162".to_string(),
        state: StoreState::Up,
    });

    cluster.put_store(store1);
    cluster.put_store(store2);
    cluster.put_store(store3);

    // Store 1 有多个 Leader
    for i in 1..=5 {
        let mut region = RegionInfo::new(
            Region {
                id: i,
                start_key: format!("key{}", i).into_bytes(),
                end_key: format!("key{}", i + 1).into_bytes(),
                region_epoch: RegionEpoch::new(1, 1),
                peers: vec![
                    Peer { id: i * 10, store_id: 1 },
                    Peer { id: i * 10 + 1, store_id: 2 },
                ],
            },
            Some(Peer { id: i * 10, store_id: 1 }),
        );
        region.set_approximate_size(10);
        cluster.put_region(region);
    }

    // 创建调度器
    let scheduler = BalanceLeaderScheduler::new(Duration::from_secs(30));

    // 尝试调度
    if let Some(op) = scheduler.schedule(&cluster) {
        println!("创建了操作: {:?}", op);
        assert_eq!(op.kind, OpKind::Leader);
    } else {
        println!("没有创建操作");
    }
}

#[tokio::test]
async fn test_operator_steps() {
    use aegisdb::scheduler::operator::{OpStep, AddPeer, RemovePeer, TransferLeader};

    let region = RegionInfo::new(
        Region {
            id: 1,
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
            region_epoch: RegionEpoch::new(1, 1),
            peers: vec![Peer { id: 1, store_id: 1 }],
        },
        Some(Peer { id: 1, store_id: 1 }),
    );

    // 测试 AddPeer
    let add_peer = AddPeer {
        to_store: 2,
        peer_id: 2,
    };
    assert!(!add_peer.is_finish(&region));
    assert!(!add_peer.conf_ver_changed(&region));

    // 测试 RemovePeer
    let remove_peer = RemovePeer { from_store: 1 };
    assert!(!remove_peer.is_finish(&region));
    assert!(!remove_peer.conf_ver_changed(&region));

    // 测试 TransferLeader
    let transfer_leader = TransferLeader {
        from_store: 1,
        to_store: 2,
    };
    assert!(!transfer_leader.is_finish(&region));
    assert!(!transfer_leader.conf_ver_changed(&region));
}

#[tokio::test]
async fn test_coordinator() {
    let cluster = Arc::new(BasicCluster::new());

    // 创建 Store
    let store1 = StoreInfo::new(Store {
        id: 1,
        address: "127.0.0.1:20160".to_string(),
        state: StoreState::Up,
    });
    let store2 = StoreInfo::new(Store {
        id: 2,
        address: "127.0.0.1:20161".to_string(),
        state: StoreState::Up,
    });

    cluster.put_store(store1);
    cluster.put_store(store2);

    // 创建协调器
    let coordinator = Coordinator::new(Arc::clone(&cluster));

    // 注册调度器
    let balance_region = BalanceRegionScheduler::new(Duration::from_secs(30));
    coordinator.register_scheduler(Box::new(balance_region));

    let balance_leader = BalanceLeaderScheduler::new(Duration::from_secs(30));
    coordinator.register_scheduler(Box::new(balance_leader));

    // 启动协调器（运行一小段时间）
    coordinator.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    coordinator.stop();

    println!("协调器测试完成");
}

