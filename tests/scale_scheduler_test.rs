use aegisdb::scheduler::*;
use aegisdb::scheduler::schedulers::{Scheduler, ScaleScheduler};
use aegisdb::scheduler::operator_controller::OperatorController;
use aegisdb::scheduler::heartbeat_streams::SimpleHeartbeatStreams;
use aegisdb::proto::metapb::{Region, Peer, RegionEpoch};
use std::time::Duration;
use std::sync::Arc;

#[test]
fn test_scale_scheduler_scale_up() {
    // 创建集群
    let cluster = Arc::new(BasicCluster::new());

    // 创建 5 个 Store
    for i in 1..=5 {
        let store = StoreInfo::new(aegisdb::proto::metapb::Store {
            id: i,
            address: format!("127.0.0.1:{}", 20160 + i),
            state: aegisdb::proto::metapb::StoreState::Up,
        });
        cluster.put_store(store);
    }

    // 创建一个只有 1 个 Peer 的 Region（需要扩容）
    let region = RegionInfo::new(
        Region {
            id: 1,
            start_key: b"key1".to_vec(),
            end_key: b"key2".to_vec(),
            region_epoch: RegionEpoch::new(1, 1),
            peers: vec![Peer { id: 1, store_id: 1 }],
        },
        Some(Peer { id: 1, store_id: 1 }),
    );
    cluster.put_region(region);

    // 创建调度器（最小 3 个 Peer，最大 5 个 Peer）
    let scheduler = ScaleScheduler::with_peer_count(Duration::from_secs(30), 3, 5);

    // 调度
    let op = scheduler.schedule(&cluster);
    assert!(op.is_some(), "应该创建扩容操作");

    let op = op.unwrap();
    assert_eq!(op.region_id, 1);
    assert_eq!(op.kind, OpKind::Region);
    assert_eq!(op.steps.len(), 1);
}

#[test]
fn test_scale_scheduler_scale_down() {
    // 创建集群
    let cluster = Arc::new(BasicCluster::new());

    // 创建 5 个 Store
    for i in 1..=5 {
        let store = StoreInfo::new(aegisdb::proto::metapb::Store {
            id: i,
            address: format!("127.0.0.1:{}", 20160 + i),
            state: aegisdb::proto::metapb::StoreState::Up,
        });
        cluster.put_store(store);
    }

    // 创建一个有 6 个 Peer 的 Region（需要缩容）
    let region = RegionInfo::new(
        Region {
            id: 1,
            start_key: b"key1".to_vec(),
            end_key: b"key2".to_vec(),
            region_epoch: RegionEpoch::new(1, 1),
            peers: vec![
                Peer { id: 1, store_id: 1 },
                Peer { id: 2, store_id: 2 },
                Peer { id: 3, store_id: 3 },
                Peer { id: 4, store_id: 4 },
                Peer { id: 5, store_id: 5 },
                Peer { id: 6, store_id: 1 }, // 重复的 store_id
            ],
        },
        Some(Peer { id: 1, store_id: 1 }), // Leader 在 store 1
    );
    cluster.put_region(region);

    // 创建调度器（最小 3 个 Peer，最大 5 个 Peer）
    let scheduler = ScaleScheduler::with_peer_count(Duration::from_secs(30), 3, 5);

    // 调度
    let op = scheduler.schedule(&cluster);
    assert!(op.is_some(), "应该创建缩容操作");

    let op = op.unwrap();
    assert_eq!(op.region_id, 1);
    assert_eq!(op.kind, OpKind::Region);
    assert_eq!(op.steps.len(), 1);
}

#[test]
fn test_scale_scheduler_no_scale_needed() {
    // 创建集群
    let cluster = Arc::new(BasicCluster::new());

    // 创建 5 个 Store
    for i in 1..=5 {
        let store = StoreInfo::new(aegisdb::proto::metapb::Store {
            id: i,
            address: format!("127.0.0.1:{}", 20160 + i),
            state: aegisdb::proto::metapb::StoreState::Up,
        });
        cluster.put_store(store);
    }

    // 创建一个有 3 个 Peer 的 Region（不需要扩容/缩容）
    let region = RegionInfo::new(
        Region {
            id: 1,
            start_key: b"key1".to_vec(),
            end_key: b"key2".to_vec(),
            region_epoch: RegionEpoch::new(1, 1),
            peers: vec![
                Peer { id: 1, store_id: 1 },
                Peer { id: 2, store_id: 2 },
                Peer { id: 3, store_id: 3 },
            ],
        },
        Some(Peer { id: 1, store_id: 1 }),
    );
    cluster.put_region(region);

    // 创建调度器（最小 3 个 Peer，最大 5 个 Peer）
    let scheduler = ScaleScheduler::with_peer_count(Duration::from_secs(30), 3, 5);

    // 调度
    let op = scheduler.schedule(&cluster);
    assert!(op.is_none(), "不应该创建操作");
}

#[test]
fn test_scale_scheduler_scale_up_flow() {
    // 创建集群
    let cluster = Arc::new(BasicCluster::new());

    // 创建 5 个 Store
    for i in 1..=5 {
        let store = StoreInfo::new(aegisdb::proto::metapb::Store {
            id: i,
            address: format!("127.0.0.1:{}", 20160 + i),
            state: aegisdb::proto::metapb::StoreState::Up,
        });
        cluster.put_store(store);
    }

    // 创建一个只有 2 个 Peer 的 Region（需要扩容到 3 个）
    let mut region = RegionInfo::new(
        Region {
            id: 1,
            start_key: b"key1".to_vec(),
            end_key: b"key2".to_vec(),
            region_epoch: RegionEpoch::new(1, 1),
            peers: vec![
                Peer { id: 1, store_id: 1 },
                Peer { id: 2, store_id: 2 },
            ],
        },
        Some(Peer { id: 1, store_id: 1 }),
    );
    cluster.put_region(region.clone());

    // 创建调度器
    let scheduler = ScaleScheduler::with_peer_count(Duration::from_secs(30), 3, 5);

    // 创建 OperatorController
    let hb_streams = Arc::new(SimpleHeartbeatStreams::new());
    let op_controller = Arc::new(OperatorController::new(cluster.clone(), hb_streams.clone()));

    // 调度并添加操作
    if let Some(op) = scheduler.schedule(&cluster) {
        assert!(op_controller.add_operator(vec![op]), "应该成功添加操作");

        // 模拟添加 Peer 成功
        let mut new_peers = region.region().peers.clone();
        new_peers.push(Peer { id: 3, store_id: 3 });
        
        let updated_region = RegionInfo::new(
            Region {
                id: 1,
                start_key: region.start_key().to_vec(),
                end_key: region.end_key().to_vec(),
                region_epoch: RegionEpoch {
                    version: 1,
                    conf_ver: 2, // conf_ver 增加
                },
                peers: new_peers,
            },
            Some(Peer { id: 1, store_id: 1 }),
        );
        cluster.put_region(updated_region.clone());

        // 分发操作（模拟心跳）
        op_controller.dispatch(&updated_region, "heartbeat");

        // 检查操作是否完成
        let op = op_controller.get_operator(1);
        assert!(op.is_none(), "操作应该已完成");
    } else {
        panic!("应该创建扩容操作");
    }
}

#[test]
fn test_scale_scheduler_scale_down_flow() {
    // 创建集群
    let cluster = Arc::new(BasicCluster::new());

    // 创建 5 个 Store
    for i in 1..=5 {
        let store = StoreInfo::new(aegisdb::proto::metapb::Store {
            id: i,
            address: format!("127.0.0.1:{}", 20160 + i),
            state: aegisdb::proto::metapb::StoreState::Up,
        });
        cluster.put_store(store);
    }

    // 创建一个有 6 个 Peer 的 Region（需要缩容到 5 个）
    let mut region = RegionInfo::new(
        Region {
            id: 1,
            start_key: b"key1".to_vec(),
            end_key: b"key2".to_vec(),
            region_epoch: RegionEpoch::new(1, 1),
            peers: vec![
                Peer { id: 1, store_id: 1 },
                Peer { id: 2, store_id: 2 },
                Peer { id: 3, store_id: 3 },
                Peer { id: 4, store_id: 4 },
                Peer { id: 5, store_id: 5 },
                Peer { id: 6, store_id: 1 }, // 重复的 store_id
            ],
        },
        Some(Peer { id: 1, store_id: 1 }), // Leader 在 store 1
    );
    cluster.put_region(region.clone());

    // 创建调度器
    let scheduler = ScaleScheduler::with_peer_count(Duration::from_secs(30), 3, 5);

    // 创建 OperatorController
    let hb_streams = Arc::new(SimpleHeartbeatStreams::new());
    let op_controller = Arc::new(OperatorController::new(cluster.clone(), hb_streams.clone()));

    // 调度并添加操作
    if let Some(op) = scheduler.schedule(&cluster) {
        assert!(op_controller.add_operator(vec![op]), "应该成功添加操作");

        // 模拟移除 Peer 成功（移除 store 1 上的非 Leader Peer）
        let mut new_peers: Vec<Peer> = region.region().peers
            .iter()
            .filter(|p| p.id != 6) // 移除 peer 6
            .cloned()
            .collect();
        
        let updated_region = RegionInfo::new(
            Region {
                id: 1,
                start_key: region.start_key().to_vec(),
                end_key: region.end_key().to_vec(),
                region_epoch: RegionEpoch {
                    version: 1,
                    conf_ver: 2, // conf_ver 增加
                },
                peers: new_peers,
            },
            Some(Peer { id: 1, store_id: 1 }),
        );
        cluster.put_region(updated_region.clone());

        // 分发操作（模拟心跳）
        op_controller.dispatch(&updated_region, "heartbeat");

        // 检查操作是否完成
        let op = op_controller.get_operator(1);
        assert!(op.is_none(), "操作应该已完成");
    } else {
        panic!("应该创建缩容操作");
    }
}

