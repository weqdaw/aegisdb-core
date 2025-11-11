use crate::pd::config::PdConfig;
use crate::pd::id_allocator::IdAllocator;
use crate::proto::metapb::{Peer, Region, Store};
use crate::proto::schedulerpb::{
    self, AskSplitResponse, GetMembersResponse, GetRegionResponse, GetStoreResponse,
    Member, RegionHeartbeatRequest, RequestHeader, ResponseHeader, StoreHeartbeatRequest,
    StoreHeartbeatResponse,
};
use crate::raftstore::store_balancer::StoreLoad;
use crate::scheduler::cluster::{BasicCluster, RegionInfo, StoreInfo};
use crate::scheduler::coordinator::Coordinator;
use crate::scheduler::heartbeat_streams::SimpleHeartbeatStreams;
use crate::scheduler::operator_controller::OperatorController;
use anyhow::{anyhow, Context, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct PdState {
    config: PdConfig,
    cluster: Arc<BasicCluster>,
    heartbeat_streams: Arc<SimpleHeartbeatStreams>,
    operator_controller: Arc<OperatorController>,
    coordinator: Arc<Coordinator>,
    id_allocator: Arc<IdAllocator>,
    is_bootstrapped: AtomicBool,
    store_loads: Arc<RwLock<HashMap<u64, StoreLoad>>>,
}

impl PdState {
    pub fn new(config: PdConfig) -> Self {
        let cluster = Arc::new(BasicCluster::new());
        let heartbeat_streams = Arc::new(SimpleHeartbeatStreams::new());
        let operator_controller =
            Arc::new(OperatorController::new(cluster.clone(), heartbeat_streams.clone()));
        let coordinator = Arc::new(Coordinator::new(
            cluster.clone(),
            operator_controller.clone(),
        ));

        Self {
            config,
            cluster,
            heartbeat_streams,
            operator_controller,
            coordinator,
            id_allocator: Arc::new(IdAllocator::new(1000)),
            is_bootstrapped: AtomicBool::new(false),
            store_loads: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start(&self) {
        self.coordinator.start().await;
    }

    pub fn cluster(&self) -> Arc<BasicCluster> {
        self.cluster.clone()
    }

    pub fn coordinator(&self) -> Arc<Coordinator> {
        self.coordinator.clone()
    }

    pub fn operator_controller(&self) -> Arc<OperatorController> {
        self.operator_controller.clone()
    }

    pub fn cluster_id(&self) -> u64 {
        self.config.cluster_id
    }

    pub fn store_loads_snapshot(&self) -> Vec<StoreLoad> {
        self.store_loads.read().values().cloned().collect()
    }

    pub fn bind_region_stream(
        &self,
        store_id: u64,
        sender: tokio::sync::mpsc::UnboundedSender<schedulerpb::RegionHeartbeatResponse>,
    ) {
        self.heartbeat_streams.bind_stream(store_id, sender);
    }

    pub fn unbind_region_stream(&self, store_id: u64) {
        self.heartbeat_streams.unbind_stream(store_id);
    }

    pub fn bootstrap(&self, store: Store) -> Result<ResponseHeader> {
        if !self.is_bootstrapped.swap(true, Ordering::SeqCst) {
            self.cluster.put_store(StoreInfo::new(store));
        }
        Ok(self.response_ok())
    }

    pub fn is_bootstrapped(&self) -> bool {
        self.is_bootstrapped.load(Ordering::SeqCst)
    }

    pub fn put_store(&self, store: Store) -> Result<ResponseHeader> {
        self.cluster.put_store(StoreInfo::new(store));
        Ok(self.response_ok())
    }

    pub fn get_store(&self, store_id: u64) -> Result<GetStoreResponse> {
        let store = self
            .cluster
            .get_store(store_id)
            .map(|s| s.meta().clone())
            .ok_or_else(|| anyhow!("store {} not found", store_id))?;

        Ok(GetStoreResponse {
            header: Some(self.response_ok()),
            store: Some(to_proto_store(&store)),
        })
    }

    pub fn handle_store_heartbeat(&self, request: StoreHeartbeatRequest) -> Result<StoreHeartbeatResponse> {
        let stats = request
            .stats
            .ok_or_else(|| anyhow!("store heartbeat missing stats"))?;

        if let Some(mut info) = self.cluster.get_store(stats.store_id) {
            info.update_heartbeat();
            info.update_region_count(stats.region_count as usize);
            info.update_leader_count(stats.leader_count as usize);
            info.update_region_size(stats.used_size);
            info.update_leader_size(stats.leader_count); // 简化：无 leader size 统计
            // extended metrics
            info.update_healthy(stats.healthy);
            info.update_avg_resp_ms(stats.avg_resp_ms);
            info.update_error_count(stats.error_count);
            info.update_mem(stats.mem_total, stats.mem_used);
            info.update_disk(stats.disk_total, stats.disk_used);
            info.update_network_state(stats.network_state.clone());
            self.cluster.put_store(info);
        }

        {
            let mut loads = self.store_loads.write();
            let load = loads
                .entry(stats.store_id)
                .or_insert_with(|| StoreLoad::new(stats.store_id));
            load.update(
                stats.region_count as usize,
                stats.leader_count as usize,
                stats.used_size,
                stats.leader_count,
            );
        }

        Ok(StoreHeartbeatResponse {
            header: Some(self.response_ok()),
        })
    }

    pub fn handle_region_heartbeat(
        &self,
        request: RegionHeartbeatRequest,
    ) -> Result<()> {
        let header = request
            .header
            .ok_or_else(|| anyhow!("region heartbeat missing header"))?;
        let store_id = header.sender_id;

        let region_proto = request
            .region
            .ok_or_else(|| anyhow!("region heartbeat missing region"))?;
        let region = from_proto_region(region_proto.clone());

        let leader_proto = request
            .leader
            .ok_or_else(|| anyhow!("region heartbeat missing leader"))?;
        let leader = from_proto_peer(leader_proto.clone());

        let mut region_info = RegionInfo::new(region.clone(), Some(leader.clone()));

        for pending in request.pending_peers {
            region_info.add_pending_peer(from_proto_peer(pending));
        }

        region_info.set_approximate_size(request.approximate_size);

        self.cluster.put_region(region_info.clone());
        self.coordinator.dispatch(&region_info, "heartbeat");

        // 绑定 store -> heartbeat 流（首次心跳时）
        if !self.heartbeat_streams.has_stream(store_id) {
            return Err(anyhow!(
                "heartbeat stream for store {} missing; PD should have bound before processing",
                store_id
            ));
        }

        Ok(())
    }

    pub fn ask_split(&self, request: schedulerpb::AskSplitRequest) -> Result<AskSplitResponse> {
        let _region = request
            .region
            .ok_or_else(|| anyhow!("ask split missing region"))?;
        let new_region_id = self.id_allocator.alloc();
        let new_peer_id = self.id_allocator.alloc();

        Ok(AskSplitResponse {
            header: Some(self.response_ok()),
            new_region_id,
            new_peer_ids: vec![new_peer_id],
        })
    }

    pub fn get_region(&self, key: Vec<u8>) -> Result<GetRegionResponse> {
        let region = self
            .cluster
            .get_regions()
            .into_iter()
            .find(|r| {
                let start = r.start_key();
                let end = r.end_key();
                (start.is_empty() || key >= start.to_vec())
                    && (end.is_empty() || key < end.to_vec())
            })
            .ok_or_else(|| anyhow!("region not found for key"))?;

        let leader = region.leader().cloned();

        Ok(GetRegionResponse {
            header: Some(self.response_ok()),
            region: Some(to_proto_region(region.region().clone())),
            leader: leader.map(|p| to_proto_peer(p)),
        })
    }

    pub fn get_region_by_id(&self, region_id: u64) -> Result<GetRegionResponse> {
        let region = self
            .cluster
            .get_region(region_id)
            .ok_or_else(|| anyhow!("region {} not found", region_id))?;

        Ok(GetRegionResponse {
            header: Some(self.response_ok()),
            region: Some(to_proto_region(region.region().clone())),
            leader: region.leader().cloned().map(to_proto_peer),
        })
    }

    pub fn get_members(&self) -> GetMembersResponse {
        let header = self.response_ok();
        let leader = Member {
            member_id: self.config.cluster_id,
            client_urls: self.config.advertise_client_urls.clone(),
            peer_urls: self.config.advertise_peer_urls.clone(),
        };

        GetMembersResponse {
            header: Some(header),
            members: vec![leader.clone()],
            leader: Some(leader),
        }
    }

    pub fn response_ok(&self) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id(),
            error: None,
        }
    }

    pub fn id_allocator(&self) -> Arc<IdAllocator> {
        self.id_allocator.clone()
    }
}

pub fn from_proto_store(store: crate::proto::metapb::Store) -> Store {
    Store {
        id: store.id,
        address: store.address,
        state: store.state,
    }
}

fn to_proto_store(store: &Store) -> crate::proto::metapb::Store {
    crate::proto::metapb::Store {
        id: store.id,
        address: store.address.clone(),
        state: store.state,
    }
}

fn to_proto_peer(peer: Peer) -> crate::proto::metapb::Peer {
    crate::proto::metapb::Peer {
        id: peer.id,
        store_id: peer.store_id,
    }
}

fn to_proto_region(region: Region) -> crate::proto::metapb::Region {
    crate::proto::metapb::Region {
        id: region.id,
        start_key: region.start_key,
        end_key: region.end_key,
        region_epoch: region.region_epoch,
        peers: region.peers.into_iter().map(to_proto_peer).collect(),
    }
}

fn from_proto_peer(peer: crate::proto::metapb::Peer) -> Peer {
    Peer {
        id: peer.id,
        store_id: peer.store_id,
    }
}

fn from_proto_region(region: crate::proto::metapb::Region) -> Region {
    Region {
        id: region.id,
        start_key: region.start_key,
        end_key: region.end_key,
        region_epoch: region.region_epoch,
        peers: region.peers.into_iter().map(from_proto_peer).collect(),
    }
}