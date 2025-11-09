use crate::proto::metapb::{Peer, RegionEpoch, Region, Store};

#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub cluster_id: u64,
}

#[derive(Debug, Clone)]
pub struct ResponseHeader {
    pub cluster_id: u64,
    pub error: Option<crate::proto::errorpb::Error>,
}

#[derive(Debug, Clone)]
pub struct ChangePeer {
    pub peer: Option<Peer>,
    pub change_type: i32, // ConfChangeType as i32
}

#[derive(Debug, Clone)]
pub struct TransferLeader {
    pub peer: Option<Peer>,
}

#[derive(Debug, Clone)]
pub struct RegionHeartbeatRequest {
    pub header: Option<RequestHeader>,
    pub region: Option<Region>,
    pub leader: Option<Peer>,
    pub pending_peers: Vec<Peer>,
    pub approximate_size: u64,
}

#[derive(Debug, Clone)]
pub struct RegionHeartbeatResponse {
    pub header: Option<ResponseHeader>,
    pub change_peer: Option<ChangePeer>,
    pub transfer_leader: Option<TransferLeader>,
    pub region_id: u64,
    pub region_epoch: Option<RegionEpoch>,
    pub target_peer: Option<Peer>,
}

#[derive(Debug, Clone)]
pub struct StoreStats {
    pub store_id: u64,
    pub capacity: u64,
    pub available: u64,
    pub used_size: u64,
    pub region_count: u64,
    pub leader_count: u64,
}

#[derive(Debug, Clone)]
pub struct StoreHeartbeatRequest {
    pub header: Option<RequestHeader>,
    pub stats: Option<StoreStats>,
}

#[derive(Debug, Clone)]
pub struct StoreHeartbeatResponse {
    pub header: Option<ResponseHeader>,
}

#[derive(Debug, Clone)]
pub struct AskSplitRequest {
    pub header: Option<RequestHeader>,
    pub region: Option<Region>,
}

#[derive(Debug, Clone)]
pub struct AskSplitResponse {
    pub header: Option<ResponseHeader>,
    pub new_region_id: u64,
    pub new_peer_ids: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct AllocIDRequest {
    pub header: Option<RequestHeader>,
}

#[derive(Debug, Clone)]
pub struct AllocIDResponse {
    pub header: Option<ResponseHeader>,
    pub id: u64,
}

#[derive(Debug, Clone)]
pub struct BootstrapRequest {
    pub header: Option<RequestHeader>,
    pub store: Option<Store>,
}

#[derive(Debug, Clone)]
pub struct BootstrapResponse {
    pub header: Option<ResponseHeader>,
}

#[derive(Debug, Clone)]
pub struct IsBootstrappedRequest {
    pub header: Option<RequestHeader>,
}

#[derive(Debug, Clone)]
pub struct IsBootstrappedResponse {
    pub header: Option<ResponseHeader>,
    pub bootstrapped: bool,
}

#[derive(Debug, Clone)]
pub struct PutStoreRequest {
    pub header: Option<RequestHeader>,
    pub store: Option<Store>,
}

#[derive(Debug, Clone)]
pub struct PutStoreResponse {
    pub header: Option<ResponseHeader>,
}

#[derive(Debug, Clone)]
pub struct GetStoreRequest {
    pub header: Option<RequestHeader>,
    pub store_id: u64,
}

#[derive(Debug, Clone)]
pub struct GetStoreResponse {
    pub header: Option<ResponseHeader>,
    pub store: Option<Store>,
}

#[derive(Debug, Clone)]
pub struct GetRegionRequest {
    pub header: Option<RequestHeader>,
    pub region_key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct GetRegionResponse {
    pub header: Option<ResponseHeader>,
    pub region: Option<Region>,
    pub leader: Option<Peer>,
}

#[derive(Debug, Clone)]
pub struct GetRegionByIDRequest {
    pub header: Option<RequestHeader>,
    pub region_id: u64,
}

#[derive(Debug, Clone)]
pub struct GetMembersRequest {}

#[derive(Debug, Clone)]
pub struct Member {
    pub member_id: u64,
    pub client_urls: Vec<String>,
    pub peer_urls: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct GetMembersResponse {
    pub header: Option<ResponseHeader>,
    pub members: Vec<Member>,
    pub leader: Option<Member>,
}