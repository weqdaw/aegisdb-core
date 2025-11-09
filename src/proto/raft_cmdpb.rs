// aegisdb/src/proto/raft_cmdpb.rs
// Raft 命令协议定义

use crate::proto::metapb::{Peer, Region, RegionEpoch};
use crate::proto::eraftpb::ConfChangeType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminCmdType {
    InvalidAdmin = 0,
    ChangePeer = 1,
    CompactLog = 3,
    TransferLeader = 4,
    Split = 10,
}

#[derive(Debug, Clone)]
pub struct ChangePeerRequest {
    pub change_type: ConfChangeType,
    pub peer: Option<Peer>,
}

#[derive(Debug, Clone)]
pub struct ChangePeerResponse {
    pub region: Option<Region>,
}

#[derive(Debug, Clone)]
pub struct TransferLeaderRequest {
    pub peer: Option<Peer>,
}

#[derive(Debug, Clone)]
pub struct TransferLeaderResponse {}

#[derive(Debug, Clone)]
pub struct SplitRequest {
    pub split_key: Vec<u8>,
    pub new_region_id: u64,
    pub new_peer_ids: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct SplitResponse {
    pub regions: Vec<Region>,
}

#[derive(Debug, Clone)]
pub struct CompactLogRequest {
    pub compact_index: u64,
    pub compact_term: u64,
}

#[derive(Debug, Clone)]
pub struct CompactLogResponse {}

#[derive(Debug, Clone)]
pub struct AdminRequest {
    pub cmd_type: i32, // AdminCmdType as i32
    pub change_peer: Option<ChangePeerRequest>,
    pub compact_log: Option<CompactLogRequest>,
    pub transfer_leader: Option<TransferLeaderRequest>,
    pub split: Option<SplitRequest>,
}

#[derive(Debug, Clone)]
pub struct AdminResponse {
    pub cmd_type: i32, // AdminCmdType as i32
    pub change_peer: Option<ChangePeerResponse>,
    pub compact_log: Option<CompactLogResponse>,
    pub transfer_leader: Option<TransferLeaderResponse>,
    pub split: Option<SplitResponse>,
}

#[derive(Debug, Clone)]
pub struct RaftRequestHeader {
    pub region_id: u64,
    pub peer: Option<Peer>,
    pub region_epoch: Option<RegionEpoch>,
    pub term: u64,
}

#[derive(Debug, Clone)]
pub struct RaftResponseHeader {
    pub error: Option<crate::proto::errorpb::Error>,
    pub uuid: Vec<u8>,
    pub current_term: u64,
}

#[derive(Debug, Clone)]
pub struct RaftCmdRequest {
    pub header: Option<RaftRequestHeader>,
    pub requests: Vec<Request>,
    pub admin_request: Option<AdminRequest>,
}

#[derive(Debug, Clone)]
pub struct RaftCmdResponse {
    pub header: Option<RaftResponseHeader>,
    pub responses: Vec<Response>,
    pub admin_response: Option<AdminResponse>,
}

// 简化的 Request 和 Response
#[derive(Debug, Clone)]
pub struct Request {
    pub cmd_type: i32,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Response {
    pub cmd_type: i32,
    pub data: Vec<u8>,
}

