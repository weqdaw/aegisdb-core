// aegisdb/src/proto/metapb.rs
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RegionEpoch {
    pub conf_ver: u64,  // 配置变更版本
    pub version: u64,    // 区域版本
}

impl RegionEpoch {
    pub fn new(conf_ver: u64, version: u64) -> Self {
        Self { conf_ver, version }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Peer {
    pub id: u64,
    pub store_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Region {
    pub id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub region_epoch: RegionEpoch,
    pub peers: Vec<Peer>,
}

#[derive(Debug,Clone,Copy,PartialEq,Eq,serde::Serialize,serde::Deserialize)]
pub enum StoreState {
    Up = 0,
    Offline = 1,
    Tombstone = 2,
}

#[derive(Debug,Clone,PartialEq,Eq,serde::Serialize,serde::Deserialize)]
pub struct Store {
    pub id : u64,
    pub address : String,
    pub state : StoreState,
}