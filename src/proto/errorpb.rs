use crate::proto::metapb::Region;

#[derive(Debug, Clone)]
pub enum Error {
    NotLeader {
        region_id: u64,
        leader: Option<Region>,
    },
    RegionNotFound {
        region_id: u64,
    },
    KeyNotInRegion {
        key: Vec<u8>,
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
    EpochNotMatch {
        current_regions: Vec<Region>,
    },
    ServerIsBusy {
        reason: String,
    },
    StaleCommand,
    StoreNotMatch {
        request_store_id: u64,
        actual_store_id: u64,
    },
    RaftEntryTooLarge {
        region_id: u64,
        entry_size: u64,
    },
}

impl Error {
    pub fn epoch_not_match(current_regions: Vec<Region>) -> Self {
        Error::EpochNotMatch { current_regions }
    }
}