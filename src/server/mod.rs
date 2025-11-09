pub mod epoch;
pub mod raw_api;
pub mod multi_level_api;
pub mod transaction_api;
pub mod grpc;

pub use raw_api::RawKvServer;
pub use transaction_api::TransactionKvServer;
pub use grpc::TinyKvService;


use crate::storage::Storage;
use std::sync::Arc;
use crate::transaction::latches::Latches;

pub struct Server<S: Storage> {
    storage: Arc<S>,
    latches: Arc<Latches>,
}

impl<S: Storage> Server<S> {
    pub fn new(storage: S) -> Self {
        Self {
            storage: Arc::new(storage),
            latches: Arc::new(Latches::new()),
        }
    }

    pub fn storage(&self) -> &Arc<S> {
        &self.storage
    }

    pub fn latches(&self) -> &Arc<Latches> {
        &self.latches
    }
}