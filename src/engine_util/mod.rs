pub mod engines;
pub mod write_batch;
pub mod iterator;
pub mod util;
pub mod wal;

pub use engines::Engines;
pub use write_batch::WriteBatch;
pub use iterator::{DBIterator, DBItem, RocksDBIterator, CFItem};
pub use util::{key_with_cf, get_cf, put_cf, delete_cf};
pub use wal::{WalManager, WalEntry, WalEntryType, verify_data_consistency, check_wal_integrity};

pub const CF_DEFAULT: &str = "default";
pub const CF_WRITE: &str = "write";
pub const CF_LOCK: &str = "lock";

pub const CFS: [&str; 3] = [CF_DEFAULT, CF_WRITE, CF_LOCK];