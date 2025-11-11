use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub struct IdAllocator {
    counter: AtomicU64,
}

impl IdAllocator {
    pub fn new(start: u64) -> Self {
        Self {
            counter: AtomicU64::new(start),
        }
    }

    pub fn alloc(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst) + 1
    }
}