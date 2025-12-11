use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;

use crate::storage::mvcc::{VersionHeader, VersionLogEntry, VersionPtr};
use crate::storage::{edge, node};

pub(crate) struct VersionCache {
    shards: Vec<Mutex<LruCache<u64, Arc<VersionLogEntry>>>>,
}

impl VersionCache {
    pub(crate) fn new(shards: usize, capacity: usize) -> Self {
        let shard_count = shards.max(1);
        let per_shard_cap = (capacity / shard_count).max(1);
        let mut shard_vec = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shard_vec.push(Mutex::new(LruCache::new(
                NonZeroUsize::new(per_shard_cap).unwrap(),
            )));
        }
        Self { shards: shard_vec }
    }

    pub(crate) fn get(&self, ptr: VersionPtr) -> Option<Arc<VersionLogEntry>> {
        let mut guard = self.shard_for(ptr).lock();
        guard.get(&ptr.raw()).cloned()
    }

    pub(crate) fn insert(&self, ptr: VersionPtr, entry: Arc<VersionLogEntry>) {
        let mut guard = self.shard_for(ptr).lock();
        guard.put(ptr.raw(), entry);
    }

    fn shard_for(&self, ptr: VersionPtr) -> &Mutex<LruCache<u64, Arc<VersionLogEntry>>> {
        let idx = (ptr.raw() as usize) % self.shards.len();
        &self.shards[idx]
    }
}

pub(crate) trait VersionChainRecord {
    fn header(&self) -> &VersionHeader;
    fn prev_ptr(&self) -> VersionPtr;
    fn inline_history(&self) -> Option<&[u8]>;
}

impl VersionChainRecord for node::VersionedNodeRow {
    fn header(&self) -> &VersionHeader {
        &self.header
    }

    fn prev_ptr(&self) -> VersionPtr {
        self.prev_ptr
    }

    fn inline_history(&self) -> Option<&[u8]> {
        self.inline_history.as_deref()
    }
}

impl VersionChainRecord for edge::VersionedEdgeRow {
    fn header(&self) -> &VersionHeader {
        &self.header
    }

    fn prev_ptr(&self) -> VersionPtr {
        self.prev_ptr
    }

    fn inline_history(&self) -> Option<&[u8]> {
        self.inline_history.as_deref()
    }
}
