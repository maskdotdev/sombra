/// Lock-free page content cache for read optimization
///
/// This cache sits *in front of* the RwLock<Pager> and serves as a
/// read-only hint cache. It dramatically reduces contention for read-heavy
/// workloads by allowing concurrent reads without acquiring the pager lock.
///
/// **Architecture:**
/// - Read path: Check hint cache first (lock-free), fall back to pager if miss
/// - Write path: Update pager (under lock), then update hint cache
///
/// **Safety:**
/// - Cache stores immutable copies of page data
/// - Writes invalidate stale entries
/// - At worst, a stale read will retry through the pager
use dashmap::DashMap;
use std::sync::Arc;

use crate::pager::PageId;

/// Immutable snapshot of page data
#[derive(Clone)]
pub struct PageHint {
    pub data: Arc<Vec<u8>>,
}

pub struct PageCacheHint {
    cache: DashMap<PageId, PageHint>,
    max_size: usize,
}

impl PageCacheHint {
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: DashMap::with_capacity(max_size),
            max_size,
        }
    }

    /// Try to get a page hint (lock-free, fast path for reads)
    pub fn get(&self, page_id: PageId) -> Option<PageHint> {
        self.cache.get(&page_id).map(|entry| entry.value().clone())
    }

    /// Update the hint cache with fresh page data (called after writes)
    pub fn put(&self, page_id: PageId, data: Vec<u8>) {
        // Simple eviction: if at capacity, remove random entry
        if self.cache.len() >= self.max_size {
            // Evict first entry we can find
            if let Some(entry) = self.cache.iter().next() {
                let key = *entry.key();
                drop(entry);
                self.cache.remove(&key);
            }
        }

        self.cache.insert(
            page_id,
            PageHint {
                data: Arc::new(data),
            },
        );
    }

    /// Invalidate a page hint (called when page is modified)
    pub fn invalidate(&self, page_id: PageId) {
        self.cache.remove(&page_id);
    }

    /// Clear the entire hint cache
    pub fn clear(&self) {
        self.cache.clear();
    }
}
