/// Lock-free page cache for Phase 3B-Simple
///
/// This wraps the Pager with a lock-free cache to dramatically reduce
/// contention in read-heavy workloads. The architecture follows the plan:
///
/// **Architecture:**
/// - Fast path: Check lock-free cache (DashMap)
/// - Slow path: Load from pager (under RwLock, only for misses)
///
/// **Tradeoffs:**
/// - ✅ Lock-free hot path for cached pages
/// - ✅ Simpler than full pager sharding
/// - ⚠️ Still has lock contention for page misses and writes
/// - ⚠️ Good for read-heavy workloads (80%+ cache hit rate)
use dashmap::DashMap;
use std::sync::{Arc, RwLock};

use crate::error::Result;
use crate::pager::{Page, PageId, Pager};

/// Lock-free page cache wrapper around Pager
pub struct LockFreePageCache {
    /// Page cache using lock-free map (hot path)
    cache: DashMap<PageId, Arc<Page>>,
    /// Underlying pager for misses (still has lock, but only for disk I/O)
    pager: RwLock<Pager>,
    /// Maximum cache size
    max_size: usize,
}

impl LockFreePageCache {
    /// Create a new lock-free page cache
    pub fn new(pager: Pager, max_size: usize) -> Self {
        Self {
            cache: DashMap::with_capacity(max_size),
            pager: RwLock::new(pager),
            max_size,
        }
    }

    /// Fetch a page - lock-free fast path for cache hits
    pub fn fetch_page(&self, page_id: PageId) -> Result<Arc<Page>> {
        // Fast path: check cache (lock-free)
        if let Some(page) = self.cache.get(&page_id) {
            return Ok(Arc::clone(page.value()));
        }

        // Slow path: load from disk (under lock)
        let page_copy = {
            let mut pager = self.pager.write().unwrap();
            pager.get_page_copy(page_id)?
        };

        // Cache for future reads
        let page_arc = Arc::new(page_copy);

        // Simple eviction if at capacity
        if self.cache.len() >= self.max_size {
            if let Some(entry) = self.cache.iter().next() {
                let key = *entry.key();
                drop(entry);
                self.cache.remove(&key);
            }
        }

        self.cache.insert(page_id, Arc::clone(&page_arc));
        Ok(page_arc)
    }

    /// Write a page - acquires write lock and invalidates cache
    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        let mut pager = self.pager.write().unwrap();

        // Write to pager
        let page = pager.fetch_page(page_id)?;
        page.data.copy_from_slice(data);
        page.dirty = true;

        // Invalidate cache entry
        self.cache.remove(&page_id);

        Ok(())
    }

    /// Allocate a new page
    pub fn allocate_page(&self) -> Result<PageId> {
        let mut pager = self.pager.write().unwrap();
        pager.allocate_page()
    }

    /// Flush dirty pages to disk
    pub fn flush(&self) -> Result<()> {
        let mut pager = self.pager.write().unwrap();
        pager.flush()
    }

    /// Sync WAL to disk
    pub fn sync_wal(&self) -> Result<()> {
        let mut pager = self.pager.write().unwrap();
        pager.sync_wal()
    }

    /// Checkpoint WAL to main database file
    pub fn checkpoint(&self) -> Result<()> {
        let mut pager = self.pager.write().unwrap();
        pager.checkpoint()
    }

    /// Append page to WAL
    pub fn append_page_to_wal(&self, page_id: PageId, tx_id: u64) -> Result<()> {
        let mut pager = self.pager.write().unwrap();
        pager.append_page_to_wal(page_id, tx_id)
    }

    /// Append commit to WAL
    pub fn append_commit_to_wal(&self, tx_id: u64) -> Result<()> {
        let mut pager = self.pager.write().unwrap();
        pager.append_commit_to_wal(tx_id)
    }

    /// Read page image
    pub fn read_page_image(&self, page_id: PageId) -> Result<Vec<u8>> {
        let mut pager = self.pager.write().unwrap();
        pager.read_page_image(page_id)
    }

    /// Flush specific pages
    pub fn flush_pages(&self, page_ids: &[PageId], tx_id: u64) -> Result<()> {
        let mut pager = self.pager.write().unwrap();
        pager.flush_pages(page_ids, tx_id)
    }

    /// Ensure shadow page exists
    pub fn ensure_shadow(&self, page_id: PageId) -> Result<()> {
        let mut pager = self.pager.write().unwrap();
        pager.ensure_shadow(page_id)
    }

    /// Begin shadow transaction
    pub fn begin_shadow_transaction(&self) {
        let mut pager = self.pager.write().unwrap();
        pager.begin_shadow_transaction()
    }

    /// Commit shadow transaction
    pub fn commit_shadow_transaction(&self) {
        let mut pager = self.pager.write().unwrap();
        pager.commit_shadow_transaction()
    }

    /// Rollback shadow transaction
    pub fn rollback_shadow_transaction(&self) -> Result<()> {
        let mut pager = self.pager.write().unwrap();
        pager.rollback_shadow_transaction()
    }

    /// Restore pages
    pub fn restore_pages(&self, page_ids: &[PageId]) -> Result<()> {
        let mut pager = self.pager.write().unwrap();
        pager.restore_pages(page_ids)
    }

    /// Append to WAL
    pub fn append_to_wal(&self, page_id: PageId, tx_id: u64, page_bytes: &[u8]) -> Result<()> {
        let mut pager = self.pager.write().unwrap();
        pager.append_to_wal(page_id, tx_id, page_bytes)
    }

    /// Get the underlying pager (for operations that need full access)
    pub fn with_pager_write<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Pager) -> Result<R>,
    {
        let mut pager = self.pager.write().unwrap();
        f(&mut pager)
    }

    /// Get the underlying pager for read operations
    pub fn with_pager_read<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&Pager) -> Result<R>,
    {
        let pager = self.pager.read().unwrap();
        f(&pager)
    }

    /// Clear the cache (useful for testing)
    pub fn clear_cache(&self) {
        self.cache.clear();
    }

    /// Invalidate a specific page in the cache
    ///
    /// This should be called after modifying a page via with_pager_write
    /// to ensure subsequent reads get the updated version from the pager.
    pub fn invalidate_page(&self, page_id: PageId) {
        self.cache.remove(&page_id);
    }

    /// Get cache statistics
    pub fn cache_size(&self) -> usize {
        self.cache.len()
    }

    /// Get dirty page count
    pub fn dirty_page_count(&self) -> usize {
        let pager = self.pager.read().unwrap();
        pager.dirty_page_count()
    }

    /// Get page size
    pub fn page_size(&self) -> usize {
        let pager = self.pager.read().unwrap();
        pager.page_size()
    }

    /// Get page count
    pub fn page_count(&self) -> usize {
        let pager = self.pager.read().unwrap();
        pager.page_count()
    }

    /// Get WAL size
    pub fn wal_size(&self) -> Result<u64> {
        let pager = self.pager.read().unwrap();
        pager.wal_size()
    }
}
