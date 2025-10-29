use super::graphdb::GraphDB;
use crate::db::config::SyncMode;
use crate::db::group_commit::{CommitRequest, ControlMessage, TxId};
use crate::error::{acquire_lock, GraphError, Result};
use crate::pager::PageId;
use crate::storage::header::Header;
use crate::storage::heap::RecordPointer;
use crate::storage::version::{VersionMetadata, VersionedRecordKind};
use std::mem;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex};
use tracing::warn;

impl GraphDB {
    pub fn commit_to_wal(&self, tx_id: TxId, dirty_pages: &[PageId]) -> Result<()> {
        if dirty_pages.is_empty() {
            self.pager.with_pager_write(|pager| {
                pager.commit_shadow_transaction();
                Ok(())
            })?;
            return Ok(());
        }

        let mut pages = dirty_pages.to_vec();
        pages.sort_unstable();
        pages.dedup();

        for &page_id in &pages {
            self.pager
                .with_pager_write(|pager| pager.append_page_to_wal(page_id, tx_id))?;
        }

        self.pager
            .with_pager_write(|pager| pager.append_commit_to_wal(tx_id))?;

        self.transactions_since_sync.fetch_add(1, Ordering::Relaxed);
        self.transactions_since_checkpoint
            .fetch_add(1, Ordering::Relaxed);

        let should_sync = match self.config.wal_sync_mode {
            SyncMode::Full => true,
            SyncMode::GroupCommit => {
                if let Some(ref state) = self.group_commit_state {
                    let notifier = Arc::new((Mutex::new(false), Condvar::new()));
                    let commit_req = CommitRequest {
                        tx_id,
                        notifier: notifier.clone(),
                    };

                    let sender = {
                        let state_guard = acquire_lock(state.as_ref())?;
                        state_guard.sender.clone()
                    };

                    sender
                        .send(ControlMessage::Commit(commit_req))
                        .map_err(|_| GraphError::Corruption("group commit thread died".into()))?;

                    let (lock, cvar) = &*notifier;
                    let mut done = acquire_lock(lock)?;
                    while !*done {
                        done = cvar.wait(done).map_err(|_| {
                            GraphError::Corruption("commit notifier lock poisoned".into())
                        })?;
                    }
                }
                false
            }
            SyncMode::Normal => {
                self.transactions_since_sync.load(Ordering::Relaxed) >= self.config.sync_interval
            }
            SyncMode::Checkpoint => false,
            SyncMode::Off => false,
        };

        if should_sync {
            self.pager.with_pager_write(|pager| pager.sync_wal())?;
            self.transactions_since_sync.store(0, Ordering::Relaxed);
        }

        if self.transactions_since_checkpoint.load(Ordering::Relaxed)
            >= self.config.checkpoint_threshold
        {
            self.checkpoint()?;
            self.transactions_since_checkpoint
                .store(0, Ordering::Relaxed);
        }

        let wal_size_bytes = self.pager.wal_size()?;
        let wal_size_mb = wal_size_bytes / (1024 * 1024);
        let max_wal_mb = self.config.max_wal_size_mb;
        let warning_threshold_mb = self.config.wal_size_warning_threshold_mb;

        if wal_size_mb >= warning_threshold_mb && wal_size_mb < max_wal_mb {
            warn!(
                wal_size_mb,
                threshold_mb = warning_threshold_mb,
                "WAL size approaching limit"
            );
        }

        if wal_size_mb >= max_wal_mb {
            warn!(
                wal_size_mb,
                max_wal_mb, "WAL size exceeded limit, forcing checkpoint"
            );
            self.checkpoint()?;
            self.transactions_since_checkpoint
                .store(0, Ordering::Relaxed);
        }

        self.pager.with_pager_write(|pager| {
            pager.commit_shadow_transaction();
            Ok(())
        })?;
        Ok(())
    }

    pub fn rollback_transaction(&self, dirty_pages: &[PageId]) -> Result<()> {
        self.pager
            .with_pager_write(|pager| pager.rollback_shadow_transaction())?;

        self.reload_header_state()?;

        if !dirty_pages.is_empty() {
            self.rebuild_indexes()?;
        }
        Ok(())
    }

    fn reload_header_state(&self) -> Result<()> {
        let (header_data, page_size) = self.pager.with_pager_write(|pager| {
            let page_size = pager.page_size();
            let page = pager.fetch_page(0)?;
            Ok((page.data.clone(), page_size))
        })?;

        let header = match Header::read(&header_data)? {
            Some(header) => header,
            None => Header::new(page_size)?,
        };
        *self.header.lock().unwrap() = HeaderState::from(header);
        Ok(())
    }

    pub fn start_tracking(&self) {
        self.recent_dirty_pages.lock().unwrap().clear();
        self.tracking_enabled.store(true, Ordering::Release);
    }

    pub fn stop_tracking(&self) {
        self.tracking_enabled.store(false, Ordering::Release);
        self.recent_dirty_pages.lock().unwrap().clear();
    }

    pub fn take_recent_dirty_pages(&self) -> Vec<PageId> {
        if !self.tracking_enabled.load(Ordering::Acquire) {
            return Vec::new();
        }
        let mut guard = self.recent_dirty_pages.lock().unwrap();
        if guard.is_empty() {
            return Vec::new();
        }
        let mut pages = mem::take(&mut *guard);
        drop(guard);
        pages.sort_unstable();
        pages.dedup();
        pages
    }

    pub(crate) fn record_page_write(&self, page_id: PageId) {
        // Invalidate the cache entry to ensure subsequent reads get fresh data
        self.pager.invalidate_page(page_id);

        if self.tracking_enabled.load(Ordering::Acquire) {
            self.recent_dirty_pages.lock().unwrap().push(page_id);
        }
    }

    /// Allocate a new transaction ID (lock-free using atomic operations)
    pub fn allocate_tx_id(&self) -> Result<TxId> {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::Relaxed);
        if tx_id == u64::MAX {
            return Err(GraphError::Corruption("transaction id overflow".into()));
        }
        Ok(tx_id)
    }

    pub fn enter_transaction(&self, tx_id: TxId) -> Result<()> {
        // Register transaction with MVCC manager (lock-free)
        if let Some(ref tx_manager) = self.mvcc_tx_manager {
            tx_manager.begin_transaction(tx_id)?;
        }
        self.pager.with_pager_write(|pager| {
            pager.begin_shadow_transaction();
            Ok(())
        })?;
        Ok(())
    }

    pub fn exit_transaction(&self, tx_id: TxId) {
        // End the transaction in the MVCC manager
        if let Some(ref tx_manager) = self.mvcc_tx_manager {
            let _ = tx_manager.end_transaction(tx_id);
        }
    }

    pub(crate) fn is_in_transaction(&self) -> bool {
        if let Some(ref tx_manager) = self.mvcc_tx_manager {
            tx_manager.active_count() > 0
        } else {
            false
        }
    }

    pub fn write_header(&self) -> Result<()> {
        let page_size = self.pager.page_size();
        let header = self.header.lock().unwrap().to_header(page_size)?;
        self.pager.with_pager_write(|pager| {
            let page = pager.fetch_page(0)?;
            Header::write(&header, &mut page.data)?;
            page.dirty = true;
            Ok(())
        })?;
        self.record_page_write(0);
        Ok(())
    }

    /// Updates commit_ts for all versions created by a transaction
    ///
    /// This is called during transaction commit to update all version metadata
    /// records from commit_ts=0 (uncommitted) to the actual commit timestamp.
    /// This allows GC to identify committed versions for cleanup.
    ///
    /// # Arguments
    /// * `tx_id` - The transaction ID that created the versions
    /// * `commit_ts` - The commit timestamp to set
    /// * `dirty_pages` - Pages modified by the transaction (used as fallback)
    /// * `version_pointers` - Direct pointers to version records (optimization)
    ///
    /// # Returns
    /// Ok(()) if successful
    pub fn update_versions_commit_ts(
        &self,
        tx_id: TxId,
        commit_ts: u64,
        dirty_pages: &[PageId],
        version_pointers: &[RecordPointer],
    ) -> Result<()> {
        use crate::storage::heap::RecordStore;

        // Fast path: if we have tracked version pointers, use them directly
        if !version_pointers.is_empty() {
            let update_dirty_pages = self.pager.with_pager_write(|pager| {
                let mut record_store = RecordStore::new(pager);
                for &pointer in version_pointers {
                    record_store.update_commit_ts(pointer, commit_ts)?;
                }

                // Extract dirty pages before dropping guard
                Ok(record_store.take_dirty_pages())
            })?;

            // Register dirty pages with GraphDB
            for page_id in update_dirty_pages {
                self.record_page_write(page_id);
            }

            return Ok(());
        }

        // Slow path: scan dirty pages (fallback for legacy code paths)
        use crate::storage::page::RecordPage;

        // Collect all version pointers that need updating first,
        // then update them to avoid borrow checker issues
        let mut versions_to_update: Vec<RecordPointer> = Vec::new();

        // Scan all dirty pages for versioned records created by this transaction
        for &page_id in dirty_pages {
            let page_versions = self.pager.with_pager_write(|pager| {
                let page = pager.fetch_page(page_id)?;
                
                // Try to parse as RecordPage; skip if it's an index page (BTree, Property)
                // Dirty pages can include index pages when nodes are added/modified
                let record_page = match RecordPage::from_bytes(&mut page.data) {
                    Ok(rp) => rp,
                    Err(GraphError::InvalidArgument(_)) => {
                        // This is an index page with magic bytes (BIDX, PIDX), skip it
                        return Ok(Vec::new());
                    }
                    Err(e) => return Err(e),
                };
                
                let record_count = record_page.record_count()? as usize;
                let mut pointers = Vec::new();

                for slot_index in 0..record_count {
                    // Get record data
                    let record_data = match record_page.record_slice(slot_index) {
                        Ok(data) => data,
                        Err(_) => continue, // Skip corrupted records
                    };

                    if record_data.is_empty() {
                        continue;
                    }

                    // Check if this is a versioned record
                    let kind_byte = record_data[0];
                    let is_versioned = match VersionedRecordKind::from_byte(kind_byte) {
                        Ok(VersionedRecordKind::VersionedNode) => true,
                        Ok(VersionedRecordKind::VersionedEdge) => true,
                        _ => false,
                    };

                    if !is_versioned {
                        continue;
                    }

                    // Read version metadata (starts at offset 8, after 8-byte record header)
                    // Record layout: [kind:1][reserved:3][payload_len:4][metadata:25][data:N]
                    if record_data.len() < 8 + 25 {
                        continue; // Not enough data for version metadata
                    }

                    let metadata = VersionMetadata::from_bytes(&record_data[8..])?;

                    // If this version was created by our transaction and is uncommitted
                    if metadata.tx_id == tx_id && metadata.commit_ts == 0 {
                        let byte_offset = record_page.record_offset(slot_index)?;
                        let pointer = RecordPointer {
                            page_id,
                            slot_index: slot_index as u16,
                            byte_offset,
                        };
                        pointers.push(pointer);
                    }
                }
                Ok(pointers)
            })?;

            versions_to_update.extend(page_versions);
        }

        // Now update all collected versions
        let update_dirty_pages = self.pager.with_pager_write(|pager| {
            let mut record_store = RecordStore::new(pager);
            for pointer in versions_to_update {
                record_store.update_commit_ts(pointer, commit_ts)?;
            }

            // Extract dirty pages before dropping guard
            Ok(record_store.take_dirty_pages())
        })?;

        // Register dirty pages with GraphDB
        for page_id in update_dirty_pages {
            self.record_page_write(page_id);
        }

        Ok(())
    }
}

use super::header::HeaderState;
