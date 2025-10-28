use super::graphdb::GraphDB;
use crate::db::config::SyncMode;
use crate::db::group_commit::{CommitRequest, ControlMessage, TxId};
use crate::error::{acquire_lock, GraphError, Result};
use crate::pager::PageId;
use crate::storage::header::Header;
use crate::storage::heap::{RecordPointer, RecordStore};
use crate::storage::version::{VersionMetadata, VersionedRecordKind};
use std::mem;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex};
use tracing::warn;

impl GraphDB {
    pub fn commit_to_wal(&mut self, tx_id: TxId, dirty_pages: &[PageId]) -> Result<()> {
        if dirty_pages.is_empty() {
            self.pager.write().unwrap().commit_shadow_transaction();
            return Ok(());
        }

        let mut pages = dirty_pages.to_vec();
        pages.sort_unstable();
        pages.dedup();

        for &page_id in &pages {
            self.pager.write().unwrap().append_page_to_wal(page_id, tx_id)?;
        }

        self.pager.write().unwrap().append_commit_to_wal(tx_id)?;

        self.transactions_since_sync += 1;
        self.transactions_since_checkpoint += 1;

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
            SyncMode::Normal => self.transactions_since_sync >= self.config.sync_interval,
            SyncMode::Checkpoint => false,
            SyncMode::Off => false,
        };

        if should_sync {
            self.pager.write().unwrap().sync_wal()?;
            self.transactions_since_sync = 0;
        }

        if self.transactions_since_checkpoint >= self.config.checkpoint_threshold {
            self.checkpoint()?;
            self.transactions_since_checkpoint = 0;
        }

        let wal_size_bytes = self.pager.read().unwrap().wal_size()?;
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
            self.transactions_since_checkpoint = 0;
        }

        self.pager.write().unwrap().commit_shadow_transaction();
        Ok(())
    }

    pub fn rollback_transaction(&mut self, dirty_pages: &[PageId]) -> Result<()> {
        self.pager.write().unwrap().rollback_shadow_transaction()?;

        self.reload_header_state()?;

        if !dirty_pages.is_empty() {
            self.rebuild_indexes()?;
        }
        Ok(())
    }

    fn reload_header_state(&mut self) -> Result<()> {
        let mut pager_guard = self.pager.write().unwrap();
        let page = pager_guard.fetch_page(0)?;
        let header = match Header::read(&page.data)? {
            Some(header) => header,
            None => Header::new(pager_guard.page_size())?,
        };
        drop(pager_guard);
        self.header = HeaderState::from(header);
        Ok(())
    }

    pub fn start_tracking(&mut self) {
        self.recent_dirty_pages.lock().unwrap().clear();
        self.tracking_enabled.store(true, Ordering::Release);
    }

    pub fn stop_tracking(&mut self) {
        self.tracking_enabled.store(false, Ordering::Release);
        self.recent_dirty_pages.lock().unwrap().clear();
    }

    pub fn take_recent_dirty_pages(&mut self) -> Vec<PageId> {
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

    pub(crate) fn record_page_write(&mut self, page_id: PageId) {
        if self.tracking_enabled.load(Ordering::Acquire) {
            self.recent_dirty_pages.lock().unwrap().push(page_id);
        }
    }

    pub fn allocate_tx_id(&mut self) -> Result<TxId> {
        let tx_id = self.next_tx_id;
        self.next_tx_id = self
            .next_tx_id
            .checked_add(1)
            .ok_or_else(|| GraphError::Corruption("transaction id overflow".into()))?;
        Ok(tx_id)
    }

    pub fn enter_transaction(&mut self, tx_id: TxId) -> Result<()> {
        // In MVCC mode, allow concurrent transactions via the manager
        if let Some(ref mut tx_manager) = self.mvcc_tx_manager {
            // Register transaction with MVCC manager
            tx_manager.begin_transaction(tx_id)?;
            self.pager.write().unwrap().begin_shadow_transaction();
            Ok(())
        } else {
            // Legacy single-writer mode
            if self.active_transaction.is_some() {
                return Err(GraphError::InvalidArgument(
                    "nested transactions are not supported".into(),
                ));
            }
            self.pager.write().unwrap().begin_shadow_transaction();
            self.active_transaction = Some(tx_id);
            Ok(())
        }
    }

    pub fn exit_transaction(&mut self, tx_id: TxId) {
        // In MVCC mode, end the transaction in the manager
        if let Some(ref mut tx_manager) = self.mvcc_tx_manager {
            let _ = tx_manager.end_transaction(tx_id);
        } else {
            // Legacy mode: clear active_transaction
            self.active_transaction = None;
        }
    }

    pub(crate) fn is_in_transaction(&self) -> bool {
        if let Some(ref tx_manager) = self.mvcc_tx_manager {
            tx_manager.active_count() > 0
        } else {
            self.active_transaction.is_some()
        }
    }

    pub fn write_header(&mut self) -> Result<()> {
        let mut pager_guard = self.pager.write().unwrap();
        let header = self.header.to_header(pager_guard.page_size())?;
        let page = pager_guard.fetch_page(0)?;
        Header::write(&header, &mut page.data)?;
        page.dirty = true;
        drop(pager_guard);
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
        &mut self,
        tx_id: TxId,
        commit_ts: u64,
        dirty_pages: &[PageId],
        version_pointers: &[RecordPointer],
    ) -> Result<()> {
        use crate::storage::heap::RecordStore;
        
        // Fast path: if we have tracked version pointers, use them directly
        if !version_pointers.is_empty() {
            let update_dirty_pages = {
                let mut pager_guard = self.pager.write().unwrap();
                let mut record_store = RecordStore::new(&mut *pager_guard);
                for &pointer in version_pointers {
                    record_store.update_commit_ts(pointer, commit_ts)?;
                }
                
                // Extract dirty pages before dropping guard
                let dirty_pages = record_store.take_dirty_pages();
                drop(record_store);
                drop(pager_guard);
                dirty_pages
            };
            
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
            let mut pager_guard = self.pager.write().unwrap();
            let page = pager_guard.fetch_page(page_id)?;
            let record_page = RecordPage::from_bytes(&mut page.data)?;
            let record_count = record_page.record_count()? as usize;
            
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
                    versions_to_update.push(pointer);
                }
            }
            drop(pager_guard);
        }
        
        // Now update all collected versions
        let update_dirty_pages = {
            let mut pager_guard = self.pager.write().unwrap();
            let mut record_store = RecordStore::new(&mut *pager_guard);
            for pointer in versions_to_update {
                record_store.update_commit_ts(pointer, commit_ts)?;
            }
            
            // Extract dirty pages before dropping guard
            let dirty_pages = record_store.take_dirty_pages();
            drop(record_store);
            drop(pager_guard);
            dirty_pages
        };
        
        // Register dirty pages with GraphDB
        for page_id in update_dirty_pages {
            self.record_page_write(page_id);
        }
        
        Ok(())
    }
}

use super::header::HeaderState;
