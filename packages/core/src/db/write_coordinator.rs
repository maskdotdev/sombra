//! Write coordinator for serializing WAL writes and checkpoints
//!
//! This module provides a thin synchronization layer for operations that MUST
//! be serialized: WAL writes, header updates, and checkpoints. This isolates
//! the serialization point to only these critical paths, allowing the rest of
//! the database to operate lock-free.

use std::sync::Mutex;

use crate::db::config::SyncMode;
use crate::db::group_commit::{CommitRequest, ControlMessage, TxId};
use crate::error::{acquire_lock, GraphError, Result};
use crate::pager::{LockFreePageCache, PageId};
use crate::storage::header::Header;

use super::core::HeaderState;

use std::sync::{Arc, Condvar};
use tracing::warn;

/// Coordinates WAL writes, header updates, and checkpoints.
///
/// This struct isolates the serialization point in the database to only
/// operations that require disk I/O and strict ordering. All operations
/// that modify the WAL or header MUST go through this coordinator.
///
/// # Thread Safety
///
/// Wrapped in `Mutex` to ensure exclusive access during critical operations.
/// This is the only global lock in the concurrent-first architecture.
pub struct WriteCoordinator {
    /// Current header state (shared with GraphDB)
    header: HeaderState,
    /// Number of transactions since last WAL sync
    transactions_since_sync: usize,
    /// Number of transactions since last checkpoint
    transactions_since_checkpoint: usize,
}

impl WriteCoordinator {
    /// Creates a new write coordinator with the given header state.
    pub fn new(header: HeaderState) -> Self {
        Self {
            header,
            transactions_since_sync: 0,
            transactions_since_checkpoint: 0,
        }
    }

    /// Commits a transaction to the WAL.
    ///
    /// This operation is serialized to ensure WAL entries are written in order.
    ///
    /// # Arguments
    /// * `tx_id` - Transaction ID to commit
    /// * `dirty_pages` - Pages modified by the transaction
    /// * `pager` - Page cache for WAL operations
    /// * `sync_mode` - WAL sync mode
    /// * `sync_interval` - Sync interval for Normal mode
    /// * `checkpoint_threshold` - Checkpoint threshold in transactions
    /// * `max_wal_size_mb` - Maximum WAL size before forcing checkpoint
    /// * `wal_size_warning_threshold_mb` - WAL size to start warning
    /// * `group_commit_state` - Optional group commit state
    /// * `checkpoint_fn` - Callback to perform checkpoint
    ///
    /// # Returns
    /// Updated header state and counters
    pub fn commit_transaction(
        &mut self,
        tx_id: TxId,
        dirty_pages: &[PageId],
        pager: &Arc<LockFreePageCache>,
        sync_mode: SyncMode,
        sync_interval: usize,
        checkpoint_threshold: usize,
        max_wal_size_mb: u64,
        wal_size_warning_threshold_mb: u64,
        group_commit_state: &Option<Arc<Mutex<crate::db::group_commit::GroupCommitState>>>,
        checkpoint_fn: &dyn Fn() -> Result<()>,
    ) -> Result<HeaderState> {
        // If no dirty pages, just commit the shadow transaction
        if dirty_pages.is_empty() {
            pager.with_pager_write(|pager| {
                pager.commit_shadow_transaction();
                Ok(())
            })?;
            return Ok(self.header.clone());
        }

        // Deduplicate and sort dirty pages
        let mut pages = dirty_pages.to_vec();
        pages.sort_unstable();
        pages.dedup();

        // Write pages to WAL
        for &page_id in &pages {
            pager.with_pager_write(|pager| pager.append_page_to_wal(page_id, tx_id))?;
        }

        // Write commit marker
        pager.with_pager_write(|pager| pager.append_commit_to_wal(tx_id))?;

        // Update counters
        self.transactions_since_sync += 1;
        self.transactions_since_checkpoint += 1;

        // Determine if we should sync the WAL
        let should_sync = match sync_mode {
            SyncMode::Full => true,
            SyncMode::GroupCommit => {
                if let Some(ref state) = group_commit_state {
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
            SyncMode::Normal => self.transactions_since_sync >= sync_interval,
            SyncMode::Checkpoint => false,
            SyncMode::Off => false,
        };

        // Sync WAL if needed
        if should_sync {
            pager.with_pager_write(|pager| pager.sync_wal())?;
            self.transactions_since_sync = 0;
        }

        // Checkpoint if needed
        if self.transactions_since_checkpoint >= checkpoint_threshold {
            checkpoint_fn()?;
            self.transactions_since_checkpoint = 0;
        }

        // Check WAL size and force checkpoint if needed
        let wal_size_bytes = pager.wal_size()?;
        let wal_size_mb = wal_size_bytes / (1024 * 1024);

        if wal_size_mb >= wal_size_warning_threshold_mb && wal_size_mb < max_wal_size_mb {
            warn!(
                wal_size_mb,
                threshold_mb = wal_size_warning_threshold_mb,
                "WAL size approaching limit"
            );
        }

        if wal_size_mb >= max_wal_size_mb {
            warn!(
                wal_size_mb,
                max_wal_size_mb, "WAL size exceeded limit, forcing checkpoint"
            );
            checkpoint_fn()?;
            self.transactions_since_checkpoint = 0;
        }

        // Commit shadow transaction
        pager.with_pager_write(|pager| {
            pager.commit_shadow_transaction();
            Ok(())
        })?;

        Ok(self.header.clone())
    }

    /// Writes the header to page 0.
    ///
    /// This operation is serialized to ensure header consistency.
    ///
    /// # Arguments
    /// * `pager` - Page cache for header write
    /// * `page_size` - Page size for header serialization
    ///
    /// # Returns
    /// Page ID 0 (header page)
    pub fn write_header(
        &mut self,
        pager: &Arc<LockFreePageCache>,
        page_size: usize,
    ) -> Result<PageId> {
        let header = self.header.to_header(page_size)?;
        pager.with_pager_write(|pager| {
            let page = pager.fetch_page(0)?;
            Header::write(&header, &mut page.data)?;
            page.dirty = true;
            Ok(())
        })?;
        Ok(0) // Return header page ID
    }

    /// Updates the header state.
    ///
    /// # Arguments
    /// * `header` - New header state
    pub fn update_header(&mut self, header: HeaderState) {
        self.header = header;
    }

    /// Gets the current header state.
    pub fn header(&self) -> &HeaderState {
        &self.header
    }

    /// Resets transaction counters (used after checkpoint).
    pub fn reset_counters(&mut self) {
        self.transactions_since_sync = 0;
        self.transactions_since_checkpoint = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::header::Header;

    #[test]
    fn test_write_coordinator_creation() {
        let header = HeaderState::from(Header::new(4096).unwrap());
        let coordinator = WriteCoordinator::new(header);
        assert_eq!(coordinator.transactions_since_sync, 0);
        assert_eq!(coordinator.transactions_since_checkpoint, 0);
    }
}
