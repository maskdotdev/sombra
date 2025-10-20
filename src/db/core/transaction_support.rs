use super::graphdb::GraphDB;
use crate::db::config::SyncMode;
use crate::db::group_commit::{CommitRequest, TxId};
use crate::error::{acquire_lock, GraphError, Result};
use crate::pager::PageId;
use crate::storage::header::Header;
use std::mem;
use std::sync::{Arc, Condvar, Mutex};
use tracing::warn;

impl GraphDB {
    pub(crate) fn commit_to_wal(&mut self, tx_id: TxId, dirty_pages: &[PageId]) -> Result<()> {
        if dirty_pages.is_empty() {
            self.pager.commit_shadow_transaction();
            return Ok(());
        }

        let mut pages = dirty_pages.to_vec();
        pages.sort_unstable();
        pages.dedup();

        for &page_id in &pages {
            self.pager.append_page_to_wal(page_id, tx_id)?;
        }

        self.pager.append_commit_to_wal(tx_id)?;

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
                        .send(commit_req)
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
            self.pager.sync_wal()?;
            self.transactions_since_sync = 0;
        }

        if self.transactions_since_checkpoint >= self.config.checkpoint_threshold {
            self.checkpoint()?;
            self.transactions_since_checkpoint = 0;
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
                max_wal_mb,
                "WAL size exceeded limit, forcing checkpoint"
            );
            self.checkpoint()?;
            self.transactions_since_checkpoint = 0;
        }

        self.pager.commit_shadow_transaction();
        Ok(())
    }

    pub(crate) fn rollback_transaction(&mut self, dirty_pages: &[PageId]) -> Result<()> {
        self.pager.rollback_shadow_transaction()?;

        self.reload_header_state()?;

        if !dirty_pages.is_empty() {
            self.rebuild_indexes()?;
        }
        Ok(())
    }

    fn reload_header_state(&mut self) -> Result<()> {
        let page = self.pager.fetch_page(0)?;
        let header = match Header::read(&page.data)? {
            Some(header) => header,
            None => Header::new(self.pager.page_size())?,
        };
        self.header = HeaderState::from(header);
        Ok(())
    }

    pub(crate) fn start_tracking(&mut self) {
        self.recent_dirty_pages.clear();
        self.tracking_enabled = true;
    }

    pub(crate) fn stop_tracking(&mut self) {
        self.tracking_enabled = false;
        self.recent_dirty_pages.clear();
    }

    pub(crate) fn take_recent_dirty_pages(&mut self) -> Vec<PageId> {
        if !self.tracking_enabled || self.recent_dirty_pages.is_empty() {
            return Vec::new();
        }
        let mut pages = mem::take(&mut self.recent_dirty_pages);
        pages.sort_unstable();
        pages.dedup();
        pages
    }

    pub(crate) fn record_page_write(&mut self, page_id: PageId) {
        if self.tracking_enabled {
            self.recent_dirty_pages.push(page_id);
        }
    }

    pub(crate) fn allocate_tx_id(&mut self) -> Result<TxId> {
        let tx_id = self.next_tx_id;
        self.next_tx_id = self
            .next_tx_id
            .checked_add(1)
            .ok_or_else(|| GraphError::Corruption("transaction id overflow".into()))?;
        Ok(tx_id)
    }

    pub(crate) fn enter_transaction(&mut self, tx_id: TxId) -> Result<()> {
        if self.active_transaction.is_some() {
            return Err(GraphError::InvalidArgument(
                "nested transactions are not supported".into(),
            ));
        }
        self.pager.begin_shadow_transaction();
        self.active_transaction = Some(tx_id);
        Ok(())
    }

    pub(crate) fn exit_transaction(&mut self) {
        self.active_transaction = None;
    }

    pub(crate) fn is_in_transaction(&self) -> bool {
        self.active_transaction.is_some()
    }

    pub(crate) fn write_header(&mut self) -> Result<()> {
        let header = self.header.to_header(self.pager.page_size())?;
        let page = self.pager.fetch_page(0)?;
        Header::write(&header, &mut page.data)?;
        page.dirty = true;
        self.record_page_write(0);
        Ok(())
    }
}

use super::header::HeaderState;
