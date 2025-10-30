//! MVCC Transaction Management
//!
//! This module provides transaction management for Multi-Version
//! Concurrency Control, allowing multiple concurrent transactions
//! with snapshot isolation.

use crate::db::group_commit::TxId;
use crate::db::timestamp_oracle::TimestampOracle;
use crate::error::{GraphError, Result};
use crate::storage::version_chain::VersionTracker;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;

/// Represents an active transaction context in an MVCC system
#[derive(Debug, Clone)]
pub struct TransactionContext {
    /// Unique transaction ID
    #[allow(dead_code)]
    pub tx_id: TxId,
    /// Snapshot timestamp (read timestamp)
    pub snapshot_ts: u64,
    /// Commit timestamp (0 until committed)
    pub commit_ts: u64,
    /// Set of records written by this transaction
    #[allow(dead_code)]
    pub written_records: HashSet<u64>,
    /// Version tracker for this transaction
    pub _version_tracker: VersionTracker,
    /// State of the transaction
    pub state: TransactionState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum TransactionState {
    /// Transaction is active and accepting operations
    Active,
    /// Transaction is preparing to commit
    Preparing,
    /// Transaction has committed successfully
    Committed,
    /// Transaction was rolled back
    _RolledBack,
}

impl TransactionContext {
    pub fn new(tx_id: TxId, snapshot_ts: u64) -> Self {
        Self {
            tx_id,
            snapshot_ts,
            commit_ts: 0,
            written_records: HashSet::new(),
            _version_tracker: VersionTracker::new(),
            state: TransactionState::Active,
        }
    }

    /// Mark a record as written by this transaction
    #[allow(dead_code)]
    pub fn mark_written(&mut self, record_id: u64) {
        self.written_records.insert(record_id);
    }

    /// Check if a record was written by this transaction
    #[allow(dead_code)]
    pub fn has_written(&self, record_id: u64) -> bool {
        self.written_records.contains(&record_id)
    }

    /// Begin commit preparation
    #[allow(dead_code)]
    pub fn start_commit(&mut self) -> Result<()> {
        if self.state != TransactionState::Active {
            return Err(GraphError::InvalidArgument(
                "transaction is not active".into(),
            ));
        }
        self.state = TransactionState::Preparing;
        Ok(())
    }

    /// Complete commit
    pub fn complete_commit(&mut self, commit_ts: u64) {
        self.commit_ts = commit_ts;
        self.state = TransactionState::Committed;
    }

    /// Rollback transaction
    pub fn _rollback(&mut self) {
        self.state = TransactionState::_RolledBack;
    }
}

/// Manages active transactions for MVCC
///
/// This structure allows multiple concurrent transactions by tracking
/// each transaction's context including its snapshot timestamp and
/// the records it has modified.
pub struct MvccTransactionManager {
    /// Timestamp oracle for allocating timestamps
    oracle: Arc<TimestampOracle>,
    /// Currently active transactions (lock-free concurrent access)
    active_transactions: Arc<DashMap<TxId, TransactionContext>>,
    /// Maximum number of concurrent transactions
    max_concurrent_transactions: usize,
}

impl MvccTransactionManager {
    /// Create a new MVCC transaction manager with a shared timestamp oracle
    pub fn new_with_oracle(oracle: Arc<TimestampOracle>, max_concurrent: usize) -> Self {
        Self {
            oracle,
            active_transactions: Arc::new(DashMap::new()),
            max_concurrent_transactions: max_concurrent,
        }
    }

    /// Create a new MVCC transaction manager (for testing)
    #[allow(dead_code)]
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            oracle: Arc::new(TimestampOracle::new()),
            active_transactions: Arc::new(DashMap::new()),
            max_concurrent_transactions: max_concurrent,
        }
    }

    /// Create a new transaction with a snapshot timestamp
    ///
    /// # Arguments
    /// * `tx_id` - Unique transaction ID
    ///
    /// # Returns
    /// Transaction context with a snapshot timestamp
    ///
    /// # Errors
    /// * Returns error if maximum concurrent transactions exceeded
    pub fn begin_transaction(&self, tx_id: TxId) -> Result<TransactionContext> {
        // Check if we've hit the concurrent transaction limit
        if self.active_transactions.len() >= self.max_concurrent_transactions {
            return Err(GraphError::InvalidArgument(format!(
                "maximum concurrent transactions ({}) exceeded",
                self.max_concurrent_transactions
            )));
        }

        // Allocate a read timestamp for this transaction
        let snapshot_ts = self.oracle.allocate_read_timestamp();

        // Create transaction context
        let context = TransactionContext::new(tx_id, snapshot_ts);

        // Register as active (lock-free insert)
        self.active_transactions.insert(tx_id, context);

        // Return a clone
        Ok(TransactionContext {
            tx_id,
            snapshot_ts,
            commit_ts: 0,
            written_records: HashSet::new(),
            _version_tracker: VersionTracker::new(),
            state: TransactionState::Active,
        })
    }

    /// Get a transaction context by ID
    #[allow(dead_code)]
    pub fn get_transaction(&self, tx_id: TxId) -> Option<TransactionContext> {
        self.active_transactions
            .get(&tx_id)
            .map(|r| r.value().clone())
    }

    /// Get a mutable transaction context by ID
    pub fn _get_transaction_mut(
        &self,
        tx_id: TxId,
    ) -> Option<dashmap::mapref::one::RefMut<TxId, TransactionContext>> {
        self.active_transactions.get_mut(&tx_id)
    }

    /// Start preparing a transaction for commit
    ///
    /// This allocates a commit timestamp for the transaction
    #[allow(dead_code)]
    pub fn prepare_commit(&self, tx_id: TxId) -> Result<u64> {
        let mut context = self
            .active_transactions
            .get_mut(&tx_id)
            .ok_or_else(|| GraphError::InvalidArgument("transaction not found".into()))?;

        context.start_commit()?;

        // Allocate commit timestamp
        let commit_ts = self.oracle.allocate_commit_timestamp();

        Ok(commit_ts)
    }

    /// Complete a transaction commit
    ///
    /// This makes the transaction's changes visible to future snapshots
    pub fn complete_commit(&self, tx_id: TxId, commit_ts: u64) -> Result<()> {
        let mut context = self
            .active_transactions
            .get_mut(&tx_id)
            .ok_or_else(|| GraphError::InvalidArgument("transaction not found".into()))?;

        let snapshot_ts = context.snapshot_ts;
        context.complete_commit(commit_ts);

        // Register commit with oracle
        self.oracle.register_snapshot(snapshot_ts, tx_id)?;

        Ok(())
    }

    /// End a transaction (commit or rollback)
    ///
    /// This removes the transaction from the active set
    pub fn end_transaction(&self, tx_id: TxId) -> Result<()> {
        let (_key, context) = self
            .active_transactions
            .remove(&tx_id)
            .ok_or_else(|| GraphError::InvalidArgument("transaction not found".into()))?;

        // If committed, unregister the snapshot from oracle
        if context.state == TransactionState::Committed {
            self.oracle.unregister_snapshot(context.snapshot_ts)?;
        }

        Ok(())
    }

    /// Get the oldest active snapshot timestamp
    ///
    /// This is used for garbage collection to determine
    /// which versions can be safely reclaimed
    pub fn _oldest_active_snapshot(&self) -> Option<u64> {
        self.active_transactions
            .iter()
            .map(|entry| entry.snapshot_ts)
            .min()
    }

    /// Check if a timestamp is visible to any active transaction
    pub fn _is_timestamp_visible(&self, ts: u64) -> bool {
        self.active_transactions
            .iter()
            .any(|entry| entry.snapshot_ts <= ts)
    }

    /// Get the timestamp oracle
    #[allow(dead_code)]
    pub fn oracle(&self) -> Arc<TimestampOracle> {
        self.oracle.clone()
    }

    /// Get count of active transactions
    pub fn active_count(&self) -> usize {
        self.active_transactions.len()
    }

    /// Get all active transaction IDs
    #[allow(dead_code)]
    pub fn active_tx_ids(&self) -> Vec<TxId> {
        self.active_transactions
            .iter()
            .map(|entry| *entry.key())
            .collect()
    }

    /// End all active transactions (used during database close)
    pub fn end_all_transactions(&self) {
        let tx_ids: Vec<TxId> = self
            .active_transactions
            .iter()
            .map(|entry| *entry.key())
            .collect();
        for tx_id in tx_ids {
            let _ = self.end_transaction(tx_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_lifecycle() {
        let manager = MvccTransactionManager::new(100);

        let tx_id = 1;
        let context = manager.begin_transaction(tx_id).unwrap();
        assert_eq!(context.tx_id, tx_id);
        assert_eq!(context.state, TransactionState::Active);
        assert!(manager.get_transaction(tx_id).is_some());

        // Prepare commit
        let commit_ts = manager.prepare_commit(tx_id).unwrap();
        assert!(commit_ts > context.snapshot_ts);

        // Complete commit
        manager.complete_commit(tx_id, commit_ts).unwrap();

        let updated_context = manager.get_transaction(tx_id).unwrap();
        assert_eq!(updated_context.state, TransactionState::Committed);
        assert_eq!(updated_context.commit_ts, commit_ts);

        // End the transaction
        manager.end_transaction(tx_id).unwrap();
        assert!(manager.get_transaction(tx_id).is_none());
    }

    #[test]
    fn test_multiple_concurrent_transactions() {
        let manager = MvccTransactionManager::new(10);

        let tx1 = manager.begin_transaction(1).unwrap();
        let tx2 = manager.begin_transaction(2).unwrap();
        let tx3 = manager.begin_transaction(3).unwrap();

        // Verify all have different snapshot timestamps
        let timestamps: Vec<u64> = vec![tx1.snapshot_ts, tx2.snapshot_ts, tx3.snapshot_ts];
        assert_eq!(timestamps.len(), 3);

        // All timestamps should be monotonically increasing
        for i in 0..timestamps.len() - 1 {
            assert!(timestamps[i] < timestamps[i + 1]);
        }

        assert_eq!(manager.active_count(), 3);
    }

    #[test]
    fn test_concurrent_transaction_limit() {
        let manager = MvccTransactionManager::new(2);

        manager.begin_transaction(1).unwrap();
        manager.begin_transaction(2).unwrap();

        // Third transaction should fail
        let result = manager.begin_transaction(3);
        assert!(result.is_err());
    }

    #[test]
    fn test_written_records_tracking() {
        let manager = MvccTransactionManager::new(100);

        let tx_id = 1;
        let mut context = manager.begin_transaction(tx_id).unwrap();

        assert!(!context.has_written(10));
        context.mark_written(10);
        assert!(context.has_written(10));
    }
}
