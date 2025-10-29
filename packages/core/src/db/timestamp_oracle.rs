//! Timestamp Oracle for MVCC Snapshot Isolation
//!
//! This module provides centralized timestamp generation for multi-version
//! concurrency control (MVCC). The timestamp oracle ensures that every
//! transaction gets a unique, monotonically increasing timestamp that
//! determines which versions of data are visible to that transaction.
//!
//! # Overview
//!
//! In an MVCC system, each transaction operates on a "snapshot" of the
//! database at the time it started. The timestamp oracle provides these
//! timestamps and tracks which snapshots are still active (needed for
//! garbage collection).
//!
//! # Usage
//!
//! ```rust
//! let oracle = TimestampOracle::new();
//! let read_ts = oracle.allocate_read_timestamp();
//! // Use read_ts for snapshot isolation...
//! let commit_ts = oracle.allocate_commit_timestamp();
//! // Mark transaction as committed with commit_ts...
//! ```

use crate::error::{GraphError, Result};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tracing::{debug, trace};

/// Information about an active snapshot
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// The timestamp when this snapshot was created
    pub ts: u64,
    /// Transaction ID that created this snapshot
    pub tx_id: u64,
    /// Whether this snapshot is still active (transaction not yet committed/aborted)
    pub is_active: bool,
}

/// Snapshot structure containing active transaction IDs
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// The timestamp of this snapshot
    pub ts: u64,
    /// Set of transaction IDs that were active when this snapshot was taken
    pub active_tx_ids: std::collections::BTreeSet<u64>,
}

/// Timestamp Oracle for MVCC snapshot isolation
///
/// The timestamp oracle is responsible for:
/// 1. Allocating monotonically increasing timestamps
/// 2. Tracking active snapshots for garbage collection
/// 3. Determining which versions are visible to a given snapshot
///
/// # Thread Safety
///
/// The timestamp oracle is fully thread-safe and can be shared across
/// multiple threads via `Arc<TimestampOracle>`.
#[derive(Debug)]
pub struct TimestampOracle {
    /// Current timestamp counter (incremented atomically)
    current: AtomicU64,
    /// Map of active snapshots (timestamp -> snapshot info)
    active_snapshots: Mutex<BTreeMap<u64, SnapshotInfo>>,
    /// Minimum active snapshot timestamp (for garbage collection)
    min_active_ts: AtomicU64,
}

impl TimestampOracle {
    /// Creates a new timestamp oracle
    ///
    /// Starts with timestamp 1 (timestamp 0 is reserved as "invalid").
    ///
    /// # Returns
    /// A new `TimestampOracle` instance
    pub fn new() -> Self {
        Self {
            current: AtomicU64::new(1),
            active_snapshots: Mutex::new(BTreeMap::new()),
            min_active_ts: AtomicU64::new(1),
        }
    }

    /// Creates a timestamp oracle with a specific starting timestamp
    ///
    /// Used during database recovery to restore the timestamp state.
    ///
    /// # Arguments
    /// * `starting_ts` - The starting timestamp (must be > 0)
    ///
    /// # Returns
    /// A new `TimestampOracle` instance with the specified starting timestamp
    ///
    /// # Errors
    /// Returns an error if `starting_ts` is 0 (reserved value)
    pub fn with_timestamp(starting_ts: u64) -> Result<Self> {
        if starting_ts == 0 {
            return Err(GraphError::InvalidArgument(
                "timestamp 0 is reserved".into(),
            ));
        }

        Ok(Self {
            current: AtomicU64::new(starting_ts),
            active_snapshots: Mutex::new(BTreeMap::new()),
            min_active_ts: AtomicU64::new(starting_ts),
        })
    }

    /// Allocates a new read timestamp (snapshot timestamp)
    ///
    /// This timestamp is used to determine which versions of data are
    /// visible to a transaction. All versions with `commit_ts <= read_ts`
    /// and not from active transactions in the snapshot are visible.
    ///
    /// # Returns
    /// A monotonically increasing timestamp
    pub fn allocate_read_timestamp(&self) -> u64 {
        let ts = self.current.fetch_add(1, Ordering::AcqRel);
        trace!(timestamp = ts, "Allocated read timestamp");
        ts
    }

    /// Allocates a new commit timestamp
    ///
    /// The commit timestamp is assigned when a transaction is committing.
    /// Versions created by this transaction will be marked with this timestamp.
    ///
    /// # Returns
    /// A monotonically increasing timestamp
    pub fn allocate_commit_timestamp(&self) -> u64 {
        let ts = self.current.fetch_add(1, Ordering::AcqRel);
        trace!(timestamp = ts, "Allocated commit timestamp");
        ts
    }

    /// Records a new active snapshot
    ///
    /// Called when a transaction starts to track its snapshot for garbage collection.
    ///
    /// # Arguments
    /// * `ts` - The snapshot timestamp
    /// * `tx_id` - The transaction ID
    pub fn register_snapshot(&self, ts: u64, tx_id: u64) -> Result<()> {
        let mut snapshots = self
            .active_snapshots
            .lock()
            .map_err(|_| GraphError::Corruption("timestamp oracle lock poisoned".into()))?;

        snapshots.insert(
            ts,
            SnapshotInfo {
                ts,
                tx_id,
                is_active: true,
            },
        );

        // Update minimum active timestamp
        if let Some(&min_ts) = snapshots.keys().next() {
            self.min_active_ts.store(min_ts, Ordering::Release);
        }

        debug!(timestamp = ts, tx_id = tx_id, "Registered snapshot");
        Ok(())
    }

    /// Unregisters a snapshot (transaction completed)
    ///
    /// Called when a transaction commits or aborts to remove it from
    /// the active snapshots set.
    ///
    /// # Arguments
    /// * `ts` - The snapshot timestamp
    pub fn unregister_snapshot(&self, ts: u64) -> Result<()> {
        let mut snapshots = self
            .active_snapshots
            .lock()
            .map_err(|_| GraphError::Corruption("timestamp oracle lock poisoned".into()))?;

        if snapshots.remove(&ts).is_some() {
            // Update minimum active timestamp if we removed the min
            if snapshots.is_empty() {
                self.min_active_ts
                    .store(self.current.load(Ordering::Acquire), Ordering::Release);
            } else if let Some(&min_ts) = snapshots.keys().next() {
                self.min_active_ts.store(min_ts, Ordering::Release);
            }

            debug!(timestamp = ts, "Unregistered snapshot");
        }

        Ok(())
    }

    /// Gets snapshot information for a given timestamp
    ///
    /// Used to determine which transactions were active when this
    /// snapshot was taken.
    ///
    /// # Arguments
    /// * `ts` - The snapshot timestamp
    ///
    /// # Returns
    /// A `Snapshot` containing the timestamp and active transaction IDs
    pub fn get_snapshot(&self, ts: u64) -> Result<Snapshot> {
        let snapshots = self
            .active_snapshots
            .lock()
            .map_err(|_| GraphError::Corruption("timestamp oracle lock poisoned".into()))?;

        // Find all snapshots with timestamp <= ts that are still active
        let active_tx_ids: std::collections::BTreeSet<u64> = snapshots
            .iter()
            .filter(|(snapshot_ts, info)| *snapshot_ts <= &ts && info.is_active)
            .map(|(_, info)| info.tx_id)
            .collect();

        Ok(Snapshot { ts, active_tx_ids })
    }

    /// Returns the timestamp before which garbage collection is safe
    ///
    /// Versions older than this timestamp can be safely garbage collected
    /// because no active transaction can still reference them.
    ///
    /// # Returns
    /// The earliest timestamp that any active snapshot might reference
    pub fn gc_eligible_before(&self) -> u64 {
        self.min_active_ts.load(Ordering::Acquire)
    }

    /// Returns the current maximum timestamp
    ///
    /// # Returns
    /// The highest timestamp that has been allocated (or will be allocated next)
    pub fn current_timestamp(&self) -> u64 {
        self.current.load(Ordering::Acquire)
    }

    /// Sets the current timestamp (for database recovery)
    ///
    /// Used during recovery to restore the timestamp counter from persistent storage.
    ///
    /// # Arguments
    /// * `ts` - The timestamp to set
    ///
    /// # Errors
    /// Returns an error if `ts` is 0 (reserved value)
    pub fn set_current_timestamp(&self, ts: u64) -> Result<()> {
        if ts == 0 {
            return Err(GraphError::InvalidArgument(
                "timestamp 0 is reserved".into(),
            ));
        }

        self.current.store(ts, Ordering::Release);
        debug!(timestamp = ts, "Set current timestamp");
        Ok(())
    }

    /// Returns information about all active snapshots
    ///
    /// Used for debugging and monitoring.
    ///
    /// # Returns
    /// A vector of all currently active snapshot informations
    pub fn active_snapshot_infos(&self) -> Result<Vec<SnapshotInfo>> {
        let snapshots = self
            .active_snapshots
            .lock()
            .map_err(|_| GraphError::Corruption("timestamp oracle lock poisoned".into()))?;

        Ok(snapshots
            .values()
            .filter(|info| info.is_active)
            .cloned()
            .collect())
    }
}

impl Default for TimestampOracle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_timestamp_allocation() {
        let oracle = TimestampOracle::new();
        let ts1 = oracle.allocate_read_timestamp();
        let ts2 = oracle.allocate_read_timestamp();
        let ts3 = oracle.allocate_commit_timestamp();

        assert_eq!(ts1, 1);
        assert_eq!(ts2, 2);
        assert_eq!(ts3, 3);
    }

    #[test]
    fn test_snapshot_registration() {
        let oracle = TimestampOracle::new();
        let ts = oracle.allocate_read_timestamp();
        oracle.register_snapshot(ts, 100).unwrap();

        let snapshot = oracle.get_snapshot(ts).unwrap();
        assert_eq!(snapshot.ts, ts);
        assert_eq!(snapshot.active_tx_ids.len(), 1);
        assert!(snapshot.active_tx_ids.contains(&100));
    }

    #[test]
    fn test_snapshot_unregistration() {
        let oracle = TimestampOracle::new();
        let ts = oracle.allocate_read_timestamp();
        oracle.register_snapshot(ts, 100).unwrap();

        let snapshot_before = oracle.get_snapshot(ts).unwrap();
        assert_eq!(snapshot_before.active_tx_ids.len(), 1);

        oracle.unregister_snapshot(ts).unwrap();

        let snapshot_after = oracle.get_snapshot(ts).unwrap();
        assert_eq!(snapshot_after.active_tx_ids.len(), 0);
    }

    #[test]
    fn test_gc_eligible_before() {
        let oracle = TimestampOracle::new();

        // Initially, nothing is GC-eligible (min_active_ts = 1)
        assert_eq!(oracle.gc_eligible_before(), 1);

        // Register a snapshot at ts=10
        let ts = 10;
        oracle.register_snapshot(ts, 100).unwrap();
        assert_eq!(oracle.gc_eligible_before(), 10);

        // Unregister it
        oracle.unregister_snapshot(ts).unwrap();
        // min_active_ts should be current
        assert!(oracle.gc_eligible_before() >= oracle.current_timestamp());
    }

    #[test]
    fn test_multiple_active_snapshots() {
        let oracle = TimestampOracle::new();

        let ts1 = oracle.allocate_read_timestamp();
        let ts2 = oracle.allocate_read_timestamp();
        let ts3 = oracle.allocate_read_timestamp();

        oracle.register_snapshot(ts1, 100).unwrap();
        oracle.register_snapshot(ts2, 200).unwrap();
        oracle.register_snapshot(ts3, 300).unwrap();

        let snapshot = oracle.get_snapshot(ts3).unwrap();
        assert_eq!(snapshot.active_tx_ids.len(), 3);
        assert!(snapshot.active_tx_ids.contains(&100));
        assert!(snapshot.active_tx_ids.contains(&200));
        assert!(snapshot.active_tx_ids.contains(&300));
    }

    #[test]
    fn test_timestamp_zero_is_reserved() {
        assert!(TimestampOracle::with_timestamp(0).is_err());
    }

    #[test]
    fn test_set_current_timestamp() {
        let oracle = TimestampOracle::new();
        oracle.set_current_timestamp(1000).unwrap();

        let ts = oracle.allocate_read_timestamp();
        assert_eq!(ts, 1000); // Next timestamp after 1000
    }

    #[test]
    fn test_get_snapshot_filters_by_timestamp() {
        let oracle = TimestampOracle::new();

        let ts1 = 10;
        let ts2 = 20;
        let ts3 = 30;

        oracle.register_snapshot(ts1, 100).unwrap();
        oracle.register_snapshot(ts2, 200).unwrap();
        oracle.register_snapshot(ts3, 300).unwrap();

        // Snapshot at ts=15 should only see tx 100
        let snapshot = oracle.get_snapshot(15).unwrap();
        assert_eq!(snapshot.active_tx_ids.len(), 1);
        assert!(snapshot.active_tx_ids.contains(&100));
    }
}
