use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

/// Snapshot of B+ tree statistics at a point in time.
#[derive(Default, Debug, Clone, Copy)]
pub struct BTreeStatsSnapshot {
    /// Number of leaf page searches performed
    pub leaf_searches: u64,
    /// Number of internal page searches performed
    pub internal_searches: u64,
    /// Number of leaf page splits performed
    pub leaf_splits: u64,
    /// Number of internal page splits performed
    pub internal_splits: u64,
    /// Number of leaf page merges performed
    pub leaf_merges: u64,
    /// Number of internal page merges performed
    pub internal_merges: u64,
}

/// Thread-safe statistics tracking for B+ tree operations.
#[derive(Default)]
pub struct BTreeStats {
    leaf_searches: AtomicU64,
    internal_searches: AtomicU64,
    leaf_splits: AtomicU64,
    internal_splits: AtomicU64,
    leaf_merges: AtomicU64,
    internal_merges: AtomicU64,
}

impl BTreeStats {
    /// Returns the current count of leaf page searches.
    pub fn leaf_searches(&self) -> u64 {
        self.leaf_searches.load(AtomicOrdering::Relaxed)
    }

    /// Returns the current count of internal page searches.
    pub fn internal_searches(&self) -> u64 {
        self.internal_searches.load(AtomicOrdering::Relaxed)
    }

    /// Returns the current count of leaf page splits.
    pub fn leaf_splits(&self) -> u64 {
        self.leaf_splits.load(AtomicOrdering::Relaxed)
    }

    /// Returns the current count of internal page splits.
    pub fn internal_splits(&self) -> u64 {
        self.internal_splits.load(AtomicOrdering::Relaxed)
    }

    /// Returns the current count of leaf page merges.
    pub fn leaf_merges(&self) -> u64 {
        self.leaf_merges.load(AtomicOrdering::Relaxed)
    }

    /// Returns the current count of internal page merges.
    pub fn internal_merges(&self) -> u64 {
        self.internal_merges.load(AtomicOrdering::Relaxed)
    }

    pub(crate) fn inc_leaf_searches(&self) {
        self.leaf_searches.fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub(crate) fn inc_internal_searches(&self) {
        self.internal_searches.fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub(crate) fn inc_leaf_splits(&self) {
        self.leaf_splits.fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub(crate) fn inc_internal_splits(&self) {
        self.internal_splits.fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub(crate) fn inc_leaf_merges(&self) {
        self.leaf_merges.fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub(crate) fn inc_internal_merges(&self) {
        self.internal_merges.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Creates a snapshot of all current statistics.
    pub fn snapshot(&self) -> BTreeStatsSnapshot {
        BTreeStatsSnapshot {
            leaf_searches: self.leaf_searches(),
            internal_searches: self.internal_searches(),
            leaf_splits: self.leaf_splits(),
            internal_splits: self.internal_splits(),
            leaf_merges: self.leaf_merges(),
            internal_merges: self.internal_merges(),
        }
    }

    /// Emits current statistics to the tracing infrastructure.
    pub fn emit_tracing(&self) {
        let snapshot = self.snapshot();
        tracing::info!(
            target: "sombra_btree::stats",
            leaf_searches = snapshot.leaf_searches,
            internal_searches = snapshot.internal_searches,
            leaf_splits = snapshot.leaf_splits,
            internal_splits = snapshot.internal_splits,
            leaf_merges = snapshot.leaf_merges,
            internal_merges = snapshot.internal_merges,
            "btree stats snapshot"
        );
    }
}
