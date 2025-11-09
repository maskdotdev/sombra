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
    /// Number of inserts applied by rewriting the entire leaf
    pub leaf_rebuilds: u64,
    /// Number of inserts applied in-place without rebuilding
    pub leaf_in_place_edits: u64,
    /// Number of leaf rebalances handled in-place.
    pub leaf_rebalance_in_place: u64,
    /// Number of leaf rebalances that rebuilt one or more pages.
    pub leaf_rebalance_rebuilds: u64,
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
    leaf_rebuilds: AtomicU64,
    leaf_in_place_edits: AtomicU64,
    leaf_rebalance_in_place: AtomicU64,
    leaf_rebalance_rebuilds: AtomicU64,
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

    /// Returns the number of inserts that rewrote the entire leaf page.
    pub fn leaf_rebuilds(&self) -> u64 {
        self.leaf_rebuilds.load(AtomicOrdering::Relaxed)
    }

    /// Returns the number of inserts that completed using the in-place path.
    pub fn leaf_in_place_edits(&self) -> u64 {
        self.leaf_in_place_edits.load(AtomicOrdering::Relaxed)
    }

    /// Returns the number of leaf rebalances that succeeded via the in-place fast path.
    pub fn leaf_rebalance_in_place(&self) -> u64 {
        self.leaf_rebalance_in_place.load(AtomicOrdering::Relaxed)
    }

    /// Returns the number of leaf rebalances that rebuilt one or both leaves.
    pub fn leaf_rebalance_rebuilds(&self) -> u64 {
        self.leaf_rebalance_rebuilds.load(AtomicOrdering::Relaxed)
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

    pub(crate) fn inc_leaf_rebuilds(&self) {
        self.leaf_rebuilds.fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub(crate) fn inc_leaf_in_place_edits(&self) {
        self.leaf_in_place_edits
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub(crate) fn inc_leaf_rebalance_in_place(&self) {
        self.leaf_rebalance_in_place
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    pub(crate) fn inc_leaf_rebalance_rebuilds(&self) {
        self.leaf_rebalance_rebuilds
            .fetch_add(1, AtomicOrdering::Relaxed);
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
            leaf_rebuilds: self.leaf_rebuilds(),
            leaf_in_place_edits: self.leaf_in_place_edits(),
            leaf_rebalance_in_place: self.leaf_rebalance_in_place(),
            leaf_rebalance_rebuilds: self.leaf_rebalance_rebuilds(),
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
            leaf_rebuilds = snapshot.leaf_rebuilds,
            leaf_in_place_edits = snapshot.leaf_in_place_edits,
            leaf_rebalance_in_place = snapshot.leaf_rebalance_in_place,
            leaf_rebalance_rebuilds = snapshot.leaf_rebalance_rebuilds,
            "btree stats snapshot"
        );
    }
}
