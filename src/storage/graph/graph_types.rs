use std::time::{Duration, SystemTime};

use crate::primitives::pager::AsyncFsyncBacklog;
use crate::primitives::wal::{WalAllocatorStats, WalCommitBacklog};
use crate::storage::adjacency::Dir;
use crate::storage::mvcc::{CommitId, CommitTableSnapshot};
use crate::storage::types::PropValueOwned;
use crate::types::{Lsn, NodeId, TypeId};

/// Default maximum size for inline property blob storage in bytes.
pub const DEFAULT_INLINE_PROP_BLOB: u32 = 128;
/// Default maximum size for inline property value storage in bytes.
pub const DEFAULT_INLINE_PROP_VALUE: u32 = 48;
/// Storage flag indicating that degree caching is enabled.
pub const STORAGE_FLAG_DEGREE_CACHE: u32 = 0x01;
/// MVCC metrics publish interval.
pub const MVCC_METRICS_PUBLISH_INTERVAL: Duration = Duration::from_millis(500);

/// Options for breadth-first traversal over the graph.
#[derive(Clone, Debug)]
pub struct BfsOptions {
    /// Maximum depth (inclusive) to explore starting from the origin node.
    pub max_depth: u32,
    /// Direction to follow for edge expansions.
    pub direction: Dir,
    /// Optional subset of edge types to consider (matches all when `None`).
    pub edge_types: Option<Vec<TypeId>>,
    /// Optional cap on the number of visited nodes returned (including the origin).
    pub max_results: Option<usize>,
}

impl Default for BfsOptions {
    fn default() -> Self {
        Self {
            max_depth: 1,
            direction: Dir::Out,
            edge_types: None,
            max_results: None,
        }
    }
}

/// Node visit captured during a breadth-first traversal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BfsVisit {
    /// Identifier of the visited node.
    pub node: NodeId,
    /// Depth (distance in hops) from the origin node.
    pub depth: u32,
}

/// Statistics describing a version-log vacuum run.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VersionVacuumStats {
    /// Number of historical versions pruned from the log.
    pub entries_pruned: u64,
}

/// Statistics describing adjacency cleanup.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AdjacencyVacuumStats {
    /// Removed forward adjacency entries.
    pub fwd_entries_pruned: u64,
    /// Removed reverse adjacency entries.
    pub rev_entries_pruned: u64,
}

/// Aggregate statistics for MVCC cleanup across storage components.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphVacuumStats {
    /// Timestamp when the run started.
    pub started_at: SystemTime,
    /// Timestamp when the run completed.
    pub finished_at: SystemTime,
    /// Horizon commit applied for the run.
    pub horizon_commit: CommitId,
    /// Trigger for the run.
    pub trigger: VacuumTrigger,
    /// Duration of the run in milliseconds.
    pub run_millis: u64,
    /// Number of version-log entries examined.
    pub log_versions_examined: u64,
    /// Number of version-log entries pruned.
    pub log_versions_pruned: u64,
    /// Number of orphaned log entries pruned.
    pub orphan_log_versions_pruned: u64,
    /// Number of tombstone heads purged.
    pub heads_purged: u64,
    /// Forward adjacency entries pruned.
    pub adjacency_fwd_pruned: u64,
    /// Reverse adjacency entries pruned.
    pub adjacency_rev_pruned: u64,
    /// Label index entries pruned.
    pub index_label_pruned: u64,
    /// Chunked index segments pruned.
    pub index_chunked_pruned: u64,
    /// B-tree postings pruned.
    pub index_btree_pruned: u64,
    /// Estimated pages read during the pass.
    pub pages_read: u64,
    /// Estimated pages written during the pass.
    pub pages_written: u64,
    /// Estimated bytes reclaimed.
    pub bytes_reclaimed: u64,
}

/// Current MVCC status for observability/CLI tooling.
#[derive(Clone, Debug)]
pub struct GraphMvccStatus {
    /// Bytes retained inside the version log B-tree.
    pub version_log_bytes: u64,
    /// Number of entries currently stored in the version log.
    pub version_log_entries: u64,
    /// Version cache hits accumulated since startup.
    pub version_cache_hits: u64,
    /// Version cache misses accumulated since startup.
    pub version_cache_misses: u64,
    /// Raw bytes processed by the version codec.
    pub version_codec_raw_bytes: u64,
    /// Encoded bytes produced by the version codec.
    pub version_codec_encoded_bytes: u64,
    /// Configured retention window used by vacuum.
    pub retention_window: Duration,
    /// Snapshot of the commit table (if MVCC is enabled).
    pub commit_table: Option<CommitTableSnapshot>,
    /// Latest committed LSN when exposed by the pager.
    pub latest_committed_lsn: Option<Lsn>,
    /// Last durable LSN (async fsync watermark).
    pub durable_lsn: Option<Lsn>,
    /// Number of commits acknowledged but not yet durable.
    pub acked_not_durable_commits: Option<u64>,
    /// Pending WAL commit backlog when available.
    pub wal_backlog: Option<WalCommitBacklog>,
    /// WAL allocator/preallocation snapshot when available.
    pub wal_allocator: Option<WalAllocatorStats>,
    /// Async fsync backlog (pending vs durable) when enabled.
    pub async_fsync_backlog: Option<AsyncFsyncBacklog>,
    /// Snapshot pool occupancy when enabled.
    pub snapshot_pool: Option<SnapshotPoolStatus>,
    /// Current vacuum mode selected by the scheduler.
    pub vacuum_mode: VacuumMode,
    /// Current vacuum horizon, if known.
    pub vacuum_horizon: Option<CommitId>,
    /// WAL/async-fsync alerts derived from current state.
    pub wal_alerts: Vec<String>,
    /// Recommended reuse queue depth based on backlog and segment sizing.
    pub wal_reuse_recommended: Option<u64>,
}

/// Summary of snapshot pooling state.
#[derive(Clone, Debug, Default)]
pub struct SnapshotPoolStatus {
    /// Maximum cached snapshots.
    pub capacity: usize,
    /// Currently cached snapshots.
    pub available: usize,
}

/// Reason a vacuum pass ran.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VacuumTrigger {
    /// Periodic timer.
    Timer,
    /// Version log exceeded configured threshold.
    HighWater,
    /// Explicit manual trigger (tests/CLI).
    Manual,
    /// Opportunistic light pass triggered by readers.
    Opportunistic,
}

/// Adaptive cadence tiers for vacuum scheduling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VacuumMode {
    /// Lowest cadence; used when version log below thresholds.
    Slow,
    /// Default cadence.
    Normal,
    /// Aggressive cadence for backlog/high-water.
    Fast,
}

impl VacuumMode {
    /// Returns the adjusted tick interval for this mode.
    pub fn tick_interval(&self, base: Duration) -> Duration {
        match self {
            VacuumMode::Fast => base
                .checked_div(2)
                .unwrap_or_else(|| Duration::from_millis(1))
                .max(Duration::from_millis(1)),
            VacuumMode::Normal => base,
            VacuumMode::Slow => base.saturating_mul(2),
        }
    }
}

#[derive(Copy, Clone)]
pub(crate) enum RootKind {
    Nodes,
    Edges,
    AdjFwd,
    AdjRev,
    VersionLog,
    #[cfg(feature = "degree-cache")]
    Degree,
}

/// Budget controls for a single vacuum pass.
#[derive(Clone, Debug)]
pub struct VacuumBudget {
    /// Maximum version-log entries per pass.
    pub max_versions: Option<usize>,
    /// Maximum runtime allowed.
    pub max_duration: Duration,
    /// Whether index cleanup should run as part of this pass.
    pub index_cleanup: bool,
}

/// Basic statistics for a (label, property) pair.
#[derive(Clone, Debug, Default)]
pub struct PropStats {
    /// Total number of rows (nodes with the label).
    pub row_count: u64,
    /// Number of rows with a non-null value for the property.
    pub non_null_count: u64,
    /// Number of rows where the property is missing or null.
    pub null_count: u64,
    /// Number of distinct non-null property values.
    pub distinct_count: u64,
    /// Minimum observed non-null property value.
    pub min: Option<PropValueOwned>,
    /// Maximum observed non-null property value.
    pub max: Option<PropValueOwned>,
}
