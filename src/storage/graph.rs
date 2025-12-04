use std::cell::{Cell, RefCell};
use std::cmp::{Ordering as CmpOrdering, Ordering};
#[cfg(feature = "degree-cache")]
use std::collections::HashMap;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::convert::TryFrom;
use std::num::NonZeroUsize;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex as StdMutex};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant, SystemTime};

use lru::LruCache;
use parking_lot::Mutex;
use tracing::{debug, info, warn};

use crate::primitives::pager::{
    AsyncFsyncBacklog, AutockptContext, BackgroundMaintainer, PageStore, ReadGuard, WriteGuard,
};
use crate::primitives::wal::{WalAllocatorStats, WalCommitBacklog};
use crate::storage::btree::{BTree, BTreeOptions, PutItem, ValCodec};
use crate::storage::index::{
    collect_all, CatalogEpoch, DdlEpoch, GraphIndexCache, GraphIndexCacheStats, IndexDef,
    IndexRoots, IndexStore, IndexVacuumStats, LabelScan, PostingStream, TypeTag,
};
use crate::storage::vstore::VStore;
use crate::storage::{record_mvcc_commit, record_mvcc_read_begin, record_mvcc_write_begin};
use crate::types::{
    EdgeId, LabelId, Lsn, NodeId, PageId, PropId, Result, SombraError, TypeId, VRef,
};

#[cfg(feature = "degree-cache")]
use super::adjacency::DegreeDir;
use super::adjacency::{self, Dir, ExpandOpts, Neighbor, NeighborCursor};
use super::edge::{
    self, EncodeOpts as EdgeEncodeOpts, PropPayload as EdgePropPayload,
    PropStorage as EdgePropStorage,
};
use super::mvcc::{
    CommitId, CommitTable, CommitTableSnapshot, VersionCodecConfig, VersionHeader, VersionLogEntry,
    VersionPtr, VersionSpace, VersionedValue, COMMIT_MAX, VERSION_HEADER_LEN,
};
use super::mvcc_flags;
use super::node::{
    self, EncodeOpts as NodeEncodeOpts, PropPayload as NodePropPayload,
    PropStorage as NodePropStorage,
};
use super::options::{GraphOptions, VacuumCfg};
use super::patch::{PropPatch, PropPatchOp};
use super::profile::{
    profile_timer as storage_profile_timer, profiling_enabled as storage_profiling_enabled,
    record_profile_timer as record_storage_profile_timer, StorageProfileKind,
};
use super::props::{self, RawPropValue};
use super::types::{
    DeleteMode, DeleteNodeOpts, EdgeData, EdgeSpec, NodeData, NodeSpec, PropEntry, PropValue,
    PropValueOwned,
};

/// Default maximum size for inline property blob storage in bytes.
pub const DEFAULT_INLINE_PROP_BLOB: u32 = 128;
/// Default maximum size for inline property value storage in bytes.
pub const DEFAULT_INLINE_PROP_VALUE: u32 = 48;
/// Storage flag indicating that degree caching is enabled.
pub const STORAGE_FLAG_DEGREE_CACHE: u32 = 0x01;
const MVCC_METRICS_PUBLISH_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, Default)]
struct UnitValue;

impl ValCodec for UnitValue {
    fn encode_val(_: &Self, _: &mut Vec<u8>) {}

    fn decode_val(src: &[u8]) -> Result<Self> {
        if src.is_empty() {
            Ok(UnitValue)
        } else {
            Err(SombraError::Corruption("adjacency value payload not empty"))
        }
    }
}

/// Main graph storage structure managing nodes, edges, adjacency lists, and indexes.
#[allow(dead_code)]
pub struct Graph {
    store: Arc<dyn PageStore>,
    commit_table: Option<Arc<Mutex<CommitTable>>>,
    nodes: BTree<u64, Vec<u8>>,
    edges: BTree<u64, Vec<u8>>,
    adj_fwd: BTree<Vec<u8>, VersionedValue<UnitValue>>,
    adj_rev: BTree<Vec<u8>, VersionedValue<UnitValue>>,
    version_log: BTree<u64, Vec<u8>>,
    #[cfg(feature = "degree-cache")]
    degree: Option<BTree<Vec<u8>, u64>>,
    vstore: VStore,
    indexes: IndexStore,
    catalog_epoch: CatalogEpoch,
    inline_prop_blob: usize,
    inline_prop_value: usize,
    #[cfg(feature = "degree-cache")]
    degree_cache_enabled: bool,
    nodes_root: AtomicU64,
    edges_root: AtomicU64,
    adj_fwd_root: AtomicU64,
    adj_rev_root: AtomicU64,
    version_log_root: AtomicU64,
    #[cfg(feature = "degree-cache")]
    degree_root: AtomicU64,
    next_node_id: AtomicU64,
    next_edge_id: AtomicU64,
    next_version_ptr: AtomicU64,
    idx_cache_hits: AtomicU64,
    idx_cache_misses: AtomicU64,
    storage_flags: u32,
    metrics: Arc<dyn super::metrics::StorageMetrics>,
    mvcc_metrics_last: StdMutex<Option<Instant>>,
    distinct_neighbors_default: bool,
    row_hash_header: bool,
    vacuum_cfg: VacuumCfg,
    vacuum_sched: VacuumSched,
    inline_history: bool,
    inline_history_max_bytes: usize,
    defer_adjacency_flush: bool,
    defer_index_flush: bool,
    snapshot_pool: Option<SnapshotPool>,
    version_cache: Option<VersionCache>,
    version_codec_cfg: VersionCodecConfig,
    version_log_bytes: AtomicU64,
    version_log_entries: AtomicU64,
    version_cache_hits: AtomicU64,
    version_cache_misses: AtomicU64,
    version_codec_raw_bytes: AtomicU64,
    version_codec_encoded_bytes: AtomicU64,
    micro_gc_last_ms: AtomicU64,
    micro_gc_budget_hint: AtomicUsize,
    micro_gc_running: AtomicBool,
}

struct VacuumSched {
    running: Cell<bool>,
    next_deadline_ms: Cell<u128>,
    last_stats: RefCell<Option<GraphVacuumStats>>,
    owner_tid: ThreadId,
    pending_trigger: Cell<Option<VacuumTrigger>>,
    mode: Cell<VacuumMode>,
}

impl VacuumSched {
    fn new() -> Self {
        Self {
            running: Cell::new(false),
            next_deadline_ms: Cell::new(0),
            last_stats: RefCell::new(None),
            owner_tid: thread::current().id(),
            pending_trigger: Cell::new(None),
            mode: Cell::new(VacuumMode::Normal),
        }
    }
}

struct VersionCache {
    shards: Vec<Mutex<LruCache<u64, Arc<VersionLogEntry>>>>,
}

impl VersionCache {
    fn new(shards: usize, capacity: usize) -> Self {
        let shard_count = shards.max(1);
        let per_shard_cap = (capacity / shard_count).max(1);
        let mut shard_vec = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shard_vec.push(Mutex::new(LruCache::new(
                NonZeroUsize::new(per_shard_cap).unwrap(),
            )));
        }
        Self { shards: shard_vec }
    }

    fn shard_for(&self, ptr: VersionPtr) -> &Mutex<LruCache<u64, Arc<VersionLogEntry>>> {
        let idx = (ptr.raw() as usize) % self.shards.len();
        &self.shards[idx]
    }

    fn get(&self, ptr: VersionPtr) -> Option<Arc<VersionLogEntry>> {
        let mut guard = self.shard_for(ptr).lock();
        guard.get(&ptr.raw()).cloned()
    }

    fn insert(&self, ptr: VersionPtr, entry: Arc<VersionLogEntry>) {
        let mut guard = self.shard_for(ptr).lock();
        guard.put(ptr.raw(), entry);
    }
}

#[derive(Default)]
struct AdjacencyBuffer {
    inserts: Vec<(NodeId, NodeId, TypeId, EdgeId, CommitId)>,
    removals: Vec<(NodeId, NodeId, TypeId, EdgeId, CommitId)>,
}

#[derive(Default)]
struct IndexBuffer {
    label_inserts: Vec<(LabelId, NodeId, CommitId)>,
    label_removes: Vec<(LabelId, NodeId, CommitId)>,
    prop_inserts: Vec<(IndexDef, Vec<u8>, NodeId, CommitId)>,
    prop_removes: Vec<(IndexDef, Vec<u8>, NodeId, CommitId)>,
}

struct PooledSnapshot {
    guard: ReadGuard,
    acquired_at: Instant,
}

struct SnapshotPool {
    store: Arc<dyn PageStore>,
    metrics: Arc<dyn super::metrics::StorageMetrics>,
    retention: Duration,
    inner: Mutex<Vec<PooledSnapshot>>,
    capacity: usize,
}

impl SnapshotPool {
    fn new(
        store: Arc<dyn PageStore>,
        metrics: Arc<dyn super::metrics::StorageMetrics>,
        capacity: usize,
        retention: Duration,
    ) -> Self {
        Self {
            store,
            metrics,
            retention,
            inner: Mutex::new(Vec::new()),
            capacity: capacity.max(1),
        }
    }

    fn lease(&self) -> Result<SnapshotLease<'_>> {
        let now = Instant::now();
        let durable = self.store.durable_lsn().map(|lsn| lsn.0).unwrap_or(0);
        let mut pool = self.inner.lock();
        while let Some(snapshot) = pool.pop() {
            let age = now.saturating_duration_since(snapshot.acquired_at);
            if age > self.retention {
                continue;
            }
            if durable > snapshot.guard.snapshot_lsn().0 {
                continue;
            }
            self.metrics.snapshot_pool_hit();
            return Ok(SnapshotLease {
                pool: Some(self),
                snapshot: Some(snapshot),
            });
        }
        drop(pool);
        self.metrics.snapshot_pool_miss();
        let guard = self.store.begin_read()?;
        Ok(SnapshotLease {
            pool: Some(self),
            snapshot: Some(PooledSnapshot {
                guard,
                acquired_at: now,
            }),
        })
    }

    fn return_snapshot(&self, snapshot: PooledSnapshot) {
        let now = Instant::now();
        let durable = self.store.durable_lsn().map(|lsn| lsn.0).unwrap_or(0);
        if now.saturating_duration_since(snapshot.acquired_at) > self.retention {
            return;
        }
        if durable > snapshot.guard.snapshot_lsn().0 {
            return;
        }
        let mut pool = self.inner.lock();
        if pool.len() >= self.capacity {
            return;
        }
        pool.push(snapshot);
    }

    fn status(&self) -> SnapshotPoolStatus {
        let pool = self.inner.lock();
        SnapshotPoolStatus {
            capacity: self.capacity,
            available: pool.len(),
        }
    }
}

struct SnapshotLease<'a> {
    pool: Option<&'a SnapshotPool>,
    snapshot: Option<PooledSnapshot>,
}

impl<'a> SnapshotLease<'a> {
    fn inner(&self) -> &ReadGuard {
        &self.snapshot.as_ref().expect("snapshot present").guard
    }

    fn direct(guard: ReadGuard) -> Self {
        Self {
            pool: None,
            snapshot: Some(PooledSnapshot {
                guard,
                acquired_at: Instant::now(),
            }),
        }
    }
}

impl<'a> std::ops::Deref for SnapshotLease<'a> {
    type Target = ReadGuard;

    fn deref(&self) -> &Self::Target {
        self.inner()
    }
}

impl<'a> Drop for SnapshotLease<'a> {
    fn drop(&mut self) {
        if let (Some(pool), Some(snapshot)) = (self.pool, self.snapshot.take()) {
            pool.return_snapshot(snapshot);
        }
    }
}

trait VersionChainRecord {
    fn header(&self) -> &VersionHeader;
    fn prev_ptr(&self) -> VersionPtr;
    fn inline_history(&self) -> Option<&[u8]>;
}

impl VersionChainRecord for node::VersionedNodeRow {
    fn header(&self) -> &VersionHeader {
        &self.header
    }

    fn prev_ptr(&self) -> VersionPtr {
        self.prev_ptr
    }

    fn inline_history(&self) -> Option<&[u8]> {
        self.inline_history.as_deref()
    }
}

impl VersionChainRecord for edge::VersionedEdgeRow {
    fn header(&self) -> &VersionHeader {
        &self.header
    }

    fn prev_ptr(&self) -> VersionPtr {
        self.prev_ptr
    }

    fn inline_history(&self) -> Option<&[u8]> {
        self.inline_history.as_deref()
    }
}

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

fn wal_health(
    page_size: u32,
    wal_backlog: Option<&WalCommitBacklog>,
    wal_allocator: Option<&WalAllocatorStats>,
    async_fsync: Option<&AsyncFsyncBacklog>,
    vacuum_horizon: Option<CommitId>,
    durable_lsn: Option<Lsn>,
) -> (Vec<String>, Option<u64>) {
    let mut alerts = Vec::new();
    let mut recommended_reuse: Option<u64> = None;
    if let Some(backlog) = wal_backlog {
        if backlog.pending_commits > WAL_BACKLOG_COMMITS_ALERT
            || backlog.pending_frames > WAL_BACKLOG_FRAMES_ALERT
        {
            alerts.push(format!(
                "wal backlog high ({} commits, {} frames)",
                backlog.pending_commits, backlog.pending_frames
            ));
        }
    }
    if let Some(async_status) = async_fsync {
        if async_status.pending_lag > ASYNC_FSYNC_LAG_ALERT {
            alerts.push(format!(
                "async fsync lagging by {} LSN from durable cookie",
                async_status.pending_lag
            ));
        }
        if let Some(err) = async_status.last_error.as_ref() {
            alerts.push(format!("async fsync error: {err}"));
        }
    }
    if let Some(allocator) = wal_allocator {
        if let Some(err) = allocator.allocation_error.as_ref() {
            alerts.push(format!("wal allocation error: {err}"));
        }
        if allocator.segment_size_bytes > 0 && page_size > 0 {
            let frames_per_segment = allocator.segment_size_bytes / page_size as u64;
            if frames_per_segment > 0 {
                if let Some(backlog) = wal_backlog {
                    let needed_segments =
                        (backlog.pending_frames as u64).div_ceil(frames_per_segment);
                    let readyish =
                        allocator.ready_segments as u64 + allocator.recycle_segments as u64;
                    if needed_segments > readyish {
                        alerts.push(format!(
                            "wal reuse queue short by {} segments (need ~{}, ready {}, recycle {}, target {})",
                            needed_segments.saturating_sub(readyish),
                            needed_segments,
                            allocator.ready_segments,
                            allocator.recycle_segments,
                            allocator.preallocate_segments
                        ));
                        recommended_reuse =
                            Some(needed_segments.max(allocator.preallocate_segments as u64));
                    } else if allocator.preallocate_segments as u64 > readyish {
                        recommended_reuse = Some(allocator.preallocate_segments as u64);
                    }
                }
            }
        }
    }
    if let (Some(horizon), Some(durable)) = (vacuum_horizon, durable_lsn) {
        if horizon != COMMIT_MAX {
            let lag = durable.0.saturating_sub(horizon);
            if lag > WAL_HORIZON_LAG_ALERT {
                alerts.push(format!(
                    "vacuum horizon lags durable LSN by {lag} commits; WAL reuse may be pinned by readers"
                ));
            }
        }
    }
    (alerts, recommended_reuse)
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
    fn tick_interval(&self, base: Duration) -> Duration {
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

const MICRO_GC_MAX_BUDGET: usize = 64;
const WAL_BACKLOG_COMMITS_ALERT: usize = 8;
const WAL_BACKLOG_FRAMES_ALERT: usize = 2_048;
const ASYNC_FSYNC_LAG_ALERT: u64 = 4_096;
const WAL_HORIZON_LAG_ALERT: u64 = 32_768;

#[derive(Copy, Clone)]
enum MicroGcTrigger {
    ReadPath,
    CacheMiss,
    PostCommit,
}

impl MicroGcTrigger {
    fn budget(self) -> usize {
        match self {
            MicroGcTrigger::ReadPath => 8,
            MicroGcTrigger::CacheMiss => 16,
            MicroGcTrigger::PostCommit => 64,
        }
    }

    fn cooldown_ms(self) -> u64 {
        match self {
            MicroGcTrigger::ReadPath => 25,
            MicroGcTrigger::CacheMiss => 10,
            MicroGcTrigger::PostCommit => 5,
        }
    }
}

enum MicroGcOutcome {
    Done,
    Retry,
    Skip,
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

#[derive(Copy, Clone)]
enum RootKind {
    Nodes,
    Edges,
    AdjFwd,
    AdjRev,
    VersionLog,
    #[cfg(feature = "degree-cache")]
    Degree,
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

impl Graph {
    fn overwrite_encoded_header(bytes: &mut [u8], header: &VersionHeader) {
        let encoded = header.encode();
        bytes[..VERSION_HEADER_LEN].copy_from_slice(&encoded);
    }

    fn maybe_inline_history(&self, bytes: &[u8]) -> Option<Vec<u8>> {
        if !self.inline_history {
            return None;
        }
        if bytes.len() > self.inline_history_max_bytes {
            return None;
        }
        Some(bytes.to_vec())
    }

    fn request_micro_gc(&self, trigger: MicroGcTrigger) {
        if !self.vacuum_cfg.enabled {
            return;
        }
        let budget = trigger.budget().min(MICRO_GC_MAX_BUDGET);
        if budget == 0 {
            return;
        }
        self.micro_gc_budget_hint
            .fetch_max(budget, AtomicOrdering::Relaxed);
    }

    fn drive_micro_gc(&self, trigger: MicroGcTrigger) {
        if !self.vacuum_cfg.enabled {
            return;
        }
        self.request_micro_gc(trigger);
        let pending_budget = self.micro_gc_budget_hint.swap(0, AtomicOrdering::Relaxed);
        if pending_budget == 0 {
            return;
        }
        let budget = pending_budget.min(MICRO_GC_MAX_BUDGET);
        let now_ms = now_millis();
        let now_ms_u64 = now_ms.min(u64::MAX as u128) as u64;
        let last = self.micro_gc_last_ms.load(AtomicOrdering::Relaxed);
        if now_ms.saturating_sub(u128::from(last)) < u128::from(trigger.cooldown_ms()) {
            self.micro_gc_budget_hint
                .fetch_max(budget, AtomicOrdering::Relaxed);
            return;
        }
        if self
            .micro_gc_running
            .compare_exchange(
                false,
                true,
                AtomicOrdering::Acquire,
                AtomicOrdering::Relaxed,
            )
            .is_err()
        {
            self.micro_gc_budget_hint
                .fetch_max(budget, AtomicOrdering::Relaxed);
            return;
        }
        let outcome = match self.compute_vacuum_horizon() {
            Some(horizon) if horizon != COMMIT_MAX => match self.micro_gc(horizon, budget) {
                Ok(Some(_)) => MicroGcOutcome::Done,
                Ok(None) => MicroGcOutcome::Retry,
                Err(err) => {
                    debug!(error = %err, "graph.micro_gc.error");
                    MicroGcOutcome::Retry
                }
            },
            _ => MicroGcOutcome::Skip,
        };
        self.micro_gc_last_ms
            .store(now_ms_u64, AtomicOrdering::Relaxed);
        self.micro_gc_running.store(false, AtomicOrdering::Release);
        if matches!(outcome, MicroGcOutcome::Retry) {
            self.micro_gc_budget_hint
                .fetch_max(budget, AtomicOrdering::Relaxed);
        }
    }

    fn lease_latest_snapshot(&self) -> Result<SnapshotLease<'_>> {
        self.drive_micro_gc(MicroGcTrigger::ReadPath);
        if let Some(pool) = &self.snapshot_pool {
            pool.lease()
        } else {
            let guard = self.begin_read_guard()?;
            Ok(SnapshotLease::direct(guard))
        }
    }

    fn log_version_entry(
        &self,
        tx: &mut WriteGuard<'_>,
        space: VersionSpace,
        id: u64,
        header: VersionHeader,
        prev_ptr: VersionPtr,
        bytes: Vec<u8>,
    ) -> Result<VersionPtr> {
        let ptr_value = self.next_version_ptr.fetch_add(1, AtomicOrdering::SeqCst);
        if ptr_value == 0 {
            return Err(SombraError::Corruption("version log pointer overflowed"));
        }
        let codec_outcome = self.version_codec_cfg.apply_owned(bytes)?;
        let raw_len = u32::try_from(codec_outcome.raw_len)
            .map_err(|_| SombraError::Invalid("version log payload too large"))?;
        let entry = VersionLogEntry {
            space,
            id,
            header,
            prev_ptr,
            codec: codec_outcome.codec,
            raw_len,
            bytes: codec_outcome.encoded,
        };
        let encoded = entry.encode()?;
        self.metrics.version_codec_bytes(
            entry.codec.as_str(),
            u64::from(raw_len),
            encoded.len() as u64,
        );
        self.version_codec_raw_bytes
            .fetch_add(u64::from(raw_len), AtomicOrdering::Relaxed);
        self.version_codec_encoded_bytes
            .fetch_add(encoded.len() as u64, AtomicOrdering::Relaxed);
        if let Some(cache) = &self.version_cache {
            cache.insert(VersionPtr::from_raw(ptr_value), Arc::new(entry.clone()));
        }
        self.version_log.put(tx, &ptr_value, &encoded)?;
        self.version_log_bytes
            .fetch_add(encoded.len() as u64, AtomicOrdering::Relaxed);
        self.version_log_entries
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.publish_version_log_usage_metrics();
        self.maybe_signal_high_water();
        self.persist_tree_root(tx, RootKind::VersionLog)?;
        let next_ptr = ptr_value
            .checked_add(1)
            .ok_or(SombraError::Corruption("version log pointer overflowed"))?;
        tx.update_meta(|meta| {
            if meta.storage_next_version_ptr <= ptr_value {
                meta.storage_next_version_ptr = next_ptr;
            }
        })?;
        Ok(VersionPtr::from_raw(ptr_value))
    }

    fn load_version_entry(
        &self,
        tx: &ReadGuard,
        ptr: VersionPtr,
    ) -> Result<Option<VersionLogEntry>> {
        if ptr.is_null() {
            return Ok(None);
        }
        if let Some(cache) = &self.version_cache {
            if let Some(hit) = cache.get(ptr) {
                self.metrics.version_cache_hit();
                self.version_cache_hits
                    .fetch_add(1, AtomicOrdering::Relaxed);
                return Ok(Some((*hit).clone()));
            }
        }
        if self.version_cache.is_some() {
            self.request_micro_gc(MicroGcTrigger::CacheMiss);
        }
        let Some(bytes) = self.version_log.get(tx, &ptr.raw())? else {
            return Ok(None);
        };
        let decoded = VersionLogEntry::decode(&bytes)?;
        if let Some(cache) = &self.version_cache {
            cache.insert(ptr, Arc::new(decoded.clone()));
            self.metrics.version_cache_miss();
            self.version_cache_misses
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        Ok(Some(decoded))
    }

    /// Returns the commit table when the underlying pager provides one.
    pub fn commit_table(&self) -> Option<Arc<Mutex<CommitTable>>> {
        self.commit_table.as_ref().map(Arc::clone)
    }

    fn version_log_bytes(&self) -> u64 {
        self.version_log_bytes.load(AtomicOrdering::Relaxed)
    }

    fn version_log_entry_count(&self) -> u64 {
        self.version_log_entries.load(AtomicOrdering::Relaxed)
    }

    fn publish_version_log_usage_metrics(&self) {
        self.metrics
            .version_log_usage(self.version_log_bytes(), self.version_log_entry_count());
    }

    /// Returns the oldest reader commit currently pinned by any snapshot.
    pub fn oldest_reader_commit(&self) -> Option<CommitId> {
        let table = self.commit_table.as_ref()?;
        let snapshot = table.lock().reader_snapshot(Instant::now());
        snapshot.oldest_snapshot
    }

    #[inline]
    fn begin_read_guard(&self) -> Result<ReadGuard> {
        let start = Instant::now();
        let guard = self.store.begin_read()?;
        let nanos = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.metrics.mvcc_read_latency_ns(nanos);
        record_mvcc_read_begin(nanos);
        Ok(guard)
    }

    #[inline]
    fn begin_write_guard(&self) -> Result<WriteGuard<'_>> {
        let start = Instant::now();
        let guard = self.store.begin_write()?;
        let nanos = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.metrics.mvcc_write_latency_ns(nanos);
        record_mvcc_write_begin(nanos);
        Ok(guard)
    }

    #[inline]
    fn commit_with_metrics(&self, write: WriteGuard<'_>) -> Result<Lsn> {
        let start = Instant::now();
        let lsn = self.store.commit(write)?;
        let nanos = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.metrics.mvcc_commit_latency_ns(nanos);
        record_mvcc_commit(nanos);
        Ok(lsn)
    }

    /// Returns the configured retention window for MVCC vacuum.
    pub fn vacuum_retention_window(&self) -> Duration {
        self.vacuum_cfg.retention_window
    }

    /// Returns MVCC-related diagnostics for the graph.
    pub fn mvcc_status(&self) -> GraphMvccStatus {
        let commit_table = self
            .commit_table
            .as_ref()
            .map(|table| table.lock().snapshot(Instant::now()));
        let latest_committed_lsn = self.store.latest_committed_lsn();
        let durable_lsn = self.store.durable_lsn();
        let wal_backlog = self.store.wal_commit_backlog();
        let wal_allocator = self.store.wal_allocator_stats();
        let async_fsync_backlog = self.store.async_fsync_backlog();
        let snapshot_pool = self.snapshot_pool.as_ref().map(|pool| pool.status());
        let vacuum_horizon = self.compute_vacuum_horizon();
        let vacuum_mode = self.select_vacuum_mode();
        let version_cache_hits = self.version_cache_hits.load(AtomicOrdering::Relaxed);
        let version_cache_misses = self.version_cache_misses.load(AtomicOrdering::Relaxed);
        let version_codec_raw_bytes = self.version_codec_raw_bytes.load(AtomicOrdering::Relaxed);
        let version_codec_encoded_bytes = self
            .version_codec_encoded_bytes
            .load(AtomicOrdering::Relaxed);
        let acked_not_durable_commits = commit_table
            .as_ref()
            .map(|snapshot| snapshot.acked_not_durable)
            .or_else(|| match (latest_committed_lsn, durable_lsn) {
                (Some(latest), Some(durable)) => Some(latest.0.saturating_sub(durable.0)),
                _ => None,
            });
        if let Some(backlog) = acked_not_durable_commits {
            self.metrics.mvcc_commit_backlog(backlog);
        }
        let (wal_alerts, wal_reuse_recommended) = wal_health(
            self.store.page_size(),
            wal_backlog.as_ref(),
            wal_allocator.as_ref(),
            async_fsync_backlog.as_ref(),
            vacuum_horizon,
            durable_lsn,
        );
        GraphMvccStatus {
            version_log_bytes: self.version_log_bytes(),
            version_log_entries: self.version_log_entry_count(),
            version_cache_hits,
            version_cache_misses,
            version_codec_raw_bytes,
            version_codec_encoded_bytes,
            retention_window: self.vacuum_cfg.retention_window,
            commit_table,
            latest_committed_lsn,
            durable_lsn,
            acked_not_durable_commits,
            wal_backlog,
            wal_allocator,
            async_fsync_backlog,
            snapshot_pool,
            vacuum_mode,
            vacuum_horizon,
            wal_alerts,
            wal_reuse_recommended,
        }
    }

    /// Removes historical versions whose visibility ended at or before `horizon`.
    ///
    /// The optional `limit` bounds how many entries are deleted in a single invocation.
    pub fn vacuum_version_log(
        &self,
        horizon: CommitId,
        limit: Option<usize>,
    ) -> Result<VersionVacuumStats> {
        let mut write = self.begin_write_guard()?;
        let stats = self.vacuum_version_log_with_write(&mut write, horizon, limit)?;
        self.commit_with_metrics(write)?;
        Ok(stats)
    }

    /// Removes adjacency entries whose visibility ended before `horizon`.
    pub fn vacuum_adjacency(&self, horizon: CommitId) -> Result<AdjacencyVacuumStats> {
        let mut write = self.begin_write_guard()?;
        let fwd = Self::prune_versioned_vec_tree(&self.adj_fwd, &mut write, horizon)?;
        let rev = Self::prune_versioned_vec_tree(&self.adj_rev, &mut write, horizon)?;
        self.commit_with_metrics(write)?;
        Ok(AdjacencyVacuumStats {
            fwd_entries_pruned: fwd,
            rev_entries_pruned: rev,
        })
    }

    /// Removes index entries whose visibility ended before `horizon`.
    pub fn vacuum_indexes(&self, horizon: CommitId) -> Result<IndexVacuumStats> {
        let mut write = self.begin_write_guard()?;
        let stats = self.indexes.vacuum(&mut write, horizon)?;
        self.commit_with_metrics(write)?;
        Ok(stats)
    }

    /// Returns the most recent vacuum statistics.
    pub fn last_vacuum_stats(&self) -> Option<GraphVacuumStats> {
        self.vacuum_sched.last_stats.borrow().clone()
    }

    /// Requests an immediate vacuum pass (primarily for tests).
    pub fn trigger_vacuum(&self) {
        self.vacuum_sched
            .pending_trigger
            .set(Some(VacuumTrigger::Manual));
        self.vacuum_sched.next_deadline_ms.set(0);
        self.maybe_background_vacuum(VacuumTrigger::Manual);
    }

    /// Opportunistically trims expired versions without scheduling full vacuum.
    pub fn micro_gc(
        &self,
        horizon: CommitId,
        max_entries: usize,
    ) -> Result<Option<VersionVacuumStats>> {
        if max_entries == 0 {
            return Ok(None);
        }
        if self.vacuum_sched.running.get() {
            return Ok(None);
        }
        let mut write = self.begin_write_guard()?;
        let stats = self.vacuum_version_log_with_write(&mut write, horizon, Some(max_entries))?;
        if stats.entries_pruned > 0 {
            self.metrics.mvcc_micro_gc_trim(stats.entries_pruned, 0);
        }
        self.commit_with_metrics(write)?;
        Ok(Some(stats))
    }

    fn maybe_background_vacuum(&self, default_trigger: VacuumTrigger) {
        debug_assert_eq!(
            thread::current().id(),
            self.vacuum_sched.owner_tid,
            "vacuum invoked from unexpected thread"
        );
        if !self.vacuum_cfg.enabled {
            return;
        }
        if self.vacuum_sched.running.get() {
            return;
        }
        let now_ms = now_millis();
        let next_deadline = self.vacuum_sched.next_deadline_ms.get();
        let pending = self.vacuum_sched.pending_trigger.get();
        let mode = self.select_vacuum_mode();
        self.vacuum_sched.mode.set(mode);
        let high_water_triggered = self.vacuum_cfg.log_high_water_bytes > 0
            && self.version_log_bytes() >= self.vacuum_cfg.log_high_water_bytes;
        let opportunistic_trigger = self.vacuum_cfg.log_high_water_bytes > 0
            && self.vacuum_sched.pending_trigger.get().is_none()
            && self.version_log_bytes() >= (self.vacuum_cfg.log_high_water_bytes / 2).max(1);
        if opportunistic_trigger {
            self.vacuum_sched
                .pending_trigger
                .set(Some(VacuumTrigger::Opportunistic));
        }
        if high_water_triggered && pending.is_none() {
            self.vacuum_sched
                .pending_trigger
                .set(Some(VacuumTrigger::HighWater));
        }
        let pending_trigger = self.vacuum_sched.pending_trigger.get();
        if pending_trigger.is_none()
            && !high_water_triggered
            && default_trigger != VacuumTrigger::Manual
        {
            let interval = mode.tick_interval(self.vacuum_cfg.interval);
            let interval_ms = interval.as_millis().max(1);
            if next_deadline != 0 && now_ms < next_deadline {
                return;
            }
            if next_deadline == 0 {
                self.vacuum_sched
                    .next_deadline_ms
                    .set(now_ms.saturating_add(interval_ms));
                return;
            }
        }
        if self.vacuum_sched.running.replace(true) {
            return;
        }
        let trigger =
            self.vacuum_sched
                .pending_trigger
                .replace(None)
                .unwrap_or(if high_water_triggered {
                    VacuumTrigger::HighWater
                } else {
                    default_trigger
                });
        let Some(horizon) = self.compute_vacuum_horizon() else {
            self.vacuum_sched.running.set(false);
            return;
        };
        let budget = VacuumBudget {
            max_versions: if self.vacuum_cfg.max_pages_per_pass == 0 {
                None
            } else if matches!(trigger, VacuumTrigger::Opportunistic) {
                Some(self.vacuum_cfg.max_pages_per_pass.min(16))
            } else {
                Some(self.vacuum_cfg.max_pages_per_pass)
            },
            max_duration: if matches!(trigger, VacuumTrigger::Opportunistic) {
                Duration::from_millis(self.vacuum_cfg.max_millis_per_pass.max(1) / 2 + 1)
            } else {
                Duration::from_millis(self.vacuum_cfg.max_millis_per_pass.max(1))
            },
            index_cleanup: self.vacuum_cfg.index_cleanup
                && !matches!(trigger, VacuumTrigger::Opportunistic),
        };
        let result = self.vacuum_mvcc(horizon, None, trigger, Some(&budget));
        match result {
            Ok(stats) => {
                self.record_vacuum_stats(stats);
            }
            Err(err) => {
                warn!(error = %err, "graph.vacuum.failed");
            }
        }
        let interval_ms = mode
            .tick_interval(self.vacuum_cfg.interval)
            .as_millis()
            .max(1);
        self.vacuum_sched
            .next_deadline_ms
            .set(now_ms.saturating_add(interval_ms));
        self.vacuum_sched.running.set(false);
    }

    /// Runs MVCC cleanup across versions, adjacency, and indexes.
    pub fn vacuum_mvcc(
        &self,
        horizon: CommitId,
        limit: Option<usize>,
        trigger: VacuumTrigger,
        budget: Option<&VacuumBudget>,
    ) -> Result<GraphVacuumStats> {
        let started_at = SystemTime::now();
        let bytes_before = self.version_log_bytes();
        let version_limit = limit.or_else(|| budget.and_then(|b| b.max_versions));
        let version_stats = self.vacuum_version_log(horizon, version_limit)?;
        let mut adjacency_stats = AdjacencyVacuumStats::default();
        let mut index_stats = IndexVacuumStats::default();
        if budget.map(|b| b.index_cleanup).unwrap_or(true) {
            adjacency_stats = self.vacuum_adjacency(horizon)?;
            index_stats = self.vacuum_indexes(horizon)?;
        }
        let finished_at = SystemTime::now();
        let bytes_after = self.version_log_bytes();
        let run_millis = finished_at
            .duration_since(started_at)
            .map(|dur| dur.as_millis() as u64)
            .unwrap_or(0);
        Ok(GraphVacuumStats {
            started_at,
            finished_at,
            horizon_commit: horizon,
            trigger,
            run_millis,
            log_versions_examined: version_stats.entries_pruned,
            log_versions_pruned: version_stats.entries_pruned,
            orphan_log_versions_pruned: version_stats.entries_pruned,
            heads_purged: 0,
            adjacency_fwd_pruned: adjacency_stats.fwd_entries_pruned,
            adjacency_rev_pruned: adjacency_stats.rev_entries_pruned,
            index_label_pruned: index_stats.label_entries_pruned,
            index_chunked_pruned: index_stats.chunked_segments_pruned,
            index_btree_pruned: index_stats.btree_entries_pruned,
            pages_read: 0,
            pages_written: 0,
            bytes_reclaimed: bytes_before.saturating_sub(bytes_after),
        })
    }

    fn vacuum_version_log_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        horizon: CommitId,
        limit: Option<usize>,
    ) -> Result<VersionVacuumStats> {
        let max_prune = limit.unwrap_or(usize::MAX);
        if max_prune == 0 {
            return Ok(VersionVacuumStats::default());
        }
        let mut to_delete = Vec::new();
        let mut retired = Vec::new();
        let mut retired_sizes = Vec::new();
        self.version_log.for_each_with_write(tx, |key, bytes| {
            if to_delete.len() >= max_prune {
                return Ok(());
            }
            let entry = VersionLogEntry::decode(&bytes)?;
            if entry.header.end != COMMIT_MAX && entry.header.end <= horizon {
                to_delete.push(key);
                retired.push(entry);
                retired_sizes.push(bytes.len() as u64);
            }
            Ok(())
        })?;
        let mut bytes_removed = 0u64;
        for entry in &retired {
            self.retire_version_resources(tx, entry)?;
        }
        for encoded_len in retired_sizes {
            bytes_removed = bytes_removed.saturating_add(encoded_len);
        }
        for key in &to_delete {
            let _ = self.version_log.delete(tx, key)?;
        }
        if !to_delete.is_empty() {
            self.persist_tree_root(tx, RootKind::VersionLog)?;
        }
        if bytes_removed > 0 {
            self.version_log_bytes
                .fetch_sub(bytes_removed, AtomicOrdering::Relaxed);
        }
        if !to_delete.is_empty() {
            self.version_log_entries
                .fetch_sub(to_delete.len() as u64, AtomicOrdering::Relaxed);
        }
        if bytes_removed > 0 || !to_delete.is_empty() {
            self.publish_version_log_usage_metrics();
        }
        self.vstore.flush_deferred(tx)?;
        if !to_delete.is_empty() {
            self.metrics.mvcc_micro_gc_trim(retired.len() as u64, 0);
        }
        Ok(VersionVacuumStats {
            entries_pruned: to_delete.len() as u64,
        })
    }

    /// Opens a graph storage instance with the specified configuration options.
    pub fn open(opts: GraphOptions) -> Result<Arc<Self>> {
        let store = Arc::clone(&opts.store);
        let meta = store.meta()?;

        let inline_blob_u32 = opts.inline_prop_blob.unwrap_or({
            if meta.storage_inline_prop_blob == 0 {
                DEFAULT_INLINE_PROP_BLOB
            } else {
                meta.storage_inline_prop_blob
            }
        });
        let inline_value_u32 = opts.inline_prop_value.unwrap_or({
            if meta.storage_inline_prop_value == 0 {
                DEFAULT_INLINE_PROP_VALUE
            } else {
                meta.storage_inline_prop_value
            }
        });
        let inline_prop_blob = inline_blob_u32 as usize;
        let inline_prop_value = inline_value_u32 as usize;

        let nodes = open_u64_vec_tree(&store, meta.storage_nodes_root)?;
        let edges = open_u64_vec_tree(&store, meta.storage_edges_root)?;
        let adj_fwd = open_unit_tree(&store, meta.storage_adj_fwd_root)?;
        let adj_rev = open_unit_tree(&store, meta.storage_adj_rev_root)?;
        let version_log = open_u64_vec_tree(&store, meta.storage_version_log_root)?;
        let index_roots = IndexRoots {
            catalog: meta.storage_index_catalog_root,
            label: meta.storage_label_index_root,
            prop_chunk: meta.storage_prop_chunk_root,
            prop_btree: meta.storage_prop_btree_root,
        };
        let (indexes, index_roots_actual) = IndexStore::open(Arc::clone(&store), index_roots)?;

        #[cfg(feature = "degree-cache")]
        let degree_cache_enabled = opts.degree_cache
            || (meta.storage_flags & STORAGE_FLAG_DEGREE_CACHE) != 0
            || meta.storage_degree_root.0 != 0;
        #[cfg(not(feature = "degree-cache"))]
        let _degree_cache_enabled = false;

        #[cfg(feature = "degree-cache")]
        let degree_tree = if degree_cache_enabled || meta.storage_degree_root.0 != 0 {
            let tree = open_degree_tree(&store, meta.storage_degree_root)?;
            Some(tree)
        } else {
            None
        };
        #[cfg(not(feature = "degree-cache"))]
        let _degree_tree: Option<BTree<Vec<u8>, u64>> = None;

        let nodes_root = nodes.root_page();
        let edges_root = edges.root_page();
        let adj_fwd_root = adj_fwd.root_page();
        let adj_rev_root = adj_rev.root_page();
        let version_log_root = version_log.root_page();
        let index_catalog_root = index_roots_actual.catalog;
        let index_label_root = index_roots_actual.label;
        let index_prop_chunk_root = index_roots_actual.prop_chunk;
        let index_prop_btree_root = index_roots_actual.prop_btree;
        #[cfg(feature = "degree-cache")]
        let degree_root = degree_tree
            .as_ref()
            .map(|tree| tree.root_page())
            .unwrap_or(PageId(0));
        #[cfg(not(feature = "degree-cache"))]
        let _degree_root = PageId(0);

        let mut storage_flags = meta.storage_flags;
        #[cfg(feature = "degree-cache")]
        {
            if degree_cache_enabled {
                storage_flags |= STORAGE_FLAG_DEGREE_CACHE;
            } else {
                storage_flags &= !STORAGE_FLAG_DEGREE_CACHE;
            }
        }
        #[cfg(not(feature = "degree-cache"))]
        {
            storage_flags &= !STORAGE_FLAG_DEGREE_CACHE;
        }

        let inline_blob_meta = u32::try_from(inline_prop_blob)
            .map_err(|_| SombraError::Invalid("inline_prop_blob exceeds u32::MAX"))?;
        let inline_value_meta = u32::try_from(inline_prop_value)
            .map_err(|_| SombraError::Invalid("inline_prop_value exceeds u32::MAX"))?;
        let next_node_id_init = meta.storage_next_node_id.max(1);
        let next_edge_id_init = meta.storage_next_edge_id.max(1);
        let next_version_ptr_init = meta.storage_next_version_ptr.max(1);

        let mut meta_update_needed = false;
        if storage_flags != meta.storage_flags {
            meta_update_needed = true;
        }
        if nodes_root != meta.storage_nodes_root
            || edges_root != meta.storage_edges_root
            || adj_fwd_root != meta.storage_adj_fwd_root
            || adj_rev_root != meta.storage_adj_rev_root
            || version_log_root != meta.storage_version_log_root
            || index_catalog_root != meta.storage_index_catalog_root
            || index_label_root != meta.storage_label_index_root
            || index_prop_chunk_root != meta.storage_prop_chunk_root
            || index_prop_btree_root != meta.storage_prop_btree_root
        {
            meta_update_needed = true;
        }
        if inline_blob_meta != meta.storage_inline_prop_blob
            || inline_value_meta != meta.storage_inline_prop_value
        {
            meta_update_needed = true;
        }
        if meta.storage_next_node_id != next_node_id_init
            || meta.storage_next_edge_id != next_edge_id_init
            || meta.storage_next_version_ptr != next_version_ptr_init
        {
            meta_update_needed = true;
        }
        #[cfg(feature = "degree-cache")]
        {
            if degree_root != meta.storage_degree_root {
                meta_update_needed = true;
            }
        }
        #[cfg(not(feature = "degree-cache"))]
        {
            if meta.storage_degree_root.0 != 0 {
                meta_update_needed = true;
            }
        }

        if meta_update_needed {
            let mut write = store.begin_write()?;
            write.update_meta(|meta| {
                meta.storage_flags = storage_flags;
                meta.storage_nodes_root = nodes_root;
                meta.storage_edges_root = edges_root;
                meta.storage_adj_fwd_root = adj_fwd_root;
                meta.storage_adj_rev_root = adj_rev_root;
                meta.storage_version_log_root = version_log_root;
                meta.storage_index_catalog_root = index_catalog_root;
                meta.storage_label_index_root = index_label_root;
                meta.storage_prop_chunk_root = index_prop_chunk_root;
                meta.storage_prop_btree_root = index_prop_btree_root;
                #[cfg(feature = "degree-cache")]
                {
                    meta.storage_degree_root = degree_root;
                }
                #[cfg(not(feature = "degree-cache"))]
                {
                    meta.storage_degree_root = PageId(0);
                }
                meta.storage_next_node_id = next_node_id_init;
                meta.storage_next_edge_id = next_edge_id_init;
                meta.storage_next_version_ptr = next_version_ptr_init;
                meta.storage_inline_prop_blob = inline_blob_meta;
                meta.storage_inline_prop_value = inline_value_meta;
            })?;
            store.commit(write)?;
        }

        let vstore = VStore::open(Arc::clone(&store))?;
        let catalog_epoch = CatalogEpoch::new(DdlEpoch(meta.storage_ddl_epoch));
        let next_node_id = AtomicU64::new(next_node_id_init);
        let next_edge_id = AtomicU64::new(next_edge_id_init);
        let idx_cache_hits = AtomicU64::new(0);
        let idx_cache_misses = AtomicU64::new(0);
        let row_hash_header = opts.row_hash_header;
        let nodes_root_id = nodes.root_page().0;
        let edges_root_id = edges.root_page().0;
        let adj_fwd_root_id = adj_fwd.root_page().0;
        let adj_rev_root_id = adj_rev.root_page().0;
        let version_log_root_id = version_log.root_page().0;
        #[cfg(feature = "degree-cache")]
        let degree_root_id = degree_tree
            .as_ref()
            .map(|tree| tree.root_page().0)
            .unwrap_or(0);

        let commit_table = store.commit_table();
        let metrics: Arc<dyn super::metrics::StorageMetrics> = opts
            .metrics
            .unwrap_or_else(|| super::metrics::default_metrics());
        let snapshot_pool = if opts.snapshot_pool_size > 0 {
            Some(SnapshotPool::new(
                Arc::clone(&store),
                Arc::clone(&metrics),
                opts.snapshot_pool_size,
                Duration::from_millis(opts.snapshot_pool_max_age_ms.max(1)),
            ))
        } else {
            None
        };
        let version_cache = if opts.version_cache_capacity > 0 {
            Some(VersionCache::new(
                opts.version_cache_shards,
                opts.version_cache_capacity,
            ))
        } else {
            None
        };

        let graph = Arc::new(Self {
            store,
            commit_table,
            nodes,
            edges,
            adj_fwd,
            adj_rev,
            version_log,
            #[cfg(feature = "degree-cache")]
            degree: degree_tree,
            vstore,
            indexes,
            catalog_epoch,
            inline_prop_blob,
            inline_prop_value,
            #[cfg(feature = "degree-cache")]
            degree_cache_enabled,
            nodes_root: AtomicU64::new(nodes_root_id),
            edges_root: AtomicU64::new(edges_root_id),
            adj_fwd_root: AtomicU64::new(adj_fwd_root_id),
            adj_rev_root: AtomicU64::new(adj_rev_root_id),
            version_log_root: AtomicU64::new(version_log_root_id),
            #[cfg(feature = "degree-cache")]
            degree_root: AtomicU64::new(degree_root_id),
            next_node_id,
            next_edge_id,
            next_version_ptr: AtomicU64::new(next_version_ptr_init),
            idx_cache_hits,
            idx_cache_misses,
            storage_flags,
            metrics: Arc::clone(&metrics),
            mvcc_metrics_last: StdMutex::new(None),
            distinct_neighbors_default: opts.distinct_neighbors_default,
            row_hash_header,
            vacuum_cfg: opts.vacuum.clone(),
            vacuum_sched: VacuumSched::new(),
            inline_history: opts.inline_history,
            inline_history_max_bytes: opts.inline_history_max_bytes,
            defer_adjacency_flush: opts.defer_adjacency_flush,
            defer_index_flush: opts.defer_index_flush,
            snapshot_pool,
            version_cache,
            version_codec_cfg: VersionCodecConfig {
                kind: opts.version_codec,
                min_payload_len: opts.version_codec_min_payload_len,
                min_savings_bytes: opts.version_codec_min_savings_bytes,
            },
            version_log_bytes: AtomicU64::new(0),
            version_log_entries: AtomicU64::new(0),
            version_cache_hits: AtomicU64::new(0),
            version_cache_misses: AtomicU64::new(0),
            version_codec_raw_bytes: AtomicU64::new(0),
            version_codec_encoded_bytes: AtomicU64::new(0),
            micro_gc_last_ms: AtomicU64::new(0),
            micro_gc_budget_hint: AtomicUsize::new(0),
            micro_gc_running: AtomicBool::new(false),
        });
        graph.recompute_version_log_bytes()?;
        graph.register_vacuum_hook();
        graph.vacuum_sched.next_deadline_ms.set(0);
        Ok(graph)
    }

    fn register_vacuum_hook(self: &Arc<Self>) {
        let maint_graph: Arc<Graph> = Arc::clone(self);
        let maint_trait: Arc<dyn BackgroundMaintainer> = maint_graph;
        self.store
            .register_background_maint(Arc::downgrade(&maint_trait));
    }

    /// Creates a new node in the graph with the given specification.
    pub fn create_node(&self, tx: &mut WriteGuard<'_>, spec: NodeSpec<'_>) -> Result<NodeId> {
        let labels = normalize_labels(spec.labels)?;
        let mut prop_owned: BTreeMap<PropId, PropValueOwned> = BTreeMap::new();
        for entry in spec.props {
            let owned = prop_value_to_owned(entry.value.clone());
            prop_owned.insert(entry.prop, owned);
        }
        let (prop_bytes, spill_vrefs) = self.encode_property_map(tx, spec.props)?;
        let mut map_vref: Option<VRef> = None;
        let payload = if prop_bytes.len() <= self.inline_prop_blob {
            NodePropPayload::Inline(&prop_bytes)
        } else {
            let vref = self.vstore.write(tx, &prop_bytes)?;
            map_vref = Some(vref);
            NodePropPayload::VRef(vref)
        };
        let root = self.nodes.root_page();
        debug_assert!(root.0 != 0, "nodes root page not initialized");
        let (commit_id, version) = self.tx_pending_version_header(tx);
        let row_bytes = match node::encode(
            &labels,
            payload,
            NodeEncodeOpts::new(self.row_hash_header),
            version,
            VersionPtr::null(),
            None,
        ) {
            Ok(encoded) => encoded.bytes,
            Err(err) => {
                if let Some(vref) = map_vref.take() {
                    let _ = self.vstore.free(tx, vref);
                }
                props::free_vrefs(&self.vstore, tx, &spill_vrefs);
                return Err(err);
            }
        };
        let id_raw = self.next_node_id.fetch_add(1, AtomicOrdering::SeqCst);
        let node_id = NodeId(id_raw);
        let next_id = node_id.0.saturating_add(1);
        tx.update_meta(|meta| {
            if meta.storage_next_node_id <= node_id.0 {
                meta.storage_next_node_id = next_id;
            }
        })?;
        if let Err(err) = self.nodes.put(tx, &node_id.0, &row_bytes) {
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        self.persist_tree_root(tx, RootKind::Nodes)?;
        if let Err(err) = self.stage_label_inserts(tx, node_id, &labels, commit_id) {
            let _ = self.nodes.delete(tx, &node_id.0);
            self.persist_tree_root(tx, RootKind::Nodes)?;
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        if let Err(err) = self.insert_indexed_props(tx, node_id, &labels, &prop_owned, commit_id) {
            let _ = self.indexes.remove_node_labels(tx, node_id, &labels);
            let _ = self.nodes.delete(tx, &node_id.0);
            self.persist_tree_root(tx, RootKind::Nodes)?;
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        self.finalize_node_head(tx, node_id)?;
        self.metrics.node_created();
        Ok(node_id)
    }

    /// Creates a label index for fast node lookups by label.
    pub fn create_label_index(&self, tx: &mut WriteGuard<'_>, label: LabelId) -> Result<()> {
        if self.indexes.has_label_index_with_write(tx, label)? {
            return Ok(());
        }
        let mut nodes = Vec::new();
        self.nodes.for_each_with_write(tx, |id_raw, bytes| {
            let versioned = node::decode(&bytes)?;
            if versioned.header.is_tombstone() {
                return Ok(());
            }
            if versioned.row.labels.binary_search(&label).is_ok() {
                nodes.push(NodeId(id_raw));
            }
            Ok(())
        })?;
        self.indexes.create_label_index(tx, label, nodes)?;
        self.sync_index_roots(tx)?;
        self.bump_ddl_epoch(tx)
    }

    /// Drops an existing label index.
    pub fn drop_label_index(&self, tx: &mut WriteGuard<'_>, label: LabelId) -> Result<()> {
        if !self.indexes.has_label_index_with_write(tx, label)? {
            return Ok(());
        }
        self.indexes.drop_label_index(tx, label)?;
        self.sync_index_roots(tx)?;
        self.bump_ddl_epoch(tx)
    }

    /// Checks if a label index exists for the given label.
    pub fn has_label_index(&self, label: LabelId) -> Result<bool> {
        self.indexes.has_label_index(label)
    }

    /// Creates a property index for fast property-based lookups.
    pub fn create_property_index(&self, tx: &mut WriteGuard<'_>, def: IndexDef) -> Result<()> {
        let existing = self
            .indexes
            .property_indexes_for_label_with_write(tx, def.label)?;
        if existing.iter().any(|entry| entry.prop == def.prop) {
            return Ok(());
        }
        let mut entries: Vec<(Vec<u8>, NodeId)> = Vec::new();
        self.nodes.for_each_with_write(tx, |id_raw, bytes| {
            let versioned = node::decode(&bytes)?;
            if versioned.header.is_tombstone() {
                return Ok(());
            }
            if versioned.row.labels.binary_search(&def.label).is_err() {
                return Ok(());
            }
            let prop_bytes = self.read_node_prop_bytes(&versioned.row.props)?;
            let props = self.materialize_props_owned(&prop_bytes)?;
            let map: BTreeMap<PropId, PropValueOwned> = props.into_iter().collect();
            if let Some(value) = map.get(&def.prop) {
                let key = encode_value_key_owned(def.ty, value)?;
                entries.push((key, NodeId(id_raw)));
            }
            Ok(())
        })?;
        entries.sort_by(|(a, node_a), (b, node_b)| match a.cmp(b) {
            CmpOrdering::Equal => node_a.0.cmp(&node_b.0),
            other => other,
        });
        self.indexes.create_property_index(tx, def, &entries)?;
        self.sync_index_roots(tx)?;
        self.bump_ddl_epoch(tx)
    }

    /// Drops a property index for the given label and property.
    pub fn drop_property_index(
        &self,
        tx: &mut WriteGuard<'_>,
        label: LabelId,
        prop: PropId,
    ) -> Result<()> {
        let defs = self
            .indexes
            .property_indexes_for_label_with_write(tx, label)?;
        let Some(def) = defs.into_iter().find(|d| d.prop == prop) else {
            return Ok(());
        };
        self.indexes.drop_property_index(tx, def)?;
        self.sync_index_roots(tx)?;
        self.bump_ddl_epoch(tx)
    }

    /// Checks if a property index exists for the given label and property.
    pub fn has_property_index(&self, label: LabelId, prop: PropId) -> Result<bool> {
        let read = self.lease_latest_snapshot()?;
        Ok(self
            .indexes
            .get_property_index(&read, label, prop)?
            .is_some())
    }

    /// Returns the root page ID of the index catalog.
    pub fn index_catalog_root(&self) -> PageId {
        self.indexes.catalog().tree().root_page()
    }

    /// Retrieves the property index definition for a given label and property.
    pub fn property_index(&self, label: LabelId, prop: PropId) -> Result<Option<IndexDef>> {
        let read = self.lease_latest_snapshot()?;
        self.indexes.get_property_index(&read, label, prop)
    }

    #[cfg(test)]
    /// Collects all entries from a property index for use in assertions.
    pub fn debug_collect_property_index(
        &self,
        tx: &ReadGuard,
        label: LabelId,
        prop: PropId,
    ) -> Result<Vec<(Vec<u8>, NodeId)>> {
        use std::ops::Bound;
        let def = self
            .indexes
            .get_property_index(tx, label, prop)?
            .ok_or(SombraError::Invalid("property index not found"))?;
        self.indexes
            .scan_property_range(tx, &def, Bound::Unbounded, Bound::Unbounded)
    }

    /// Returns all property index definitions currently registered.
    pub fn all_property_indexes(&self) -> Result<Vec<IndexDef>> {
        let read = self.lease_latest_snapshot()?;
        self.indexes.all_property_indexes(&read)
    }

    /// Scans for nodes matching an exact property value using an index.
    pub fn property_scan_eq(
        &self,
        tx: &ReadGuard,
        label: LabelId,
        prop: PropId,
        value: &PropValueOwned,
    ) -> Result<Vec<NodeId>> {
        let mut stream = self.property_scan_eq_stream(tx, label, prop, value)?;
        collect_posting_stream(&mut *stream)
    }

    /// Returns a stream of node IDs with the specified label and property value.
    pub fn property_scan_eq_stream<'a>(
        &'a self,
        tx: &'a ReadGuard,
        label: LabelId,
        prop: PropId,
        value: &PropValueOwned,
    ) -> Result<Box<dyn PostingStream + 'a>> {
        let lookup_timer = storage_profile_timer();
        let def = self
            .indexes
            .get_property_index(tx, label, prop)?
            .ok_or(SombraError::Invalid("property index not found"))?;
        record_storage_profile_timer(StorageProfileKind::PropIndexLookup, lookup_timer);

        let encode_timer = storage_profile_timer();
        let key = encode_value_key_owned(def.ty, value)?;
        record_storage_profile_timer(StorageProfileKind::PropIndexKeyEncode, encode_timer);

        let stream_timer = storage_profile_timer();
        let stream = self.indexes.scan_property_eq_stream(tx, &def, &key)?;
        record_storage_profile_timer(StorageProfileKind::PropIndexStreamBuild, stream_timer);
        let filtered = PropertyFilterStream::new_eq(self, tx, stream, label, prop, value.clone());
        Ok(instrument_posting_stream(filtered))
    }

    /// Scans for nodes with property values in a range (inclusive bounds).
    pub fn property_scan_range(
        &self,
        tx: &ReadGuard,
        label: LabelId,
        prop: PropId,
        start: &PropValueOwned,
        end: &PropValueOwned,
    ) -> Result<Vec<NodeId>> {
        self.property_scan_range_bounds(
            tx,
            label,
            prop,
            Bound::Included(start),
            Bound::Included(end),
        )
    }

    /// Returns aggregate cache statistics for label→index lookups.
    pub fn index_cache_stats(&self) -> GraphIndexCacheStats {
        GraphIndexCacheStats {
            hits: self.idx_cache_hits.load(AtomicOrdering::Relaxed),
            misses: self.idx_cache_misses.load(AtomicOrdering::Relaxed),
        }
    }

    /// Scans for nodes with property values in a range with custom bounds.
    pub fn property_scan_range_bounds(
        &self,
        tx: &ReadGuard,
        label: LabelId,
        prop: PropId,
        start: Bound<&PropValueOwned>,
        end: Bound<&PropValueOwned>,
    ) -> Result<Vec<NodeId>> {
        let mut stream = self.property_scan_range_stream(tx, label, prop, start, end)?;
        collect_posting_stream(&mut *stream)
    }

    /// Returns a stream of node IDs with the specified label and property values in the given range.
    pub fn property_scan_range_stream<'a>(
        &'a self,
        tx: &'a ReadGuard,
        label: LabelId,
        prop: PropId,
        start: Bound<&PropValueOwned>,
        end: Bound<&PropValueOwned>,
    ) -> Result<Box<dyn PostingStream + 'a>> {
        let lookup_timer = storage_profile_timer();
        let def = self
            .indexes
            .get_property_index(tx, label, prop)?
            .ok_or(SombraError::Invalid("property index not found"))?;
        record_storage_profile_timer(StorageProfileKind::PropIndexLookup, lookup_timer);

        let encode_timer = storage_profile_timer();
        let start_key = encode_range_bound(def.ty, start)?;
        let end_key = encode_range_bound(def.ty, end)?;
        record_storage_profile_timer(StorageProfileKind::PropIndexKeyEncode, encode_timer);

        let stream_timer = storage_profile_timer();
        let stream = self
            .indexes
            .scan_property_range_stream(tx, &def, start_key, end_key)?;
        record_storage_profile_timer(StorageProfileKind::PropIndexStreamBuild, stream_timer);
        let filtered = PropertyFilterStream::new_range(
            self,
            tx,
            stream,
            label,
            prop,
            clone_owned_bound(start),
            clone_owned_bound(end),
        );
        Ok(instrument_posting_stream(filtered))
    }

    /// Returns a label scan iterator; this prefers real indexes.
    pub fn label_scan<'a>(
        &'a self,
        tx: &'a ReadGuard,
        label: LabelId,
    ) -> Result<Option<LabelScan<'a>>> {
        self.indexes.label_scan(tx, label)
    }

    /// Returns a label scan iterator and falls back to a full scan when no index exists.
    pub fn label_scan_stream<'a>(
        &'a self,
        tx: &'a ReadGuard,
        label: LabelId,
    ) -> Result<Box<dyn PostingStream + 'a>> {
        if let Some(scan) = self.indexes.label_scan(tx, label)? {
            return Ok(Box::new(scan));
        }
        let fallback = self.build_fallback_label_scan(tx, label)?;
        Ok(Box::new(fallback))
    }

    /// Retrieves node data by ID.
    pub fn get_node(&self, tx: &ReadGuard, id: NodeId) -> Result<Option<NodeData>> {
        let Some(bytes) = self.nodes.get(tx, &id.0)? else {
            return Ok(None);
        };
        let Some(versioned) = self.visible_node_from_bytes(tx, id, &bytes)? else {
            return Ok(None);
        };
        let row = versioned.row;
        let prop_bytes = match row.props {
            NodePropStorage::Inline(bytes) => bytes,
            NodePropStorage::VRef(vref) => self.vstore.read(tx, vref)?,
        };
        let raw = props::decode_raw(&prop_bytes)?;
        let props = props::materialize_props(&raw, &self.vstore, tx)?;
        Ok(Some(NodeData {
            labels: row.labels,
            props,
        }))
    }

    /// Scans and returns all nodes in the graph.
    pub fn scan_all_nodes(&self, tx: &ReadGuard) -> Result<Vec<(NodeId, NodeData)>> {
        let mut cursor = self.nodes.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut rows = Vec::new();
        while let Some((key, bytes)) = cursor.next()? {
            let Some(versioned) = self.visible_node_from_bytes(tx, NodeId(key), &bytes)? else {
                continue;
            };
            let row = versioned.row;
            let prop_bytes = match row.props {
                NodePropStorage::Inline(bytes) => bytes,
                NodePropStorage::VRef(vref) => self.vstore.read(tx, vref)?,
            };
            let props = self.materialize_props_owned(&prop_bytes)?;
            rows.push((
                NodeId(key),
                NodeData {
                    labels: row.labels,
                    props,
                },
            ));
        }
        Ok(rows)
    }

    fn build_fallback_label_scan(
        &self,
        tx: &ReadGuard,
        label: LabelId,
    ) -> Result<FallbackLabelScan> {
        let mut cursor = self.nodes.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut nodes = Vec::new();
        while let Some((key, bytes)) = cursor.next()? {
            let Some(versioned) = self.visible_node_from_bytes(tx, NodeId(key), &bytes)? else {
                continue;
            };
            let row = versioned.row;
            if row.labels.binary_search(&label).is_ok() {
                nodes.push(NodeId(key));
            }
        }
        Ok(FallbackLabelScan { nodes, pos: 0 })
    }

    /// Counts how many nodes exist with the specified label.
    pub fn count_nodes_with_label(&self, tx: &ReadGuard, label: LabelId) -> Result<u64> {
        let mut stream = self.label_scan_stream(tx, label)?;
        const BATCH: usize = 256;
        let mut buf = Vec::with_capacity(BATCH);
        let mut count = 0u64;
        loop {
            buf.clear();
            let has_more = stream.next_batch(&mut buf, BATCH)?;
            for node in &buf {
                if self.node_has_label(tx, *node, label)? {
                    count = count.saturating_add(1);
                }
            }
            if !has_more {
                break;
            }
        }
        Ok(count)
    }

    /// Returns all node identifiers that carry the provided label.
    pub fn nodes_with_label(&self, tx: &ReadGuard, label: LabelId) -> Result<Vec<NodeId>> {
        let mut stream = self.label_scan_stream(tx, label)?;
        const BATCH: usize = 256;
        let mut buf = Vec::with_capacity(BATCH);
        let mut nodes = Vec::new();
        loop {
            buf.clear();
            let has_more = stream.next_batch(&mut buf, BATCH)?;
            for node in &buf {
                if self.node_has_label(tx, *node, label)? {
                    nodes.push(*node);
                }
            }
            if !has_more {
                break;
            }
        }
        Ok(nodes)
    }

    /// Counts the number of edges that have the provided type.
    pub fn count_edges_with_type(&self, tx: &ReadGuard, ty: TypeId) -> Result<u64> {
        let mut cursor = self.edges.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut count = 0u64;
        while let Some((key, bytes)) = cursor.next()? {
            let Some(versioned) = self.visible_edge_from_bytes(tx, EdgeId(key), &bytes)? else {
                continue;
            };
            let row = versioned.row;
            if row.ty == ty {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Samples up to `limit` nodes from the B-Tree and returns their label lists.
    pub fn sample_node_labels(&self, tx: &ReadGuard, limit: usize) -> Result<Vec<Vec<LabelId>>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut cursor = self.nodes.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut labels = Vec::new();
        while let Some((_key, bytes)) = cursor.next()? {
            if let Some(versioned) = self.visible_node_from_bytes(tx, NodeId(_key), &bytes)? {
                labels.push(versioned.row.labels);
            }
            if labels.len() >= limit {
                break;
            }
        }
        Ok(labels)
    }

    /// Deletes a node from the graph with the given options.
    pub fn delete_node(
        &self,
        tx: &mut WriteGuard<'_>,
        id: NodeId,
        opts: DeleteNodeOpts,
    ) -> Result<()> {
        let Some(bytes) = self.nodes.get_with_write(tx, &id.0)? else {
            return Err(SombraError::NotFound);
        };
        let versioned = node::decode(&bytes)?;
        if versioned.header.is_tombstone() {
            return Err(SombraError::NotFound);
        }
        let row = versioned.row;
        let prop_bytes = self.read_node_prop_bytes_with_write(tx, &row.props)?;
        let old_props_vec = self.materialize_props_owned_with_write(tx, &prop_bytes)?;
        let old_props: BTreeMap<PropId, PropValueOwned> = old_props_vec.into_iter().collect();
        let read = self.lease_latest_snapshot()?;
        let incident = self.collect_incident_edges(&read, id)?;
        drop(read);

        match opts.mode {
            DeleteMode::Restrict => {
                if !incident.is_empty() {
                    return Err(SombraError::Invalid("node has incident edges"));
                }
            }
            DeleteMode::Cascade => {
                let mut edges: Vec<EdgeId> = incident.into_iter().collect();
                edges.sort_by_key(|edge| edge.0);
                for edge_id in edges {
                    self.delete_edge(tx, edge_id)?;
                }
            }
        }

        let (commit_id, mut tombstone_header) = self.tx_pending_version_header(tx);
        self.stage_label_removals(tx, id, &row.labels, commit_id)?;
        let empty_props = BTreeMap::new();
        self.update_indexed_props_for_node(
            tx,
            id,
            &row.labels,
            &old_props,
            &empty_props,
            commit_id,
        )?;
        let mut old_header = versioned.header;
        old_header.end = commit_id;
        let mut log_bytes = bytes.clone();
        Self::overwrite_encoded_header(&mut log_bytes, &old_header);
        let prev_ptr = self.log_version_entry(
            tx,
            VersionSpace::Node,
            id.0,
            old_header,
            versioned.prev_ptr,
            log_bytes.clone(),
        )?;
        let inline_history = self.maybe_inline_history(&log_bytes);
        tombstone_header.flags |= mvcc_flags::TOMBSTONE;
        if inline_history.is_some() {
            tombstone_header.flags |= mvcc_flags::INLINE_HISTORY;
        }
        let encoded = node::encode(
            &[],
            node::PropPayload::Inline(&[]),
            NodeEncodeOpts::new(false),
            tombstone_header,
            prev_ptr,
            inline_history.as_deref(),
        )?;
        self.nodes.put(tx, &id.0, &encoded.bytes)?;
        self.persist_tree_root(tx, RootKind::Nodes)?;
        self.finalize_node_head(tx, id)?;
        self.metrics.node_deleted();
        Ok(())
    }

    /// Updates the properties of an existing node by applying the given patch.
    pub fn update_node(
        &self,
        tx: &mut WriteGuard<'_>,
        id: NodeId,
        patch: PropPatch<'_>,
    ) -> Result<()> {
        if patch.is_empty() {
            return Ok(());
        }
        let Some(existing_bytes) = self.nodes.get_with_write(tx, &id.0)? else {
            return Err(SombraError::NotFound);
        };
        let versioned = node::decode(&existing_bytes)?;
        if versioned.header.is_tombstone() {
            return Err(SombraError::NotFound);
        }
        let node::NodeRow {
            labels,
            props: storage,
            row_hash,
        } = versioned.row;
        let prop_bytes = self.read_node_prop_bytes_with_write(tx, &storage)?;
        let Some(delta) = self.build_prop_delta(tx, &prop_bytes, &patch)? else {
            return Ok(());
        };
        let (commit_id, new_header) = self.tx_pending_version_header(tx);
        let mut map_vref: Option<VRef> = None;
        let payload = if delta.encoded.bytes.len() <= self.inline_prop_blob {
            NodePropPayload::Inline(&delta.encoded.bytes)
        } else {
            let vref = self.vstore.write(tx, &delta.encoded.bytes)?;
            map_vref = Some(vref);
            NodePropPayload::VRef(vref)
        };
        let mut old_header = versioned.header;
        old_header.end = commit_id;
        let mut log_bytes = existing_bytes.clone();
        Self::overwrite_encoded_header(&mut log_bytes, &old_header);
        let prev_ptr = self.log_version_entry(
            tx,
            VersionSpace::Node,
            id.0,
            old_header,
            versioned.prev_ptr,
            log_bytes.clone(),
        )?;
        let inline_history = self.maybe_inline_history(&log_bytes);
        let mut new_header = new_header;
        if inline_history.is_some() {
            new_header.flags |= mvcc_flags::INLINE_HISTORY;
        }
        let encoded_row = match node::encode(
            &labels,
            payload,
            NodeEncodeOpts::new(self.row_hash_header),
            new_header,
            prev_ptr,
            inline_history.as_deref(),
        ) {
            Ok(encoded) => encoded,
            Err(err) => {
                if let Some(vref) = map_vref.take() {
                    let _ = self.vstore.free(tx, vref);
                }
                props::free_vrefs(&self.vstore, tx, &delta.encoded.spill_vrefs);
                return Err(err);
            }
        };
        if self.row_hash_header {
            if let (Some(old_hash), Some(new_hash)) = (row_hash, encoded_row.row_hash) {
                if old_hash == new_hash {
                    if let Some(vref) = map_vref.take() {
                        let _ = self.vstore.free(tx, vref);
                    }
                    props::free_vrefs(&self.vstore, tx, &delta.encoded.spill_vrefs);
                    return Ok(());
                }
            }
        }
        if let Err(err) = self.nodes.put(tx, &id.0, &encoded_row.bytes) {
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &delta.encoded.spill_vrefs);
            return Err(err);
        }
        self.persist_tree_root(tx, RootKind::Nodes)?;
        self.update_indexed_props_for_node(
            tx,
            id,
            &labels,
            &delta.old_map,
            &delta.new_map,
            commit_id,
        )?;
        self.finalize_node_head(tx, id)?;
        Ok(())
    }

    /// Creates a new edge in the graph with the given specification.
    pub fn create_edge(&self, tx: &mut WriteGuard<'_>, spec: EdgeSpec<'_>) -> Result<EdgeId> {
        self.ensure_node_exists(tx, spec.src, "edge source node missing")?;
        self.ensure_node_exists(tx, spec.dst, "edge destination node missing")?;
        self.insert_edge_unchecked(tx, spec)
    }

    fn insert_edge_unchecked(&self, tx: &mut WriteGuard<'_>, spec: EdgeSpec<'_>) -> Result<EdgeId> {
        let (prop_bytes, spill_vrefs) = self.encode_property_map(tx, spec.props)?;
        let mut map_vref: Option<VRef> = None;
        let payload = if prop_bytes.len() <= self.inline_prop_blob {
            EdgePropPayload::Inline(&prop_bytes)
        } else {
            let vref = self.vstore.write(tx, &prop_bytes)?;
            map_vref = Some(vref);
            EdgePropPayload::VRef(vref)
        };
        let (commit_id, version) = self.tx_pending_version_header(tx);
        let row_bytes = match edge::encode(
            spec.src,
            spec.dst,
            spec.ty,
            payload,
            EdgeEncodeOpts::new(self.row_hash_header),
            version,
            VersionPtr::null(),
            None,
        ) {
            Ok(encoded) => encoded.bytes,
            Err(err) => {
                if let Some(vref) = map_vref.take() {
                    let _ = self.vstore.free(tx, vref);
                }
                props::free_vrefs(&self.vstore, tx, &spill_vrefs);
                return Err(err);
            }
        };
        let id_raw = self.next_edge_id.fetch_add(1, AtomicOrdering::SeqCst);
        let edge_id = EdgeId(id_raw);
        let next_id = edge_id.0.saturating_add(1);
        tx.update_meta(|meta| {
            if meta.storage_next_edge_id <= edge_id.0 {
                meta.storage_next_edge_id = next_id;
            }
        })?;
        if let Err(err) = self.edges.put(tx, &edge_id.0, &row_bytes) {
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        self.persist_tree_root(tx, RootKind::Edges)?;
        if let Err(err) =
            self.stage_adjacency_inserts(tx, &[(spec.src, spec.dst, spec.ty, edge_id)], commit_id)
        {
            let _ = self.edges.delete(tx, &edge_id.0);
            self.persist_tree_root(tx, RootKind::Edges)?;
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        self.finalize_edge_head(tx, edge_id)?;
        self.metrics.edge_created();
        Ok(edge_id)
    }

    /// Retrieves edge data by ID.
    pub fn get_edge(&self, tx: &ReadGuard, id: EdgeId) -> Result<Option<EdgeData>> {
        let Some(bytes) = self.edges.get(tx, &id.0)? else {
            return Ok(None);
        };
        let Some(versioned) = self.visible_edge_from_bytes(tx, id, &bytes)? else {
            return Ok(None);
        };
        let row = versioned.row;
        let prop_bytes = match row.props {
            EdgePropStorage::Inline(bytes) => bytes,
            EdgePropStorage::VRef(vref) => self.vstore.read(tx, vref)?,
        };
        let raw = props::decode_raw(&prop_bytes)?;
        let props = props::materialize_props(&raw, &self.vstore, tx)?;
        Ok(Some(EdgeData {
            src: row.src,
            dst: row.dst,
            ty: row.ty,
            props,
        }))
    }

    /// Scans and returns all edges in the graph.
    pub fn scan_all_edges(&self, tx: &ReadGuard) -> Result<Vec<(EdgeId, EdgeData)>> {
        let mut cursor = self.edges.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut rows = Vec::new();
        while let Some((key, bytes)) = cursor.next()? {
            let Some(versioned) = self.visible_edge_from_bytes(tx, EdgeId(key), &bytes)? else {
                continue;
            };
            let row = versioned.row;
            let prop_bytes = match row.props {
                EdgePropStorage::Inline(bytes) => bytes,
                EdgePropStorage::VRef(vref) => self.vstore.read(tx, vref)?,
            };
            let props = self.materialize_props_owned(&prop_bytes)?;
            rows.push((
                EdgeId(key),
                EdgeData {
                    src: row.src,
                    dst: row.dst,
                    ty: row.ty,
                    props,
                },
            ));
        }
        Ok(rows)
    }

    /// Updates edge properties with the given patch.
    pub fn update_edge(
        &self,
        tx: &mut WriteGuard<'_>,
        id: EdgeId,
        patch: PropPatch<'_>,
    ) -> Result<()> {
        if patch.is_empty() {
            return Ok(());
        }
        let Some(existing_bytes) = self.edges.get_with_write(tx, &id.0)? else {
            return Err(SombraError::NotFound);
        };
        let versioned = edge::decode(&existing_bytes)?;
        if versioned.header.is_tombstone() {
            return Err(SombraError::NotFound);
        }
        let edge::EdgeRow {
            src,
            dst,
            ty,
            props: storage,
            row_hash: old_row_hash,
        } = versioned.row;
        let prop_bytes = self.read_edge_prop_bytes_with_write(tx, &storage)?;
        let Some(delta) = self.build_prop_delta(tx, &prop_bytes, &patch)? else {
            return Ok(());
        };
        let (commit_id, new_header) = self.tx_pending_version_header(tx);
        let mut map_vref: Option<VRef> = None;
        let payload = if delta.encoded.bytes.len() <= self.inline_prop_blob {
            EdgePropPayload::Inline(&delta.encoded.bytes)
        } else {
            let vref = self.vstore.write(tx, &delta.encoded.bytes)?;
            map_vref = Some(vref);
            EdgePropPayload::VRef(vref)
        };
        let mut old_header = versioned.header;
        old_header.end = commit_id;
        let mut log_bytes = existing_bytes.clone();
        Self::overwrite_encoded_header(&mut log_bytes, &old_header);
        let prev_ptr = self.log_version_entry(
            tx,
            VersionSpace::Edge,
            id.0,
            old_header,
            versioned.prev_ptr,
            log_bytes.clone(),
        )?;
        let inline_history = self.maybe_inline_history(&log_bytes);
        let mut new_header = new_header;
        if inline_history.is_some() {
            new_header.flags |= mvcc_flags::INLINE_HISTORY;
        }
        let encoded_row = match edge::encode(
            src,
            dst,
            ty,
            payload,
            EdgeEncodeOpts::new(self.row_hash_header),
            new_header,
            prev_ptr,
            inline_history.as_deref(),
        ) {
            Ok(encoded) => encoded,
            Err(err) => {
                if let Some(vref) = map_vref.take() {
                    let _ = self.vstore.free(tx, vref);
                }
                props::free_vrefs(&self.vstore, tx, &delta.encoded.spill_vrefs);
                return Err(err);
            }
        };

        if self.row_hash_header {
            if let (Some(old_hash), Some(new_hash)) = (old_row_hash, encoded_row.row_hash) {
                if old_hash == new_hash {
                    if let Some(vref) = map_vref.take() {
                        let _ = self.vstore.free(tx, vref);
                    }
                    props::free_vrefs(&self.vstore, tx, &delta.encoded.spill_vrefs);
                    return Ok(());
                }
            }
        }

        if let Err(err) = self.edges.put(tx, &id.0, &encoded_row.bytes) {
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &delta.encoded.spill_vrefs);
            return Err(err);
        }
        self.persist_tree_root(tx, RootKind::Edges)?;
        self.finalize_edge_head(tx, id)?;
        Ok(())
    }

    /// Returns neighboring nodes of a given node based on direction and edge type.
    pub fn neighbors(
        &self,
        tx: &ReadGuard,
        id: NodeId,
        dir: Dir,
        ty: Option<TypeId>,
        opts: ExpandOpts,
    ) -> Result<NeighborCursor> {
        let mut neighbors: Vec<Neighbor> = Vec::new();
        let enable_distinct = opts.distinct_nodes || self.distinct_neighbors_default;
        let mut seen_set = enable_distinct.then(HashSet::new);
        if dir.includes_out() {
            self.metrics.adjacency_scan("out");
            self.collect_neighbors(tx, id, ty, true, seen_set.as_mut(), &mut neighbors)?;
        }
        if dir.includes_in() {
            self.metrics.adjacency_scan("in");
            self.collect_neighbors(tx, id, ty, false, seen_set.as_mut(), &mut neighbors)?;
        }
        Ok(NeighborCursor::new(neighbors))
    }

    /// Performs a breadth-first traversal from `start`, returning visited nodes up to `max_depth`.
    pub fn bfs(&self, tx: &ReadGuard, start: NodeId, opts: &BfsOptions) -> Result<Vec<BfsVisit>> {
        if !self.node_exists(tx, start)? {
            return Err(SombraError::NotFound);
        }
        let mut queue: VecDeque<(NodeId, u32)> = VecDeque::new();
        let mut seen: HashSet<NodeId> = HashSet::new();
        let mut visits: Vec<BfsVisit> = Vec::new();
        queue.push_back((start, 0));
        seen.insert(start);
        let type_filters = opts.edge_types.as_deref();
        while let Some((node, depth)) = queue.pop_front() {
            visits.push(BfsVisit { node, depth });
            if let Some(limit) = opts.max_results {
                if visits.len() >= limit {
                    break;
                }
            }
            if depth >= opts.max_depth {
                continue;
            }
            match type_filters {
                Some(types) if !types.is_empty() => {
                    for ty in types {
                        self.enqueue_bfs_neighbors(
                            tx,
                            node,
                            opts.direction,
                            Some(*ty),
                            depth + 1,
                            &mut seen,
                            &mut queue,
                        )?;
                    }
                }
                _ => {
                    self.enqueue_bfs_neighbors(
                        tx,
                        node,
                        opts.direction,
                        None,
                        depth + 1,
                        &mut seen,
                        &mut queue,
                    )?;
                }
            }
        }
        Ok(visits)
    }

    /// Computes the degree (number of edges) for a node in a given direction.
    pub fn degree(&self, tx: &ReadGuard, id: NodeId, dir: Dir, ty: Option<TypeId>) -> Result<u64> {
        let result = match dir {
            Dir::Out => self.degree_single(tx, id, true, ty)?,
            Dir::In => self.degree_single(tx, id, false, ty)?,
            Dir::Both => {
                let out = self.degree_single(tx, id, true, ty)?;
                let inn = self.degree_single(tx, id, false, ty)?;
                let loops = self.count_loop_edges(tx, id, ty)?;
                out + inn - loops
            }
        };
        let direction_str = match dir {
            Dir::Out => "out",
            Dir::In => "in",
            Dir::Both => "both",
        };
        let cached = self.degree_has_cache_entry(tx, id, dir, ty)?;
        self.metrics.degree_query(direction_str, cached);
        Ok(result)
    }

    /// Deletes an edge from the graph by ID.
    pub fn delete_edge(&self, tx: &mut WriteGuard<'_>, id: EdgeId) -> Result<()> {
        let Some(bytes) = self.edges.get_with_write(tx, &id.0)? else {
            return Err(SombraError::NotFound);
        };
        let versioned = edge::decode(&bytes)?;
        if versioned.header.is_tombstone() {
            return Err(SombraError::NotFound);
        }
        let row = versioned.row;
        let (commit_id, mut tombstone_header) = self.tx_pending_version_header(tx);
        self.stage_adjacency_removals(tx, &[(row.src, row.dst, row.ty, id)], commit_id)?;
        let mut old_header = versioned.header;
        old_header.end = commit_id;
        let mut log_bytes = bytes.clone();
        Self::overwrite_encoded_header(&mut log_bytes, &old_header);
        let prev_ptr = self.log_version_entry(
            tx,
            VersionSpace::Edge,
            id.0,
            old_header,
            versioned.prev_ptr,
            log_bytes.clone(),
        )?;
        tombstone_header.flags |= mvcc_flags::TOMBSTONE;
        let inline_history = self.maybe_inline_history(&log_bytes);
        if inline_history.is_some() {
            tombstone_header.flags |= mvcc_flags::INLINE_HISTORY;
        }
        let encoded = edge::encode(
            row.src,
            row.dst,
            row.ty,
            EdgePropPayload::Inline(&[]),
            EdgeEncodeOpts::new(false),
            tombstone_header,
            prev_ptr,
            inline_history.as_deref(),
        )?;
        self.edges.put(tx, &id.0, &encoded.bytes)?;
        self.persist_tree_root(tx, RootKind::Edges)?;
        self.finalize_edge_head(tx, id)?;
        Ok(())
    }

    fn maybe_publish_mvcc_metrics(&self) {
        if self.commit_table.is_none() {
            return;
        }
        let mut last = match self.mvcc_metrics_last.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        let now = Instant::now();
        if let Some(prev) = *last {
            if now.duration_since(prev) < MVCC_METRICS_PUBLISH_INTERVAL {
                return;
            }
        }
        *last = Some(now);
        drop(last);
        let stats = self.store.stats();
        let oldest_reader_commit = stats.mvcc_reader_oldest_snapshot;
        self.metrics.mvcc_page_versions(
            stats.mvcc_page_versions_total,
            stats.mvcc_pages_with_versions,
        );
        self.metrics.mvcc_reader_gauges(
            stats.mvcc_readers_active,
            stats.mvcc_reader_oldest_snapshot,
            stats.mvcc_reader_newest_snapshot,
            stats.mvcc_reader_max_age_ms,
        );
        self.metrics
            .mvcc_reader_totals(stats.mvcc_reader_begin_total, stats.mvcc_reader_end_total);
        self.indexes.set_oldest_reader_commit(oldest_reader_commit);
        self.vstore.set_oldest_reader_commit(oldest_reader_commit);
    }

    #[inline]
    fn tx_version_header(&self, tx: &mut WriteGuard<'_>) -> (CommitId, VersionHeader) {
        self.maybe_publish_mvcc_metrics();
        let commit_lsn = tx.reserve_commit_id();
        let commit_id = commit_lsn.0;
        (commit_id, VersionHeader::new(commit_id, COMMIT_MAX, 0, 0))
    }

    #[inline]
    fn tx_pending_version_header(&self, tx: &mut WriteGuard<'_>) -> (CommitId, VersionHeader) {
        let (commit_id, mut header) = self.tx_version_header(tx);
        header.set_pending();
        (commit_id, header)
    }

    fn adjacency_value_for_commit(commit: CommitId, tombstone: bool) -> VersionedValue<UnitValue> {
        let mut header = VersionHeader::new(commit, COMMIT_MAX, 0, 0);
        if tombstone {
            header.flags |= mvcc_flags::TOMBSTONE;
        }
        header.set_pending();
        VersionedValue::new(header, UnitValue)
    }

    fn finalize_version_header(&self, header: &mut VersionHeader) -> bool {
        if !header.is_pending() {
            return false;
        }
        header.clear_pending();
        true
    }

    fn finalize_version_value(&self, value: &mut VersionedValue<UnitValue>) -> bool {
        self.finalize_version_header(&mut value.header)
    }

    fn finalize_node_head(&self, tx: &mut WriteGuard<'_>, id: NodeId) -> Result<()> {
        let Some(mut bytes) = self.nodes.get_with_write(tx, &id.0)? else {
            return Err(SombraError::Corruption("node head missing during finalize"));
        };
        let mut header = VersionHeader::decode(&bytes[..VERSION_HEADER_LEN])?;
        if !self.finalize_version_header(&mut header) {
            return Ok(());
        }
        Self::overwrite_encoded_header(&mut bytes, &header);
        self.nodes.put(tx, &id.0, &bytes)?;
        Ok(())
    }

    fn finalize_edge_head(&self, tx: &mut WriteGuard<'_>, id: EdgeId) -> Result<()> {
        let Some(mut bytes) = self.edges.get_with_write(tx, &id.0)? else {
            return Err(SombraError::Corruption("edge head missing during finalize"));
        };
        let mut header = VersionHeader::decode(&bytes[..VERSION_HEADER_LEN])?;
        if !self.finalize_version_header(&mut header) {
            return Ok(());
        }
        Self::overwrite_encoded_header(&mut bytes, &header);
        self.edges.put(tx, &id.0, &bytes)?;
        Ok(())
    }

    fn finalize_adjacency_entry(
        &self,
        tx: &mut WriteGuard<'_>,
        tree: &BTree<Vec<u8>, VersionedValue<UnitValue>>,
        key: &Vec<u8>,
    ) -> Result<()> {
        let Some(mut value) = tree.get_with_write(tx, key)? else {
            return Err(SombraError::Corruption(
                "adjacency entry missing during finalize",
            ));
        };
        if !self.finalize_version_value(&mut value) {
            return Ok(());
        }
        tree.put(tx, key, &value)
    }

    fn finalize_adjacency_entries(
        &self,
        tx: &mut WriteGuard<'_>,
        keys: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<()> {
        for (fwd, rev) in keys {
            self.finalize_adjacency_entry(tx, &self.adj_fwd, fwd)?;
            self.finalize_adjacency_entry(tx, &self.adj_rev, rev)?;
        }
        Ok(())
    }

    #[inline]
    fn reader_snapshot_commit(tx: &ReadGuard) -> CommitId {
        tx.snapshot_lsn().0
    }

    #[inline]
    fn version_visible(header: &VersionHeader, snapshot: CommitId) -> bool {
        header.visible_at(snapshot) && !header.is_tombstone() && !header.is_pending()
    }

    fn visible_version<T, Decode>(
        &self,
        tx: &ReadGuard,
        space: VersionSpace,
        id: u64,
        bytes: &[u8],
        decode: Decode,
    ) -> Result<Option<T>>
    where
        T: VersionChainRecord,
        Decode: Fn(&[u8]) -> Result<T>,
    {
        let snapshot = Self::reader_snapshot_commit(tx);
        let current = decode(bytes)?;
        if Self::version_visible(<T as VersionChainRecord>::header(&current), snapshot) {
            return Ok(Some(current));
        }
        if let Some(inline) = <T as VersionChainRecord>::inline_history(&current) {
            let decoded_inline = decode(inline)?;
            if Self::version_visible(<T as VersionChainRecord>::header(&decoded_inline), snapshot) {
                return Ok(Some(decoded_inline));
            }
            let mut ptr = <T as VersionChainRecord>::prev_ptr(&decoded_inline);
            while let Some(entry) = self.load_version_entry(tx, ptr)? {
                if entry.space != space || entry.id != id {
                    ptr = entry.prev_ptr;
                    continue;
                }
                let decoded = decode(&entry.bytes)?;
                if Self::version_visible(<T as VersionChainRecord>::header(&decoded), snapshot) {
                    return Ok(Some(decoded));
                }
                ptr = <T as VersionChainRecord>::prev_ptr(&decoded);
            }
            return Ok(None);
        }
        let mut ptr = <T as VersionChainRecord>::prev_ptr(&current);
        if ptr.is_null() {
            return Ok(None);
        }
        while let Some(entry) = self.load_version_entry(tx, ptr)? {
            if entry.space != space || entry.id != id {
                ptr = entry.prev_ptr;
                continue;
            }
            let decoded = decode(&entry.bytes)?;
            if Self::version_visible(<T as VersionChainRecord>::header(&decoded), snapshot) {
                return Ok(Some(decoded));
            }
            ptr = <T as VersionChainRecord>::prev_ptr(&decoded);
        }
        Ok(None)
    }

    fn visible_node_from_bytes(
        &self,
        tx: &ReadGuard,
        id: NodeId,
        bytes: &[u8],
    ) -> Result<Option<node::VersionedNodeRow>> {
        self.visible_version(tx, VersionSpace::Node, id.0, bytes, node::decode)
    }

    fn visible_edge_from_bytes(
        &self,
        tx: &ReadGuard,
        id: EdgeId,
        bytes: &[u8],
    ) -> Result<Option<edge::VersionedEdgeRow>> {
        self.visible_version(tx, VersionSpace::Edge, id.0, bytes, edge::decode)
    }

    fn visible_node(&self, tx: &ReadGuard, id: NodeId) -> Result<Option<node::VersionedNodeRow>> {
        let Some(bytes) = self.nodes.get(tx, &id.0)? else {
            return Ok(None);
        };
        self.visible_node_from_bytes(tx, id, &bytes)
    }

    fn visible_edge(&self, tx: &ReadGuard, id: EdgeId) -> Result<Option<edge::VersionedEdgeRow>> {
        let Some(bytes) = self.edges.get(tx, &id.0)? else {
            return Ok(None);
        };
        self.visible_edge_from_bytes(tx, id, &bytes)
    }

    fn node_has_label(&self, tx: &ReadGuard, id: NodeId, label: LabelId) -> Result<bool> {
        if let Some(versioned) = self.visible_node(tx, id)? {
            Ok(versioned.row.labels.binary_search(&label).is_ok())
        } else {
            Ok(false)
        }
    }

    fn materialize_raw_prop_value(
        &self,
        tx: &ReadGuard,
        value: &RawPropValue,
    ) -> Result<PropValueOwned> {
        let owned = match value {
            RawPropValue::Null => PropValueOwned::Null,
            RawPropValue::Bool(v) => PropValueOwned::Bool(*v),
            RawPropValue::Int(v) => PropValueOwned::Int(*v),
            RawPropValue::Float(v) => PropValueOwned::Float(*v),
            RawPropValue::StrInline(bytes) => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|_| SombraError::Corruption("stored string not utf8"))?;
                PropValueOwned::Str(s.to_owned())
            }
            RawPropValue::StrVRef(vref) => {
                let bytes = self.vstore.read(tx, *vref)?;
                let s = String::from_utf8(bytes)
                    .map_err(|_| SombraError::Corruption("stored string not utf8"))?;
                PropValueOwned::Str(s)
            }
            RawPropValue::BytesInline(bytes) => PropValueOwned::Bytes(bytes.clone()),
            RawPropValue::BytesVRef(vref) => {
                let bytes = self.vstore.read(tx, *vref)?;
                PropValueOwned::Bytes(bytes)
            }
            RawPropValue::Date(v) => PropValueOwned::Date(*v),
            RawPropValue::DateTime(v) => PropValueOwned::DateTime(*v),
        };
        Ok(owned)
    }

    fn node_property_value(
        &self,
        tx: &ReadGuard,
        versioned: &node::VersionedNodeRow,
        prop: PropId,
    ) -> Result<Option<PropValueOwned>> {
        let bytes = self.read_node_prop_bytes(&versioned.row.props)?;
        let raw = props::decode_raw(&bytes)?;
        for entry in raw {
            if entry.prop == prop {
                let owned = self.materialize_raw_prop_value(tx, &entry.value)?;
                return Ok(Some(owned));
            }
        }
        Ok(None)
    }

    fn node_matches_property_eq(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        label: LabelId,
        prop: PropId,
        expected: &PropValueOwned,
    ) -> Result<bool> {
        let Some(versioned) = self.visible_node(tx, node)? else {
            return Ok(false);
        };
        if versioned.row.labels.binary_search(&label).is_err() {
            return Ok(false);
        }
        let Some(value) = self.node_property_value(tx, &versioned, prop)? else {
            return Ok(false);
        };
        Ok(value == *expected)
    }

    fn node_matches_property_range(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        label: LabelId,
        prop: PropId,
        start: &Bound<PropValueOwned>,
        end: &Bound<PropValueOwned>,
    ) -> Result<bool> {
        let Some(versioned) = self.visible_node(tx, node)? else {
            return Ok(false);
        };
        if versioned.row.labels.binary_search(&label).is_err() {
            return Ok(false);
        }
        let Some(value) = self.node_property_value(tx, &versioned, prop)? else {
            return Ok(false);
        };
        if !Self::bound_allows(&value, start, true)? {
            return Ok(false);
        }
        if !Self::bound_allows(&value, end, false)? {
            return Ok(false);
        }
        Ok(true)
    }

    fn bound_allows(
        value: &PropValueOwned,
        bound: &Bound<PropValueOwned>,
        is_lower: bool,
    ) -> Result<bool> {
        use std::cmp::Ordering::{Equal, Greater, Less};
        match bound {
            Bound::Unbounded => Ok(true),
            Bound::Included(b) => match compare_prop_values(value, b)? {
                Less if is_lower => Ok(false),
                Greater if !is_lower => Ok(false),
                _ => Ok(true),
            },
            Bound::Excluded(b) => match compare_prop_values(value, b)? {
                Less if is_lower => Ok(false),
                Equal => Ok(!is_lower),
                Greater if !is_lower => Ok(false),
                _ => Ok(true),
            },
        }
    }

    fn encode_property_map(
        &self,
        tx: &mut WriteGuard<'_>,
        props: &[PropEntry<'_>],
    ) -> Result<(Vec<u8>, Vec<VRef>)> {
        let result = props::encode_props(props, self.inline_prop_value, &self.vstore, tx)?;
        Ok((result.bytes, result.spill_vrefs))
    }

    fn ensure_node_exists(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        context: &'static str,
    ) -> Result<()> {
        if self.node_exists_with_write(tx, node)? {
            Ok(())
        } else {
            Err(SombraError::Invalid(context))
        }
    }

    /// Returns true if the node exists using a read guard.
    pub fn node_exists(&self, tx: &ReadGuard, node: NodeId) -> Result<bool> {
        Ok(self.visible_node(tx, node)?.is_some())
    }

    fn node_exists_with_write(&self, tx: &mut WriteGuard<'_>, node: NodeId) -> Result<bool> {
        let Some(bytes) = self.nodes.get_with_write(tx, &node.0)? else {
            return Ok(false);
        };
        let versioned = node::decode(&bytes)?;
        Ok(!versioned.header.is_tombstone() && !versioned.header.is_pending())
    }

    fn stage_adjacency_inserts(
        &self,
        tx: &mut WriteGuard<'_>,
        entries: &[(NodeId, NodeId, TypeId, EdgeId)],
        commit: CommitId,
    ) -> Result<()> {
        if !self.defer_adjacency_flush {
            return self.insert_adjacencies(tx, entries, commit);
        }
        let mut state = self.take_txn_state(tx);
        let buffer = state
            .deferred_adj
            .get_or_insert_with(AdjacencyBuffer::default);
        for (src, dst, ty, edge) in entries {
            buffer.inserts.push((*src, *dst, *ty, *edge, commit));
        }
        self.store_txn_state(tx, state);
        Ok(())
    }

    fn stage_adjacency_removals(
        &self,
        tx: &mut WriteGuard<'_>,
        entries: &[(NodeId, NodeId, TypeId, EdgeId)],
        commit: CommitId,
    ) -> Result<()> {
        if !self.defer_adjacency_flush {
            for (src, dst, ty, edge) in entries {
                self.remove_adjacency(tx, *src, *dst, *ty, *edge, commit)?;
            }
            return Ok(());
        }
        let mut state = self.take_txn_state(tx);
        let buffer = state
            .deferred_adj
            .get_or_insert_with(AdjacencyBuffer::default);
        for (src, dst, ty, edge) in entries {
            buffer.removals.push((*src, *dst, *ty, *edge, commit));
        }
        self.store_txn_state(tx, state);
        Ok(())
    }

    /// Flushes buffered adjacency and index updates for the current transaction.
    pub fn flush_deferred_writes(&self, tx: &mut WriteGuard<'_>) -> Result<()> {
        self.flush_deferred_adjacency(tx)?;
        self.flush_deferred_indexes(tx)?;
        Ok(())
    }

    fn flush_deferred_adjacency(&self, tx: &mut WriteGuard<'_>) -> Result<()> {
        if !self.defer_adjacency_flush {
            return Ok(());
        }
        let mut state = self.take_txn_state(tx);
        let Some(mut buffer) = state.deferred_adj.take() else {
            self.store_txn_state(tx, state);
            return Ok(());
        };
        let mut total_inserts = 0usize;
        if !buffer.inserts.is_empty() {
            let mut grouped: BTreeMap<CommitId, Vec<(NodeId, NodeId, TypeId, EdgeId)>> =
                BTreeMap::new();
            for (src, dst, ty, edge, commit) in buffer.inserts.drain(..) {
                grouped
                    .entry(commit)
                    .or_default()
                    .push((src, dst, ty, edge));
            }
            for (commit, batch) in grouped {
                total_inserts = total_inserts.saturating_add(batch.len());
                self.insert_adjacencies(tx, &batch, commit)?;
            }
        }
        let total_removals = buffer.removals.len();
        for (src, dst, ty, edge, commit) in buffer.removals.drain(..) {
            self.remove_adjacency(tx, src, dst, ty, edge, commit)?;
        }
        self.metrics
            .adjacency_bulk_flush(total_inserts, total_removals);
        self.store_txn_state(tx, state);
        Ok(())
    }

    fn stage_label_inserts(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        labels: &[LabelId],
        commit: CommitId,
    ) -> Result<()> {
        if !self.defer_index_flush {
            return self
                .indexes
                .insert_node_labels_with_commit(tx, node, labels, Some(commit));
        }
        let mut state = self.take_txn_state(tx);
        let buffer = state
            .deferred_index
            .get_or_insert_with(IndexBuffer::default);
        for label in labels {
            buffer.label_inserts.push((*label, node, commit));
        }
        self.store_txn_state(tx, state);
        Ok(())
    }

    fn stage_label_removals(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        labels: &[LabelId],
        commit: CommitId,
    ) -> Result<()> {
        if !self.defer_index_flush {
            return self
                .indexes
                .remove_node_labels_with_commit(tx, node, labels, Some(commit));
        }
        let mut state = self.take_txn_state(tx);
        let buffer = state
            .deferred_index
            .get_or_insert_with(IndexBuffer::default);
        for label in labels {
            buffer.label_removes.push((*label, node, commit));
        }
        self.store_txn_state(tx, state);
        Ok(())
    }

    fn stage_prop_index_op(
        &self,
        tx: &mut WriteGuard<'_>,
        def: IndexDef,
        key: Vec<u8>,
        node: NodeId,
        commit: CommitId,
        insert: bool,
    ) -> Result<()> {
        if !self.defer_index_flush {
            if insert {
                self.indexes.insert_property_value_with_commit(
                    tx,
                    &def,
                    &key,
                    node,
                    Some(commit),
                )?;
            } else {
                self.indexes.remove_property_value_with_commit(
                    tx,
                    &def,
                    &key,
                    node,
                    Some(commit),
                )?;
            }
            return Ok(());
        }
        let mut state = self.take_txn_state(tx);
        let buffer = state
            .deferred_index
            .get_or_insert_with(IndexBuffer::default);
        if insert {
            buffer.prop_inserts.push((def, key, node, commit));
        } else {
            buffer.prop_removes.push((def, key, node, commit));
        }
        self.store_txn_state(tx, state);
        Ok(())
    }

    fn flush_deferred_indexes(&self, tx: &mut WriteGuard<'_>) -> Result<()> {
        if !self.defer_index_flush {
            return Ok(());
        }
        let mut state = self.take_txn_state(tx);
        let Some(mut buffer) = state.deferred_index.take() else {
            self.store_txn_state(tx, state);
            return Ok(());
        };
        for (label, node, commit) in buffer.label_inserts.drain(..) {
            if self.indexes.has_label_index_with_write(tx, label)? {
                self.indexes
                    .insert_node_labels_with_commit(tx, node, &[label], Some(commit))?;
            }
        }
        for (label, node, commit) in buffer.label_removes.drain(..) {
            if self.indexes.has_label_index_with_write(tx, label)? {
                self.indexes
                    .remove_node_labels_with_commit(tx, node, &[label], Some(commit))?;
            }
        }
        for (def, key, node, commit) in buffer.prop_inserts.drain(..) {
            self.indexes
                .insert_property_value_with_commit(tx, &def, &key, node, Some(commit))?;
        }
        for (def, key, node, commit) in buffer.prop_removes.drain(..) {
            self.indexes
                .remove_property_value_with_commit(tx, &def, &key, node, Some(commit))?;
        }
        self.store_txn_state(tx, state);
        Ok(())
    }

    fn insert_adjacencies(
        &self,
        tx: &mut WriteGuard<'_>,
        entries: &[(NodeId, NodeId, TypeId, EdgeId)],
        commit: CommitId,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut keys: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(entries.len());
        for (src, dst, ty, edge) in entries {
            let fwd_key = adjacency::encode_fwd_key(*src, *ty, *dst, *edge);
            let rev_key = adjacency::encode_rev_key(*dst, *ty, *src, *edge);
            keys.push((fwd_key, rev_key));
        }
        let versioned_unit = Self::adjacency_value_for_commit(commit, false);
        {
            let mut refs: Vec<&Vec<u8>> = keys.iter().map(|(fwd, _)| fwd).collect();
            refs.sort_unstable();
            let value_ref = &versioned_unit;
            let iter = refs.into_iter().map(|key| PutItem {
                key,
                value: value_ref,
            });
            self.adj_fwd.put_many(tx, iter)?;
            self.persist_tree_root(tx, RootKind::AdjFwd)?;
        }
        {
            let mut refs: Vec<&Vec<u8>> = keys.iter().map(|(_, rev)| rev).collect();
            refs.sort_unstable();
            let value_ref = &versioned_unit;
            let iter = refs.into_iter().map(|key| PutItem {
                key,
                value: value_ref,
            });
            if let Err(err) = self.adj_rev.put_many(tx, iter) {
                self.rollback_adjacency_batch(tx, &keys)?;
                return Err(err);
            }
            self.persist_tree_root(tx, RootKind::AdjRev)?;
        }
        #[cfg(feature = "degree-cache")]
        if self.degree_cache_enabled {
            let mut deltas: BTreeMap<(NodeId, DegreeDir, TypeId), i64> = BTreeMap::new();
            for (src, dst, ty, _edge) in entries {
                *deltas.entry((*src, DegreeDir::Out, *ty)).or_default() += 1;
                *deltas.entry((*dst, DegreeDir::In, *ty)).or_default() += 1;
            }
            if let Err(err) = self.apply_degree_batch(tx, &deltas) {
                self.rollback_adjacency_batch(tx, &keys)?;
                return Err(err);
            }
        }
        self.finalize_adjacency_entries(tx, &keys)?;
        Ok(())
    }

    fn remove_adjacency(
        &self,
        tx: &mut WriteGuard<'_>,
        src: NodeId,
        dst: NodeId,
        ty: TypeId,
        edge: EdgeId,
        commit: CommitId,
    ) -> Result<()> {
        let fwd_key = adjacency::encode_fwd_key(src, ty, dst, edge);
        let rev_key = adjacency::encode_rev_key(dst, ty, src, edge);
        let mut retire_entry =
            |tree: &BTree<Vec<u8>, VersionedValue<UnitValue>>, key: &Vec<u8>| -> Result<()> {
                let Some(mut current) = tree.get_with_write(tx, key)? else {
                    return Err(SombraError::Corruption(
                        "adjacency entry missing during delete",
                    ));
                };
                if current.header.end != COMMIT_MAX {
                    return Err(SombraError::Corruption("adjacency entry already retired"));
                }
                current.header.end = commit;
                tree.put(tx, key, &current)
            };
        retire_entry(&self.adj_fwd, &fwd_key)?;
        retire_entry(&self.adj_rev, &rev_key)?;
        self.persist_tree_root(tx, RootKind::AdjFwd)?;
        self.persist_tree_root(tx, RootKind::AdjRev)?;
        #[cfg(feature = "degree-cache")]
        if self.degree_cache_enabled {
            self.bump_degree(tx, src, DegreeDir::Out, ty, -1)?;
            self.bump_degree(tx, dst, DegreeDir::In, ty, -1)?;
        }
        Ok(())
    }

    fn collect_incident_edges(&self, read: &ReadGuard, node: NodeId) -> Result<HashSet<EdgeId>> {
        let mut edges = HashSet::new();
        self.collect_adjacent_edges(read, node, true, &mut edges)?;
        self.collect_adjacent_edges(read, node, false, &mut edges)?;
        Ok(edges)
    }

    fn collect_adjacent_edges(
        &self,
        read: &ReadGuard,
        node: NodeId,
        forward: bool,
        edges: &mut HashSet<EdgeId>,
    ) -> Result<()> {
        let (lo, hi) = adjacency_bounds_for_node(node);
        let tree = if forward {
            &self.adj_fwd
        } else {
            &self.adj_rev
        };
        let snapshot = Self::reader_snapshot_commit(read);
        let mut cursor = tree.range(read, Bound::Included(lo), Bound::Included(hi))?;
        while let Some((key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            let decoded = if forward {
                adjacency::decode_fwd_key(&key)
            } else {
                adjacency::decode_rev_key(&key)
            }
            .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
            let edge_id = decoded.3;
            if self.visible_edge(read, edge_id)?.is_none() {
                continue;
            }
            edges.insert(edge_id);
        }
        Ok(())
    }

    fn collect_neighbors(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        ty_filter: Option<TypeId>,
        forward: bool,
        seen: Option<&mut HashSet<NodeId>>,
        out: &mut Vec<Neighbor>,
    ) -> Result<()> {
        let (lo, hi) = if forward {
            adjacency::fwd_bounds(node, ty_filter)
        } else {
            adjacency::rev_bounds(node, ty_filter)
        };
        let tree = if forward {
            &self.adj_fwd
        } else {
            &self.adj_rev
        };
        let snapshot = Self::reader_snapshot_commit(tx);
        let mut cursor = tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
        let mut seen = seen;
        while let Some((key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            if forward {
                let (src, ty, dst, edge) = adjacency::decode_fwd_key(&key)
                    .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
                debug_assert_eq!(src, node);
                match src.cmp(&node) {
                    CmpOrdering::Less => continue,
                    CmpOrdering::Greater => break,
                    CmpOrdering::Equal => {}
                }
                if let Some(filter) = ty_filter {
                    if ty != filter {
                        continue;
                    }
                }
                if self.visible_edge(tx, edge)?.is_none() {
                    continue;
                }
                if let Some(set) = seen.as_deref_mut() {
                    if !set.insert(dst) {
                        continue;
                    }
                }
                out.push(Neighbor {
                    neighbor: dst,
                    edge,
                    ty,
                });
            } else {
                let (dst, ty, src, edge) = adjacency::decode_rev_key(&key)
                    .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
                debug_assert_eq!(dst, node);
                match dst.cmp(&node) {
                    CmpOrdering::Less => continue,
                    CmpOrdering::Greater => break,
                    CmpOrdering::Equal => {}
                }
                if let Some(filter) = ty_filter {
                    if ty != filter {
                        continue;
                    }
                }
                if self.visible_edge(tx, edge)?.is_none() {
                    continue;
                }
                if let Some(set) = seen.as_deref_mut() {
                    if !set.insert(src) {
                        continue;
                    }
                }
                out.push(Neighbor {
                    neighbor: src,
                    edge,
                    ty,
                });
            }
        }
        Ok(())
    }

    fn enqueue_bfs_neighbors(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        dir: Dir,
        ty_filter: Option<TypeId>,
        next_depth: u32,
        seen: &mut HashSet<NodeId>,
        queue: &mut VecDeque<(NodeId, u32)>,
    ) -> Result<()> {
        let cursor = self.neighbors(
            tx,
            node,
            dir,
            ty_filter,
            ExpandOpts {
                distinct_nodes: false,
            },
        )?;
        for neighbor in cursor {
            if seen.insert(neighbor.neighbor) {
                queue.push_back((neighbor.neighbor, next_depth));
            }
        }
        Ok(())
    }

    fn degree_single(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        forward: bool,
        ty: Option<TypeId>,
    ) -> Result<u64> {
        #[cfg(feature = "degree-cache")]
        {
            if self.degree_cache_enabled {
                if let Some(tree) = &self.degree {
                    let dir = if forward {
                        DegreeDir::Out
                    } else {
                        DegreeDir::In
                    };
                    match ty {
                        Some(ty) => {
                            let key = adjacency::encode_degree_key(node, dir, ty);
                            if let Some(value) = tree.get(tx, &key)? {
                                return Ok(value);
                            }
                        }
                        None => {
                            let (lo, hi) = adjacency::degree_bounds(node, dir, None);
                            let mut cursor =
                                tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
                            let mut total = 0u64;
                            while let Some((_key, value)) = cursor.next()? {
                                total = total.saturating_add(value);
                            }
                            return Ok(total);
                        }
                    }
                }
            }
        }
        self.count_adjacent_edges(tx, node, ty, forward)
    }

    fn degree_has_cache_entry(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        dir: Dir,
        ty: Option<TypeId>,
    ) -> Result<bool> {
        #[cfg(feature = "degree-cache")]
        {
            if !self.degree_cache_enabled {
                return Ok(false);
            }
            let Some(tree) = &self.degree else {
                return Ok(false);
            };
            let result = match dir {
                Dir::Out => {
                    let tag = DegreeDir::Out;
                    match ty {
                        Some(ty) => {
                            let key = adjacency::encode_degree_key(node, tag, ty);
                            tree.get(tx, &key)?.is_some()
                        }
                        None => {
                            let (lo, hi) = adjacency::degree_bounds(node, tag, None);
                            let mut cursor =
                                tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
                            cursor.next()?.is_some()
                        }
                    }
                }
                Dir::In => {
                    let tag = DegreeDir::In;
                    match ty {
                        Some(ty) => {
                            let key = adjacency::encode_degree_key(node, tag, ty);
                            tree.get(tx, &key)?.is_some()
                        }
                        None => {
                            let (lo, hi) = adjacency::degree_bounds(node, tag, None);
                            let mut cursor =
                                tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
                            cursor.next()?.is_some()
                        }
                    }
                }
                Dir::Both => {
                    let out_has = self.degree_has_cache_entry(tx, node, Dir::Out, ty)?;
                    let in_has = self.degree_has_cache_entry(tx, node, Dir::In, ty)?;
                    out_has && in_has
                }
            };
            Ok(result)
        }
        #[cfg(not(feature = "degree-cache"))]
        {
            let _ = (tx, node, dir, ty);
            return Ok(false);
        }
    }
    fn count_adjacent_edges(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        ty: Option<TypeId>,
        forward: bool,
    ) -> Result<u64> {
        let (lo, hi) = if forward {
            adjacency::fwd_bounds(node, ty)
        } else {
            adjacency::rev_bounds(node, ty)
        };
        let tree = if forward {
            &self.adj_fwd
        } else {
            &self.adj_rev
        };
        let mut cursor = tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
        let mut count = 0u64;
        let snapshot = Self::reader_snapshot_commit(tx);
        while let Some((_key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    fn count_loop_edges(&self, tx: &ReadGuard, node: NodeId, ty: Option<TypeId>) -> Result<u64> {
        let (lo, hi) = adjacency::fwd_bounds(node, ty);
        let mut cursor = self
            .adj_fwd
            .range(tx, Bound::Included(lo), Bound::Included(hi))?;
        let mut loops = 0u64;
        let snapshot = Self::reader_snapshot_commit(tx);
        while let Some((key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            let (src, ty_val, dst, _edge) = adjacency::decode_fwd_key(&key)
                .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
            debug_assert_eq!(src, node);
            if dst == node && ty.map(|t| t == ty_val).unwrap_or(true) {
                loops = loops.saturating_add(1);
            }
        }
        Ok(loops)
    }

    fn free_edge_props(&self, tx: &mut WriteGuard<'_>, props: EdgePropStorage) -> Result<()> {
        match props {
            EdgePropStorage::Inline(bytes) => self.free_prop_values_from_bytes(tx, &bytes),
            EdgePropStorage::VRef(vref) => {
                let bytes = self.vstore.read_with_write(tx, vref)?;
                self.free_prop_values_from_bytes(tx, &bytes)?;
                self.vstore.free(tx, vref)
            }
        }
    }

    fn retire_version_resources(
        &self,
        tx: &mut WriteGuard<'_>,
        entry: &VersionLogEntry,
    ) -> Result<()> {
        match entry.space {
            VersionSpace::Node => {
                let versioned = node::decode(&entry.bytes)?;
                self.free_node_props(tx, versioned.row.props)
            }
            VersionSpace::Edge => {
                let versioned = edge::decode(&entry.bytes)?;
                self.free_edge_props(tx, versioned.row.props)
            }
        }
    }

    fn prune_versioned_vec_tree<V: ValCodec>(
        tree: &BTree<Vec<u8>, VersionedValue<V>>,
        tx: &mut WriteGuard<'_>,
        horizon: CommitId,
    ) -> Result<u64> {
        let mut keys = Vec::new();
        tree.for_each_with_write(tx, |key, value| {
            if value.header.end != COMMIT_MAX && value.header.end <= horizon {
                keys.push(key);
            }
            Ok(())
        })?;
        let mut pruned = 0u64;
        for key in keys {
            if tree.delete(tx, &key)? {
                pruned = pruned.saturating_add(1);
            }
        }
        Ok(pruned)
    }

    fn maybe_signal_high_water(&self) {
        let threshold = self.vacuum_cfg.log_high_water_bytes;
        if threshold == 0 {
            return;
        }
        if self.version_log_bytes() < threshold {
            return;
        }
        if !matches!(
            self.vacuum_sched.pending_trigger.get(),
            Some(VacuumTrigger::Manual)
        ) {
            self.vacuum_sched
                .pending_trigger
                .set(Some(VacuumTrigger::HighWater));
        }
        self.vacuum_sched.next_deadline_ms.set(0);
    }

    fn compute_vacuum_horizon(&self) -> Option<CommitId> {
        if let Some(table) = &self.commit_table {
            let guard = table.lock();
            Some(guard.vacuum_horizon(self.vacuum_cfg.retention_window))
        } else {
            Some(COMMIT_MAX)
        }
    }

    fn select_vacuum_mode(&self) -> VacuumMode {
        let bytes = self.version_log_bytes();
        let high_water = self.vacuum_cfg.log_high_water_bytes;
        let stats = self.store.stats();
        let retention_ms = self.vacuum_cfg.retention_window.as_millis().max(1) as u64;
        let reader_lag_ms = stats.mvcc_reader_max_age_ms;
        let reader_lag_ratio = (reader_lag_ms as f64 / retention_ms as f64).min(1.0);
        let fast_due_to_lag = reader_lag_ratio >= 0.8;
        let slow_due_to_lag = reader_lag_ratio <= 0.25;
        let fast_due_to_bytes = high_water > 0 && bytes >= high_water.saturating_mul(3) / 2;
        let slow_due_to_bytes = high_water > 0 && bytes <= high_water / 4;
        let mode = if fast_due_to_bytes || fast_due_to_lag {
            VacuumMode::Fast
        } else if slow_due_to_bytes && slow_due_to_lag {
            VacuumMode::Slow
        } else {
            VacuumMode::Normal
        };
        let mode_label = match mode {
            VacuumMode::Fast => "fast",
            VacuumMode::Normal => "normal",
            VacuumMode::Slow => "slow",
        };
        self.metrics.mvcc_vacuum_mode(mode_label);
        mode
    }

    fn recompute_version_log_bytes(&self) -> Result<()> {
        let mut total_bytes = 0u64;
        let mut total_entries = 0u64;
        let mut write = self.begin_write_guard()?;
        self.version_log
            .for_each_with_write(&mut write, |_, bytes| {
                total_bytes = total_bytes.saturating_add(bytes.len() as u64);
                total_entries = total_entries.saturating_add(1);
                Ok(())
            })?;
        drop(write);
        self.version_log_bytes
            .store(total_bytes, AtomicOrdering::Relaxed);
        self.version_log_entries
            .store(total_entries, AtomicOrdering::Relaxed);
        self.publish_version_log_usage_metrics();
        Ok(())
    }

    fn record_vacuum_stats(&self, stats: GraphVacuumStats) {
        self.publish_vacuum_metrics(&stats);
        *self.vacuum_sched.last_stats.borrow_mut() = Some(stats.clone());
        self.log_vacuum_stats(&stats);
    }

    fn publish_vacuum_metrics(&self, stats: &GraphVacuumStats) {
        self.metrics
            .vacuum_versions_pruned(stats.log_versions_pruned);
        self.metrics
            .vacuum_orphan_versions_pruned(stats.orphan_log_versions_pruned);
        self.metrics
            .vacuum_tombstone_heads_purged(stats.heads_purged);
        self.metrics
            .vacuum_adjacency_pruned(stats.adjacency_fwd_pruned, stats.adjacency_rev_pruned);
        self.metrics.vacuum_index_entries_pruned(
            stats.index_label_pruned,
            stats.index_chunked_pruned,
            stats.index_btree_pruned,
        );
        self.metrics.vacuum_bytes_reclaimed(stats.bytes_reclaimed);
        self.metrics.vacuum_run_millis(stats.run_millis);
        self.metrics.vacuum_horizon_commit(stats.horizon_commit);
        self.publish_version_log_usage_metrics();
    }

    fn log_vacuum_stats(&self, stats: &GraphVacuumStats) {
        let made_progress = stats.log_versions_pruned > 0
            || stats.orphan_log_versions_pruned > 0
            || stats.heads_purged > 0
            || stats.adjacency_fwd_pruned > 0
            || stats.adjacency_rev_pruned > 0
            || stats.index_label_pruned > 0
            || stats.index_chunked_pruned > 0
            || stats.index_btree_pruned > 0
            || stats.bytes_reclaimed > 0;
        if made_progress {
            info!(
                horizon = stats.horizon_commit,
                trigger = ?stats.trigger,
                run_millis = stats.run_millis,
                versions = stats.log_versions_pruned,
                orphan_versions = stats.orphan_log_versions_pruned,
                tombstone_heads = stats.heads_purged,
                adj_fwd = stats.adjacency_fwd_pruned,
                adj_rev = stats.adjacency_rev_pruned,
                index_label = stats.index_label_pruned,
                index_chunked = stats.index_chunked_pruned,
                index_btree = stats.index_btree_pruned,
                bytes_reclaimed = stats.bytes_reclaimed,
                "graph.vacuum.completed"
            );
            return;
        }
        debug!(
            horizon = stats.horizon_commit,
            trigger = ?stats.trigger,
            run_millis = stats.run_millis,
            "graph.vacuum.noop"
        );
    }

    fn free_prop_values_from_bytes(&self, tx: &mut WriteGuard<'_>, bytes: &[u8]) -> Result<()> {
        let raw = props::decode_raw(bytes)?;
        for entry in raw {
            match entry.value {
                RawPropValue::StrVRef(vref) | RawPropValue::BytesVRef(vref) => {
                    self.vstore.free(tx, vref)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn free_node_props(&self, tx: &mut WriteGuard<'_>, props: NodePropStorage) -> Result<()> {
        match props {
            NodePropStorage::Inline(bytes) => self.free_prop_values_from_bytes(tx, &bytes),
            NodePropStorage::VRef(vref) => {
                let bytes = self.vstore.read_with_write(tx, vref)?;
                self.free_prop_values_from_bytes(tx, &bytes)?;
                self.vstore.free(tx, vref)
            }
        }
    }

    fn read_node_prop_bytes(&self, storage: &NodePropStorage) -> Result<Vec<u8>> {
        match storage {
            NodePropStorage::Inline(bytes) => Ok(bytes.clone()),
            NodePropStorage::VRef(vref) => {
                let read = self.lease_latest_snapshot()?;
                self.vstore.read(&read, *vref)
            }
        }
    }

    fn read_node_prop_bytes_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        storage: &NodePropStorage,
    ) -> Result<Vec<u8>> {
        match storage {
            NodePropStorage::Inline(bytes) => Ok(bytes.clone()),
            NodePropStorage::VRef(vref) => self.vstore.read_with_write(tx, *vref),
        }
    }

    fn read_edge_prop_bytes_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        storage: &EdgePropStorage,
    ) -> Result<Vec<u8>> {
        match storage {
            EdgePropStorage::Inline(bytes) => Ok(bytes.clone()),
            EdgePropStorage::VRef(vref) => self.vstore.read_with_write(tx, *vref),
        }
    }

    fn materialize_props_owned(&self, bytes: &[u8]) -> Result<Vec<(PropId, PropValueOwned)>> {
        let raw = props::decode_raw(bytes)?;
        let read = self.lease_latest_snapshot()?;
        let props = props::materialize_props(&raw, &self.vstore, &read)?;
        Ok(props)
    }

    fn materialize_props_owned_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        bytes: &[u8],
    ) -> Result<Vec<(PropId, PropValueOwned)>> {
        let raw = props::decode_raw(bytes)?;
        props::materialize_props_with_write(&raw, &self.vstore, tx)
    }

    #[cfg(feature = "degree-cache")]
    fn bump_degree(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: DegreeDir,
        ty: TypeId,
        delta: i64,
    ) -> Result<()> {
        if !self.degree_cache_enabled {
            return Ok(());
        }
        let Some(tree) = &self.degree else {
            return Ok(());
        };
        let key = adjacency::encode_degree_key(node, dir, ty);
        let current = tree.get_with_write(tx, &key)?;
        let current_val = current.unwrap_or(0);
        let new_val = if delta.is_negative() {
            let abs = delta.unsigned_abs();
            if abs > current_val {
                return Err(SombraError::Corruption("degree underflow"));
            }
            current_val - abs
        } else {
            current_val.saturating_add(delta as u64)
        };
        if new_val == 0 {
            let removed = tree.delete(tx, &key)?;
            if delta.is_negative() && !removed {
                return Err(SombraError::Corruption(
                    "degree entry missing during delete",
                ));
            }
        } else {
            tree.put(tx, &key, &new_val)?;
        }
        self.persist_tree_root(tx, RootKind::Degree)?;
        Ok(())
    }
}

fn now_millis() -> u128 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

impl Drop for Graph {
    fn drop(&mut self) {}
}

impl BackgroundMaintainer for Graph {
    fn run_background_maint(&self, _ctx: &AutockptContext) {
        self.drive_micro_gc(MicroGcTrigger::PostCommit);
        self.maybe_background_vacuum(VacuumTrigger::Timer);
    }
}

const TRUST_VALIDATOR_REQUIRED: &str = "trusted endpoints require validator";
const TRUST_BATCH_REQUIRED: &str = "trusted endpoints batch must be validated";

/// Options controlling how [`GraphWriter`] inserts edges.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateEdgeOptions {
    /// Whether edge endpoints have been validated externally and can skip lookups.
    pub trusted_endpoints: bool,
    /// Capacity of the node-existence cache when validation is required.
    pub exists_cache_capacity: usize,
}

impl Default for CreateEdgeOptions {
    fn default() -> Self {
        Self {
            trusted_endpoints: false,
            exists_cache_capacity: 1024,
        }
    }
}

/// Aggregate statistics captured by [`GraphWriter`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GraphWriterStats {
    /// Number of cache hits for endpoint existence checks.
    pub exists_cache_hits: u64,
    /// Number of cache misses for endpoint existence checks.
    pub exists_cache_misses: u64,
    /// Number of edges inserted using trusted endpoints.
    pub trusted_edges: u64,
    /// Oldest reader commit observed when stats were captured.
    pub oldest_reader_commit: CommitId,
}

/// Validator used by [`GraphWriter`] to confirm endpoints exist before trusting batches.
pub trait BulkEdgeValidator {
    /// Validates a batch of `(src, dst)` pairs before inserts begin.
    fn validate_batch(&self, edges: &[(NodeId, NodeId)]) -> Result<()>;
}

/// Batched edge writer that amortizes endpoint probes and supports trusted inserts.
pub struct GraphWriter<'a> {
    graph: &'a Graph,
    opts: CreateEdgeOptions,
    exists_cache: Option<LruCache<NodeId, bool>>,
    validator: Option<Box<dyn BulkEdgeValidator + 'a>>,
    stats: GraphWriterStats,
    trust_budget: usize,
}

impl<'a> GraphWriter<'a> {
    /// Constructs a new writer for the provided [`Graph`].
    pub fn try_new(
        graph: &'a Graph,
        opts: CreateEdgeOptions,
        validator: Option<Box<dyn BulkEdgeValidator + 'a>>,
    ) -> Result<Self> {
        if opts.trusted_endpoints && validator.is_none() {
            return Err(SombraError::Invalid(TRUST_VALIDATOR_REQUIRED));
        }
        let exists_cache = NonZeroUsize::new(opts.exists_cache_capacity).map(LruCache::new);
        Ok(Self {
            graph,
            opts,
            exists_cache,
            validator,
            stats: GraphWriterStats::default(),
            trust_budget: 0,
        })
    }

    /// Returns the options associated with this writer.
    pub fn options(&self) -> &CreateEdgeOptions {
        &self.opts
    }

    /// Returns current statistics collected by the writer.
    pub fn stats(&self) -> GraphWriterStats {
        let mut stats = self.stats;
        if let Some(oldest) = self.graph.oldest_reader_commit() {
            stats.oldest_reader_commit = oldest;
        }
        stats
    }

    /// Validates a batch of edges before inserting them in trusted mode.
    pub fn validate_trusted_batch(&mut self, edges: &[(NodeId, NodeId)]) -> Result<()> {
        if !self.opts.trusted_endpoints {
            return Ok(());
        }
        let Some(validator) = self.validator.as_ref() else {
            return Err(SombraError::Invalid(TRUST_VALIDATOR_REQUIRED));
        };
        validator.validate_batch(edges)?;
        self.trust_budget = edges.len();
        Ok(())
    }

    /// Creates an edge with the configured validation strategy.
    pub fn create_edge(&mut self, tx: &mut WriteGuard<'_>, spec: EdgeSpec<'_>) -> Result<EdgeId> {
        if self.opts.trusted_endpoints {
            if self.trust_budget == 0 {
                return Err(SombraError::Invalid(TRUST_BATCH_REQUIRED));
            }
            self.trust_budget -= 1;
            self.stats.trusted_edges = self.stats.trusted_edges.saturating_add(1);
        } else {
            self.ensure_endpoint(tx, spec.src, "edge source node missing")?;
            self.ensure_endpoint(tx, spec.dst, "edge destination node missing")?;
        }
        self.graph.insert_edge_unchecked(tx, spec)
    }

    fn ensure_endpoint(
        &mut self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        context: &'static str,
    ) -> Result<()> {
        if let Some(cache) = self.exists_cache.as_mut() {
            if let Some(hit) = cache.get(&node).copied() {
                self.stats.exists_cache_hits = self.stats.exists_cache_hits.saturating_add(1);
                if hit {
                    return Ok(());
                }
                return Err(SombraError::Invalid(context));
            }
        }
        let exists = self.graph.node_exists_with_write(tx, node)?;
        if let Some(cache) = self.exists_cache.as_mut() {
            cache.put(node, exists);
        }
        self.stats.exists_cache_misses = self.stats.exists_cache_misses.saturating_add(1);
        if exists {
            Ok(())
        } else {
            Err(SombraError::Invalid(context))
        }
    }
}

fn open_u64_vec_tree(store: &Arc<dyn PageStore>, root: PageId) -> Result<BTree<u64, Vec<u8>>> {
    let mut opts = BTreeOptions::default();
    opts.root_page = (root.0 != 0).then_some(root);
    BTree::open_or_create(store, opts)
}

fn open_unit_tree(
    store: &Arc<dyn PageStore>,
    root: PageId,
) -> Result<BTree<Vec<u8>, VersionedValue<UnitValue>>> {
    let mut opts = BTreeOptions::default();
    opts.root_page = (root.0 != 0).then_some(root);
    BTree::open_or_create(store, opts)
}

#[cfg(feature = "degree-cache")]
fn open_degree_tree(store: &Arc<dyn PageStore>, root: PageId) -> Result<BTree<Vec<u8>, u64>> {
    let mut opts = BTreeOptions::default();
    opts.root_page = (root.0 != 0).then_some(root);
    BTree::open_or_create(store, opts)
}

struct GraphTxnState {
    index_cache: GraphIndexCache,
    deferred_adj: Option<AdjacencyBuffer>,
    deferred_index: Option<IndexBuffer>,
}

impl GraphTxnState {
    fn new(epoch: DdlEpoch) -> Self {
        Self {
            index_cache: GraphIndexCache::new(epoch),
            deferred_adj: None,
            deferred_index: None,
        }
    }
}

struct PropDelta {
    old_map: BTreeMap<PropId, PropValueOwned>,
    new_map: BTreeMap<PropId, PropValueOwned>,
    encoded: props::PropEncodeResult,
}

fn apply_patch_ops(map: &mut BTreeMap<PropId, PropValueOwned>, ops: &[PropPatchOp<'_>]) {
    for op in ops {
        match op {
            PropPatchOp::Set(prop, value) => {
                map.insert(*prop, prop_value_to_owned(value.clone()));
            }
            PropPatchOp::Delete(prop) => {
                map.remove(prop);
            }
        }
    }
}

fn encode_value_key_owned(ty: TypeTag, value: &PropValueOwned) -> Result<Vec<u8>> {
    match (ty, value) {
        (TypeTag::Null, PropValueOwned::Null) => Ok(Vec::new()),
        (TypeTag::Bool, PropValueOwned::Bool(v)) => Ok(vec![u8::from(*v)]),
        (TypeTag::Int, PropValueOwned::Int(v)) => Ok(encode_i64_key(*v).to_vec()),
        (TypeTag::Float, PropValueOwned::Float(v)) => encode_f64_key(*v),
        (TypeTag::String, PropValueOwned::Str(s)) => encode_bytes_key(s.as_bytes()),
        (TypeTag::Bytes, PropValueOwned::Bytes(b)) => encode_bytes_key(b),
        (TypeTag::Date, PropValueOwned::Date(v)) => Ok(encode_i64_key(*v).to_vec()),
        (TypeTag::DateTime, PropValueOwned::DateTime(v)) => Ok(encode_i64_key(*v).to_vec()),
        _ => Err(SombraError::Invalid(
            "property value type mismatch for index",
        )),
    }
}

fn encode_i64_key(value: i64) -> [u8; 8] {
    ((value as u64) ^ 0x8000_0000_0000_0000).to_be_bytes()
}

fn encode_f64_key(value: f64) -> Result<Vec<u8>> {
    if value.is_nan() {
        return Err(SombraError::Invalid("NaN values cannot be indexed"));
    }
    let bits = value.to_bits();
    let normalized = if bits & 0x8000_0000_0000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000_0000_0000
    };
    Ok(normalized.to_be_bytes().to_vec())
}

fn encode_bytes_key(bytes: &[u8]) -> Result<Vec<u8>> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| SombraError::Invalid("property value exceeds maximum length"))?;
    let mut out = Vec::with_capacity(4 + bytes.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(out)
}

fn prop_stats_key(value: &PropValueOwned) -> Vec<u8> {
    use PropValueOwned::*;
    let mut out = Vec::new();
    match value {
        Null => out.push(0),
        Bool(v) => {
            out.push(1);
            out.push(u8::from(*v));
        }
        Int(v) => {
            out.push(2);
            out.extend_from_slice(&encode_i64_key(*v));
        }
        Float(v) => {
            out.push(3);
            out.extend_from_slice(&encode_f64_key(*v).unwrap_or_else(|_| vec![0; 8]));
        }
        Str(v) => {
            out.push(4);
            out.extend(encode_bytes_key(v.as_bytes()).unwrap_or_else(|_| v.as_bytes().to_vec()));
        }
        Bytes(v) => {
            out.push(5);
            out.extend(encode_bytes_key(v).unwrap_or_else(|_| v.clone()));
        }
        Date(v) => {
            out.push(6);
            out.extend_from_slice(&encode_i64_key(*v));
        }
        DateTime(v) => {
            out.push(7);
            out.extend_from_slice(&encode_i64_key(*v));
        }
    }
    out
}

fn update_min_max(
    slot: &mut Option<PropValueOwned>,
    candidate: &PropValueOwned,
    desired: Ordering,
) -> Result<()> {
    match slot {
        Some(current) => {
            if compare_prop_values(candidate, current)? == desired {
                *slot = Some(candidate.clone());
            }
        }
        None => {
            *slot = Some(candidate.clone());
        }
    }
    Ok(())
}

fn compare_prop_values(a: &PropValueOwned, b: &PropValueOwned) -> Result<Ordering> {
    use PropValueOwned::*;
    Ok(match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Bool(a), Bool(b)) => a.cmp(b),
        (Int(a), Int(b)) => a.cmp(b),
        (Float(a), Float(b)) => a
            .partial_cmp(b)
            .ok_or(SombraError::Invalid("float comparison invalid"))?,
        (Str(a), Str(b)) => a.cmp(b),
        (Bytes(a), Bytes(b)) => a.cmp(b),
        (Date(a), Date(b)) => a.cmp(b),
        (DateTime(a), DateTime(b)) => a.cmp(b),
        (va, vb) => value_rank(va).cmp(&value_rank(vb)),
    })
}

fn value_rank(value: &PropValueOwned) -> u8 {
    use PropValueOwned::*;
    match value {
        Null => 0,
        Bool(_) => 1,
        Int(_) => 2,
        Float(_) => 3,
        Str(_) => 4,
        Bytes(_) => 5,
        Date(_) => 6,
        DateTime(_) => 7,
    }
}

fn prop_value_to_owned(value: PropValue<'_>) -> PropValueOwned {
    match value {
        PropValue::Null => PropValueOwned::Null,
        PropValue::Bool(v) => PropValueOwned::Bool(v),
        PropValue::Int(v) => PropValueOwned::Int(v),
        PropValue::Float(v) => PropValueOwned::Float(v),
        PropValue::Str(v) => PropValueOwned::Str(v.to_owned()),
        PropValue::Bytes(v) => PropValueOwned::Bytes(v.to_vec()),
        PropValue::Date(v) => PropValueOwned::Date(v),
        PropValue::DateTime(v) => PropValueOwned::DateTime(v),
    }
}

fn encode_range_bound(ty: TypeTag, bound: Bound<&PropValueOwned>) -> Result<Bound<Vec<u8>>> {
    match bound {
        Bound::Unbounded => Ok(Bound::Unbounded),
        Bound::Included(value) => encode_value_key_owned(ty, value).map(Bound::Included),
        Bound::Excluded(value) => encode_value_key_owned(ty, value).map(Bound::Excluded),
    }
}

fn clone_owned_bound(bound: Bound<&PropValueOwned>) -> Bound<PropValueOwned> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(value) => Bound::Included(value.clone()),
        Bound::Excluded(value) => Bound::Excluded(value.clone()),
    }
}

fn collect_posting_stream(stream: &mut dyn PostingStream) -> Result<Vec<NodeId>> {
    let mut nodes = Vec::new();
    collect_all(stream, &mut nodes)?;
    nodes.sort_by_key(|node| node.0);
    nodes.dedup_by_key(|node| node.0);
    Ok(nodes)
}

fn instrument_posting_stream<'a>(
    stream: Box<dyn PostingStream + 'a>,
) -> Box<dyn PostingStream + 'a> {
    if storage_profiling_enabled() {
        Box::new(ProfilingPostingStream { inner: stream })
    } else {
        stream
    }
}

struct ProfilingPostingStream<'a> {
    inner: Box<dyn PostingStream + 'a>,
}

impl PostingStream for ProfilingPostingStream<'_> {
    fn next_batch(&mut self, out: &mut Vec<NodeId>, max: usize) -> Result<bool> {
        let iter_timer = storage_profile_timer();
        let result = self.inner.next_batch(out, max);
        record_storage_profile_timer(StorageProfileKind::PropIndexStreamIter, iter_timer);
        result
    }
}

enum PropertyPredicate {
    Eq(PropValueOwned),
    Range {
        start: Bound<PropValueOwned>,
        end: Bound<PropValueOwned>,
    },
}

struct PropertyFilterStream<'a> {
    graph: &'a Graph,
    tx: &'a ReadGuard,
    inner: Box<dyn PostingStream + 'a>,
    label: LabelId,
    prop: PropId,
    predicate: PropertyPredicate,
    pending: VecDeque<NodeId>,
    scratch: Vec<NodeId>,
    inner_exhausted: bool,
}

impl<'a> PropertyFilterStream<'a> {
    fn new_eq(
        graph: &'a Graph,
        tx: &'a ReadGuard,
        inner: Box<dyn PostingStream + 'a>,
        label: LabelId,
        prop: PropId,
        value: PropValueOwned,
    ) -> Box<dyn PostingStream + 'a> {
        Box::new(Self {
            graph,
            tx,
            inner,
            label,
            prop,
            predicate: PropertyPredicate::Eq(value),
            pending: VecDeque::new(),
            scratch: Vec::new(),
            inner_exhausted: false,
        })
    }

    fn new_range(
        graph: &'a Graph,
        tx: &'a ReadGuard,
        inner: Box<dyn PostingStream + 'a>,
        label: LabelId,
        prop: PropId,
        start: Bound<PropValueOwned>,
        end: Bound<PropValueOwned>,
    ) -> Box<dyn PostingStream + 'a> {
        Box::new(Self {
            graph,
            tx,
            inner,
            label,
            prop,
            predicate: PropertyPredicate::Range { start, end },
            pending: VecDeque::new(),
            scratch: Vec::new(),
            inner_exhausted: false,
        })
    }

    fn fill_pending(&mut self, max: usize) -> Result<()> {
        self.scratch.clear();
        let has_more = self.inner.next_batch(&mut self.scratch, max)?;
        self.inner_exhausted = !has_more;
        for node in &self.scratch {
            if self.node_matches(*node)? {
                self.pending.push_back(*node);
            }
        }
        Ok(())
    }

    fn node_matches(&self, node: NodeId) -> Result<bool> {
        match &self.predicate {
            PropertyPredicate::Eq(expected) => self
                .graph
                .node_matches_property_eq(self.tx, node, self.label, self.prop, expected),
            PropertyPredicate::Range { start, end } => self
                .graph
                .node_matches_property_range(self.tx, node, self.label, self.prop, start, end),
        }
    }
}

impl PostingStream for PropertyFilterStream<'_> {
    fn next_batch(&mut self, out: &mut Vec<NodeId>, max: usize) -> Result<bool> {
        if max == 0 {
            if !self.pending.is_empty() {
                return Ok(true);
            }
            if self.inner_exhausted {
                return Ok(false);
            }
            self.fill_pending(0)?;
            return Ok(!self.pending.is_empty() || !self.inner_exhausted);
        }
        while out.len() < max {
            if let Some(node) = self.pending.pop_front() {
                out.push(node);
                continue;
            }
            if self.inner_exhausted {
                break;
            }
            self.fill_pending(max)?;
            if self.pending.is_empty() && self.inner_exhausted {
                break;
            }
        }
        Ok(!self.pending.is_empty() || !self.inner_exhausted)
    }
}

/// Computes adjacency list bounds for a given node.
fn adjacency_bounds_for_node(node: NodeId) -> (Vec<u8>, Vec<u8>) {
    const SUFFIX_LEN: usize = 4 + 8 + 8;
    let mut lower = Vec::with_capacity(8 + SUFFIX_LEN);
    lower.extend_from_slice(&node.0.to_be_bytes());
    lower.extend_from_slice(&[0u8; SUFFIX_LEN]);
    let mut upper = Vec::with_capacity(8 + SUFFIX_LEN);
    upper.extend_from_slice(&node.0.to_be_bytes());
    upper.extend_from_slice(&[0xFF; SUFFIX_LEN]);
    (lower, upper)
}

/// Normalizes and deduplicates a list of labels.
fn normalize_labels(labels: &[LabelId]) -> Result<Vec<LabelId>> {
    let mut result: Vec<LabelId> = labels.to_vec();
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result.dedup_by(|a, b| a.0 == b.0);
    if result.len() > u8::MAX as usize {
        return Err(SombraError::Invalid("too many labels for node"));
    }
    Ok(result)
}

impl Graph {
    /// Collects all forward adjacency entries for debugging purposes.
    pub fn debug_collect_adj_fwd(
        &self,
        tx: &ReadGuard,
    ) -> Result<Vec<(NodeId, TypeId, NodeId, EdgeId)>> {
        let mut cursor = self.adj_fwd.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut entries = Vec::new();
        let snapshot = Self::reader_snapshot_commit(tx);
        while let Some((key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            let decoded =
                adjacency::decode_fwd_key(&key).ok_or(SombraError::Corruption("adj key decode"))?;
            entries.push(decoded);
        }
        Ok(entries)
    }

    /// Collects all reverse adjacency entries for debugging purposes.
    pub fn debug_collect_adj_rev(
        &self,
        tx: &ReadGuard,
    ) -> Result<Vec<(NodeId, TypeId, NodeId, EdgeId)>> {
        let mut cursor = self.adj_rev.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut entries = Vec::new();
        let snapshot = Self::reader_snapshot_commit(tx);
        while let Some((key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            let decoded =
                adjacency::decode_rev_key(&key).ok_or(SombraError::Corruption("adj key decode"))?;
            entries.push(decoded);
        }
        Ok(entries)
    }

    fn insert_indexed_props(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        labels: &[LabelId],
        props: &BTreeMap<PropId, PropValueOwned>,
        commit: CommitId,
    ) -> Result<()> {
        for label in labels {
            let defs = self.index_defs_for_label(tx, *label)?;
            for def in defs.iter() {
                if let Some(value) = props.get(&def.prop) {
                    let key = encode_value_key_owned(def.ty, value)?;
                    self.indexes.insert_property_value_with_commit(
                        tx,
                        def,
                        &key,
                        node,
                        Some(commit),
                    )?;
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "degree-cache")]
    fn apply_degree_batch(
        &self,
        tx: &mut WriteGuard<'_>,
        deltas: &BTreeMap<(NodeId, DegreeDir, TypeId), i64>,
    ) -> Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }
        let mut applied: Vec<(NodeId, DegreeDir, TypeId, i64)> = Vec::new();
        for ((node, dir, ty), delta) in deltas {
            if *delta == 0 {
                continue;
            }
            if let Err(err) = self.bump_degree(tx, *node, *dir, *ty, *delta) {
                for (node_applied, dir_applied, ty_applied, delta_applied) in
                    applied.into_iter().rev()
                {
                    let _ =
                        self.bump_degree(tx, node_applied, dir_applied, ty_applied, -delta_applied);
                }
                return Err(err);
            }
            applied.push((*node, *dir, *ty, *delta));
        }
        Ok(())
    }

    fn rollback_adjacency_batch(
        &self,
        tx: &mut WriteGuard<'_>,
        keys: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<()> {
        for (fwd, rev) in keys {
            let _ = self.adj_fwd.delete(tx, fwd);
            let _ = self.adj_rev.delete(tx, rev);
        }
        self.persist_tree_root(tx, RootKind::AdjFwd)?;
        self.persist_tree_root(tx, RootKind::AdjRev)?;
        Ok(())
    }

    fn update_indexed_props_for_node(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        labels: &[LabelId],
        old_props: &BTreeMap<PropId, PropValueOwned>,
        new_props: &BTreeMap<PropId, PropValueOwned>,
        commit: CommitId,
    ) -> Result<()> {
        for label in labels {
            let defs = self.index_defs_for_label(tx, *label)?;
            if self.defer_index_flush {
                for def in defs.iter() {
                    let old = old_props.get(&def.prop);
                    let new = new_props.get(&def.prop);
                    match (old, new) {
                        (_, Some(value)) => {
                            let key = encode_value_key_owned(def.ty, value)?;
                            self.stage_prop_index_op(tx, *def, key, node, commit, true)?;
                        }
                        (Some(prev), None) => {
                            let key = encode_value_key_owned(def.ty, prev)?;
                            self.stage_prop_index_op(tx, *def, key, node, commit, false)?;
                        }
                        _ => {}
                    };
                }
                continue;
            }
            for def in defs.iter() {
                let old = old_props.get(&def.prop);
                let new = new_props.get(&def.prop);
                match (old, new) {
                    (_, Some(value)) => {
                        let key = encode_value_key_owned(def.ty, value)?;
                        self.indexes.insert_property_value_with_commit(
                            tx,
                            def,
                            &key,
                            node,
                            Some(commit),
                        )?;
                    }
                    (Some(prev), None) => {
                        let key = encode_value_key_owned(def.ty, prev)?;
                        self.indexes.remove_property_value_with_commit(
                            tx,
                            def,
                            &key,
                            node,
                            Some(commit),
                        )?;
                    }
                    _ => {}
                };
            }
        }
        Ok(())
    }

    fn build_prop_delta(
        &self,
        tx: &mut WriteGuard<'_>,
        prop_bytes: &[u8],
        patch: &PropPatch<'_>,
    ) -> Result<Option<PropDelta>> {
        if patch.is_empty() {
            return Ok(None);
        }
        let current = self.materialize_props_owned_with_write(tx, prop_bytes)?;
        let mut new_map: BTreeMap<PropId, PropValueOwned> = current.into_iter().collect();
        let old_map = new_map.clone();
        apply_patch_ops(&mut new_map, &patch.ops);
        if new_map == old_map {
            return Ok(None);
        }
        let ordered = new_map
            .iter()
            .map(|(prop, value)| (*prop, value.clone()))
            .collect::<Vec<_>>();
        let encoded =
            props::encode_props_owned(&ordered, self.inline_prop_value, &self.vstore, tx)?;
        Ok(Some(PropDelta {
            old_map,
            new_map,
            encoded,
        }))
    }

    fn index_defs_for_label(
        &self,
        tx: &mut WriteGuard<'_>,
        label: LabelId,
    ) -> Result<Arc<Vec<IndexDef>>> {
        let mut state = self.take_txn_state(tx);
        state.index_cache.sync_epoch(self.catalog_epoch.current());
        let result = state.index_cache.get_or_load(label, |label| {
            self.indexes
                .property_indexes_for_label_with_write(tx, label)
        });
        self.store_txn_state(tx, state);
        result
    }

    fn take_txn_state(&self, tx: &mut WriteGuard<'_>) -> GraphTxnState {
        tx.take_extension::<GraphTxnState>()
            .unwrap_or_else(|| GraphTxnState::new(self.catalog_epoch.current()))
    }

    fn store_txn_state(&self, tx: &mut WriteGuard<'_>, mut state: GraphTxnState) {
        let stats = state.index_cache.take_stats();
        self.idx_cache_hits
            .fetch_add(stats.hits, AtomicOrdering::Relaxed);
        self.idx_cache_misses
            .fetch_add(stats.misses, AtomicOrdering::Relaxed);
        tx.store_extension(state);
    }

    fn invalidate_txn_cache(&self, tx: &mut WriteGuard<'_>) {
        if let Some(mut state) = tx.take_extension::<GraphTxnState>() {
            let stats = state.index_cache.take_stats();
            self.idx_cache_hits
                .fetch_add(stats.hits, AtomicOrdering::Relaxed);
            self.idx_cache_misses
                .fetch_add(stats.misses, AtomicOrdering::Relaxed);
        }
    }

    fn bump_ddl_epoch(&self, tx: &mut WriteGuard<'_>) -> Result<()> {
        self.catalog_epoch.bump_in_txn(tx)?;
        self.invalidate_txn_cache(tx);
        Ok(())
    }

    fn sync_index_roots(&self, tx: &mut WriteGuard<'_>) -> Result<()> {
        let roots = self.indexes.roots();
        tx.update_meta(|meta| {
            meta.storage_index_catalog_root = roots.catalog;
            meta.storage_label_index_root = roots.label;
            meta.storage_prop_chunk_root = roots.prop_chunk;
            meta.storage_prop_btree_root = roots.prop_btree;
        })
    }

    fn persist_tree_root(&self, tx: &mut WriteGuard<'_>, kind: RootKind) -> Result<()> {
        match kind {
            RootKind::Nodes => self.persist_root_impl(
                tx,
                &self.nodes_root,
                self.nodes.root_page(),
                |meta, root| {
                    meta.storage_nodes_root = root;
                },
            ),
            RootKind::Edges => self.persist_root_impl(
                tx,
                &self.edges_root,
                self.edges.root_page(),
                |meta, root| {
                    meta.storage_edges_root = root;
                },
            ),
            RootKind::AdjFwd => self.persist_root_impl(
                tx,
                &self.adj_fwd_root,
                self.adj_fwd.root_page(),
                |meta, root| {
                    meta.storage_adj_fwd_root = root;
                },
            ),
            RootKind::AdjRev => self.persist_root_impl(
                tx,
                &self.adj_rev_root,
                self.adj_rev.root_page(),
                |meta, root| {
                    meta.storage_adj_rev_root = root;
                },
            ),
            RootKind::VersionLog => self.persist_root_impl(
                tx,
                &self.version_log_root,
                self.version_log.root_page(),
                |meta, root| {
                    meta.storage_version_log_root = root;
                },
            ),
            #[cfg(feature = "degree-cache")]
            RootKind::Degree => {
                if let Some(tree) = &self.degree {
                    self.persist_root_impl(tx, &self.degree_root, tree.root_page(), |meta, root| {
                        meta.storage_degree_root = root;
                    })
                } else {
                    Ok(())
                }
            }
        }
    }

    fn persist_root_impl<F>(
        &self,
        tx: &mut WriteGuard<'_>,
        cached: &AtomicU64,
        page_id: PageId,
        update: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut crate::primitives::pager::Meta, PageId),
    {
        if cached.load(AtomicOrdering::SeqCst) == page_id.0 {
            return Ok(());
        }
        tx.update_meta(|meta| update(meta, page_id))?;
        cached.store(page_id.0, AtomicOrdering::SeqCst);
        Ok(())
    }

    #[cfg(feature = "degree-cache")]
    /// Returns every stored degree cache entry for debugging purposes.
    pub fn debug_collect_degree(
        &self,
        tx: &ReadGuard,
    ) -> Result<Vec<(NodeId, adjacency::DegreeDir, TypeId, u64)>> {
        let Some(tree) = &self.degree else {
            return Ok(Vec::new());
        };
        let mut cursor = tree.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut rows = Vec::new();
        while let Some((key, value)) = cursor.next()? {
            let (node, dir, ty) = adjacency::decode_degree_key(&key)
                .ok_or(SombraError::Corruption("degree key decode"))?;
            rows.push((node, dir, ty, value));
        }
        Ok(rows)
    }

    #[cfg(feature = "degree-cache")]
    /// Verifies that the degree cache matches the adjacency trees.
    pub fn validate_degree_cache(&self, tx: &ReadGuard) -> Result<()> {
        let Some(tree) = &self.degree else {
            return Ok(());
        };
        let mut actual: HashMap<(NodeId, adjacency::DegreeDir, TypeId), u64> = HashMap::new();
        {
            let mut cursor = self.adj_fwd.range(tx, Bound::Unbounded, Bound::Unbounded)?;
            let snapshot = Self::reader_snapshot_commit(tx);
            while let Some((key, value)) = cursor.next()? {
                if !Self::version_visible(&value.header, snapshot) {
                    continue;
                }
                let (src, ty, _, _) = adjacency::decode_fwd_key(&key)
                    .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
                *actual
                    .entry((src, adjacency::DegreeDir::Out, ty))
                    .or_insert(0) += 1;
            }
        }
        {
            let mut cursor = self.adj_rev.range(tx, Bound::Unbounded, Bound::Unbounded)?;
            let snapshot = Self::reader_snapshot_commit(tx);
            while let Some((key, value)) = cursor.next()? {
                if !Self::version_visible(&value.header, snapshot) {
                    continue;
                }
                let (dst, ty, _, _) = adjacency::decode_rev_key(&key)
                    .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
                *actual
                    .entry((dst, adjacency::DegreeDir::In, ty))
                    .or_insert(0) += 1;
            }
        }

        let mut degree_cursor = tree.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        while let Some((key, stored)) = degree_cursor.next()? {
            let (node, dir, ty) = adjacency::decode_degree_key(&key)
                .ok_or(SombraError::Corruption("degree key decode failed"))?;
            let actual_count = actual.remove(&(node, dir, ty)).unwrap_or(0);
            if actual_count != stored {
                return Err(SombraError::Corruption("degree cache mismatch"));
            }
        }
        if actual.values().any(|count| *count > 0) {
            return Err(SombraError::Corruption(
                "degree cache missing entry for adjacency",
            ));
        }
        Ok(())
    }

    #[cfg(feature = "degree-cache")]
    /// Inserts or removes a single degree cache entry while running tests.
    pub fn debug_set_degree_entry(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: adjacency::DegreeDir,
        ty: TypeId,
        value: u64,
    ) -> Result<()> {
        if let Some(tree) = &self.degree {
            let key = adjacency::encode_degree_key(node, dir, ty);
            if value == 0 {
                let _ = tree.delete(tx, &key)?;
            } else {
                tree.put(tx, &key, &value)?;
            }
        }
        Ok(())
    }
    /// Computes light-weight property statistics for a label/property pair.
    pub fn property_stats(&self, label: LabelId, prop: PropId) -> Result<Option<PropStats>> {
        let read = self.store.begin_latest_committed_read()?;
        let Some(mut scan) = self.indexes.label_scan(&read, label)? else {
            return Ok(None);
        };

        let mut stats = PropStats::default();
        let mut distinct_keys: HashSet<Vec<u8>> = HashSet::new();

        while let Some(node_id) = scan.next()? {
            stats.row_count += 1;
            let Some(node) = self.get_node(&read, node_id)? else {
                continue;
            };
            let value = node
                .props
                .iter()
                .find_map(|(prop_id, value)| (*prop_id == prop).then(|| value.clone()));
            match value {
                Some(PropValueOwned::Null) | None => {
                    stats.null_count += 1;
                }
                Some(value) => {
                    stats.non_null_count += 1;
                    distinct_keys.insert(prop_stats_key(&value));
                    update_min_max(&mut stats.min, &value, Ordering::Less)?;
                    update_min_max(&mut stats.max, &value, Ordering::Greater)?;
                }
            }
        }

        stats.distinct_count = distinct_keys.len() as u64;
        Ok(Some(stats))
    }

    /// Returns the current catalog epoch used for DDL invalidation.
    pub fn catalog_epoch(&self) -> u64 {
        self.catalog_epoch.current().0
    }
}

struct FallbackLabelScan {
    nodes: Vec<NodeId>,
    pos: usize,
}

impl PostingStream for FallbackLabelScan {
    fn next_batch(&mut self, out: &mut Vec<NodeId>, max: usize) -> Result<bool> {
        if max == 0 {
            return Ok(self.pos >= self.nodes.len());
        }
        let mut produced = 0;
        while self.pos < self.nodes.len() && produced < max {
            out.push(self.nodes[self.pos]);
            self.pos += 1;
            produced += 1;
        }
        Ok(self.pos >= self.nodes.len())
    }
}

#[cfg(test)]
mod vacuum_background_tests {
    use super::*;
    use crate::primitives::pager::{PageStore, Pager, PagerOptions};
    use crate::storage::GraphOptions;
    use crate::types::{LabelId, PropId, Result};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;

    fn setup_graph(cfg: VacuumCfg) -> (tempfile::TempDir, Arc<Pager>, Arc<Graph>) {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("vacuum-bg.db");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
        let store: Arc<dyn PageStore> = pager.clone();
        let graph = Graph::open(GraphOptions::new(store).vacuum(cfg)).unwrap();
        (dir, pager, graph)
    }

    fn create_and_delete_node(pager: &Pager, graph: &Graph) -> Result<()> {
        let mut write = pager.begin_write()?;
        let node = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(1)],
                props: &[PropEntry::new(PropId(1), PropValue::Int(1))],
            },
        )?;
        pager.commit(write)?;
        eprintln!("node created");
        let mut write = pager.begin_write()?;
        graph.delete_node(&mut write, node, DeleteNodeOpts::default())?;
        pager.commit(write)?;
        Ok(())
    }

    #[test]
    fn reentrancy_guard_skips_when_running() -> Result<()> {
        let cfg = VacuumCfg {
            enabled: true,
            interval: Duration::from_millis(50),
            retention_window: Duration::from_millis(0),
            log_high_water_bytes: u64::MAX,
            max_pages_per_pass: 32,
            max_millis_per_pass: 50,
            index_cleanup: true,
        };
        let (_tmpdir, pager, graph) = setup_graph(cfg.clone());
        create_and_delete_node(&pager, &graph)?;
        graph.vacuum_sched.last_stats.borrow_mut().take();
        graph.vacuum_sched.running.set(true);
        graph.trigger_vacuum();
        assert!(graph.vacuum_sched.last_stats.borrow().is_none());
        graph.vacuum_sched.running.set(false);
        graph.trigger_vacuum();
        assert!(graph.last_vacuum_stats().is_some());
        drop(graph);
        drop(pager);
        Ok(())
    }

    #[test]
    fn high_water_sets_pending_trigger() -> Result<()> {
        let cfg = VacuumCfg {
            enabled: false,
            interval: Duration::from_secs(60),
            retention_window: Duration::from_millis(0),
            log_high_water_bytes: 1,
            max_pages_per_pass: 16,
            max_millis_per_pass: 10,
            index_cleanup: true,
        };
        let (_tmpdir, pager, graph) = setup_graph(cfg.clone());
        let mut write = pager.begin_write()?;
        let node = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[PropEntry::new(PropId(2), PropValue::Int(2))],
            },
        )?;
        pager.commit(write)?;
        let mut write = pager.begin_write()?;
        graph.delete_node(&mut write, node, DeleteNodeOpts::default())?;
        pager.commit(write)?;
        let bytes = graph.version_log_bytes();
        assert!(
            bytes >= cfg.log_high_water_bytes,
            "version_log_bytes below threshold: {} < {}",
            bytes,
            cfg.log_high_water_bytes
        );
        assert!(matches!(
            graph.vacuum_sched.pending_trigger.get(),
            Some(VacuumTrigger::HighWater)
        ));
        assert_eq!(graph.vacuum_sched.next_deadline_ms.get(), 0);
        drop(graph);
        drop(pager);
        Ok(())
    }

    #[test]
    fn reports_configured_retention_window() -> Result<()> {
        let retention = Duration::from_secs(42);
        let cfg = VacuumCfg {
            enabled: true,
            interval: Duration::from_millis(50),
            retention_window: retention,
            log_high_water_bytes: u64::MAX,
            max_pages_per_pass: 16,
            max_millis_per_pass: 10,
            index_cleanup: true,
        };
        let (_tmpdir, _pager, graph) = setup_graph(cfg);
        assert_eq!(graph.vacuum_retention_window(), retention);
        Ok(())
    }
}

#[cfg(test)]
mod adjacency_commit_tests {
    use super::*;
    use crate::primitives::pager::{PageStore, Pager, PagerOptions};
    use crate::storage::{adjacency, GraphOptions};
    use crate::types::{Result, TypeId};
    use std::ops::Bound;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn setup_graph() -> (tempfile::TempDir, Arc<Pager>, Arc<Graph>) {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("adj-commit.db");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
        let store: Arc<dyn PageStore> = pager.clone();
        let graph = Graph::open(GraphOptions::new(store)).unwrap();
        (dir, pager, graph)
    }

    fn insert_simple_nodes(pager: &Pager, graph: &Graph) -> Result<(NodeId, NodeId)> {
        let mut write = pager.begin_write()?;
        let a = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[],
                props: &[],
            },
        )?;
        let b = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[],
                props: &[],
            },
        )?;
        pager.commit(write)?;
        Ok((a, b))
    }

    fn fetch_edge_commit(graph: &Graph, read: &ReadGuard, edge: EdgeId) -> Result<CommitId> {
        let bytes = graph
            .edges
            .get(read, &edge.0)?
            .ok_or(SombraError::Corruption("edge missing in test"))?;
        let versioned = edge::decode(&bytes)?;
        Ok(versioned.header.begin)
    }

    fn fetch_single_adj_commit(graph: &Graph, read: &ReadGuard) -> Result<CommitId> {
        let mut cursor = graph
            .adj_fwd
            .range(read, Bound::Unbounded, Bound::Unbounded)?;
        let (_, value) = cursor
            .next()?
            .ok_or(SombraError::Corruption("adjacency missing in test"))?;
        Ok(value.header.begin)
    }

    #[test]
    fn adjacency_commit_matches_graph_edges() -> Result<()> {
        let (_tmp, pager, graph) = setup_graph();
        let (src, dst) = insert_simple_nodes(&pager, &graph)?;
        let mut write = pager.begin_write()?;
        let edge_id = graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty: TypeId(1),
                props: &[],
            },
        )?;
        pager.commit(write)?;
        let read = pager.begin_latest_committed_read()?;
        let adj_commit = fetch_single_adj_commit(&graph, &read)?;
        let edge_commit = fetch_edge_commit(&graph, &read, edge_id)?;
        assert_eq!(adj_commit, edge_commit);
        Ok(())
    }

    #[test]
    fn adjacency_commit_matches_graph_writer_edges() -> Result<()> {
        let (_tmp, pager, graph) = setup_graph();
        let (src, dst) = insert_simple_nodes(&pager, &graph)?;
        let mut writer = GraphWriter::try_new(&graph, CreateEdgeOptions::default(), None).unwrap();
        let mut write = pager.begin_write()?;
        let edge_id = writer.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty: TypeId(2),
                props: &[],
            },
        )?;
        pager.commit(write)?;
        let read = pager.begin_latest_committed_read()?;
        let adj_commit = fetch_single_adj_commit(&graph, &read)?;
        let edge_commit = fetch_edge_commit(&graph, &read, edge_id)?;
        assert_eq!(adj_commit, edge_commit);
        Ok(())
    }

    #[test]
    fn adjacency_entries_clear_pending_after_insert() -> Result<()> {
        let (_tmp, pager, graph) = setup_graph();
        let (src, dst) = insert_simple_nodes(&pager, &graph)?;
        let mut write = pager.begin_write()?;
        let ty = TypeId(7);
        let edge_id = graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty,
                props: &[],
            },
        )?;
        let fwd_key = adjacency::encode_fwd_key(src, ty, dst, edge_id);
        let rev_key = adjacency::encode_rev_key(dst, ty, src, edge_id);
        let fwd_value = graph
            .adj_fwd
            .get_with_write(&mut write, &fwd_key)?
            .expect("forward adjacency present");
        assert!(
            !fwd_value.header.is_pending(),
            "forward adjacency must clear pending flag"
        );
        let rev_value = graph
            .adj_rev
            .get_with_write(&mut write, &rev_key)?
            .expect("reverse adjacency present");
        assert!(
            !rev_value.header.is_pending(),
            "reverse adjacency must clear pending flag"
        );
        pager.commit(write)?;
        Ok(())
    }

    #[test]
    fn deferred_adjacency_flushes_before_commit() -> Result<()> {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("adj-deferred.db");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
        let store: Arc<dyn PageStore> = pager.clone();
        let graph = Graph::open(GraphOptions::new(store).defer_adjacency_flush(true)).unwrap();
        let (src, dst) = insert_simple_nodes(&pager, &graph)?;

        let mut write = pager.begin_write()?;
        let edge_id = graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty: TypeId(9),
                props: &[],
            },
        )?;
        let fwd_key = adjacency::encode_fwd_key(src, TypeId(9), dst, edge_id);
        assert!(
            graph
                .adj_fwd
                .get_with_write(&mut write, &fwd_key)?
                .is_none(),
            "adjacency should be buffered until flush"
        );
        graph.flush_deferred_writes(&mut write)?;
        assert!(
            graph
                .adj_fwd
                .get_with_write(&mut write, &fwd_key)?
                .is_some(),
            "adjacency should be visible after flush"
        );
        pager.commit(write)?;
        let read = pager.begin_latest_committed_read()?;
        let adj_commit = fetch_single_adj_commit(&graph, &read)?;
        let edge_commit = fetch_edge_commit(&graph, &read, edge_id)?;
        assert_eq!(adj_commit, edge_commit);
        Ok(())
    }
}

#[cfg(test)]
mod wal_recovery_tests {
    use super::*;
    use crate::primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions};
    use crate::storage::mvcc::VersionLogEntry;
    use crate::storage::node;
    use crate::storage::patch::{PropPatch, PropPatchOp};
    use crate::storage::props;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn wal_recovery_restores_version_log_entries() -> Result<()> {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("mvcc_wal_recovery.db");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
        let store: Arc<dyn PageStore> = pager.clone();
        let graph = Graph::open(GraphOptions::new(store)).unwrap();

        let mut write = pager.begin_write()?;
        let node = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[],
                props: &[PropEntry::new(PropId(1), PropValue::Int(1))],
            },
        )?;
        pager.commit(write)?;

        let mut write = pager.begin_write()?;
        graph.update_node(
            &mut write,
            node,
            PropPatch::new(vec![PropPatchOp::Set(PropId(1), PropValue::Int(2))]),
        )?;
        pager.commit(write)?;
        eprintln!("node updated");
        pager.checkpoint(CheckpointMode::Force)?;
        eprintln!("checkpoint after update");

        drop(graph);
        drop(pager);

        let reopened_store: Arc<dyn PageStore> =
            Arc::new(Pager::open(&path, PagerOptions::default()).unwrap());
        let reopened_graph = Graph::open(GraphOptions::new(Arc::clone(&reopened_store))).unwrap();
        let read = reopened_store.begin_latest_committed_read()?;

        let mut cursor = reopened_graph
            .nodes
            .range(&read, Bound::Unbounded, Bound::Unbounded)?;
        let (_node_key, head_bytes) = cursor.next()?.expect("node present after crash recovery");
        let head = node::decode(&head_bytes)?;
        let head_props = reopened_graph.read_node_prop_bytes(&head.row.props)?;
        let head_raw = props::decode_raw(&head_props)?;
        assert_eq!(head_raw.len(), 1);
        match head_raw[0].value {
            RawPropValue::Int(value) => assert_eq!(value, 2),
            _ => panic!("unexpected property value {:?}", head_raw[0].value),
        }

        let prev_ptr = head.prev_ptr;
        assert!(
            !prev_ptr.is_null(),
            "node update should log previous version"
        );
        let log_bytes = reopened_graph
            .version_log
            .get(&read, &prev_ptr.raw())?
            .expect("version log entry missing after recovery");
        let entry = VersionLogEntry::decode(&log_bytes)?;
        let old_version = node::decode(&entry.bytes)?;
        let old_props_bytes = reopened_graph.read_node_prop_bytes(&old_version.row.props)?;
        let old_raw = props::decode_raw(&old_props_bytes)?;
        assert_eq!(old_raw.len(), 1);
        match old_raw[0].value {
            RawPropValue::Int(value) => assert_eq!(value, 1),
            _ => panic!("unexpected historical property {:?}", old_raw[0].value),
        }

        drop(read);
        drop(reopened_graph);
        drop(reopened_store);
        Ok(())
    }
}

#[cfg(test)]
mod wal_alert_tests {
    use super::*;

    #[test]
    fn recommends_reuse_when_backlog_exceeds_ready_queue() {
        let backlog = WalCommitBacklog {
            pending_commits: 12,
            pending_frames: 20_000,
            worker_running: true,
        };
        let allocator = WalAllocatorStats {
            segment_size_bytes: 64 * 1024 * 1024,
            preallocate_segments: 1,
            ready_segments: 0,
            recycle_segments: 0,
            reused_segments_total: 0,
            created_segments_total: 0,
            allocation_error: None,
        };
        let (alerts, recommended) =
            wal_health(4096, Some(&backlog), Some(&allocator), None, None, None);
        assert!(alerts.iter().any(|a| a.contains("reuse queue short")));
        assert_eq!(recommended, Some(2));
    }

    #[test]
    fn alerts_on_async_fsync_lag_and_horizon_gap() {
        let async_fsync = AsyncFsyncBacklog {
            pending_lsn: Lsn(9000),
            durable_lsn: Lsn(100),
            pending_lag: ASYNC_FSYNC_LAG_ALERT + 10,
            last_error: None,
        };
        let (alerts, recommended) = wal_health(
            4096,
            None,
            None,
            Some(&async_fsync),
            Some(5),
            Some(Lsn(WAL_HORIZON_LAG_ALERT + 50)),
        );
        assert!(alerts.iter().any(|a| a.contains("async fsync lag")));
        assert!(alerts.iter().any(|a| a.contains("vacuum horizon lags")));
        assert!(recommended.is_none());
    }
}
