use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

/// Snapshot of storage profiling metrics.
#[derive(Debug, Clone, Copy, Default)]
pub struct StorageProfileSnapshot {
    /// Total nanoseconds spent in property index lookups.
    pub prop_index_lookup_ns: u64,
    /// Number of property index lookups performed.
    pub prop_index_lookup_count: u64,
    /// Total nanoseconds spent encoding property index keys.
    pub prop_index_encode_ns: u64,
    /// Number of property index key encodings performed.
    pub prop_index_encode_count: u64,
    /// Total nanoseconds spent building property index streams.
    pub prop_index_stream_build_ns: u64,
    /// Number of property index streams built.
    pub prop_index_stream_build_count: u64,
    /// Total nanoseconds spent iterating property index streams.
    pub prop_index_stream_iter_ns: u64,
    /// Number of property index stream iterations performed.
    pub prop_index_stream_iter_count: u64,
    /// Total nanoseconds spent scanning leaf pages.
    pub btree_leaf_search_ns: u64,
    /// Number of measured leaf page searches.
    pub btree_leaf_search_count: u64,
    /// Total nanoseconds spent inserting into leaf pages.
    pub btree_leaf_insert_ns: u64,
    /// Number of measured leaf insertions.
    pub btree_leaf_insert_count: u64,
    /// Total nanoseconds spent building slot extents / slicing records.
    pub btree_slot_extent_ns: u64,
    /// Number of slot-extent builds measured.
    pub btree_slot_extent_count: u64,
    /// Total slots scanned while building slot extents.
    pub btree_slot_extent_slots: u64,
    /// Total nanoseconds spent committing through the pager.
    pub pager_commit_ns: u64,
    /// Number of measured pager commits.
    pub pager_commit_count: u64,
    /// Approximate p50 pager commit latency (nanoseconds).
    pub pager_commit_p50_ns: u64,
    /// Approximate p90 pager commit latency (nanoseconds).
    pub pager_commit_p90_ns: u64,
    /// Approximate p99 pager commit latency (nanoseconds).
    pub pager_commit_p99_ns: u64,
    /// Total nanoseconds spent starting reads (snapshot + registration).
    pub mvcc_read_begin_ns: u64,
    /// Number of read-begin operations measured.
    pub mvcc_read_begin_count: u64,
    /// Total nanoseconds spent starting writes (writer lock + setup).
    pub mvcc_write_begin_ns: u64,
    /// Number of write-begin operations measured.
    pub mvcc_write_begin_count: u64,
    /// Approximate p50 write-begin latency (nanoseconds).
    pub mvcc_write_begin_p50_ns: u64,
    /// Approximate p90 write-begin latency (nanoseconds).
    pub mvcc_write_begin_p90_ns: u64,
    /// Approximate p99 write-begin latency (nanoseconds).
    pub mvcc_write_begin_p99_ns: u64,
    /// Total nanoseconds spent committing (MVCC + pager).
    pub mvcc_commit_ns: u64,
    /// Number of commit operations measured.
    pub mvcc_commit_count: u64,
    /// Approximate p50 read-begin latency (nanoseconds).
    pub mvcc_read_begin_p50_ns: u64,
    /// Approximate p90 read-begin latency (nanoseconds).
    pub mvcc_read_begin_p90_ns: u64,
    /// Approximate p99 read-begin latency (nanoseconds).
    pub mvcc_read_begin_p99_ns: u64,
    /// Number of reconstructed keys during leaf operations.
    pub btree_leaf_key_decodes: u64,
    /// Number of key comparisons performed in leaf searches.
    pub btree_leaf_key_cmps: u64,
    /// Total bytes copied when rebuilding leaf keys.
    pub btree_leaf_memcopy_bytes: u64,
    /// Number of WAL frames written.
    pub pager_wal_frames: u64,
    /// Total bytes appended to the WAL.
    pub pager_wal_bytes: u64,
    /// Number of fsync calls issued by the pager.
    pub pager_fsync_count: u64,
    /// Number of leaf rebalances that completed without rebuilding.
    pub btree_leaf_rebalance_in_place: u64,
    /// Number of leaf rebalances that rewrote whole leaves.
    pub btree_leaf_rebalance_rebuilds: u64,
    /// Number of WAL write batches emitted via writev
    pub wal_coalesced_writes: u64,
    /// Number of WAL segments reused/reclaimed
    pub wal_reused_segments: u64,
    /// Bytes flushed via borrowed page images during commits
    pub pager_commit_borrowed_bytes: u64,
    /// Median WAL batch size (frames) since last snapshot
    pub wal_commit_group_p50: u64,
    /// 95th percentile WAL batch size (frames) since last snapshot
    pub wal_commit_group_p95: u64,
    /// Number of allocator compactions performed while editing leaves.
    pub btree_leaf_allocator_compactions: u64,
    /// Total bytes moved by the leaf allocator during compactions.
    pub btree_leaf_allocator_bytes_moved: u64,
    /// Number of times the leaf allocator could not satisfy a request.
    pub btree_leaf_allocator_failures: u64,
    /// Number of failures due to slot directory growth exceeding payload.
    pub btree_leaf_allocator_failure_slot_overflow: u64,
    /// Number of failures due to fences consuming all payload capacity.
    pub btree_leaf_allocator_failure_payload: u64,
    /// Number of failures because the leaf payload itself was full.
    pub btree_leaf_allocator_failure_page_full: u64,
    /// Total nanoseconds spent building new leaf allocator instances.
    pub btree_leaf_allocator_build_ns: u64,
    /// Number of times we rebuilt allocator metadata from scratch.
    pub btree_leaf_allocator_build_count: u64,
    /// Total free-region entries observed after allocator builds.
    pub btree_leaf_allocator_build_free_regions: u64,
    /// Number of times we reused an allocator snapshot.
    pub btree_leaf_allocator_snapshot_reuse: u64,
    /// Total free-region entries observed when reusing snapshots.
    pub btree_leaf_allocator_snapshot_free_regions: u64,
    /// Number of MVCC writer lock conflicts.
    pub mvcc_write_lock_conflicts: u64,
    /// Number of commits via direct path (no thread handoff).
    pub wal_commit_direct: u64,
    /// Number of commits via group commit path.
    pub wal_commit_group: u64,
    /// Number of direct commit attempts that fell back due to contention.
    pub wal_commit_direct_contention: u64,
    /// Number of syncs that coalesced multiple commits.
    pub wal_sync_coalesced: u64,
    /// Total nanoseconds spent building WAL frames (CRC, page gather).
    pub commit_frame_build_ns: u64,
    /// Total nanoseconds spent writing to WAL.
    pub commit_wal_write_ns: u64,
    /// Total nanoseconds spent in fsync.
    pub commit_fsync_ns: u64,
    /// Total nanoseconds spent in post-commit finalization.
    pub commit_finalize_ns: u64,
    /// Number of commit phase measurements.
    pub commit_phase_count: u64,

    // ========================================================================
    // Create-path profiling (node/edge creation)
    // ========================================================================
    /// Total nanoseconds spent in create_node (graph layer).
    pub create_node_ns: u64,
    /// Number of create_node calls.
    pub create_node_count: u64,
    /// Total nanoseconds spent encoding properties in create_node.
    pub create_node_encode_props_ns: u64,
    /// Number of property encoding operations in create_node.
    pub create_node_encode_props_count: u64,
    /// Total nanoseconds spent in BTree insert for nodes.
    pub create_node_btree_ns: u64,
    /// Number of node BTree inserts.
    pub create_node_btree_count: u64,
    /// Total nanoseconds spent updating label indexes.
    pub create_node_label_index_ns: u64,
    /// Number of label index update operations.
    pub create_node_label_index_count: u64,
    /// Total nanoseconds spent updating property indexes.
    pub create_node_prop_index_ns: u64,
    /// Number of property index update operations.
    pub create_node_prop_index_count: u64,
    /// Total nanoseconds spent in create_edge (graph layer).
    pub create_edge_ns: u64,
    /// Number of create_edge calls.
    pub create_edge_count: u64,
    /// Total nanoseconds spent encoding properties in create_edge.
    pub create_edge_encode_props_ns: u64,
    /// Number of property encoding operations in create_edge.
    pub create_edge_encode_props_count: u64,
    /// Total nanoseconds spent in BTree inserts for edges.
    pub create_edge_btree_ns: u64,
    /// Number of edge BTree inserts.
    pub create_edge_btree_count: u64,
    /// Total nanoseconds spent updating adjacency indexes.
    pub create_edge_adjacency_ns: u64,
    /// Number of adjacency index update operations.
    pub create_edge_adjacency_count: u64,
    /// Total nanoseconds spent resolving dictionary entries (labels/types/props).
    pub dict_resolve_ns: u64,
    /// Number of dictionary resolution operations.
    pub dict_resolve_count: u64,
    /// Total nanoseconds in FFI create_typed_batch.
    pub ffi_create_batch_ns: u64,
    /// Number of FFI create_typed_batch calls.
    pub ffi_create_batch_count: u64,
    /// Total nanoseconds spent converting typed props to storage format.
    pub ffi_typed_props_convert_ns: u64,
    /// Number of typed props conversion operations.
    pub ffi_typed_props_convert_count: u64,

    // ========================================================================
    // Granular BTree leaf insert profiling
    // ========================================================================
    /// Total nanoseconds spent in binary search during leaf inserts.
    pub btree_leaf_binary_search_ns: u64,
    /// Number of binary search operations in leaf inserts.
    pub btree_leaf_binary_search_count: u64,
    /// Total nanoseconds spent encoding records (key+value) for leaf inserts.
    pub btree_leaf_record_encode_ns: u64,
    /// Number of record encoding operations.
    pub btree_leaf_record_encode_count: u64,
    /// Total nanoseconds spent in slot allocation (insert_slot).
    pub btree_leaf_slot_alloc_ns: u64,
    /// Number of slot allocation operations.
    pub btree_leaf_slot_alloc_count: u64,
    /// Total nanoseconds spent persisting slot directories.
    pub btree_leaf_slot_persist_ns: u64,
    /// Number of slot directory persist operations.
    pub btree_leaf_slot_persist_count: u64,
    /// Number of leaf page splits triggered.
    pub btree_leaf_splits: u64,
    /// Number of in-place inserts that succeeded.
    pub btree_leaf_in_place_success: u64,
    /// Total nanoseconds in allocator get/restore from cache.
    pub btree_leaf_allocator_cache_ns: u64,
    /// Number of allocator cache operations.
    pub btree_leaf_allocator_cache_count: u64,
    /// Total nanoseconds spent in flush_deferred_writes.
    pub flush_deferred_ns: u64,
    /// Number of flush_deferred_writes calls.
    pub flush_deferred_count: u64,

    // ========================================================================
    // Flush deferred adjacency breakdown
    // ========================================================================
    /// Total nanoseconds spent encoding adjacency keys.
    pub flush_adj_key_encode_ns: u64,
    /// Number of adjacency key encoding batches.
    pub flush_adj_key_encode_count: u64,
    /// Total nanoseconds spent sorting forward adjacency keys.
    pub flush_adj_fwd_sort_ns: u64,
    /// Number of forward sort operations.
    pub flush_adj_fwd_sort_count: u64,
    /// Total nanoseconds spent in forward adjacency put_many.
    pub flush_adj_fwd_put_ns: u64,
    /// Number of forward put_many operations.
    pub flush_adj_fwd_put_count: u64,
    /// Total nanoseconds spent sorting reverse adjacency keys.
    pub flush_adj_rev_sort_ns: u64,
    /// Number of reverse sort operations.
    pub flush_adj_rev_sort_count: u64,
    /// Total nanoseconds spent in reverse adjacency put_many.
    pub flush_adj_rev_put_ns: u64,
    /// Number of reverse put_many operations.
    pub flush_adj_rev_put_count: u64,
    /// Total number of adjacency entries flushed.
    pub flush_adj_entries: u64,
    /// Total nanoseconds spent in finalize_adjacency_entries.
    pub flush_adj_finalize_ns: u64,
    /// Number of finalize operations.
    pub flush_adj_finalize_count: u64,
    /// Total nanoseconds spent in flush_deferred_indexes.
    pub flush_deferred_indexes_ns: u64,
    /// Number of flush_deferred_indexes calls.
    pub flush_deferred_indexes_count: u64,
}

#[derive(Default)]
struct StorageProfileCounters {
    prop_index_lookup_ns: AtomicU64,
    prop_index_lookup_count: AtomicU64,
    prop_index_encode_ns: AtomicU64,
    prop_index_encode_count: AtomicU64,
    prop_index_stream_build_ns: AtomicU64,
    prop_index_stream_build_count: AtomicU64,
    prop_index_stream_iter_ns: AtomicU64,
    prop_index_stream_iter_count: AtomicU64,
    btree_leaf_search_ns: AtomicU64,
    btree_leaf_search_count: AtomicU64,
    btree_leaf_insert_ns: AtomicU64,
    btree_leaf_insert_count: AtomicU64,
    btree_slot_extent_ns: AtomicU64,
    btree_slot_extent_count: AtomicU64,
    btree_slot_extent_slots: AtomicU64,
    pager_commit_ns: AtomicU64,
    pager_commit_count: AtomicU64,
    btree_leaf_key_decodes: AtomicU64,
    btree_leaf_key_cmps: AtomicU64,
    btree_leaf_memcopy_bytes: AtomicU64,
    pager_wal_frames: AtomicU64,
    pager_wal_bytes: AtomicU64,
    pager_fsync_count: AtomicU64,
    btree_leaf_rebalance_in_place: AtomicU64,
    btree_leaf_rebalance_rebuilds: AtomicU64,
    wal_coalesced_writes: AtomicU64,
    wal_reused_segments: AtomicU64,
    pager_commit_borrowed_bytes: AtomicU64,
    btree_leaf_allocator_compactions: AtomicU64,
    btree_leaf_allocator_bytes_moved: AtomicU64,
    btree_leaf_allocator_failures: AtomicU64,
    btree_leaf_allocator_failure_slot_overflow: AtomicU64,
    btree_leaf_allocator_failure_payload: AtomicU64,
    btree_leaf_allocator_failure_page_full: AtomicU64,
    btree_leaf_allocator_build_ns: AtomicU64,
    btree_leaf_allocator_build_count: AtomicU64,
    btree_leaf_allocator_build_free_regions: AtomicU64,
    btree_leaf_allocator_snapshot_reuse: AtomicU64,
    btree_leaf_allocator_snapshot_free_regions: AtomicU64,
    pager_commit_latency: LatencyHistogram,
    mvcc_read_begin_ns: AtomicU64,
    mvcc_read_begin_count: AtomicU64,
    mvcc_write_begin_ns: AtomicU64,
    mvcc_write_begin_count: AtomicU64,
    mvcc_write_begin_latency: LatencyHistogram,
    mvcc_commit_ns: AtomicU64,
    mvcc_commit_count: AtomicU64,
    mvcc_read_begin_latency: LatencyHistogram,
    #[allow(dead_code)]
    mvcc_commit_latency: LatencyHistogram,
    mvcc_write_lock_conflicts: AtomicU64,
    wal_commit_direct: AtomicU64,
    wal_commit_group: AtomicU64,
    wal_commit_direct_contention: AtomicU64,
    wal_sync_coalesced: AtomicU64,
    commit_frame_build_ns: AtomicU64,
    commit_wal_write_ns: AtomicU64,
    commit_fsync_ns: AtomicU64,
    commit_finalize_ns: AtomicU64,
    commit_phase_count: AtomicU64,

    // Create-path profiling counters
    create_node_ns: AtomicU64,
    create_node_count: AtomicU64,
    create_node_encode_props_ns: AtomicU64,
    create_node_encode_props_count: AtomicU64,
    create_node_btree_ns: AtomicU64,
    create_node_btree_count: AtomicU64,
    create_node_label_index_ns: AtomicU64,
    create_node_label_index_count: AtomicU64,
    create_node_prop_index_ns: AtomicU64,
    create_node_prop_index_count: AtomicU64,
    create_edge_ns: AtomicU64,
    create_edge_count: AtomicU64,
    create_edge_encode_props_ns: AtomicU64,
    create_edge_encode_props_count: AtomicU64,
    create_edge_btree_ns: AtomicU64,
    create_edge_btree_count: AtomicU64,
    create_edge_adjacency_ns: AtomicU64,
    create_edge_adjacency_count: AtomicU64,
    dict_resolve_ns: AtomicU64,
    dict_resolve_count: AtomicU64,
    ffi_create_batch_ns: AtomicU64,
    ffi_create_batch_count: AtomicU64,
    ffi_typed_props_convert_ns: AtomicU64,
    ffi_typed_props_convert_count: AtomicU64,

    // Granular BTree leaf insert profiling
    btree_leaf_binary_search_ns: AtomicU64,
    btree_leaf_binary_search_count: AtomicU64,
    btree_leaf_record_encode_ns: AtomicU64,
    btree_leaf_record_encode_count: AtomicU64,
    btree_leaf_slot_alloc_ns: AtomicU64,
    btree_leaf_slot_alloc_count: AtomicU64,
    btree_leaf_slot_persist_ns: AtomicU64,
    btree_leaf_slot_persist_count: AtomicU64,
    btree_leaf_splits: AtomicU64,
    btree_leaf_in_place_success: AtomicU64,
    btree_leaf_allocator_cache_ns: AtomicU64,
    btree_leaf_allocator_cache_count: AtomicU64,
    flush_deferred_ns: AtomicU64,
    flush_deferred_count: AtomicU64,

    // Flush deferred adjacency breakdown
    flush_adj_key_encode_ns: AtomicU64,
    flush_adj_key_encode_count: AtomicU64,
    flush_adj_fwd_sort_ns: AtomicU64,
    flush_adj_fwd_sort_count: AtomicU64,
    flush_adj_fwd_put_ns: AtomicU64,
    flush_adj_fwd_put_count: AtomicU64,
    flush_adj_rev_sort_ns: AtomicU64,
    flush_adj_rev_sort_count: AtomicU64,
    flush_adj_rev_put_ns: AtomicU64,
    flush_adj_rev_put_count: AtomicU64,
    flush_adj_entries: AtomicU64,
    flush_adj_finalize_ns: AtomicU64,
    flush_adj_finalize_count: AtomicU64,
    flush_deferred_indexes_ns: AtomicU64,
    flush_deferred_indexes_count: AtomicU64,
}

static PROFILE_ENABLED: OnceLock<bool> = OnceLock::new();
static PROFILE_COUNTERS: OnceLock<StorageProfileCounters> = OnceLock::new();
static WAL_IO_SAMPLES: OnceLock<Mutex<VecDeque<u64>>> = OnceLock::new();
const WAL_SAMPLE_WINDOW: usize = 512;
/// Latency buckets for pager commit duration (nanoseconds).
const COMMIT_LATENCY_BUCKETS: &[u64] = &[
    100_000,       // 100µs
    250_000,       // 250µs
    500_000,       // 500µs
    1_000_000,     // 1ms
    2_000_000,     // 2ms
    5_000_000,     // 5ms
    10_000_000,    // 10ms
    20_000_000,    // 20ms
    50_000_000,    // 50ms
    100_000_000,   // 100ms
    250_000_000,   // 250ms
    500_000_000,   // 500ms
    1_000_000_000, // 1s
    2_000_000_000, // 2s
    5_000_000_000, // 5s
];

#[derive(Debug)]
struct LatencyHistogram {
    counts: Vec<AtomicU64>,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            counts: COMMIT_LATENCY_BUCKETS
                .iter()
                .map(|_| AtomicU64::new(0))
                .collect(),
        }
    }
}

impl LatencyHistogram {
    fn record_ns(&self, value: u64) {
        let idx = COMMIT_LATENCY_BUCKETS
            .iter()
            .position(|bucket| value <= *bucket)
            .unwrap_or(COMMIT_LATENCY_BUCKETS.len() - 1);
        self.counts[idx].fetch_add(1, Ordering::Relaxed);
    }

    fn percentile_ns(&self, percentile: f64, reset: bool) -> u64 {
        let reader = |counter: &AtomicU64| {
            if reset {
                counter.swap(0, Ordering::Relaxed)
            } else {
                counter.load(Ordering::Relaxed)
            }
        };
        let snapshot: Vec<u64> = self.counts.iter().map(reader).collect();
        let total: u64 = snapshot.iter().sum();
        if total == 0 {
            return 0;
        }
        let target = ((percentile / 100.0) * total as f64).ceil() as u64;
        let mut cumulative: u64 = 0;
        for (idx, bucket) in COMMIT_LATENCY_BUCKETS.iter().enumerate() {
            cumulative = cumulative.saturating_add(snapshot[idx]);
            if cumulative >= target {
                return *bucket;
            }
        }
        *COMMIT_LATENCY_BUCKETS.last().unwrap_or(&0)
    }
}

pub fn profiling_enabled() -> bool {
    *PROFILE_ENABLED.get_or_init(|| {
        match std::env::var("SOMBRA_PROFILE") {
            Ok(value) => {
                let lowered = value.to_ascii_lowercase();
                lowered != "0" && lowered != "false"
            }
            Err(_) => false, // default off for performance
        }
    })
}

fn counters() -> Option<&'static StorageProfileCounters> {
    profiling_enabled().then(|| PROFILE_COUNTERS.get_or_init(StorageProfileCounters::default))
}

/// Returns a timestamp for profiling if profiling is enabled.
pub fn profile_timer() -> Option<Instant> {
    profiling_enabled().then(Instant::now)
}

/// Types of storage operations that can be profiled.
#[derive(Clone, Copy, Debug)]
pub enum StorageProfileKind {
    /// Property index lookup operation.
    PropIndexLookup,
    /// Property index key encoding operation.
    PropIndexKeyEncode,
    /// Property index stream building operation.
    PropIndexStreamBuild,
    /// Property index stream iteration operation.
    PropIndexStreamIter,
    /// B-tree leaf search (e.g., `search_leaf_bytes`).
    BTreeLeafSearch,
    /// B-tree leaf insertion (`insert_into_leaf`).
    BTreeLeafInsert,
    /// Slot extent building / record slicing.
    BTreeSlotExtent,
    /// Pager commit duration.
    PagerCommit,
    // Create-path profiling kinds
    /// Total create_node operation.
    CreateNode,
    /// Property encoding in create_node.
    CreateNodeEncodeProps,
    /// BTree insert in create_node.
    CreateNodeBTree,
    /// Label index update in create_node.
    CreateNodeLabelIndex,
    /// Property index update in create_node.
    CreateNodePropIndex,
    /// Total create_edge operation.
    CreateEdge,
    /// Property encoding in create_edge.
    CreateEdgeEncodeProps,
    /// BTree insert in create_edge.
    CreateEdgeBTree,
    /// Adjacency index update in create_edge.
    CreateEdgeAdjacency,
    /// Dictionary resolution (label/type/prop lookup).
    DictResolve,
    /// FFI create_typed_batch total.
    FfiCreateBatch,
    /// FFI typed props conversion.
    FfiTypedPropsConvert,
}

/// Records a profiling timer measurement for the given operation kind.
pub fn record_profile_timer(kind: StorageProfileKind, start: Option<Instant>) {
    let Some(start) = start else {
        return;
    };
    let Some(counters) = counters() else {
        return;
    };
    let nanos = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
    match kind {
        StorageProfileKind::PropIndexLookup => {
            counters
                .prop_index_lookup_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .prop_index_lookup_count
                .fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::PropIndexKeyEncode => {
            counters
                .prop_index_encode_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .prop_index_encode_count
                .fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::PropIndexStreamBuild => {
            counters
                .prop_index_stream_build_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .prop_index_stream_build_count
                .fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::PropIndexStreamIter => {
            counters
                .prop_index_stream_iter_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .prop_index_stream_iter_count
                .fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::BTreeLeafSearch => {
            counters
                .btree_leaf_search_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .btree_leaf_search_count
                .fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::BTreeLeafInsert => {
            counters
                .btree_leaf_insert_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .btree_leaf_insert_count
                .fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::BTreeSlotExtent => {
            counters
                .btree_slot_extent_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .btree_slot_extent_count
                .fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::PagerCommit => {
            counters.pager_commit_ns.fetch_add(nanos, Ordering::Relaxed);
            counters.pager_commit_count.fetch_add(1, Ordering::Relaxed);
            counters.pager_commit_latency.record_ns(nanos);
        }
        StorageProfileKind::CreateNode => {
            counters.create_node_ns.fetch_add(nanos, Ordering::Relaxed);
            counters.create_node_count.fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::CreateNodeEncodeProps => {
            counters
                .create_node_encode_props_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .create_node_encode_props_count
                .fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::CreateNodeBTree => {
            counters
                .create_node_btree_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .create_node_btree_count
                .fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::CreateNodeLabelIndex => {
            counters
                .create_node_label_index_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .create_node_label_index_count
                .fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::CreateNodePropIndex => {
            counters
                .create_node_prop_index_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .create_node_prop_index_count
                .fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::CreateEdge => {
            counters.create_edge_ns.fetch_add(nanos, Ordering::Relaxed);
            counters.create_edge_count.fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::CreateEdgeEncodeProps => {
            counters
                .create_edge_encode_props_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .create_edge_encode_props_count
                .fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::CreateEdgeBTree => {
            counters
                .create_edge_btree_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .create_edge_btree_count
                .fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::CreateEdgeAdjacency => {
            counters
                .create_edge_adjacency_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .create_edge_adjacency_count
                .fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::DictResolve => {
            counters.dict_resolve_ns.fetch_add(nanos, Ordering::Relaxed);
            counters.dict_resolve_count.fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::FfiCreateBatch => {
            counters
                .ffi_create_batch_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .ffi_create_batch_count
                .fetch_add(1, Ordering::Relaxed);
        }
        StorageProfileKind::FfiTypedPropsConvert => {
            counters
                .ffi_typed_props_convert_ns
                .fetch_add(nanos, Ordering::Relaxed);
            counters
                .ffi_typed_props_convert_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Records latency for beginning a read (snapshot acquisition + registration).
pub fn record_mvcc_read_begin(nanos: u64) {
    if let Some(counters) = counters() {
        counters
            .mvcc_read_begin_ns
            .fetch_add(nanos, Ordering::Relaxed);
        counters
            .mvcc_read_begin_count
            .fetch_add(1, Ordering::Relaxed);
        counters.mvcc_read_begin_latency.record_ns(nanos);
    }
}

/// Records latency for beginning a write (writer lock + setup).
pub fn record_mvcc_write_begin(nanos: u64) {
    if let Some(counters) = counters() {
        counters
            .mvcc_write_begin_ns
            .fetch_add(nanos, Ordering::Relaxed);
        counters
            .mvcc_write_begin_count
            .fetch_add(1, Ordering::Relaxed);
        counters.mvcc_write_begin_latency.record_ns(nanos);
    }
}

/// Records latency for committing a write (MVCC + pager).
pub fn record_mvcc_commit(nanos: u64) {
    if let Some(counters) = counters() {
        counters.mvcc_commit_ns.fetch_add(nanos, Ordering::Relaxed);
        counters.mvcc_commit_count.fetch_add(1, Ordering::Relaxed);
    }
}

/// Records how many slots were scanned while building slot extents.
pub fn record_btree_slot_extent_slots(count: u64) {
    if let Some(counters) = counters() {
        counters
            .btree_slot_extent_slots
            .fetch_add(count, Ordering::Relaxed);
    }
}

/// Retrieves a snapshot of current profiling metrics, optionally resetting counters.
pub fn profile_snapshot(reset: bool) -> Option<StorageProfileSnapshot> {
    let counters = counters()?;
    let load = |counter: &AtomicU64| {
        if reset {
            counter.swap(0, Ordering::Relaxed)
        } else {
            counter.load(Ordering::Relaxed)
        }
    };
    let commit_p50_ns = counters.pager_commit_latency.percentile_ns(50.0, reset);
    let commit_p90_ns = counters.pager_commit_latency.percentile_ns(90.0, reset);
    let commit_p99_ns = counters.pager_commit_latency.percentile_ns(99.0, reset);
    let read_begin_p50 = counters.mvcc_read_begin_latency.percentile_ns(50.0, reset);
    let read_begin_p90 = counters.mvcc_read_begin_latency.percentile_ns(90.0, reset);
    let read_begin_p99 = counters.mvcc_read_begin_latency.percentile_ns(99.0, reset);
    let write_begin_p50 = counters.mvcc_write_begin_latency.percentile_ns(50.0, reset);
    let write_begin_p90 = counters.mvcc_write_begin_latency.percentile_ns(90.0, reset);
    let write_begin_p99 = counters.mvcc_write_begin_latency.percentile_ns(99.0, reset);
    let (wal_p50, wal_p95) = wal_sample_snapshot(reset);
    Some(StorageProfileSnapshot {
        prop_index_lookup_ns: load(&counters.prop_index_lookup_ns),
        prop_index_lookup_count: load(&counters.prop_index_lookup_count),
        prop_index_encode_ns: load(&counters.prop_index_encode_ns),
        prop_index_encode_count: load(&counters.prop_index_encode_count),
        prop_index_stream_build_ns: load(&counters.prop_index_stream_build_ns),
        prop_index_stream_build_count: load(&counters.prop_index_stream_build_count),
        prop_index_stream_iter_ns: load(&counters.prop_index_stream_iter_ns),
        prop_index_stream_iter_count: load(&counters.prop_index_stream_iter_count),
        btree_leaf_search_ns: load(&counters.btree_leaf_search_ns),
        btree_leaf_search_count: load(&counters.btree_leaf_search_count),
        btree_leaf_insert_ns: load(&counters.btree_leaf_insert_ns),
        btree_leaf_insert_count: load(&counters.btree_leaf_insert_count),
        btree_slot_extent_ns: load(&counters.btree_slot_extent_ns),
        btree_slot_extent_count: load(&counters.btree_slot_extent_count),
        btree_slot_extent_slots: load(&counters.btree_slot_extent_slots),
        pager_commit_ns: load(&counters.pager_commit_ns),
        pager_commit_count: load(&counters.pager_commit_count),
        pager_commit_p50_ns: commit_p50_ns,
        pager_commit_p90_ns: commit_p90_ns,
        pager_commit_p99_ns: commit_p99_ns,
        mvcc_read_begin_ns: load(&counters.mvcc_read_begin_ns),
        mvcc_read_begin_count: load(&counters.mvcc_read_begin_count),
        mvcc_write_begin_ns: load(&counters.mvcc_write_begin_ns),
        mvcc_write_begin_count: load(&counters.mvcc_write_begin_count),
        mvcc_write_begin_p50_ns: write_begin_p50,
        mvcc_write_begin_p90_ns: write_begin_p90,
        mvcc_write_begin_p99_ns: write_begin_p99,
        mvcc_commit_ns: load(&counters.mvcc_commit_ns),
        mvcc_commit_count: load(&counters.mvcc_commit_count),
        mvcc_read_begin_p50_ns: read_begin_p50,
        mvcc_read_begin_p90_ns: read_begin_p90,
        mvcc_read_begin_p99_ns: read_begin_p99,
        btree_leaf_key_decodes: load(&counters.btree_leaf_key_decodes),
        btree_leaf_key_cmps: load(&counters.btree_leaf_key_cmps),
        btree_leaf_memcopy_bytes: load(&counters.btree_leaf_memcopy_bytes),
        pager_wal_frames: load(&counters.pager_wal_frames),
        pager_wal_bytes: load(&counters.pager_wal_bytes),
        pager_fsync_count: load(&counters.pager_fsync_count),
        btree_leaf_rebalance_in_place: load(&counters.btree_leaf_rebalance_in_place),
        btree_leaf_rebalance_rebuilds: load(&counters.btree_leaf_rebalance_rebuilds),
        wal_coalesced_writes: load(&counters.wal_coalesced_writes),
        wal_reused_segments: load(&counters.wal_reused_segments),
        pager_commit_borrowed_bytes: load(&counters.pager_commit_borrowed_bytes),
        wal_commit_group_p50: wal_p50,
        wal_commit_group_p95: wal_p95,
        btree_leaf_allocator_compactions: load(&counters.btree_leaf_allocator_compactions),
        btree_leaf_allocator_bytes_moved: load(&counters.btree_leaf_allocator_bytes_moved),
        btree_leaf_allocator_failures: load(&counters.btree_leaf_allocator_failures),
        btree_leaf_allocator_failure_slot_overflow: load(
            &counters.btree_leaf_allocator_failure_slot_overflow,
        ),
        btree_leaf_allocator_failure_payload: load(&counters.btree_leaf_allocator_failure_payload),
        btree_leaf_allocator_failure_page_full: load(
            &counters.btree_leaf_allocator_failure_page_full,
        ),
        btree_leaf_allocator_build_ns: load(&counters.btree_leaf_allocator_build_ns),
        btree_leaf_allocator_build_count: load(&counters.btree_leaf_allocator_build_count),
        btree_leaf_allocator_build_free_regions: load(
            &counters.btree_leaf_allocator_build_free_regions,
        ),
        btree_leaf_allocator_snapshot_reuse: load(&counters.btree_leaf_allocator_snapshot_reuse),
        btree_leaf_allocator_snapshot_free_regions: load(
            &counters.btree_leaf_allocator_snapshot_free_regions,
        ),
        mvcc_write_lock_conflicts: load(&counters.mvcc_write_lock_conflicts),
        wal_commit_direct: load(&counters.wal_commit_direct),
        wal_commit_group: load(&counters.wal_commit_group),
        wal_commit_direct_contention: load(&counters.wal_commit_direct_contention),
        wal_sync_coalesced: load(&counters.wal_sync_coalesced),
        commit_frame_build_ns: load(&counters.commit_frame_build_ns),
        commit_wal_write_ns: load(&counters.commit_wal_write_ns),
        commit_fsync_ns: load(&counters.commit_fsync_ns),
        commit_finalize_ns: load(&counters.commit_finalize_ns),
        commit_phase_count: load(&counters.commit_phase_count),
        // Create-path profiling
        create_node_ns: load(&counters.create_node_ns),
        create_node_count: load(&counters.create_node_count),
        create_node_encode_props_ns: load(&counters.create_node_encode_props_ns),
        create_node_encode_props_count: load(&counters.create_node_encode_props_count),
        create_node_btree_ns: load(&counters.create_node_btree_ns),
        create_node_btree_count: load(&counters.create_node_btree_count),
        create_node_label_index_ns: load(&counters.create_node_label_index_ns),
        create_node_label_index_count: load(&counters.create_node_label_index_count),
        create_node_prop_index_ns: load(&counters.create_node_prop_index_ns),
        create_node_prop_index_count: load(&counters.create_node_prop_index_count),
        create_edge_ns: load(&counters.create_edge_ns),
        create_edge_count: load(&counters.create_edge_count),
        create_edge_encode_props_ns: load(&counters.create_edge_encode_props_ns),
        create_edge_encode_props_count: load(&counters.create_edge_encode_props_count),
        create_edge_btree_ns: load(&counters.create_edge_btree_ns),
        create_edge_btree_count: load(&counters.create_edge_btree_count),
        create_edge_adjacency_ns: load(&counters.create_edge_adjacency_ns),
        create_edge_adjacency_count: load(&counters.create_edge_adjacency_count),
        dict_resolve_ns: load(&counters.dict_resolve_ns),
        dict_resolve_count: load(&counters.dict_resolve_count),
        ffi_create_batch_ns: load(&counters.ffi_create_batch_ns),
        ffi_create_batch_count: load(&counters.ffi_create_batch_count),
        ffi_typed_props_convert_ns: load(&counters.ffi_typed_props_convert_ns),
        ffi_typed_props_convert_count: load(&counters.ffi_typed_props_convert_count),
        // Granular BTree leaf insert profiling
        btree_leaf_binary_search_ns: load(&counters.btree_leaf_binary_search_ns),
        btree_leaf_binary_search_count: load(&counters.btree_leaf_binary_search_count),
        btree_leaf_record_encode_ns: load(&counters.btree_leaf_record_encode_ns),
        btree_leaf_record_encode_count: load(&counters.btree_leaf_record_encode_count),
        btree_leaf_slot_alloc_ns: load(&counters.btree_leaf_slot_alloc_ns),
        btree_leaf_slot_alloc_count: load(&counters.btree_leaf_slot_alloc_count),
        btree_leaf_slot_persist_ns: load(&counters.btree_leaf_slot_persist_ns),
        btree_leaf_slot_persist_count: load(&counters.btree_leaf_slot_persist_count),
        btree_leaf_splits: load(&counters.btree_leaf_splits),
        btree_leaf_in_place_success: load(&counters.btree_leaf_in_place_success),
        btree_leaf_allocator_cache_ns: load(&counters.btree_leaf_allocator_cache_ns),
        btree_leaf_allocator_cache_count: load(&counters.btree_leaf_allocator_cache_count),
        flush_deferred_ns: load(&counters.flush_deferred_ns),
        flush_deferred_count: load(&counters.flush_deferred_count),
        // Flush deferred adjacency breakdown
        flush_adj_key_encode_ns: load(&counters.flush_adj_key_encode_ns),
        flush_adj_key_encode_count: load(&counters.flush_adj_key_encode_count),
        flush_adj_fwd_sort_ns: load(&counters.flush_adj_fwd_sort_ns),
        flush_adj_fwd_sort_count: load(&counters.flush_adj_fwd_sort_count),
        flush_adj_fwd_put_ns: load(&counters.flush_adj_fwd_put_ns),
        flush_adj_fwd_put_count: load(&counters.flush_adj_fwd_put_count),
        flush_adj_rev_sort_ns: load(&counters.flush_adj_rev_sort_ns),
        flush_adj_rev_sort_count: load(&counters.flush_adj_rev_sort_count),
        flush_adj_rev_put_ns: load(&counters.flush_adj_rev_put_ns),
        flush_adj_rev_put_count: load(&counters.flush_adj_rev_put_count),
        flush_adj_entries: load(&counters.flush_adj_entries),
        flush_adj_finalize_ns: load(&counters.flush_adj_finalize_ns),
        flush_adj_finalize_count: load(&counters.flush_adj_finalize_count),
        flush_deferred_indexes_ns: load(&counters.flush_deferred_indexes_ns),
        flush_deferred_indexes_count: load(&counters.flush_deferred_indexes_count),
    })
}

/// Records that `count` encoded keys were reconstructed while scanning a leaf.
pub fn record_btree_leaf_key_decodes(count: u64) {
    if count == 0 {
        return;
    }
    if let Some(counters) = counters() {
        counters
            .btree_leaf_key_decodes
            .fetch_add(count, Ordering::Relaxed);
    }
}

/// Records that `count` key comparisons were executed inside a leaf search.
pub fn record_btree_leaf_key_cmps(count: u64) {
    if count == 0 {
        return;
    }
    if let Some(counters) = counters() {
        counters
            .btree_leaf_key_cmps
            .fetch_add(count, Ordering::Relaxed);
    }
}

/// Records the number of bytes copied while materializing keys.
pub fn record_btree_leaf_memcopy_bytes(bytes: u64) {
    if bytes == 0 {
        return;
    }
    if let Some(counters) = counters() {
        counters
            .btree_leaf_memcopy_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Records that the leaf allocator performed a compaction and moved `bytes` bytes.
pub fn record_leaf_allocator_compaction(bytes: u64) {
    if let Some(counters) = counters() {
        counters
            .btree_leaf_allocator_compactions
            .fetch_add(1, Ordering::Relaxed);
        if bytes > 0 {
            counters
                .btree_leaf_allocator_bytes_moved
                .fetch_add(bytes, Ordering::Relaxed);
        }
    }
}

/// Records that the leaf allocator could not satisfy a request without rebuilding.
#[derive(Clone, Copy, Debug)]
pub enum LeafAllocatorFailureKind {
    SlotOverflow,
    PayloadExhausted,
    PageFull,
}

pub fn record_leaf_allocator_failure(kind: LeafAllocatorFailureKind) {
    if let Some(counters) = counters() {
        counters
            .btree_leaf_allocator_failures
            .fetch_add(1, Ordering::Relaxed);
        match kind {
            LeafAllocatorFailureKind::SlotOverflow => counters
                .btree_leaf_allocator_failure_slot_overflow
                .fetch_add(1, Ordering::Relaxed),
            LeafAllocatorFailureKind::PayloadExhausted => counters
                .btree_leaf_allocator_failure_payload
                .fetch_add(1, Ordering::Relaxed),
            LeafAllocatorFailureKind::PageFull => counters
                .btree_leaf_allocator_failure_page_full
                .fetch_add(1, Ordering::Relaxed),
        };
    }
}

/// Records how long it took to build a brand-new allocator plus the observed free regions.
pub fn record_leaf_allocator_build(duration_ns: u64, free_regions: u64) {
    if let Some(counters) = counters() {
        counters
            .btree_leaf_allocator_build_ns
            .fetch_add(duration_ns, Ordering::Relaxed);
        counters
            .btree_leaf_allocator_build_count
            .fetch_add(1, Ordering::Relaxed);
        counters
            .btree_leaf_allocator_build_free_regions
            .fetch_add(free_regions, Ordering::Relaxed);
    }
}

/// Records that we reused a cached allocator snapshot along with the free regions it carried.
pub fn record_leaf_allocator_snapshot_reuse(free_regions: u64) {
    if let Some(counters) = counters() {
        counters
            .btree_leaf_allocator_snapshot_reuse
            .fetch_add(1, Ordering::Relaxed);
        counters
            .btree_leaf_allocator_snapshot_free_regions
            .fetch_add(free_regions, Ordering::Relaxed);
    }
}

/// Records the number of coalesced WAL write batches.
pub fn record_wal_coalesced_writes(count: u64) {
    if count == 0 {
        return;
    }
    if let Some(counters) = counters() {
        counters
            .wal_coalesced_writes
            .fetch_add(count, Ordering::Relaxed);
    }
}

/// Records how many WAL segments were reclaimed for reuse.
pub fn record_wal_reused_segments(count: u64) {
    if count == 0 {
        return;
    }
    if let Some(counters) = counters() {
        counters
            .wal_reused_segments
            .fetch_add(count, Ordering::Relaxed);
    }
}

/// Records how many bytes were flushed via borrowed page images during commit.
pub fn record_pager_commit_borrowed_bytes(bytes: u64) {
    if bytes == 0 {
        return;
    }
    if let Some(counters) = counters() {
        counters
            .pager_commit_borrowed_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Stores a WAL batch length sample used for p50/p95 reporting.
pub fn record_wal_io_group_sample(len: u64) {
    if len == 0 {
        return;
    }
    let Some(samples) = wal_samples() else {
        return;
    };
    let Ok(mut guard) = samples.lock() else {
        tracing::warn!("wal sample mutex poisoned; dropping sample");
        return;
    };
    if guard.len() >= WAL_SAMPLE_WINDOW {
        guard.pop_front();
    }
    guard.push_back(len);
}

fn wal_samples() -> Option<&'static Mutex<VecDeque<u64>>> {
    profiling_enabled().then(|| {
        WAL_IO_SAMPLES.get_or_init(|| Mutex::new(VecDeque::with_capacity(WAL_SAMPLE_WINDOW)))
    })
}

fn wal_sample_snapshot(reset: bool) -> (u64, u64) {
    let Some(samples) = WAL_IO_SAMPLES.get() else {
        return (0, 0);
    };
    let Ok(mut guard) = samples.lock() else {
        tracing::warn!("wal sample mutex poisoned; reporting zeros");
        return (0, 0);
    };
    if guard.is_empty() {
        if reset {
            guard.clear();
        }
        return (0, 0);
    }
    let mut data: Vec<u64> = guard.iter().copied().collect();
    data.sort_unstable();
    let p50 = percentile(&data, 0.5);
    let p95 = percentile(&data, 0.95);
    if reset {
        guard.clear();
    }
    (p50, p95)
}

fn percentile(values: &[u64], pct: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let max_index = values.len() - 1;
    let idx = ((max_index as f64) * pct).round() as usize;
    values[idx.min(max_index)]
}

/// Records the number of WAL frames emitted during a commit.
pub fn record_pager_wal_frames(frames: u64) {
    if frames == 0 {
        return;
    }
    if let Some(counters) = counters() {
        counters
            .pager_wal_frames
            .fetch_add(frames, Ordering::Relaxed);
    }
}

/// Records the total WAL bytes emitted during a commit.
pub fn record_pager_wal_bytes(bytes: u64) {
    if bytes == 0 {
        return;
    }
    if let Some(counters) = counters() {
        counters.pager_wal_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Records a pager fsync invocation.
pub fn record_pager_fsync() {
    if let Some(counters) = counters() {
        counters.pager_fsync_count.fetch_add(1, Ordering::Relaxed);
    }
}

/// Records that a leaf rebalance completed using the in-place slot directory path.
pub fn record_btree_leaf_rebalance_in_place(count: u64) {
    if count == 0 {
        return;
    }
    if let Some(counters) = counters() {
        counters
            .btree_leaf_rebalance_in_place
            .fetch_add(count, Ordering::Relaxed);
    }
}

/// Records that a leaf rebalance rebuilt one or more pages.
pub fn record_btree_leaf_rebalance_rebuilds(count: u64) {
    if count == 0 {
        return;
    }
    if let Some(counters) = counters() {
        counters
            .btree_leaf_rebalance_rebuilds
            .fetch_add(count, Ordering::Relaxed);
    }
}

/// Records a commit via the direct path (no thread handoff).
pub fn record_wal_commit_direct() {
    if let Some(counters) = counters() {
        counters.wal_commit_direct.fetch_add(1, Ordering::Relaxed);
    }
}

/// Records a commit via the group commit path.
pub fn record_wal_commit_group() {
    if let Some(counters) = counters() {
        counters.wal_commit_group.fetch_add(1, Ordering::Relaxed);
    }
}

/// Records a direct commit attempt that fell back due to contention.
pub fn record_wal_commit_direct_contention() {
    if let Some(counters) = counters() {
        counters
            .wal_commit_direct_contention
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Records a sync that coalesced multiple commits.
pub fn record_wal_sync_coalesced(count: u64) {
    if count == 0 {
        return;
    }
    if let Some(counters) = counters() {
        counters
            .wal_sync_coalesced
            .fetch_add(count, Ordering::Relaxed);
    }
}

/// Records commit phase timings for detailed profiling.
pub fn record_commit_phases(
    frame_build_ns: u64,
    wal_write_ns: u64,
    fsync_ns: u64,
    finalize_ns: u64,
) {
    if let Some(counters) = counters() {
        counters
            .commit_frame_build_ns
            .fetch_add(frame_build_ns, Ordering::Relaxed);
        counters
            .commit_wal_write_ns
            .fetch_add(wal_write_ns, Ordering::Relaxed);
        counters
            .commit_fsync_ns
            .fetch_add(fsync_ns, Ordering::Relaxed);
        counters
            .commit_finalize_ns
            .fetch_add(finalize_ns, Ordering::Relaxed);
        counters.commit_phase_count.fetch_add(1, Ordering::Relaxed);
    }
}

/// RAII helper that records a duration in [`record_profile_timer`] when dropped.
pub struct ProfileScope {
    kind: StorageProfileKind,
    start: Option<Instant>,
}

impl ProfileScope {
    /// Creates a new profiling scope for the provided kind.
    pub fn new(kind: StorageProfileKind) -> Self {
        Self {
            kind,
            start: profile_timer(),
        }
    }
}

impl Drop for ProfileScope {
    fn drop(&mut self) {
        record_profile_timer(self.kind, self.start.take());
    }
}

/// Convenience helper that creates a [`ProfileScope`].
pub fn profile_scope(kind: StorageProfileKind) -> ProfileScope {
    ProfileScope::new(kind)
}

// ============================================================================
// Granular BTree leaf insert profiling functions
// ============================================================================

/// Records time spent in binary search during leaf insert.
pub fn record_btree_leaf_binary_search(nanos: u64) {
    if let Some(counters) = counters() {
        counters
            .btree_leaf_binary_search_ns
            .fetch_add(nanos, Ordering::Relaxed);
        counters
            .btree_leaf_binary_search_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Records time spent encoding a leaf record (key + value).
pub fn record_btree_leaf_record_encode(nanos: u64) {
    if let Some(counters) = counters() {
        counters
            .btree_leaf_record_encode_ns
            .fetch_add(nanos, Ordering::Relaxed);
        counters
            .btree_leaf_record_encode_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Records time spent in slot allocation (insert_slot).
pub fn record_btree_leaf_slot_alloc(nanos: u64) {
    if let Some(counters) = counters() {
        counters
            .btree_leaf_slot_alloc_ns
            .fetch_add(nanos, Ordering::Relaxed);
        counters
            .btree_leaf_slot_alloc_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Records time spent persisting slot directories.
#[allow(dead_code)]
pub fn record_btree_leaf_slot_persist(nanos: u64) {
    if let Some(counters) = counters() {
        counters
            .btree_leaf_slot_persist_ns
            .fetch_add(nanos, Ordering::Relaxed);
        counters
            .btree_leaf_slot_persist_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Records a leaf page split.
pub fn record_btree_leaf_split() {
    if let Some(counters) = counters() {
        counters.btree_leaf_splits.fetch_add(1, Ordering::Relaxed);
    }
}

/// Records a successful in-place insert.
pub fn record_btree_leaf_in_place_success() {
    if let Some(counters) = counters() {
        counters
            .btree_leaf_in_place_success
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Records time spent getting/restoring allocator from cache.
pub fn record_btree_leaf_allocator_cache(nanos: u64) {
    if let Some(counters) = counters() {
        counters
            .btree_leaf_allocator_cache_ns
            .fetch_add(nanos, Ordering::Relaxed);
        counters
            .btree_leaf_allocator_cache_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Records time spent in flush_deferred_writes (adjacency + index flush).
pub fn record_flush_deferred(nanos: u64) {
    if let Some(counters) = counters() {
        counters
            .flush_deferred_ns
            .fetch_add(nanos, Ordering::Relaxed);
        counters
            .flush_deferred_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Records time spent encoding adjacency keys.
pub fn record_flush_adj_key_encode(nanos: u64) {
    if let Some(counters) = counters() {
        counters
            .flush_adj_key_encode_ns
            .fetch_add(nanos, Ordering::Relaxed);
        counters
            .flush_adj_key_encode_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Records time spent sorting forward adjacency keys.
pub fn record_flush_adj_fwd_sort(nanos: u64) {
    if let Some(counters) = counters() {
        counters
            .flush_adj_fwd_sort_ns
            .fetch_add(nanos, Ordering::Relaxed);
        counters
            .flush_adj_fwd_sort_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Records time spent in forward adjacency put_many.
pub fn record_flush_adj_fwd_put(nanos: u64) {
    if let Some(counters) = counters() {
        counters
            .flush_adj_fwd_put_ns
            .fetch_add(nanos, Ordering::Relaxed);
        counters
            .flush_adj_fwd_put_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Records time spent sorting reverse adjacency keys.
pub fn record_flush_adj_rev_sort(nanos: u64) {
    if let Some(counters) = counters() {
        counters
            .flush_adj_rev_sort_ns
            .fetch_add(nanos, Ordering::Relaxed);
        counters
            .flush_adj_rev_sort_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Records time spent in reverse adjacency put_many.
pub fn record_flush_adj_rev_put(nanos: u64) {
    if let Some(counters) = counters() {
        counters
            .flush_adj_rev_put_ns
            .fetch_add(nanos, Ordering::Relaxed);
        counters
            .flush_adj_rev_put_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Records the number of adjacency entries flushed.
pub fn record_flush_adj_entries(count: u64) {
    if let Some(counters) = counters() {
        counters
            .flush_adj_entries
            .fetch_add(count, Ordering::Relaxed);
    }
}

/// Records time spent in finalize_adjacency_entries.
pub fn record_flush_adj_finalize(nanos: u64, count: u64) {
    if let Some(counters) = counters() {
        counters
            .flush_adj_finalize_ns
            .fetch_add(nanos, Ordering::Relaxed);
        counters
            .flush_adj_finalize_count
            .fetch_add(count, Ordering::Relaxed);
    }
}

/// Records time spent in flush_deferred_indexes.
pub fn record_flush_deferred_indexes(nanos: u64) {
    if let Some(counters) = counters() {
        counters
            .flush_deferred_indexes_ns
            .fetch_add(nanos, Ordering::Relaxed);
        counters
            .flush_deferred_indexes_count
            .fetch_add(1, Ordering::Relaxed);
    }
}
