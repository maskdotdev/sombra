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
            Err(_) => true, // default on
        }
    })
}

fn counters() -> Option<&'static StorageProfileCounters> {
    profiling_enabled().then(|| PROFILE_COUNTERS.get_or_init(StorageProfileCounters::default))
}

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
}

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
