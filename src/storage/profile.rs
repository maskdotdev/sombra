use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
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
    /// Total nanoseconds spent committing through the pager.
    pub pager_commit_ns: u64,
    /// Number of measured pager commits.
    pub pager_commit_count: u64,
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
    pager_commit_ns: AtomicU64,
    pager_commit_count: AtomicU64,
    btree_leaf_key_decodes: AtomicU64,
    btree_leaf_key_cmps: AtomicU64,
    btree_leaf_memcopy_bytes: AtomicU64,
    pager_wal_frames: AtomicU64,
    pager_wal_bytes: AtomicU64,
    pager_fsync_count: AtomicU64,
}

static PROFILE_ENABLED: OnceLock<bool> = OnceLock::new();
static PROFILE_COUNTERS: OnceLock<StorageProfileCounters> = OnceLock::new();

pub fn profiling_enabled() -> bool {
    *PROFILE_ENABLED.get_or_init(|| std::env::var_os("SOMBRA_PROFILE").is_some())
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
        StorageProfileKind::PagerCommit => {
            counters.pager_commit_ns.fetch_add(nanos, Ordering::Relaxed);
            counters.pager_commit_count.fetch_add(1, Ordering::Relaxed);
        }
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
        pager_commit_ns: load(&counters.pager_commit_ns),
        pager_commit_count: load(&counters.pager_commit_count),
        btree_leaf_key_decodes: load(&counters.btree_leaf_key_decodes),
        btree_leaf_key_cmps: load(&counters.btree_leaf_key_cmps),
        btree_leaf_memcopy_bytes: load(&counters.btree_leaf_memcopy_bytes),
        pager_wal_frames: load(&counters.pager_wal_frames),
        pager_wal_bytes: load(&counters.pager_wal_bytes),
        pager_fsync_count: load(&counters.pager_fsync_count),
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
