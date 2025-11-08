use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

#[derive(Debug, Clone, Copy, Default)]
pub struct StorageProfileSnapshot {
    pub prop_index_lookup_ns: u64,
    pub prop_index_lookup_count: u64,
    pub prop_index_encode_ns: u64,
    pub prop_index_encode_count: u64,
    pub prop_index_stream_build_ns: u64,
    pub prop_index_stream_build_count: u64,
    pub prop_index_stream_iter_ns: u64,
    pub prop_index_stream_iter_count: u64,
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

#[derive(Clone, Copy, Debug)]
pub enum StorageProfileKind {
    PropIndexLookup,
    PropIndexKeyEncode,
    PropIndexStreamBuild,
    PropIndexStreamIter,
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
    }
}

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
    })
}
