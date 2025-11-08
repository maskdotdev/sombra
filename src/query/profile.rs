use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

use crate::storage::storage_profile_snapshot;

#[derive(Debug, Clone, Copy, Default)]
pub struct QueryProfileSnapshot {
    pub read_guard_ns: u64,
    pub read_guard_count: u64,
    pub stream_build_ns: u64,
    pub stream_build_count: u64,
    pub stream_iter_ns: u64,
    pub stream_iter_count: u64,
    pub prop_index_ns: u64,
    pub prop_index_count: u64,
    pub prop_index_lookup_ns: u64,
    pub prop_index_lookup_count: u64,
    pub prop_index_encode_ns: u64,
    pub prop_index_encode_count: u64,
    pub prop_index_stream_build_ns: u64,
    pub prop_index_stream_build_count: u64,
    pub prop_index_stream_iter_ns: u64,
    pub prop_index_stream_iter_count: u64,
    pub expand_ns: u64,
    pub expand_count: u64,
    pub filter_ns: u64,
    pub filter_count: u64,
}

#[derive(Default)]
struct QueryProfileCounters {
    read_guard_ns: AtomicU64,
    read_guard_count: AtomicU64,
    stream_build_ns: AtomicU64,
    stream_build_count: AtomicU64,
    stream_iter_ns: AtomicU64,
    stream_iter_count: AtomicU64,
    prop_index_ns: AtomicU64,
    prop_index_count: AtomicU64,
    expand_ns: AtomicU64,
    expand_count: AtomicU64,
    filter_ns: AtomicU64,
    filter_count: AtomicU64,
}

static PROFILE_ENABLED: OnceLock<bool> = OnceLock::new();
static PROFILE_COUNTERS: OnceLock<QueryProfileCounters> = OnceLock::new();

fn profiling_enabled() -> bool {
    *PROFILE_ENABLED.get_or_init(|| std::env::var_os("SOMBRA_PROFILE").is_some())
}

fn counters() -> Option<&'static QueryProfileCounters> {
    profiling_enabled().then(|| PROFILE_COUNTERS.get_or_init(QueryProfileCounters::default))
}

pub(crate) fn profile_timer() -> Option<Instant> {
    profiling_enabled().then(Instant::now)
}

pub(crate) enum QueryProfileKind {
    ReadGuard,
    StreamBuild,
    StreamIter,
    PropIndex,
    Expand,
    Filter,
}

pub(crate) fn record_profile_timer(kind: QueryProfileKind, start: Option<Instant>) {
    let Some(start) = start else {
        return;
    };
    let Some(counters) = counters() else {
        return;
    };
    let nanos = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
    match kind {
        QueryProfileKind::ReadGuard => {
            counters.read_guard_ns.fetch_add(nanos, Ordering::Relaxed);
            counters.read_guard_count.fetch_add(1, Ordering::Relaxed);
        }
        QueryProfileKind::StreamBuild => {
            counters.stream_build_ns.fetch_add(nanos, Ordering::Relaxed);
            counters.stream_build_count.fetch_add(1, Ordering::Relaxed);
        }
        QueryProfileKind::StreamIter => {
            counters.stream_iter_ns.fetch_add(nanos, Ordering::Relaxed);
            counters.stream_iter_count.fetch_add(1, Ordering::Relaxed);
        }
        QueryProfileKind::PropIndex => {
            counters.prop_index_ns.fetch_add(nanos, Ordering::Relaxed);
            counters.prop_index_count.fetch_add(1, Ordering::Relaxed);
        }
        QueryProfileKind::Expand => {
            counters.expand_ns.fetch_add(nanos, Ordering::Relaxed);
            counters.expand_count.fetch_add(1, Ordering::Relaxed);
        }
        QueryProfileKind::Filter => {
            counters.filter_ns.fetch_add(nanos, Ordering::Relaxed);
            counters.filter_count.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn profile_snapshot(reset: bool) -> Option<QueryProfileSnapshot> {
    let counters = counters()?;
    let load = |counter: &AtomicU64| {
        if reset {
            counter.swap(0, Ordering::Relaxed)
        } else {
            counter.load(Ordering::Relaxed)
        }
    };
    let storage_snapshot = storage_profile_snapshot(reset);
    let (
        prop_index_lookup_ns,
        prop_index_lookup_count,
        prop_index_encode_ns,
        prop_index_encode_count,
        prop_index_stream_build_ns,
        prop_index_stream_build_count,
        prop_index_stream_iter_ns,
        prop_index_stream_iter_count,
    ) = match storage_snapshot {
        Some(snapshot) => (
            snapshot.prop_index_lookup_ns,
            snapshot.prop_index_lookup_count,
            snapshot.prop_index_encode_ns,
            snapshot.prop_index_encode_count,
            snapshot.prop_index_stream_build_ns,
            snapshot.prop_index_stream_build_count,
            snapshot.prop_index_stream_iter_ns,
            snapshot.prop_index_stream_iter_count,
        ),
        None => (0, 0, 0, 0, 0, 0, 0, 0),
    };
    Some(QueryProfileSnapshot {
        read_guard_ns: load(&counters.read_guard_ns),
        read_guard_count: load(&counters.read_guard_count),
        stream_build_ns: load(&counters.stream_build_ns),
        stream_build_count: load(&counters.stream_build_count),
        stream_iter_ns: load(&counters.stream_iter_ns),
        stream_iter_count: load(&counters.stream_iter_count),
        prop_index_ns: load(&counters.prop_index_ns),
        prop_index_count: load(&counters.prop_index_count),
        prop_index_lookup_ns,
        prop_index_lookup_count,
        prop_index_encode_ns,
        prop_index_encode_count,
        prop_index_stream_build_ns,
        prop_index_stream_build_count,
        prop_index_stream_iter_ns,
        prop_index_stream_iter_count,
        expand_ns: load(&counters.expand_ns),
        expand_count: load(&counters.expand_count),
        filter_ns: load(&counters.filter_ns),
        filter_count: load(&counters.filter_count),
    })
}
