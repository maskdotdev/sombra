use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

use crate::storage::storage_profile_snapshot;

/// A snapshot of query execution profiling metrics.
///
/// This structure captures timing and count information for various query operations.
/// Profiling is enabled via the `SOMBRA_PROFILE` environment variable and tracks
/// performance characteristics across different query execution phases.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryProfileSnapshot {
    /// Total nanoseconds spent acquiring read guards.
    pub read_guard_ns: u64,
    /// Number of read guard acquisitions.
    pub read_guard_count: u64,
    /// Total nanoseconds spent building streams.
    pub stream_build_ns: u64,
    /// Number of stream build operations.
    pub stream_build_count: u64,
    /// Total nanoseconds spent iterating over streams.
    pub stream_iter_ns: u64,
    /// Number of stream iteration operations.
    pub stream_iter_count: u64,
    /// Total nanoseconds spent in property index operations.
    pub prop_index_ns: u64,
    /// Number of property index operations.
    pub prop_index_count: u64,
    /// Total nanoseconds spent looking up property index entries.
    pub prop_index_lookup_ns: u64,
    /// Number of property index lookup operations.
    pub prop_index_lookup_count: u64,
    /// Total nanoseconds spent encoding property index data.
    pub prop_index_encode_ns: u64,
    /// Number of property index encode operations.
    pub prop_index_encode_count: u64,
    /// Total nanoseconds spent building property index streams.
    pub prop_index_stream_build_ns: u64,
    /// Number of property index stream build operations.
    pub prop_index_stream_build_count: u64,
    /// Total nanoseconds spent iterating over property index streams.
    pub prop_index_stream_iter_ns: u64,
    /// Number of property index stream iteration operations.
    pub prop_index_stream_iter_count: u64,
    /// Total nanoseconds spent expanding query results.
    pub expand_ns: u64,
    /// Number of expand operations.
    pub expand_count: u64,
    /// Total nanoseconds spent filtering query results.
    pub filter_ns: u64,
    /// Number of filter operations.
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
    /// Profiling for read guard acquisition.
    ReadGuard,
    /// Profiling for stream building operations.
    StreamBuild,
    /// Profiling for stream iteration operations.
    StreamIter,
    /// Profiling for property index operations.
    PropIndex,
    /// Profiling for query result expansion operations.
    Expand,
    /// Profiling for query result filtering operations.
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

/// Retrieves a snapshot of current query profiling metrics.
///
/// # Arguments
///
/// * `reset` - If `true`, resets all counters to zero after reading them.
///            If `false`, reads the current values without modification.
///
/// # Returns
///
/// Returns `Some(QueryProfileSnapshot)` if profiling is enabled via the
/// `SOMBRA_PROFILE` environment variable, or `None` if profiling is disabled.
///
/// # Example
///
/// ```no_run
/// use sombra::query::profile::profile_snapshot;
///
/// // Get snapshot without resetting counters
/// if let Some(snapshot) = profile_snapshot(false) {
///     println!("Read guard time: {}ns", snapshot.read_guard_ns);
/// }
///
/// // Get snapshot and reset counters
/// if let Some(snapshot) = profile_snapshot(true) {
///     println!("Total filter operations: {}", snapshot.filter_count);
/// }
/// ```
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
