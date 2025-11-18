use super::mvcc::CommitId;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Trait for tracking storage operations and performance metrics in the graph database.
///
/// Implementations of this trait collect statistics about graph operations such as
/// node/edge creation/deletion, adjacency scans, and degree queries. This information
/// can be used for monitoring, profiling, and optimization purposes.
pub trait StorageMetrics: Send + Sync {
    /// Records the creation of a new node in the graph.
    fn node_created(&self);

    /// Records the deletion of a node from the graph.
    fn node_deleted(&self);

    /// Records the creation of a new edge in the graph.
    fn edge_created(&self);

    /// Records the deletion of an edge from the graph.
    fn edge_deleted(&self);

    /// Records an adjacency scan operation.
    ///
    /// # Parameters
    /// * `direction` - The direction of the scan: "out" for outgoing edges, "in" for incoming edges.
    fn adjacency_scan(&self, direction: &'static str);

    /// Records a degree query operation.
    ///
    /// # Parameters
    /// * `direction` - The direction of the query: "out", "in", or "both".
    /// * `cached` - Whether the result was served from cache (`true`) or computed (`false`).
    fn degree_query(&self, direction: &'static str, cached: bool);

    /// Records MVCC reader gauge statistics.
    fn mvcc_reader_gauges(
        &self,
        _active: u64,
        _oldest_commit: CommitId,
        _newest_commit: CommitId,
        _max_age_ms: u64,
    ) {
    }

    /// Records MVCC reader lifecycle totals.
    fn mvcc_reader_totals(&self, _begin_total: u64, _end_total: u64) {}

    /// Records MVCC page version statistics.
    fn mvcc_page_versions(&self, _total_versions: u64, _pages_with_versions: u64) {}
}

/// A no-op implementation of [`StorageMetrics`] that discards all recorded metrics.
///
/// This implementation is useful when metrics collection is disabled or not needed,
/// providing zero overhead for metric tracking operations.
#[derive(Default)]
pub struct NoopMetrics;

impl StorageMetrics for NoopMetrics {
    fn node_created(&self) {}
    fn node_deleted(&self) {}
    fn edge_created(&self) {}
    fn edge_deleted(&self) {}
    fn adjacency_scan(&self, _direction: &'static str) {}
    fn degree_query(&self, _direction: &'static str, _cached: bool) {}
}

/// A thread-safe counter-based implementation of [`StorageMetrics`].
///
/// This implementation uses atomic counters to track various graph operations
/// and query patterns. All counters are thread-safe and can be safely accessed
/// from multiple threads concurrently.
#[derive(Default)]
pub struct CounterMetrics {
    /// Number of nodes created.
    pub nodes_created: AtomicU64,

    /// Number of nodes deleted.
    pub nodes_deleted: AtomicU64,

    /// Number of edges created.
    pub edges_created: AtomicU64,

    /// Number of edges deleted.
    pub edges_deleted: AtomicU64,

    /// Number of outgoing adjacency scans performed.
    pub adjacency_scans_out: AtomicU64,

    /// Number of incoming adjacency scans performed.
    pub adjacency_scans_in: AtomicU64,

    /// Number of outgoing degree queries performed.
    pub degree_queries_out: AtomicU64,

    /// Number of incoming degree queries performed.
    pub degree_queries_in: AtomicU64,

    /// Number of bidirectional degree queries performed.
    pub degree_queries_both: AtomicU64,

    /// Number of degree queries served from cache.
    pub degree_cache_hits: AtomicU64,

    /// Number of degree queries that required computation.
    pub degree_cache_misses: AtomicU64,

    /// Active MVCC readers.
    pub mvcc_reader_active: AtomicU64,

    /// Oldest MVCC reader commit.
    pub mvcc_reader_oldest_commit: AtomicU64,

    /// Newest MVCC reader commit.
    pub mvcc_reader_newest_commit: AtomicU64,

    /// Maximum reader age observed in milliseconds.
    pub mvcc_reader_max_age_ms: AtomicU64,

    /// Total MVCC reader begin events.
    pub mvcc_reader_begin_total: AtomicU64,

    /// Total MVCC reader end events.
    pub mvcc_reader_end_total: AtomicU64,

    /// Total MVCC page versions retained.
    pub mvcc_page_versions_total: AtomicU64,

    /// Pages currently holding historical versions.
    pub mvcc_pages_with_versions: AtomicU64,
}

impl StorageMetrics for CounterMetrics {
    fn node_created(&self) {
        self.nodes_created.fetch_add(1, Ordering::Relaxed);
    }

    fn node_deleted(&self) {
        self.nodes_deleted.fetch_add(1, Ordering::Relaxed);
    }

    fn edge_created(&self) {
        self.edges_created.fetch_add(1, Ordering::Relaxed);
    }

    fn edge_deleted(&self) {
        self.edges_deleted.fetch_add(1, Ordering::Relaxed);
    }

    fn adjacency_scan(&self, direction: &'static str) {
        match direction {
            "out" => {
                self.adjacency_scans_out.fetch_add(1, Ordering::Relaxed);
            }
            "in" => {
                self.adjacency_scans_in.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn degree_query(&self, direction: &'static str, cached: bool) {
        match direction {
            "out" => {
                self.degree_queries_out.fetch_add(1, Ordering::Relaxed);
            }
            "in" => {
                self.degree_queries_in.fetch_add(1, Ordering::Relaxed);
            }
            "both" => {
                self.degree_queries_both.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        if cached {
            self.degree_cache_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.degree_cache_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn mvcc_reader_gauges(
        &self,
        active: u64,
        oldest_commit: CommitId,
        newest_commit: CommitId,
        max_age_ms: u64,
    ) {
        self.mvcc_reader_active.store(active, Ordering::Relaxed);
        self.mvcc_reader_oldest_commit
            .store(oldest_commit, Ordering::Relaxed);
        self.mvcc_reader_newest_commit
            .store(newest_commit, Ordering::Relaxed);
        self.mvcc_reader_max_age_ms
            .store(max_age_ms, Ordering::Relaxed);
    }

    fn mvcc_reader_totals(&self, begin_total: u64, end_total: u64) {
        self.mvcc_reader_begin_total
            .store(begin_total, Ordering::Relaxed);
        self.mvcc_reader_end_total
            .store(end_total, Ordering::Relaxed);
    }

    fn mvcc_page_versions(&self, total_versions: u64, pages_with_versions: u64) {
        self.mvcc_page_versions_total
            .store(total_versions, Ordering::Relaxed);
        self.mvcc_pages_with_versions
            .store(pages_with_versions, Ordering::Relaxed);
    }
}

/// Returns the default metrics implementation wrapped in an [`Arc`].
///
/// The default implementation is [`NoopMetrics`], which has zero overhead
/// as it discards all recorded metrics.
///
/// # Returns
/// An [`Arc`] containing a [`NoopMetrics`] instance.
pub fn default_metrics() -> Arc<dyn StorageMetrics> {
    Arc::new(NoopMetrics::default())
}
