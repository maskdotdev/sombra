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
    /// Records the current vacuum mode selection.
    fn mvcc_vacuum_mode(&self, _mode: &'static str) {}
    /// Records opportunistic micro-GC trimming of version chains.
    fn mvcc_micro_gc_trim(&self, _entries_pruned: u64, _pages_pruned: u64) {}

    /// Increments the total number of version-log entries pruned by vacuum.
    fn vacuum_versions_pruned(&self, _count: u64) {}

    /// Increments the total number of orphaned version-log entries pruned by vacuum.
    fn vacuum_orphan_versions_pruned(&self, _count: u64) {}

    /// Increments the total number of tombstone heads purged by vacuum.
    fn vacuum_tombstone_heads_purged(&self, _count: u64) {}

    /// Records adjacency entries removed by vacuum (forward and reverse).
    fn vacuum_adjacency_pruned(&self, _fwd: u64, _rev: u64) {}

    /// Records index entries removed by vacuum (label, chunked, btree).
    fn vacuum_index_entries_pruned(&self, _label: u64, _chunked: u64, _btree: u64) {}

    /// Increments the total number of bytes reclaimed by vacuum.
    fn vacuum_bytes_reclaimed(&self, _bytes: u64) {}

    /// Records the runtime in milliseconds for the most recent vacuum pass.
    fn vacuum_run_millis(&self, _millis: u64) {}

    /// Records the horizon commit applied by the most recent vacuum pass.
    fn vacuum_horizon_commit(&self, _horizon: CommitId) {}

    /// Updates gauges describing current version-log usage.
    fn version_log_usage(&self, _bytes: u64, _entries: u64) {}

    /// Records the number of commits acknowledged but not yet durable.
    fn mvcc_commit_backlog(&self, _acked_not_durable: u64) {}

    /// Records bytes processed by a version codec.
    fn version_codec_bytes(&self, _codec: &'static str, _raw_bytes: u64, _encoded_bytes: u64) {}

    /// Records a cache hit for the version cache.
    fn version_cache_hit(&self) {}

    /// Records a cache miss for the version cache.
    fn version_cache_miss(&self) {}

    /// Records a bulk adjacency flush.
    fn adjacency_bulk_flush(&self, _inserts: usize, _removals: usize) {}
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

    /// Total version-log entries pruned by background vacuum.
    pub vacuum_versions_pruned_total: AtomicU64,

    /// Total orphaned version-log entries pruned by background vacuum.
    pub vacuum_orphan_versions_pruned_total: AtomicU64,

    /// Total tombstone heads purged by vacuum.
    pub vacuum_tombstone_heads_purged_total: AtomicU64,

    /// Total forward adjacency entries pruned.
    pub vacuum_adjacency_fwd_pruned_total: AtomicU64,

    /// Total reverse adjacency entries pruned.
    pub vacuum_adjacency_rev_pruned_total: AtomicU64,

    /// Total label index entries pruned.
    pub vacuum_index_label_pruned_total: AtomicU64,

    /// Total chunked index segments pruned.
    pub vacuum_index_chunked_pruned_total: AtomicU64,

    /// Total B-tree index entries pruned.
    pub vacuum_index_btree_pruned_total: AtomicU64,

    /// Total bytes reclaimed by vacuum.
    pub vacuum_bytes_reclaimed_total: AtomicU64,

    /// Last observed vacuum runtime in milliseconds.
    pub vacuum_last_run_millis: AtomicU64,

    /// Horizon commit applied by the most recent vacuum pass.
    pub vacuum_horizon_commit: AtomicU64,

    /// Current bytes tracked in the version log.
    pub version_log_bytes: AtomicU64,

    /// Current number of entries tracked in the version log.
    pub version_log_entries: AtomicU64,

    /// Current number of commits acknowledged but not yet durable.
    pub mvcc_commit_backlog: AtomicU64,

    /// Total version cache hits.
    pub version_cache_hits: AtomicU64,

    /// Total version cache misses.
    pub version_cache_misses: AtomicU64,

    /// Last advertised vacuum mode (as numeric tag).
    pub mvcc_vacuum_mode: AtomicU64,

    /// Total entries trimmed by micro-GC.
    pub mvcc_micro_gc_entries_pruned_total: AtomicU64,

    /// Total pages reclaimed by micro-GC.
    pub mvcc_micro_gc_pages_pruned_total: AtomicU64,

    /// Bytes seen by version codecs (raw).
    pub version_codec_raw_bytes: AtomicU64,

    /// Bytes written after version codecs.
    pub version_codec_encoded_bytes: AtomicU64,

    /// Bulk adjacency inserts flushed.
    pub adjacency_bulk_inserts: AtomicU64,

    /// Bulk adjacency removals flushed.
    pub adjacency_bulk_removals: AtomicU64,
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

    fn vacuum_versions_pruned(&self, count: u64) {
        self.vacuum_versions_pruned_total
            .fetch_add(count, Ordering::Relaxed);
    }

    fn vacuum_orphan_versions_pruned(&self, count: u64) {
        self.vacuum_orphan_versions_pruned_total
            .fetch_add(count, Ordering::Relaxed);
    }

    fn vacuum_tombstone_heads_purged(&self, count: u64) {
        self.vacuum_tombstone_heads_purged_total
            .fetch_add(count, Ordering::Relaxed);
    }

    fn vacuum_adjacency_pruned(&self, fwd: u64, rev: u64) {
        self.vacuum_adjacency_fwd_pruned_total
            .fetch_add(fwd, Ordering::Relaxed);
        self.vacuum_adjacency_rev_pruned_total
            .fetch_add(rev, Ordering::Relaxed);
    }

    fn vacuum_index_entries_pruned(&self, label: u64, chunked: u64, btree: u64) {
        self.vacuum_index_label_pruned_total
            .fetch_add(label, Ordering::Relaxed);
        self.vacuum_index_chunked_pruned_total
            .fetch_add(chunked, Ordering::Relaxed);
        self.vacuum_index_btree_pruned_total
            .fetch_add(btree, Ordering::Relaxed);
    }

    fn vacuum_bytes_reclaimed(&self, bytes: u64) {
        self.vacuum_bytes_reclaimed_total
            .fetch_add(bytes, Ordering::Relaxed);
    }

    fn vacuum_run_millis(&self, millis: u64) {
        self.vacuum_last_run_millis.store(millis, Ordering::Relaxed);
    }

    fn vacuum_horizon_commit(&self, horizon: CommitId) {
        self.vacuum_horizon_commit.store(horizon, Ordering::Relaxed);
    }

    fn version_log_usage(&self, bytes: u64, entries: u64) {
        self.version_log_bytes.store(bytes, Ordering::Relaxed);
        self.version_log_entries.store(entries, Ordering::Relaxed);
    }

    fn mvcc_commit_backlog(&self, acked_not_durable: u64) {
        self.mvcc_commit_backlog
            .store(acked_not_durable, Ordering::Relaxed);
    }

    fn version_codec_bytes(&self, _codec: &'static str, raw_bytes: u64, encoded_bytes: u64) {
        self.version_codec_raw_bytes
            .fetch_add(raw_bytes, Ordering::Relaxed);
        self.version_codec_encoded_bytes
            .fetch_add(encoded_bytes, Ordering::Relaxed);
    }

    fn version_cache_hit(&self) {
        self.version_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn version_cache_miss(&self) {
        self.version_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    fn mvcc_vacuum_mode(&self, mode: &'static str) {
        let tag = match mode {
            "fast" => 2,
            "normal" => 1,
            "slow" => 0,
            _ => 3,
        };
        self.mvcc_vacuum_mode.store(tag, Ordering::Relaxed);
    }

    fn mvcc_micro_gc_trim(&self, entries_pruned: u64, pages_pruned: u64) {
        if entries_pruned > 0 {
            self.mvcc_micro_gc_entries_pruned_total
                .fetch_add(entries_pruned, Ordering::Relaxed);
        }
        if pages_pruned > 0 {
            self.mvcc_micro_gc_pages_pruned_total
                .fetch_add(pages_pruned, Ordering::Relaxed);
        }
    }

    fn adjacency_bulk_flush(&self, inserts: usize, removals: usize) {
        if inserts > 0 {
            self.adjacency_bulk_inserts
                .fetch_add(inserts as u64, Ordering::Relaxed);
        }
        if removals > 0 {
            self.adjacency_bulk_removals
                .fetch_add(removals as u64, Ordering::Relaxed);
        }
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
