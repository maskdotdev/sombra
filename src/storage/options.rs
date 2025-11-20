use std::sync::Arc;
use std::time::Duration;

use crate::primitives::pager::PageStore;
use crate::storage::mvcc::VersionCodecKind;

/// Configuration options supplied when opening a [`super::Graph`].
#[derive(Clone)]
pub struct GraphOptions {
    /// The page store backend to use
    pub store: Arc<dyn PageStore>,
    /// Maximum size in bytes for inlining property blobs
    pub inline_prop_blob: Option<u32>,
    /// Maximum size in bytes for inlining property values
    pub inline_prop_value: Option<u32>,
    /// Whether to enable degree caching for nodes
    pub degree_cache: bool,
    /// Default behavior for distinct neighbors traversal
    pub distinct_neighbors_default: bool,
    /// Optional metrics collection implementation
    pub metrics: Option<Arc<dyn super::metrics::StorageMetrics>>,
    /// Whether to append SipHash64 footers to node/edge rows.
    pub row_hash_header: bool,
    /// Whether to attempt in-place inserts for B-tree write paths.
    pub btree_inplace: bool,
    /// Background MVCC vacuum configuration.
    pub vacuum: VacuumCfg,
    /// Codec to apply to historical version payloads.
    pub version_codec: VersionCodecKind,
    /// Minimum payload size before attempting compression.
    pub version_codec_min_payload_len: usize,
    /// Minimum bytes saved for compression to be applied.
    pub version_codec_min_savings_bytes: usize,
    /// Whether to embed the newest historical version inline on page heads.
    pub inline_history: bool,
    /// Maximum inline history payload size in bytes.
    pub inline_history_max_bytes: usize,
    /// Number of shards to split the version cache across.
    pub version_cache_shards: usize,
    /// Total capacity (entries) for the version cache.
    pub version_cache_capacity: usize,
    /// Whether adjacency updates should be buffered and flushed in bulk at commit.
    pub defer_adjacency_flush: bool,
    /// Whether index updates should be buffered and flushed in bulk at commit.
    pub defer_index_flush: bool,
    /// Maximum number of snapshots to retain for reuse.
    pub snapshot_pool_size: usize,
    /// Maximum age in milliseconds for cached snapshots.
    pub snapshot_pool_max_age_ms: u64,
}

impl GraphOptions {
    /// Creates a new GraphOptions with default settings.
    pub fn new(store: Arc<dyn PageStore>) -> Self {
        Self {
            store,
            inline_prop_blob: None,
            inline_prop_value: None,
            degree_cache: cfg!(feature = "degree-cache"),
            distinct_neighbors_default: false,
            metrics: None,
            row_hash_header: false,
            btree_inplace: false,
            vacuum: VacuumCfg::default(),
            version_codec: VersionCodecKind::None,
            version_codec_min_payload_len: 64,
            version_codec_min_savings_bytes: 8,
            inline_history: true,
            inline_history_max_bytes: 1024,
            version_cache_shards: 8,
            version_cache_capacity: 2048,
            defer_adjacency_flush: false,
            defer_index_flush: false,
            snapshot_pool_size: 0,
            snapshot_pool_max_age_ms: 200,
        }
    }

    /// Sets the maximum size for inlining property blobs.
    pub fn inline_prop_blob(mut self, bytes: u32) -> Self {
        self.inline_prop_blob = Some(bytes);
        self
    }

    /// Sets the maximum size for inlining property values.
    pub fn inline_prop_value(mut self, bytes: u32) -> Self {
        self.inline_prop_value = Some(bytes);
        self
    }

    /// Enables or disables degree caching.
    pub fn degree_cache(mut self, enabled: bool) -> Self {
        self.degree_cache = enabled;
        self
    }

    /// Sets the default behavior for distinct neighbors traversal.
    pub fn distinct_neighbors_default(mut self, distinct: bool) -> Self {
        self.distinct_neighbors_default = distinct;
        self
    }

    /// Sets the metrics collection implementation.
    pub fn metrics(mut self, metrics: Arc<dyn super::metrics::StorageMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Enables or disables SipHash64 footers on node and edge rows.
    pub fn row_hash_header(mut self, enabled: bool) -> Self {
        self.row_hash_header = enabled;
        self
    }

    /// Enables or disables in-place inserts for underlying B-trees.
    pub fn btree_inplace(mut self, enabled: bool) -> Self {
        self.btree_inplace = enabled;
        self
    }

    /// Sets the background vacuum configuration.
    pub fn vacuum(mut self, cfg: VacuumCfg) -> Self {
        self.vacuum = cfg;
        self
    }

    /// Selects the codec applied to version-log payloads.
    pub fn version_codec(mut self, codec: VersionCodecKind) -> Self {
        self.version_codec = codec;
        self
    }

    /// Sets the minimum payload length before compression runs.
    pub fn version_codec_min_payload_len(mut self, bytes: usize) -> Self {
        self.version_codec_min_payload_len = bytes;
        self
    }

    /// Sets the minimum bytes that must be saved by compression to accept it.
    pub fn version_codec_min_savings_bytes(mut self, bytes: usize) -> Self {
        self.version_codec_min_savings_bytes = bytes;
        self
    }

    /// Enables or disables embedding the newest historical version inline.
    pub fn inline_history(mut self, enabled: bool) -> Self {
        self.inline_history = enabled;
        self
    }

    /// Sets the maximum inline history payload length.
    pub fn inline_history_max_bytes(mut self, bytes: usize) -> Self {
        self.inline_history_max_bytes = bytes;
        self
    }

    /// Configures the per-page version cache shards.
    pub fn version_cache_shards(mut self, shards: usize) -> Self {
        self.version_cache_shards = shards;
        self
    }

    /// Configures the total capacity for the version cache.
    pub fn version_cache_capacity(mut self, capacity: usize) -> Self {
        self.version_cache_capacity = capacity;
        self
    }

    /// Enables or disables buffering adjacency updates until commit.
    pub fn defer_adjacency_flush(mut self, enabled: bool) -> Self {
        self.defer_adjacency_flush = enabled;
        self
    }

    /// Enables or disables buffering index updates until commit.
    pub fn defer_index_flush(mut self, enabled: bool) -> Self {
        self.defer_index_flush = enabled;
        self
    }

    /// Sets the snapshot pool size (0 disables pooling).
    pub fn snapshot_pool_size(mut self, size: usize) -> Self {
        self.snapshot_pool_size = size;
        self
    }

    /// Sets the maximum cached snapshot age in milliseconds.
    pub fn snapshot_pool_max_age_ms(mut self, ms: u64) -> Self {
        self.snapshot_pool_max_age_ms = ms;
        self
    }
}

/// Configuration for background MVCC cleanup.
#[derive(Clone)]
pub struct VacuumCfg {
    /// Whether the background worker is enabled.
    pub enabled: bool,
    /// Target interval between cleanup passes.
    pub interval: Duration,
    /// Retention window for historical versions.
    pub retention_window: Duration,
    /// Version-log size that triggers eager cleanup.
    pub log_high_water_bytes: u64,
    /// Maximum version-log entries to prune per pass.
    pub max_pages_per_pass: usize,
    /// Soft runtime budget per pass (milliseconds).
    pub max_millis_per_pass: u64,
    /// Whether the pass should also clean secondary indexes.
    pub index_cleanup: bool,
}

impl Default for VacuumCfg {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(5),
            retention_window: Duration::from_secs(60 * 60 * 24),
            log_high_water_bytes: 512 * 1024 * 1024,
            max_pages_per_pass: 128,
            max_millis_per_pass: 50,
            index_cleanup: true,
        }
    }
}
