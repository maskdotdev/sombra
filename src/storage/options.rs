use std::sync::Arc;
use std::time::Duration;

use crate::primitives::pager::PageStore;

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
