use std::sync::Arc;

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
}
