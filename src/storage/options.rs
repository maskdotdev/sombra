use std::sync::Arc;

use crate::primitives::pager::PageStore;

/// Configuration options supplied when opening a [`Graph`].
#[derive(Clone)]
pub struct GraphOptions {
    pub store: Arc<dyn PageStore>,
    pub inline_prop_blob: Option<u32>,
    pub inline_prop_value: Option<u32>,
    pub degree_cache: bool,
    pub distinct_neighbors_default: bool,
    pub metrics: Option<Arc<dyn super::metrics::StorageMetrics>>,
}

impl GraphOptions {
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

    pub fn inline_prop_blob(mut self, bytes: u32) -> Self {
        self.inline_prop_blob = Some(bytes);
        self
    }

    pub fn inline_prop_value(mut self, bytes: u32) -> Self {
        self.inline_prop_value = Some(bytes);
        self
    }

    pub fn degree_cache(mut self, enabled: bool) -> Self {
        self.degree_cache = enabled;
        self
    }

    pub fn distinct_neighbors_default(mut self, distinct: bool) -> Self {
        self.distinct_neighbors_default = distinct;
        self
    }

    pub fn metrics(mut self, metrics: Arc<dyn super::metrics::StorageMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }
}
