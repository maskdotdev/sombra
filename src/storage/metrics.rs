use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub trait StorageMetrics: Send + Sync {
    fn node_created(&self);
    fn node_deleted(&self);
    fn edge_created(&self);
    fn edge_deleted(&self);
    fn adjacency_scan(&self, direction: &'static str);
    fn degree_query(&self, direction: &'static str, cached: bool);
}

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

#[derive(Default)]
pub struct CounterMetrics {
    pub nodes_created: AtomicU64,
    pub nodes_deleted: AtomicU64,
    pub edges_created: AtomicU64,
    pub edges_deleted: AtomicU64,
    pub adjacency_scans_out: AtomicU64,
    pub adjacency_scans_in: AtomicU64,
    pub degree_queries_out: AtomicU64,
    pub degree_queries_in: AtomicU64,
    pub degree_queries_both: AtomicU64,
    pub degree_cache_hits: AtomicU64,
    pub degree_cache_misses: AtomicU64,
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
}

pub fn default_metrics() -> Arc<dyn StorageMetrics> {
    Arc::new(NoopMetrics::default())
}
