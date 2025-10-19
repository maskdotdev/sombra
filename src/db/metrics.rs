#[derive(Debug, Clone, Default)]
pub struct PerformanceMetrics {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub label_index_queries: u64,
    pub node_lookups: u64,
    pub edge_traversals: u64,
}

impl PerformanceMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cache_hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }

    pub fn print_report(&self) {
        println!("\n=== Performance Metrics ===");
        println!("Cache Hits:           {}", self.cache_hits);
        println!("Cache Misses:         {}", self.cache_misses);
        println!(
            "Cache Hit Rate:       {:.2}%",
            self.cache_hit_rate() * 100.0
        );
        println!("Label Index Queries:  {}", self.label_index_queries);
        println!("Node Lookups:         {}", self.node_lookups);
        println!("Edge Traversals:      {}", self.edge_traversals);
    }
}
