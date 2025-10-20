use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

const MAX_LATENCY_SAMPLES: usize = 10000;
const MAX_COMMIT_LATENCY_SAMPLES: usize = 1000;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub label_index_queries: u64,
    pub node_lookups: u64,
    pub edge_traversals: u64,
    pub property_index_hits: u64,
    pub property_index_misses: u64,
    pub transactions_committed: u64,
    pub transactions_rolled_back: u64,
    pub wal_syncs: u64,
    pub wal_bytes_written: u64,
    pub checkpoints_performed: u64,
    pub page_evictions: u64,
    pub corruption_errors: u64,
    #[serde(skip)]
    commit_latencies_ms: VecDeque<u64>,
    #[serde(skip)]
    read_latencies_us: VecDeque<u64>,
}

impl PerformanceMetrics {
    pub fn new() -> Self {
        Self {
            commit_latencies_ms: VecDeque::with_capacity(MAX_COMMIT_LATENCY_SAMPLES),
            read_latencies_us: VecDeque::with_capacity(MAX_LATENCY_SAMPLES),
            ..Default::default()
        }
    }

    pub fn cache_hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }

    pub fn property_index_hit_rate(&self) -> f64 {
        let total = self.property_index_hits + self.property_index_misses;
        if total == 0 {
            0.0
        } else {
            self.property_index_hits as f64 / total as f64
        }
    }

    pub fn record_property_index_hit(&mut self) {
        self.property_index_hits += 1;
    }

    pub fn record_property_index_miss(&mut self) {
        self.property_index_misses += 1;
    }

    pub fn record_commit_latency(&mut self, latency_ms: u64) {
        if self.commit_latencies_ms.len() >= MAX_COMMIT_LATENCY_SAMPLES {
            self.commit_latencies_ms.pop_front();
        }
        self.commit_latencies_ms.push_back(latency_ms);
        self.transactions_committed += 1;
    }

    pub fn record_read_latency(&mut self, latency_us: u64) {
        if self.read_latencies_us.len() >= MAX_LATENCY_SAMPLES {
            self.read_latencies_us.pop_front();
        }
        self.read_latencies_us.push_back(latency_us);
    }

    pub fn p50_commit_latency(&self) -> Option<u64> {
        self.percentile(&self.commit_latencies_ms, 50)
    }

    pub fn p95_commit_latency(&self) -> Option<u64> {
        self.percentile(&self.commit_latencies_ms, 95)
    }

    pub fn p99_commit_latency(&self) -> Option<u64> {
        self.percentile(&self.commit_latencies_ms, 99)
    }

    pub fn p50_read_latency(&self) -> Option<u64> {
        self.percentile(&self.read_latencies_us, 50)
    }

    pub fn p95_read_latency(&self) -> Option<u64> {
        self.percentile(&self.read_latencies_us, 95)
    }

    pub fn p99_read_latency(&self) -> Option<u64> {
        self.percentile(&self.read_latencies_us, 99)
    }

    fn percentile(&self, samples: &VecDeque<u64>, percentile: u8) -> Option<u64> {
        if samples.is_empty() {
            return None;
        }
        let mut sorted: Vec<u64> = samples.iter().copied().collect();
        sorted.sort_unstable();
        let index = (percentile as f64 / 100.0 * sorted.len() as f64).ceil() as usize;
        sorted.get(index.saturating_sub(1)).copied()
    }

    pub fn reset(&mut self) {
        *self = Self::new();
    }

    pub fn print_report(&self) {
        println!("\n=== Performance Metrics ===");
        println!("Cache Hits:                {}", self.cache_hits);
        println!("Cache Misses:              {}", self.cache_misses);
        println!(
            "Cache Hit Rate:            {:.2}%",
            self.cache_hit_rate() * 100.0
        );
        println!("Label Index Queries:       {}", self.label_index_queries);
        println!("Node Lookups:              {}", self.node_lookups);
        println!("Edge Traversals:           {}", self.edge_traversals);
        println!("Property Index Hits:       {}", self.property_index_hits);
        println!("Property Index Misses:     {}", self.property_index_misses);
        println!(
            "Property Index Hit Rate:   {:.2}%",
            self.property_index_hit_rate() * 100.0
        );
        println!("Transactions Committed:    {}", self.transactions_committed);
        println!("Transactions Rolled Back:  {}", self.transactions_rolled_back);
        println!("WAL Syncs:                 {}", self.wal_syncs);
        println!("WAL Bytes Written:         {}", self.wal_bytes_written);
        println!("Checkpoints Performed:     {}", self.checkpoints_performed);
        println!("Page Evictions:            {}", self.page_evictions);
        println!("Corruption Errors:         {}", self.corruption_errors);
        if let Some(p50) = self.p50_commit_latency() {
            println!("P50 Commit Latency:        {}ms", p50);
        }
        if let Some(p95) = self.p95_commit_latency() {
            println!("P95 Commit Latency:        {}ms", p95);
        }
        if let Some(p99) = self.p99_commit_latency() {
            println!("P99 Commit Latency:        {}ms", p99);
        }
        if let Some(p50) = self.p50_read_latency() {
            println!("P50 Read Latency:          {}μs", p50);
        }
        if let Some(p95) = self.p95_read_latency() {
            println!("P95 Read Latency:          {}μs", p95);
        }
        if let Some(p99) = self.p99_read_latency() {
            println!("P99 Read Latency:          {}μs", p99);
        }
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn to_prometheus_format(&self) -> String {
        let mut output = String::new();
        
        output.push_str("# HELP sombra_cache_hits Total number of cache hits\n");
        output.push_str("# TYPE sombra_cache_hits counter\n");
        output.push_str(&format!("sombra_cache_hits {}\n", self.cache_hits));
        
        output.push_str("# HELP sombra_cache_misses Total number of cache misses\n");
        output.push_str("# TYPE sombra_cache_misses counter\n");
        output.push_str(&format!("sombra_cache_misses {}\n", self.cache_misses));
        
        output.push_str("# HELP sombra_cache_hit_rate Current cache hit rate\n");
        output.push_str("# TYPE sombra_cache_hit_rate gauge\n");
        output.push_str(&format!("sombra_cache_hit_rate {:.4}\n", self.cache_hit_rate()));
        
        output.push_str("# HELP sombra_label_index_queries Total label index queries\n");
        output.push_str("# TYPE sombra_label_index_queries counter\n");
        output.push_str(&format!("sombra_label_index_queries {}\n", self.label_index_queries));
        
        output.push_str("# HELP sombra_node_lookups Total node lookups\n");
        output.push_str("# TYPE sombra_node_lookups counter\n");
        output.push_str(&format!("sombra_node_lookups {}\n", self.node_lookups));
        
        output.push_str("# HELP sombra_edge_traversals Total edge traversals\n");
        output.push_str("# TYPE sombra_edge_traversals counter\n");
        output.push_str(&format!("sombra_edge_traversals {}\n", self.edge_traversals));
        
        output.push_str("# HELP sombra_property_index_hits Total property index hits\n");
        output.push_str("# TYPE sombra_property_index_hits counter\n");
        output.push_str(&format!("sombra_property_index_hits {}\n", self.property_index_hits));
        
        output.push_str("# HELP sombra_property_index_misses Total property index misses\n");
        output.push_str("# TYPE sombra_property_index_misses counter\n");
        output.push_str(&format!("sombra_property_index_misses {}\n", self.property_index_misses));
        
        output.push_str("# HELP sombra_transactions_committed Total committed transactions\n");
        output.push_str("# TYPE sombra_transactions_committed counter\n");
        output.push_str(&format!("sombra_transactions_committed {}\n", self.transactions_committed));
        
        output.push_str("# HELP sombra_transactions_rolled_back Total rolled back transactions\n");
        output.push_str("# TYPE sombra_transactions_rolled_back counter\n");
        output.push_str(&format!("sombra_transactions_rolled_back {}\n", self.transactions_rolled_back));
        
        output.push_str("# HELP sombra_wal_syncs Total WAL syncs\n");
        output.push_str("# TYPE sombra_wal_syncs counter\n");
        output.push_str(&format!("sombra_wal_syncs {}\n", self.wal_syncs));
        
        output.push_str("# HELP sombra_wal_bytes_written Total bytes written to WAL\n");
        output.push_str("# TYPE sombra_wal_bytes_written counter\n");
        output.push_str(&format!("sombra_wal_bytes_written {}\n", self.wal_bytes_written));
        
        output.push_str("# HELP sombra_checkpoints_performed Total checkpoints performed\n");
        output.push_str("# TYPE sombra_checkpoints_performed counter\n");
        output.push_str(&format!("sombra_checkpoints_performed {}\n", self.checkpoints_performed));
        
        output.push_str("# HELP sombra_page_evictions Total page evictions\n");
        output.push_str("# TYPE sombra_page_evictions counter\n");
        output.push_str(&format!("sombra_page_evictions {}\n", self.page_evictions));
        
        output.push_str("# HELP sombra_corruption_errors Total corruption errors\n");
        output.push_str("# TYPE sombra_corruption_errors counter\n");
        output.push_str(&format!("sombra_corruption_errors {}\n", self.corruption_errors));
        
        if let Some(p50) = self.p50_commit_latency() {
            output.push_str("# HELP sombra_commit_latency_p50_ms P50 commit latency in milliseconds\n");
            output.push_str("# TYPE sombra_commit_latency_p50_ms gauge\n");
            output.push_str(&format!("sombra_commit_latency_p50_ms {}\n", p50));
        }
        
        if let Some(p95) = self.p95_commit_latency() {
            output.push_str("# HELP sombra_commit_latency_p95_ms P95 commit latency in milliseconds\n");
            output.push_str("# TYPE sombra_commit_latency_p95_ms gauge\n");
            output.push_str(&format!("sombra_commit_latency_p95_ms {}\n", p95));
        }
        
        if let Some(p99) = self.p99_commit_latency() {
            output.push_str("# HELP sombra_commit_latency_p99_ms P99 commit latency in milliseconds\n");
            output.push_str("# TYPE sombra_commit_latency_p99_ms gauge\n");
            output.push_str(&format!("sombra_commit_latency_p99_ms {}\n", p99));
        }
        
        output
    }

    pub fn to_statsd(&self, prefix: &str) -> Vec<String> {
        let mut metrics = Vec::new();
        
        metrics.push(format!("{}.cache_hits:{}|c", prefix, self.cache_hits));
        metrics.push(format!("{}.cache_misses:{}|c", prefix, self.cache_misses));
        metrics.push(format!("{}.cache_hit_rate:{:.4}|g", prefix, self.cache_hit_rate()));
        metrics.push(format!("{}.label_index_queries:{}|c", prefix, self.label_index_queries));
        metrics.push(format!("{}.node_lookups:{}|c", prefix, self.node_lookups));
        metrics.push(format!("{}.edge_traversals:{}|c", prefix, self.edge_traversals));
        metrics.push(format!("{}.property_index_hits:{}|c", prefix, self.property_index_hits));
        metrics.push(format!("{}.property_index_misses:{}|c", prefix, self.property_index_misses));
        metrics.push(format!("{}.transactions_committed:{}|c", prefix, self.transactions_committed));
        metrics.push(format!("{}.transactions_rolled_back:{}|c", prefix, self.transactions_rolled_back));
        metrics.push(format!("{}.wal_syncs:{}|c", prefix, self.wal_syncs));
        metrics.push(format!("{}.wal_bytes_written:{}|c", prefix, self.wal_bytes_written));
        metrics.push(format!("{}.checkpoints_performed:{}|c", prefix, self.checkpoints_performed));
        metrics.push(format!("{}.page_evictions:{}|c", prefix, self.page_evictions));
        metrics.push(format!("{}.corruption_errors:{}|c", prefix, self.corruption_errors));
        
        if let Some(p50) = self.p50_commit_latency() {
            metrics.push(format!("{}.commit_latency_p50_ms:{}|g", prefix, p50));
        }
        if let Some(p95) = self.p95_commit_latency() {
            metrics.push(format!("{}.commit_latency_p95_ms:{}|g", prefix, p95));
        }
        if let Some(p99) = self.p99_commit_latency() {
            metrics.push(format!("{}.commit_latency_p99_ms:{}|g", prefix, p99));
        }
        
        metrics
    }
}
