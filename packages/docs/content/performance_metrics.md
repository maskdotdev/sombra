# Performance Metrics

Sombra includes built-in performance metrics to help you understand how the database is performing and optimize your usage patterns.

## Available Metrics

The `PerformanceMetrics` struct tracks the following statistics:

- **Cache Hits**: Number of times a node was found in the LRU cache
- **Cache Misses**: Number of times a node had to be read from disk
- **Cache Hit Rate**: Percentage of cache hits (calculated as hits / (hits + misses))
- **Label Index Queries**: Number of times the label index was queried
- **Node Lookups**: Total number of node lookup operations
- **Edge Traversals**: Number of edges traversed during graph operations

## Accessing Metrics

Metrics are automatically tracked on the `GraphDB` instance and can be accessed via the `metrics` field:

```rust
use sombra::GraphDB;

let mut db = GraphDB::open_arc("my_database.db")?;

// Perform some operations
for i in 1..=100 {
    let node = db.get_node(i)?;
}

// View the metrics
db.metrics.print_report();

// Access individual metrics
println!("Cache hit rate: {:.2}%", db.metrics.cache_hit_rate() * 100.0);
println!("Total node lookups: {}", db.metrics.node_lookups);

// Reset metrics for a fresh measurement
db.metrics.reset();
```

## Interpreting Metrics

### Cache Hit Rate

A high cache hit rate (>80%) indicates that your access patterns benefit from caching:
- **90%+**: Excellent - repeated access to the same nodes
- **50-90%**: Good - moderate locality of reference
- **<50%**: Poor - random access pattern or cache too small

If you have a low cache hit rate, consider:
1. Increasing the cache size via `Config::page_cache_size`
2. Optimizing query patterns to access nearby nodes together
3. Pre-loading frequently accessed nodes

### Label Index Queries

Label index queries are O(1) operations thanks to the secondary index. High numbers here indicate:
- Heavy use of label-based filtering
- Effective use of the optimization (vs. linear scans)

### Edge Traversals

This metric shows how many edges were traversed during graph operations. High numbers relative to node lookups suggest:
- Graph traversal operations (neighbors, BFS, etc.)
- Dense graphs with many connections
- Potential benefit from adjacency caching (Phase 2 optimization)

## Example: Comparing Cold vs Warm Cache

```rust
use sombra::{GraphDB, data_generator::DataGenerator};

let mut db = GraphDB::open_arc("benchmark.db")?;
let mut generator = DataGenerator::new();
let (nodes, edges) = generator.generate_medium_dataset();

// Populate database
let mut tx = db.begin_transaction()?;
for node in &nodes {
    tx.add_node(node.clone())?;
}
tx.commit()?;

// Cold cache test
println!("--- Cold Cache ---");
for i in 1..=100 {
    let _node = db.get_node(i)?;
}
db.metrics.print_report();
db.metrics.reset();

// Warm cache test
println!("\n--- Warm Cache ---");
for _ in 0..10 {
    for i in 1..=100 {
        let _node = db.get_node(i)?;
    }
}
db.metrics.print_report();
```

Expected output:
```
--- Cold Cache ---
=== Performance Metrics ===
Cache Hits:           0
Cache Misses:         100
Cache Hit Rate:       0.00%
Node Lookups:         100

--- Warm Cache ---
=== Performance Metrics ===
Cache Hits:           900
Cache Misses:         100
Cache Hit Rate:       90.00%
Node Lookups:         1000
```

## Performance Metrics in Benchmarks

The scalability benchmark suite automatically collects and reports performance metrics. Run it with:

```bash
cargo bench --bench scalability_benchmark --features benchmarks
```

Or run the demo example:

```bash
cargo run --example performance_metrics_demo --features benchmarks
```

## API Reference

### `PerformanceMetrics` Struct

```rust
pub struct PerformanceMetrics {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub label_index_queries: u64,
    pub node_lookups: u64,
    pub edge_traversals: u64,
}
```

### Methods

- `new()` - Create a new metrics instance
- `cache_hit_rate() -> f64` - Calculate cache hit rate (0.0 to 1.0)
- `reset()` - Reset all metrics to zero
- `print_report()` - Print a formatted metrics report to stdout

## Integration with Monitoring Systems

For production use, you can export metrics to your monitoring system:

```rust
use sombra::GraphDB;

let mut db = GraphDB::open_arc("production.db")?;

// Perform operations...

// Export to your monitoring system
let metrics = &db.metrics;
my_monitoring_system.gauge("sombra.cache_hit_rate", metrics.cache_hit_rate());
my_monitoring_system.counter("sombra.node_lookups", metrics.node_lookups);
my_monitoring_system.counter("sombra.edge_traversals", metrics.edge_traversals);
```

## See Also

- [Optimization Implementation Guide](./lookup_optimization_implementation.md)
- [Optimization API Guide](./optimization_api_guide.md)
- [Benchmarking Guide](../README_BENCHMARKS.md)
