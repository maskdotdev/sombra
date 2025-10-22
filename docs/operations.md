# Operations Guide

This guide covers operational aspects of running Sombra, including monitoring, maintenance, and troubleshooting.

## Monitoring

### Health Checks

Sombra provides built-in health monitoring:

```rust
use sombra::GraphDB;

let db = GraphDB::open("production.db")?;
let health = db.health_check()?;

println!("Health status: {:?}", health.status);

for check in &health.checks {
    println!("Check: {:?}", check);
}
```

The health check evaluates:
- Cache hit rate
- WAL size
- Corruption errors
- Time since last checkpoint

### Performance Metrics

Access performance metrics:

```rust
use sombra::GraphDB;

let db = GraphDB::open("production.db")?;
let metrics = db.metrics.lock().unwrap();

println!("Cache hit rate: {:.2}%", metrics.cache_hit_rate() * 100.0);
println!("Transactions committed: {}", metrics.transactions_committed);
println!("Node lookups: {}", metrics.node_lookups);
println!("Edge traversals: {}", metrics.edge_traversals);

if let Some(p99) = metrics.p99_commit_latency() {
    println!("P99 commit latency: {}ms", p99);
}
```

Available metrics include:
- **Cache metrics**: hits, misses, hit rate
- **Index metrics**: label index queries, property index hits/misses
- **Transaction metrics**: commits, rollbacks, latencies (P50/P95/P99)
- **WAL metrics**: syncs, bytes written
- **Checkpoint metrics**: checkpoints performed
- **Compaction metrics**: compactions, pages compacted, bytes reclaimed

### Metrics Export

Export metrics to monitoring systems:

```rust
let metrics = db.metrics.lock().unwrap();

println!("{}", metrics.to_prometheus_format());

let json = metrics.to_json()?;
println!("{}", json);

let statsd_metrics = metrics.to_statsd("sombra");
for metric in statsd_metrics {
    println!("{}", metric);
}
```

### Structured Logging

Configure logging for monitoring:

```rust
use sombra::logging;

logging::init_logging("info")?;
```

## Backup and Restore

Sombra uses WAL (Write-Ahead Logging) for durability. To backup a database:

1. Stop all write operations or use a read-only connection
2. Call `checkpoint()` to flush WAL to main database file
3. Copy the `.db` and `.db-wal` files

```bash
# Simple backup script
DATE=$(date +%Y%m%d_%H%M%S)
cp production.db "backups/backup_$DATE.db"
cp production.db-wal "backups/backup_$DATE.db-wal"
```

WAL recovery happens automatically on `GraphDB::open()` if the database was not cleanly shut down.

## Database Maintenance

### Checkpoint Management

Manually trigger WAL checkpoints:

```rust
let mut db = GraphDB::open("production.db")?;

db.checkpoint()?;
```

Checkpoints flush WAL entries to the main database file. They are triggered automatically based on configuration settings:
- `checkpoint_threshold`: Number of WAL frames before auto-checkpoint (default: 1000)

### Database Integrity

Verify database integrity:

```rust
use sombra::{GraphDB, IntegrityOptions};

let db = GraphDB::open("production.db")?;

let options = IntegrityOptions::default();
let report = db.verify_integrity(&options)?;

println!("Checked {} pages", report.checked_pages);
println!("Checksum failures: {}", report.checksum_failures);
println!("Record errors: {}", report.record_errors);
println!("Index errors: {}", report.index_errors);

for error in &report.errors {
    println!("Error: {}", error);
}
```

Integrity checking options:
- `checksum_only`: Only verify page checksums, skip record validation
- `max_errors`: Maximum errors to collect before stopping (default: 16)
- `verify_indexes`: Verify that indexes match actual data (default: true)
- `verify_adjacency`: Verify edge references point to valid nodes (default: true)

### Configuration Tuning

See the [Configuration Guide](configuration.md) for tuning:
- Cache size
- WAL sync mode
- Checkpoint threshold
- Memory-mapped I/O
- Compaction settings

## Troubleshooting

### High Memory Usage

**Symptoms:**
- Process using more memory than expected
- OOM errors

**Diagnosis:**
```rust
let metrics = db.metrics.lock().unwrap();
println!("Cache hit rate: {:.2}%", metrics.cache_hit_rate() * 100.0);
println!("Page evictions: {}", metrics.page_evictions);
```

**Solutions:**
- Reduce `page_cache_size` in config
- Ensure checkpoint is running regularly

### Slow Performance

**Symptoms:**
- High query latency
- Low throughput

**Diagnosis:**
```rust
let metrics = db.metrics.lock().unwrap();
println!("Cache hit rate: {:.2}%", metrics.cache_hit_rate() * 100.0);

if let Some(p99) = metrics.p99_commit_latency() {
    println!("P99 commit latency: {}ms", p99);
}
```

**Solutions:**
- Increase `page_cache_size` if cache hit rate is low (< 90%)
- Use appropriate WAL `sync_mode` for your durability requirements
- Enable `use_mmap` for read-heavy workloads
- Checkpoint regularly to prevent large WAL files

### Database Corruption

**Symptoms:**
- Corruption errors in logs
- Crashes on read/write
- Checksum failures

**Diagnosis:**
```rust
use sombra::{GraphDB, IntegrityOptions};

let db = GraphDB::open("production.db")?;
let report = db.verify_integrity(&IntegrityOptions::default())?;

if report.checksum_failures > 0 || report.record_errors > 0 {
    println!("Corruption detected:");
    for error in &report.errors {
        println!("  {}", error);
    }
}
```

**Solutions:**
1. Restore from backup if available
2. WAL recovery may fix some issues automatically on restart
3. Check hardware (disk errors, memory issues)
4. Review logs for patterns before corruption occurred

## Range Queries and Ordered Access

### Node Range Queries

Sombra provides efficient range queries using the BTreeMap-based node index:

```rust
use sombra::GraphDB;

let db = GraphDB::open("production.db")?;

let node_ids = db.get_nodes_in_range(100, 200);
println!("Found {} nodes between IDs 100 and 200", node_ids.len());

let node_ids = db.get_nodes_from(1000);
println!("Found {} nodes with ID >= 1000", node_ids.len());

let node_ids = db.get_nodes_to(500);
println!("Found {} nodes with ID <= 500", node_ids.len());
```

### Ordered Node Access

Access nodes in sorted order by their IDs:

```rust
if let Some(first_id) = db.get_first_node() {
    let node = db.get_node(first_id)?;
    println!("First node: {:?}", node);
}

if let Some(last_id) = db.get_last_node() {
    let node = db.get_node(last_id)?;
    println!("Last node: {:?}", node);
}

let first_100 = db.get_first_n_nodes(100);
println!("First 100 node IDs: {:?}", first_100);

let last_100 = db.get_last_n_nodes(100);
println!("Last 100 node IDs: {:?}", last_100);

let all_ids = db.get_all_node_ids_ordered();
println!("Total nodes: {}", all_ids.len());
```

### Use Cases for Range Queries

**Pagination:**
```rust
let page_size = 100;
let page_number = 5;

let all_ids = db.get_all_node_ids_ordered();
let start = page_number * page_size;
let page_ids = &all_ids[start..std::cmp::min(start + page_size, all_ids.len())];

for &node_id in page_ids {
    let node = db.get_node(node_id)?;
    println!("{:?}", node);
}
```

**Timeline Views:**
```rust
let recent_ids = db.get_last_n_nodes(50);
for &node_id in &recent_ids {
    let node = db.get_node(node_id)?;
    println!("Recent: {:?}", node);
}
```

**Batch Processing:**
```rust
let chunk_size = 1000;
let all_ids = db.get_all_node_ids_ordered();

for chunk in all_ids.chunks(chunk_size) {
    for &node_id in chunk {
        let node = db.get_node(node_id)?;
    }
    
    db.checkpoint()?;
}
```

### Range Queries in Transactions

Range queries work in transactions:

```rust
let mut tx = db.begin_transaction()?;

let node_ids = tx.get_nodes_in_range(100, 200);

for &node_id in &node_ids {
    tx.set_node_property(
        node_id,
        "processed".to_string(),
        PropertyValue::Bool(true)
    )?;
}

tx.commit()?;
```

### Performance Characteristics

Range queries leverage the BTreeMap index for optimal performance:

- **Point lookup**: O(log n) - ~440ns for 10K nodes
- **Range scan**: O(log n + k) - where k is result size
- **Full iteration**: O(n) - ~2.6ns per node
- **First/Last N**: O(log n + k) - < 1µs for N=100

## Property Updates

### Updating Node Properties

Modify node properties using `set_node_property`:

```rust
use sombra::{GraphDB, PropertyValue};

let mut db = GraphDB::open("production.db")?;

db.set_node_property(
    node_id,
    "status".to_string(),
    PropertyValue::String("active".to_string())
)?;

db.set_node_property(node_id, "count".to_string(), PropertyValue::Int(42))?;
db.set_node_property(node_id, "verified".to_string(), PropertyValue::Bool(true))?;
```

### Removing Node Properties

Remove properties from nodes:

```rust
db.remove_node_property(node_id, "temporary_flag")?;
```

### Property Updates in Transactions

Property updates within transactions:

```rust
let mut tx = db.begin_transaction()?;

tx.set_node_property(node_id, "counter".to_string(), PropertyValue::Int(42))?;
tx.remove_node_property(node_id, "old_field")?;

tx.commit()?;
```

### Performance Characteristics

Property updates use **update-in-place** optimization when possible:
- **In-place update**: When the new record fits in existing space, only one page write occurs
- **Fallback to reinsert**: When the record grows, the system falls back to delete+reinsert
- **Automatic index updates**: Property indexes are updated atomically with the property change

## Monitoring Integration

### Prometheus Metrics Exporter

Example Prometheus exporter:

```rust
use sombra::GraphDB;
use std::time::Duration;
use std::thread;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = GraphDB::open("production.db")?;
    
    loop {
        let metrics = db.metrics.lock().unwrap();
        println!("{}", metrics.to_prometheus_format());
        
        drop(metrics);
        thread::sleep(Duration::from_secs(60));
    }
}
```

### JSON Metrics API

For custom monitoring dashboards:

```rust
use sombra::GraphDB;
use std::fs::File;
use std::io::Write;

let db = GraphDB::open("production.db")?;
let metrics = db.metrics.lock().unwrap();

let json = metrics.to_json()?;
let mut file = File::create("metrics.json")?;
file.write_all(json.as_bytes())?;
```

## Next Steps

- Read the [Configuration Guide](configuration.md) for performance tuning
- Check the [Getting Started Guide](getting-started.md) for basic usage
- Review the [examples](../examples/) for operational patterns
