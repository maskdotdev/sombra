# Configuration Guide

Sombra provides flexible configuration options to optimize performance for different use cases. This guide covers all configuration options and their tradeoffs.

## Configuration Overview

### Rust

```rust
use sombra::{Config, GraphDB, SyncMode};

// Create custom configuration
let mut config = Config::default();
config.page_cache_size = 250000;  // ~1GB cache (4KB pages)
config.wal_sync_mode = SyncMode::GroupCommit;
config.auto_checkpoint_interval_ms = Some(30000);  // 30 seconds
config.max_transaction_pages = 10000;

let db = GraphDB::open_with_config("my_graph.db", config)?;
```

### Python

```python
# Python bindings currently only support default configuration
# Configuration must be set when opening the database in Rust
import sombra

db = sombra.SombraDB("my_graph.db")
```

### Node.js

```typescript
// Node.js bindings currently only support default configuration
// Configuration must be set when opening the database in Rust
import { SombraDB } from 'sombra';

const db = new SombraDB('my_graph.db');
```

## Configuration Options

### Core Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `page_cache_size` | `usize` | 10000 | Number of pages to cache (1 page = 4KB) |
| `wal_sync_mode` | `SyncMode` | `Full` | WAL synchronization mode |
| `auto_checkpoint_interval_ms` | `Option<u64>` | `Some(30000)` | Auto-checkpoint interval in milliseconds |
| `max_transaction_pages` | `usize` | 10000 | Maximum pages per transaction |
| `checkpoint_threshold` | `usize` | 1000 | WAL frames before auto-checkpoint |

### Performance Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `group_commit_timeout_ms` | `u64` | 1 | Group commit timeout in milliseconds |
| `sync_interval` | `usize` | 1 | Operations between syncs in Normal mode |
| `use_mmap` | `bool` | `true` | Use memory-mapped I/O |
| `parallel_traversal_threshold` | `usize` | 1024 | Min workload for parallel traversal |
| `rayon_thread_pool_size` | `Option<usize>` | `None` | Override thread pool size |

### Safety Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_database_size_mb` | `Option<u64>` | `None` | Maximum database size in MB |
| `max_wal_size_mb` | `u64` | 100 | Maximum WAL size in MB |
| `transaction_timeout_ms` | `Option<u64>` | `None` | Transaction timeout in milliseconds |
| `checksum_enabled` | `bool` | `true` | Enable page checksums |
| `wal_size_warning_threshold_mb` | `u64` | 80 | WAL size warning threshold |

### Compaction Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enable_background_compaction` | `bool` | `false` | Enable background compaction |
| `compaction_interval_secs` | `Option<u64>` | `Some(300)` | Interval between compaction runs |
| `compaction_threshold_percent` | `u8` | 50 | Min dead space % to trigger compaction |
| `compaction_batch_size` | `usize` | 100 | Max pages per compaction run |

### Sync Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `SyncMode::Full` | Sync after every write | Maximum durability |
| `SyncMode::Normal` | Periodic sync | Balanced |
| `SyncMode::Checkpoint` | Sync only at checkpoints | Better performance |
| `SyncMode::GroupCommit` | Batch multiple transactions | High throughput |
| `SyncMode::Off` | No sync | Testing only |

## Predefined Profiles

### Production Profile

Optimized for durability and consistency in production environments.

```rust
let config = Config::production();
```

**Characteristics:**
- WAL enabled with fsync
- Conservative cache size (25% of available memory)
- Auto-checkpoint every 30 seconds
- Transaction timeout of 5 minutes
- Maximum database size limits

**Use Cases:**
- Production applications
- Financial systems
- Data that cannot be lost

### Balanced Profile

Balanced performance and durability for general use.

```rust
let config = Config::balanced();
```

**Characteristics:**
- WAL enabled with relaxed fsync
- Moderate cache size (50% of available memory)
- Auto-checkpoint every 60 seconds
- Group commit enabled
- No transaction timeout

**Use Cases:**
- Development environments
- Internal tools
- Applications with moderate durability requirements

### Benchmark Profile

Optimized for maximum performance, sacrificing durability.

```rust
let config = Config::benchmark();
```

**Characteristics:**
- WAL disabled
- Large cache size (80% of available memory)
- No auto-checkpoint
- Group commit enabled with aggressive settings
- No safety limits

**Use Cases:**
- Performance testing
- Temporary data processing
- ETL pipelines where data can be regenerated

## Performance Tuning Guidelines

### Memory Usage

#### Cache Size Tuning

The page cache is the most important performance setting:

```rust
// Calculate optimal cache size
// Note: 1 page = 4KB, so 250000 pages = ~1GB
let total_memory_mb = 8192; // 8GB system
let cache_percentage = 0.75;
let cache_size_mb = (total_memory_mb as f64 * cache_percentage) as u64;
let cache_pages = (cache_size_mb * 1024 * 1024 / 4096) as usize;

let mut config = Config::default();
config.page_cache_size = cache_pages;
```

**Guidelines:**
- **Embedded applications**: Use 50-70% of available memory
- **Server applications**: Use 70-80% of available memory
- **Memory-constrained**: Use 20-30% of available memory

**Calculation:** `pages = (MB * 1024 * 1024) / 4096`

#### Transaction Memory

Limit transaction memory usage to prevent OOM:

```rust
let mut config = Config::default();
config.max_transaction_pages = 25600;

// Calculation: For 100MB transaction limit
// 100MB / 4KB per page = 25600 pages
```

### Disk I/O Optimization

#### WAL Configuration

```rust
use sombra::SyncMode;

// High durability
let mut config = Config::default();
config.wal_sync_mode = SyncMode::Full;
config.auto_checkpoint_interval_ms = Some(30000);  // 30 seconds
config.max_wal_size_mb = 100;

// High performance
let mut config = Config::default();
config.wal_sync_mode = SyncMode::Normal;
config.sync_interval = 100;  // Sync every 100 operations
config.auto_checkpoint_interval_ms = Some(300000);  // 5 minutes
config.max_wal_size_mb = 1000;
```

#### Group Commit

Enable group commit for high-throughput workloads:

```rust
let mut config = Config::default();
config.wal_sync_mode = SyncMode::GroupCommit;
config.group_commit_timeout_ms = 5;  // Shorter = lower latency
```

**Tradeoffs:**
- **Shorter timeout**: Lower latency, less batching
- **Longer timeout**: Higher throughput, more latency

### Concurrency Optimization

#### Read-Heavy Workloads

```rust
let mut config = Config::default();
config.page_cache_size = 50000;  // Large cache
config.parallel_traversal_threshold = 512;  // Lower threshold for parallelism
```

#### Write-Heavy Workloads

```rust
let mut config = Config::default();
config.wal_sync_mode = SyncMode::GroupCommit;
config.group_commit_timeout_ms = 10;
config.checkpoint_threshold = 5000;  // Less frequent checkpoints
```

#### Mixed Workloads

```rust
let mut config = Config::balanced();  // Use balanced preset
config.page_cache_size = 20000;
config.auto_checkpoint_interval_ms = Some(60000);  // 1 minute
```

## Environment-Specific Tuning

### Development

```rust
let mut config = Config::default();
config.page_cache_size = 16000;  // ~64MB
config.wal_sync_mode = SyncMode::Full;  // Ensure data integrity
config.checksum_enabled = true;
config.auto_checkpoint_interval_ms = Some(10000);  // 10 seconds - frequent
config.max_transaction_pages = 1000;  // Small transactions
```

### Testing

```rust
let mut config = Config::benchmark();  // Fast, no durability
config.page_cache_size = 8000;  // ~32MB minimal
config.auto_checkpoint_interval_ms = None;  // No auto-checkpoint
config.max_transaction_pages = 500;  // Very small transactions
```

### Production

```rust
let mut config = Config::production();  // Start with production preset
// Customize based on your needs:
config.page_cache_size = calculate_production_cache();
config.max_database_size_mb = Some(1024 * 1024);  // 1TB limit
config.max_wal_size_mb = 500;
config.transaction_timeout_ms = Some(300000);  // 5 minutes
```

## Monitoring Configuration Impact

### Performance Metrics

Monitor these metrics to evaluate configuration changes:

```rust
use sombra::PerformanceMetrics;

let metrics = db.get_performance_metrics();

println!("Cache hit rate: {:.2}%", metrics.cache_hit_rate * 100.0);
println!("Average commit latency: {}ms", metrics.avg_commit_latency_ms);
println!("WAL size: {}MB", metrics.wal_size_bytes / 1024 / 1024);
println!("Dirty pages: {}", metrics.dirty_pages);
```

### Health Checks

```rust
let health = db.health_check();

match health.status {
    HealthStatus::Healthy => println!("System is healthy"),
    HealthStatus::Degraded => println!("System performance degraded"),
    HealthStatus::Unhealthy => println!("System needs attention"),
}

for check in health.checks {
    println!("{}: {} - {}", check.name, check.status, check.message);
}
```

## Configuration Best Practices

### 1. Start Conservative

Begin with the `balanced` profile and adjust based on measurements:

```rust
let mut config = Config::balanced();

// Measure baseline performance
let baseline = benchmark_with_config(&config);

// Make one change at a time
config.page_cache_size = new_cache_size;
let with_cache = benchmark_with_config(&config);

// Compare and keep improvements
if with_cache.throughput > baseline.throughput {
    println!("Cache size improvement: +{:.2}%", 
        (with_cache.throughput / baseline.throughput - 1.0) * 100.0);
}
```

### 2. Monitor Resource Usage

Track memory, disk I/O, and CPU usage:

```rust
// Monitor memory usage  
let memory_usage = get_process_memory();
let cache_bytes = config.page_cache_size * 4096;

if memory_usage > cache_bytes * 2 {
    println!("Consider reducing cache size");
}
```

### 3. Test Failure Scenarios

Test configuration under various failure conditions:

```rust
// Test with simulated disk full
test_with_disk_full(&config);

// Test with memory pressure
test_with_memory_pressure(&config);

// Test crash recovery
test_wal_recovery(&config);
```

### 4. Document Configuration Changes

Keep track of configuration changes and their impact:

```toml
# config-history.toml
[[change]]
date = "2024-01-15"
config_version = "v1.2.0"
changes = ["cache_size: 512MB -> 1GB", "group_commit: false -> true"]
impact = "throughput +15%, latency +5ms"
reason = "Handle increased user load"
```

## Troubleshooting

### High Memory Usage

**Symptoms:**
- Process using more memory than expected
- OOM errors

**Solutions:**
```rust
// Reduce cache size
config.page_cache_size = config.page_cache_size / 2;

// Reduce transaction limits
config.max_transaction_pages = config.max_transaction_pages / 2;
```

### Poor Performance

**Symptoms:**
- Slow queries
- Low throughput

**Solutions:**
```rust
// Increase cache size
config.page_cache_size = config.page_cache_size * 2;

// Enable group commit
config.wal_sync_mode = SyncMode::GroupCommit;

// Enable parallel traversal
config.parallel_traversal_threshold = 512;
```

### Data Loss Risk

**Symptoms:**
- Using `SyncMode::Off` or `SyncMode::Checkpoint`
- Auto-checkpoint disabled

**Solutions:**
```rust
// Enable durability features
config.wal_sync_mode = SyncMode::Full;
config.checksum_enabled = true;
config.auto_checkpoint_interval_ms = Some(30000);
```

## Next Steps

- Read the [Operations Guide](operations.md) for production deployment
- Check the [Architecture Documentation](architecture.md) for internal details
- Browse [examples](../examples/) for configuration patterns
- Review [Performance Metrics](performance_metrics.md) for monitoring