# Configuration Guide

Sombra provides flexible configuration options to optimize performance for different use cases. This guide covers all configuration options and their tradeoffs.

## Configuration Overview

### Rust

```rust
use sombra::{Config, GraphDB};

// Create custom configuration
let config = Config::builder()
    .cache_size(1024 * 1024 * 1024)  // 1GB cache
    .wal_enabled(true)
    .auto_checkpoint_interval(30000)  // 30 seconds
    .max_transaction_pages(10000)
    .build();

let db = GraphDB::open_with_config("my_graph.db", config)?;
```

### Python

```python
import sombra

# Create custom configuration
config = sombra.Config.builder() \
    .cache_size(1024 * 1024 * 1024)  # 1GB cache \
    .wal_enabled(True) \
    .auto_checkpoint_interval(30000)  # 30 seconds \
    .max_transaction_pages(10000) \
    .build()

db = sombra.GraphDB("my_graph.db", config)
```

### Node.js

```typescript
import { Config, GraphDB } from 'sombra';

// Create custom configuration
const config = Config.builder()
    .cacheSize(1024 * 1024 * 1024)  // 1GB cache
    .walEnabled(true)
    .autoCheckpointInterval(30000)  // 30 seconds
    .maxTransactionPages(10000)
    .build();

const db = new GraphDB('my_graph.db', config);
```

## Configuration Options

### Core Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `cache_size` | `usize` | 64MB | Page cache size in bytes |
| `wal_enabled` | `bool` | `true` | Enable Write-Ahead Logging |
| `auto_checkpoint_interval` | `Option<u64>` | `Some(30000)` | Auto-checkpoint interval in milliseconds |
| `max_transaction_pages` | `usize` | 10000 | Maximum pages per transaction |

### Performance Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `group_commit_enabled` | `bool` | `false` | Enable group commit for higher throughput |
| `group_commit_interval_ms` | `u64` | 10 | Group commit timeout in milliseconds |
| `group_commit_max_transactions` | `usize` | 100 | Max transactions per group commit |
| `read_ahead_enabled` | `bool` | `true` | Enable read-ahead optimization |
| `read_ahead_pages` | `usize` | 4 | Number of pages to read ahead |

### Safety Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_database_size_mb` | `Option<u64>` | `None` | Maximum database size in MB |
| `max_wal_size_mb` | `u64` | 100 | Maximum WAL size in MB |
| `transaction_timeout_ms` | `Option<u64>` | `None` | Transaction timeout in milliseconds |
| `fsync_enabled` | `bool` | `true` | Force fsync on commit |

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
// Calculate optimal cache size (70-80% of available memory)
let total_memory = get_available_memory(); // Implementation depends on OS
let cache_size = (total_memory as f64 * 0.75) as usize;

let config = Config::builder()
    .cache_size(cache_size)
    .build();
```

**Guidelines:**
- **Embedded applications**: Use 50-70% of available memory
- **Server applications**: Use 70-80% of available memory
- **Memory-constrained**: Use 20-30% of available memory

#### Transaction Memory

Limit transaction memory usage to prevent OOM:

```rust
let config = Config::builder()
    .max_transaction_pages(max_pages_for_workload)
    .build();

// Rough calculation: 1 page = 4KB
// For 100MB transaction limit: 100 * 1024 * 1024 / 4096 = 25600 pages
```

### Disk I/O Optimization

#### WAL Configuration

```rust
// High durability
let config = Config::builder()
    .wal_enabled(true)
    .fsync_enabled(true)
    .auto_checkpoint_interval(Some(30000))  // 30 seconds
    .max_wal_size_mb(100)
    .build();

// High performance
let config = Config::builder()
    .wal_enabled(true)
    .fsync_enabled(false)  // Risk: data loss on crash
    .auto_checkpoint_interval(Some(300000))  // 5 minutes
    .max_wal_size_mb(1000)
    .build();
```

#### Group Commit

Enable group commit for high-throughput workloads:

```rust
let config = Config::builder()
    .group_commit_enabled(true)
    .group_commit_interval_ms(5)   // Shorter = lower latency
    .group_commit_max_transactions(50)  // Batch size
    .build();
```

**Tradeoffs:**
- **Shorter interval**: Lower latency, less batching
- **Longer interval**: Higher throughput, more latency
- **Larger batch**: Higher throughput, more memory usage

### Concurrency Optimization

#### Read-Heavy Workloads

```rust
let config = Config::builder()
    .cache_size(large_cache)  // Maximize cache
    .read_ahead_enabled(true)
    .read_ahead_pages(8)      // Aggressive read-ahead
    .group_commit_enabled(false)  // Disable for consistency
    .build();
```

#### Write-Heavy Workloads

```rust
let config = Config::builder()
    .group_commit_enabled(true)
    .group_commit_interval_ms(10)
    .group_commit_max_transactions(100)
    .wal_enabled(true)
    .fsync_enabled(false)  // If durability allows
    .build();
```

#### Mixed Workloads

```rust
let config = Config::builder()
    .cache_size(moderate_cache)
    .group_commit_enabled(true)
    .group_commit_interval_ms(5)   // Balanced latency
    .read_ahead_enabled(true)
    .auto_checkpoint_interval(Some(60000))  // 1 minute
    .build();
```

## Environment-Specific Tuning

### Development

```rust
let config = Config::builder()
    .cache_size(64 * 1024 * 1024)  // 64MB - conservative
    .wal_enabled(true)
    .fsync_enabled(true)          // Ensure data integrity
    .auto_checkpoint_interval(Some(10000))  // 10 seconds - frequent
    .max_transaction_pages(1000)   // Small transactions
    .build();
```

### Testing

```rust
let config = Config::builder()
    .cache_size(32 * 1024 * 1024)  // 32MB - minimal
    .wal_enabled(false)            // Faster tests
    .auto_checkpoint_interval(None) // No auto-checkpoint
    .max_transaction_pages(500)    // Very small transactions
    .build();
```

### Production

```rust
let config = Config::builder()
    .cache_size(calculate_production_cache())
    .wal_enabled(true)
    .fsync_enabled(true)
    .auto_checkpoint_interval(Some(30000))  // 30 seconds
    .max_transaction_pages(10000)
    .max_database_size_mb(Some(1024 * 1024))  // 1TB limit
    .max_wal_size_mb(500)
    .transaction_timeout_ms(Some(300000))     // 5 minutes
    .build();
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
config.set_cache_size(new_cache_size);
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
let cache_usage = config.cache_size;

if memory_usage > cache_usage * 2 {
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

// Test with power loss
test_with_power_loss(&config);
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
config.set_cache_size(config.cache_size / 2);

// Reduce transaction limits
config.set_max_transaction_pages(config.max_transaction_pages / 2);
```

### Poor Performance

**Symptoms:**
- Slow queries
- Low throughput

**Solutions:**
```rust
// Increase cache size
config.set_cache_size(config.cache_size * 2);

// Enable group commit
config.set_group_commit_enabled(true);

// Enable read-ahead
config.set_read_ahead_enabled(true);
```

### Data Loss Risk

**Symptoms:**
- fsync disabled
- WAL disabled

**Solutions:**
```rust
// Enable durability features
config.set_fsync_enabled(true);
config.set_wal_enabled(true);
config.set_auto_checkpoint_interval(Some(30000));
```

## Next Steps

- Read the [Operations Guide](operations.md) for production deployment
- Check the [Architecture Documentation](architecture.md) for internal details
- Browse [examples](../examples/) for configuration patterns
- Review [Performance Metrics](performance_metrics.md) for monitoring