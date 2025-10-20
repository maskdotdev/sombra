# Migration Guide: Sombra 0.1.x → 0.2.0

This guide helps you migrate your application from Sombra 0.1.x to 0.2.0.

## Overview

Version 0.2.0 is a **production-hardening release** focused on reliability, observability, and operational safety. The main changes are:

1. **Improved error handling** - Many panic paths replaced with proper `Result` returns
2. **Enhanced configuration** - New resource limits and safety options
3. **Structured logging** - Comprehensive tracing support
4. **Graceful shutdown** - New `close()` method for clean database closure
5. **Enhanced metrics** - Extended performance monitoring

## Breaking Changes

### 1. Error Handling Changes

Many FFI functions that previously panicked on lock poisoning now return `GraphError::LockPoisoned`.

**Before (0.1.x):**
```rust
use sombra::prelude::*;

// Could panic on poisoned lock
let db = GraphDB::open("my_graph.db").unwrap();
let node = db.get_node(42).unwrap();
```

**After (0.2.0):**
```rust
use sombra::prelude::*;

// Returns proper errors
let db = GraphDB::open("my_graph.db")?;
let node = db.get_node(42)?;  // Can return LockPoisoned error
```

**Migration Strategy:**
- Replace all `.unwrap()` calls with `?` operator or explicit error handling
- Add `GraphError::LockPoisoned` to your error handling patterns
- Use `Result` return types in your application code

### 2. Configuration Structure Changes

New required fields added to `Config`:

**Before (0.1.x):**
```rust
use sombra::db::config::Config;

let config = Config {
    page_size: 8192,
    cache_size: 1000,
    enable_wal: true,
};
```

**After (0.2.0):**
```rust
use sombra::db::config::Config;

let config = Config {
    page_size: 8192,
    cache_size: 1000,
    enable_wal: true,
    // New required fields:
    max_wal_size_mb: 100,
    max_transaction_pages: 10000,
    // New optional fields (use None for unlimited):
    max_database_size_mb: None,
    transaction_timeout_ms: None,
    auto_checkpoint_interval_ms: Some(30000),
};
```

**Migration Strategy:**
- Use `Config::default()` for sensible defaults
- Or use production-ready presets: `Config::production()`
- Customize only what you need

**Recommended approach:**
```rust
let mut config = Config::production();
config.cache_size = 5000;  // Customize as needed
let db = GraphDB::open_with_config("my_graph.db", config)?;
```

### 3. Deserialization Error Handling

Corrupted data now returns `GraphError::Corruption` instead of panicking.

**Before (0.1.x):**
```rust
// Could panic on corrupted data
let node = db.get_node(42).unwrap();
```

**After (0.2.0):**
```rust
// Gracefully handles corruption
match db.get_node(42) {
    Ok(node) => { /* Use node */ },
    Err(GraphError::Corruption { context }) => {
        eprintln!("Database corruption detected: {}", context);
        // Run repair tool or restore from backup
    },
    Err(e) => { /* Handle other errors */ }
}
```

## New Features to Adopt

### 1. Structured Logging

Version 0.2.0 adds comprehensive structured logging with the `tracing` crate.

**Enable logging in your application:**
```rust
use sombra::logging::init_logging;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging (call once at startup)
    init_logging("info")?;
    
    // Your application code
    let db = GraphDB::open("my_graph.db")?;
    // All database operations now logged
    
    Ok(())
}
```

**Log levels:**
- `ERROR` - Corruption, lock failures, critical errors
- `WARN` - Lock contention, large WAL, slow operations (>100ms)
- `INFO` - Transaction commits, checkpoints, database open/close
- `DEBUG` - Transaction begin/rollback, cache operations
- `TRACE` - High-frequency operations (sampled)

**Environment variable control:**
```bash
# Set log level via environment
RUST_LOG=sombra=debug cargo run

# JSON output for log aggregation
RUST_LOG=sombra=info,sombra::db=debug cargo run
```

### 2. Enhanced Metrics

Use the new extended metrics for monitoring:

```rust
use sombra::prelude::*;

let db = GraphDB::open("my_graph.db")?;

// Perform operations...

let metrics = db.metrics();
println!("Transactions committed: {}", metrics.transactions_committed);
println!("Transactions rolled back: {}", metrics.transactions_rolled_back);
println!("WAL syncs: {}", metrics.wal_syncs);
println!("WAL bytes: {}", metrics.wal_bytes_written);
println!("Checkpoints: {}", metrics.checkpoints_performed);
println!("Page evictions: {}", metrics.page_evictions);
println!("Corruption errors: {}", metrics.corruption_errors);

// Latency percentiles
println!("P50 commit latency: {}ms", metrics.p50_commit_latency());
println!("P95 commit latency: {}ms", metrics.p95_commit_latency());
println!("P99 commit latency: {}ms", metrics.p99_commit_latency());
```

**Export to monitoring systems:**
```rust
// Prometheus format
let prom = metrics.to_prometheus_format();

// JSON format
let json = metrics.to_json()?;

// StatsD format
let statsd = metrics.to_statsd();
```

### 3. Health Checks

Monitor database health programmatically:

```rust
use sombra::prelude::*;

let db = GraphDB::open("my_graph.db")?;

let health = db.health_check();
match health.status {
    HealthStatus::Healthy => println!("✓ Database healthy"),
    HealthStatus::Degraded => {
        println!("⚠ Database degraded:");
        for check in health.checks {
            if !check.healthy {
                println!("  - {}", check.description());
            }
        }
    },
    HealthStatus::Unhealthy => {
        eprintln!("✗ Database unhealthy:");
        for check in health.checks {
            eprintln!("  - {}", check.description());
        }
    }
}
```

### 4. Graceful Shutdown

Always close databases cleanly in production:

```rust
use sombra::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = GraphDB::open("my_graph.db")?;
    
    // Application logic...
    
    // Clean shutdown (flushes, checkpoints, truncates WAL)
    db.close()?;
    
    Ok(())
}
```

**With signal handlers:**
```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })?;
    
    let db = GraphDB::open("my_graph.db")?;
    
    while running.load(Ordering::SeqCst) {
        // Application logic
    }
    
    println!("Shutting down gracefully...");
    db.close()?;
    println!("Database closed cleanly");
    
    Ok(())
}
```

### 5. Resource Limits

Configure safety limits for production:

```rust
use sombra::db::config::Config;

let mut config = Config::default();

// Limit database size (prevents disk exhaustion)
config.max_database_size_mb = Some(10_000);  // 10GB limit

// Limit WAL size (triggers auto-checkpoint)
config.max_wal_size_mb = 100;

// Limit transaction size (prevents memory exhaustion)
config.max_transaction_pages = 10_000;

// Transaction timeout (prevents runaway transactions)
config.transaction_timeout_ms = Some(30_000);  // 30 seconds

// Auto-checkpoint interval
config.auto_checkpoint_interval_ms = Some(60_000);  // 1 minute

let db = GraphDB::open_with_config("my_graph.db", config)?;
```

## Developer Tooling

### Database Inspector

Use the new CLI tool to inspect databases:

```bash
# Build the tools
cargo build --release --bin sombra-inspect

# Inspect database
./target/release/sombra-inspect info my_graph.db

# Verify integrity
./target/release/sombra-inspect verify my_graph.db

# View statistics
./target/release/sombra-inspect stats my_graph.db

# Check WAL status
./target/release/sombra-inspect wal-info my_graph.db
```

### Database Repair Tool

Repair common issues:

```bash
# Build the repair tool
cargo build --release --bin sombra-repair

# Checkpoint WAL
./target/release/sombra-repair checkpoint my_graph.db

# Vacuum database (reclaim space)
./target/release/sombra-repair vacuum my_graph.db
```

## Python API Changes

The Python API maintains backward compatibility, but you can now handle lock poisoning:

**Before (0.1.x):**
```python
from sombra import SombraDB

db = SombraDB("my_graph.db")
node = db.get_node(42)  # Could crash on lock poisoning
```

**After (0.2.0):**
```python
from sombra import SombraDB, SombraError

db = SombraDB("my_graph.db")

try:
    node = db.get_node(42)
except SombraError as e:
    if "lock poisoned" in str(e).lower():
        print("Database lock poisoned, restart required")
    elif "corruption" in str(e).lower():
        print("Database corruption detected, restore from backup")
    else:
        raise
```

## Node.js API Changes

The Node.js API maintains backward compatibility, but errors are now more descriptive:

**Before (0.1.x):**
```typescript
import { SombraDB } from 'sombradb';

const db = new SombraDB('./my_graph.db');
const node = db.getNode(42);  // Could throw generic error
```

**After (0.2.0):**
```typescript
import { SombraDB } from 'sombradb';

const db = new SombraDB('./my_graph.db');

try {
  const node = db.getNode(42);
} catch (error) {
  if (error.message.includes('lock poisoned')) {
    console.error('Database lock poisoned, restart required');
  } else if (error.message.includes('corruption')) {
    console.error('Database corruption detected, restore from backup');
  } else {
    throw error;
  }
}
```

## Testing Your Migration

### 1. Run the Test Suite

```bash
# Test Rust code
cargo test

# Test Python bindings
pytest tests/python_integration.py

# Test Node.js bindings
npm test
```

### 2. Validate Performance

```bash
# Run benchmarks to ensure no regression
cargo bench --bench benchmark_main
cargo bench --bench read_benchmark
cargo bench --bench traversal_benchmark
```

### 3. Test Error Handling

Verify your application handles new error cases:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_lock_poisoning_handling() {
        // Test that your app gracefully handles lock poisoning
    }
    
    #[test]
    fn test_corruption_handling() {
        // Test that your app handles corruption errors
    }
    
    #[test]
    fn test_resource_limits() {
        // Test that resource limits work as expected
    }
}
```

## Rollback Plan

If you encounter issues with 0.2.0:

1. **Database files are backward compatible** - 0.1.x can read 0.2.0 databases
2. **Revert dependency versions:**
   ```toml
   [dependencies]
   sombra = "0.1.29"
   ```
3. **No data migration needed** - Database format unchanged

## Getting Help

- **Documentation**: [https://docs.rs/sombra](https://docs.rs/sombra)
- **GitHub Issues**: [https://github.com/maskdotdev/sombra/issues](https://github.com/maskdotdev/sombra/issues)
- **Examples**: Check the `examples/` directory for updated code samples

## Checklist

Before deploying 0.2.0 to production:

- [ ] Updated error handling to use `?` operator
- [ ] Configured resource limits in `Config`
- [ ] Added structured logging with `init_logging()`
- [ ] Implemented graceful shutdown with `db.close()`
- [ ] Added health check monitoring
- [ ] Tested error handling paths (lock poisoning, corruption)
- [ ] Ran full test suite
- [ ] Validated performance benchmarks
- [ ] Updated monitoring/alerting for new metrics
- [ ] Tested graceful shutdown and restart
- [ ] Documented operational procedures
- [ ] Prepared rollback plan

Welcome to Sombra 0.2.0 - Production Ready! 🚀
