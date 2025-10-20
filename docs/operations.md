# Operations Guide

This guide covers operational aspects of running Sombra in production, including monitoring, backup procedures, disaster recovery, and troubleshooting.

## Monitoring

### Health Checks

Sombra provides built-in health monitoring to assess system status:

```rust
use sombra::{GraphDB, HealthStatus};

let db = GraphDB::open("production.db")?;
let health = db.health_check()?;

match health.status {
    HealthStatus::Healthy => println!("System operating normally"),
    HealthStatus::Degraded => println!("Performance degraded, investigate"),
    HealthStatus::Unhealthy => println!("System needs immediate attention"),
}

// Check individual health indicators
for check in health.checks {
    println!("{}: {} - {}", check.name, check.healthy, check.message);
}
```

### Performance Metrics

Monitor key performance indicators:

```rust
let metrics = db.get_performance_metrics();

// Throughput metrics
println!("Transactions/sec: {}", metrics.transactions_per_second);
println!("Reads/sec: {}", metrics.reads_per_second);
println!("Writes/sec: {}", metrics.writes_per_second);

// Latency metrics
println!("P50 commit latency: {}ms", metrics.p50_commit_latency());
println!("P95 commit latency: {}ms", metrics.p95_commit_latency());
println!("P99 commit latency: {}ms", metrics.p99_commit_latency());

// Resource metrics
println!("Cache hit rate: {:.2}%", metrics.cache_hit_rate * 100.0);
println!("WAL size: {}MB", metrics.wal_size_bytes / 1024 / 1024);
println!("Dirty pages: {}", metrics.dirty_pages);
```

### Metrics Export

Export metrics to monitoring systems:

```rust
// Prometheus format
let prometheus_metrics = metrics.to_prometheus_format();
println!("{}", prometheus_metrics);

// JSON format
let json_metrics = metrics.to_json();
println!("{}", json_metrics);

// StatsD format
let statsd_metrics = metrics.to_statsd();
println!("{}", statsd_metrics);
```

### Structured Logging

Configure structured logging for production monitoring:

```rust
use sombra::logging;

// Initialize logging with JSON output
logging::init_logging("info,json")?;

// Logs will include:
// - Transaction lifecycle events
// - Performance warnings (>100ms operations)
// - Error conditions and corruption detection
// - Resource usage alerts
```

## Backup and Restore

### Online Backup

Create consistent backups without downtime:

```rust
use sombra::backup::BackupManager;

let db = GraphDB::open("production.db")?;
let backup_manager = BackupManager::new(&db);

// Create incremental backup
backup_manager.create_incremental_backup("backups/inc_20240115_120000")?;

// Create full backup
backup_manager.create_full_backup("backups/full_20240115_120000")?;

// List available backups
let backups = backup_manager.list_backups()?;
for backup in backups {
    println!("{}: {} ({})", backup.name, backup.type_, backup.size);
}
```

### Point-in-Time Recovery

Restore to a specific point in time:

```rust
// Restore from backup
backup_manager.restore_from_backup(
    "backups/full_20240115_120000",
    "restored.db"
)?;

// Apply WAL replay up to specific time
backup_manager.replay_wal_until(
    "restored.db",
    chrono::DateTime::parse_from_rfc3339("2024-01-15T12:30:00Z")?
)?;
```

### Automated Backup Script

Set up automated backups:

```bash
#!/bin/bash
# backup.sh - Daily backup script

BACKUP_DIR="/backups/sombra"
DB_PATH="/data/production.db"
DATE=$(date +%Y%m%d_%H%M%S)

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Create full backup on Sunday, incremental otherwise
if [ $(date +%u) -eq 7 ]; then
    sombra-backup --type full --input "$DB_PATH" --output "$BACKUP_DIR/full_$DATE"
else
    sombra-backup --type incremental --input "$DB_PATH" --output "$BACKUP_DIR/inc_$DATE"
fi

# Clean up old backups (keep 30 days)
find "$BACKUP_DIR" -name "*.backup" -mtime +30 -delete

# Verify backup integrity
sombra-verify --backup "$BACKUP_DIR/inc_$DATE" || {
    echo "Backup verification failed!"
    exit 1
}
```

## Disaster Recovery

### WAL Recovery

Automatic WAL recovery on database startup:

```rust
// This happens automatically on GraphDB::open()
let db = GraphDB::open("production.db")?;

// Manual WAL recovery if needed
use sombra::recovery::WALRecovery;

let recovery = WALRecovery::new("production.db");
let report = recovery.recover()?;

println!("Recovered {} transactions", report.transactions_recovered);
println!("Applied {} WAL frames", report.frames_applied);
if !report.corrupted_frames.is_empty() {
    println!("Found {} corrupted frames", report.corrupted_frames.len());
}
```

### Database Repair

Repair corrupted databases:

```rust
use sombra::repair::DatabaseRepair;

let repair = DatabaseRepair::new("corrupted.db");

// Verify integrity
let report = repair.verify_integrity()?;
if report.is_healthy() {
    println!("Database is healthy");
} else {
    println!("Found {} issues", report.issues.len());
    
    // Attempt repair
    let repair_report = repair.repair()?;
    println!("Repaired {} issues", repair_report.issues_fixed);
}
```

### Salvage Data from Corrupted Database

Extract readable data when repair fails:

```rust
let salvage = DatabaseRepair::new("corrupted.db");

// Salvage nodes
let nodes = salvage.salvage_nodes()?;
println!("Salvaged {} nodes", nodes.len());

// Salvage edges
let edges = salvage.salvage_edges()?;
println!("Salvaged {} edges", edges.len());

// Export to new database
let new_db = GraphDB::create("salvaged.db")?;
let mut tx = new_db.begin_transaction()?;

for node in nodes {
    tx.create_node(&node.label, node.properties)?;
}

for edge in edges {
    tx.create_edge(edge.from_id, edge.to_id, &edge.label, edge.properties)?;
}

tx.commit()?;
```

## Database Maintenance

### Checkpoint Management

Manage WAL checkpoints for optimal performance:

```rust
// Manual checkpoint
db.checkpoint()?;

// Checkpoint with options
use sombra::CheckpointMode;

db.checkpoint_with_mode(CheckpointMode::Full)?;     // Force full checkpoint
db.checkpoint_with_mode(CheckpointMode::Restart)?;   // Restart log sequence
db.checkpoint_with_mode(CheckpointMode::Truncate)?;  // Truncate WAL file

// Checkpoint status
let status = db.checkpoint_status()?;
println!("WAL size: {}MB", status.wal_size_bytes / 1024 / 1024);
println!("Checkpoint pending: {}", status.checkpoint_pending);
```

### Index Maintenance

Rebuild indexes for optimal query performance:

```rust
use sombra::index::IndexManager;

let index_manager = IndexManager::new(&db);

// Rebuild all indexes
index_manager.rebuild_all_indexes()?;

// Rebuild specific index
index_manager.rebuild_label_index("User")?;
index_manager.rebuild_property_index("email")?;

// Analyze index statistics
let stats = index_manager.get_index_statistics()?;
for stat in stats {
    println!("Index {}: {} entries, {:.2}% selectivity", 
        stat.name, stat.entries, stat.selectivity * 100.0);
}
```

### Database Compaction

Reduce database file size and improve performance:

```rust
use sombra::maintenance::DatabaseCompactor;

let compactor = DatabaseCompactor::new(&db);

// Analyze fragmentation
let analysis = compactor.analyze_fragmentation()?;
println!("Fragmentation: {:.2}%", analysis.fragmentation_percentage);
println!("Free pages: {}", analysis.free_pages);
println!("Reclaimable space: {}MB", analysis.reclaimable_bytes / 1024 / 1024);

// Compact database
if analysis.fragmentation_percentage > 20.0 {
    println!("Starting compaction...");
    let report = compactor.compact()?;
    println!("Compaction completed:");
    println!("  Pages moved: {}", report.pages_moved);
    println!("  Space reclaimed: {}MB", report.bytes_reclaimed / 1024 / 1024);
    println!("  Time taken: {}ms", report.duration_ms);
}
```

## Performance Tuning in Production

### Real-time Performance Adjustment

Adjust configuration based on current load:

```rust
use sombra::adaptive::AdaptiveConfig;

let adaptive_config = AdaptiveConfig::new(&db);

// Enable adaptive tuning
adaptive_config.enable_adaptive_tuning()?;

// Monitor and adjust automatically
adaptive_config.set_monitoring_interval(60000); // 1 minute

// Manual adjustment based on metrics
let metrics = db.get_performance_metrics();

if metrics.cache_hit_rate < 0.8 {
    // Increase cache size
    let new_cache_size = (db.config().cache_size as f64 * 1.2) as usize;
    db.update_cache_size(new_cache_size)?;
}

if metrics.p99_commit_latency() > 100 {
    // Enable group commit
    db.enable_group_commit(5, 50)?; // 5ms interval, 50 max tx
}
```

### Load Balancing

For multi-instance deployments:

```rust
use sombra::cluster::LoadBalancer;

let load_balancer = LoadBalancer::new(vec![
    "db1.example.com:8080",
    "db2.example.com:8080", 
    "db3.example.com:8080"
]);

// Route read requests
let db = load_balancer.get_read_connection()?;

// Route write requests to primary
let primary = load_balancer.get_write_connection()?;

// Monitor instance health
load_balancer.health_check_all()?;
```

## Troubleshooting

### Common Issues

#### High Memory Usage

**Symptoms:**
- Process using more memory than configured
- OOM errors

**Diagnosis:**
```rust
let metrics = db.get_performance_metrics();
println!("Cache size: {}MB", metrics.cache_size_bytes / 1024 / 1024);
println!("Dirty pages: {}", metrics.dirty_pages);
println!("Memory pressure: {}", metrics.memory_pressure);
```

**Solutions:**
```rust
// Reduce cache size
db.update_cache_size(db.config().cache_size / 2)?;

// Force checkpoint to clear dirty pages
db.checkpoint_with_mode(CheckpointMode::Full)?;

// Enable memory pressure monitoring
db.enable_memory_pressure_monitoring(0.8)?; // Alert at 80%
```

#### Slow Performance

**Symptoms:**
- High query latency
- Low throughput

**Diagnosis:**
```rust
let metrics = db.get_performance_metrics();
println!("Cache hit rate: {:.2}%", metrics.cache_hit_rate * 100.0);
println!("WAL size: {}MB", metrics.wal_size_bytes / 1024 / 1024);
println!("Dirty pages: {}", metrics.dirty_pages);
```

**Solutions:**
```rust
// Increase cache size if hit rate is low
if metrics.cache_hit_rate < 0.9 {
    db.update_cache_size(db.config().cache_size * 2)?;
}

// Checkpoint if WAL is large
if metrics.wal_size_bytes > 100 * 1024 * 1024 { // 100MB
    db.checkpoint()?;
}

// Enable group commit for write-heavy workloads
db.enable_group_commit(10, 100)?;
```

#### Database Corruption

**Symptoms:**
- Corruption errors
- Crashes on read/write

**Diagnosis:**
```rust
use sombra::integrity::IntegrityChecker;

let checker = IntegrityChecker::new("production.db");
let report = checker.verify()?;

if !report.is_healthy() {
    println!("Corruption detected:");
    for issue in &report.issues {
        println!("  {}: {}", issue.severity, issue.description);
    }
}
```

**Solutions:**
```rust
// Attempt repair
let repair = DatabaseRepair::new("production.db");
let repair_report = repair.repair()?;

if repair_report.success {
    println!("Repair successful");
} else {
    println!("Repair failed, attempting salvage");
    
    // Salvage data to new database
    let salvage = DatabaseRepair::new("production.db");
    let nodes = salvage.salvage_nodes()?;
    let edges = salvage.salvage_edges()?;
    
    // Create new database with salvaged data
    create_new_database_from_salvage("recovered.db", nodes, edges)?;
}
```

### Debug Mode

Enable detailed debugging for troubleshooting:

```rust
use sombra::debug::DebugMode;

// Enable debug mode
let debug = DebugMode::enable(&db)?;

// Trace specific operations
debug.trace_operations(vec!["transaction_commit", "page_read"])?;

// Capture detailed logs
debug.enable_detailed_logging("debug.log")?;

// Performance profiling
debug.start_profiling()?;
// ... run operations ...
let profile = debug.stop_profiling()?;
println!("Profile: {}", profile.summary());
```

## Security Operations

### Access Control

Implement database-level access control:

```rust
use sombra::security::{AccessControl, Permission, Role};

let acl = AccessControl::new();

// Define roles
acl.add_role("admin", vec![
    Permission::Read,
    Permission::Write,
    Permission::Delete,
    Permission::Backup,
    Permission::Configure
])?;

acl.add_role("user", vec![
    Permission::Read,
    Permission::Write
])?;

acl.add_role("readonly", vec![
    Permission::Read
])?;

// Assign roles to users
acl.assign_role("alice", "admin")?;
acl.assign_role("bob", "user")?;
acl.assign_role("charlie", "readonly")?;

// Enforce access control
db.set_access_control(acl)?;
```

### Audit Logging

Track all database operations:

```rust
use sombra::audit::AuditLogger;

let audit = AuditLogger::new("audit.log");

// Log all operations
audit.log_all_operations()?;

// Log specific operations
audit.log_operations(vec![
    "node_create",
    "edge_create", 
    "node_delete",
    "transaction_commit"
])?;

// Query audit log
let operations = audit.query_operations(
    chrono::Utc::now() - chrono::Duration::hours(24),
    chrono::Utc::now(),
    Some("alice")
)?;
```

### Encryption

Enable data-at-rest encryption:

```rust
use sombra::encryption::EncryptionConfig;

let encryption_config = EncryptionConfig::new()
    .with_key_from_env("SOMBRA_ENCRYPTION_KEY")?
    .with_algorithm("AES-256-GCM")?;

let config = Config::builder()
    .encryption(encryption_config)
    .build();

let db = GraphDB::open_with_config("encrypted.db", config)?;
```

## Automation and Scripting

### Maintenance Automation

Automate routine maintenance tasks:

```bash
#!/bin/bash
# maintenance.sh - Daily maintenance script

DB_PATH="/data/production.db"
BACKUP_DIR="/backups"
LOG_FILE="/var/log/sombra-maintenance.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Health check
log "Starting health check..."
if ! sombra-health-check --db "$DB_PATH"; then
    log "Health check failed!"
    exit 1
fi

# Backup
log "Creating backup..."
sombra-backup --type incremental --input "$DB_PATH" --output "$BACKUP_DIR/inc_$(date +%Y%m%d_%H%M%S)"

# Checkpoint if WAL is large
WAL_SIZE=$(sombra-info --db "$DB_PATH" --wal-size)
if [ "$WAL_SIZE" -gt 104857600 ]; then  # 100MB
    log "WAL size is ${WAL_SIZE} bytes, checkpointing..."
    sombra-checkpoint --db "$DB_PATH" --mode full
fi

# Compact if fragmentation is high
FRAGMENTATION=$(sombra-info --db "$DB_PATH" --fragmentation)
if (( $(echo "$FRAGMENTATION > 20.0" | bc -l) )); then
    log "Fragmentation is ${FRAGMENTATION}%, compacting..."
    sombra-compact --db "$DB_PATH"
fi

log "Maintenance completed successfully"
```

### Monitoring Integration

Integrate with monitoring systems:

```python
# monitoring.py - Prometheus metrics exporter
import time
import sombra
from prometheus_client import start_http_server, Gauge, Counter

# Metrics
cache_hit_rate = Gauge('sombra_cache_hit_rate', 'Cache hit rate')
commit_latency = Gauge('sombra_commit_latency_ms', 'Commit latency')
transactions_total = Counter('sombra_transactions_total', 'Total transactions')

def update_metrics():
    db = sombra.GraphDB('/data/production.db')
    metrics = db.get_performance_metrics()
    
    cache_hit_rate.set(metrics.cache_hit_rate)
    commit_latency.set(metrics.p99_commit_latency())
    transactions_total.inc(metrics.transactions_committed)

def main():
    start_http_server(8080)
    
    while True:
        update_metrics()
        time.sleep(60)

if __name__ == '__main__':
    main()
```

## Next Steps

- Read the [Production Deployment Guide](production.md) for deployment specifics
- Check the [Security Guide](security.md) for security best practices
- Review the [Performance Metrics](performance_metrics.md) for detailed monitoring
- Browse the [examples](../examples/) for operational patterns