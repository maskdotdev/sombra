# Sombra Production Deployment Guide

This guide provides comprehensive instructions for deploying Sombra v0.2.0 in production environments.

## Table of Contents

1. [Hardware Requirements](#hardware-requirements)
2. [Operating System Configuration](#operating-system-configuration)
3. [Filesystem Recommendations](#filesystem-recommendations)
4. [Application Configuration](#application-configuration)
5. [Monitoring and Observability](#monitoring-and-observability)
6. [Backup and Recovery](#backup-and-recovery)
7. [High Availability Patterns](#high-availability-patterns)
8. [Security Considerations](#security-considerations)
9. [Performance Tuning](#performance-tuning)
10. [Troubleshooting](#troubleshooting)

## Hardware Requirements

### Minimum Requirements

**Small Deployments** (< 1M nodes, < 10M edges):
- CPU: 2 cores
- RAM: 512MB + (cache_size × 8KB)
- Disk: 10GB SSD
- I/O: 100 IOPS

**Medium Deployments** (< 10M nodes, < 100M edges):
- CPU: 4 cores
- RAM: 2GB + (cache_size × 8KB)
- Disk: 100GB SSD
- I/O: 500 IOPS

**Large Deployments** (< 100M nodes, < 1B edges):
- CPU: 8+ cores
- RAM: 8GB + (cache_size × 8KB)
- Disk: 1TB NVMe SSD
- I/O: 5000+ IOPS

### Recommended Specifications

For production workloads, we recommend:

```
CPU: 4-8 cores (for parallel operations)
RAM: 4-16GB (generous cache sizing)
Disk: SSD or NVMe (NVMe 3-5x faster for WAL)
Network: 1Gbps+ (for monitoring/backup)
```

### Storage Sizing

**Formula:**
```
Total Storage = (Database Size × 1.5) + WAL Overhead + Backup Space

Where:
  Database Size = estimated graph size
  WAL Overhead = max_wal_size_mb × 2 (safety margin)
  Backup Space = Database Size × retention_days
```

**Example:**
```
Database: 100GB graph
WAL: 500MB × 2 = 1GB
Backups: 100GB × 7 days = 700GB
Total: 100GB + 1GB + 700GB = 801GB
```

## Operating System Configuration

### Linux (Recommended)

#### File Descriptor Limits

Sombra requires file descriptors for the database file, WAL, and monitoring:

```bash
# /etc/security/limits.conf
* soft nofile 65536
* hard nofile 65536

# Verify
ulimit -n
# Should show: 65536
```

#### Virtual Memory Tuning

```bash
# /etc/sysctl.conf

# Reduce swappiness (prefer RAM over swap)
vm.swappiness=10

# Increase dirty page writeback time for better batching
vm.dirty_writeback_centisecs=500
vm.dirty_expire_centisecs=3000

# Apply changes
sudo sysctl -p
```

#### I/O Scheduler

For SSDs, use `noop` or `deadline` scheduler:

```bash
# Check current scheduler
cat /sys/block/sda/queue/scheduler

# Set to deadline (better for databases)
echo deadline > /sys/block/sda/queue/scheduler

# Make persistent (add to /etc/rc.local or systemd)
```

For NVMe drives, use `none`:

```bash
echo none > /sys/block/nvme0n1/queue/scheduler
```

#### Transparent Huge Pages

Disable THP for predictable performance:

```bash
# Temporary
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag

# Permanent (add to /etc/rc.local)
```

### macOS

macOS generally has good default settings, but consider:

```bash
# Increase file descriptor limit
ulimit -n 65536

# Add to ~/.zshrc or ~/.bash_profile
echo "ulimit -n 65536" >> ~/.zshrc
```

### Windows

```powershell
# Run as Administrator

# Disable Windows Defender real-time scanning for database directory
Add-MpPreference -ExclusionPath "C:\path\to\sombra\data"

# Disable search indexing for database files
# Use GUI: File Explorer → Right-click folder → Properties → 
#         Uncheck "Allow files in this folder to have contents indexed"
```

## Filesystem Recommendations

### Linux Filesystems

**ext4** (Recommended for most deployments):
- Pros: Mature, stable, good performance
- Cons: Slightly slower than XFS for large files
- Configuration:
  ```bash
  # Mount with noatime for better performance
  mount -o noatime,nodiratime /dev/sda1 /data
  
  # Add to /etc/fstab:
  /dev/sda1  /data  ext4  noatime,nodiratime  0  2
  ```

**XFS** (Recommended for large databases):
- Pros: Better performance for large files, good parallelism
- Cons: Slightly more complex
- Configuration:
  ```bash
  # Format with larger allocation groups
  mkfs.xfs -d agcount=16 /dev/sda1
  
  # Mount with optimal options
  mount -o noatime,nodiratime,logbufs=8,logbsize=256k /dev/sda1 /data
  ```

**Btrfs** (Use with caution):
- Pros: Snapshots, compression
- Cons: Potential performance issues, less mature
- Not recommended for production Sombra deployments

### macOS Filesystems

**APFS** (Default, recommended):
- Good SSD performance
- Native macOS support
- No special configuration needed

### Windows Filesystems

**NTFS**:
- Disable 8.3 filename generation:
  ```powershell
  fsutil 8dot3name set 1
  ```
- Defragmentation: Not needed for SSDs

## Application Configuration

### Production Configuration Template

```rust
use sombra::db::config::Config;

let config = Config {
    // Core settings
    page_size: 8192,                    // 8KB pages (standard)
    cache_size: 5000,                   // 40MB cache (adjust based on RAM)
    enable_wal: true,                   // Always true in production
    
    // Resource limits
    max_database_size_mb: Some(50_000), // 50GB limit (prevent disk exhaustion)
    max_wal_size_mb: 500,               // 500MB WAL (auto-checkpoint trigger)
    max_transaction_pages: 10_000,      // Limit transaction size
    
    // Timeouts
    transaction_timeout_ms: Some(60_000), // 1 minute timeout
    auto_checkpoint_interval_ms: Some(30_000), // 30 second auto-checkpoint
    
    // Safety
    fsync_enabled: true,                // Full durability
    checksum_mode: true,                // Enable checksums (when available)
};

let db = GraphDB::open_with_config("/data/sombra/graph.db", config)?;
```

### Configuration by Workload

#### Read-Heavy Workload
```rust
Config {
    cache_size: 10_000,              // Large cache (80MB)
    max_wal_size_mb: 100,            // Small WAL (less writes)
    auto_checkpoint_interval_ms: Some(60_000), // Less frequent checkpoints
    ..Config::production()
}
```

#### Write-Heavy Workload
```rust
Config {
    cache_size: 2_000,               // Moderate cache (16MB)
    max_wal_size_mb: 1_000,          // Large WAL buffer
    auto_checkpoint_interval_ms: Some(10_000), // Frequent checkpoints
    max_transaction_pages: 50_000,   // Large transactions
    ..Config::production()
}
```

#### Balanced Workload
```rust
Config::production()  // Use defaults
```

### Environment-Specific Settings

```rust
// Development
let config = if cfg!(debug_assertions) {
    Config {
        cache_size: 100,
        enable_wal: false,  // Faster for tests
        ..Config::default()
    }
// Staging
} else if env::var("ENV").unwrap_or_default() == "staging" {
    Config {
        cache_size: 1000,
        max_wal_size_mb: 100,
        ..Config::production()
    }
// Production
} else {
    Config::production()
};
```

## Monitoring and Observability

### Structured Logging Setup

```rust
use sombra::logging::init_logging;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging based on environment
    let log_level = env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    init_logging(&log_level)?;
    
    // Your application code
    let db = GraphDB::open("production.db")?;
    
    Ok(())
}
```

**Log Levels in Production:**
- `error`: Always enable (critical failures)
- `warn`: Always enable (degradation, slow queries)
- `info`: Recommended (transactions, checkpoints) - < 2% overhead
- `debug`: Only for troubleshooting (3-5% overhead)
- `trace`: Never in production (10-15% overhead)

### Metrics Collection

```rust
use std::time::Duration;
use std::thread;

// Metrics reporting thread
thread::spawn(move || {
    loop {
        thread::sleep(Duration::from_secs(60));
        
        let metrics = db.metrics();
        
        // Export to monitoring system
        report_to_prometheus(&metrics);
        
        // Log summary
        info!(
            "Metrics: tx_commit={} tx_rollback={} cache_hit_rate={:.2}% wal_bytes={}",
            metrics.transactions_committed,
            metrics.transactions_rolled_back,
            metrics.cache_hit_rate() * 100.0,
            metrics.wal_bytes_written
        );
    }
});
```

### Prometheus Integration

```rust
// Export metrics in Prometheus format
fn report_to_prometheus(metrics: &PerformanceMetrics) {
    let prometheus_text = metrics.to_prometheus_format();
    
    // Write to file for node_exporter textfile collector
    std::fs::write("/var/lib/node_exporter/sombra.prom", prometheus_text)?;
    
    // Or expose via HTTP endpoint
    // prometheus_server.update_metrics(prometheus_text);
}
```

**Prometheus Queries:**
```promql
# Transaction rate
rate(sombra_transactions_committed_total[5m])

# Error rate
rate(sombra_corruption_errors_total[5m])

# Cache hit rate
sombra_cache_hits / (sombra_cache_hits + sombra_cache_misses) * 100

# P95 commit latency
histogram_quantile(0.95, sombra_commit_latency_seconds)
```

### Health Checks

```rust
use std::time::Duration;

// Health check endpoint (for load balancers, orchestrators)
fn health_check(db: &GraphDB) -> Result<HealthStatus> {
    let health = db.health_check();
    
    match health.status {
        HealthStatus::Healthy => {
            info!("Health check: HEALTHY");
            Ok(health.status)
        },
        HealthStatus::Degraded => {
            warn!("Health check: DEGRADED - {}", 
                  health.checks.iter()
                      .filter(|c| !c.healthy)
                      .map(|c| c.description())
                      .collect::<Vec<_>>()
                      .join(", "));
            Ok(health.status)
        },
        HealthStatus::Unhealthy => {
            error!("Health check: UNHEALTHY - {}", 
                   health.checks.iter()
                       .filter(|c| !c.healthy)
                       .map(|c| c.description())
                       .collect::<Vec<_>>()
                       .join(", "));
            Err(GraphError::DatabaseUnhealthy)
        }
    }
}
```

### Alerting Rules

**Critical Alerts** (Page on-call):
- Database corruption detected
- Lock poisoning (requires restart)
- Disk space < 10% free
- Error rate > 1% of requests
- Service down for > 1 minute

**Warning Alerts** (Slack/email):
- Cache hit rate < 70%
- WAL size > 80% of max
- P95 latency > 10ms
- Transaction timeout rate > 0.1%
- Health check degraded

**Info Alerts** (Dashboard only):
- Checkpoint completed
- WAL rotated
- Cache size changed

## Backup and Recovery

### Backup Strategies

#### 1. Snapshot Backups (Recommended)

```bash
#!/bin/bash
# backup.sh - Daily database backup

DATE=$(date +%Y%m%d)
DB_PATH="/data/sombra/graph.db"
BACKUP_DIR="/backups/sombra"
RETENTION_DAYS=7

# Trigger checkpoint (flushes WAL)
/usr/local/bin/sombra-repair checkpoint "$DB_PATH"

# Copy database file
cp "$DB_PATH" "$BACKUP_DIR/graph-$DATE.db"

# Compress
gzip "$BACKUP_DIR/graph-$DATE.db"

# Remove old backups
find "$BACKUP_DIR" -name "graph-*.db.gz" -mtime +$RETENTION_DAYS -delete

echo "Backup completed: graph-$DATE.db.gz"
```

**Schedule with cron:**
```cron
# Daily at 2 AM
0 2 * * * /usr/local/bin/backup.sh >> /var/log/sombra-backup.log 2>&1
```

#### 2. Continuous Backup (WAL Archiving)

```bash
#!/bin/bash
# wal-archive.sh - Continuous WAL backup

WAL_DIR="/data/sombra"
ARCHIVE_DIR="/backups/sombra/wal-archive"

# Watch for WAL rotations and copy
inotifywait -m -e close_write "$WAL_DIR" |
while read -r directory events filename; do
    if [[ "$filename" == *"-wal" ]]; then
        cp "$directory/$filename" "$ARCHIVE_DIR/"
        echo "Archived: $filename"
    fi
done
```

#### 3. Cloud Backup (S3 Example)

```bash
#!/bin/bash
# s3-backup.sh - Upload backups to S3

BACKUP_FILE="/backups/sombra/graph-$(date +%Y%m%d).db.gz"
S3_BUCKET="s3://my-company-backups/sombra"

# Upload to S3
aws s3 cp "$BACKUP_FILE" "$S3_BUCKET/" --storage-class STANDARD_IA

# Verify
aws s3 ls "$S3_BUCKET/" | grep "$(date +%Y%m%d)"
```

### Recovery Procedures

#### 1. Point-in-Time Recovery (from snapshot)

```bash
# 1. Stop application
systemctl stop myapp

# 2. Restore database file
gunzip -c /backups/sombra/graph-20251020.db.gz > /data/sombra/graph.db

# 3. Remove WAL (will be rebuilt)
rm -f /data/sombra/graph.db-wal
rm -f /data/sombra/graph.db-shm

# 4. Verify integrity
sombra-inspect verify /data/sombra/graph.db

# 5. Start application
systemctl start myapp
```

#### 2. Recovery from Corruption

```bash
# 1. Assess damage
sombra-inspect verify /data/sombra/graph.db

# 2. Attempt repair
sombra-repair checkpoint /data/sombra/graph.db

# 3. If repair fails, restore from backup
# (see Point-in-Time Recovery above)

# 4. Report corruption details for analysis
sombra-inspect stats /data/sombra/graph.db > corruption-report.txt
```

#### 3. Disaster Recovery (total data loss)

```bash
# 1. Provision new infrastructure
# 2. Restore latest backup
# 3. Restore application state
# 4. Validate data integrity
# 5. Resume operations

# See: docs/disaster-recovery-runbook.md (create this for your org)
```

### Backup Testing

**Monthly Backup Validation:**
```bash
#!/bin/bash
# test-backup.sh - Validate backups are restorable

LATEST_BACKUP=$(ls -t /backups/sombra/graph-*.db.gz | head -1)
TEST_DIR="/tmp/sombra-backup-test"

mkdir -p "$TEST_DIR"
gunzip -c "$LATEST_BACKUP" > "$TEST_DIR/graph.db"

# Verify
sombra-inspect verify "$TEST_DIR/graph.db"
RESULT=$?

rm -rf "$TEST_DIR"

if [ $RESULT -eq 0 ]; then
    echo "✓ Backup valid: $LATEST_BACKUP"
    exit 0
else
    echo "✗ Backup corrupted: $LATEST_BACKUP"
    exit 1
fi
```

## High Availability Patterns

### 1. Active-Passive Failover

```
┌─────────────┐         ┌─────────────┐
│   Primary   │◄────────│  Secondary  │
│   (Active)  │ Backup  │  (Standby)  │
└─────────────┘         └─────────────┘
       │                       │
       ▼                       ▼
   Clients              (ready to serve)
```

**Implementation:**
- Primary serves all traffic
- Secondary receives continuous backups (or replication in future version)
- On failure, promote secondary to primary
- DNS/load balancer failover

**Failover procedure:**
```bash
# Detect failure
if ! health_check primary; then
    # Promote secondary
    systemctl stop myapp@primary
    systemctl start myapp@secondary
    
    # Update DNS
    update-dns-record myapp.example.com -> secondary-ip
    
    # Alert ops team
    send-alert "Primary failed, secondary promoted"
fi
```

### 2. Read Replicas (Future v0.3.0+)

*Note: Not available in v0.2.0. Requires MVCC implementation.*

### 3. Application-Level Sharding

```rust
// Partition graph by entity type
fn get_shard(entity_id: u64) -> usize {
    (entity_id % NUM_SHARDS) as usize
}

let shards: Vec<GraphDB> = (0..NUM_SHARDS)
    .map(|i| GraphDB::open(format!("/data/shard-{}.db", i)))
    .collect::<Result<Vec<_>>>()?;

// Route operations to appropriate shard
let shard = get_shard(node_id);
shards[shard].get_node(node_id)?;
```

## Security Considerations

### File Permissions

```bash
# Database files should be owned by application user only
chown myapp:myapp /data/sombra/graph.db
chmod 600 /data/sombra/graph.db

# WAL files
chmod 600 /data/sombra/graph.db-wal
chmod 600 /data/sombra/graph.db-shm
```

### Encryption at Rest

Sombra does not provide built-in encryption. Use filesystem-level encryption:

**Linux (LUKS):**
```bash
# Encrypt partition
cryptsetup luksFormat /dev/sda1
cryptsetup open /dev/sda1 sombra-data

# Mount encrypted volume
mount /dev/mapper/sombra-data /data
```

**macOS (FileVault):**
```bash
# Enable FileVault via System Preferences → Security & Privacy
```

### Network Security

- **No network exposure**: Sombra is an embedded database (no network protocol)
- **Application-level auth**: Implement authentication in your application
- **API security**: Use HTTPS, API keys, OAuth, etc. for your application API

### Audit Logging

```rust
// Log all mutations for audit trail
fn add_node_with_audit(db: &mut GraphDB, node: Node, user: &str) -> Result<NodeId> {
    let node_id = db.add_node(node)?;
    
    audit_log!(
        user = user,
        action = "add_node",
        node_id = node_id,
        timestamp = Utc::now()
    );
    
    Ok(node_id)
}
```

## Performance Tuning

See [docs/performance.md](performance.md) for detailed performance analysis and tuning guide.

**Quick tips:**
- Cache size = (working set size / page_size)
- Batch operations in transactions (100-1000 ops/tx)
- Use property indexes for frequent queries
- Monitor cache hit rate (target: >80%)
- Checkpoint every 1000-10000 transactions

## Troubleshooting

### Common Issues

#### 1. High Memory Usage

**Symptoms:** Process memory grows over time

**Diagnosis:**
```rust
let metrics = db.metrics();
println!("Cache size: {}MB", (metrics.cache_size * 8192) / 1_000_000);
println!("Page evictions: {}", metrics.page_evictions);
```

**Solutions:**
- Reduce `cache_size` in config
- Check for memory leaks in application code
- Monitor with `valgrind` or `heaptrack`

#### 2. Slow Performance

**Symptoms:** High latency, low throughput

**Diagnosis:**
```rust
let metrics = db.metrics();
println!("Cache hit rate: {:.2}%", metrics.cache_hit_rate() * 100.0);
println!("P95 commit latency: {}ms", metrics.p95_commit_latency());
```

**Solutions:**
- Increase `cache_size` (if RAM available)
- Batch small operations into transactions
- Create property indexes for frequent queries
- Use SSD/NVMe storage

#### 3. Database Corruption

**Symptoms:** `GraphError::Corruption` errors

**Diagnosis:**
```bash
sombra-inspect verify /data/sombra/graph.db
```

**Solutions:**
- Restore from latest backup
- Check disk health (SMART errors)
- Ensure `fsync_enabled = true` in production
- Enable checksums (when available)

#### 4. Lock Poisoning

**Symptoms:** `GraphError::LockPoisoned` errors

**Cause:** Another thread panicked while holding a lock

**Solution:**
```bash
# Requires application restart
systemctl restart myapp
```

**Prevention:**
- Fix any panic paths in application code
- Use `std::panic::catch_unwind` for external code
- Monitor for panics in logs

### Getting Support

- **Documentation**: [https://docs.rs/sombra](https://docs.rs/sombra)
- **GitHub Issues**: [https://github.com/maskdotdev/sombra/issues](https://github.com/maskdotdev/sombra/issues)
- **Examples**: Check the `examples/` directory

## Production Readiness Checklist

Before deploying to production:

- [ ] Hardware meets minimum requirements
- [ ] OS tuning applied (file descriptors, vm.swappiness, I/O scheduler)
- [ ] Filesystem configured (noatime, proper scheduler)
- [ ] Production configuration reviewed (`Config::production()`)
- [ ] Structured logging enabled (INFO level)
- [ ] Metrics collection configured
- [ ] Health checks implemented
- [ ] Monitoring dashboards created
- [ ] Alerting rules configured (critical + warning)
- [ ] Backup strategy implemented and tested
- [ ] Backup restoration tested monthly
- [ ] Disaster recovery plan documented
- [ ] Security hardening applied (file permissions, encryption)
- [ ] Performance baseline established
- [ ] Capacity planning completed
- [ ] Runbooks created for common issues
- [ ] On-call rotation trained
- [ ] Graceful shutdown implemented (`db.close()`)

---

**Congratulations! You're ready to run Sombra in production.** 🚀

For questions or issues, please file a GitHub issue or check the documentation at [docs.rs/sombra](https://docs.rs/sombra).
