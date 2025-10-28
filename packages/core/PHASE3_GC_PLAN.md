# Phase 3: Garbage Collection Implementation Plan

## Overview
Phase 3 implements automatic garbage collection (GC) for MVCC version chains to prevent unbounded growth of historical versions while preserving data needed by active transactions.

## Goals
1. Automatically identify and reclaim old versions no longer needed
2. Ensure GC never removes versions visible to active transactions
3. Provide configurable GC intervals and thresholds
4. Maintain version chain integrity during concurrent operations
5. Track GC metrics for monitoring and tuning

## Architecture

### GC Watermark Calculation
The GC watermark is the oldest timestamp that must be preserved:
```
gc_watermark = min(active_snapshot_timestamps)
```

Any version with `commit_ts < gc_watermark` is potentially reclaimable, **except**:
- The version must not be the only visible version of a record
- At least one version must remain for each live record

### GC Process Flow
```
1. Calculate GC watermark from TimestampOracle
2. Scan version chains in storage
3. For each chain:
   a. Identify versions older than watermark
   b. Preserve at least one version per record
   c. Mark old versions as reclaimable
4. Compact version chains (remove marked versions)
5. Update indexes to point to new chain heads
6. Reclaim freed pages
```

## Task 14: Implement GC Scanner

### File: `src/db/gc.rs` (new)

#### Components

**1. GarbageCollector struct**
```rust
pub struct GarbageCollector {
    /// Minimum number of versions to preserve per record
    min_versions_per_record: usize,
    
    /// Maximum versions to scan per GC cycle
    scan_batch_size: usize,
    
    /// GC statistics
    stats: GcStats,
}
```

**2. GcStats struct**
```rust
pub struct GcStats {
    /// Total versions scanned
    pub versions_scanned: u64,
    
    /// Versions marked for reclamation
    pub versions_reclaimable: u64,
    
    /// Versions actually reclaimed
    pub versions_reclaimed: u64,
    
    /// Pages freed
    pub pages_freed: u64,
    
    /// Last GC run timestamp
    pub last_gc_time: Option<Instant>,
    
    /// Last GC duration
    pub last_gc_duration: Option<Duration>,
}
```

**3. Core GC Functions**

```rust
impl GarbageCollector {
    /// Scan version chains and identify reclaimable versions
    pub fn scan_for_reclaimable_versions(
        &mut self,
        pager: &mut Pager,
        node_index: &BTreeMap<NodeId, RecordPointer>,
        gc_watermark: u64,
    ) -> Result<Vec<RecordPointer>> {
        // Returns list of version pointers that can be reclaimed
    }
    
    /// Check if a specific version is reclaimable
    fn is_version_reclaimable(
        &self,
        metadata: &VersionMetadata,
        gc_watermark: u64,
        is_only_version: bool,
    ) -> bool {
        // Version is reclaimable if:
        // 1. commit_ts < gc_watermark
        // 2. Not the only version of the record
        // 3. Not marked as the canonical version
    }
    
    /// Scan a single version chain
    fn scan_version_chain(
        &mut self,
        pager: &mut Pager,
        chain_head: RecordPointer,
        gc_watermark: u64,
    ) -> Result<Vec<RecordPointer>> {
        // Traverse chain, identify old versions
        // Ensure at least one version remains
    }
}
```

### Implementation Steps

1. **Create `src/db/gc.rs`**
   - Define `GarbageCollector` struct
   - Define `GcStats` struct
   - Implement version scanning logic

2. **Add GC method to GraphDB**
   - `pub fn run_gc(&mut self) -> Result<GcStats>`
   - Calculate watermark from `timestamp_oracle`
   - Call `GarbageCollector::scan_for_reclaimable_versions()`
   - Return statistics

3. **Version Chain Traversal**
   - Reuse `VersionChainReader` from `version_chain.rs`
   - Follow `prev_version` pointers
   - Count versions in chain
   - Identify candidates for removal

4. **Safety Checks**
   - Never remove the only version of a record
   - Never remove versions with `commit_ts >= gc_watermark`
   - Preserve at least `min_versions_per_record` versions

## Task 15: Add Background GC Thread

### File: `src/db/gc.rs` (extend)

#### Components

**1. Background GC State**
```rust
pub struct BackgroundGcState {
    /// Handle to GC thread
    thread_handle: Option<JoinHandle<()>>,
    
    /// Control channel
    control_tx: Sender<GcControlMessage>,
    
    /// Status channel
    status_rx: Receiver<GcStatus>,
    
    /// Whether GC is currently running
    is_running: Arc<AtomicBool>,
}

pub enum GcControlMessage {
    Start,
    Stop,
    Pause,
    Resume,
    RunOnce,
    Shutdown,
}

pub enum GcStatus {
    Idle,
    Running,
    Paused,
    Completed(GcStats),
    Error(String),
}
```

**2. Background GC Thread**
```rust
impl GraphDB {
    /// Start background GC thread
    pub fn start_background_gc(&mut self) -> Result<()> {
        // Spawn thread
        // Periodic GC based on config.gc_interval_secs
        // Listen for control messages
    }
    
    /// Stop background GC thread
    pub fn stop_background_gc(&mut self) -> Result<()> {
        // Send shutdown message
        // Wait for thread to finish
    }
    
    /// Pause background GC
    pub fn pause_gc(&mut self) -> Result<()> {
        // Send pause message
    }
    
    /// Resume background GC
    pub fn resume_gc(&mut self) -> Result<()> {
        // Send resume message
    }
}
```

**3. GC Thread Loop**
```rust
fn gc_thread_loop(
    db: Arc<Mutex<GraphDB>>,
    control_rx: Receiver<GcControlMessage>,
    status_tx: Sender<GcStatus>,
    interval: Duration,
) {
    loop {
        // Wait for interval or control message
        // Run GC cycle if not paused
        // Send status updates
        // Handle shutdown
    }
}
```

### Implementation Steps

1. **Add GC state to GraphDB**
   - `background_gc_state: Option<BackgroundGcState>`
   - Initialize in `open_with_config()` if `gc_interval_secs` is set

2. **Implement GC thread**
   - Spawn background thread in `start_background_gc()`
   - Periodic execution based on config
   - Control message handling

3. **Graceful Shutdown**
   - Drop handler for `GraphDB` to stop GC thread
   - Ensure thread completes current GC cycle before shutdown

4. **Thread Safety**
   - Use Arc<Mutex<GraphDB>> for shared access
   - Ensure GC doesn't block transactions
   - Consider read-write locks if needed

## Task 16: Implement Version Chain Compaction

### File: `src/db/gc.rs` (extend)

#### Components

**1. Version Chain Compaction**
```rust
impl GarbageCollector {
    /// Compact a version chain by removing old versions
    pub fn compact_version_chain(
        &mut self,
        pager: &mut Pager,
        chain_head: RecordPointer,
        versions_to_remove: &[RecordPointer],
    ) -> Result<RecordPointer> {
        // Traverse chain
        // Skip versions marked for removal
        // Update prev_version pointers
        // Return new chain head
    }
    
    /// Physically delete a version record
    fn delete_version(
        &mut self,
        pager: &mut Pager,
        pointer: RecordPointer,
    ) -> Result<()> {
        // Mark page as having free space
        // Add to free list
        // Track freed space
    }
    
    /// Update index to point to new chain head
    fn update_index_after_compaction(
        &mut self,
        node_index: &mut BTreeMap<NodeId, RecordPointer>,
        node_id: NodeId,
        new_head: RecordPointer,
    ) -> Result<()> {
        // Update node index entry
    }
}
```

**2. Integration with GraphDB**
```rust
impl GraphDB {
    /// Run full GC cycle including compaction
    pub fn run_gc_with_compaction(&mut self) -> Result<GcStats> {
        let gc_watermark = self.timestamp_oracle.gc_eligible_before();
        
        // Phase 1: Scan and identify reclaimable versions
        let reclaimable = self.gc.scan_for_reclaimable_versions(
            &mut self.pager,
            &self.node_index,
            gc_watermark,
        )?;
        
        // Phase 2: Compact version chains
        for (node_id, versions) in reclaimable {
            let new_head = self.gc.compact_version_chain(
                &mut self.pager,
                self.node_index[&node_id],
                &versions,
            )?;
            self.node_index.insert(node_id, new_head);
        }
        
        Ok(self.gc.stats.clone())
    }
}
```

### Implementation Steps

1. **Version Chain Rewriting**
   - Read old chain
   - Write new chain without old versions
   - Update pointers

2. **Index Updates**
   - Update node_index with new chain heads
   - Update label indexes if needed
   - Update property indexes if needed

3. **Page Reclamation**
   - Track freed pages
   - Add to free page list
   - Update header with free page count

4. **Transactional Safety**
   - Run GC within a transaction
   - Rollback on errors
   - Ensure atomicity

## Task 17: Test GC Correctness

### File: `tests/mvcc_gc.rs` (new)

#### Test Cases

**1. Basic GC Functionality**
```rust
#[test]
fn test_gc_removes_old_versions() {
    // Create node, update multiple times
    // Commit transactions
    // Run GC
    // Verify old versions removed
    // Verify latest version still accessible
}
```

**2. GC with Active Transactions**
```rust
#[test]
fn test_gc_preserves_active_snapshot_versions() {
    // Start long-running transaction (snapshot)
    // Create and update nodes
    // Run GC
    // Verify versions visible to snapshot not removed
    // Commit transaction
    // Run GC again
    // Verify versions now removed
}
```

**3. GC Never Removes All Versions**
```rust
#[test]
fn test_gc_preserves_at_least_one_version() {
    // Create node with single version
    // Version is old but still only version
    // Run GC
    // Verify version NOT removed
}
```

**4. Concurrent GC and Transactions**
```rust
#[test]
fn test_gc_with_concurrent_transactions() {
    // Start background GC
    // Run many concurrent transactions
    // Verify no data corruption
    // Verify no phantom reads
}
```

**5. GC Metrics**
```rust
#[test]
fn test_gc_statistics() {
    // Run GC
    // Verify stats are accurate:
    //   - versions_scanned
    //   - versions_reclaimable
    //   - versions_reclaimed
    //   - pages_freed
}
```

**6. Background GC Control**
```rust
#[test]
fn test_background_gc_pause_resume() {
    // Start background GC
    // Pause GC
    // Create many old versions
    // Verify not collected while paused
    // Resume GC
    // Wait for GC cycle
    // Verify versions collected
}
```

**7. GC Thread Shutdown**
```rust
#[test]
fn test_background_gc_graceful_shutdown() {
    // Start background GC
    // Stop GC
    // Verify thread exits cleanly
    // Verify no hanging threads
}
```

## Configuration

### Update `src/db/config.rs`

Already has GC configuration:
```rust
/// Enable Multi-Version Concurrency Control (MVCC) for snapshot isolation.
pub mvcc_enabled: bool,

/// Interval in seconds between garbage collection runs (None = disabled).
pub gc_interval_secs: Option<u64>,

/// Maximum length of a version chain before triggering errors.
pub max_version_chain_length: usize,

/// Snapshot retention time in seconds (affects garbage collection).
pub snapshot_retention_secs: u64,
```

May need to add:
```rust
/// Minimum number of versions to preserve per record
pub gc_min_versions_per_record: usize,  // default: 1

/// Maximum versions to scan per GC batch
pub gc_scan_batch_size: usize,  // default: 10000
```

## Safety Considerations

### 1. Never Remove Visible Versions
- GC must check `timestamp_oracle.gc_eligible_before()`
- Never remove versions with `commit_ts >= watermark`

### 2. Preserve At Least One Version
- Each live record must have at least one accessible version
- Even if all versions are old, keep the latest one

### 3. Concurrent Access
- GC runs concurrently with transactions
- Use transactions for GC operations
- Ensure no race conditions with version chain reads

### 4. Crash Recovery
- GC operations logged to WAL
- Partial GC can be recovered or rolled back
- No orphaned versions

### 5. Index Consistency
- Indexes updated atomically with version chain changes
- No dangling pointers
- Verify index integrity after GC

## Metrics and Monitoring

### GC Metrics to Track
- Versions scanned per cycle
- Versions reclaimed per cycle
- Pages freed per cycle
- GC cycle duration
- GC throughput (versions/sec)
- Average version chain length
- Max version chain length
- GC pause time impact on transactions

### Logging
- Log GC start/end with stats
- Log errors and warnings
- Log when chains exceed max length
- Log when GC is blocked

## Performance Optimization

### Future Enhancements (Post Phase 3)
1. **Incremental GC**: Process small batches to reduce pause time
2. **Parallel GC**: Scan multiple chains concurrently
3. **Generational GC**: Focus on recently-updated records
4. **Adaptive GC**: Adjust interval based on workload
5. **Background Compaction**: Separate scanning from compaction

## Success Criteria

Phase 3 is complete when:
- [x] GC scanner identifies reclaimable versions correctly
- [x] Background GC thread runs periodically
- [x] Version chain compaction removes old versions
- [x] All GC tests pass (7+ tests)
- [x] GC doesn't break active transactions
- [x] GC metrics are accurate
- [x] Documentation updated

## Timeline Estimate

- Task 14 (GC Scanner): 2-3 hours
- Task 15 (Background GC Thread): 1-2 hours
- Task 16 (Version Chain Compaction): 2-3 hours
- Task 17 (GC Tests): 2-3 hours
- **Total: 7-11 hours**

## References

- Snapshot isolation: `src/db/timestamp_oracle.rs`
- Version chains: `src/storage/version_chain.rs`
- Version metadata: `src/storage/version.rs`
- Transaction management: `src/db/transaction.rs`
