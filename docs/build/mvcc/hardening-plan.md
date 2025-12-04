# MVCC Hardening Implementation Plan

> **Status**: Planning  
> **Created**: 2025-12-03  
> **Goal**: Move MVCC implementation from Alpha to Beta readiness

## Overview

This plan addresses 5 critical gaps in the MVCC implementation:

1. **Concurrent stress tests** — Multi-threaded readers + writer exercising MVCC
2. **Isolation anomaly tests** — Explicit tests for write skew, phantom reads
3. **Document isolation guarantees** — Clear statement that SI (not serializable) is provided
4. **Reader timeout** — Prevent long-running readers from blocking vacuum forever
5. **Conflict detection** — First-committer-wins or similar for robustness

---

## 1. Concurrent Stress Tests

### Goal
Validate MVCC correctness under concurrent multi-threaded access with readers and writers operating simultaneously.

### Location
New file: `tests/integration/mvcc_concurrent_stress.rs`

### Test Cases

#### 1.1 `concurrent_readers_with_active_writer`
- **Setup**: Create graph with 100 nodes
- **Threads**: 
  - 1 writer thread: continuously updates nodes in a loop (500 iterations)
  - 4 reader threads: each opens a snapshot, reads all nodes, verifies consistency
- **Assertions**:
  - Each reader sees a consistent snapshot (no partial updates)
  - Reader snapshot doesn't change even as writes continue
  - All threads complete without panic/error
- **Pattern**: Use `std::thread::spawn` with `Arc<Pager>` and `Arc<Graph>`

#### 1.2 `snapshot_stability_under_concurrent_writes`
- **Setup**: Create 10 nodes with property `version=0`
- **Flow**:
  1. Reader A opens snapshot
  2. Writer updates all nodes to `version=1`
  3. Reader B opens snapshot
  4. Writer updates all nodes to `version=2`
  5. All readers verify their snapshot
- **Assertions**:
  - Reader A sees `version=0` for all nodes
  - Reader B sees `version=1` for all nodes
  - New reader sees `version=2`

#### 1.3 `vacuum_respects_active_readers`
- **Setup**: Create nodes, delete some, hold old snapshot
- **Flow**:
  1. Reader pins old snapshot
  2. Vacuum runs (should NOT reclaim versions pinned by reader)
  3. Reader verifies deleted nodes still visible in its snapshot
  4. Reader drops
  5. Vacuum runs again (should reclaim)
- **Assertions**: Old snapshot sees deleted data until dropped

#### 1.4 `high_contention_read_write_mix`
- **Setup**: 50 nodes, 200 edges
- **Threads**: 
  - 2 writer threads (alternating writes with small sleep)
  - 8 reader threads (continuous reads)
- **Duration**: 5 seconds
- **Assertions**:
  - No deadlocks (test completes within timeout)
  - No corruption (final verification passes)
  - Metrics show expected reader/writer counts

### Implementation Notes
```rust
// Pattern from wal_backlog.rs
let handles: Vec<_> = (0..READER_COUNT)
    .map(|i| {
        let pager = Arc::clone(&pager);
        let graph = Arc::clone(&graph);
        thread::spawn(move || -> Result<()> {
            let read = pager.begin_latest_committed_read()?;
            // ... assertions ...
            Ok(())
        })
    })
    .collect();

for handle in handles {
    handle.join().expect("thread join").unwrap()?;
}
```

### Dependencies
- None (uses existing infrastructure)

### Estimated Effort
~4-6 hours

---

## 2. Isolation Anomaly Tests

### Goal
Explicitly test for known SQL isolation anomalies to document which are prevented by SI and which are allowed.

### Location
New file: `tests/integration/mvcc_isolation_anomalies.rs`

### Test Cases

#### 2.1 `dirty_read_prevented` (SI prevents)
- **Flow**:
  1. Txn A: begin write, create node with `value=1`, DO NOT commit
  2. Txn B: begin read, try to read the node
- **Assertion**: Txn B does NOT see uncommitted node

#### 2.2 `non_repeatable_read_prevented` (SI prevents)
- **Flow**:
  1. Create node with `value=1`
  2. Txn A: begin read, read node (sees `value=1`)
  3. Txn B: update node to `value=2`, commit
  4. Txn A: read node again
- **Assertion**: Txn A still sees `value=1` (repeatable read)

#### 2.3 `phantom_read_prevented` (SI prevents)
- **Flow**:
  1. Create nodes with label `L1`: node1, node2
  2. Txn A: begin read, scan nodes with label `L1` (sees 2 nodes)
  3. Txn B: create node3 with label `L1`, commit
  4. Txn A: scan nodes with label `L1` again
- **Assertion**: Txn A still sees only 2 nodes (no phantom)

#### 2.4 `lost_update_prevented_by_single_writer` (Single-writer prevents)
- **Flow**:
  1. Create node with `counter=0`
  2. Txn A: begin write, read counter (0), increment to 1
  3. Txn B: try to begin write (should block or fail)
- **Assertion**: Only one writer allowed (lost update impossible with single-writer)
- **Note**: Document this is architecture-enforced, not SI-enforced

#### 2.5 `write_skew_scenario_documented` (SI allows - document this!)
- **Purpose**: Demonstrate that write skew IS possible under SI (with multi-writer)
- **Setup**: 
  - `account_a` with `balance=100`
  - `account_b` with `balance=100`
  - Constraint: `sum(balances) >= 0` (enforced by app)
- **Flow** (hypothetical with multi-writer):
  1. Txn A: read both accounts, sees total 200
  2. Txn B: read both accounts, sees total 200
  3. Txn A: withdraw 150 from account_a (thinks 50 remains)
  4. Txn B: withdraw 150 from account_b (thinks 50 remains)
  5. Both commit
  6. Result: total balance = -100 (constraint violated!)
- **Note**: Since single-writer prevents this, this test demonstrates the SCENARIO but documents it as "would be possible with multi-writer"

#### 2.6 `read_only_anomaly_not_applicable`
- **Purpose**: Document that read-only anomaly is not applicable to SI
- **Notes-only test**: Just comments explaining the anomaly

### Implementation Notes
- Each test should have extensive comments explaining the anomaly
- Tests should work with current single-writer; some tests document what WOULD happen with multi-writer
- Use `#[test]` with clear naming: `si_prevents_*`, `si_allows_*`

### Dependencies
- None

### Estimated Effort
~3-4 hours

---

## 3. Document Isolation Guarantees

### Goal
Create clear, user-facing documentation of isolation guarantees.

### Location
New file: `docs/isolation-guarantees.md`

### Document Structure

```markdown
# Isolation Guarantees

## Summary

Sombra provides **Snapshot Isolation (SI)** for all transactions. This document
explains what SI guarantees, what anomalies it prevents, and what anomalies
remain possible.

## Transaction Model

- **Single-writer**: Only one write transaction can be active at a time
- **Multi-reader**: Multiple read transactions can run concurrently
- **Readers never block writers**: Read transactions see a consistent snapshot
  and never wait for write transactions
- **Writers never block readers**: Write transactions proceed without waiting
  for read transactions to complete

## Snapshot Isolation Guarantees

### Prevented Anomalies

| Anomaly | Description | Status |
|---------|-------------|--------|
| Dirty Read | Reading uncommitted data | Prevented |
| Non-Repeatable Read | Same query returning different results | Prevented |
| Phantom Read | New rows appearing in repeated scans | Prevented |
| Lost Update | Concurrent updates overwriting each other | Prevented (single-writer) |

### Allowed Anomalies (by SI design)

| Anomaly | Description | Status |
|---------|-------------|--------|
| Write Skew | ... | Would be allowed (prevented by single-writer currently) |

## Comparison to SQL Isolation Levels

| Level | SI Equivalent? |
|-------|---------------|
| READ UNCOMMITTED | No (stronger) |
| READ COMMITTED | No (stronger) |
| REPEATABLE READ | Approximately |
| SERIALIZABLE | No (weaker) |

## Implementation Details

### Visibility Rules
[Link to mvcc-baseline.md sections]

### Vacuum and Reader Interaction
[Explain how old readers pin versions]

### Future: Serializable Support
[Roadmap for SSI if planned]
```

### Updates to Existing Docs

1. **`docs/production-readiness.md`**: Add checkmark to "Document guarantees" task
2. **`docs/test-matrices/README.md`**: Update "Anomaly detection" row status
3. **`README.md`**: Add brief mention with link to isolation doc

### Estimated Effort
~2-3 hours

---

## 4. Reader Timeout

### Goal
Prevent long-running readers from blocking vacuum indefinitely by implementing configurable reader timeouts.

### Design Decisions Required

**Question 1**: What should happen when a reader times out?
- **Option A**: Return error on next read operation (`SombraError::SnapshotTooOld`)
- **Option B**: Silently invalidate and force re-acquisition on next operation
- **Option C**: Just log a warning but allow continued operation (monitoring only)

**Question 2**: Should timeout be:
- **Option A**: Wall-clock time since reader began
- **Option B**: Time since last read operation (activity-based)

**Question 3**: Default timeout value?
- Suggestion: 30 minutes for wall-clock, with ability to disable (`Duration::MAX`)

### Proposed Implementation

#### 4.1 Configuration Changes

**File**: `src/storage/options.rs`

```rust
pub struct VacuumCfg {
    // ... existing fields ...
    
    /// Maximum age for readers before they are considered stale.
    /// Readers older than this may be forcibly invalidated.
    /// Set to Duration::MAX to disable.
    pub reader_timeout: Duration,
    
    /// Whether to log warnings for readers approaching timeout.
    pub reader_timeout_warn_threshold_pct: u8, // e.g., 80 = warn at 80% of timeout
}

impl Default for VacuumCfg {
    fn default() -> Self {
        Self {
            // ... existing ...
            reader_timeout: Duration::from_secs(30 * 60), // 30 minutes
            reader_timeout_warn_threshold_pct: 80,
        }
    }
}
```

#### 4.2 CommitTable Changes

**File**: `src/storage/mvcc.rs`

Add method to check for timed-out readers:

```rust
impl CommitTable {
    /// Returns readers that have exceeded the timeout threshold.
    pub fn expired_readers(&self, timeout: Duration, now: Instant) -> Vec<ReaderId> {
        let cutoff = now.checked_sub(timeout).unwrap_or(now);
        self.readers
            .iter()
            .filter(|(_, info)| info.begin_instant < cutoff)
            .map(|(&id, _)| id)
            .collect()
    }
    
    /// Forcibly evicts a reader, releasing its pin on the snapshot.
    /// Returns the commit that was pinned (for logging).
    pub fn evict_reader(&mut self, reader_id: ReaderId) -> Option<CommitId> {
        if let Some(info) = self.readers.remove(&reader_id) {
            // Decrement the appropriate refcount
            match self.reader_floor.get_mut(&info.snapshot) {
                Some(count) if *count > 1 => *count -= 1,
                Some(_) => { self.reader_floor.remove(&info.snapshot); }
                None => {
                    // Reader was on an entry, not floor
                    if let Some(entry) = self.entries.iter_mut()
                        .find(|e| e.id == info.snapshot) {
                        entry.reader_refs = entry.reader_refs.saturating_sub(1);
                    }
                }
            }
            Some(info.snapshot)
        } else {
            None
        }
    }
}
```

#### 4.3 ReadGuard Validation

**File**: `src/primitives/pager/pager.rs`

Add validation to `ReadGuard` operations:

```rust
impl ReadGuard {
    /// Checks if this read guard is still valid.
    pub fn validate(&self) -> Result<()> {
        // Check if reader has been evicted
        if self.evicted.load(Ordering::Acquire) {
            return Err(SombraError::Invalid("reader snapshot expired"));
        }
        Ok(())
    }
}
```

#### 4.4 Vacuum Integration

**File**: `src/storage/graph.rs` (vacuum worker)

In the vacuum loop, add reader timeout enforcement:

```rust
fn run_vacuum_pass(&self, /* ... */) -> Result<GraphVacuumStats> {
    // ... existing vacuum logic ...
    
    // Check for expired readers
    if let Some(ref commit_table) = self.commit_table {
        let mut table = commit_table.lock();
        let expired = table.expired_readers(self.vacuum_cfg.reader_timeout, Instant::now());
        for reader_id in expired {
            if let Some(commit) = table.evict_reader(reader_id) {
                warn!(
                    reader_id = reader_id,
                    commit = commit,
                    "evicted stale reader exceeding timeout"
                );
                self.metrics.mvcc_reader_evicted();
            }
        }
    }
    
    // ... continue with vacuum ...
}
```

#### 4.5 Metrics

**File**: `src/storage/metrics.rs`

Add metric:
```rust
fn mvcc_reader_evicted(&self);
pub mvcc_readers_evicted: AtomicU64,
```

#### 4.6 Tests

**File**: `tests/integration/mvcc_reader_timeout.rs`

```rust
#[test]
fn reader_timeout_eviction() -> Result<()> {
    // Setup with very short timeout (100ms)
    let cfg = VacuumCfg {
        reader_timeout: Duration::from_millis(100),
        ..small_vacuum_cfg()
    };
    
    let (_dir, pager, graph) = setup_graph(cfg)?;
    
    // Create and hold a reader
    let read = pager.begin_latest_committed_read()?;
    
    // Wait for timeout
    thread::sleep(Duration::from_millis(150));
    
    // Trigger vacuum (which checks timeouts)
    // ... trigger vacuum ...
    
    // Verify reader is invalidated
    assert!(read.validate().is_err());
    
    Ok(())
}
```

### Estimated Effort
~6-8 hours

---

## 5. Conflict Detection (First-Committer-Wins)

### Goal
Implement conflict detection so that if the single-writer lock ever fails or for future multi-writer support, the system can detect and handle conflicts.

### Design Considerations

The current single-writer lock prevents write-write conflicts at the process level. Conflict detection adds a second layer of defense:

1. **Page-level tracking**: Track which pages were read/written by each transaction
2. **Commit-time validation**: Before committing, verify no conflicts
3. **First-committer-wins**: If conflict detected, abort the later transaction

### Implementation

#### 5.1 Write Intent Tracking

**File**: `src/storage/mvcc.rs`

```rust
/// Tracks pages written by a pending transaction for conflict detection.
#[derive(Clone, Debug, Default)]
pub struct WriteSet {
    /// Pages modified during this transaction.
    pub pages: HashSet<PageId>,
    /// Records (nodes/edges) modified during this transaction.
    pub records: HashSet<(VersionSpace, u64)>,
    /// Intent ID for this transaction.
    pub intent_id: IntentId,
}

impl WriteSet {
    pub fn new(intent_id: IntentId) -> Self {
        Self {
            pages: HashSet::new(),
            records: HashSet::new(),
            intent_id,
        }
    }
    
    pub fn mark_page(&mut self, page_id: PageId) {
        self.pages.insert(page_id);
    }
    
    pub fn mark_record(&mut self, space: VersionSpace, id: u64) {
        self.records.insert((space, id));
    }
}
```

#### 5.2 Conflict Detection Logic

**File**: `src/storage/mvcc.rs`

```rust
/// Result of conflict check.
#[derive(Debug)]
pub enum ConflictResult {
    /// No conflict detected.
    Ok,
    /// Conflict with another committed transaction.
    Conflict {
        conflicting_commit: CommitId,
        reason: ConflictReason,
    },
}

#[derive(Debug)]
pub enum ConflictReason {
    /// Same record was modified.
    RecordConflict { space: VersionSpace, id: u64 },
    /// Same page was modified.
    PageConflict { page_id: PageId },
}

impl CommitTable {
    /// Checks if the write set conflicts with any committed transaction
    /// since the transaction began.
    pub fn check_conflicts(
        &self,
        write_set: &WriteSet,
        started_at: CommitId,
    ) -> ConflictResult {
        // For single-writer, this is mostly a no-op since there can't be
        // concurrent writers. But we track anyway for:
        // 1. Future multi-writer support
        // 2. Defense in depth if lock fails
        
        // Check committed transactions since we started
        for entry in &self.entries {
            if entry.id <= started_at {
                continue;
            }
            if entry.status != CommitStatus::Committed && 
               entry.status != CommitStatus::Durable {
                continue;
            }
            
            // In full implementation, we'd check entry's write set
            // against our write set for overlaps.
            // For now, single-writer ensures no overlap.
        }
        
        ConflictResult::Ok
    }
}
```

#### 5.3 Integration with Commit Path

**File**: `src/primitives/pager/pager.rs` (commit path)

```rust
pub fn commit(&self, write: WriteGuard) -> Result<Lsn> {
    // ... existing setup ...
    
    // Conflict check (defense in depth)
    if let Some(ref commit_table) = self.commit_table {
        let table = commit_table.lock();
        match table.check_conflicts(&write.write_set, write.started_at) {
            ConflictResult::Ok => {}
            ConflictResult::Conflict { conflicting_commit, reason } => {
                self.metrics.mvcc_write_lock_conflict();
                return Err(SombraError::Conflict(format!(
                    "write conflict with commit {}: {:?}",
                    conflicting_commit, reason
                )));
            }
        }
    }
    
    // ... continue with commit ...
}
```

#### 5.4 New Error Type

**File**: `src/types/mod.rs`

```rust
pub enum SombraError {
    // ... existing variants ...
    
    /// Write-write conflict detected during commit.
    Conflict(String),
}
```

#### 5.5 Tests

**File**: `tests/integration/mvcc_conflict_detection.rs`

```rust
#[test]
fn conflict_detection_metric_recorded() -> Result<()> {
    // Even with single-writer, verify the conflict check path runs
    // and metrics are recorded correctly
    
    let metrics = Arc::new(TestMetrics::default());
    let opts = GraphOptions::new(store).metrics(metrics.clone());
    
    // Normal commit should record zero conflicts
    let mut write = pager.begin_write()?;
    graph.create_node(&mut write, /* ... */)?;
    pager.commit(write)?;
    
    assert_eq!(metrics.mvcc_write_lock_conflicts.load(Ordering::Relaxed), 0);
    
    Ok(())
}

#[test] 
fn write_set_tracks_modified_records() -> Result<()> {
    let mut write = pager.begin_write()?;
    let node_id = graph.create_node(&mut write, /* ... */)?;
    
    // Verify write set contains the record
    assert!(write.write_set.records.contains(&(VersionSpace::Node, node_id.0)));
    
    pager.commit(write)?;
    Ok(())
}
```

### Estimated Effort
~8-10 hours

---

## Summary and Execution Order

| Task | Files | Effort | Priority |
|------|-------|--------|----------|
| 3. Document isolation guarantees | `docs/isolation-guarantees.md` | 2-3h | P1 (documentation first) |
| 1. Concurrent stress tests | `tests/integration/mvcc_concurrent_stress.rs` | 4-6h | P1 |
| 2. Isolation anomaly tests | `tests/integration/mvcc_isolation_anomalies.rs` | 3-4h | P1 |
| 4. Reader timeout | `src/storage/mvcc.rs`, `src/storage/options.rs`, etc. | 6-8h | P2 |
| 5. Conflict detection | `src/storage/mvcc.rs`, `src/primitives/pager/pager.rs` | 8-10h | P2 |

**Total estimated effort**: 23-31 hours

### Recommended Execution Order

1. **Documentation first** (Task 3) — Establishes the contract before testing
2. **Concurrent stress tests** (Task 1) — Most likely to find existing bugs
3. **Isolation anomaly tests** (Task 2) — Documents behavior clearly
4. **Reader timeout** (Task 4) — Operational safety feature
5. **Conflict detection** (Task 5) — Defense in depth / future-proofing

---

## Open Questions

Before proceeding with implementation, decisions needed on:

1. **Reader timeout behavior**: What should happen when a reader times out?
   - Option A: Return error on next read operation (`SombraError::SnapshotTooOld`)
   - Option B: Silently invalidate and force re-acquisition
   - Option C: Log warning only (monitoring)

2. **Timeout measurement**: Wall-clock time since reader began, or activity-based?

3. **Default timeout value**: Suggested 30 minutes, configurable

4. **Conflict detection scope**: Page-level only, or also record-level (nodes/edges)?

5. **Test parallelism**: Should concurrent tests use `#[ignore]` for separate runs, or run in normal CI?

---

## References

- `docs/production-readiness.md` — Phase 2 checklist items
- `docs/mvcc-baseline.md` — MVCC design specification
- `docs/test-matrices/README.md` — Test coverage tracking
- `src/storage/mvcc.rs` — Core MVCC implementation
- `tests/integration/storage_phase2.rs` — Existing MVCC tests
