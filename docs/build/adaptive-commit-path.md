# Adaptive Commit Path Plan

## Goal

Close the single-writer sequential small-transaction performance gap between Sombra and SQLite by eliminating unnecessary thread handoff overhead when there's no contention to benefit from group commit.

**Target**: Reduce per-commit latency by eliminating 2-4 context switches (~10-50µs each) for the common case of sequential single-writer commits.

---

## Problem Analysis

### Current Commit Flow (Owned Path)

When committing with owned frames (`has_borrowed = false`), the flow is:

```
Caller Thread                    Worker Thread
─────────────────────────────────────────────────
1. Convert frames to owned
2. Lock state mutex
3. Push request to queue
4. Spawn worker (if needed)
5. Notify condvar                → 6. Wake up
7. Release mutex
8. Wait on ticket.cv             → 9. Lock state, pop request
                                 → 10. Coalesce (wait for more)
                                 → 11. wal.append_frame_batch()
                                 → 12. wal.sync()
                                 → 13. Notify ticket.cv
14. Wake up, return              ←
```

**For single-writer with no contention**, steps 4-8 and 13-14 are pure overhead:
- Thread spawn/condvar signal: ~1-5µs
- Context switch caller→worker: ~10-50µs  
- Context switch worker→caller: ~10-50µs
- Coalesce wait (2ms default): potentially waiting for nothing

### The Borrowed Path is Already Fast

When `has_borrowed = true` (pager.rs line 2158), the pager does:
```rust
let offsets = self.flush_pending_wal_frames(&wal_frames, sync_mode)?;
```

This calls `wal.append_frame_batch()` + `wal.sync()` directly in the caller thread - **no WalCommitter involvement**. This is exactly what we want for the owned path when there's no contention.

---

## Solution: Adaptive Direct Commit with Fsync Coalescing

### Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     commit_txn()                            │
├─────────────────────────────────────────────────────────────┤
│  has_borrowed?  ──yes──>  flush_pending_wal_frames()        │
│       │                          (already direct)           │
│      no                                                     │
│       │                                                     │
│       v                                                     │
│  try_direct_commit()?  ──success──>  direct WAL write+sync  │
│       │                              (with coalescing)      │
│     contention                                              │
│       │                                                     │
│       v                                                     │
│  WalCommitter.enqueue()  ──>  group commit path             │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

1. **Atomic Contention Detection**: Lock-free check using `AtomicUsize` for queue depth and `AtomicBool` for direct commit slot
2. **Direct WAL Write**: Bypass WalCommitter thread handoff when no contention
3. **Fsync Coalescing**: Optional delay window (default 100µs) to batch fsyncs across rapid sequential commits

---

## Implementation Details

### New Fields in WalCommitter

```rust
pub struct WalCommitter {
    wal: Arc<Wal>,
    state: Arc<Mutex<CommitState>>,
    wakeup: Arc<Condvar>,
    config: Arc<Mutex<WalCommitConfig>>,
    
    // Lock-free contention detection
    pending_count: AtomicUsize,       // Approximate queue depth
    direct_commit_active: AtomicBool, // True while direct commit in progress
    
    // Fsync coalescing state
    direct_sync_state: Arc<Mutex<DirectSyncState>>,
    sync_wakeup: Arc<Condvar>,
}

struct DirectSyncState {
    pending_syncs: Vec<Arc<SyncWaiter>>,
    last_append_time: Option<Instant>,
    sync_scheduled: bool,
}
```

### Direct Commit Flow

```rust
pub fn try_direct_commit(
    &self,
    frames: Vec<WalFrameOwned>,
    sync_mode: WalSyncMode,
) -> Result<Vec<WalFramePtr>, Vec<WalFrameOwned>> {
    // 1. Fast path: check if queue is empty (lock-free)
    if self.pending_count.load(Ordering::Acquire) > 0 {
        return Err(frames);
    }
    
    // 2. Try to claim direct commit slot
    if self.direct_commit_active.compare_exchange(
        false, true, Ordering::AcqRel, Ordering::Relaxed
    ).is_err() {
        return Err(frames);
    }
    
    // 3. Double-check queue
    if self.pending_count.load(Ordering::Acquire) > 0 {
        self.direct_commit_active.store(false, Ordering::Release);
        return Err(frames);
    }
    
    // 4. Perform direct WAL write with optional fsync coalescing
    let result = self.do_direct_write(&frames, sync_mode);
    
    self.direct_commit_active.store(false, Ordering::Release);
    
    result.map_err(|_| frames)
}
```

### Fsync Coalescing

When `direct_fsync_delay > 0`:

1. After `append_frame_batch()`, check if we can coalesce with a pending sync
2. If within the delay window of the last sync request, join the pending batch
3. The last writer in a batch triggers the actual sync
4. All waiters are notified when sync completes

```rust
fn do_direct_write_with_coalesce(
    &self,
    frames: &[WalFrameOwned],
    sync_mode: WalSyncMode,
    config: &WalCommitConfig,
) -> Result<Vec<WalFramePtr>> {
    // Append frames
    let refs = Self::frames_to_refs(frames);
    let offsets = self.wal.append_frame_batch(&refs)?;
    
    if !matches!(sync_mode, WalSyncMode::Immediate) {
        return Ok(offsets);
    }
    
    if config.direct_fsync_delay.is_zero() {
        // Immediate sync
        self.wal.sync()?;
        return Ok(offsets);
    }
    
    // Coalesced sync
    self.wait_for_coalesced_sync(config.direct_fsync_delay)?;
    Ok(offsets)
}
```

---

## Configuration

### New Config Fields

```rust
pub struct WalCommitConfig {
    // Existing
    pub max_batch_commits: usize,      // Default: 32
    pub max_batch_frames: usize,       // Default: 512  
    pub max_batch_wait: Duration,      // Default: 2ms
    
    // New
    pub direct_commit_enabled: bool,   // Default: true
    pub direct_fsync_delay: Duration,  // Default: 100µs
}
```

### CLI Flags

```
--direct-commit=on|off           Enable/disable direct commit path
--direct-fsync-delay-us=N        Fsync coalesce window in microseconds (default: 100)
```

---

## Metrics

| Metric | Description |
|--------|-------------|
| `wal.commit.direct` | Counter: commits via direct path |
| `wal.commit.group` | Counter: commits via group commit path |
| `wal.commit.direct_contention` | Counter: direct commit attempts that fell back due to contention |
| `wal.sync.coalesced` | Counter: syncs that coalesced multiple commits |
| `wal.sync.coalesce_batch_size` | Histogram: number of commits per coalesced sync |

---

## Safety Invariants

1. **Durability**: Direct path calls `wal.sync()` before returning (with `Synchronous::Full`)
2. **Ordering**: WAL append order matches commit order (single writer lock ensures this)
3. **Crash safety**: Frames are durable after sync returns - no change from current behavior
4. **Concurrent readers**: No impact - readers use snapshot isolation

### Race Condition Handling

| Scenario | Handling |
|----------|----------|
| Direct commit vs enqueue race | `direct_commit_active` atomic prevents concurrent direct commits |
| Queue check race | Double-check `pending_count` after claiming direct slot |
| Worker startup race | If worker starts between checks, direct commit aborts and falls back |

---

## Testing Strategy

### Unit Tests

1. Direct commit succeeds when queue empty
2. Direct commit falls back when queue has pending items
3. Direct commit falls back when another direct commit is active
4. Fsync coalescing batches multiple commits
5. Coalesce timeout triggers sync after delay

### Stress Tests

1. Rapid path toggling between direct and group
2. High contention with many writers
3. Mixed borrowed/owned frame commits
4. Fsync coalescing under burst load

### Chaos Tests

1. Kill process during coalesced sync wait
2. Verify durability after crash with various sync states

---

## Success Metrics

| Metric | Baseline | Target |
|--------|----------|--------|
| Single-writer commit latency (excluding fsync) | ~200-300µs | <100µs |
| Thread context switches per commit | 2-4 | 0 |
| Direct commit ratio (no contention) | 0% | >95% |
| Fsync count (with 100µs coalescing) | 1 per commit | ~0.5-0.7 per commit |

---

## Implementation Phases

### Phase 1: Core Implementation
- Add atomic fields to WalCommitter
- Implement `try_direct_commit()`
- Update `enqueue()` to maintain `pending_count`
- Implement fsync coalescing
- Integrate with Pager

### Phase 2: Observability
- Add metrics
- Wire up CLI flags
- Add to `mvcc-status` output

### Phase 3: Testing
- Unit tests for direct commit
- Stress tests for path toggling
- Chaos tests for crash safety

### Phase 4: Validation
- Run `compare-bench` to measure improvement
- Document results and tuning guidance
