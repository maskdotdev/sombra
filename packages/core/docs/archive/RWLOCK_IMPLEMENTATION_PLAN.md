# RwLock Optimization Implementation Plan

## Overview

This document provides a detailed implementation plan for optimizing `ConcurrentGraphDB` 
by replacing the coarse-grained `Arc<Mutex<GraphDB>>` with `Arc<RwLock<GraphDB>>` to 
enable concurrent reads.

## Current Performance Problem

**Benchmarked on concurrent_throughput:**
- **Read @ 1 thread**: 2,629,565 ops/sec
- **Read @ 50 threads**: 559,647 ops/sec (5.8x **throughput drop**)
- **Mixed @ 1 thread**: 285,483 ops/sec
- **Mixed @ 2 threads**: 64,449 ops/sec (4.4x **slower**)

**Root Cause**: `Arc<Mutex<GraphDB>>` serializes all operations, even reads.

## Performance Goal

**Target after both phases:**
- **Read @ 50 threads**: 3-5M ops/sec (5-9x improvement from 560K)
- **Write @ 20 threads**: ~16K ops/sec (unchanged - acceptable)
- **Mixed @ 10 threads**: 300-500K ops/sec (4-7x improvement from 65K)

---

## Phase 1: Interior Mutability

**Objective**: Modify GraphDB to allow read methods to work with `&self` instead of `&mut self`

**Estimated Effort**: 2-3 days  
**Risk Level**: Medium (lock ordering, performance overhead)

### 1.1 Core Struct Changes

**File**: `src/db/core/graphdb.rs`

**Current:**
```rust
pub struct GraphDB {
    pub(crate) path: PathBuf,
    pub(crate) pager: Pager,
    pub(crate) node_cache: LruCache<NodeId, Node>,
    pub(crate) edge_cache: LruCache<EdgeId, Edge>,
    pub(crate) tracking_enabled: bool,
    pub(crate) recent_dirty_pages: Vec<PageId>,
    // ... other fields
}
```

**Target:**
```rust
use std::sync::{Mutex, atomic::{AtomicBool, Ordering}};

pub struct GraphDB {
    pub(crate) path: PathBuf,
    pub(crate) pager: Mutex<Pager>,                        // ← Wrap in Mutex
    pub(crate) node_cache: Mutex<LruCache<NodeId, Node>>,  // ← Wrap in Mutex
    pub(crate) edge_cache: Mutex<LruCache<EdgeId, Edge>>,  // ← Wrap in Mutex
    pub(crate) tracking_enabled: AtomicBool,               // ← Use AtomicBool
    pub(crate) recent_dirty_pages: Mutex<Vec<PageId>>,     // ← Wrap in Mutex
    // ... other fields unchanged
}
```

**Changes Required:**
1. Update struct definition (5 fields)
2. Update initialization in `open_with_config()` (wrap fields in Mutex/AtomicBool)
3. Update all access patterns throughout codebase

**Estimated Changes**: ~150 locations

---

### 1.2 Method Signature Changes

**Files**: 
- `src/db/core/graphdb.rs`
- `src/db/core/nodes.rs`
- `src/db/core/edges.rs`
- `src/db/core/records.rs`
- `src/db/core/transaction_support.rs`

**Read Methods to Update** (change `&mut self` → `&self`):

#### Core Read Operations:
```rust
// In graphdb.rs and nodes.rs
pub fn get_node(&self, node_id: NodeId) -> Result<Option<Node>>
pub fn get_node_with_snapshot(&self, node_id: NodeId, snapshot_ts: u64, tx_id: Option<TxId>) -> Result<Option<Node>>
pub fn get_nodes_by_label(&self, label: &str) -> Result<Vec<Node>>
pub fn find_nodes_by_property(&self, label: &str, key: &str, value: &PropertyValue) -> Result<Vec<Node>>

// In edges.rs
pub fn load_edge(&self, edge_id: EdgeId) -> Result<Edge>
pub fn load_edge_with_snapshot(&self, edge_id: EdgeId, snapshot_ts: u64, tx_id: Option<TxId>) -> Result<Edge>
pub fn load_edges_batch(&self, edge_ids: &[EdgeId]) -> Result<Vec<Edge>>

// In transaction_support.rs
pub fn enable_tracking(&self)
pub fn disable_tracking(&self)
pub fn take_recent_dirty_pages(&self) -> Vec<PageId>
```

**Write Methods Keep** `&mut self` (no changes):
```rust
pub fn add_node_internal(&mut self, node: Node, tx_id: TxId, commit_ts: u64) -> Result<(NodeId, Option<RecordPointer>)>
pub fn add_edge_internal(&mut self, edge: Edge, tx_id: TxId, commit_ts: u64) -> Result<(EdgeId, Option<RecordPointer>)>
pub fn update_node(&mut self, node_id: NodeId, node: Node) -> Result<()>
// ... all other write operations
```

**Total Methods to Update**: ~15 read methods

---

### 1.3 RecordStore Pattern Change

**File**: `src/db/core/records.rs`

**Current:**
```rust
impl GraphDB {
    pub(crate) fn record_store(&mut self) -> RecordStore<'_> {
        RecordStore::new(&mut self.pager)
    }
}
```

**Target - Option A (Simpler):**
```rust
impl GraphDB {
    pub(crate) fn record_store(&self) -> RecordStoreGuard<'_> {
        RecordStoreGuard {
            pager_guard: self.pager.lock().unwrap()
        }
    }
}

pub struct RecordStoreGuard<'a> {
    pager_guard: MutexGuard<'a, Pager>,
}

impl<'a> std::ops::Deref for RecordStoreGuard<'a> {
    type Target = RecordStore<'a>;
    
    fn deref(&self) -> &Self::Target {
        // Return RecordStore wrapping the pager
        // Note: This requires RecordStore to be refactored
    }
}
```

**Target - Option B (More invasive):**
Keep `record_store()` signature, change call sites to lock pager first:
```rust
// At call sites:
let mut pager = self.pager.lock().unwrap();
let mut record_store = RecordStore::new(&mut *pager);
```

**Recommendation**: Start with Option B (less refactoring), can optimize later.

**Call Sites to Update**: ~14 locations

---

### 1.4 Access Pattern Updates

#### Pager Access (57 locations)

**Current Pattern:**
```rust
let page = self.pager.fetch_page(page_id)?;
self.pager.flush_page(page_id)?;
```

**New Pattern:**
```rust
let page = self.pager.lock().unwrap().fetch_page(page_id)?;
self.pager.lock().unwrap().flush_page(page_id)?;
```

**Optimization for Multiple Calls:**
```rust
// Instead of:
self.pager.lock().unwrap().fetch_page(page1)?;
self.pager.lock().unwrap().fetch_page(page2)?;

// Do:
let mut pager = self.pager.lock().unwrap();
pager.fetch_page(page1)?;
pager.fetch_page(page2)?;
drop(pager); // Release lock explicitly
```

**Files to Update:**
- `src/db/core/nodes.rs` (~20 pager calls)
- `src/db/core/edges.rs` (~15 pager calls)
- `src/db/core/graphdb.rs` (~10 pager calls)
- `src/db/core/index.rs` (~5 pager calls)
- `src/db/core/traversal.rs` (~7 pager calls)

---

#### Node Cache Access (30 locations)

**Current Pattern:**
```rust
if let Some(node) = self.node_cache.get(&node_id) {
    return Ok(Some(node.clone()));
}
self.node_cache.put(node_id, node.clone());
```

**New Pattern:**
```rust
{
    let mut cache = self.node_cache.lock().unwrap();
    if let Some(node) = cache.get(&node_id) {
        return Ok(Some(node.clone()));
    }
}
// ... load node ...
self.node_cache.lock().unwrap().put(node_id, node.clone());
```

**Files to Update:**
- `src/db/core/nodes.rs` (~20 cache accesses)
- `src/db/core/graphdb.rs` (~10 cache accesses)

---

#### Edge Cache Access (15 locations)

**Current Pattern:**
```rust
if let Some(edge) = self.edge_cache.get(&edge_id) {
    return Ok(edge.clone());
}
self.edge_cache.put(edge_id, edge.clone());
```

**New Pattern:**
```rust
{
    let mut cache = self.edge_cache.lock().unwrap();
    if let Some(edge) = cache.get(&edge_id) {
        return Ok(edge.clone());
    }
}
// ... load edge ...
self.edge_cache.lock().unwrap().put(edge_id, edge.clone());
```

**Files to Update:**
- `src/db/core/edges.rs` (~12 cache accesses)
- `src/db/core/graphdb.rs` (~3 cache accesses)

---

#### Tracking State Access (6 locations)

**Current Pattern:**
```rust
if self.tracking_enabled {
    self.recent_dirty_pages.push(page_id);
}
```

**New Pattern:**
```rust
if self.tracking_enabled.load(Ordering::Acquire) {
    self.recent_dirty_pages.lock().unwrap().push(page_id);
}
```

**Files to Update:**
- `src/db/core/transaction_support.rs` (~4 tracking accesses)
- `src/db/core/graphdb.rs` (~2 tracking accesses)

---

### 1.5 Lock Ordering Strategy

**Critical**: Establish consistent lock ordering to prevent deadlocks.

**Proposed Lock Hierarchy** (acquire in this order):
1. `pager` (most coarse-grained - I/O operations)
2. `node_cache` / `edge_cache` (caches - medium grain)
3. `recent_dirty_pages` (finest grain - tracking)

**Rules**:
1. Never hold `cache` lock while acquiring `pager` lock
2. Never hold `recent_dirty_pages` lock while acquiring `cache` lock
3. Always release locks in reverse order of acquisition
4. Keep lock critical sections as short as possible
5. Don't call other GraphDB methods while holding locks

**Example - Correct Pattern:**
```rust
// Step 1: Check cache (acquire cache lock briefly)
{
    let mut cache = self.node_cache.lock().unwrap();
    if let Some(node) = cache.get(&node_id) {
        return Ok(Some(node.clone()));
    }
} // cache lock released

// Step 2: Load from pager (acquire pager lock)
let node = {
    let mut pager = self.pager.lock().unwrap();
    // ... load node from pager ...
    node
}; // pager lock released

// Step 3: Update cache (acquire cache lock again)
{
    let mut cache = self.node_cache.lock().unwrap();
    cache.put(node_id, node.clone());
} // cache lock released

Ok(Some(node))
```

**Example - Deadlock Pattern (AVOID):**
```rust
// WRONG: Holding cache lock while acquiring pager lock
let mut cache = self.node_cache.lock().unwrap();
let mut pager = self.pager.lock().unwrap(); // DEADLOCK RISK
```

---

### 1.6 Error Handling for Poison Locks

**Strategy**: Treat poisoned locks as corruption errors.

**Pattern:**
```rust
let mut pager = self.pager.lock().map_err(|e| {
    GraphError::Corruption(format!("pager lock poisoned: {}", e))
})?;
```

**Alternative** (for non-critical paths):
```rust
let mut cache = self.node_cache.lock().unwrap_or_else(|poisoned| {
    warn!("node_cache lock poisoned, clearing cache");
    poisoned.into_inner()
});
```

---

### 1.7 Phase 1 Testing Strategy

**Unit Tests**:
- All existing tests must pass
- No new test failures introduced
- Specifically verify: `cargo test --lib`

**Integration Tests**:
- Run full MVCC test suite: `cargo test mvcc`
- Run concurrent tests: `cargo test concurrent`
- Verify snapshot isolation still works

**Performance Tests**:
- Benchmark read performance: Should be 5-10% slower (acceptable)
- Run `cargo bench --bench concurrent_throughput`
- Document Phase 1 overhead (baseline for Phase 2)

**Stress Tests**:
- Run 24-hour stress test with 10 concurrent threads
- Monitor for deadlocks (test should complete without hanging)
- Check for lock poisoning (no panics)

---

### 1.8 Phase 1 Implementation Checklist

#### Step 1: Update GraphDB Struct
- [ ] Add `use std::sync::{Mutex, atomic::{AtomicBool, Ordering}};`
- [ ] Change `pager: Pager` → `pager: Mutex<Pager>`
- [ ] Change `node_cache: LruCache<..>` → `node_cache: Mutex<LruCache<..>>`
- [ ] Change `edge_cache: LruCache<..>` → `edge_cache: Mutex<LruCache<..>>`
- [ ] Change `tracking_enabled: bool` → `tracking_enabled: AtomicBool`
- [ ] Change `recent_dirty_pages: Vec<PageId>` → `recent_dirty_pages: Mutex<Vec<PageId>>`

#### Step 2: Update GraphDB Initialization
- [ ] Wrap `Pager::open(...)` in `Mutex::new(...)`
- [ ] Wrap `LruCache::new(...)` in `Mutex::new(...)` (both caches)
- [ ] Wrap `Vec::new()` in `Mutex::new(...)` for recent_dirty_pages
- [ ] Change `tracking_enabled: false` → `tracking_enabled: AtomicBool::new(false)`

#### Step 3: Update Method Signatures
- [ ] `get_node()`: `&mut self` → `&self`
- [ ] `get_node_with_snapshot()`: `&mut self` → `&self`
- [ ] `get_nodes_by_label()`: `&mut self` → `&self`
- [ ] `find_nodes_by_property()`: `&mut self` → `&self`
- [ ] `load_edge()`: `&mut self` → `&self`
- [ ] `load_edge_with_snapshot()`: `&mut self` → `&self`
- [ ] `load_edges_batch()`: `&mut self` → `&self`
- [ ] `enable_tracking()`: `&mut self` → `&self`
- [ ] `disable_tracking()`: `&mut self` → `&self`
- [ ] `take_recent_dirty_pages()`: `&mut self` → `&self`

#### Step 4: Update Pager Access (57 locations)
- [ ] `src/db/core/nodes.rs` - Update all pager accesses
- [ ] `src/db/core/edges.rs` - Update all pager accesses
- [ ] `src/db/core/graphdb.rs` - Update all pager accesses
- [ ] `src/db/core/index.rs` - Update all pager accesses
- [ ] `src/db/core/traversal.rs` - Update all pager accesses

#### Step 5: Update Cache Access (45 locations)
- [ ] `src/db/core/nodes.rs` - Update node_cache accesses
- [ ] `src/db/core/edges.rs` - Update edge_cache accesses
- [ ] `src/db/core/graphdb.rs` - Update both cache accesses

#### Step 6: Update RecordStore Usage (14 locations)
- [ ] Update `record_store()` call sites to lock pager first
- [ ] Verify RecordStore lifetime handling is correct

#### Step 7: Update Tracking Access (6 locations)
- [ ] `src/db/core/transaction_support.rs` - Update tracking_enabled/recent_dirty_pages
- [ ] `src/db/core/graphdb.rs` - Update tracking accesses

#### Step 8: Fix Compilation Errors
- [ ] Run `cargo check` iteratively
- [ ] Fix all borrowing/lifetime errors
- [ ] Ensure lock guards are dropped correctly

#### Step 9: Run Tests
- [ ] `cargo test --lib` - All unit tests pass
- [ ] `cargo test mvcc` - MVCC tests pass
- [ ] `cargo test concurrent` - Concurrent tests pass
- [ ] `cargo test` - Full test suite passes

#### Step 10: Benchmark Performance
- [ ] Run `cargo bench --bench concurrent_throughput`
- [ ] Document overhead (should be 5-10% slower)
- [ ] Verify no deadlocks in stress tests

---

## Phase 2: RwLock Migration

**Objective**: Replace `Arc<Mutex<GraphDB>>` with `Arc<RwLock<GraphDB>>` in ConcurrentGraphDB

**Estimated Effort**: 1 day  
**Risk Level**: Low (single file change, well-tested pattern)

### 2.1 Core Change

**File**: `src/db/concurrent.rs`

**Current:**
```rust
use std::sync::{Arc, Mutex};

pub struct ConcurrentGraphDB {
    inner: Arc<Mutex<GraphDB>>,
}
```

**Target:**
```rust
use std::sync::{Arc, RwLock};

pub struct ConcurrentGraphDB {
    inner: Arc<RwLock<GraphDB>>,
}
```

**Lines Changed**: 2 lines (import + struct field)

---

### 2.2 Read Operations Update

**Pattern Change:**

**Current:**
```rust
pub fn get_node(&self, node_id: NodeId) -> Result<Option<Node>> {
    let mut db = self.inner.lock().map_err(|e| {
        GraphError::InvalidArgument(format!("failed to acquire database lock: {}", e))
    })?;
    
    db.get_node_with_snapshot(node_id, self.snapshot_ts, Some(self.tx_id))
}
```

**Target:**
```rust
pub fn get_node(&self, node_id: NodeId) -> Result<Option<Node>> {
    let db = self.inner.read().map_err(|e| {
        GraphError::InvalidArgument(format!("failed to acquire read lock: {}", e))
    })?;
    
    db.get_node_with_snapshot(node_id, self.snapshot_ts, Some(self.tx_id))
}
```

**Changes**: `lock()` → `read()`, `mut db` → `db`

**Methods to Update (Read Operations)**:
- [ ] `ConcurrentTransaction::get_node()` - Use `.read()`
- [ ] `ConcurrentTransaction::get_edge()` - Use `.read()`
- [ ] Any future read-only methods

**Total**: ~2-3 methods

---

### 2.3 Write Operations Update

**Pattern Change:**

**Current:**
```rust
pub fn add_node(&mut self, node: Node) -> Result<NodeId> {
    let mut db = self.db.lock().map_err(|e| {
        GraphError::InvalidArgument(format!("failed to acquire database lock: {}", e))
    })?;
    
    let (node_id, version_ptr) = db.add_node_internal(node, self.tx_id, 0)?;
    // ... rest of method
}
```

**Target:**
```rust
pub fn add_node(&mut self, node: Node) -> Result<NodeId> {
    let mut db = self.db.write().map_err(|e| {
        GraphError::InvalidArgument(format!("failed to acquire write lock: {}", e))
    })?;
    
    let (node_id, version_ptr) = db.add_node_internal(node, self.tx_id, 0)?;
    // ... rest of method
}
```

**Changes**: `lock()` → `write()`

**Methods to Update (Write Operations)**:
- [ ] `ConcurrentTransaction::add_node()` - Use `.write()`
- [ ] `ConcurrentTransaction::add_edge()` - Use `.write()`
- [ ] `ConcurrentTransaction::commit()` - Use `.write()`
- [ ] `ConcurrentGraphDB::begin_transaction()` - Use `.write()` (allocates tx_id)

**Total**: ~4-5 methods

---

### 2.4 Close Method Update

**Current:**
```rust
pub fn close(self) -> Result<()> {
    let db = Arc::try_unwrap(self.inner)
        .map_err(|_| {
            GraphError::InvalidArgument(
                "cannot close database: active references exist".into(),
            )
        })?
        .into_inner()
        .map_err(|e| {
            GraphError::InvalidArgument(format!("failed to acquire database lock: {}", e))
        })?;

    db.close()
}
```

**Target:**
```rust
pub fn close(self) -> Result<()> {
    let db = Arc::try_unwrap(self.inner)
        .map_err(|_| {
            GraphError::InvalidArgument(
                "cannot close database: active references exist".into(),
            )
        })?
        .into_inner()
        .map_err(|e| {
            GraphError::InvalidArgument(format!("failed to acquire write lock: {}", e))
        })?;

    db.close()
}
```

**Changes**: Error message update (cosmetic)

---

### 2.5 Phase 2 Testing Strategy

**Unit Tests**:
- All existing concurrent tests must pass
- Verify snapshot isolation still works
- Test concurrent reads (multiple threads reading simultaneously)

**Performance Tests**:
- Run `cargo bench --bench concurrent_throughput`
- **Expected results**:
  - Read @ 50 threads: 560K → 3-5M ops/sec (5-9x improvement)
  - Write @ 20 threads: ~16K ops/sec (unchanged)
  - Mixed @ 10 threads: 65K → 300-500K ops/sec (4-7x improvement)

**Stress Tests**:
- 50+ concurrent reader threads for 1 hour
- Mixed read/write workload for 24 hours
- Monitor for:
  - Deadlocks (should not occur)
  - Lock poisoning (should not occur)
  - Data corruption (run integrity checks)

---

### 2.6 Phase 2 Implementation Checklist

#### Step 1: Update Imports
- [ ] Change `use std::sync::{Arc, Mutex};` → `use std::sync::{Arc, RwLock};`

#### Step 2: Update Struct
- [ ] Change `inner: Arc<Mutex<GraphDB>>` → `inner: Arc<RwLock<GraphDB>>`
- [ ] Change `db: Arc<Mutex<GraphDB>>` in ConcurrentTransaction → `db: Arc<RwLock<GraphDB>>`

#### Step 3: Update Initialization
- [ ] Change `Arc::new(Mutex::new(db))` → `Arc::new(RwLock::new(db))`

#### Step 4: Update Read Operations
- [ ] `get_node()`: Change `.lock()` → `.read()`, remove `mut`
- [ ] `get_edge()`: Change `.lock()` → `.read()`, remove `mut`

#### Step 5: Update Write Operations
- [ ] `begin_transaction()`: Change `.lock()` → `.write()`
- [ ] `add_node()`: Change `.lock()` → `.write()`
- [ ] `add_edge()`: Change `.lock()` → `.write()`
- [ ] `commit()`: Change `.lock()` → `.write()`

#### Step 6: Update Error Messages
- [ ] Update error messages from "database lock" → "read lock" / "write lock"

#### Step 7: Run Tests
- [ ] `cargo test concurrent` - All concurrent tests pass
- [ ] `cargo test mvcc` - MVCC tests pass
- [ ] `cargo test` - Full test suite passes

#### Step 8: Benchmark Performance
- [ ] Run `cargo bench --bench concurrent_throughput`
- [ ] Verify 5-10x read improvement
- [ ] Document final performance numbers

#### Step 9: Stress Test
- [ ] Run 24-hour stress test with 50 threads
- [ ] Verify no deadlocks, no corruption
- [ ] Run database integrity check after stress test

---

## Combined Effort Estimate

| Phase | Task | Estimated Time |
|-------|------|----------------|
| **Phase 1** | Update GraphDB struct | 2 hours |
| | Update method signatures | 2 hours |
| | Update pager accesses (57 locations) | 4 hours |
| | Update cache accesses (45 locations) | 3 hours |
| | Update tracking accesses (6 locations) | 1 hour |
| | Update RecordStore calls (14 locations) | 2 hours |
| | Fix compilation errors | 3 hours |
| | Testing & debugging | 4 hours |
| | **Phase 1 Total** | **21 hours (2.5 days)** |
| **Phase 2** | Update ConcurrentGraphDB | 2 hours |
| | Testing & benchmarking | 3 hours |
| | Stress testing | 3 hours |
| | **Phase 2 Total** | **8 hours (1 day)** |
| **Grand Total** | | **29 hours (3.5 days)** |

---

## Risk Mitigation

### Risk 1: Deadlocks
**Mitigation**:
- Establish clear lock ordering (documented above)
- Keep lock critical sections minimal
- Add `#[cfg(debug_assertions)]` deadlock detection
- Comprehensive testing with many threads

### Risk 2: Performance Regression in Phase 1
**Mitigation**:
- Accept 5-10% overhead as temporary (Phase 2 fixes it)
- Document baseline vs Phase 1 vs Phase 2 performance
- If >10% overhead, investigate lock contention hotspots

### Risk 3: Lock Poisoning
**Mitigation**:
- Treat poisoned locks as corruption errors
- Add recovery strategy for non-critical caches
- Log poisoning events for debugging

### Risk 4: Subtle Race Conditions
**Mitigation**:
- Extensive stress testing (24-hour runs)
- Use ThreadSanitizer if available
- Run with `RUST_TEST_THREADS=1` to isolate issues
- Add assertions for invariants

---

## Success Criteria

### Phase 1 Complete When:
- [ ] All tests pass (`cargo test`)
- [ ] Performance overhead <10% on single-threaded benchmarks
- [ ] No deadlocks in 1-hour stress test
- [ ] Code review approved

### Phase 2 Complete When:
- [ ] All tests pass (`cargo test`)
- [ ] Read throughput >3M ops/sec @ 50 threads
- [ ] Mixed workload >300K ops/sec @ 10 threads
- [ ] 24-hour stress test passes without issues
- [ ] Integrity check passes after stress test

---

## Rollback Plan

If Phase 1 or Phase 2 encounters critical issues:

1. **Revert commits** to last known good state
2. **Document issues** in GitHub issue
3. **Keep changes in feature branch** for future work
4. **Update docs** to note optimization is deferred

**Feature Flag Alternative**:
```rust
#[cfg(feature = "rwlock-optimization")]
pub struct ConcurrentGraphDB {
    inner: Arc<RwLock<GraphDB>>,
}

#[cfg(not(feature = "rwlock-optimization"))]
pub struct ConcurrentGraphDB {
    inner: Arc<Mutex<GraphDB>>,
}
```

This allows gradual rollout and easy A/B testing.

---

## Next Steps

1. **Review this plan** with team/maintainer
2. **Create feature branch**: `git checkout -b rwlock-phase1-interior-mutability`
3. **Begin Phase 1 implementation** following checklist
4. **Commit incrementally** as major milestones complete
5. **Open PR after Phase 1** for review before Phase 2
6. **Merge Phase 2** after performance validation

---

## Questions for Review

1. Should we implement RecordStore Option A or Option B? (Recommendation: B)
2. Should we add feature flag for easy rollback? (Recommendation: Yes)
3. Should we benchmark after each major step or only at phase completion? (Recommendation: Each step)
4. Should we run ThreadSanitizer in CI? (Recommendation: If available)
5. What's the acceptable performance regression threshold? (Recommendation: 10%)

---

**Document Version**: 1.0  
**Last Updated**: 2025-10-28  
**Author**: OpenCode Assistant  
**Status**: Ready for Implementation
