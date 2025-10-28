# RwLock Optimization - Implementation Checklist

Quick reference checklist for tracking implementation progress.
See `RWLOCK_IMPLEMENTATION_PLAN.md` for detailed instructions.

---

## Phase 1: Interior Mutability [IN PROGRESS]

### Struct Changes (graphdb.rs)
- [ ] Add imports: `Mutex`, `AtomicBool`, `Ordering`
- [ ] `pager: Pager` → `pager: Mutex<Pager>`
- [ ] `node_cache` → `Mutex<LruCache<..>>`
- [ ] `edge_cache` → `Mutex<LruCache<..>>`
- [ ] `tracking_enabled` → `AtomicBool`
- [ ] `recent_dirty_pages` → `Mutex<Vec<PageId>>`

### Initialization Updates
- [ ] Wrap Pager in `Mutex::new()`
- [ ] Wrap caches in `Mutex::new()`
- [ ] Wrap recent_dirty_pages in `Mutex::new()`
- [ ] `AtomicBool::new(false)` for tracking_enabled

### Method Signatures (&mut self → &self)
- [ ] `get_node()`
- [ ] `get_node_with_snapshot()`
- [ ] `get_nodes_by_label()`
- [ ] `find_nodes_by_property()`
- [ ] `load_edge()`
- [ ] `load_edge_with_snapshot()`
- [ ] `load_edges_batch()`
- [ ] `enable_tracking()`
- [ ] `disable_tracking()`
- [ ] `take_recent_dirty_pages()`

### Pager Access Updates (57 locations)
- [ ] `src/db/core/nodes.rs` (~20 locations)
- [ ] `src/db/core/edges.rs` (~15 locations)
- [ ] `src/db/core/graphdb.rs` (~10 locations)
- [ ] `src/db/core/index.rs` (~5 locations)
- [ ] `src/db/core/traversal.rs` (~7 locations)

### Cache Access Updates (45 locations)
- [ ] `src/db/core/nodes.rs` - node_cache (~20 locations)
- [ ] `src/db/core/edges.rs` - edge_cache (~12 locations)
- [ ] `src/db/core/graphdb.rs` - both caches (~13 locations)

### RecordStore Updates (14 locations)
- [ ] Update all `record_store()` call sites

### Tracking Access Updates (6 locations)
- [ ] `src/db/core/transaction_support.rs` (~4 locations)
- [ ] `src/db/core/graphdb.rs` (~2 locations)

### Compilation & Testing
- [ ] `cargo check` passes
- [ ] `cargo test --lib` passes
- [ ] `cargo test mvcc` passes
- [ ] `cargo test concurrent` passes
- [ ] `cargo test` passes (full suite)

### Benchmarking
- [ ] Run `cargo bench --bench concurrent_throughput`
- [ ] Document Phase 1 overhead (target: <10%)
- [ ] Save baseline for Phase 2 comparison

### Stress Testing
- [ ] 1-hour multi-threaded stress test
- [ ] No deadlocks detected
- [ ] No lock poisoning
- [ ] Integrity check passes

---

## Phase 2: RwLock Migration [NOT STARTED]

### Core Changes (concurrent.rs)
- [ ] Change import: `Mutex` → `RwLock`
- [ ] `Arc<Mutex<GraphDB>>` → `Arc<RwLock<GraphDB>>`
- [ ] Update initialization: `Mutex::new()` → `RwLock::new()`

### Read Operations (use .read())
- [ ] `ConcurrentTransaction::get_node()` - `.lock()` → `.read()`
- [ ] `ConcurrentTransaction::get_edge()` - `.lock()` → `.read()`
- [ ] Remove `mut` from read operation bindings

### Write Operations (use .write())
- [ ] `ConcurrentGraphDB::begin_transaction()` - `.lock()` → `.write()`
- [ ] `ConcurrentTransaction::add_node()` - `.lock()` → `.write()`
- [ ] `ConcurrentTransaction::add_edge()` - `.lock()` → `.write()`
- [ ] `ConcurrentTransaction::commit()` - `.lock()` → `.write()`

### Error Messages
- [ ] Update "database lock" → "read lock" / "write lock"

### Testing
- [ ] `cargo test concurrent` passes
- [ ] `cargo test mvcc` passes
- [ ] `cargo test` passes (full suite)

### Performance Benchmarking
- [ ] Run `cargo bench --bench concurrent_throughput`
- [ ] Verify read throughput: >3M ops/sec @ 50 threads
- [ ] Verify mixed workload: >300K ops/sec @ 10 threads
- [ ] Document improvement vs Phase 1 baseline

### Stress Testing
- [ ] 24-hour stress test (50+ threads)
- [ ] No deadlocks
- [ ] No lock poisoning
- [ ] No data corruption
- [ ] Integrity check passes

---

## Performance Targets

### Baseline (Current - Mutex)
- Read @ 1 thread: 2.6M ops/sec
- Read @ 50 threads: 560K ops/sec
- Mixed @ 1 thread: 285K ops/sec
- Mixed @ 2 threads: 65K ops/sec

### Phase 1 Target (Interior Mutability)
- Acceptable: 5-10% slower than baseline
- Read @ 1 thread: ~2.3M ops/sec (acceptable)

### Phase 2 Target (RwLock)
- Read @ 50 threads: 3-5M ops/sec (5-9x improvement)
- Write @ 20 threads: ~16K ops/sec (unchanged)
- Mixed @ 10 threads: 300-500K ops/sec (4-7x improvement)

---

## Status Legend
- [ ] Not Started
- [⏳] In Progress
- [✅] Complete
- [❌] Blocked

---

**Last Updated**: Phase 1 Not Started  
**Current Branch**: mvcc (commit: dd01ecc)  
**Next Step**: Create feature branch `rwlock-phase1-interior-mutability`
