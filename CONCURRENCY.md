# Concurrency Summary

Be extremely concise. Sacrifice grammar for the sake of concision.

FOLLOW THESE RULES NO MATTER WHAT:
Rule #1: NO PANICS. Use Result<T, E>.

Never use .unwrap() or .expect() for any error that can occur at runtime (e.g., file not found, disk full, parse error, record not found).

Use the ? operator to propagate errors up the call stack.

Only use .expect() to assert logical invariants that you (the programmer) guarantee are true. A panic here signals a programmer bug, not a user or runtime error.

Define a Custom Error Enum with thiserror.

Don't just return Box<dyn Error>.

Use the thiserror crate to create a rich, custom Error enum.

This allows consumers of your database to programmatically match on and handle specific errors (e.g., DatabaseError::NotFound, DatabaseError::Io).

Isolate and Document unsafe Code.

Keep unsafe blocks as small as humanly possible.

Wrap them in a safe, public API.

Use # Safety doc comments to rigorously explain why the code is safe and what invariants it must uphold.

Handle Concurrency Correctly.

Prefer Arc<RwLock<T>> over Arc<Mutex<T>> for read-heavy workloads, as it allows multiple concurrent readers.

If using async, you must use the async-aware versions (e.g., tokio::sync::Mutex). Never hold a std::sync::Mutex across an .await point.

Pay close attention to Send and Sync traits to ensure thread safety.

Automate Your Safety Net (CI/Tooling).

Clippy: Run cargo clippy -- -D warnings in your CI to treat all lints and warnings as build-breaking errors.


## Summary of Current State

**The Core Problem Identified:**
- Phase 3B implemented lock-free page cache successfully ✅
- BUT it's hidden behind `Arc<RwLock<GraphDB>>` in ConcurrentGraphDB ❌
- Result: **Negative scaling** (87% worse at 32 threads vs 1 thread)

**Architecture Bottleneck:**
```
ConcurrentGraphDB::begin_transaction() 
  → db.write()  ← GLOBAL LOCK (line 163 in concurrent.rs)
```

Every operation, even pure reads, grabs the global RwLock, defeating all lock-free optimizations underneath.

---

## My Recommendations for Concurrent-First Architecture

Based on my analysis, here are my answers to the design questions from the previous session:

### 1. **Performance vs Correctness Priority**
**Answer: A) Absolute correctness with simpler pessimistic locking (Phase 1), then optimize to B)**

**Rationale:**
- Start with correctness: Make GraphDB internally thread-safe with clear, auditable locks
- Phase 1: Remove outer RwLock, add fine-grained locks inside GraphDB
- Phase 2: Profile and optimize hot paths to optimistic concurrency
- This matches your "defensive coding" requirement

### 2. **Index Update Strategy**
**Answer: B) Keep them immediately consistent (synchronous updates)**

**Rationale:**
- Snapshot isolation requires indexes to be consistent with transaction snapshots
- Eventually consistent indexes break MVCC guarantees
- We can make them concurrent (DashMap-based) while keeping them synchronous
- Example: `node_index: Arc<DashMap<NodeId, RecordPointer>>`

### 3. **Error Handling Philosophy**
**Answer: A) Return error to caller, let them retry**

**Rationale:**
- Matches "no panics" requirement - all failures return Results
- Auto-retry hides errors and can mask bugs
- Application layer knows best when/how to retry
- Clear error messages guide users

### 4. **Backwards Compatibility**
**Answer: B) Make GraphDB itself concurrent-first, keep wrapper thin**

**Rationale:**
- `GraphDB` already has lock-free components (pager, caches, timestamp oracle)
- Making GraphDB thread-safe simplifies the architecture
- `ConcurrentGraphDB` becomes a thin alias: `Arc<GraphDB>` (no RwLock!)
- Single-threaded users can still use `GraphDB` directly

### 5. **Transaction Isolation Model**
**Answer: True parallel transaction execution with MVCC isolation**

**Rationale:**
- MVCC is already implemented (timestamp oracle, version chains)
- Goal: Multiple transactions reading/writing simultaneously
- Each sees its own snapshot timestamp
- Conflicts detected at commit time

---

## Proposed Concurrent-First Architecture

### Core Principle: Interior Mutability via Concurrent Data Structures

Instead of wrapping GraphDB in `RwLock`, make its internals thread-safe:

```rust
pub struct GraphDB {
    // === LOCK-FREE STORAGE ===
    pager: Arc<LockFreePageCache>,              // ✅ Already done (Phase 3B)
    
    // === CONCURRENT INDEXES ===
    node_index: Arc<DashMap<NodeId, RecordPointer>>,
    edge_index: Arc<DashMap<EdgeId, RecordPointer>>,
    label_index: Arc<DashMap<String, BTreeSet<NodeId>>>,  // BTreeSet: copy-on-write
    property_indexes: Arc<DashMap<(String, String), BTreeMap<IndexableValue, BTreeSet<NodeId>>>>,
    
    // === ADJACENCY (Concurrent) ===
    outgoing_adjacency: Arc<DashMap<NodeId, Vec<EdgeId>>>,
    incoming_adjacency: Arc<DashMap<NodeId, Vec<EdgeId>>>,
    
    // === CACHES ===
    node_cache: Arc<ConcurrentLruCache<NodeId, Node>>,    // ✅ Already concurrent
    edge_cache: Arc<ConcurrentLruCache<EdgeId, Edge>>,    // ✅ Already concurrent
    outgoing_neighbors_cache: Arc<DashMap<NodeId, Vec<NodeId>>>,
    incoming_neighbors_cache: Arc<DashMap<NodeId, Vec<NodeId>>>,
    
    // === MVCC COORDINATION ===
    mvcc_tx_manager: Arc<RwLock<MvccTransactionManager>>, // Fine-grained lock
    timestamp_oracle: Arc<TimestampOracle>,               // ✅ Already lock-free (AtomicU64)
    
    // === ATOMICS ===
    next_tx_id: AtomicU64,
    epoch: AtomicU64,                                     // ✅ Already atomic
    tracking_enabled: AtomicBool,                         // ✅ Already atomic
    transactions_since_sync: AtomicUsize,
    transactions_since_checkpoint: AtomicUsize,
    
    // === WRITE COORDINATION ===
    write_coordinator: Arc<Mutex<WriteCoordinator>>,      // WAL, checkpoints, header writes
    recent_dirty_pages: Arc<Mutex<Vec<PageId>>>,         // ✅ Already Mutex
    
    // === BACKGROUND TASKS ===
    group_commit_state: Option<Arc<Mutex<GroupCommitState>>>,  // ✅ Already Mutex
    bg_gc_state: Option<Arc<Mutex<BackgroundGcState>>>,        // ✅ Already Mutex
    
    // === METADATA ===
    pages_with_free_slots: Arc<RwLock<BTreeSet<PageId>>>,     // Read-heavy, use RwLock
    
    // === IMMUTABLE ===
    path: PathBuf,
    config: Config,
    page_cache_hint: Arc<PageCacheHint>,
    lock_file: Option<File>,
}
```

### Key Changes from Current

**REMOVE:**
- ❌ `Arc<RwLock<GraphDB>>` wrapper in ConcurrentGraphDB
- ❌ `Option<TxId> active_transaction` (single active tx model)

**CHANGE:**
- ✅ HashMap → DashMap for all indexes
- ✅ TxId → AtomicU64
- ✅ usize counters → AtomicUsize

**ADD:**
- ✅ `WriteCoordinator` struct to coordinate WAL/checkpoint writes

---

## WriteCoordinator Design

```rust
struct WriteCoordinator {
    header: HeaderState,
    wal_writer: WalWriter,
    pending_checkpoints: VecDeque<CheckpointRequest>,
}

impl WriteCoordinator {
    fn commit_transaction(&mut self, tx_id: TxId, dirty_pages: &[PageId]) -> Result<()> {
        // Coordinate: WAL write → Header update → Sync
        // This MUST be serialized (hence Mutex)
    }
    
    fn checkpoint(&mut self) -> Result<()> {
        // Coordinate: Flush dirty pages → Truncate WAL → Update header
    }
}
```

This isolates the **serialization point** to WAL writes only, not the entire database.

---

## ConcurrentGraphDB Becomes Thin Wrapper

```rust
pub struct ConcurrentGraphDB {
    inner: Arc<GraphDB>,  // NO RwLock!
}

impl ConcurrentGraphDB {
    pub fn begin_transaction(&self) -> Result<ConcurrentTransaction> {
        // Allocate TX ID (atomic)
        let tx_id = self.inner.next_tx_id.fetch_add(1, Ordering::SeqCst);
        
        // Get snapshot timestamp (lock-free via AtomicU64)
        let snapshot_ts = self.inner.timestamp_oracle.allocate_read_timestamp();
        
        // Register in MVCC manager (fine-grained RwLock)
        let context = self.inner.mvcc_tx_manager
            .write()
            .map_err(...)?
            .begin_transaction(tx_id, snapshot_ts)?;
        
        Ok(ConcurrentTransaction {
            db: Arc::clone(&self.inner),  // Direct access!
            tx_id,
            snapshot_ts: context.snapshot_ts,
            ...
        })
    }
}
```

**No global lock!** Transactions can now access lock-free components concurrently.

---

## ConcurrentTransaction Operations

```rust
impl ConcurrentTransaction {
    pub fn add_node(&mut self, node: Node) -> Result<NodeId> {
        // 1. Allocate node ID (from index or atomic counter)
        let node_id = self.allocate_node_id()?;
        
        // 2. Write to lock-free page cache
        let version_ptr = self.db.pager.with_pager_write(|pager| {
            let mut record_store = RecordStore::new(pager);
            record_store.create_node_version(node, self.tx_id, 0)
        })?;
        
        // 3. Update concurrent indexes (lock-free)
        self.db.node_index.insert(node_id, version_ptr);
        for label in &node.labels {
            self.db.label_index
                .entry(label.clone())
                .or_insert_with(BTreeSet::new)
                .insert(node_id);  // Need copy-on-write here
        }
        
        // 4. Track local state
        self.created_versions.push(version_ptr);
        
        Ok(node_id)
    }
    
    pub fn get_node(&self, node_id: NodeId) -> Result<Option<Node>> {
        // 1. Lookup in concurrent index (lock-free read)
        let ptr = self.db.node_index.get(&node_id)?;
        
        // 2. Read from lock-free page cache
        self.db.pager.with_pager_read(|pager| {
            let record_store = RecordStore::new(pager);
            record_store.load_node_with_snapshot(ptr, self.snapshot_ts, Some(self.tx_id))
        })
    }
    
    pub fn commit(self) -> Result<()> {
        // 1. Allocate commit timestamp (lock-free)
        let commit_ts = self.db.timestamp_oracle.allocate_commit_timestamp();
        
        // 2. Update version timestamps (lock-free page cache)
        self.db.pager.with_pager_write(|pager| {
            for version_ptr in &self.created_versions {
                update_version_commit_timestamp(pager, *version_ptr, commit_ts)?;
            }
            Ok(())
        })?;
        
        // 3. SERIALIZED: WAL write + header update
        self.db.write_coordinator.lock()
            .map_err(...)?
            .commit_transaction(self.tx_id, &self.dirty_pages)?;
        
        // 4. Complete in MVCC manager (fine-grained lock)
        self.db.mvcc_tx_manager.write()
            .map_err(...)?
            .complete_commit(self.tx_id, commit_ts)?;
        
        Ok(())
    }
}
```

---

## Synchronization Points Summary

| Component | Synchronization | Rationale |
|-----------|----------------|-----------|
| **Page Cache** | Lock-free (DashMap) | ✅ Already done (Phase 3B) |
| **Indexes** | DashMap (lock-free) | High read rate, minimal contention |
| **Timestamp Oracle** | AtomicU64 | ✅ Already done, proven lock-free |
| **MVCC Tx Manager** | RwLock | Track active txs (reads >> writes) |
| **WAL/Checkpoint** | Mutex | MUST serialize (disk I/O) |
| **Counters** | Atomics | Simple, fast |
| **Caches** | ConcurrentLru | ✅ Already done |

**Key Insight:** Only 2 locks in entire system:
1. `RwLock<MvccTransactionManager>` - fine-grained, read-heavy
2. `Mutex<WriteCoordinator>` - isolated to commit path

---

## Migration Plan (4 Phases)

### Phase 1: Prepare GraphDB (1-2 days)
- Make all indexes use `Arc<DashMap<...>>`
- Convert counters to atomics
- Extract `WriteCoordinator` struct
- Tests: Verify single-threaded correctness

### Phase 2: Remove Outer RwLock (1 day)
- Change `ConcurrentGraphDB::inner` from `Arc<RwLock<GraphDB>>` to `Arc<GraphDB>`
- Update `begin_transaction()` to not grab global lock
- Update transaction operations to use concurrent indexes
- Tests: Verify MVCC isolation still works

### Phase 3: Optimize Hot Paths (1-2 days)
- Profile concurrent workloads
- Optimize label_index copy-on-write strategy
- Add batch operations if needed
- Tests: Concurrent benchmarks show improvement

### Phase 4: Validate & Document (1 day)
- Re-run Phase 3B benchmarks → expect 50-80% improvement
- Stress test with 100+ concurrent transactions
- Update MVCC_PRODUCTION_GUIDE.md
- Document concurrency guarantees

---

## Expected Performance

### Baseline (Current):
```
 1 thread:  2.4M ops/sec
 2 threads: 2.0M ops/sec (0.70x - worse!)
32 threads: 392K ops/sec (0.13x - much worse!)
```

### After Concurrent-First (Target):
```
 1 thread:  2.4M ops/sec (baseline)
 2 threads: 4.0M ops/sec (1.7x speedup)
 8 threads: 14M ops/sec (5.8x speedup)
16 threads: 22M ops/sec (9.2x speedup)
32 threads: 28M ops/sec (11.7x speedup)
```

Based on lock-free page cache having ~80% cache hit rate + DashMap scalability.

---

## Critical Decisions Needed from You

Before I proceed to create the detailed implementation plan:

1. **DashMap for BTreeSet values**: The `label_index: HashMap<String, BTreeSet<NodeId>>` needs concurrent updates. Options:
   - A) **Wrap BTreeSet in RwLock**: `DashMap<String, Arc<RwLock<BTreeSet<NodeId>>>>`
   - B) **Use lock-free set**: Replace BTreeSet with `dashmap::DashSet` (unordered)
   - C) **Copy-on-write**: Clone BTreeSet on every update (simple but potentially expensive)
   
   **My recommendation: A** (RwLock per label is fine-grained enough)

2. **Node/Edge ID allocation**: Currently sequential from index. Options:
   - A) **Keep BTreeIndex with lock**: Wrap in `RwLock<BTreeIndex>`
   - B) **Atomic counter**: Simpler but loses sparse ID support
   - C) **Hybrid**: Atomic counter + background reclamation
   
   **My recommendation: A** (preserves existing behavior)

3. **Testing strategy**: Concurrent correctness is hard. Should we:
   - A) Use `loom` for model checking (catches race conditions, slow)
   - B) Stress tests only (faster, might miss edge cases)
   - C) Both (most thorough)
   
   **My recommendation: C** (critical for database correctness)

4. **Implementation timeline**: You mentioned "no complexity limits" but also maintainability:
   - A) **Fast track (1 week)**: Minimal changes, some rough edges
   - B) **Thorough (2-3 weeks)**: Proper design, full test coverage, documentation
   - C) **Production-ready (4+ weeks)**: Plus stress testing, profiling, edge case handling
   
   **My recommendation: B** (balances speed with quality)

Please let me know your preferences on these 4 decisions, and I'll create the detailed implementation plan with specific file changes, code examples, and testing strategies.

---

**Note:** I'm currently in READ-ONLY mode per the system reminder, so I'm only analyzing and planning. Once you approve the architecture direction, I can proceed with implementation.
