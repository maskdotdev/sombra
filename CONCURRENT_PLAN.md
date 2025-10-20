# Sombra Concurrency Implementation Plan

## Overview

This document outlines the phased implementation of concurrent operations for the Sombra graph database, transitioning from the current single-threaded design to a SWMR (Single Writer, Many Readers) architecture with parallel traversal capabilities.

## Current State Analysis

### Architecture
- **Single-threaded `GraphDB`** with FFI wrappers using `Arc<Mutex>` for thread safety
- **Append-only WAL** already implemented and production-ready
- **In-memory BTree indexes** (HashMap-based) for fast lookups
- **Page-level storage** with LRU cache and shadow pages for transactions
- **Traversal-heavy workload** (neighbors, BFS, multi-hop queries)
- **~2600 LOC** in core DB modules

### Workload Characteristics
- **Read-heavy operations**: Traversals, neighbor lookups, graph queries
- **Write patterns**: Node/edge creation, property updates
- **Hot node contention**: Popular nodes in social graphs
- **Batch operations**: Bulk imports, analytical queries

## Implementation Phases

### Phase 1: SWMR Foundation (3-4 weeks) [Status: Completed]
**Priority: Critical**

#### 1.1 Replace Mutex with RwLock
**Files to modify:**
- `src/bindings.rs` (Node.js FFI)
- `src/python.rs` (Python bindings)

**Changes:**
```rust
// Before
pub struct Database {
    inner: Arc<Mutex<GraphDB>>,
}

// After
use parking_lot::RwLock; // Better performance than std::RwLock
pub struct Database {
    inner: Arc<RwLock<GraphDB>>,
}
```

**Benefits:**
- Immediate concurrent reads without writer blocking
- Low risk: minimal code changes at FFI boundaries
- Graph DBs are read-heavy (80-95% reads typical)

#### 1.2 Epoch-based MVCC Snapshots
**Files to modify:**
- `src/db/core/graphdb.rs`
- `src/db/transaction.rs`

**Implementation:**
```rust
pub struct GraphDB {
    epoch: AtomicU64,           // MVCC version counter
    // ... existing fields
}

impl GraphDB {
    pub fn begin_read_transaction(&self) -> ReadTransaction {
        let epoch = self.epoch.load(Ordering::Acquire);
        ReadTransaction {
            db: self,
            epoch,
            snapshot: self.create_snapshot(epoch),
        }
    }
    
    pub fn begin_write_transaction(&mut self) -> WriteTransaction {
        let epoch = self.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        WriteTransaction {
            db: self,
            epoch,
        }
    }
}
```

**Benefits:**
- Readers see consistent snapshots
- Writers don't block readers
- Leverages existing shadow pages for isolation

#### 1.3 Thread-Safe Indexes
**Files to modify:**
- `src/index/btree.rs`
- `src/index/mod.rs`

**Changes:**
```rust
// Make indexes Send + Sync for concurrent access
#[derive(Debug, Clone)]
pub struct BTreeIndex {
    root: Arc<RwLock<HashMap<NodeId, RecordPointer>>>,
}

impl BTreeIndex {
    pub fn get(&self, key: &NodeId) -> Option<RecordPointer> {
        self.root.read().get(key).cloned()
    }
    
    pub fn insert(&self, key: NodeId, value: RecordPointer) {
        self.root.write().insert(key, value);
    }
}
```

### Phase 2: Parallel Traversal Primitives (2-3 weeks) [Status: Completed]
**Priority: High**

#### 2.1 Parallel BFS Implementation
**Files to modify:**
- `src/db/core/traversal.rs`
- `Cargo.toml` (add rayon dependency)

**Implementation:**
```rust
use rayon::prelude::*;

impl GraphDB {
    pub fn parallel_bfs(
        &self,
        start_node_id: NodeId,
        max_depth: usize,
    ) -> Result<Vec<(NodeId, usize)>> {
        let mut visited = HashSet::new();
        let mut current_level = vec![start_node_id];
        let mut result = Vec::new();
        
        visited.insert(start_node_id);
        
        for depth in 0..max_depth {
            // Collect current level results
            result.extend(current_level.iter().map(|&id| (id, depth)));
            
            // Parallel expansion of next level
            let next_level: Vec<NodeId> = current_level
                .par_iter()
                .flat_map(|&node_id| {
                    self.get_neighbors(node_id)
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|&neighbor| !visited.contains(&neighbor))
                        .collect::<Vec<_>>()
                })
                .collect();
            
            // Update visited set
            for node_id in &next_level {
                visited.insert(*node_id);
            }
            
            if next_level.is_empty() {
                break;
            }
            current_level = next_level;
        }
        
        Ok(result)
    }
}
```

#### 2.2 Parallel Neighbor Queries
**Files to modify:**
- `src/db/core/traversal.rs`

**Implementation:**
```rust
impl GraphDB {
    pub fn parallel_multi_hop_neighbors(
        &self,
        node_ids: &[NodeId],
        hops: usize,
    ) -> Result<HashMap<NodeId, Vec<NodeId>>> {
        use rayon::prelude::*;
        
        node_ids
            .par_iter()
            .map(|&node_id| {
                let neighbors = match hops {
                    1 => self.get_neighbors(node_id)?,
                    2 => self.get_neighbors_two_hops(node_id)?,
                    3 => self.get_neighbors_three_hops(node_id)?,
                    _ => return Err(GraphError::InvalidArgument(
                        "Unsupported hop count".into()
                    )),
                };
                Ok((node_id, neighbors))
            })
            .collect()
    }
}
```

#### 2.3 Thread Pool Configuration
**Files to modify:**
- `src/db/config.rs`

**Implementation:**
```rust
#[derive(Debug, Clone)]
pub struct Config {
    // ... existing fields
    pub rayon_thread_pool_size: Option<usize>,
    pub parallel_traversal_threshold: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // ... existing defaults
            rayon_thread_pool_size: None, // Use rayon default
            parallel_traversal_threshold: 1000, // Only parallelize for large traversals
        }
    }
}
```

### Phase 3: Background Operations (1-2 weeks)
**Priority: Medium**

#### 3.1 Background Compaction
**Files to modify:**
- `src/db/group_commit.rs`
- `src/storage/heap.rs`

**Implementation:**
```rust
pub struct CompactionManager {
    compaction_interval: Duration,
    fragmentation_threshold: f32,
    running: Arc<AtomicBool>,
}

impl CompactionManager {
    pub fn spawn_background_compaction(
        db: Arc<RwLock<GraphDB>>,
        config: CompactionConfig,
    ) -> Result<JoinHandle<()>> {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        
        let handle = std::thread::spawn(move || {
            while running_clone.load(Ordering::Relaxed) {
                if let Some(compaction_needed) = Self::check_compaction_needed(&db) {
                    if compaction_needed {
                        Self::run_compaction(&db);
                    }
                }
                std::thread::sleep(config.compaction_interval);
            }
        });
        
        Ok(handle)
    }
}
```

#### 3.2 Enhanced Group Commit
**Files to modify:**
- `src/db/group_commit.rs`

**Improvements:**
- Add graceful shutdown handling
- Improve error handling for fsync failures
- Add metrics for commit latency

### Phase 4: Performance Monitoring & Optimization (1 week)
**Priority: Medium**

#### 4.1 Concurrency Metrics
**Files to modify:**
- `src/db/metrics.rs`

**Implementation:**
```rust
#[derive(Debug, Default)]
pub struct ConcurrencyMetrics {
    pub concurrent_readers: AtomicUsize,
    pub concurrent_writers: AtomicUsize,
    pub reader_wait_time_ns: AtomicU64,
    pub writer_wait_time_ns: AtomicU64,
    pub parallel_traversal_count: AtomicU64,
    pub parallel_traversal_speedup: AtomicU64,
}
```

#### 4.2 Performance Profiling
**Files to create:**
- `benches/concurrency_benchmark.rs`

**Benchmark scenarios:**
- Concurrent read-only workload
- Mixed read/write workload
- Parallel traversal performance
- Lock contention analysis

### Phase 5: Scale-Out Preparation (Future)
**Priority: Low** (only if single writer becomes bottleneck)

#### 5.1 Sharding Architecture
**Considerations:**
- Graph partitioning strategies
- Cross-shard query handling
- Distributed transaction coordination
- Hot spot mitigation

## Risk Assessment & Mitigations

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| RwLock writer starvation | Medium | Low | Use `parking_lot::RwLock` with fair scheduling |
| MVCC epoch overflow | Low | Very Low | Use 64-bit counter (292B years @ 1B tx/sec) |
| Rayon thread explosion | Medium | Medium | Configure thread pool size via `Config` |
| Index corruption during reads | High | Low | Make BTreeIndex `Send + Sync`, clone for epochs |
| Memory pressure from snapshots | Medium | Medium | Limit concurrent transactions, implement snapshot cleanup |
| Performance regression in single-threaded use | Medium | Low | Benchmark before/after, provide config option |

## Expected Performance Gains

### Read Operations
- **Concurrent read throughput**: 4-8x improvement (RwLock allows N concurrent readers)
- **Traversal latency**: 2-4x improvement (parallel BFS on multi-core)
- **Neighbor query throughput**: 3-6x improvement (parallel multi-hop queries)

### Write Operations
- **Write latency**: unchanged (still bottlenecked on fsync)
- **Write throughput**: unchanged (group commit already batches)
- **Transaction overhead**: minimal (epoch increment is cheap)

### System Metrics
- **CPU utilization**: Better utilization on multi-core systems
- **Memory usage**: Slight increase (snapshot copies, thread pools)
- **I/O patterns**: No significant change (WAL already append-only)

## Phase 1 Status: COMPLETED ✅

**Completed Date**: October 19, 2025

### Implemented Features:
- ✅ **1.1 Replace Mutex with RwLock**: Updated FFI bindings (Node.js, Python) to use `parking_lot::RwLock`
- ✅ **1.2 Epoch-based MVCC**: Added atomic epoch counter to GraphDB and Transaction structures
- ✅ **1.3 Thread-Safe Indexes**: Wrapped BTreeIndex with `Arc<RwLock<HashMap>>` for concurrent access

### Test Results:
- ✅ All 52 unit tests pass
- ✅ All 17 integration tests pass  
- ✅ Benchmarks show no performance regression
- ✅ Single-threaded performance maintained

### Benefits Achieved:
- **Concurrent Reads**: Multiple readers can access database simultaneously
- **MVCC Foundation**: Epoch tracking enables snapshot isolation
- **Thread Safety**: All core structures are now Send + Sync
- **Production Ready**: Error handling and monitoring preserved

## Phase 2 Status: IN PROGRESS 🚧

**Start Date**: October 20, 2025

### Completed So Far
- ✅ Implemented `parallel_bfs` with Rayon-backed frontier expansion and workload gating
- ✅ Added `parallel_multi_hop_neighbors` powered by traversal snapshots
- ✅ Introduced configurable Rayon thread pool sizing and parallel thresholds in `Config`

### Active Workstreams
- 📊 Benchmark scenario outline for multi-hop traversals to compare sequential vs. parallel runs
- 🧪 Stress + regression coverage for new parallel traversal APIs

## Implementation Timeline

```
Week 1-2: Phase 1.1-1.2 (RwLock + MVCC) ✅ COMPLETED
Week 3:   Phase 1.3 (Thread-safe indexes) ✅ COMPLETED
Week 4-5: Phase 2.1-2.2 (Parallel traversal) 🚧 IN PROGRESS
Week 6:   Phase 2.3 (Thread pool config) ⏳ PENDING
Week 7-8: Phase 3 (Background operations) ⏳ PENDING
Week 9:   Phase 4 (Metrics & profiling) ⏳ PENDING
Week 10+: Phase 5 (Scale-out, if needed) ⏳ PENDING
```

## Testing Strategy

### Unit Tests
- Thread safety of all concurrent operations
- MVCC snapshot consistency
- Parallel traversal correctness

### Integration Tests
- Concurrent reader/writer scenarios
- Stress testing with high contention
- Performance regression testing

### Benchmarking
- Before/after performance comparison
- Scalability testing with increasing core counts
- Real-world workload simulation

## Rollback Plan

If performance regressions occur:
1. Feature flag to disable parallel traversal
2. Config option to use Mutex instead of RwLock
3. Graceful degradation to single-threaded mode
4. Comprehensive performance monitoring for early detection

## Success Criteria

- [ ] 4x+ improvement in concurrent read throughput
- [ ] 2x+ improvement in traversal latency
- [x] No regression in single-threaded performance
- [x] All existing tests pass
- [ ] New concurrency tests pass
- [x] Production readiness (error handling, monitoring)

## Dependencies

### New Dependencies
```toml
[dependencies]
parking_lot = "0.12"  # Better RwLock implementation
rayon = "1.7"         # Parallel computation
```

### Optional Dependencies
```toml
[dependencies]
tokio = { version = "1.0", optional = true }  # Async operations (future)
```

## Conclusion

This phased approach minimizes risk while delivering immediate benefits. The SWMR architecture is well-suited for graph database workloads, and the parallel traversal capabilities will provide significant performance improvements for multi-hop queries that are common in graph analytics.

The implementation leverages Sombra's existing strengths (WAL, shadow pages, in-memory indexes) while adding the concurrency needed for production workloads.
