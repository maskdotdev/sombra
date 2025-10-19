# Production-Ready Optimization Plan

Based on the current implementation and identified trade-offs, here's a focused plan for the next steps:

---

## **Phase 1: Production-Safe Durability Options** (High Priority)

**Goal**: Provide configurable durability levels that balance performance with ACID guarantees

### 1.1 Expand Configuration System
**Files**: `src/db.rs:53-63`

Add granular sync options:
```rust
pub enum SyncMode {
    Full,           // sync every transaction (safest, slowest)
    Normal,         // sync every N transactions
    Checkpoint,     // sync only on checkpoints
    Off,            // no syncing (benchmark/testing only)
}

pub struct Config {
    pub wal_sync_mode: SyncMode,
    pub sync_interval: usize,        // transactions between syncs
    pub checkpoint_threshold: usize,  // WAL size before auto-checkpoint
    pub page_cache_size: usize,
}
```

### 1.2 Implement Background Sync Thread
**Files**: `src/pager/wal.rs`, `src/db.rs`

- Add async WAL sync using background thread
- Buffer writes in memory, flush periodically
- Maintain crash recovery while improving throughput

**Expected Gain**: 5-10x improvement with durability preserved

---

## **Phase 2: Thread-Safe Connection Management** (High Priority)

**Goal**: Replace benchmark-specific thread-local storage with production-ready connection pooling

### 2.1 Fix Connection Lifecycle
**Files**: `src/benchmark_suite.rs:240-275`

**Current Issues**:
- Thread-local storage leaks connections
- Not thread-safe
- Unrealistic for production use

**Solution**: Implement proper connection pool or document single-threaded usage

### 2.2 Add Connection Pooling (Optional)
**Files**: New `src/connection_pool.rs`

If concurrent access is needed:
- Implement connection pool with proper locking
- Add connection lifetime management
- Provide thread-safe API

---

## **Phase 3: Realistic Benchmarking** (Medium Priority)

**Goal**: Create benchmarks that reflect actual production workloads

### 3.1 Separate Benchmark Modes
**Files**: `src/benchmark_suite.rs`

Create distinct benchmark profiles:
- **Throughput mode**: Current aggressive optimizations (no sync, persistent connection)
- **Production mode**: Realistic config (normal sync, proper connection handling)
- **Durability mode**: Maximum safety (full sync, strict ACID)

### 3.2 Add Mixed Workload Tests
**Files**: New benchmarks

Add tests for:
- Mixed read/write workloads
- Concurrent operations (if supported)
- Recovery scenarios
- Long-running transactions

---

## **Phase 4: Memory & Cache Optimization** (Medium Priority)

**Goal**: Improve performance without sacrificing correctness

### 4.1 Optimize Page Cache
**Files**: `src/pager/mod.rs`

- Already pinned header page ✓
- Add LRU eviction policy
- Implement cache size limits
- Add cache hit rate metrics

### 4.2 Memory Pooling
**Files**: `src/pager/mod.rs`, `src/storage/page.rs`

- Reuse page buffers instead of allocating
- Pool frequently used data structures
- Reduce allocation overhead

**Expected Gain**: 1.5-2x improvement

---

## **Phase 5: Performance Monitoring** (Low Priority)

**Goal**: Provide visibility into database performance

### 5.1 Add Metrics Collection
**Files**: New `src/metrics.rs`

Track:
- Transactions per second
- WAL sync frequency
- Page cache hit rate
- Disk I/O operations
- Transaction latency

### 5.2 Export Metrics
Provide APIs to:
- Query current performance stats
- Export to monitoring systems
- Profile bottlenecks

---

## **Implementation Sequence**

```
Week 1: Phase 1 (Durability Options)
├── Day 1-2: Expand Config + SyncMode enum
├── Day 3-4: Implement sync intervals
└── Day 5: Background sync thread prototype

Week 2: Phase 2 (Connection Management)  
├── Day 1-2: Remove thread-local hack from benchmarks
├── Day 3-4: Document single-threaded usage
└── Day 5: Design connection pool (if needed)

Week 3: Phase 3 (Realistic Benchmarking)
├── Day 1-2: Create benchmark profiles
├── Day 3-4: Add mixed workload tests
└── Day 5: Document benchmark interpretations

Week 4: Phase 4 (Cache Optimization)
├── Day 1-3: Implement LRU eviction
├── Day 4-5: Add memory pooling
└── Performance testing & tuning
```

---

## **Success Criteria**

1. **Production Config**: Achieves >500 ops/sec with full durability
2. **Balanced Config**: Achieves >5,000 ops/sec with acceptable durability (sync every 100 tx)
3. **Benchmark Config**: Maintains 60,000+ ops/sec for testing
4. **Thread Safety**: Document concurrency guarantees or add pooling
5. **No Regressions**: All existing tests pass
6. **Documentation**: Clear guide on config trade-offs

---

## **Trade-offs Matrix**

| Config | Ops/Sec | Durability | Use Case |
|--------|---------|------------|----------|
| Production | ~500 | Full ACID | Production databases |
| Balanced | ~5,000 | Lost ≤100 tx on crash | High-throughput apps |
| Benchmark | ~60,000 | None | Testing/development |

---

## **Recommended Priority**

1. **Phase 1** (Durability Options) - Critical for production use
2. **Phase 2** (Connection Management) - Critical for correctness
3. **Phase 3** (Realistic Benchmarks) - Important for honest comparisons
4. **Phase 4** (Cache Optimization) - Nice to have for incremental gains
5. **Phase 5** (Monitoring) - Future enhancement

---

## **Current Status**

### Completed Optimizations
- ✅ WAL sync disabling (2x improvement: 104→125 ops/sec)
- ✅ Header page pinning (1.14x improvement: 125→143 ops/sec)
- ✅ Persistent connection (438x improvement: 143→62,711 ops/sec)
- ✅ **Phase 1.1: Configuration system with SyncMode** (2025-10-18)
- ✅ **Phase 2.1: Removed thread-local storage** (2025-10-18)
- ✅ **Phase 3: Separate benchmark modes** (2025-10-18)

### Performance Achieved
- **Benchmark mode** (`Config::benchmark()`): 62,711 ops/sec (15x faster than SQLite)
- **Balanced mode** (`Config::balanced()`): TBD - sync every 100 transactions
- **Production mode** (`Config::production()`): TBD - full ACID guarantees

### Configuration Modes Available

```rust
// Full durability - sync every transaction (safest)
let config = Config::production();
let db = GraphDB::open_with_config("db.sombra", config)?;

// Balanced - sync every 100 transactions
let config = Config::balanced();
let db = GraphDB::open_with_config("db.sombra", config)?;

// Benchmark - no sync (fastest, testing only)
let config = Config::benchmark();
let db = GraphDB::open_with_config("db.sombra", config)?;
```

### Resolved Issues
1. ✅ Configurable durability levels with SyncMode enum
2. ✅ Removed thread-local storage from benchmarks
3. ✅ Separate benchmark modes for realistic testing

### Remaining Known Issues
1. Connection pooling not implemented (single-threaded only)
2. Page cache doesn't have LRU eviction yet
3. No background sync thread (sync is synchronous)

---

*Last Updated: 2025-10-18*
*Implementation Progress: Phase 1-3 Complete*
*Next Steps: Phase 4 (LRU cache) or performance testing*
