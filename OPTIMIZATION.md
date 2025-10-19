# Sombra Performance Optimization Plan

## Current Performance Issue

Sombra database is **58x slower** than SQLite in stress tests:
- **Sombra**: 68 ops/sec (2,043 operations in 30 seconds)
- **SQLite**: 3,966 ops/sec (118,976 operations in 30 seconds)

## Root Cause Analysis

### Critical Bottlenecks Identified

1. **WAL Sync Overhead** (`src/pager/wal.rs:102-105`)
   - Every transaction calls `sync_data()` 
   - Disk syncs are extremely expensive operations
   - Each insert = 1 transaction = 1 WAL sync

2. **Transaction Granularity** (`src/benchmark_suite.rs:241-257`)
   - Stress test creates 1 transaction per insert
   - 2,043 transactions for 2,043 inserts
   - SQLite uses bulk operations

3. **Page Management Overhead** (`src/db.rs:494-534`)
   - Multiple page fetches per insert
   - Free page list requires decode/clear/initialize cycle
   - No page cache optimization

4. **Excessive Checkpointing** (`src/benchmark_suite.rs:255`)
   - Checkpoint called after every operation in stress test
   - Forces WAL replay and file syncs

## Optimization Plan

### Priority 1: High-Impact Quick Wins (Expected 20-50x improvement)

#### 1. Add Optional WAL Sync Disabling
**Location**: `src/pager/wal.rs:102-105`, `src/pager/mod.rs:147-149`

**Implementation**:
```rust
// Add to Wal struct
pub struct Wal {
    file: File,
    page_size: usize,
    next_frame_number: u32,
    sync_enabled: bool,  // NEW
}

// Modify sync method
pub(crate) fn sync(&mut self) -> Result<()> {
    if self.sync_enabled {
        self.file.sync_data()?;
    }
    Ok(())
}
```

**Expected Gain**: 10-20x improvement (disk syncs are the biggest bottleneck)

#### 2. Implement Batch Transactions in Stress Test
**Location**: `src/benchmark_suite.rs:238-256`

**Implementation**:
```rust
// Add batch size parameter
const BATCH_SIZE: usize = 100;

// In stress test loop
let mut db = GraphDB::open(&db_path).unwrap();
let mut tx = db.begin_transaction().unwrap();
let mut batch_count = 0;

for _ in 0..operations {
    // ... create node ...
    tx.add_node(node).unwrap();
    batch_count += 1;
    
    if batch_count >= BATCH_SIZE {
        tx.commit().unwrap();
        tx = db.begin_transaction().unwrap();
        batch_count = 0;
    }
}
if batch_count > 0 {
    tx.commit().unwrap();
}
```

**Expected Gain**: 5-10x improvement (reduces 2,043 transactions to ~21)

#### 3. Remove Mid-Test Checkpointing
**Location**: `src/benchmark_suite.rs:255`

**Implementation**:
- Remove `tx.commit().unwrap()` call from stress test loop
- Only checkpoint at the very end of the test

**Expected Gain**: 2-3x improvement (eliminates unnecessary WAL replays)

### Priority 2: Medium-Impact Optimizations (Expected 1.5-2x improvement)

#### 4. Optimize Page Cache & Reuse
**Location**: `src/db.rs:494-534`, `src/pager/mod.rs:76-83`

**Issues**:
- Page cache uses `HashMap` with no eviction policy
- Free page list requires expensive fetch/decode/clear cycle
- Header page (page 0) is frequently accessed but not cached

**Implementation**:
```rust
// Pin header page in cache
impl Pager {
    pub fn fetch_page(&mut self, page_id: PageId) -> Result<&mut Page> {
        if page_id == 0 && !self.cache.contains_key(&page_id) {
            // Always keep header page cached
            let mut page = Page::new(page_id, self.page_size);
            self.read_page_from_disk(&mut page)?;
            self.cache.insert(page_id, page);
        }
        // ... rest of method
    }
}

// Optimize free page reuse
fn take_free_page(&mut self) -> Result<Option<PageId>> {
    // Skip expensive clear/initialize for pages that will be overwritten
    // Add fast path for bulk operations
}
```

#### 5. Reduce Index Update Overhead
**Location**: `src/db.rs:124`, `src/db.rs:178`

**Current**: HashMap insert on every add operation
**Optimization**: Indexes are already reasonably efficient, minimal gains expected

### Priority 3: Long-term Architectural Improvements

#### 6. Record Serialization Optimization
**Location**: `src/storage/ser.rs`

**Investigation Needed**:
- Check if using efficient serialization (bincode vs manual)
- Consider zero-copy serialization for large records

#### 7. Memory Pool for Page Buffers
**Implementation**: Reuse page buffers instead of allocating new ones

## Implementation Sequence

### Phase 1: Quick Wins (Total: ~1 hour)

```bash
# Step 1: Add sync_enabled flag (30 min)
- Modify Wal struct with sync_enabled field
- Modify Pager struct to pass sync config
- Thread through GraphDB::open() with config parameter
- Update benchmark suite to disable syncing

# Step 2: Batch stress test transactions (20 min)  
- Modify run_stress_test() to use batch transactions
- Add BATCH_SIZE constant
- Update transaction management logic

# Step 3: Remove mid-test checkpoints (5 min)
- Remove checkpoint() calls from stress loop
- Keep only final checkpoint

# Step 4: Test and measure (15 min)
- Run benchmark suite
- Compare results with baseline
- Verify correctness of operations
```

### Phase 2: Medium Optimizations (Total: ~2 hours)

```bash
# Step 5: Optimize page cache (1 hour)
- Pin header page in cache
- Optimize free page reuse logic
- Add page cache size limits

# Step 6: Additional micro-optimizations (1 hour)
- Profile remaining bottlenecks
- Optimize hot paths identified by profiling
- Consider memory pool for allocations
```

## Expected Results

| Optimization | Current | After | Improvement |
|--------------|---------|-------|-------------|
| Baseline | 68 ops/sec | - | - |
| + WAL sync disable | 68 ops/sec | 680-1,360 ops/sec | 10-20x |
| + Batch transactions | 680-1,360 ops/sec | 3,400-13,600 ops/sec | 5-10x |
| + Remove checkpoints | 3,400-13,600 ops/sec | 6,800-40,800 ops/sec | 2-3x |
| **Total Expected** | **68 ops/sec** | **6,800-40,800 ops/sec** | **100-600x** |

**Realistic Target**: 3,000-5,000 ops/sec (approaching SQLite's 3,966 ops/sec)

## Trade-offs & Considerations

### Durability vs Performance
- **Sync disabling**: Only safe for benchmarks, not production
- **Batch transactions**: Increases risk of partial failures
- **Reduced checkpoints**: Longer recovery time after crashes

### Fairness in Benchmarking
- Ensure SQLite comparison uses similar durability guarantees
- Consider running SQLite with `PRAGMA synchronous = OFF` for fair comparison
- Document all configuration differences

### Memory Usage
- Batching increases peak memory usage
- Page cache optimization increases memory footprint
- Monitor memory consumption during stress tests

### Complexity
- Keep changes minimal and well-documented
- Add configuration flags for all optimizations
- Maintain backward compatibility

## Testing Strategy

### Performance Testing
```bash
# Run baseline benchmark
cargo test --release stress

# Run after each optimization
cargo test --release stress

# Compare results
python scripts/compare_benchmarks.py baseline.csv optimized.csv
```

### Correctness Testing
```bash
# Ensure ACID properties still work
cargo test --release transaction

# Verify crash recovery
cargo test --release recovery

# Check data integrity
cargo test --release integrity
```

## Configuration Options

Add to `GraphDB::open()`:
```rust
pub struct Config {
    pub wal_sync_enabled: bool,      // Default: true
    pub batch_size: usize,           // Default: 1
    pub checkpoint_frequency: usize,  // Default: 1
    pub page_cache_size: usize,       // Default: 1000
}

impl Default for Config {
    fn default() -> Self {
        Self {
            wal_sync_enabled: true,
            batch_size: 1,
            checkpoint_frequency: 1,
            page_cache_size: 1000,
        }
    }
}
```

## Monitoring & Metrics

Track these metrics during optimization:
- Operations per second
- Disk I/O operations
- Memory usage
- WAL file size
- Transaction commit time
- Page cache hit rate

## Success Criteria

1. **Performance**: Achieve >3,000 ops/sec (within 25% of SQLite)
2. **Correctness**: All existing tests pass
3. **Stability**: No crashes during stress tests
4. **Configurability**: All optimizations can be toggled
5. **Documentation**: Changes are well-documented

## Next Steps

1. Implement Phase 1 optimizations
2. Measure performance improvements
3. If still below target, proceed to Phase 2
4. Consider architectural changes for remaining gaps
5. Document final configuration for production use

---

*Last Updated: 2025-10-18*
*Performance Baseline: 68 ops/sec*
*Target Performance: 3,000+ ops/sec*