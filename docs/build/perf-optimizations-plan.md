# Performance Optimizations Plan

## Summary of Changes

| # | Change | Files | Risk | Impact |
|---|--------|-------|------|--------|
| 1 | Profiling default OFF | `storage/profile.rs` | Very Low | ~18% |
| 2 | GraphWriter in FFI + configurable cache (default 8192) | `ffi/mod.rs`, `storage/graph/writer.rs` | Low | High |
| 3 | is_sorted() guards | `adjacency_ops.rs`, `label.rs`, new `storage/util.rs` | Very Low | Medium |
| 4-6 | BufferPool for fence/snapshot allocations | New `btree/buffer_pool.rs`, `leaf.rs` | Medium | High |
| 7 | SmallVec in LeafAllocatorSnapshot | `leaf_allocator.rs` | Low | Medium |
| 8-10 | put_many buffer reuse + borrowed insert_into_leaf | `api.rs`, `leaf.rs` | Medium | High |
| 11-12 | Benchmarks + tests | New benchmark file, run tests | Low | Validation |

---

## Detailed Changes

### 1. Profiling Default OFF
**File:** `src/storage/profile.rs:482`

```rust
// Change from:
Err(_) => true,
// To:
Err(_) => false,
```

---

### 2. GraphWriter Integration + Configurable Cache

**File:** `src/storage/graph/writer.rs:33`
```rust
// Change default from 1024 to 8192
exists_cache_capacity: 8192,
```

**File:** `src/ffi/mod.rs` - Add option to DatabaseOptions:
```rust
pub struct DatabaseOptions {
    // ... existing fields ...
    /// Capacity of the node-existence cache for bulk edge operations.
    /// Higher values improve performance for large batches at the cost of memory.
    pub edge_cache_capacity: usize,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            // ... existing defaults ...
            edge_cache_capacity: 8192,
        }
    }
}
```

**File:** `src/ffi/mod.rs` - Modify `CreateBuilder::execute()`, `create_typed_batch()`, and edge operations in `mutate()`:

```rust
// In CreateBuilder::execute() around line 3166
pub fn execute(self) -> Result<CreateResult> {
    let mut write = self.db.pager.begin_write()?;
    // ... node creation loop stays the same ...
    
    // Create GraphWriter for cached edge validation
    let opts = CreateEdgeOptions {
        trusted_endpoints: false,
        exists_cache_capacity: self.db.edge_cache_capacity,
    };
    let mut edge_writer = GraphWriter::try_new(&self.db.graph, opts, None)?;
    
    for edge in &self.edges {
        let src_id = self.resolve_node_ref(&edge.src, &handle_ids, &alias_ids)?;
        let dst_id = self.resolve_node_ref(&edge.dst, &handle_ids, &alias_ids)?;
        let edge_id = self.insert_edge_with_writer(&mut write, &mut edge_writer, src_id, dst_id, edge)?;
        created_edges.push(edge_id);
    }
    // ...
}

fn insert_edge_with_writer(
    &self,
    write: &mut WriteGuard<'_>,
    writer: &mut GraphWriter<'_>,
    src: NodeId,
    dst: NodeId,
    edge: &DraftEdge,
) -> Result<EdgeId> {
    let ty = self.db.resolve_type(write, &edge.ty)?;
    let prop_storage = collect_prop_storage(self.db, write, &edge.props)?;
    let mut prop_entries = Vec::with_capacity(prop_storage.len());
    for (prop, owned) in &prop_storage {
        prop_entries.push(PropEntry::new(*prop, prop_value_ref(owned)));
    }
    writer.create_edge(write, StorageEdgeSpec { src, dst, ty, props: &prop_entries })
}
```

Similar changes for `create_typed_batch()` and `mutate()`.

---

### 3. is_sorted() Helper + Guards

**New file:** `src/storage/util.rs`
```rust
//! Storage utility functions.

use std::cmp::Ordering;

/// Checks if a slice is sorted in ascending order using a comparison function.
#[inline]
pub fn is_sorted_by<T, F>(slice: &[T], mut cmp: F) -> bool
where
    F: FnMut(&T, &T) -> Ordering,
{
    slice.windows(2).all(|w| cmp(&w[0], &w[1]) != Ordering::Greater)
}

/// Checks if a slice of references is sorted in ascending order.
#[inline]
pub fn is_sorted<T: Ord>(slice: &[T]) -> bool {
    is_sorted_by(slice, |a, b| a.cmp(b))
}
```

**File:** `src/storage/mod.rs` - Add module:
```rust
mod util;
pub use util::{is_sorted, is_sorted_by};
```

**File:** `src/storage/graph/adjacency_ops.rs:167-168`
```rust
// Before:
refs.sort_unstable();

// After:
if !crate::storage::is_sorted(&refs) {
    refs.sort_unstable();
}
```

Same change at line 188-189 for reverse adjacency.

**File:** `src/storage/index/label.rs:315`
```rust
// Before:
all_entries.sort_by(|a, b| a.0.cmp(&b.0));

// After:
if !crate::storage::is_sorted_by(&all_entries, |a, b| a.0.cmp(&b.0)) {
    all_entries.sort_by(|a, b| a.0.cmp(&b.0));
}
```

---

### 4-6. BufferPool Implementation

**New file:** `src/storage/btree/tree/definition/buffer_pool.rs`
```rust
//! Pool of reusable byte buffers for reducing allocation churn during writes.

/// Pool of reusable byte buffers.
#[derive(Default)]
pub struct BufferPool {
    buffers: Vec<Vec<u8>>,
    max_buffers: usize,
}

impl BufferPool {
    /// Creates a new buffer pool with the given maximum capacity.
    pub fn new(max_buffers: usize) -> Self {
        Self {
            buffers: Vec::with_capacity(max_buffers),
            max_buffers,
        }
    }

    /// Acquires a buffer, either from the pool or freshly allocated.
    pub fn acquire(&mut self) -> Vec<u8> {
        self.buffers.pop().unwrap_or_default()
    }

    /// Acquires a buffer with at least the given capacity.
    pub fn acquire_with_capacity(&mut self, capacity: usize) -> Vec<u8> {
        let mut buf = self.acquire();
        buf.clear();
        if buf.capacity() < capacity {
            buf.reserve(capacity - buf.capacity());
        }
        buf
    }

    /// Returns a buffer to the pool for reuse.
    pub fn release(&mut self, mut buffer: Vec<u8>) {
        buffer.clear();
        if self.buffers.len() < self.max_buffers {
            self.buffers.push(buffer);
        }
    }
}
```

**File:** `src/storage/btree/tree/definition/mod.rs` - Add module and helper:
```rust
mod buffer_pool;
pub use buffer_pool::BufferPool;

use crate::primitives::pager::WriteGuard;

const DEFAULT_BUFFER_POOL_SIZE: usize = 16;

/// Gets or initializes the transaction-scoped buffer pool.
pub(crate) fn get_buffer_pool(tx: &mut WriteGuard<'_>) -> &mut BufferPool {
    if tx.extension_mut::<BufferPool>().is_none() {
        tx.store_extension(BufferPool::new(DEFAULT_BUFFER_POOL_SIZE));
    }
    tx.extension_mut::<BufferPool>().unwrap()
}
```

**File:** `src/storage/btree/tree/definition/leaf.rs` - Use BufferPool for fence allocations:

In `try_insert_leaf_in_place()` around line 601-602:
```rust
// Before:
let low_fence: Vec<u8> = low_fence_slice.to_vec();

// After:
let pool = get_buffer_pool(tx);
let mut low_fence = pool.acquire_with_capacity(low_fence_slice.len());
low_fence.extend_from_slice(low_fence_slice);
```

Similarly in `insert_into_leaf()` at lines 425-427 for both low_fence_vec and high_fence_vec.

**Note:** Buffer release happens automatically when the transaction completes (WriteGuard drops), or we can explicitly return buffers when they're no longer needed.

---

### 7. SmallVec in LeafAllocatorSnapshot

**File:** `src/storage/btree/tree/definition/leaf_allocator.rs`

Change the snapshot struct:
```rust
// Before:
pub struct LeafAllocatorSnapshot {
    pub slot_meta: Vec<SlotMeta>,
    pub free_regions: Vec<FreeRegion>,
    pub arena_start: usize,
    pub payload_len: usize,
}

// After:
pub struct LeafAllocatorSnapshot {
    pub slot_meta: SmallVec<[SlotMeta; 128]>,
    pub free_regions: SmallVec<[FreeRegion; 32]>,
    pub arena_start: usize,
    pub payload_len: usize,
}
```

Update `into_snapshot()`:
```rust
// Before:
pub fn into_snapshot(self) -> LeafAllocatorSnapshot {
    LeafAllocatorSnapshot {
        slot_meta: self.slot_meta.into_vec(),
        free_regions: self.free_regions.into_vec(),
        ...
    }
}

// After:
pub fn into_snapshot(self) -> LeafAllocatorSnapshot {
    LeafAllocatorSnapshot {
        slot_meta: self.slot_meta,      // Move, no allocation
        free_regions: self.free_regions, // Move, no allocation
        ...
    }
}
```

Update `from_snapshot()` to use SmallVec directly (no change needed since `SmallVec::from_vec` also accepts `SmallVec`).

---

### 8-10. put_many Refactoring

**File:** `src/storage/btree/tree/definition/leaf.rs`

Change `insert_into_leaf` signature:
```rust
// Before:
fn insert_into_leaf(
    &self,
    tx: &mut WriteGuard<'_>,
    mut page: PageMut<'_>,
    header: page::Header,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<LeafInsert>

// After:
fn insert_into_leaf(
    &self,
    tx: &mut WriteGuard<'_>,
    mut page: PageMut<'_>,
    header: page::Header,
    key: &[u8],
    value: &[u8],
) -> Result<LeafInsert>
```

Update internal usage - only allocate when needed:
```rust
// Line 467, when inserting into entries:
Err(idx) => {
    entries.insert(idx, (key.to_vec(), value.to_vec()));
}
```

**File:** `src/storage/btree/tree/definition/api.rs`

Update `put()`:
```rust
pub fn put(&self, tx: &mut WriteGuard<'_>, key: &K, val: &V) -> Result<()> {
    let mut key_buf = Vec::new();
    K::encode_key(key, &mut key_buf);
    let mut val_buf = Vec::new();
    V::encode_val(val, &mut val_buf);
    let (leaf_id, header, path) = self.find_leaf_mut(tx, &key_buf)?;
    let leaf = tx.page_mut(leaf_id)?;
    // Pass slices instead of owned vecs
    match self.insert_into_leaf(tx, leaf, header, &key_buf, &val_buf)? {
        // ...
    }
}
```

Update `put_many()`:
```rust
pub fn put_many<'a, I>(&self, tx: &mut WriteGuard<'_>, items: I) -> Result<()>
where
    I: IntoIterator<Item = PutItem<'a, K, V>>,
    K: 'a,
    V: 'a,
{
    let mut cache: Option<LeafCache> = None;
    let mut prev_key: Option<Vec<u8>> = None;
    
    // Reusable buffers - moved outside loop
    let mut key_buf = Vec::new();
    let mut val_buf = Vec::new();
    
    for item in items.into_iter() {
        // Reuse key buffer
        key_buf.clear();
        K::encode_key(item.key, &mut key_buf);
        
        if let Some(prev) = &prev_key {
            debug_assert!(
                K::compare_encoded(prev, &key_buf) != Ordering::Greater,
                "put_many keys must be sorted"
            );
        }
        
        // Reuse value buffer
        val_buf.clear();
        V::encode_val(item.value, &mut val_buf);
        
        let (leaf_id, header, path) = match cache.take() {
            Some(cached) => match self.try_reuse_leaf(tx, cached, &key_buf)? {
                Some(result) => result,
                None => self.find_leaf_mut(tx, &key_buf)?,
            },
            None => self.find_leaf_mut(tx, &key_buf)?,
        };
        
        let leaf_page = tx.page_mut(leaf_id)?;
        // No more clone! Pass slices directly
        match self.insert_into_leaf(tx, leaf_page, header, &key_buf, &val_buf)? {
            LeafInsert::Done { new_first_key } => {
                if let (Some(first), Some(parent_frame)) = (new_first_key.as_ref(), path.last()) {
                    self.update_parent_separator(tx, parent_frame, first)?;
                }
                cache = Some(LeafCache { leaf_id, path: path.clone() });
            }
            LeafInsert::Split { left_min, right_min, right_page } => {
                self.propagate_split(tx, path, leaf_id, left_min, right_min, right_page)?;
                cache = None;
            }
        }
        
        // Swap key_buf into prev_key efficiently
        prev_key = Some(std::mem::replace(&mut key_buf, prev_key.take().unwrap_or_default()));
    }
    Ok(())
}
```

The key optimization here is the `std::mem::replace` at the end - instead of cloning `key_buf`, we swap it with `prev_key`, reusing both allocations.

---

### 11. Benchmarks

**New file:** `benches/perf_optimizations.rs`

```rust
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};

fn bench_bulk_edge_creation(c: &mut Criterion) {
    // Test GraphWriter caching effectiveness
    let mut group = c.benchmark_group("bulk_edge_creation");
    for node_count in [1000, 5000, 20000] {
        let edge_count = node_count * 4; // 4 edges per node
        group.bench_with_input(
            BenchmarkId::new("edges", edge_count),
            &(node_count, edge_count),
            |b, &(nodes, edges)| {
                b.iter(|| {
                    // Create nodes and edges
                });
            },
        );
    }
    group.finish();
}

fn bench_put_many_allocations(c: &mut Criterion) {
    // Test put_many buffer reuse
}

fn bench_profiling_overhead(c: &mut Criterion) {
    // Compare with SOMBRA_PROFILE=0 vs =1
}

criterion_group!(
    benches,
    bench_bulk_edge_creation,
    bench_put_many_allocations,
    bench_profiling_overhead,
);
criterion_main!(benches);
```

---

### 12. Test Execution

```bash
# Run all tests
cargo test --all-features

# Run release tests
cargo test --release --all-features

# Run specific integration tests
cargo test --test storage_phase3
cargo test --test storage_stress
```

---

## Execution Order

1. **Task 1** - Profiling default (1 line change, immediate validation)
2. **Task 3** - is_sorted guards (new file + 3 small changes)
3. **Task 7** - SmallVec in snapshot (contained change in leaf_allocator.rs)
4. **Tasks 8-10** - put_many + insert_into_leaf refactor (interdependent)
5. **Tasks 4-6** - BufferPool (new infrastructure + integration)
6. **Task 2** - GraphWriter in FFI (higher-level change)
7. **Tasks 11-12** - Benchmarks and test validation

---

## Expected Performance Impact

| Optimization | Expected Gain |
|-------------|---------------|
| Profiling OFF | ~18% in simple runs |
| GraphWriter caching | ~80% reduction in node lookups for bulk edge creation |
| is_sorted guards | Variable; O(n) vs O(n log n) when pre-sorted |
| SmallVec snapshot | Eliminates allocation for pages with <128 slots |
| put_many buffer reuse | Eliminates 2 allocations per item |
| clone elimination | Eliminates 1 clone per item in put_many |
| BufferPool | Reduces fence allocation churn |

**Combined for 20k nodes / 80k edges:** Estimated 30-50% overall improvement on bulk ingest.
