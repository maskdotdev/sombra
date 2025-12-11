# Flush Deferred Optimization Plan

## Executive Summary

| Metric | Current | Target | Improvement |
|--------|---------|--------|-------------|
| **10k edges with props** | 1,162ms | ~400ms | **~3x faster** |
| `flush_deferred` time | 820ms | ~50ms | **~16x faster** |
| `finalize_adjacency` | 225ms | 0ms | **eliminated** |
| `flush_deferred_indexes` | 569ms | ~50ms | **~11x faster** |

## Current State Analysis

### Performance Baseline (10k edges with properties)

| Operation | Current Time | % of Total | Ops | Cost/Op |
|-----------|--------------|------------|-----|---------|
| **Total** | **1,162ms** | 100% | - | - |
| flush_deferred_indexes | 569ms | 49% | ~10k | ~57µs |
| finalize_adjacency_entries | 225ms | 19% | 20k | ~11µs |
| create_node | 185ms | 16% | 10k | ~18µs |
| create_edge | 147ms | 13% | 10k | ~15µs |
| adj put_many (fwd+rev) | 26ms | 2% | 2 batches | ~13ms |
| Other | ~10ms | 1% | - | - |

### Root Cause: Individual Read-Modify-Write Operations

Both bottlenecks perform **individual B-tree operations** instead of batched operations:

```
Current flush_deferred_indexes (per property):
  tree.get_with_write() → Segment::decode() → segment.insert() → Segment::encode() → tree.put()
  ~57µs × 10,000 = 569ms

Current finalize_adjacency_entries (per adjacency key):
  tree.get_with_write() → finalize_version_value() → tree.put()
  ~11µs × 20,000 = 225ms
```

---

## Optimization 1: Eliminate Adjacency Finalization

### Problem

Currently, adjacency entries are written with a `PENDING` flag, then immediately finalized by reading and rewriting each entry individually:

```
put_many(20k entries with PENDING) → finalize(20k individual get+put)
```

Cost: **225ms** (11µs × 20,000 ops)

### Solution

Write finalized values directly, eliminating the finalize step entirely.

### Changes

#### File: `src/storage/graph.rs`

**1. Modify `adjacency_value_for_commit()` (line 2673)**

```rust
// BEFORE:
fn adjacency_value_for_commit(commit: CommitId, tombstone: bool) -> VersionedValue<UnitValue> {
    let mut header = VersionHeader::new(commit, COMMIT_MAX, 0, 0);
    if tombstone {
        header.flags |= mvcc_flags::TOMBSTONE;
    }
    header.set_pending();  // ← REMOVE THIS
    VersionedValue::new(header, UnitValue)
}

// AFTER:
fn adjacency_value_for_commit(commit: CommitId, tombstone: bool) -> VersionedValue<UnitValue> {
    let mut header = VersionHeader::new(commit, COMMIT_MAX, 0, 0);
    if tombstone {
        header.flags |= mvcc_flags::TOMBSTONE;
    }
    // No pending flag - write finalized values directly
    VersionedValue::new(header, UnitValue)
}
```

**2. Update `insert_adjacencies()` (line 3298-3303)**

```rust
// BEFORE:
// Profile finalize_adjacency_entries
let finalize_start = profile_timer();
self.finalize_adjacency_entries(tx, &keys)?;  // ← REMOVE
if let Some(start) = finalize_start {
    record_flush_adj_finalize(start.elapsed().as_nanos() as u64, (keys.len() * 2) as u64);
}

// AFTER:
// No finalization needed - values are already finalized
```

**3. Update test `adjacency_entries_clear_pending_after_insert` (line 5234)**

- Rename to `adjacency_entries_not_pending_after_insert`
- Verify entries are NOT pending (current behavior after change)

### Operations Before/After

| Operation | Before | After |
|-----------|--------|-------|
| `put_many` (fwd) | 1 | 1 |
| `put_many` (rev) | 1 | 1 |
| `get_with_write` | 20,000 | **0** |
| `tree.put` | 20,000 | **0** |

**Expected savings: ~225ms**

---

## Optimization 2: Batch Property Index Updates

### Problem

Property index updates are done individually with read-modify-write:

```
For each of ~10k property ops:
  tree.get() → Segment::decode() → segment.insert() → Segment::encode() → tree.put()
```

Cost: **569ms** (57µs × 10,000 ops)

### Key Insight: Two Index Types Have Different Optimizations

| Index Type | Key Structure | Value | Optimization |
|------------|---------------|-------|--------------|
| **BTreePostings** | `(label, prop, value, node_id)` | `Unit` | Direct `put_many` - no read-modify-write needed |
| **ChunkedIndex** | `(label, prop, value, segment_id)` | `Segment(Vec<NodeId>)` | Group by key, batch read + multi-insert + `put_many` |

### Solution

#### Part A: BTreePostings Optimization (simpler)

BTreePostings already stores one entry per `(prefix, node)` pair. We can use the existing `put_many` directly!

**File: `src/storage/index/store.rs`**

Add new batch method:

```rust
/// Batch insert property values for BTree indexes.
pub fn insert_property_values_batch_btree(
    &self,
    tx: &mut WriteGuard<'_>,
    items: Vec<(Vec<u8>, NodeId, Option<CommitId>)>,  // (prefix, node, commit)
) -> Result<()> {
    // Sort by full key (prefix + node)
    let mut keyed: Vec<(Vec<u8>, VersionedValue<Unit>)> = items
        .into_iter()
        .map(|(prefix, node, commit)| {
            let key = BTreePostings::make_key(&prefix, node);
            let value = self.btree.versioned_unit(tx, false, commit);
            (key, value)
        })
        .collect();
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    
    let iter = keyed.iter().map(|(k, v)| PutItem { key: k, value: v });
    self.btree.put_many(tx, iter)
}
```

#### Part B: ChunkedIndex Optimization (more complex)

ChunkedIndex stores a `Segment(Vec<NodeId>)` per property value. Multiple nodes with the same property value share a segment.

**File: `src/storage/index/chunked.rs`**

Add new batch method:

```rust
/// Batch insert nodes into segments.
/// 
/// items: Vec<(prefix, Vec<NodeId>, commit)> where prefix = (label, prop, value_key)
pub fn put_batch(
    &self,
    tx: &mut WriteGuard<'_>,
    items: Vec<(Vec<u8>, Vec<NodeId>, Option<CommitId>)>,
) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    self.ensure_tree_with_write(tx)?;
    let tree_ref = self.tree.borrow();
    let Some(tree) = tree_ref.as_ref() else {
        return Err(SombraError::Corruption("chunked postings tree missing"));
    };
    
    // Step 1: Build full keys and collect existing segments
    let mut updates: Vec<(Vec<u8>, Segment, Option<CommitId>)> = Vec::with_capacity(items.len());
    
    for (prefix, nodes, commit) in items {
        let key = Self::make_key(&prefix, SEGMENT_PRIMARY);
        
        // Get existing segment or create new one
        let mut segment = match tree.get_with_write(tx, &key)? {
            Some(bytes) => Segment::decode(&bytes.value)?,
            None => Segment::new(),
        };
        
        // Insert all nodes into segment
        for node in nodes {
            segment.insert(node);
        }
        
        updates.push((key, segment, commit));
    }
    
    // Step 2: Sort by key for put_many
    updates.sort_by(|a, b| a.0.cmp(&b.0));
    
    // Step 3: Encode and put_many
    let encoded: Vec<(Vec<u8>, VersionedValue<Vec<u8>>)> = updates
        .into_iter()
        .map(|(key, segment, commit)| {
            let encoded = segment.encode();
            let value = self.versioned_bytes(tx, encoded, commit, false);
            (key, value)
        })
        .collect();
    
    let iter = encoded.iter().map(|(k, v)| PutItem { key: k, value: v });
    tree.put_many(tx, iter)
}
```

#### Part C: Update `flush_deferred_indexes()` to use batching

**File: `src/storage/graph.rs`**

```rust
fn flush_deferred_indexes(&self, tx: &mut WriteGuard<'_>) -> Result<()> {
    let flush_idx_start = profile_timer();
    if !self.defer_index_flush {
        return Ok(());
    }
    let mut state = self.take_txn_state(tx);
    let Some(mut buffer) = state.deferred_index.take() else {
        self.store_txn_state(tx, state);
        return Ok(());
    };
    
    // Process label inserts (unchanged - typically small count)
    for (label, node, commit) in buffer.label_inserts.drain(..) {
        if self.indexes.has_label_index_with_write(tx, label)? {
            self.indexes.insert_node_labels_with_commit(tx, node, &[label], Some(commit))?;
        }
    }
    
    // Process label removes (unchanged)
    for (label, node, commit) in buffer.label_removes.drain(..) {
        if self.indexes.has_label_index_with_write(tx, label)? {
            self.indexes.remove_node_labels_with_commit(tx, node, &[label], Some(commit))?;
        }
    }
    
    // === OPTIMIZED: Batch property inserts ===
    if !buffer.prop_inserts.is_empty() {
        // Separate by index kind
        let mut btree_items: Vec<(Vec<u8>, NodeId, Option<CommitId>)> = Vec::new();
        let mut chunked_groups: BTreeMap<Vec<u8>, (Vec<NodeId>, Option<CommitId>)> = BTreeMap::new();
        
        for (def, key, node, commit) in buffer.prop_inserts.drain(..) {
            match def.kind {
                IndexKind::BTree => {
                    let prefix = BTreePostings::make_prefix(def.label, def.prop, &key);
                    btree_items.push((prefix, node, Some(commit)));
                }
                IndexKind::Chunked => {
                    let prefix = ChunkedIndex::make_prefix(def.label, def.prop, &key);
                    chunked_groups
                        .entry(prefix)
                        .or_insert_with(|| (Vec::new(), Some(commit)))
                        .0
                        .push(node);
                }
            }
        }
        
        // Batch insert BTree items
        if !btree_items.is_empty() {
            self.indexes.insert_property_values_batch_btree(tx, btree_items)?;
        }
        
        // Batch insert Chunked items
        if !chunked_groups.is_empty() {
            let chunked_items: Vec<_> = chunked_groups
                .into_iter()
                .map(|(prefix, (nodes, commit))| (prefix, nodes, commit))
                .collect();
            self.indexes.insert_property_values_batch_chunked(tx, chunked_items)?;
        }
    }
    
    // === Property removes (keep individual for now) ===
    for (def, key, node, commit) in buffer.prop_removes.drain(..) {
        self.indexes.remove_property_value_with_commit(tx, &def, &key, node, Some(commit))?;
    }
    
    self.store_txn_state(tx, state);
    if let Some(start) = flush_idx_start {
        record_flush_deferred_indexes(start.elapsed().as_nanos() as u64);
    }
    Ok(())
}
```

### Operations Before/After

**BTreePostings (per 5k items assuming 5k unique property values):**

| Operation | Before | After |
|-----------|--------|-------|
| `tree.get_with_write` | 5,000 | **0** |
| `tree.put` (individual) | 5,000 | **0** |
| `tree.put_many` | 0 | **1** |

**ChunkedIndex (per 5k items assuming 2.5k unique keys):**

| Operation | Before | After |
|-----------|--------|-------|
| `tree.get_with_write` | 5,000 | **2,500** (1 per unique key) |
| `Segment::decode` | 5,000 | **2,500** |
| `segment.insert` | 5,000 | 5,000 (batched per segment) |
| `Segment::encode` | 5,000 | **2,500** |
| `tree.put` (individual) | 5,000 | **0** |
| `tree.put_many` | 0 | **1** |

**Expected savings: ~500-520ms**

---

## Implementation Order

### Phase 1: Adjacency Finalization (Est: 30 min)

1. Remove `set_pending()` from `adjacency_value_for_commit()`
2. Remove `finalize_adjacency_entries()` call from `insert_adjacencies()`
3. Update test case
4. Build and run `profile_create` to verify

### Phase 2: BTreePostings Batching (Est: 1 hour)

1. Add `insert_property_values_batch_btree()` to `store.rs`
2. Update `flush_deferred_indexes()` to separate BTree items and batch them
3. Build and test

### Phase 3: ChunkedIndex Batching (Est: 2 hours)

1. Add `put_batch()` to `chunked.rs`
2. Add `insert_property_values_batch_chunked()` to `store.rs`
3. Update `flush_deferred_indexes()` to group and batch Chunked items
4. Build and test

### Phase 4: Property Removes Batching (Optional, Est: 1 hour)

1. Add `remove_batch()` methods similar to inserts
2. Update `flush_deferred_indexes()` for removes
3. Build and test

### Phase 5: Verification (Est: 30 min)

1. Run full benchmark suite
2. Verify correctness with existing integration tests
3. Document results

---

## Expected Final Results

| Scenario | Current | After Opt 1 | After Opt 2 | Total Improvement |
|----------|---------|-------------|-------------|-------------------|
| **10k edges + props** | 1,162ms | 937ms | **~400ms** | **~3x faster** |
| `flush_deferred` | 820ms | 595ms | **~50ms** | **~16x faster** |
| Throughput | 8,603 ops/s | ~10,700 ops/s | **~25,000 ops/s** | **~3x higher** |

---

## Files to Modify

| File | Changes |
|------|---------|
| `src/storage/graph.rs` | Remove finalization, batch index flush logic |
| `src/storage/index/store.rs` | Add batch insert methods |
| `src/storage/index/chunked.rs` | Add `put_batch()` method |
| `src/storage/index/btree_postings.rs` | May need `versioned_unit` public or helper |

---

## Appendix: Profiling Data

### Raw Benchmark Output (Edges with Properties)

```
Total: 1.162419s (8603 ops/sec, 116.24 µs/op)

FLUSH DEFERRED BREAKDOWN (10000 adj entries):
  key_encode:       158792 ns (  0.0%, 1 calls, 158792 ns/call)
  fwd_sort:          14250 ns (  0.0%, 1 calls)
  fwd_put_many:   12870625 ns (  1.6%, 1 calls)
  rev_sort:          15125 ns (  0.0%, 1 calls)
  rev_put_many:   13262917 ns (  1.6%, 1 calls)
  finalize:      224828833 ns ( 27.4%, 20000 ops, 11241 ns/op)
  def_indexes:   569141500 ns ( 69.4%, 1 calls)
  other:            247249 ns (  0.0%)

TIME BREAKDOWN (% of wall clock):
  FFI overhead:  71.3% (828.899 ms)
  create_node:   15.9% (184.986 ms)
  create_edge:   12.7% (147.106 ms)
  flush_defer:   70.6% (820.539 ms)
```
