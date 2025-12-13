# IFA Inline Segments Optimization Plan

## Executive Summary

| Metric | Current | Target | Improvement |
|--------|---------|--------|-------------|
| **Low-degree node lookup** | 2 page reads | 1 page read | **2x faster** |
| **Edge insert (1-2 edges)** | B-tree + segment alloc | B-tree only | **~50% faster** |
| **Memory per edge (small nodes)** | ~82 bytes | ~20 bytes | **~4x smaller** |

## Current State Analysis

### Architecture Overview

The IFA (Index-Free Adjacency) system uses a two-level structure:

1. **NodeAdjHeader** (72 bytes) - stored in B-tree via `IfaStore`
   - 6 `TypeBucket` slots @ 12 bytes each
   - Each bucket: `TypeId` (4B) + `SegmentPtr` (8B)
   - Maps edge type -> segment pointer

2. **AdjSegment** - stored on separate pages via `SegmentManager`
   - Header: 50 bytes (owner, dir, type, xmin, xmax, prev_version, next_extent, entry_count)
   - Entries: 32 bytes each (neighbor 8B + edge 8B + xmin 8B + xmax 8B)

### Current Data Flow (Read Path)

```
get_neighbors(node, dir, type) →
  1. B-tree lookup: IfaStore.lookup_type() → NodeAdjHeader → SegmentPtr
  2. Page read: SegmentManager.read_segment(SegmentPtr) → AdjSegment
  3. Filter: segment.entries.iter().filter(visible_at(snapshot))
```

### Current Data Flow (Write Path)

```
add_edge(src, dst, type) →
  1. B-tree lookup: IfaStore.lookup_type_mut() → Option<SegmentPtr>
  2. If Some(ptr): SegmentManager.cow_clone() → new AdjSegment
  3. If None: SegmentManager.create_segment() → new AdjSegment
  4. segment.insert(entry)
  5. SegmentManager.allocate_segment() → new page
  6. IfaStore.upsert_type(new_ptr)
```

### Problem: Overhead for Low-Degree Nodes

Most real-world graphs follow a power-law distribution where:
- **80%+ of nodes** have 1-5 edges per type
- **Only ~5%** are "hub" nodes with 100+ edges

For a node with 1 edge:
- Current: 72B (header) + 82B (segment page overhead + 1 entry) = **154 bytes, 2 page reads**
- Optimal: Could fit in ~20 bytes inline

### Key Insight

The `NodeAdjHeader` has 6 bucket slots but most nodes use only 1-2 edge types.
Unused buckets (4-5 slots = 48-60 bytes) could store small adjacency lists inline.

---

## Design: Inline Segment Storage

### Concept

Store small adjacency lists directly in `NodeAdjHeader` bucket slots, eliminating the need for separate `AdjSegment` pages for low-degree nodes.

### New `TypeBucket` Format

```
Standard Bucket (current):
+----------+----------------+
| TypeId   | SegmentPtr     |
| 4 bytes  | 8 bytes        |
+----------+----------------+

Inline Bucket (new):
+----------+------+---------+----------+
| TypeId   | Flag | Count   | Entries  |
| 4 bytes  | 1b   | 7 bits  | variable |
+----------+------+---------+----------+
```

**Flag encoding in TypeId:**
- Bit 31 = 0: Standard bucket (SegmentPtr follows)
- Bit 31 = 1: Inline bucket (inline entries follow)

### Inline Entry Format (Compact)

For inline storage, we use a compact 16-byte entry format:

```
InlineAdjEntry:
+------------+----------+
| neighbor   | edge     |
| 8 bytes    | 8 bytes  |
+------------+----------+
```

Note: `xmin`/`xmax` are omitted for inline entries. All inline entries inherit the segment-level visibility from the header's creation time. When visibility tracking is needed (delete/update), the entry promotes to external storage.

### Capacity Analysis

With 6 buckets @ 12 bytes = 72 bytes total:

| Configuration | Inline Capacity |
|---------------|-----------------|
| 1 type, 5 spare buckets | 5 * 12 = 60 bytes → **3 entries** (48B) + 1 count byte |
| 2 types, 4 spare buckets | 4 * 12 = 48 bytes → **2 entries** (32B) + 2 count bytes |
| 3 types, 3 spare buckets | 3 * 12 = 36 bytes → **2 entries** (32B) |

**Threshold:** Promote to external segment when:
- Entry count > 3 for single-type nodes
- Entry count > 2 for multi-type nodes
- Any entry is deleted (needs xmax tracking)

### New Data Structures

```rust
// In src/storage/graph/ifa/types.rs

/// Marker bit in TypeId indicating inline storage
pub const INLINE_STORAGE_FLAG: u32 = 1 << 31;

/// Maximum inline entries per type (single-type node)
pub const MAX_INLINE_ENTRIES_SINGLE: usize = 3;

/// Maximum inline entries per type (multi-type node)  
pub const MAX_INLINE_ENTRIES_MULTI: usize = 2;

/// Compact inline adjacency entry (no MVCC fields)
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InlineAdjEntry {
    pub neighbor: NodeId,
    pub edge: EdgeId,
}

impl InlineAdjEntry {
    pub const LEN: usize = 16;
    
    pub fn encode(&self) -> [u8; 16] { ... }
    pub fn decode(bytes: &[u8]) -> Result<Self> { ... }
}

/// Extended TypeBucket supporting inline storage
#[derive(Clone, Debug)]
pub enum TypeBucketData {
    /// Standard: points to external segment
    External { head: SegmentPtr },
    /// Inline: entries stored directly
    Inline { entries: SmallVec<[InlineAdjEntry; 3]> },
}

impl TypeBucket {
    /// Returns true if this bucket uses inline storage
    pub fn is_inline(&self) -> bool {
        (self.type_id.0 & INLINE_STORAGE_FLAG) != 0
    }
    
    /// Gets the actual type ID (without flag bit)
    pub fn actual_type_id(&self) -> TypeId {
        TypeId(self.type_id.0 & !INLINE_STORAGE_FLAG)
    }
}
```

### Modified Read Path

```rust
// In src/storage/graph/ifa/adjacency.rs

pub fn get_neighbors(
    &self,
    tx: &ReadGuard,
    node: NodeId,
    dir: Dir,
    type_id: TypeId,
    snapshot: TxId,
) -> Result<Vec<AdjEntry>> {
    let header = self.store.get_header(tx, node, dir)?;
    let Some(header) = header else { return Ok(vec![]); };
    
    // Check inline buckets first
    for bucket in &header.buckets[..INLINE_BUCKET_COUNT - 1] {
        if bucket.actual_type_id() == type_id {
            if bucket.is_inline() {
                // Fast path: return inline entries directly
                return Ok(bucket.decode_inline_entries()?
                    .into_iter()
                    .map(|e| AdjEntry::new(e.neighbor, e.edge, header.xmin))
                    .collect());
            } else {
                // Standard path: read external segment
                let segment = self.segment_mgr.find_visible_segment_ro(
                    tx, bucket.head, snapshot
                )?;
                return Ok(segment.map(|s| s.entries).unwrap_or_default());
            }
        }
    }
    
    // Check overflow if needed
    if header.has_overflow() {
        // ... existing overflow logic
    }
    
    Ok(vec![])
}
```

### Modified Write Path

```rust
// In src/storage/graph/ifa/adjacency.rs

pub fn add_edge(
    &self,
    tx: &mut WriteGuard<'_>,
    src: NodeId,
    dst: NodeId,
    edge: EdgeId,
    type_id: TypeId,
    xmin: TxId,
) -> Result<()> {
    let mut header = self.store.get_header_mut(tx, src, Dir::Out)?
        .unwrap_or_else(NodeAdjHeader::new);
    
    // Find or create bucket for this type
    let bucket_idx = header.find_or_allocate_bucket(type_id)?;
    let bucket = &mut header.buckets[bucket_idx];
    
    if bucket.is_empty() {
        // New type: start with inline storage
        bucket.set_inline(type_id, InlineAdjEntry { neighbor: dst, edge });
    } else if bucket.is_inline() {
        let mut entries = bucket.decode_inline_entries()?;
        
        // Check if we need to promote to external
        let max_inline = if header.active_count() == 1 {
            MAX_INLINE_ENTRIES_SINGLE
        } else {
            MAX_INLINE_ENTRIES_MULTI
        };
        
        if entries.len() >= max_inline {
            // Promote to external segment
            let segment = self.promote_to_segment(tx, src, Dir::Out, type_id, &entries, xmin)?;
            segment.insert(AdjEntry::new(dst, edge, xmin));
            let ptr = self.segment_mgr.allocate_segment(tx, &segment)?;
            bucket.set_external(type_id, ptr);
        } else {
            // Still fits inline
            entries.push(InlineAdjEntry { neighbor: dst, edge });
            bucket.set_inline_entries(type_id, &entries);
        }
    } else {
        // External segment: use existing CoW path
        let ptr = self.segment_mgr.insert_edge(
            tx, Some(bucket.head), src, Dir::Out, type_id, dst, edge, xmin
        )?;
        bucket.head = ptr;
    }
    
    self.store.put_header(tx, src, Dir::Out, &header)?;
    Ok(())
}
```

---

## Implementation Plan

### Phase 1: Add Inline Entry Types (Est: 1 hour)

**Files:** `src/storage/graph/ifa/types.rs`

1. Add `INLINE_STORAGE_FLAG` constant
2. Add `InlineAdjEntry` struct with encode/decode
3. Add `TypeBucket::is_inline()`, `actual_type_id()` methods
4. Add `TypeBucket::decode_inline_entries()` method
5. Add unit tests

### Phase 2: Extend NodeAdjHeader (Est: 1 hour)

**Files:** `src/storage/graph/ifa/types.rs`

1. Add `NodeAdjHeader::find_or_allocate_bucket()` method
2. Add `TypeBucket::set_inline()`, `set_external()` methods
3. Add inline entry encoding in bucket serialization
4. Update `NodeAdjHeader::encode()`/`decode()` for inline support
5. Add roundtrip tests

### Phase 3: Update Read Path (Est: 2 hours)

**Files:** `src/storage/graph/ifa/adjacency.rs`, `src/storage/graph/ifa/store.rs`

1. Update `IfaStore::lookup_type()` to handle inline buckets
2. Update `IfaAdjacency::get_neighbors()` to return inline entries directly
3. Add `IfaStore::iter_types()` inline support
4. Add integration tests for inline reads

### Phase 4: Update Write Path (Est: 3 hours)

**Files:** `src/storage/graph/ifa/adjacency.rs`, `src/storage/graph/ifa/store.rs`

1. Add `promote_to_segment()` helper
2. Update `IfaAdjacency::add_edge()` with inline-first logic
3. Update `IfaAdjacency::remove_edge()` to handle inline entries
4. Handle promotion threshold logic
5. Add integration tests for inline writes

### Phase 5: Handle Edge Cases (Est: 2 hours)

**Files:** Various

1. Handle deletion of inline entries (may need to keep inline or promote)
2. Handle visibility queries on inline entries
3. Handle concurrent access patterns
4. Update `NodeAdjPage` for true IFA path

### Phase 6: Testing & Benchmarks (Est: 2 hours)

1. Run existing IFA tests, fix any failures
2. Add new benchmark comparing inline vs external
3. Run `ifa_bench` to measure improvement
4. Update `ifa_validation` integration tests

---

## Migration Strategy

### Backward Compatibility

- **Read path:** Check inline flag before interpreting bucket data
- **Existing data:** External segments continue to work unchanged
- **New data:** Starts inline, promotes when needed

### No Migration Required

The inline format is additive - existing external segments are unaffected.
New edges on low-degree nodes will use inline storage automatically.

---

## Expected Results

### Performance Improvements

| Scenario | Current | After | Improvement |
|----------|---------|-------|-------------|
| Read 1 neighbor | 2 page reads | 1 page read | **2x faster** |
| Insert 1st edge | B-tree + segment alloc | B-tree only | **~40% faster** |
| Insert 2nd edge | B-tree + CoW + alloc | B-tree only | **~50% faster** |
| Insert 4th edge | B-tree + CoW + alloc | Promote + alloc | Same (one-time) |

### Space Savings

| Node Type | Current | After | Savings |
|-----------|---------|-------|---------|
| 1 edge | 154 bytes | 72 bytes | **53%** |
| 2 edges | 186 bytes | 72 bytes | **61%** |
| 3 edges | 218 bytes | 72 bytes | **67%** |
| 10 edges | 402 bytes | 402 bytes | 0% (external) |

---

## Files to Modify

| File | Changes |
|------|---------|
| `src/storage/graph/ifa/types.rs` | Inline entry types, bucket extensions |
| `src/storage/graph/ifa/store.rs` | Inline-aware lookup/upsert |
| `src/storage/graph/ifa/adjacency.rs` | Inline read/write paths |
| `src/storage/graph/ifa/node_adj_page.rs` | True IFA inline support |
| `src/storage/graph/ifa/segment_manager.rs` | Promote helper |
| `tests/integration/ifa_validation.rs` | New inline tests |
| `src/bin/ifa_bench.rs` | Inline benchmarks |

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Complexity in encode/decode | Comprehensive unit tests, fuzz testing |
| Promotion overhead | Only happens once per node at threshold |
| MVCC visibility for inline | Inherit from header xmin, promote on delete |
| Concurrent promotion races | Rely on B-tree CoW semantics |

---

## Appendix: Current IFA Structures

### NodeAdjHeader Layout (72 bytes)

```
+------------------+------------------+-----+------------------+
| Bucket 0         | Bucket 1         | ... | Bucket 5         |
| type | head_ptr  | type | head_ptr  |     | type | head_ptr  |
| 4B   | 8B        | 4B   | 8B        |     | 4B   | 8B        |
+------------------+------------------+-----+------------------+
```

### AdjSegment Layout

```
+-------------------+
| AdjSegmentHeader  |  50 bytes
| - owner (8B)      |
| - dir (1B)        |
| - type_id (4B)    |
| - xmin/xmax (16B) |
| - prev_ver (8B)   |
| - next_ext (8B)   |
| - count (4B)      |
+-------------------+
| AdjEntry[0]       |  32 bytes
| AdjEntry[1]       |  32 bytes
| ...               |
+-------------------+
```

### AdjEntry Layout (32 bytes)

```
+------------+----------+----------+----------+
| neighbor   | edge     | xmin     | xmax     |
| 8 bytes    | 8 bytes  | 8 bytes  | 8 bytes  |
+------------+----------+----------+----------+
```
