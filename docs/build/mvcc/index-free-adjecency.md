# Index-Free Adjacency Implementation Plan

**Goal**: Transform Sombra from B-tree-backed adjacency to true native graph storage with index-free adjacency.

**Core Principle**: For 99% of nodes, neighbor expansion should be:
- One node lookup -> in-cache type map -> direct jump to adjacency segment
- Zero extra seeks, no B-tree, minimal pointer chasing

---

## Phase 1: NodeAdjHeader Storage Foundation

**Objective**: Introduce the per-node type map infrastructure that enables O(1) adjacency lookups.

### 1.1 Design NodeAdjHeader Location
- [ ] **Decision**: Choose storage location for NodeAdjHeader
  - Option A: Dedicated "node heads" file with fixed slots per NodeId
  - Option B: Sidecar structure in existing meta pages
- [ ] Document decision rationale in ADR

### 1.2 Implement NodeAdjHeader Structure
```rust
// Target structure (K = 6 recommended)
NodeAdjHeader {
    InlineBuckets[0..K-1]   // K = 6 slots
    // Each entry:
    //   TypeId type;        // 0 = empty slot
    //   SegmentPtr head;    // 0 = unused
    //
    // Convention:
    //   - Empty slot: type=0, head=0
    //   - Last slot: if type == OVERFLOW_TAG, head points to overflow chain
}
```

- [ ] Define `NodeAdjHeader` struct with inline buckets
- [ ] Define `OVERFLOW_TAG` constant for overflow detection
- [ ] Implement serialization/deserialization for NodeAdjHeader

### 1.3 Implement Overflow Structure
```rust
OverflowBlock {
    TypeBucket[0..M-1];      // sorted by TypeId
    OverflowBlockPtr next;   // 0 if last block
}

TypeBucket {
    TypeId type;
    SegmentPtr head;
}
```

- [ ] Define `OverflowBlock` and `TypeBucket` structs
- [ ] Implement overflow block allocation
- [ ] Implement overflow chain traversal (sorted linked list, binary search within blocks)

### 1.4 NodeId Free List Semantics
- [ ] Implement NodeId free list to keep IDs dense and reusable
- [ ] Ensure NodeId allocation/deallocation integrates with NodeAdjHeader slots

### 1.5 Type Map Lookup Implementation
- [ ] Implement inline bucket lookup (linear scan for small K)
- [ ] Implement overflow fallback when `OVERFLOW_TAG` detected
- [ ] Add unit tests for type map operations

**Exit Criteria Phase 1**:
- NodeAdjHeader can be created, stored, and retrieved for any NodeId
- Type lookups work for both inline (<=K-1 types) and overflow (>K-1 types) cases
- NodeId free list maintains dense ID allocation

---

## Phase 2: Adjacency Segments & CoW Updates

**Objective**: Implement the segment-based adjacency storage with copy-on-write semantics.

### 2.1 Define AdjSegment Structure
```rust
AdjSegment {
    // Identity & grouping
    NodeId owner;
    Dir dir;               // OUT / IN
    TypeId type;
    
    // MVCC
    TxId xmin;             // creator transaction
    TxId xmax;             // 0 == infinity (not superseded)
    
    // Version chaining
    SegmentPtr prev_version;   // previous version for (node,dir,type)
    SegmentPtr next_extent;    // next page in same version (high degree)
    
    // Stats
    u32 entry_count;
    
    // Entries (sorted by neighbor)
    AdjEntry[entry_count];
}

AdjEntry {
    NodeId neighbor;       // dst for OUT, src for IN
    EdgeId edge;           // identifies the edge row
}
```

- [ ] Define `AdjSegment` struct on top of existing pager
- [ ] Define `AdjEntry` struct
- [ ] Implement segment allocation/deallocation
- [ ] Implement extent chaining for high-degree nodes

### 2.2 Implement CoW Insert Algorithm
Per side (source OUT, target IN):

1. [ ] Look up `NodeAdjHeader` for (node, dir) and bucket for TypeId
2. [ ] Get `old_head = bucket.head`
   - If `old_head == 0`: creating first segment version
   - Otherwise: `old_head` is latest (SWMR simplification)
3. [ ] Clone current version:
   - [ ] Allocate new page(s) for `new_seg`
   - [ ] Copy header and entries from current visible version
   - [ ] Set: `xmin = current_tx`, `xmax = 0`, `prev_version = old_head`
   - [ ] Insert `(neighbor, edge)` in sorted order
   - [ ] Handle extent chain cloning for high-degree nodes
4. [ ] Update `bucket.head = new_seg` in transaction view
5. [ ] **Shadow-write**: Mirror insertion to legacy B-tree adjacency tables
6. [ ] Integrate with commit flow

### 2.3 Implement CoW Delete Algorithm
- [ ] Clone current version to `new_seg`
- [ ] Remove `(neighbor, edge)` from entries
- [ ] Handle `entry_count == 0` case (set `bucket.head = 0` or keep empty segment)
- [ ] Set: `prev_version = old_head`, `xmin = current_tx`, `xmax = 0`
- [ ] Shadow-write to legacy B-tree

### 2.4 Integration Tests
- [ ] Test single edge insert/delete
- [ ] Test multiple edges same type
- [ ] Test multiple edge types (inline bucket scenarios)
- [ ] Test overflow trigger (>K-1 types)
- [ ] Test high-degree node extent chaining

**Exit Criteria Phase 2**:
- Edge inserts/deletes create proper CoW segment versions
- Both segment storage AND legacy B-tree are updated (shadow-write)
- Version chains correctly link `prev_version` pointers

---

## Phase 3: SWMR Semantics & Snapshot Visibility

**Objective**: Wire snapshot isolation into adjacency reads so readers see consistent views.

### 3.1 Implement Reader Snapshot Latching
- [ ] Readers acquire `snapshot_ts` or `snapshot_gen` at transaction start
- [ ] NodeAdjHeader reads respect snapshot boundaries
- [ ] Bucket head pointers resolved as of snapshot

### 3.2 Implement Segment Version Resolution
- [ ] Walk `prev_version` chain to find visible segment
- [ ] Visibility check: first `AdjSegment` where `(xmin, xmax)` satisfies snapshot
- [ ] Integrate with existing TST (Transaction Status Table)

### 3.3 Implement Adjacency Backend Config
```
adjacency_backend = btrees | segments | dual
```

- [ ] `btrees`: Current behavior (read from B-tree)
- [ ] `segments`: Read from segments only
- [ ] `dual`: Run both, cross-check for correctness validation

### 3.4 Dual-Mode Validation
- [ ] Implement comparison logic for dual mode
- [ ] Log discrepancies for debugging
- [ ] Add metrics for read path performance comparison

### 3.5 Writer Commit Flow
- [ ] Writer allocates segments, updates NodeAdjHeader in private uncommitted view
- [ ] On commit:
  - [ ] Write dirty pages to WAL
  - [ ] Fsync WAL
  - [ ] Update TST to COMMITTED
  - [ ] Update root generation counter in meta
- [ ] New readers see new adjacency layout; old readers see old state

**Exit Criteria Phase 3**:
- Readers with different snapshots see correct segment versions
- Dual mode validates segment reads match B-tree reads
- No partial updates visible to any reader

---

## Phase 4: Garbage Collection & Format Versioning

**Objective**: Reclaim dead segment versions and establish format migration path.

### 4.1 Extend Vacuum for Segment GC
- [ ] Walk segment `prev_version` chains
- [ ] Identify reclaimable segments:
  - `xmax` is committed AND below GC horizon
  - `commit_ts < oldest_active_snapshot_ts`
- [ ] Reclaim pages from dead segments
- [ ] Update free list

### 4.2 Root Generation Tracking
- [ ] Track which root generations are still in use by active readers
- [ ] Free pages only reachable from fully obsolete generations

### 4.3 Format Versioning & Feature Flags
Add to meta/pager header:
- [ ] `adjacency_mode` field: `BTree_Only | Hybrid | Native_Only`
- [ ] `segment_adjacency_present` flag
- [ ] Version compatibility checks on database open

### 4.4 Migration Tooling
- [ ] Implement offline migration: BTree_Only -> Hybrid
- [ ] Implement online migration validation
- [ ] Implement rollback path: Hybrid -> BTree_Only (if segments corrupted)

**Exit Criteria Phase 4**:
- Dead segment versions are reclaimed by vacuum
- Database files include format version for adjacency mode
- Migration between modes is supported

---

## Phase 5: Deprecate B-Tree Adjacency (Native-Only Mode)

**Objective**: Remove shadow-writes and B-tree dependency for adjacency.

### 5.1 Confidence Building
- [ ] Run dual-mode validation in production/staging for extended period
- [ ] Benchmark segment-only reads vs B-tree reads
- [ ] Document performance characteristics

### 5.2 Remove Shadow-Write Path
- [ ] Add config to disable B-tree shadow-writes
- [ ] Update insert/delete to skip B-tree when `adjacency_mode == Native_Only`
- [ ] Remove B-tree adjacency tables from new databases

### 5.3 Migration for Existing Databases
- [ ] Implement `VACUUM FULL` variant that rebuilds without B-tree adjacency
- [ ] Document migration procedure
- [ ] Add compatibility warnings for downgrade attempts

### 5.4 Code Cleanup
- [ ] Remove B-tree adjacency read path
- [ ] Remove dual-mode validation code (or keep behind debug flag)
- [ ] Update documentation

**Exit Criteria Phase 5**:
- New databases use segment-only adjacency
- Existing databases can migrate to Native_Only mode
- B-tree adjacency code is removed or deprecated

---

## Phase 6: Super Node Optimization (Future)

**Objective**: Handle "Justin Bieber" nodes with millions of edges efficiently.

> **Note**: Only pursue this phase if benchmarks show extreme write cost for super nodes. The basic per-type segment + CoW design does not block this optimization.

### 6.1 Identify Super Node Threshold
- [ ] Benchmark CoW cost vs node degree
- [ ] Define threshold (e.g., >100K edges) for jumbo treatment

### 6.2 Implement Jumbo Adjacency
- [ ] Partition giant adjacency list into multiple segments per version
- [ ] Partitioning strategy: hash on neighbor or range-partition
- [ ] CoW at individual segment granularity (not entire list)

### 6.3 Jumbo Segment Management
- [ ] Track which partition a neighbor belongs to
- [ ] Update only affected partition on edge insert/delete
- [ ] Implement partition rebalancing if needed

**Exit Criteria Phase 6**:
- Super node edge updates don't require full list copy
- Read performance maintained for super node traversals

---

## Data Structures Reference

### NodeAdjHeader (Inline Hybrid)
```
+------------------+------------------+------------------+-----+------------------+
| Bucket 0         | Bucket 1         | Bucket 2         | ... | Bucket K-1       |
| type | head_ptr  | type | head_ptr  | type | head_ptr  |     | type | head_ptr  |
+------------------+------------------+------------------+-----+------------------+
                                                               ^
                                                               |
                                      If type == OVERFLOW_TAG, head_ptr -> OverflowBlock
```

### Segment Version Chain
```
bucket.head -> [AdjSegment v3] -> [AdjSegment v2] -> [AdjSegment v1] -> null
               xmin=103           xmin=102           xmin=101
               xmax=0             xmax=103           xmax=102
               prev_version ------^                  ^
                                  prev_version ------+
```

### Extent Chain (High Degree)
```
[AdjSegment page 1] -> [AdjSegment page 2] -> [AdjSegment page 3] -> null
 next_extent ----------^                      ^
                       next_extent -----------+
```

---

## Success Metrics

| Metric | Target | Phase |
|--------|--------|-------|
| Single-hop traversal latency | <1us (cache hot) | 3 |
| Memory per node overhead | <64 bytes | 1 |
| CoW write amplification | <2x for typical nodes | 2 |
| Dual-mode discrepancy rate | 0% | 3 |
| GC reclaim efficiency | >90% dead space reclaimed | 4 |
| Segment-only read parity | <=1.1x B-tree latency | 5 |

---

## Risk Register

| Risk | Mitigation | Phase |
|------|------------|-------|
| Segment corruption loses adjacency | Shadow-write to B-tree until Phase 5 | 2-4 |
| Super node CoW too expensive | Defer to Phase 6 jumbo optimization | 2 |
| Format version incompatibility | Explicit version flags, migration tooling | 4 |
| GC removes still-visible segments | Track oldest active snapshot strictly | 4 |
| Performance regression vs B-tree | Dual-mode benchmarking before Native_Only | 3-5 |
