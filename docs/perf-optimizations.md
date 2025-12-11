# Sombra-DB Performance Optimizations & Profiling Plan

## Potential Optimizations (ranked by impact/effort)
- **Reduce profiling overhead**: Granular timers add ~18% wall time on simple workloads (hundreds of thousands of `Instant::now()` calls). Make granular timers opt-in or sampling-based; keep only top-level scopes on by default.
- **Profile and optimize leaf splits**: In realistic runs we see ~4.5k splits; split path is unprofiled and likely heavy (page alloc, copy, parent updates). Add timers inside `split_with_allocator` (allocator build/split, page alloc, finalize) and consider reducing split frequency (fill factor, write-ahead buffering, or larger pages for big rows).
- **Avoid redundant node existence checks during bulk edge insert**: `ensure_node_exists` triggers ~160k leaf searches (68 ms in the 20k/80k edge run). Skip or batch these when the batch already owns the node set, or cache recently-created nodes.
- **Optimize adjacency put_many**: 160k adjacency entries take ~1.8 s (11 µs/entry). Explore sequential-insert fast path, larger page size for adjacency trees, or batching that keeps working set in cache (leaf cache reuse, fewer splits).
- **Reduce per-insert allocations/copies**: `into_snapshot` (SmallVec→Vec) and `fence_slices` Vec copies add ~10–15% in small cases. Consider arena/reuse for allocator snapshots and fence buffers.
- **Shrink slot allocator “other” cost for large rows**: With bigger edge rows, 91% of `leaf_insert` is unaccounted. Likely inside slot moves/compaction and split path. After profiling split path, consider strategies to minimize copying (append-mostly layout, log-structured leaves, or deferred compaction).
- **Index flush tuning**: Label/property `put_many` shares the same BTree bottlenecks as main writes. Investigate pre-sized pages or batched split handling to lower flush cost.

## Profiling Improvement Plan
- **Make granular timers opt-in**: Keep top-level scopes (`CreateNode`, `CreateEdge`, `BTreeLeafInsert`, `flush_deferred`) on by default; gate fine-grained leaf timers behind an env like `SOMBRA_PROFILE_GRANULAR=1`.
- **Sampling mode**: Optionally sample N% of operations to lower overhead while retaining signal on hot paths.
- **Split-path instrumentation**: Add timing inside `split_with_allocator` (page alloc, allocator split, finalize) to explain the current 91% “other” during large inserts.
- **Leaf allocator internals**: Time `reserve_for_insert/compact_with_gap/persist_slot_directory` to see copy/compaction cost per insert for large records.
- **Adjacency flush breakdown**: Add per-phase timers in `put_many` for adjacency trees (key encode already cheap; we need page-level costs inside `insert_into_leaf`).
- **Easy disable switch**: Honor `SOMBRA_PROFILE=0` to kill all profiling (already works); document it and make granular timers default-off to avoid accidental overhead.

## Next Investigation Steps
- Instrument leaf split path and allocator internals to attribute the 91% “other” in large-row inserts.
- Add a granular-profiling gate (`SOMBRA_PROFILE_GRANULAR`) and rerun benchmarks to measure overhead reduction.
- Prototype skipping `ensure_node_exists` when edge batches reference known-created nodes; measure impact on edge “other” time.
- Explore adjacency `put_many` optimizations (leaf cache reuse, sequential insert fast path) to reduce the ~1.8 s cost for 160k entries.
