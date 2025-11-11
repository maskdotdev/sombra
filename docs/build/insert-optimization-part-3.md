# Insert Optimization – Part 3

Goal: eliminate the rebuild-on-every-miss fallback by making in-place edits universal. This plan assumes the SlotExtents cache from Part 2 is shipped and focuses on replacing the `build_leaf_layout` path with an always-on incremental allocator.

## Phase 0 – Research & Guardrails
- Enumerate every path that currently bails out of `try_insert_leaf_in_place` or relies on `build_leaf_layout` (leaf insert, delete, borrow/merge, maintenance helpers, split prep). Record the concrete failure reason for each (contiguous space check, fence mismatch, replacement, fragmentation, split prep).
- Collect workload telemetry (compare-bench + targeted traces) to quantify how often each guardrail fires so we can sequence the work by impact.
- Write down invariants the allocator must uphold: sorted slots, accurate fences, non-overlapping record extents, corruption detection hooks.

## Phase 1 – Incremental Layout Design
- Choose the per-page storage model (log-structured region, free-list allocator, or hybrid) that supports arbitrary inserts/deletes without full rewrites.
- Define metadata structures (e.g., `LeafAllocator`, `FreeRegion`) and how we keep them heap-free (SmallVec on stack, bounded array in the page).
- Specify fence maintenance rules for variable-length keys and how low/high fences update when first/last keys move.
- Document API sketches/invariants and circulate for a quick review before coding.

## Phase 2 – Allocator & Instrumentation
- Implement allocator helpers for the payload: allocate spans, free spans, compact/defragment when needed, and rewrite fences safely.
- Add instrumentation counters (allocator compactions, bytes moved in place, allocation failures) so we can verify the new hot path behaves.
- Extend fuzz/property tests to hammer the allocator with random insert/delete sequences, ensuring invariants hold and corruption is detected.

## Phase 3 – Mutation Paths
- Refactor `try_insert_leaf_in_place` to use the allocator so it succeeds for new keys, overwrites, and first-key changes (no special-case bailouts).
- Update delete paths to reclaim space via the allocator, including low-fence adjustments and tombstone handling.
- Ensure APIs like `put_many`, cursor rewrites, and any leaf-editing helper reuse the allocator instead of rebuilding.

## Phase 4 – Split/Merge & Maintenance
- Rework split/merge logic to gather left/right halves using live allocator state instead of `build_leaf_layout`. If compact images are needed for new sibling pages, make that an explicit helper invoked only during splits.
- Update maintenance routines (rebalance, borrow, merge, snapshots) so none of them depend on the legacy rebuild helpers.
- Add tests that trigger splits/merges on heavily fragmented pages to validate allocator metadata across page boundaries.

## Phase 5 – Remove Legacy Layout
- Delete or gate `build_leaf_layout`, `apply_leaf_layout`, and related helpers once all call sites are migrated.
- Flip `BTreeOptions::in_place_leaf_edits` to default true (or drop the flag) so every tree uses the new allocator automatically. ✅ (flag removed; allocator path is always on)
- Update docs/changelogs describing the new layout strategy and the migration expectations for existing trees.

## Phase 6 – Validation & Rollout
- Rerun the full benchmark matrix (reads/mixed/inserts, multiple `commit_every`, `put_many`) with `SOMBRA_PROFILE=1`, capturing the new allocator metrics plus `slot_extent` timings.
- Stress randomized workloads with deletes + inserts to confirm the allocator never requires a rebuild and corruption detection stays intact.
- Document rollout guidance: monitoring hooks, metrics to watch, and fallback plan (if any) before declaring the rebuild path removed.
