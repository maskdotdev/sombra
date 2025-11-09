# Leaf Allocator Performance Investigation (2025-11-09)

This note documents the current state of the in-place insert path, why it is
still ~2× slower than earlier experiments, and the concrete work we need to
prioritize to regain the lost performance headroom.

## 1. Current Signals

Recent local runs (see `bench/results/2025-11-09-inplace-study.md`) show:

| Config | Time | Ops/s | Insert Avg (µs) | Key Decodes | Allocator Metrics |
|--------|------|-------|-----------------|-------------|-------------------|
| `--btree-inplace` | 38.98 ms | 256,564 | 3.026 | 95,299 | 1 compaction / 53 failures / 18 bytes moved |
| fallback rebuild | 113.43 ms | 88,156 | 10.577 | 2,727,056 | 0 / 0 / 0 |

Key takeaways:

- In-place edits are still ~3× faster than rewriting every leaf, but we lost the
  earlier ~24 ms result once `LeafAllocator` landed.
- Even in a workload with no deletes we record 53 allocator failures and a
  compaction, signaling that `LeafAllocator::new` repeatedly discovers capacity
  pressure before every insert.
- `rebalance_in_place` stays at zero because the inserts-only benchmark never
  touches borrow/merge paths; new allocator metrics are therefore our primary
  view into the hot path.

## 2. Root Causes

Code review points to three dominant costs:

1. **Allocator instantiation copies full metadata every op.** Every
   `try_insert_leaf_in_place` first builds a `SlotView` (constructing
   `SlotExtents`) and then immediately spins up a `LeafAllocator`, which rebuilds
   the same extent table plus a free-region list (`src/storage/btree/tree/definition/leaf.rs:142-217`
   and `src/storage/btree/tree/definition/leaf_allocator.rs:24-44`). We pay two
   O(n) scans before even attempting the insert.

2. **Allocator state is throwaway.** `LeafAllocator::new` recomputes offsets,
   re-sorts extents, and rebuilds free lists from scratch on every call
   (`leaf_allocator.rs:209-274`). As soon as the insert finishes we drop that
   work, so the next insert repeats the same rebuild. This also explains the
   allocator failures: the free list never persists low-fragmentation ordering.

3. **Splits/merges still rely on the legacy rebuild helper.** We still call
   `build_leaf_layout` in the main insert loop and during splits/merges
   (`src/storage/btree/tree/definition/leaf.rs:194-332`,
   `maintenance.rs:420-860`, `985-1120`). This keeps `memcopy_bytes` high and
   forces us to materialize whole `Vec<(Vec<u8>, Vec<u8>)>` snapshots, undoing
   much of the allocator’s intended win.

## 3. Action Plan

### Phase A — Instrument & Observe

1. Landed: compare-bench now prints allocator compactions/failures/bytes moved
   so regression hunting is easier (`src/bin/compare_bench.rs` output).
2. Next: add `LeafAllocator` trace spans/stat counters to confirm how often we
   rebuild metadata versus reusing cached state (e.g., sample how long
   `LeafAllocator::new` takes, how many free regions exist per page).

### Phase B — Remove Redundant Rebuilds

1. **(Done)** Persist allocator state per page visit. We now stash per-leaf
   snapshots inside the write-transaction extension so repeated inserts/deletes
   reuse the cached `slot_meta` without rescanning slot directories.
2. **Share slot extents with allocator build.** Extend `SlotView` so it can hand
   its already-built `SlotExtents` into `LeafAllocator::new`, avoiding even the
   initial scan when the cache is cold.
3. **Avoid repeated `Vec` allocations.** Reuse a small `Vec<u8>` scratch buffer
   for `record` encoding and consider SmallVec for the `entries` list when we do
   need rebuild snapshots.

### Phase C — Make Allocator Universal

1. **Switch splits to `LeafAllocator`.** When a leaf needs to split, feed the
   allocator with half ranges instead of rebuilding via `build_leaf_layout`.
   This eliminates the copy-heavy `LeafSplitBuilder` path and lets us compact
   only when necessary.
2. **(In progress)** Deletes + borrow/merge now reuse the allocator cache, but
   splits still fall back to `build_leaf_layout`. Port the split logic and then
   delete the rebuild helpers entirely.
3. **Flip `BTreeOptions::in_place_leaf_edits` to default `true`.** With the old
   path removed there is no configuration switch; we can migrate existing trees
   once the allocator is stable.

### Phase D — Regression Guardrails

1. Extend `compare-bench` CI runs to record allocator metrics and alert when
   compactions/failures spike.
2. Add fuzz/regression tests that hammer alternating insert/delete workloads
   with the allocator on, ensuring fragment-heavy cases stay under budget.
3. Capture flamegraphs before/after each phase to verify `LeafAllocator::new`
   shrinks as expected.

## 4. Immediate Tasks

1. Implement SlotExtents handoff + allocator scratch cache (Phase B items 1–2).
2. Port split path to the allocator, reusing the same metadata (Phase C.1).
3. Add allocator-specific telemetry to tests/benchmarks so we can assert no
   allocator failures occur on steady workloads (Phase D.1).

With these queued up we should be able to close the gap between the present
~39 ms inserts-only run and the earlier 24 ms experiment while keeping the
hot-path deterministic and instrumentation-rich.
