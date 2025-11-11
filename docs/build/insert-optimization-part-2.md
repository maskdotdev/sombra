# Insert Optimization – Part 2

## Phase 0 – Baseline & Instrumentation
- Reproduce the current insert workload (`compare-bench --mode inserts-only …`) and capture every parameter, WAL stats, page metrics, and raw outputs for future comparison.
- Gather profiling in two passes: (a) CPU sampling (`dtrace profile-997` or Instruments) to confirm `record_slice_from_parts` hotness, (b) fine-grained internal timers/counters (`StorageProfileKind::BTreeLeafInsert/Search`, temporary scoped timers in `record_slice_from_parts` and friends) to measure call counts and average slot counts.
- Log acceptance gates that the optimization must hit or beat: ops/sec, µs/op, leaf split frequency, WAL/fsync counts, and the regression-test suite that must stay green.

## Phase 1 – Design & Review
- Enumerate candidate fixes with trade-offs:
  1. Expand slot entries to store both start and end (or length), yielding constant-time record slicing.
  2. Keep the on-disk format, but build a sorted per-page cache of slot offsets/extents shared across hot call sites.
  3. Hybrid: cache extents only for rebuild/split paths while leaving read-only paths untouched.
- For each option, document compatibility (existing data files), page-size impact, write amplification, corruption-detection implications, and estimated implementation complexity.
- Produce a short design doc outlining the chosen approach, invariants, migration/feature-flag strategy, and expected asymptotic improvements. Hold a quick review before coding.

## Phase 2 – Implementation
- Format change path:
  - Introduce a new slot directory version (`SlotDirectoryV2`) and a header flag/version bit so readers can distinguish formats.
  - Update page builders (`apply_leaf_layout`, split/merge logic, initialization) to emit the new slot structure.
  - Provide backward-compatible readers: existing pages continue to use V1, new trees default to V2 unless disabled via `TreeOptions`.
- Cache-only path:
  - Add a lightweight helper (e.g., `SlotExtents<'a>`) that precomputes sorted slot offsets once per page visit using stack storage (`SmallVec`).
  - Refactor `record_slice_from_parts` to leverage the precomputed extents in leaf search, insert, and internal traversal loops so each record lookup is O(1).
- Touch every consumer (cursor walks, search, insert, splits, merges) to ensure they reuse the new API and avoid redundant scans. Keep hot-path allocations off the heap; prefer stack buffers or smallvecs.

## Phase 3 – Verification
- Extend btree tests to cover old/new slot formats, mixed-version trees, fence-boundary edge cases, and corrupted slot metadata.
- Add property/fuzz tests that generate random page layouts, feed them through both old and new slicing logic (behind cfg/test harness), and assert identical record bytes and error conditions.
- Run the full `cargo test` suite plus clippy/fmt. Explicitly test large-slot pages to ensure no regressions in corruption detection.

## Phase 4 – Performance Validation & Rollout
- Re-run the baseline benchmark matrix (10k, 100k, 1M docs; varied `put-many` group sizes; in-place edits on/off) and capture ops/sec, µs/op, and profiler output to confirm hot spots migrate away from `record_slice_from_parts`.
- Stress-test for IO regressions (WAL frames, fsync count) and monitor page split/merge behavior to confirm no unexpected changes.
- Document results, migration notes (if format changed), and rollout guidance (feature flags, canary plan, monitoring hooks). Only remove any temporary instrumentation once metrics look good in staging/production.
