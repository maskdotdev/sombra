## Leaf Record Refactor & Binary Search Plan

> Objective: replace delta-encoded leaf keys with a binary-search-friendly layout so Phase 1 (borrowed comparisons + O(log n) lookups) is achievable without reconstructing prefixes; ensure correctness through tests/benchmarks and prevent regressions.

---

### Phase 0 — Specs & Guardrails

1. **Format definition**
   * Document the new on-page encoding: `varint key_len | varint val_len | key | value` (no prefix deltas). Full invariants live in [`leaf-record-format.md`](leaf-record-format.md).
   * Reserve header `flags` bit (`LEAF_FLAG_PLAIN_RECORDS = 0x01`) during rollout; Phase 3 retires the bit once all leaves are rewritten.
2. **Migration policy**
   * Since the DB is unreleased, we can block opening pre-refactor files; `storage::core::Db::bail_legacy_leaf_layout` carries the canonical error message.
   * Decide whether inserts rewrite a page immediately (recommended) or gate by config.
3. **Acceptance**
   * RFC doc reviewed; checkbox list committed alongside this plan.

### Phase 1 — Encode/Decode & Layout Rewrite

1. **Page module updates**
   * Add helpers for plain records (length bounds, encode/decode).
   * Update `LeafRecordRef` to expose total key bytes directly.
2. **B-tree writer changes**
   * Rework `build_leaf_layout`, splits, merges, borrow logic to emit full keys.
   * Remove `prefix_len` branching except when reading legacy pages (if any remain).
3. **Read path shims**
   * For interim compatibility, keep the old decoding path flagged behind `header.flags`.
   * Cursor/tests updated to consume plain keys first.
4. **Tests**
   * Update existing leaf layout/property tests; add corrupt-length cases to ensure validation works.

### Phase 2 — KeyCursor + Binary Search

1. **KeyCursor implementation**
   * Cursor struct that walks varints/byte slices without allocation (`storage/btree/key_cursor.rs`).
   * Bench micro-tests for varint parsing limits.
2. **Leaf search rewrite**
   * Replace linear scan with slot-directory binary search when the page advertises “plain keys”.
   * Fall back to old scan if flag unset (temporary).
3. **Metrics**
   * Extend instrumentation to record how many pages used each path for visibility.

### Phase 3 — Cleanup & Toggle Removal

1. **Drop legacy path**
   * Once all code writes the new format, delete the prefix-compressed encoding and flag checks (the bit remains reserved for detecting truly old files).
   * Shrink tests to cover only the new invariant; remove temporary instrumentation that compared plain vs legacy paths.
2. **Codebase sweep**
   * Remove dead helpers (`shared_prefix_len`, `prefix_len` checks, etc.).

### Phase 4 — Validation & Regression Gates

1. **Unit / Property tests**
   * Ensure every updated module has revised tests (leaf layout, cursor, tree insert/delete).
2. **Integration tests**
   * Run `cargo test --all`, existing benches, and fuzz targets touching B-tree pages.
3. **Benchmark verification**
   * Re-run `compare-bench` matrix (`reads`, `inserts`, `mixed`, `mixed --tx-mode read-with-write`) and update `bench/baseline.md`.
   * Confirm instrumentation counters reflect O(log n) behavior (key decode counts drop to ≈log₂(slot_count)).
4. **CI hook**
   * Document required commands (fmt, clippy if applicable, `cargo test`, bench runs).

### Task Checklist (per phase)

- [x] Phase 0 spec doc & flag definition merged.
- [x] Phase 1 implementation + tests green.
- [x] Phase 2 KeyCursor/binary search landed with perf evidence.
- [x] Phase 3 cleanup removes old prefix logic.
- [x] Phase 4 validation artifacts (tests, bench outputs, updated baseline) recorded; no regressions observed.

Keep this file updated as work lands so we can track progress alongside existing build plans.
