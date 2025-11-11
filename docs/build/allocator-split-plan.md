# Allocator-Driven Leaf Splits

This note captures the plan for making `LeafAllocator` drive leaf splits end-to-end so we can drop the rebuild fallback and (as of now) keep `in_place_leaf_edits` always-on.

## 1. Snapshot Enrichment
- Extend `InPlaceInsertResult::NotApplied` to carry the pending insert context:
  - target slot index and fence update intent
  - encoded record bytes (ready to drop into the allocator)
- Teach `LeafAllocatorSnapshot` to expose lightweight iterators over existing records via `LeafRecordRef`, plus helper methods for reporting arena/fence boundaries.

## 2. Allocator Rebuild APIs
- Add `LeafAllocator::rebuild_from_records<'a, I>(low, high, iter)` that consumes an iterator of record slices instead of allocating `Vec<(Vec<u8>, Vec<u8>)>`.
- Add `LeafAllocator::split_into(left, right, split_idx, pending_insert)` which:
  - Applies the pending insert while streaming records
  - Produces rebuilt layouts for both halves, returning fresh snapshots to cache
  - Computes new low/high fences and min keys for parent propagation

## 3. Split Path Refactor
- When `try_insert_leaf_in_place` fails with a snapshot, call the new allocator split helper instead of cloning entries.
- Reuse the returned snapshots:
  - Apply the left snapshot directly to the existing leaf page (and keep it cached)
  - Initialize the new right page via `LeafAllocator::from_snapshot` using the provided snapshot (no additional scans)
- Update sibling pointers & parent separators exactly as today, but assert allocator headers already have correct `free_start/free_end`.

## 4. Telemetry & Cache
- Record key-decoding / memcopy counters inside the snapshot iterator so metrics stay consistent.
- Ensure both left/right snapshots reenter the per-write allocator cache so follow-up inserts/deletes stay hot.

## 5. Validation
- Extend B-tree unit tests to cover split scenarios using the allocator-driven path (min keys, fence propagation, sibling wiring).
- `cargo check` + `SOMBRA_PROFILE=1 cargo run --release --bin compare-bench -- --mode inserts-only --docs 10000 --commit-every 10000 --btree-inplace` to confirm:
  - `allocator_builds` ≈ number of new pages (no unexpected rebuild spikes)
  - `allocator_failures` only occur when the allocator intentionally triggers a split
  - Slot-extent timing drops because we no longer reparse pages after allocator failures

With this in place, every leaf mutation (insert/delete/borrow/split) flows through `LeafAllocator`, removing the rebuild-only path and letting us keep `BTreeOptions::in_place_leaf_edits` permanently enabled.
