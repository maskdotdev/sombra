<!-- Segmented WAL implementation plan -->

# Segmented WAL Rollout Plan

## Phase 1 – Foundations & Planning

1. Capture breaking-change requirements in docs (manifest format, directory layout, error messages, config validation) so reviewers have a single spec.
2. Define Rust data structures for manifest/cookie/segment headers plus enums for WAL errors (e.g., `LegacyWalDetected`).
3. Update `PagerOptions` validation + CLI/config schema to require `wal_segment_size_bytes > 0`.

## Phase 2 – Pager/Admin Path Updates

1. Teach `Pager::open/create` to require a `db-wal/` directory.
2. Emit the explicit `ERR_LEGACY_WAL` if a monolithic `db-wal` file is present.
3. Update admin helpers (`wal_path`, `wal_cookie_path`, stats/vacuum tooling) to treat WAL as a directory.

## Phase 3 – Segmented WAL Core

1. Replace `Wal` backend with a `SegmentedWal` manager:
   - Manifest read/write, directory scaffolding.
   - Segment allocator (active + ready + recycle queues).
   - Segment header encode/decode + CRCs.
2. Implement append/reset/iterate/read APIs using `(segment_id, offset)` pointers.
3. Record WAL reuse metrics when recycling segments.

## Phase 4 – Background Preallocator & ENOSPC Handling

1. Add background worker that maintains `wal_preallocate_segments` ready segments.
2. When ready queue empty or `posix_fallocate` fails with `ENOSPC`, surface the new metric/flag and block new commit batches until space frees.
3. Provide hooks for checkpoint trigger when WAL space pressure is active.

## Phase 5 – Migration CLI & Docs

1. (omitted; legacy single-file WAL support removed)
2. (omitted; legacy single-file WAL support removed)
3. Update docs/release notes with upgrade guidance about segmented WAL being mandatory.

## Phase 6 – Testing & Validation

1. Unit/integration tests: manifest parsing, segment reuse, iterator, crash recovery with cookie, ENOSPC path.
2. Update existing WAL tests to use the segmented layout.
3. Run `cargo fmt`, `cargo check`, and targeted integration tests.
