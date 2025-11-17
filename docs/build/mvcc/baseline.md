---
title: MVCC Baseline — Pager, Concurrency, and Storage Flow
---

This document captures the current single-writer design so MVCC work can anchor to real code paths. Line references follow the repository layout.

## 1. Pager & Locking Flow

- **SingleWriter Coordinator** — `src/primitives/concurrency/mod.rs:12-160` owns a file-backed lock with independent byte ranges for readers, writers, and checkpoints. Readers do not block writers today; the writer slot simply guarantees only one writer transaction at a time.
- **WriteGuard Lifecycle** — `src/primitives/pager/pager.rs:347-433` shows how a writer reserves an LSN (monotonic `inner.next_lsn`) and tracks dirty/original pages plus freelist snapshots so rollback can restore cached frames.
- **Commit Sequence** — `src/primitives/pager/pager.rs:1447-1700` acquires buffered page images, writes them to the WAL via `WalCommitter`, then schedules checkpoints. Dirty cache entries flip to `pending_checkpoint=true` so checkpoints know which pages still need flushing.
- **Checkpoint Guarding** — `src/primitives/concurrency/mod.rs:138-160` and `src/primitives/pager/pager.rs:1657-1870` outline how checkpoints block both new readers and writers before replaying WAL frames into the main database file and advancing `meta.last_checkpoint_lsn`.

## 2. Read Path & Snapshot Semantics

- **ReadGuard Construction** — `Pager::begin_read` (`src/primitives/pager/pager.rs:2071-2080`) pins every reader to `inner.meta.last_checkpoint_lsn`, so snapshots only advance after a checkpoint completes.
- **Page Fetch Rules** — `Pager::get_page` (`src/primitives/pager/pager.rs:1961-2030`) serves cache hits only when the requesting guard’s snapshot matches the checkpoint LSN. If the cached frame is dirty or pending a checkpoint, the reader is forced to read from the on-disk page image to ensure no uncheckpointed data leaks into the snapshot.
- **Implication** — Readers never observe commits newer than the last checkpoint, so “latest committed” is synonymous with “last checkpointed.” Bridging this gap is the first MVCC requirement.

## 3. Storage Mutation Entry Points

- **Graph Write Helpers** — `src/storage/graph.rs:1270-1295` obtains commit IDs from `WriteGuard::reserve_commit_id()` and stamps new `VersionHeader` values, but the headers always use `end = COMMIT_MAX` (zero), so tombstones are the only version boundary recognized today.
- **Indexes** — Label and property indexes (`src/storage/index/*.rs`) wrap payloads in `VersionedValue`, yet inserts and deletes mutate the same logical key in place. The lack of version chains means older snapshots cannot survive once a commit overwrites a page.
- **Version Header Encoding** — `src/storage/mvcc.rs:4-144` already defines the binary layout for `VersionHeader` plus helpers like `visible_at`, which storage readers rely on when filtering postings or adjacency entries.

## 4. WAL & Recovery Hooks

- **WAL Layout** — `src/primitives/wal/mod.rs:24-520` documents the 32-byte file and frame headers. Frames contain whole-page images; no commit metadata or version pointers are persisted yet.
- **Recovery** — On startup `recover_database` (`src/primitives/pager/pager.rs:203-238`) iterates WAL frames newer than `meta.last_checkpoint_lsn`, applies them to the main file, and then truncates the WAL. Since read snapshots only reference `last_checkpoint_lsn`, recovery never exposes intermediate versions either.

## Takeaways

1. Snapshot visibility is hardwired to checkpoint state; MVCC must decouple read guards from checkpoints and supply a “latest committed” LSN.
2. Once a commit overwrites a page, no historical version remains except in WAL; to serve stable snapshots across commits we need persistent version chains or page shadowing.
3. WAL currently stores only opaque page frames, so commit tables / version metadata must live elsewhere (pager-resident structures with WAL redo records) to survive crashes.

This baseline feeds directly into MVCC design tasks: extend `ReadGuard` semantics, teach the pager how to surface uncheckpointed data safely, and plug real metadata into the `VersionHeader` scaffolding already used across storage components.
