---
title: Pager Snapshot Design
---

This note ties the fresh code paths together so we can keep iterating on MVCC snapshots.

## Goals

1. **Expose explicit read semantics** — callers can now request either a checkpoint-only snapshot or the latest committed LSN via `ReadConsistency` (`src/primitives/pager/pager.rs:120-138`).
2. **Track newest visible LSN** — `Pager::latest_committed_lsn()` (`src/primitives/pager/pager.rs:1968-1983`) uses an `AtomicU64` that is advanced after each successful commit (`src/primitives/pager/pager.rs:1560-1668`). This is the anchor for read-committed snapshots.
3. **Materialize uncheckpointed state** — `Pager::get_page` replays WAL frames when the reader’s snapshot LSN exceeds the last checkpoint (`src/primitives/pager/pager.rs:1974-2054`). If the page is cached with only checkpoint-pending data, the reader can grab the frame directly; otherwise it rebuilds the page by scanning WAL and applying the latest frame ≤ the snapshot LSN.

## Flow Overview

1. Writer commits → `commit_txn` flushes WAL frames, updates `latest_visible_lsn`, and keeps page frames marked `pending_checkpoint`.
2. Reader calls `pager.begin_latest_committed_read()` which acquires the lock and pins `ReadGuard.snapshot_lsn` to the `latest_visible_lsn`.
3. `get_page` chooses between:
   - Cached frame (only if it does not contain uncommitted data and, when `pending_checkpoint`, the reader asked for the latest committed snapshot), or
   - Base image from the data file plus an overlay built from WAL frames newer than the checkpoint but ≤ the reader’s snapshot.

## Next Steps

- Cache WAL overlays (per page) to avoid reiterating over the entire log for hot pages.
- Share the `ReadConsistency` API with higher layers so graph/index readers can opt into read-committed mode once the pager is fully MVCC-aware.
- Carry the snapshot LSN into commit-table lookups so storage structures can reject versions whose `begin/end` boundaries lie outside the guard’s visibility window.
