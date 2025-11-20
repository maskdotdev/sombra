<!-- plan-id: mvcc-versioning -->
# MVCC Version Chain Plan

## Goal
Implement head-in-primary + side-chain MVCC storage, tombstone heads, and on-the-fly visibility filtering with optional vacuum.

## Tasks

- [x] Add `VersionPtr`/version-log schema and integrate with WAL/IO.
- [x] Extend node/edge payload encoders to store `VersionHeader + prev_ptr + payload`.
- [x] Update graph write paths (insert/update/delete) to append old heads to the version log and install new heads/tombstones using commit IDs.
- [x] Update adjacency and secondary index writers to stay insert-only (no delete) and ensure uniqueness checks consult MVCC metadata.
- [x] Implement `visible_version(snapshot_ts)` helper and switch graph/index readers to filter using `begin_ts/end_ts`, skipping tombstones.
- [ ] Add writer-intent bits / conflict detection so concurrent writers see existing heads.
  - Adjacency B-tree writers now mark entries as pending intents and only clear the bit once both directions have been installed, so inserts are atomically published even when readers race with the batch.
- [x] Track `global_xmin` / retention window and hook up a basic vacuum that prunes version logs and stale heads.
- [x] Extend WAL/recovery to replay version-log appends plus head updates atomically.
  - Version log entries are now validated across reopen via a crash-style integration test, ensuring head updates and their historical copies survive pager recovery.
