<!-- plan-id: mvcc-versioning -->
# MVCC Version Chain Plan

## Goal
Implement head-in-primary + side-chain MVCC storage, tombstone heads, and on-the-fly visibility filtering with optional vacuum.

## Tasks

- [x] Add `VersionPtr`/version-log schema and integrate with WAL/IO.
- [x] Extend node/edge payload encoders to store `VersionHeader + prev_ptr + payload`.
- [ ] Update graph write paths (insert/update/delete) to append old heads to the version log and install new heads/tombstones using commit IDs.
- [ ] Update adjacency and secondary index writers to stay insert-only (no delete) and ensure uniqueness checks consult MVCC metadata.
- [ ] Implement `visible_version(snapshot_ts)` helper and switch graph/index readers to filter using `begin_ts/end_ts`, skipping tombstones.
- [ ] Add writer-intent bits / conflict detection so concurrent writers see existing heads.
- [ ] Track `global_xmin` / retention window and hook up a basic vacuum that prunes version logs and stale heads.
- [ ] Extend WAL/recovery to replay version-log appends plus head updates atomically.
