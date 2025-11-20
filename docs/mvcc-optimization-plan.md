<!-- MVP MVCC performance roadmap tailored to current modules (Pager, Graph, CommitTable, WAL) -->

# Sombra MVCC + WAL Performance Plan

This document captures the concrete optimization work required to lift MVCC write throughput and reduce WAL pressure. It references existing modules (`Pager`, `Graph`, `CommitTable`, `compare-bench`, `sombra mvcc-status`) so we can drop the plan straight into issues/PRs.

## 0. Goals & Success Criteria

**Primary outcomes**

- Reduce p50/p95 commit latency for OLTP workloads.
- Increase sustained WAL write throughput without violating durability semantics.
- Shrink version-log footprint and vacuum lag at steady state.
- Improve read-only latency via caching/inline histories.

**Quantitative targets** (pass/fail when instrumentation lands)

| Scenario | Target |
| --- | --- |
| Group commit enabled (`group_commit_max_writers=16`, `group_commit_max_wait_ms=2`) | p95 commit latency drops ≥40% vs. single-writer baseline |
| Async fsync enabled | Sustained write throughput ↑ ≥25% while induced fsync stalls do **not** stall writers |
| WAL batching/preallocation | fsyncs per 10 k commits ↓ ≥50% |
| Version compression + micro-GC | Version log bytes ↓ ≥30% at steady state (24 h retention) |
| Per-page version cache | p95 `Graph::load_version_entry` latency ↓ ≥30% on read-only microbench |

Instrumentation path: reuse `storage::profile_snapshot`, extend `sombra mvcc-status`, and teach `compare-bench` to emit CSV metrics.

## 1. Commit Cadence & WAL

### 1.1 Configurable group commit

- Extend `PagerOptions` (and CLI config) with:

```rust
pub struct PagerOptions {
    pub group_commit_max_writers: usize,   // 0/1 disables grouping
    pub group_commit_max_wait_ms: u32,     // 0 disables time-based gating
    pub async_fsync: bool,
    pub wal_segment_size_bytes: u64,
    pub wal_preallocate_segments: u32,
    // ... existing fields
}
```

- Add CLI flags & config keys (`pager.group_commit_max_writers`, etc.).
- Implement a **Commit Aggregator** task:
  - Writers append WAL frames into per-writer buffers, enqueue `CommitReq` into a bounded MPSC.
  - Aggregator batches up to `N` writers or waits `M` ms, writes frames via `pwritev`, marks commits `Logged`, then either fsyncs (sync mode) or hands off to async fsync worker.
  - Use admission control when queue nearly full; emit metrics (`commit.group.size`, `commit.group.wait_ms`).
- Update `CommitTable` entries with `Durability` enum (`Prepared`, `Logged`, `AckedNv`, `Durable`) and expose counts in `mvcc_status`.
- Tests: deterministic harness to freeze clocks and assert `N/M` triggers; loom/Jepsen-style tests for fairness under contention.

### 1.2 Async fsync + durable watermark

- Use a dedicated fsync worker thread:
  1. Aggregator writes frames, marks batch `AckedNv`, unblocks writers immediately.
  2. Worker `fdatasync`s WAL FD, updates a tiny durable watermark cookie (`wal.dwm`) with latest group sequence, `fdatasync`s the cookie.
  3. `CommitTable` flips matching commits to `Durable`.
- Crash semantics: on recovery, replay WAL up to last frame but apply only commits with `group_seq <= cookie`. Document as “Flush semantics when `async_fsync=on`”.
- CLI: `sombra mvcc-status` shows backlog (`acked_nv=NN`, `durable_group_seq=...`).
- Implementation notes:
  - Teach `CommitTable`/pager to track the latest durable LSN alongside the latest committed LSN so we can compute the backlog count (`latest_committed - durable`).
  - Expose the count via `Graph::mvcc_status` and surface it in the admin CLI plus metrics (`commit.acked_nv_backlog`, `commit.durable_lag_commits`).
  - Add crash tests that enqueue AckedNv commits, kill the process before the async fsync worker catches up, and assert recovery replays only through the durable watermark recorded in `wal.dwm`.
- Tests: kill after AckedNv, ensure only cookie-confirmed groups survive.

### 1.3 WAL preallocation & segment reuse

- Split WAL into fixed-size segments (`wal-%06d`).
- Preallocate `wal_preallocate_segments` segments with `posix_fallocate` and maintain a reuse queue for checkpointed segments.
- Batch writes (`pwritev`), align to 4 KiB, and fsync once per commit group.
- Metrics: `wal.reused_segments_total`, `wal.prealloc_failures_total`, `wal.batch_bytes_avg`.
- Implementation notes:
  - Maintain a queue of segment descriptors `{start_offset,len}` that become eligible after checkpoints/wal resets. When new writes need space, pop from the queue, reset the region, and continue appending without re-allocating from the filesystem.
  - Track how many segments are preallocated ahead of the append pointer. If the queue is empty and `truncate/allocate` fails, bubble a clear ENOSPC error so writers stall instead of corrupting data.
  - Record each reuse in `wal.reused_segments_total` and expose the current queue depth via metrics / `mvcc-status` to help operators size `wal_preallocate_segments` correctly.
- Tests: ensure no writer proceeds if reuse queue empty and disk full (bubble error up cleanly).

## 2. Version Chain & Storage Layout

### 2.1 Version compression infrastructure

- Introduce `VersionCodec` trait with pluggable strategies (`None`, `Prefix`, `Delta`).
- `Graph::log_version_entry` and `Graph::load_version_entry` call codec when writing/reading historical payloads.
- Apply only when payload length > threshold (e.g., 64 B) and compression ratio < 0.9.
- Metrics: `version_codec.bytes_in`, `version_codec.bytes_compressed` per codec.
- Tests: fuzz round-trip decode (prop tests) for chained versions.

### 2.2 Inline single-version histories

- Add `HistoryHdr` to primary node/edge records to store one historical copy inline.
- First update promotes to version log; until then, reads skip external lookups.
- Implementation change localized to `node.rs`/`edge.rs` encode/decode + `Graph::create/update` paths.
- Tests: mixed updates verifying head promotion + GC.

### 2.3 Per-page version cache

- Sharded lock-free ring buffer caching `(page_id, record_id)` → decoded version pointer.
- Use seqlock-style versioning (`gen` atomic) to avoid per-slot mutexes.
- Limit memory via `GraphOptions::version_cache_slots` & `version_cache_shards`.
- Stats: hit/miss counters surfaced through `storage::metrics` + CLI.
- Tests: loom/crossbeam concurrency tests, integration bench for read latency.

## 3. Index & Adjacency Pipelines

### 3.1 Deferred index flush (commit-time)

- For transactional writers with `TxMode::Commit`, buffer `IndexOp { kind, key, value }` per txn; apply via `BTree::put_many` right before commit.
- Ensure read-your-own-writes by probing staging buffers during txn lifetime.
- WAL: index writes already covered by LSN; no extra structures needed.
- CLI toggle: `--index-flush=immediate|on-commit`.
- Tests: correctness (index visibility matches storage), perf (reduced latch churn).

### 3.2 Adjacency `put_many`

- Add bulk adjacency insert/delete helpers that sort keys per page and lock each page once.
- Reuse `BTree::put_many` interface but localized to adjacency B-trees.
- Metrics: `adjacency.bulk.pages_touched`, `adjacency.bulk.lock_collisions`.
- Tests: contested writers hitting same node should report fewer lock acquisitions vs. single insert.

## 4. Commit Table & Snapshots

### 4.1 Writer intent IDs

- Reserve commit IDs only when ready to publish, not at txn start.
- Flow: `IntentId = CommitTable::reserve_intent()`, decode pages, then `CommitId = promote_intent(intent)`. Shortens time under commit lock.
- Update WAL metadata to store both IDs (intent only needed transiently for tracing).
- Tests: high-concurrency microbench verifying lower contention around `reserve_commit_id`.

### 4.2 Snapshot reuse & version cache integration

- Maintain pool of `ReadGuard`s tied to recent durable LSN; read-only threads lease/return them.
- Record stats (`mvcc.snapshot_pool.hit/miss`).
- Drop guards when durable watermark advances beyond their LSN.

### 4.3 mvcc-status telemetry

- Extend `Graph::mvcc_status` output (already used by CLI) with backlog, durable group seq, horizon commit, snapshot pool occupancy.
- CLI text view already prints commit table + slow readers; add backlog lines (AckedNv vs. Durable).

## 5. Garbage Collection & Vacuum

### 5.1 Adaptive cadence controller

- Inputs: `version_log_bytes`, retention target, `oldest_visible_commit`, growth rate.
- Control loop selects vacuum cadence: `Fast` (lagging), `Normal`, `Slow` (ahead). Persist choice to metrics.
- CLI `mvcc-status`: `vacuum { mode=fast lag_bytes=... horizon_commit=... }`.

### 5.2 Micro-GC (opportunistic trimming)

- When reading version chains, if suffix < global horizon and not referenced, schedule trim job.
- Maintain hazard pointers or epoch GC to ensure trimmed entries aren’t in use.
- Stats: `vacuum.micro.trim_ops_total`, `vacuum.micro.bytes_reclaimed_total`.
- Tests: long-running readers + micro-GC; ensure no double-free.

## 6. Benchmarks & Diagnostics

### 6.1 `compare-bench` extensions

- New CLI options: `--commit-every`, `--group-commit`, `--group-commit-wait-ms`, `--async-fsync=on|off`, `--csv-out`.
- Output CSV row per database mode with throughput, latency quantiles (from `storage::profile_snapshot`), WAL stats, fsync count.

### 6.2 `sombra mvcc-profile`

- New CLI subcommand running short read-only / insert-only / mixed loops against a DB (using existing Graph APIs).
- Emits JSON + text summary with deltas relative to stored baselines (in `.sombra/baselines.json`).

## 7. Docs & Rollout

- Update `docs/mvcc-baseline.md` + `docs/cli.md` with tuning guidance: when to enable group commit, async fsync semantics, WAL sizing, index flush modes.
- Document migration playbook:
  1. Deploy binaries with knobs (disabled by default).
  2. Enable group commit with small N; monitor `sombra mvcc-status` for backlog.
  3. Optionally enable async fsync (document data-loss window = acked-but-not-durable groups).
  4. Turn on WAL preallocation + segment reuse.
  5. Enable deferred index updates / adjacency `put_many` where safe.
  6. Roll out version compression + caches with metrics observing CPU/bytes reclaimed.
  7. Adjust vacuum cadence thresholds once lag charts stabilize.

- Provide rollback instructions per knob (all runtime-configurable via CLI/config).

## 8. Observability additions

- Metrics (Prometheus): `commit.group.size`, `commit.group.wait_ms`, `commit.acked_nv_backlog`, `wal.write_bytes_total`, `wal.sync_total`, `wal.reused_segments_total`, `mvcc.snapshot_pool.{size,hits,misses}`, `vacuum.micro.bytes_reclaimed_total`.
- CLI prints (already partly implemented via `mvcc-status`).
- Structured logs for group commit start/end, async fsync, micro-GC trims.

## 9. Testing matrix

- **Unit**: commit aggregator logic, async fsync cookie persistence, version codec round-trips, ring buffer concurrency, WAL segment allocator, micro-GC trimming.
- **Integration**: crash/restart across configurations (`group_commit` on/off, `async_fsync` on/off, WAL reuse), deferred index flush idempotence, snapshot reuse pool under churn.
- **Perf**: extend `compare-bench` to run OLTP/bulk suites with knobs; capture CSV for regression tracking.
- **Chaos**: kill processes during AckedNv backlog, vacuum trimming, or WAL segment reuse; verify `sqlite` and `Graph::verify` stay happy.

## 10. Work Breakdown

1. Implement group commit (sync) + metrics + CLI wiring.
2. Add async fsync + durable watermark cookie + recovery changes.
3. WAL preallocation/segment reuse + batched writes.
4. Writer intent IDs + snapshot reuse + per-page cache scaffolding.
5. Deferred index updates + adjacency `put_many`.
6. Version codec + inline histories.
7. Adaptive vacuum + micro-GC.
8. Benchmark/diagnostics updates (`compare-bench`, new CLI command).
9. Documentation + migration playbook.

Each item is individually deployable behind config toggles to de-risk rollout.

---

**Next steps**: convert each numbered item into GitHub issues, wire up feature flags in `GraphOptions`/`PagerOptions`, and begin plumbing the metrics so we can measure progress with `sombra mvcc-status` + `compare-bench`.
