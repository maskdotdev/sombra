# Performance Baseline — 2025-11-08

## Setup
- Host: Darwin 25.1.0 (arm64) — `uname -a` in repo root.
- Dataset: synthetic key/value workload with `docs=10_000` (64-bit integer keys/values).
- Pager default options (4 KiB pages, WAL `synchronous=Full`).
- Measurements captured via `cargo run --release --bin compare-bench` with the CLI knobs introduced in this PR. The bench binary forces `SOMBRA_PROFILE=1` so the instrumentation counters are collected for every run.
- SQLite configured with `PRAGMA journal_mode=WAL`, `synchronous=FULL`, `page_size=4096`, `cache_size=128`.

## Runs & Results
Each block shows the command, the primary throughput numbers, and the Sombra-only instrumentation snapshot (`StorageProfileSnapshot` + pager stats) immediately after the run.

### Reads-only
```
cargo run --release --bin compare-bench -- --mode reads-only
```
| DB      | Time     | Ops/sec | µs/op |
|---------|----------|---------|-------|
| Sombra  | 21.74 ms | 459,976 | 2.2   |
| SQLite  | 22.36 ms | 447,293 | 2.2   |

Sombra instrumentation:
- `wal_frames=0`, `wal_bytes=0`, `fsyncs=0`
- `btree_search_avg_us≈1.18` (no inserts in this mode)
- `key_decodes=67,734`, `key_cmps=67,734`, `memcopy_bytes=0`
- Pager stats: `hits=49,833`, `misses=51`, `dirty_writebacks=51`

### Inserts-only (tx-mode=commit, commit_every=1)
```
cargo run --release --bin compare-bench -- --mode inserts-only
```
| DB      | Time     | Ops/sec | µs/op |
|---------|----------|---------|-------|
| Sombra  | 55.97 s  | 178     | 5,597.0 |
| SQLite  | 628.03 ms| 15,922  | 62.8    |

Sombra instrumentation:
- `wal_frames=25,098`, `wal_bytes=205,602,816`, `fsyncs=10,000`
- `pager_commit_avg_ms≈5.51`, `btree_insert_avg_us≈87.96`
- `key_decodes=2,994,816`, `key_cmps=96,888`, `memcopy_bytes=23,958,528`
- Pager stats: `hits=29,833`, `misses=51`, `dirty_writebacks=19,644`

### Mixed (tx-mode=commit, commit_every=1)
```
cargo run --release --bin compare-bench -- --mode mixed
```
| DB      | Time     | Ops/sec | µs/op |
|---------|----------|---------|-------|
| Sombra  | 38.65 s  | 258     | 3,865.4 |
| SQLite  | 386.44 ms| 25,877  | 38.6    |

Sombra instrumentation:
- `wal_frames=17,500`, `wal_bytes=143,360,000`, `fsyncs=6,967`
- `pager_commit_avg_ms≈5.45`, `btree_search_avg_us≈1.43`, `btree_insert_avg_us≈86.77`
- `key_decodes=2,093,661`, `key_cmps=88,754`, `memcopy_bytes=16,578,192`
- Pager stats: `hits=23,202`, `misses=3,388`, `dirty_writebacks=13,563`

### Mixed (tx-mode=read-with-write)
```
cargo run --release --bin compare-bench -- --mode mixed --tx-mode read-with-write
```
| DB      | Time     | Ops/sec | µs/op |
|---------|----------|---------|-------|
| Sombra  | 581.44 ms| 17,198  | 58.1   |
| SQLite  | 8.39 ms  | 1,191,114 | 0.8 |

Sombra instrumentation:
- `wal_frames=36`, `wal_bytes=294,912`, `fsyncs=1`
- `pager_commit_avg_ms≈7.46`, `btree_search_avg_us≈1.43`, `btree_insert_avg_us≈81.30`
- `key_decodes=2,093,903`, `key_cmps=88,996`, `memcopy_bytes=16,578,192`
- Pager stats: `hits=29,587`, `misses=36`, `dirty_writebacks=36`

## Reproducing
1. Ensure `cargo` nightly toolchain is installed and run from repo root.
2. Example command for inserts with batched commits every 10 writes:
   ```bash
   cargo run --release --bin compare-bench -- --mode inserts-only --commit-every 10
   ```
3. Mixed read-through-write (no fsync pressure):
   ```bash
   cargo run --release --bin compare-bench -- --mode mixed --tx-mode read-with-write
   ```
4. All runs emit instrumentation details automatically; capture stdout and archive alongside this file when updating the baseline.

## Next Steps
- Capture `perf`/`dtrace` flamegraphs for the three primary modes and store the SVG artifacts in `bench/flamegraphs/` to complete Phase 0 acceptance criteria.
- Automate running `compare-bench` inside CI and attach the stdout + metrics snapshot as artifacts so regressions (>10%) can be flagged automatically.
- Extend the baseline to track additional counters (page reads/hits per workload) once we add explicit pager read metrics.
