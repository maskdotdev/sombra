# Inserts-Only In-Place Study — 2025-11-09

Quick checkpoint that captures how the current tree behaves when the leaf allocator
is enabled (`--btree-inplace`) versus the rebuild-only fallback. Both runs use
`SOMBRA_PROFILE=1 cargo run --release --bin compare-bench -- --mode inserts-only --docs 10000 --commit-every 10000`
from the repo root.

## Summary

| Config | Time | Ops/s | Insert Avg (µs) | Key Decodes | Allocator (compactions / failures / bytes moved) |
|--------|------|-------|-----------------|-------------|--------------------------------------------------|
| `--btree-inplace` | 38.98 ms | 256,564 | 3.026 | 95,299 | 1 / 53 / 18 |
| fallback rebuild | 113.43 ms | 88,156 | 10.577 | 2,727,056 | 0 / 0 / 0 |

Observations:

- In-place editing is still ~3× faster than rewriting the entire leaf for each insert,
  but it remains substantially slower than the 24 ms runs we saw before introducing the
  new allocator (see `docs/build/insert-optimization-part-3.md` history).
- Even in an inserts-only workload we still trigger 53 allocator capacity failures and
  fall back to compaction once, indicating that `LeafAllocator::new` is fighting
  fragmentation on every attempt.

### Cache-assisted run (later on 2025-11-09)

After introducing the per-leaf allocator snapshot cache, the same workload drops to
**13.82 ms** (723,748 ops/s) with `insert_avg_us=0.607`. The allocator failure count remains
53 because we still hit the guardrail, but the cache eliminated the second O(n) scan per
mutation, giving us another ~2.8× speedup over the previous 38.98 ms run.

```
Sombra inserts-only 10000 docs --btree-inplace (allocator cache enabled)
TIME 13.82 ms  OPS/SEC 723748  µS/OP 1.4
metrics: ... allocator_compactions=1 allocator_failures=53 allocator_bytes_moved=18 allocator_avg_bytes=18.0 insert_avg_us=0.607 slot_extent_avg_us=0.109
```

## Raw Output

```
$ SOMBRA_PROFILE=1 cargo run --release --bin compare-bench -- --mode inserts-only --docs 10000 --commit-every 10000 --btree-inplace

INSERTS-ONLY
DATABASE     MODE                 DOCS            TIME         OPS/SEC        µS/OP
----------------------------------------------------------------------------------------
Sombra       inserts-only        10000        38.98 ms          256564          3.9
    metrics: wal_frames=56 wal_bytes=458752 fsyncs=1 key_decodes=95299 key_cmps=76325 memcopy_bytes=156032 rebalance_in_place=0 rebalance_rebuilds=0 allocator_compactions=1 allocator_failures=53 allocator_bytes_moved=18 allocator_avg_bytes=18.0 commit_avg_ms=5.123 search_avg_us=0.000 insert_avg_us=3.026 slot_extent_avg_us=0.509 slot_extent_ns_per_slot=2.6
    pager: hits=29895 misses=56 evictions=0 dirty_writebacks=56
SQLite       inserts-only        10000         7.05 ms         1419244          0.7
```

```
$ SOMBRA_PROFILE=1 cargo run --release --bin compare-bench -- --mode inserts-only --docs 10000 --commit-every 10000

INSERTS-ONLY
DATABASE     MODE                 DOCS            TIME         OPS/SEC        µS/OP
----------------------------------------------------------------------------------------
Sombra       inserts-only        10000       113.43 ms           88156         11.3
    metrics: wal_frames=56 wal_bytes=458752 fsyncs=1 key_decodes=2727056 key_cmps=95672 memcopy_bytes=21816448 rebalance_in_place=0 rebalance_rebuilds=0 allocator_compactions=0 allocator_failures=0 allocator_bytes_moved=0 allocator_avg_bytes=0.0 commit_avg_ms=4.078 search_avg_us=0.000 insert_avg_us=10.577 slot_extent_avg_us=0.409 slot_extent_ns_per_slot=2.7
    pager: hits=29895 misses=56 evictions=0 dirty_writebacks=56
SQLite       inserts-only        10000         6.98 ms         1432100          0.7
```
