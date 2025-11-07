# Benchmarks

The Stage 9 performance checklist is covered by the dedicated `sombra-bench` crate.
It contains Criterion-powered micro and macro workloads plus helper binaries that
collect artifacts and run the LDBC SNB SF=0.1 baseline.

## Micro workloads

| Suite | Description |
| ----- | ----------- |
| `micro/btree` | Sequential/random insert, delete, point lookup, and range scans over the Stage 4 B+ tree. |
| `micro/adjacency` | Neighbor expansion throughput for out/in/both directions with and without deduplication. |
| `micro/property_index` | Equality, range, and postings intersection throughput over the property index. |
| `micro/vstore` | Overflow value page write/read throughput for 1 KiB, 16 KiB, and 1 MiB blobs. |
| `micro/wal` | WAL append and checkpoint throughput under `FULL` and `NORMAL` synchronous modes. |

Each micro suite uses synthetic data seeded via deterministic RNGs, so results are
stable across runs and comparable between machines.

## Macro workloads

| Suite | Description |
| ----- | ----------- |
| `macro/import` | Runs the CLI CSV importer over a synthetic social graph (50K nodes / 200K edges). |
| `macro/query_mix` | Executes the “mutual follows”, “two-hop expansion”, and “name filter + expand” query mix. |

The macro import suite exercises the exact code path that `sombra-cli import`
uses in production, including schema mapping and property coercions. The query
mix uses the Stage 8 planner/executor stack through the FFI `Database` wrapper.

## Running the suite

```
# optional: export LDBC_NODES/LDBC_EDGES to include the SNB baseline
export LDBC_NODES=$HOME/datasets/ldbc/nodes.csv
export LDBC_EDGES=$HOME/datasets/ldbc/edges.csv

./scripts/run-benchmarks.sh            # writes artifacts under bench-results/TIMESTAMP
```

The script performs the following steps:

1. `cargo bench -p sombra-bench --features bench` to run every Criterion suite.
2. `bench-collector` parses `target/criterion`, snapshots host metadata, and emits
   `env.json`, `results.json`, and `results.csv` under `bench-results/<DATE>/`.
3. If `LDBC_NODES` and `LDBC_EDGES` are set it runs the `ldbc-baseline` binary,
   which imports the supplied CSVs (or reuses an existing DB when `--skip-import`
   is passed), runs `VERIFY --level full`, executes the Stage 9 query mix, and
   writes `ldbc_baseline.json` plus `ldbc_env.json` into the same directory.

### Artifact layout

```
bench-results/20240519T193050Z/
├── env.json
├── results.csv
├── results.json
├── ldbc_env.json          # only when SNB inputs are provided
└── ldbc_baseline.json
```

`results.json` mirrors the CSV contents with the addition of Criterion’s raw
`value_str` (e.g. “42.3 µs/iter”) and throughput metadata. CSV values are
expressed in nanoseconds for easy ingestion into spreadsheets or dashboards.

### Fast smoke runs

Set `SOMBRA_BENCH_FAST=1` before invoking `cargo bench`/`cargo test --benches`
when you only need to check for regressions. Fast mode drops the CRUD sample size
to 5 and shrinks each batch to 32 ops with 8 prefill batches, cutting setup time
dramatically. Example:

```
SOMBRA_BENCH_FAST=1 cargo bench -p sombra-bench --bench crud --features bench \
    synchronous_normal/read_users
```

Need to isolate single-row lookups? Target the new `read_user_by_name` benchmark:

```
SOMBRA_BENCH_FAST=1 cargo bench -p sombra-bench --bench crud --features bench \
    synchronous_normal/read_user_by_name
```

When you only need a smaller dataset for the point-lookups (e.g. 10 k users instead of the
default 100 k), export `SOMBRA_BENCH_READ_USERS=<count>` before running Criterion. Fast mode
uses 10 k automatically, so `SOMBRA_BENCH_FAST=1` already cuts the seeding time even if you
leave the new knob unset.

### Quick ops snapshots

For instant “ops/sec” numbers without waiting on Criterion, use the standalone runner:

```
cargo run -p sombra-bench --bin quick-ops -- \
  --user-count 10000 --edges-per-user 2 --iterations 1000 \
  --op read_user_by_name --op expand_one_hop
```

`quick-ops` seeds a temp database, runs the selected CRUD/traversal workloads once, and prints
raw throughput (no statistical warmup). Omit `--op` to execute the full suite or dial in the
workload via `--iterations`, `--user-count`, and `--batch-size`.

`scripts/run-benchmarks.sh` does not export this flag, so CI and formal runs keep
the full-fidelity settings.

## LDBC SNB baseline

The repository does not ship the LDBC SF=0.1 dataset. Use the upstream generator
and convert the resulting CSVs into the two files required by `sombra-cli`:

1. `nodes.csv` with headers `id,label,name,...` (additional property columns are
   supported and will be imported automatically).
2. `edges.csv` with headers `src,dst,type,...` where `type` maps to the edge
   relationship.

Once prepared, run:

```
cargo run -p sombra-bench --bin ldbc-baseline \
    --nodes path/to/nodes.csv \
    --edges path/to/edges.csv \
    --db    target/ldbc-small.sombra \
    --out-dir bench-results/ldbc-small
```

This produces `ldbc_baseline.json` summarizing import counts, verification status,
and per-query timings along with the host metadata snapshot. The binary can reuse
an existing database via `--skip-import`, which is useful for repeated query
measurements or when running the mandatory 8h fuzz campaign in parallel.

Need to collapse the raw SNB exports into the consolidated files first? The helper
`scripts/ldbc_to_sombra.py` expects the Docker datagen layout (the default path
matches `out_sf0.1_bi/graphs/csv/bi/composite-merged-fk/initial_snapshot`) and
emits User nodes plus bidirectional FOLLOWS edges:

```
scripts/ldbc_to_sombra.py \
    --input out_sf0.1_bi/graphs/csv/bi/composite-merged-fk/initial_snapshot \
    --nodes ldbc_nodes.csv \
    --edges ldbc_edges.csv
```

Those two outputs can then be imported directly via `sombra-cli import` or the
`ldbc-baseline` binary shown above.
