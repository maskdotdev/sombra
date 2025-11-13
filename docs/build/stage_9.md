# 📄 STAGE 9 — CLI, Import/Export, Admin, Benches, Fuzz

**Monolithic crate components:**

* `src/bin/cli.rs` — installs as the `sombra` CLI, wiring admin + import/export subcommands.
* `sombra::admin` (`src/admin/*`) — stats, checkpoint, vacuum, verify building blocks.
* `sombra::cli::import_export` — CSV import/export pipeline shared by the CLI and scripts.
* `src/bin/fast_bench.rs` / `src/bin/compare_bench.rs` — criterion + micro/macro bench entry points.
* `fuzz/` (managed via `cargo fuzz`, added in this stage) — libFuzzer/honggfuzz targets for WAL/B-tree/VStore.
* Core modules: `src/storage`, `src/query`, `src/primitives`, `src/types`, shared across every binary.

**Outcome:** a production‑usable command‑line tool; import/export pipelines; administrative operations; repeatable benchmarks; long‑running fuzzers.

> ℹ️ **Architecture note:** Stage 9 now targets the single `sombra` crate. Instead of adding sibling crates, every deliverable lands in the modules/binaries listed above.

---

## Phased Plan

1. **Admin foundations**
   * Finish `sombra::admin::{stats, checkpoint, vacuum_into, verify}` plus the supporting data structures.
   * Add smoke tests that exercise those modules directly before wiring them into the CLI.
2. **CLI + import/export**
   * Round out `src/bin/cli.rs` parsing, subcommands, and the (deferred) REPL shell so every admin call is exposed.
   * Build the CSV import/export pipeline (ID mapping, schema coercions, index toggles) inside `sombra::cli::import_export` and land sample fixtures.
3. **Benchmarks & perf artifacts**
   * Expand the `fast_bench`/`compare_bench` bins to cover the micro/macro workloads, and add scripts to snapshot JSON/CSV + environment metadata into `bench-results/DATE/...`.
   * Run LDBC SNB (SF=0.1) import + query mix to establish baseline numbers the acceptance criteria requires.
4. **Fuzzing, docs, acceptance**
   * Add libFuzzer/honggfuzz targets under `fuzz/` (via `cargo fuzz`), seed corpora, and automate short/overnight runs.
   * Ship the Stage 9 documentation set (`file_format.md`, `abi_c.md`, `cli.md`, `benchmarks.md`, `fuzzing.md`), then close out with the end-to-end import→verify→benchmark→vacuum→verify scenario.

---

## 0) Goals & Non‑Goals

**Goals**

* CSV import/export with schema mapping.
* Admin commands: `VACUUM`, `CHECKPOINT`, `PRAGMA stats`, `VERIFY`.
* Benchmarks: criterion suite + LDBC SNB (small).
* Fuzzers: WAL decode/replay, B+ tree pages/ops, var‑len store.
* Docs: file format + ABI finalized.

**Non‑Goals**

* Distributed ingestion or parallel import across nodes.
* Cost‑based optimizer research (Stage 8 already has rule‑based).

---

## 1) CLI overview

**Binary:** `sombra` (built from `src/bin/cli.rs`)
**Modes:** *one‑shot command* (MVP) and *(deferred)* interactive REPL. The REPL spec remains for post-MVP follow-up; MVP explicitly ships without it.

### 1.1 One‑shot commands

```
sombra open graph.sombra --pragma stats
sombra import graph.sombra --nodes people.csv --edges follows.csv --id-column id --src src --dst dst --type FOLLOWS --labels Person
sombra export graph.sombra --nodes out_nodes.csv --edges out_edges.csv
sombra vacuum graph.sombra --into compact.sombra
sombra checkpoint graph.sombra --mode force
sombra verify graph.sombra --level full
sombra explain graph.sombra --query 'MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a,b'
```

Common flags:

* `--synchronous {off|normal|full}`
* `--cache-pages N`
* `--page-size 8192` (create‑time only via `sombra create`)

### 1.2 REPL

> ⏸️ **Deferred:** REPL is not part of the Stage 9 MVP deliverable. Specs below describe the intended post-MVP behavior for planning purposes.

```
$ sombra repl graph.sombra
sombra> PRAGMA stats;
sombra> IMPORT NODES FROM 'people.csv' WITH (labels='Person', id_column='id');
sombra> IMPORT EDGES FROM 'follows.csv' WITH (src='src', dst='dst', type='FOLLOWS');
sombra> VACUUM INTO 'compact.sombra';
sombra> CHECKPOINT;
sombra> VERIFY LEVEL full;
sombra> .quit
```

REPL supports multi‑line, command history, `.help`, `.quit`.

---

## 2) Import / Export

### 2.1 CSV schema

**Nodes CSV** (header required):

* Required: an id column (string or integer) → external id map.
* Optional: `labels` (pipe‑separated), property columns.
  Example:

```
id,labels,name,age
42,Person|Engineer,Ada,36
77,Person,Grace,54
```

**Edges CSV**:

* Required: `src`, `dst` (external ids), `type`.
* Optional: property columns.
  Example:

```
src,dst,type,since
42,77,FOLLOWS,2019-01-01
```

### 2.2 Import command behavior

* **Mapping external ids:** a temporary B+ tree `ext→int` and `int→ext`:

  * Nodes: if `id` unseen → create `NodeId` (monotonic), insert in map; else reuse.
  * Edges: resolve `src/dst` via map (error if missing, unless `--defer-endpoints`).
* **Labels & types:** strings are interned via dictionary; cache aggressively during import.
* **Properties:** scalar coercions: `int,float,bool,date,datetime,string,bytes(hex)`; values over `INLINE_PROP_VAL_MAX` route to VStore automatically.
* **Bulk index build:** optional `--disable-indexes` for Stage‑7 property indexes, then `--build-indexes` after load (offline build by scanning).

**Command:**

```
sombra import DB \
  --nodes people.csv --id-column id --labels Person \
  --edges follows.csv --src src --dst dst --type FOLLOWS \
  --disable-indexes
```

### 2.3 Export

* `sombra export DB --nodes out_nodes.csv --edges out_edges.csv [--labels '*'|'Person|Movie'] [--props 'name,age']`
* Exports `NodeId` as external id if reverse map present; else emits internal numeric ids.

---

## 3) Admin operations

### 3.1 PRAGMA stats

The `sombra::admin::stats` helper returns a struct; the CLI prints it as text or JSON.

**Stats include:**

* Pager: page size, cache pages, hits/misses, dirty writebacks.
* WAL: frames appended, bytes, last checkpoint LSN, WAL file size.
* Storage: node/edge counts, label cardinalities (cached), index sizes.
* VStore: pages allocated/freed, bytes stored.
* Query: operator counters (if `metrics` feature).
* System: file sizes (main, wal), last open mode.

**Example output (JSON pretty if `--json`):**

```json
{
  "pager": {"page_size":8192,"cache_pages":16384,"hits":1203345,"misses":44231},
  "wal": {"frames": 908332, "bytes": 7423913984, "last_checkpoint_lsn": 112938},
  "storage": {"nodes": 1000000, "edges": 5000000},
  "vstore": {"pages_allocated": 8044, "pages_freed": 5120, "bytes": 120394993}
}
```

### 3.2 CHECKPOINT

```
sombra checkpoint DB --mode {force|best-effort}
```

* `force`: wait for readers to drain; then apply WAL/MVL (Stage 10) to base.
* `best-effort`: attempt, skip if readers present.

### 3.3 VACUUM

```
sombra vacuum DB --into compact.sombra [--analyze]
```

* Copy‑out: re‑encode and compact, rebuild freelist, optionally recompute histograms.
* Produces a **new file**; doesn’t mutate original (safer).

### 3.4 VERIFY

```
sombra verify DB --level {fast|full}
```

* **fast**: check page headers/crc, meta consistency, freelist sanity, root reachability.
* **full**: plus:

  * Walk B+ trees (nodes/edges/adjacency/label/property);
  * Edge endpoint existence;
  * FWD/REV symmetry;
  * Property blobs decode; VRef chains valid and checksummed;
  * Label/property indexes cover expected sets.

**Output:** structured report with counts and first N errors.

---

## 4) Benchmarks

**Framework:** `criterion` via the `fast_bench` / `compare_bench` bins.
**Datasets:** synthetic + **LDBC SNB (SF=0.1)** small.

### 4.1 Microbenchmarks

* B+ tree: random/sequential insert; point/range lookup; split/merge rate.
* Adjacency expand: neighbors/sec (various degree distributions).
* Property index: equality/range lookup; intersection throughput.
* VStore: write/read 1 KiB, 16 KiB, 1 MiB blobs.
* WAL append & checkpoint throughput under `FULL`/`NORMAL`.

### 4.2 Macrobenchmarks

* **Import**: CSV → DB, nodes+edges counts, elapsed, MB/s.
* **Query mix**: 10 typical queries (mutual follows, 2‑hop, name filter + expansion).
* **LDBC SNB** subset: Q1, Q2, Q3 equivalents mapping to our operators.

**Output:**

* JSON/CSV artifacts into `bench-results/DATE/…` with environment metadata (CPU, RAM, FS).

---

## 5) Fuzzing

**Harnesses:** `fuzz/` directory managed by **cargo‑fuzz** (libFuzzer) with optional honggfuzz shims.

### 5.1 Targets

* `wal_frame_decode`: arbitrary bytes → parse header+payload; expect no UB/panic; verify crc errors detected.
* `wal_replay`: corpus of valid/invalid WAL sequences → replay idempotence.
* `btree_page_decode`: random page bodies → decode slot dir, records; reject invalid with error, never panic.
* `btree_ops_seq`: generate op sequences (put/get/del) mirrored to `BTreeMap`; check equivalence after each step.
* `vstore_chain_decode`: random chains → detect malformed links, bounds.

### 5.2 Corpus & sanitizers

* Seed initial corpus with real frames/pages from unit tests.
* Enable ASAN/UBSAN on Linux builds.
* Run parallel fuzzers with per‑target time budgets; nightly job runs **overnight**.

---

## 6) Docs to ship (Stage 9)

* `docs/file_format.md`: meta page, page header, B+ tree layout, WAL frames, overflow pages, adjacency/index keys, checksums.
* `docs/abi_c.md`: C ABI (`sombra::ffi`): opaque handles, function signatures, error codes.
* `docs/cli.md`: commands, flags, examples, import schema mapping.
* `docs/benchmarks.md`: how to run, datasets, interpreting results.
* `docs/fuzzing.md`: running fuzzers, adding corpora, triaging crashes.

---

## 7) Tests & Acceptance

**Acceptance (Stage 9)**

* Baseline perf **published** (checked into repo or artifacts).
* Fuzzers run **overnight** (≥8h) with zero crashes/UB.
* `VERIFY --level full` passes on imported LDBC SNB small.
* Documentation listed above present and up‑to‑date.

---

## 8) Step‑by‑Step Checklist (coding agent)

* [ ] Implement `sombra::admin::{stats, checkpoint, vacuum_into, verify}`.
* [ ] Finalize `src/bin/cli.rs` parsing & (deferred) REPL; wire commands to the admin module.
* [ ] Implement CSV import/export with schema mapping and type coercions.
* [ ] Build criterion benches + scripts to save JSON/CSV results with env metadata.
* [ ] Add fuzz targets; seed corpora; CI fuzz smoke (short); nightly long run.
* [ ] Write/complete docs; add `docs/` TOC entry.
* [ ] End‑to‑end tests: import→verify→benchmark→vacuum→verify.

---
