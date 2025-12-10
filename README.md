# Sombra – High-Performance Graph Database

[![Crates.io](https://img.shields.io/crates/v/sombra)](https://crates.io/crates/sombra)
[![Documentation](https://docs.rs/sombra/badge.svg)](https://docs.rs/sombra)
[![CI](https://github.com/maskdotdev/sombra/workflows/CI/badge.svg)](https://github.com/maskdotdev/sombra/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> ⚠️ **Alpha Software**: The storage engine, bindings, and dashboard are under active development. Expect API changes, breaking releases, and schema churn while we finish the Stage 10 roadmap.

Sombra is a single-file property graph database inspired by SQLite’s architecture. The Rust core couples an 8 KiB page-based storage engine, WAL-backed pager, B-tree indexes, and a Stage 8 query planner/executor that surfaces identical semantics to Rust, Node.js, Python, and the bundled CLI/dashboard.

## Features

### Core Features
- **Property Graph Model** with labels, typed properties, and dictionary-backed IDs, plus helpers to build and mutate graphs transactionally through the FFI `Database` and fluent builders. Property values are scalars only (`null/bool/int/float/string/bytes/datetime`); nested objects/arrays aren’t supported—serialize them to JSON/bytes if you need to store structured payloads.
- **Single-File Storage** that mirrors SQLite’s layout: a pager (`src/primitives/pager`) manages 8 KiB pages, dictionaries, adjacency tables, property/value stores, and WAL files next to the `.sombra` database.
- **Stage 8 Query Planner & Executor** (`src/query`) exposes AST + logical/physical plans, JSON specs, streaming iterators, and explain plans with literal redaction for query inspection.
- **CLI & Admin Toolkit** (`src/bin/cli.rs`) ships `stats`, `checkpoint`, `vacuum`, `verify`, `import`, `export`, `seed-demo`, and the experimental `dashboard` server with JSON output via `--format json`.
- **Dashboard & API Server** (`packages/api-server`, `packages/dashboard`) serve `/health`, `/api/stats`, `/api/query`, and `/api/labels` over Axum while embedding the React/shadcn UI directly into the CLI binary (`bundled-dashboard` feature on by default).
- **Create/Mutation Builders** wrap batched CRUD operations (`Database::create`, `mutate_many`, `transaction`) so bindings can script migrations, seed data, or enforce custom workflows without raw JSON.

### Performance Features ✨ NEW
- **Label & Property Indexes** combine chunked postings with B-tree backends (`src/storage/index`) for O(log n) lookups, equality/range scans, and chunked bitmap intersections.
- **Adjacency + Degree Cache** maintain forward/reverse edge tables and optional degree B-tree (guarded by the `degree-cache` feature) to answer neighbor counts without scanning adjacency lists.
- **Configurable Pager** supports WAL sync modes (`full`, `normal`, `off`), cache sizing, WAL commit coalescing, and automatic checkpoints (`autocheckpoint_pages` / `autocheckpoint_ms`) via `AdminOpenOptions` or runtime `pragma`s.
- **Streaming Query Execution** enforces a 1 000-row cap per stream (`MAX_STREAMED_ROWS`) to protect the dashboard and bindings while still offering async iteration and cancellation tokens.
- **Criterion + Fast Benches** live in `benches/`, `src/bin/fast_bench.rs`, `src/bin/compare_bench.rs`, and `bench-results/` to capture reproducible micro (B-tree, WAL, property index, vstore) and macro (import/query mix, LDBC SNB) numbers.
- **End-to-End Profiling** through `SOMBRA_PROFILE=1` and `Database::profile_snapshot` exposes planner/executor/serialization timing, property index counters, and WAL statistics for regression tracking.

### Language & Runtime Support
- **Rust crate `sombra` (0.3.6)** powers the CLI, dashboard server, and direct embedding via `sombra::ffi::{Database, DatabaseOptions}`.
- **Node.js/TypeScript `sombradb` (0.5.4)** is built with `napi-rs` + Bun 1.3, exposing a strongly typed fluent query/mutation DSL, transactions, pragmas, and streaming AsyncIterables.
- **Python `sombra` (0.3.7)** relies on PyO3/maturin and mirrors the Stage 8 builder (`Database.query()`, `db.create()`, `db.transaction(...)`, streaming, explain plans).
- **Axum-based Dashboard Server** (compiled into the Rust binary) hosts the JSON API and static React bundle; `packages/dashboard` can also run in dev mode with Vite for rapid UI iteration.

### Reliability & Operations
- **Write-Ahead Logging** with WAL files colocated next to the main DB, configurable sync levels, WAL commit coalescing, and forced checkpoints (`sombra checkpoint`, `db.pragma("autocheckpoint_ms", …)`).
- **On-Disk Verification & Vacuum** via `sombra verify --level full` and `sombra vacuum [--into <path> | --replace [--backup <path>]] [--analyze]`, plus statistics from `sombra stats --format json` and WAL metadata on every CLI run.
- **Import/Export Tooling** supports typed CSV ingest with property type overrides, inline label/type definitions, endpoint validation caches (`--edge-exists-cache`), `--disable-indexes` / `--build-indexes`, and CSV export filters.
- [CLI Guide](docs/cli/README.md) – global flags, telemetry, init/doctor, profiles
- **Structured Logging & Telemetry** use `tracing` + `tracing-subscriber` from both the CLI and dashboard server for consistent logs, with optional JSON outputs for automation.
- **Safety Rails for Bindings** include payload-size enforcement (8 MiB cap), schema-version negotiation, cancellable query IDs, and request-scoped streaming with graceful shutdown.
- **Configurable Resource Limits** through `pragma`s (`synchronous`, `autocheckpoint_ms`, `wal_coalesce_ms`) and CLI flags (`--page-size`, `--cache-pages`, `--distinct-neighbors-default`) to fit edge devices or SSD-heavy hosts.

### Testing & Quality
- **Extensive Integration Tests** live under `tests/integration/` (pager stages, storage stages, CLI admin flows, stress tests) and are executed in CI (`.github/workflows/ci.yml`).
- **Criterion Benchmark Suites** (`benches/*.rs`) enforce performance envelopes for B-tree operations, adjacency scans, property index queries, macro import/query mixes, and WAL throughput.
- **Static Analysis Everywhere**: Rust forbids `unsafe`, warns on missing docs, and pins Rust 1.88 (`rust-toolchain.toml`); Node bindings run `oxlint`, `taplo`, and `prettier`; Python bindings rely on `pytest` + type/lint hooks.
- **Release Automation** uses `release-please`, language-specific `CHANGELOG-*.md`, and publish workflows for crates.io, npm, and PyPI.
- **Sandboxes & Fixtures** include deterministic demo databases (`tests/fixtures/demo-db/graph-demo.sombra`), CSV generators (`scripts/generate_graph_demo.py`), and LDBC conversion helpers (`scripts/ldbc_to_sombra.py`).
- **Dashboard Checks**: `packages/dashboard` ships `npm run typecheck` + Vitest-friendly setup.

## 📦 Version Compatibility

Each binding version is published independently but shares the same storage format. Check the language-specific changelogs before mixing releases.

| Runtime | Package | Latest |
| ------- | ------- | ------ |
| Rust core & CLI | `sombra` crate | `0.3.6` |
| Node.js / TypeScript | `sombradb` (napi-rs + Bun) | `0.5.4` |
| Python | `sombra` (PyO3) | `0.3.7` |

**Quick reference**
- Rust: `cargo add sombra@0.3.6`
- Node.js: `npm install sombradb@0.5.4` (or `bun install`)
- Python: `pip install sombra==0.3.7`
- Changelogs: [`CHANGELOG-rust.md`](CHANGELOG-rust.md), [`CHANGELOG-js.md`](CHANGELOG-js.md), [`CHANGELOG-python.md`](CHANGELOG-python.md)

## Quick Start

### Rust API

```rust
use sombra::ffi::{Database, DatabaseOptions};
use serde_json::json;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let mut opts = DatabaseOptions::default();
    opts.create_if_missing = true;

    let db = Database::open("quickstart.sombra", opts)?;
    db.seed_demo()?;

    let query = json!({
        "$schemaVersion": 1,
        "request_id": "readme-rust",
        "matches": [{ "var": "u", "label": "User" }],
        "predicate": {
            "op": "eq",
            "var": "u",
            "prop": "name",
            "value": { "t": "String", "v": "Ada" }
        },
        "projections": [
            { "kind": "var", "var": "u" },
            { "kind": "prop", "var": "u", "prop": "name", "alias": "name" }
        ]
    });

    let rows = db.execute_json(&query)?;
    println!("{}", serde_json::to_string_pretty(&rows)?);
    Ok(())
}
```

- Use `Database::create()` for batched graph creation, `mutate_many` for JSON scripts, and `transaction(|tx| …)` to collect multiple mutations that either fully apply or roll back.
- `db.pragma("synchronous", "normal")`, `db.pragma("autocheckpoint_ms", 250)` and siblings let you tune WAL durability at runtime.

### TypeScript / Node.js API

```ts
import { Database, eq } from 'sombradb'

const db = Database.open('./quickstart.sombra', { createIfMissing: true })
db.seedDemo()

const result = await db
  .query()
  .nodes('User')
  .where(eq('name', 'Ada'))
  .requestId('ts-readme')
  .execute()

const ada = result[0]?.n0
await db.transaction(async (tx) => {
  const charlie = tx.createNode(['User'], { name: 'Charlie' })
  tx.createEdge(ada._id, charlie, 'KNOWS', { since: 2024 })
  return charlie
})

const plan = await db
  .query()
  .match({ a: { label: 'User' } })
  .where('a', (builder) => builder.eq('name', 'Ada'))
  .select([{ kind: 'var', var: 'a' }])
  .explain({ redactLiterals: true })

db.pragma('autocheckpoint_ms', 500)
console.log({ plan })
```

- The fluent builder mirrors the Stage 8 AST: `.match`, `.on`, `.where`, `.select`, `.distinct`, `.stream()`.
- `db.mutateMany([...])`, `db.create()` with aliases, and `db.cancelRequest(requestId)` are exposed for migration tooling and dashboards.

### Python API

```python
from sombra import Database
from sombra.query import eq

db = Database.open("quickstart.sombra")
db.seed_demo()

rows = db.query().nodes("User").where(eq("name", "Ada")).select("name").execute()
print("Found:", rows[0]["name"])

payload = (
    db.query()
    .nodes("User")
    .request_id("py-meta")
    .select("name")
    .execute(with_meta=True)
)
print("request_id:", payload.request_id(), "rows:", len(payload.rows()))

def builder(tx):
    alice = tx.create_node("User", {"name": "Alice"})
    tx.create_edge(alice, payload.rows()[0]["n0"]["_id"], "KNOWS")

db.transaction(builder)
db.pragma("synchronous", "normal")
```

- Builders cover CRUD (`db.create()`, `db.mutate_many([...])`), bulk inserts, and batched delete cascades.
- Async streaming is available via `async for row in db.query().nodes("User").stream(): ...`.

### CLI + Dashboard

```bash
# Seed or recreate a demo database (Ada, Grace, Alan)
sombra seed-demo tests/fixtures/demo-db/graph-demo.sombra --create

# Inspect pager/WAL/storage stats (use --format json for automation)
sombra stats tests/fixtures/demo-db/graph-demo.sombra --format json

# Import CSV fixtures with typed columns
sombra import graph.sombra \
  --nodes tests/fixtures/import/people_nodes.csv \
  --edges tests/fixtures/import/follows_edges.csv \
  --node-prop-types birth_date:date,created_at:datetime \
  --edge-type FOLLOWS \
  --create --disable-indexes --build-indexes

# Launch the dashboard API + UI
sombra dashboard tests/fixtures/demo-db/graph-demo.sombra --read-only
VITE_SOMBRA_API=http://127.0.0.1:7654 npm run dev --prefix packages/dashboard
```

Run the `seed-demo` command at least once to materialize
`tests/fixtures/demo-db/graph-demo.sombra`; the repository no longer
ships a pre-generated copy so the later examples assume you've created it locally.

- `--assets /path/to/packages/dashboard/build/client` overrides the embedded bundle if you rebuild the frontend locally.
- `--allow-origin https://example.com` enables remote access (defaults to loopback + strict CORS).

## Installation

### Rust

```bash
cargo add sombra
# or install the CLI (bundled dashboard on by default)
cargo install sombra --locked
```

### TypeScript / Node.js

```bash
# project dependency
npm install sombradb
# or Bun
bun install

# build + test the addon locally
bun run build
bun run test
```

### Python

```bash
pip install sombra

# build from source (PyO3 + maturin)
pip install maturin
cd bindings/python
maturin develop
pytest -q
```

### CLI & Dashboard

```bash
cargo install sombra --locked

# help + JSON output
sombra --help
sombra stats mydb.sombra --format json

# serve the dashboard with custom assets
sombra dashboard mydb.sombra --assets packages/dashboard/build/client --read-only
```

## Architecture

```
sombra-db/
├── src/                # Rust core: pager, storage, query engine, CLI, dashboard server
├── bindings/
│   ├── node/           # napi-rs addon + TypeScript DSL, AVA tests, Bun scripts
│   └── python/         # PyO3 package, examples, pytest suite
├── packages/
│   ├── api-server/     # Axum handlers compiled into the CLI binary
│   ├── dashboard/      # React + Vite UI served by the CLI
│   └── docs/           # Documentation site
├── benches/            # Criterion micro/macro workloads
├── bench-results/      # Checked-in benchmark logs (latest: 2025-11-08)
├── bench/              # Flamegraphs, aggregated reports, baseline notes
├── docs/               # CLI guide, dashboard plan, Stage 0–10 build notes, MVCC plan
├── scripts/            # Fixture generators, benchmark runners, LDBC converters
├── tests/              # Integration tests + fixtures (`demo-db/`, `import/`)
└── packages/dashboard/README.md, bindings/*/README.md for component-specific docs
```

## Documentation

- **Operational Guides**
  - [docs/cli.md](docs/cli.md) – CLI/admin surfaces, CSV import/export, demo seeding.
  - [docs/benchmarks.md](docs/benchmarks.md) – Criterion suites, LDBC baseline runner, artifact layout.
- **Architecture & Design**
  - Stage-by-stage build notes ([docs/build/stage_0.md](docs/build/stage_0.md) → [stage_10.md](docs/build/stage_10.md)) cover pager, WAL, B-tree, adjacency, planner, and query executor milestones.
  - Deep dives such as [docs/build/mvcc/plan.md](docs/build/mvcc/plan.md), [docs/build/leaf-record-plan.md](docs/build/leaf-record-plan.md), [docs/build/performance-improvements-part-1.md](docs/build/performance-improvements-part-1.md), and the insert optimization series.
- **Dashboard & UI**
  - [docs/dashboard-plan.md](docs/dashboard-plan.md) – delivery phases, security posture, API surface.
  - [packages/dashboard/README.md](packages/dashboard/README.md) – Vite dev server, CLI integration, demo dataset regeneration.
  - [packages/docs](packages/docs) – Documentation site with shadcn components.
- **Developer Workflow**
  - [docs/build/create_builder_plan.md](docs/build/create_builder_plan.md), [docs/build/dx-fluent-query.md](docs/build/dx-fluent-query.md), and the `insert-optimization-*` series document how the Stage 8 builder, mutation DSL, and storage layout evolved.

## Testing

```bash
# Rust core
cargo fmt --all --check
cargo clippy --all-targets --all-features -D warnings
cargo test --workspace
SOMBRA_BENCH_FAST=1 cargo bench -p sombra-bench --benches

# Node bindings (Bun 1.3+)
(cd bindings/node && bun install && bun run lint && bun run test)

# Python bindings
(cd bindings/python && maturin develop && pytest -q)

# Dashboard
(cd packages/dashboard && npm install && npm run typecheck && npm run build)
```

- Use `cargo test -- --ignored` for the longer storage fuzzers.
- `scripts/run-benchmarks.sh` orchestrates Criterion, bench-collector, and optional LDBC imports; set `SOMBRA_BENCH_FAST=1` for quicker CI smoke runs.

## Performance

### Latest Snapshot (bench-results/2025-11-08)

| Workload (10 K docs) | Sombra | SQLite baseline | Notes |
| -------------------- | ------ | ---------------- | ----- |
| Reads-only (`reads-only.log`) | **460 441 ops/s** | 475 590 ops/s | Neighbor lookups at parity with SQLite, zero WAL churn (`wal_frames=0`). |
| Mixed read-heavy (`mixed-read-with-write.log`) | **17 288 ops/s** | 1 244 251 ops/s | WAL batching keeps latency sub-ms but throughput still trails SQLite — ongoing Stage 10 work. |
| Inserts-only (`inserts-only.log`) | **250 ops/s** | 19 282 ops/s | Flush-safe commit path prioritizes durability; batching + index rebuild tuning in progress. |

- Raw logs (with WAL stats, cache hits, key decode counts) live under `bench-results/2025-11-08/*.log`.
- `bench/results/` + `flamegraph.svg` capture historical timelines and perf traces for regression analysis.

### Running Benchmarks

```bash
# Full sweep (Criterion + optional LDBC SNB baseline)
./scripts/run-benchmarks.sh

# Targeted micro benches
cargo bench --bench micro_btree --features bench
cargo bench --bench micro_adjacency --features bench
cargo bench --bench micro_property --features bench

# Macro workloads / scalability
cargo bench --bench macro_import --features bench
cargo bench --bench macro_queries --features bench

# Quick throughput snapshot (no Criterion warmup)
cargo run -p sombra-bench --bin quick-ops -- \
  --user-count 10000 --edges-per-user 2 --iterations 1000 \
  --op read_user_by_name --op expand_one_hop

# LDBC SF=0.1 baseline
scripts/ldbc_to_sombra.py --input out_sf0.1_bi/... --nodes ldbc_nodes.csv --edges ldbc_edges.csv
cargo run -p sombra-bench --bin ldbc-baseline -- --nodes ldbc_nodes.csv --edges ldbc_edges.csv --db target/ldbc.sombra --out-dir bench-results/ldbc
```

- Export `SOMBRA_PROFILE=1` to collect planner/executor counters during ad-hoc runs.
- Supply `--skip-import` to `ldbc-baseline` when reusing a prepared database for repeated query mixes.

## Current Status

### Version 0.3.6 – Alpha

**Core Engine**
- File-backed pager with WAL checkpoints, auto-checkpointing, commit batches, and streaming iterators (`src/primitives/pager`, `src/storage`).
- Label + property indexes (`IndexKind::Chunked` + B-tree), adjacency tables, and overflow value stores (vstore) driving equality/range scans.
- Stage 8 planner/executor with logical/physical plans, explain output, request IDs, and JSON specs validated against `$schemaVersion`.
- CSV importer/exporter with property typing, label/type mapping, endpoint caches, and offline index rebuild hooks.

**Reliability & Observability**
- `sombra stats/checkpoint/vacuum/verify` expose pager hits/misses, WAL sizes, label stats, adjacency counts, and verification findings.
- `Database::profile_snapshot` & `storage::metrics` capture plan/exec/serde timings, property index lookups, cache hits, degree cache, and adjacency reads.
- `pragma`s let clients toggle sync modes, WAL coalescing, auto checkpoints, and distinct-neighbor defaults at runtime.

**Language Bindings & Tools**
- Node `sombradb` bundles the fluent query builder, mutation/create DSLs, transactions, schema-aware projections, streaming, and request cancellation APIs.
- Python bindings mirror the same builder semantics, add async streaming helpers, and expose CRUD shortcuts plus metadata envelopes for dashboards.
- Axum dashboard server (embedded) streams stats + query results, while the React/Vite frontend offers overview cards, query console, and graph explorer.
- The repo ships deterministic demo datasets + scripts for reproducible dashboards.

## 🚀 Roadmap to Production (v1.0)

**In Progress**
- Dashboard background job executor + long-running admin endpoints (vacuum/import) with progress polling.
- Query planner profiling + literal redaction improvements for multi-tenant dashboards.

**Planned**
- MVCC + concurrent writers ([docs/build/mvcc/plan.md](docs/build/mvcc/plan.md)).
- Page-level checksums and WAL validation to harden on-disk recovery.
- Cost-based query planner, smarter distinct-neighbor defaults, and histogram-driven selectivity.
- Replication / backup tooling plus `sombra restore` pipeline.
- Observability upgrades: structured HTTP logs, metrics export, and dashboard auth tokens.

## Examples

- `bindings/node/examples/` – CRUD, bulk create, fluent query, and reopen scenarios (Bun + TypeScript).
- `bindings/python/examples/` – CRUD + fluent query demos, streaming iterators, and builder alias workflows.
- `tests/fixtures/demo-db/` – Ready-to-use `.sombra` database with ≈420 nodes / 3.2 K edges for dashboards, bindings, and manual testing.
- `tests/fixtures/import/` – CSV fixtures covering typed properties, label columns, and property overrides for `sombra import`.
- `scripts/generate_graph_demo.py` – regenerates the deterministic dataset shipped with the repo.

## License

This project is released under the [MIT License](LICENSE). The same terms apply to the Rust crate, CLI, dashboard server, and both language bindings.

## Contributing

- Read [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) for coding standards, review expectations, and release automation.
- Use Rust 1.88 (pinned via `rust-toolchain.toml`), Bun 1.3+, Node 18+, and Python 3.8+ when hacking locally.
- Run the formatting, linting, test, and benchmark commands above before opening a PR. CI enforces the same workflows plus `release-please` checks.
- Discuss large feature work (dashboard, MVCC, planner rewrites) in GitHub discussions with references to the Stage docs before landing major changes.


## Release Workflow

1. Land at least one Conventional Commit using the `feat` type that touches the component you want to publish (`.` for the Rust crate/CLI, `bindings/node` for the napi package, `bindings/python` for PyO3). For v0.x packages a `feat` commit becomes a **minor** bump, which is what we need for coordinated releases.
2. Run the Release Please workflow (`npx release-please release-pr` locally or the GitHub Action) so each component gets its own PR with a bumped version, changelog entry, and annotated tag.
3. Before merging the release PRs, dry-run the artifacts:
   - `cargo publish --dry-run`
   - `(cd bindings/node && bun run build && bun run test && npm pack)`
   - `(cd bindings/python && maturin build --release)`
4. Merge the release PRs to tag the repo. `publish-rust.yml`, `publish-npm.yml`, and `publish-python.yml` run automatically to push crates.io, npm, and PyPI uploads. Re-run the publish commands locally only if a registry rejects the CI upload.
