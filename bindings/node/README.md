# `sombradb` Node bindings

This package surfaces the Sombra graph database planner/executor to Node.js. It is built with [`napi-rs`](https://napi.rs) and ships the same fluent query builder used in Stage 8 of the build docs.

## Prerequisites

- [Rust toolchain](https://www.rust-lang.org/tools/install)
- Node.js 18+
- [Bun 1.3+](https://bun.sh) for dependency management and scripts

## Setup

```bash
bun install
```

The command above installs dependencies and produces the native addon (`sombradb.*.node`).

## Build

```bash
bun run build
```

This runs `napi build --platform --release` and refreshes the compiled addon via Cargo.

## Test

```bash
bun run test
```

The test suite uses [AVA](https://github.com/avajs/ava) and exercises the fluent API against the in-process demo dataset (`seedDemo`).

## Usage example

```ts
import { Database } from 'sombradb'

const db = Database.open('/tmp/sombra.db').seedDemo()
const rows = await db
  .query()
  .match('User')
  .where('a', (pred) => pred.eq('name', 'Ada'))
  .select([{ var: 'a', prop: 'name', as: 'label' }])
  .execute()

console.log(rows)

const createdId = db.createNode('User', { name: 'New User', bio: 'Hello from Node' })
db.updateNode(createdId, { set: { bio: 'updated' } })
db.deleteNode(createdId, true)
```

`Database.seedDemo()` materialises a small example graph so the builder can be exercised without any additional seeding. Predicate helpers understand JS `Date` objects and ISO 8601 strings with timezone offsets and automatically convert them into nanosecond timestamps. Property projections (`{ var, prop, as }`) return flat scalar result sets when you only need specific values instead of entire nodes.

## CRUD helpers

`Database.mutate(script)` submits batched mutations directly to the core engine. The `Database` class also exposes ergonomic helpers:

- `createNode(labels, props)` / `createEdge(src, dst, type, props)`
- `updateNode(id, { set, unset })` / `updateEdge(...)`
- `deleteNode(id, cascade?)` / `deleteEdge(id)`

See `examples/crud.js` for an end-to-end walkthrough:

```bash
node examples/crud.js
```

### Bulk creation example

`examples/bulk_create.js` shows how to use the fluent create builder to batch 10,000 nodes and 20,000 edges (counts are configurable):

```bash
# Defaults to 10k nodes / 20k edges
node examples/bulk_create.js

# Override the counts
node examples/bulk_create.js 5000 10000
```

The script executes everything in a single transaction and prints a small sample query so you can verify the inserts.

### Scaling bulk loads

Large one-shot builders keep every staged node/edge in memory until `execute()` runs, and the pager must cache a comparable number of leaf pages. If you want to ingest 100k+ nodes and hundreds of thousands of edges as fast as possible, you have two options:

1. **Single transaction with a bigger cache (fastest per-row).**

   ```js
   const db = Database.open(path, {
     synchronous: 'normal',      // relaxed fsyncs for bulk loads
     commitCoalesceMs: 0,        // flush immediately after execute()
     commitMaxFrames: 16384,     // allow larger WAL batches
     cachePages: 8192,           // ~512 MiB cache to avoid evictions
   })
   const summary = db.create()
     // ... enqueue 10k+ nodes / 50k+ edges ...
     .execute()
   console.log(summary.nodes.length, summary.edges.length)
   ```

   **Pros:** one commit, highest throughput, easy progress accounting.  
   **Cons:** needs plenty of RAM; with default cache settings the pager will eventually run out of eviction candidates and abort.

2. **Chunked batches (less memory, more commits).**

   ```js
   async function loadChunks(db, totalNodes, totalEdges, chunkSize = 10000) {
     let nodesDone = 0
     let edgesDone = 0
     while (nodesDone < totalNodes) {
       const builder = db.create()
       const handles = []
       const take = Math.min(chunkSize, totalNodes - nodesDone)
       for (let i = 0; i < take; i++) {
         handles.push(builder.node(['User'], { name: `User ${nodesDone + i}` }))
       }
       const edgesThisChunk = Math.min(
         Math.floor((take / chunkSize) * totalEdges),
         totalEdges - edgesDone,
       )
       for (let e = 0; e < edgesThisChunk; e++) {
         builder.edge(
           handles[e % take],
           'KNOWS',
           handles[(e * 13 + 7) % take],
           {},
         )
       }
       const summary = builder.execute()
       nodesDone += summary.nodes.length
       edgesDone += summary.edges.length
     }
   }
   ```

   **Pros:** works with default cache sizes; keeps process memory bounded.  
   **Cons:** more commits (one per chunk) so total runtime is slightly higher.

Regardless of the approach, build the addon in release mode (`bun run build`) before benchmarking; debug builds of the Rust core are ~100× slower.

## Benchmarks

The `tinybench` harness in `benchmark/crud.mjs` measures basic create/read/update/delete throughput:

```bash
bun run bench
```

Each benchmark run spins up a fresh database under `/tmp` to avoid polluting your workspace.

## Release workflow

When you are ready to publish a new revision, bump the version and push the tag:

```bash
bun run prepublishOnly
npm version [major|minor|patch]
git push --follow-tags
```

GitHub Actions will build the platform-specific artifacts with `napi prepublish`. Ensure `NPM_TOKEN` is configured in the repository secrets before triggering a release.
