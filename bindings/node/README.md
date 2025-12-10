# `sombradb` for Node.js

The package ships a fluent graph query builder, transactional CRUD helpers, and typed schema support via the same execution engine that powers the Rust CLI. Everything runs in-process—no daemon to manage—so you can embed Sombra anywhere you can run Node 18+ (prebuilt binaries for macOS/Linux on x64 and arm64).

## Installation

Install from npm like any other dependency; the prebuilt native addon is included in the tarball.

```bash
npm install sombradb
# or
pnpm add sombradb
bun add sombradb
```

If you need to build from source (for contrib work), install the Rust toolchain plus [Bun](https://bun.sh) and run `bun install`.

## Quick start

```ts
import { Database, eq } from 'sombradb'

const db = Database.open('/tmp/sombra.db').seedDemo()
const users = await db
  .query()
  .nodes('User')
  .where(eq('name', 'Ada Lovelace'))
  .select('name', 'bio')
  .execute()

console.log(users)

const createdId = db.createNode('User', { name: 'New User', bio: 'from Node' })
db.updateNode(createdId, { set: { bio: 'updated' } })
db.deleteNode(createdId, true)
```

- `Database.open(path, options?)` boots the embedded engine. Pass `':memory:'` for ephemeral work or a file path for persistence.
- `seedDemo()` materialises a tiny sample graph so you can explore the query surface immediately.
- `execute(true)` returns `{ rows, request_id, features }` when you need metadata; omit the flag for a plain row array.
- `Database` implements `close()` plus `Symbol.dispose`/`Symbol.asyncDispose`, and `QueryStream` exposes `close()`/`return()` so you can use `using` or `for await` without leaking native handles.
- All native failures are surfaced as typed `SombraError` subclasses (e.g. `IoError`, `ConflictError`) with stable `code` fields for programmatic handling.

## Query builder at a glance

The fluent builder mirrors Cypher-like traversal but stays fully typed:

```ts
import { and, between, eq, inList, Database } from 'sombradb'

const db = Database.open(':memory:').seedDemo()
const result = await db
  .query()
  .nodes('User')
  .where(
    and(
      inList('name', ['Ada Lovelace', 'Alan Turing']),
      between('created_at', new Date('1840-01-01'), new Date('1955-01-01')),
    ),
  )
  .select(['node', 'name', 'bio'])
  .orderBy('name', 'asc')
  .limit(10)
  .execute()
```

- Predicate helpers (`eq`, `and`, `or`, `not`, `between`, `inList`, `gt`, `lt`, etc.) understand JS primitives, Dates, and ISO strings and handle nanosecond conversions for you.
- `select()` accepts strings for scalar projections or `{ var, prop, as }` objects to alias nested values.
- Chain `.edges(type)` or `.path()` calls to traverse relationships; everything compiles into a single plan executed inside the Rust core.

## Mutations and bulk ingest

`Database.mutate()` accepts raw mutation scripts, but the higher-level helpers cover most cases:

```ts
const created = db.createNode(['User'], { name: 'Nova', followerCount: 0 })
db.createEdge(created, created, 'KNOWS', { strength: 1 })
db.updateNode(created, { set: { followerCount: 5 }, unset: ['temporary_flag'] })
db.deleteNode(created, true) // cascade through connected edges
```

For high-volume ingestion, use the builder returned by `db.create()`:

```ts
const summary = db.create()
  .node(['User'], { name: 'User 1' })
  .node(['User'], { name: 'User 2' })
  .edge(0, 'KNOWS', 1, { since: 2024 })
  .execute()

console.log(summary.nodes.length, summary.edges.length)
```

The builder batches everything into one transaction. Chunk the work manually if you need to cap memory usage (see `examples/bulk_create.js`).

## Typing your schema

Supply a `NodeSchema` to get end-to-end type hints in editors:

```ts
import type { NodeSchema } from 'sombradb'
import { Database, eq } from 'sombradb'

interface Schema extends NodeSchema {
  User: {
    labels: ['User']
    properties: {
      id: string
      name: string
      created_at: Date
      tags: string[]
    }
  }
}

const db = Database.open<Schema>('app.db')
await db.query().nodes('User').where(eq('name', 'Trillian')).select('name').execute()
```

Every projection, predicate, and mutation now benefits from compile-time checks.

## Higher-level typed API (experimental)

When you want a batteries-included experience that models nodes, edges, and traversal helpers
directly, use the `sombradb/typed` entry point. It layers a `SombraDB<MyGraphSchema>` facade on
top of the existing `Database` but enforces your schema everywhere—CRUD helpers, neighbor lookups,
analytics counters, and the traversal builder all carry precise TypeScript types.

```ts
import { SombraDB } from 'sombradb/typed'

interface Graph extends GraphSchema {
  nodes: {
    Person: { name: string; age: number }
    Company: { name: string; employees: number }
  }
  edges: {
    WORKS_AT: { from: 'Person'; to: 'Company'; properties: { role: string } }
  }
}

const schema: Graph = {
  nodes: {
    Person: { properties: { name: '', age: 0 } },
    Company: { properties: { name: '', employees: 0 } },
  },
  edges: {
    WORKS_AT: { from: 'Person', to: 'Company', properties: { role: '' } },
  },
}

const db = new SombraDB<Graph>('typed.db', { schema })
const ada = db.addNode('Person', { name: 'Ada', age: 36 })
const sombra = db.addNode('Company', { name: 'Sombra', employees: 12 })
db.addEdge(ada, sombra, 'WORKS_AT', { role: 'Researcher' })

console.log(db.countNodesWithLabel('Person')) // strongly typed labels
console.log(db.countEdgesWithType('WORKS_AT'))
```

See `examples/typed.ts` for a complete walk through featuring analytics, traversal, and the fluent query builder built on top of the new traversal primitives.

## Performance

The Node.js bindings have minimal overhead compared to the Rust core (~4-8%). Benchmark results on a typical developer machine:

| Operation | Throughput |
|-----------|------------|
| Node + edge creation | ~9,000 ops/sec |
| Point reads | ~20,000 reads/sec |

**Tips for optimal performance:**

1. **Use the builder for bulk operations** – `db.create()` batches all nodes and edges into a single transaction, which is significantly faster than individual `createNode`/`createEdge` calls.

2. **Use release builds** – If building from source, always use `bun run build` (release mode). Debug builds are ~40x slower.

3. **Tune synchronous mode** – For write-heavy workloads where durability can be relaxed, set `synchronous: 'normal'` in options. The default `'full'` ensures every commit is fsync'd.

4. **Use direct lookups when possible** – `db.getNodeRecord(id)` is faster than running a query for single-node fetches.

## Examples and scripts

- `examples/crud.js` – end-to-end walkthrough of opening the DB, seeding data, and exercising CRUD helpers.
- `examples/bulk_create.js` – demonstrates the bulk builder and scaling knobs for large inserts.
- `examples/fluent_query.ts` – a TypeScript-first tour of predicates, ordering, pagination, and configuration options.
- `benchmark/crud.mjs` – micro-benchmarks using `tinybench`; helpful for smoke-testing performance-sensitive changes.

Run any of the scripts with `node`/`bun` from the `bindings/node` directory.

## Working inside this repo

If you are hacking on the bindings themselves:

```bash
bun install        # installs JS deps and builds the native addon
bun run build      # release-mode napi build
bun run test       # AVA-based contract tests
```
