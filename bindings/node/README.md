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
const users = await db.query().nodes('User').where(eq('name', 'Ada Lovelace')).select('name', 'bio').execute()

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

## Connection options

When opening a database, you can pass an options object to configure the connection:

```ts
import { Database } from 'sombradb'

const db = Database.open('/tmp/sombra.db', {
  createIfMissing: true, // Create the database file if it doesn't exist (default: true)
  pageSize: 4096, // Database page size in bytes
  cachePages: 1000, // Number of pages to cache in memory
  distinctNeighborsDefault: true, // Default distinct behavior for neighbor queries
  synchronous: 'full', // Durability mode: 'full' | 'normal' | 'off'
  commitCoalesceMs: 10, // Milliseconds to coalesce commits
  commitMaxFrames: 1000, // Maximum WAL frames before forcing a commit
  commitMaxCommits: 100, // Maximum commits to coalesce
  groupCommitMaxWriters: 4, // Maximum concurrent writers for group commit
  groupCommitMaxFrames: 10000, // Maximum frames for group commit
  groupCommitMaxWaitMs: 50, // Maximum wait time for group commit
  asyncFsync: false, // Enable async fsync for better write throughput
  walSegmentBytes: 16777216, // WAL segment size in bytes (16MB default)
  walPreallocateSegments: 2, // Number of WAL segments to preallocate
  autocheckpointMs: 30000, // Auto-checkpoint interval in milliseconds (null to disable)
  schema: { User: { name: '' } }, // Optional runtime schema for validation
})
```

### Synchronous modes

- `'full'` (default): Every commit is fsync'd. Maximum durability, lowest throughput.
- `'normal'`: WAL sync at critical points. Good balance of durability and performance.
- `'off'`: No sync. Maximum throughput but data loss risk on crash.

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

### Predicate reference

| Predicate                         | Description           | Example                                               |
| --------------------------------- | --------------------- | ----------------------------------------------------- |
| `eq(prop, value)`                 | Equals                | `eq('name', 'Ada')`                                   |
| `ne(prop, value)`                 | Not equals            | `ne('status', 'deleted')`                             |
| `lt(prop, value)`                 | Less than             | `lt('age', 30)`                                       |
| `le(prop, value)`                 | Less than or equal    | `le('age', 30)`                                       |
| `gt(prop, value)`                 | Greater than          | `gt('score', 100)`                                    |
| `ge(prop, value)`                 | Greater than or equal | `ge('score', 100)`                                    |
| `between(prop, low, high, opts?)` | Range check           | `between('age', 18, 65)`                              |
| `inList(prop, values)`            | In array              | `inList('status', ['active', 'pending'])`             |
| `exists(prop)`                    | Property exists       | `exists('email')`                                     |
| `isNull(prop)`                    | Is null               | `isNull('deletedAt')`                                 |
| `isNotNull(prop)`                 | Is not null           | `isNotNull('email')`                                  |
| `and(...exprs)`                   | Logical AND           | `and(eq('a', 1), eq('b', 2))`                         |
| `or(...exprs)`                    | Logical OR            | `or(eq('status', 'active'), eq('status', 'pending'))` |
| `not(expr)`                       | Logical NOT           | `not(eq('deleted', true))`                            |

### Between options

The `between` predicate supports an `inclusive` option to control boundary behavior:

```ts
// Default: inclusive on both ends [low, high]
between('age', 18, 65)

// Exclusive on high end [low, high)
between('age', 18, 65, { inclusive: [true, false] })

// Exclusive on both ends (low, high)
between('age', 18, 65, { inclusive: [false, false] })
```

### Multi-variable queries

For complex graph traversals, use the multi-variable query syntax:

```ts
const results = await db
  .query()
  .match({ u: 'User', p: 'Post' })
  .where('AUTHORED', { var: 'p', label: 'Post' })
  .on('u', (scope) => scope.where(eq('name', 'Ada')))
  .select([
    { var: 'u', prop: 'name', as: 'author' },
    { var: 'p', prop: 'title' },
  ])
  .execute()
```

### PredicateBuilder (fluent predicate API)

For more complex predicates, use the fluent `PredicateBuilder`:

```ts
const results = await db
  .query()
  .match({ u: 'User' })
  .where('u', (b) =>
    b
      .eq('status', 'active')
      .gt('age', 18)
      .or((nested) => nested.eq('role', 'admin').eq('role', 'moderator')),
  )
  .execute()
```

Available methods on `PredicateBuilder`:

- `eq`, `ne`, `lt`, `lte`/`le`, `gt`, `gte`/`ge`, `between`, `in`, `exists`, `isNull`, `isNotNull`
- `and(callback)`, `or(callback)`, `not(callback)` for nested groups
- `done()` to return to the parent builder

### Streaming results

For large result sets, use streaming to avoid loading everything into memory:

```ts
const stream = db.query().nodes('User').stream()

for await (const row of stream) {
  console.log(row)
}

// Or manually control the stream
stream.close() // Abort early
```

### Request cancellation

Long-running queries can be cancelled using request IDs:

```ts
const requestId = 'my-query-123'
const queryPromise = db.query().nodes('User').requestId(requestId).execute()

// Cancel from another context (e.g., timeout handler)
db.cancelRequest(requestId) // Returns true if cancellation was requested
```

### Query explanation

Get the execution plan without running the query:

```ts
const plan = await db.query().nodes('User').where(eq('name', 'Ada')).explain()

// Optionally redact literal values for logging
const safePlan = await db.query().nodes('User').where(eq('name', 'Ada')).explain({ redactLiterals: true })
```

## Mutations and bulk ingest

`Database.mutate()` accepts raw mutation scripts, but the higher-level helpers cover most cases:

```ts
const created = db.createNode(['User'], { name: 'Nova', followerCount: 0 })
db.createEdge(created, created, 'KNOWS', { strength: 1 })
db.updateNode(created, { set: { followerCount: 5 }, unset: ['temporary_flag'] })
db.deleteNode(created, true) // cascade through connected edges
```

### CRUD method reference

| Method                               | Description                                 | Returns                       |
| ------------------------------------ | ------------------------------------------- | ----------------------------- |
| `createNode(labels, props?)`         | Create a node with labels and properties    | `number \| null` (node ID)    |
| `updateNode(id, { set?, unset? })`   | Update node properties                      | `this` (chainable)            |
| `deleteNode(id, cascade?)`           | Delete a node (optionally cascade to edges) | `this` (chainable)            |
| `createEdge(src, dst, type, props?)` | Create an edge between nodes                | `number \| null` (edge ID)    |
| `deleteEdge(id)`                     | Delete an edge                              | `this` (chainable)            |
| `getNodeRecord(nodeId)`              | Get a node's full record                    | `Record<string, any> \| null` |
| `getEdgeRecord(edgeId)`              | Get an edge's full record                   | `Record<string, any> \| null` |

### Bulk creation with the builder

For high-volume ingestion, use the builder returned by `db.create()`:

```ts
const summary = db
  .create()
  .node(['User'], { name: 'User 1' })
  .node(['User'], { name: 'User 2' })
  .edge(0, 'KNOWS', 1, { since: 2024 })
  .execute()

console.log(summary.nodes.length, summary.edges.length)
```

The builder batches everything into one transaction. Chunk the work manually if you need to cap memory usage (see `examples/bulk_create.js`).

#### Using aliases

Aliases let you reference nodes by name instead of index:

```ts
const summary = db
  .create()
  .node(['User'], { name: 'Alice' }, 'alice')
  .node(['User'], { name: 'Bob' }, 'bob')
  .edge('alice', 'KNOWS', 'bob', { since: 2024 })
  .execute()

// Look up created IDs by alias
const aliceId = summary.alias('alice')
const bobId = summary.aliases['bob']
```

### Batch mutations

For large mutation sets, use batched mutations to control memory and transaction size:

```ts
const ops = [
  { op: 'createNode', labels: ['User'], props: { name: 'User 1' } },
  { op: 'createNode', labels: ['User'], props: { name: 'User 2' } },
  // ... thousands more
]

// Execute in batches of 1000
const summary = db.mutateBatched(ops, { batchSize: 1000 })
```

### Transactions

Use the `transaction()` helper for atomic multi-operation commits:

```ts
const { summary, result } = await db.transaction(async (tx) => {
  tx.createNode(['User'], { name: 'Alice' })
  tx.createNode(['User'], { name: 'Bob' })
  tx.createEdge(0, 1, 'KNOWS', {})
  return 'completed'
})
```

## Graph traversal

### Neighbor queries

```ts
// Get all neighbors (returns NeighborEntry[])
const neighbors = db.neighbors(nodeId, {
  direction: 'out', // 'out' | 'in' | 'both'
  edgeType: 'KNOWS', // Optional: filter by edge type
  distinct: true, // Optional: deduplicate results
})

// Convenience methods returning just node IDs
const outgoing = db.getOutgoingNeighbors(nodeId, 'KNOWS', true)
const incoming = db.getIncomingNeighbors(nodeId, 'FOLLOWS', true)
```

### BFS traversal

Breadth-first search from a starting node:

```ts
const visits = db.bfsTraversal(startNodeId, 3, {
  direction: 'out', // 'out' | 'in' | 'both'
  edgeTypes: ['KNOWS'], // Optional: filter by edge types
  maxResults: 1000, // Optional: limit results
})

for (const visit of visits) {
  console.log(`Node ${visit.nodeId} at depth ${visit.depth}`)
}
```

### Counting and listing

```ts
const userCount = db.countNodesWithLabel('User')
const edgeCount = db.countEdgesWithType('KNOWS')
const userIds = db.listNodesWithLabel('User')
```

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

### SombraDB API reference

| Method                                           | Description                               |
| ------------------------------------------------ | ----------------------------------------- |
| `raw()`                                          | Access the underlying `Database` instance |
| `addNode(label, props)`                          | Create a typed node                       |
| `addEdge(src, dst, type, props)`                 | Create a typed edge                       |
| `getNode(id, expectedLabel?)`                    | Get a node instance                       |
| `findNodeByProperty(label, prop, value)`         | Find node by property value               |
| `listNodesWithLabel(label)`                      | List all node IDs with label              |
| `getIncomingNeighbors(id, edgeType?, distinct?)` | Get incoming neighbor IDs                 |
| `getOutgoingNeighbors(id, edgeType?, distinct?)` | Get outgoing neighbor IDs                 |
| `countNodesWithLabel(label)`                     | Count nodes with label                    |
| `countEdgesWithType(type)`                       | Count edges with type                     |
| `bfsTraversal(id, maxDepth, opts?)`              | BFS traversal                             |
| `query()`                                        | Start a typed query builder               |

See `examples/typed.ts` for a complete walkthrough featuring analytics, traversal, and the fluent query builder built on top of the new traversal primitives.

## Error handling

All native errors are wrapped as typed `SombraError` subclasses:

```ts
import {
  SombraError,
  AnalyzerError,
  IoError,
  ConflictError,
  ClosedError,
  // ... other error types
} from 'sombradb'

try {
  await db.query().nodes('User').execute()
} catch (err) {
  if (err instanceof ConflictError) {
    // Handle write-write conflict
  } else if (err instanceof ClosedError) {
    // Database was closed
  } else if (err instanceof SombraError) {
    console.log(`Error code: ${err.code}`)
  }
}
```

### Error types

| Error Class           | Code               | Description                      |
| --------------------- | ------------------ | -------------------------------- |
| `SombraError`         | `UNKNOWN`          | Base class for all errors        |
| `AnalyzerError`       | `ANALYZER`         | Query analysis/parsing failed    |
| `JsonError`           | `JSON`             | JSON serialization error         |
| `IoError`             | `IO`               | File/network I/O error           |
| `CorruptionError`     | `CORRUPTION`       | Data corruption detected         |
| `ConflictError`       | `CONFLICT`         | Write-write transaction conflict |
| `SnapshotTooOldError` | `SNAPSHOT_TOO_OLD` | MVCC snapshot expired            |
| `CancelledError`      | `CANCELLED`        | Operation was cancelled          |
| `InvalidArgError`     | `INVALID_ARG`      | Invalid argument provided        |
| `NotFoundError`       | `NOT_FOUND`        | Resource not found               |
| `ClosedError`         | `CLOSED`           | Database is closed               |

## Pragmas (runtime configuration)

Adjust database behavior at runtime with pragmas:

```ts
// Get current value
const syncMode = db.pragma('synchronous')

// Set a new value
db.pragma('synchronous', 'normal')
```

### Available pragmas

| Pragma              | Description              | Values                        |
| ------------------- | ------------------------ | ----------------------------- |
| `synchronous`       | Durability mode          | `'full'`, `'normal'`, `'off'` |
| `wal_coalesce_ms`   | WAL coalesce delay       | milliseconds                  |
| `autocheckpoint_ms` | Auto-checkpoint interval | milliseconds or `null`        |

## Resource management

### Closing the database

Always close the database when done to release resources:

```ts
const db = Database.open('/tmp/sombra.db')
try {
  // ... use the database
} finally {
  db.close()
}

// Check if closed
if (db.isClosed) {
  console.log('Database is closed')
}
```

### Using with `using` (TC39 Explicit Resource Management)

```ts
{
  using db = Database.open('/tmp/sombra.db')
  // db.close() called automatically at end of block
}
```

### QueryStream cleanup

```ts
{
  using stream = db.query().nodes('User').stream()
  for await (const row of stream) {
    if (shouldStop) break // stream.close() called automatically
  }
}
```

## Performance

The Node.js bindings have minimal overhead compared to the Rust core (~4-8%). Benchmark results on a typical developer machine:

| Operation            | Throughput        |
| -------------------- | ----------------- |
| Node + edge creation | ~9,000 ops/sec    |
| Point reads          | ~20,000 reads/sec |

**Tips for optimal performance:**

1. **Use the builder for bulk operations** – `db.create()` batches all nodes and edges into a single transaction, which is significantly faster than individual `createNode`/`createEdge` calls.

2. **Use release builds** – If building from source, always use `bun run build` (release mode). Debug builds are ~40x slower.

3. **Tune synchronous mode** – For write-heavy workloads where durability can be relaxed, set `synchronous: 'normal'` in options. The default `'full'` ensures every commit is fsync'd.

4. **Use direct lookups when possible** – `db.getNodeRecord(id)` is faster than running a query for single-node fetches.

5. **Use streaming for large results** – `query().stream()` avoids loading all rows into memory.

6. **Batch mutations** – Use `mutateBatched()` for large mutation sets to control transaction size.

## Examples and scripts

- `examples/crud.js` – end-to-end walkthrough of opening the DB, seeding data, and exercising CRUD helpers.
- `examples/bulk_create.js` – demonstrates the bulk builder and scaling knobs for large inserts.
- `examples/fluent_query.ts` – a TypeScript-first tour of predicates, ordering, pagination, and configuration options.
- `examples/typed.ts` – complete typed API walkthrough with schema validation.
- `benchmark/crud.mjs` – micro-benchmarks using `tinybench`; helpful for smoke-testing performance-sensitive changes.

Run any of the scripts with `node`/`bun` from the `bindings/node` directory.

## Working inside this repo

If you are hacking on the bindings themselves:

```bash
bun install        # installs JS deps and builds the native addon
bun run build      # release-mode napi build
bun run test       # AVA-based contract tests
```
