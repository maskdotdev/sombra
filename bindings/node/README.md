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
  .whereProp('a', 'name', '=', 'Ada')
  .select(['a'])
  .execute()

console.log(rows)

const createdId = db.createNode('User', { name: 'New User', bio: 'Hello from Node' })
db.updateNode(createdId, { set: { bio: 'updated' } })
db.deleteNode(createdId, true)
```

`Database.seedDemo()` materialises a small example graph so the builder can be exercised without any additional seeding.

## CRUD helpers

`Database.mutate(script)` submits batched mutations directly to the core engine. The `Database` class also exposes ergonomic helpers:

- `createNode(labels, props)` / `createEdge(src, dst, type, props)`
- `updateNode(id, { set, unset })` / `updateEdge(...)`
- `deleteNode(id, cascade?)` / `deleteEdge(id)`

See `examples/crud.js` for an end-to-end walkthrough:

```bash
node examples/crud.js
```

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
