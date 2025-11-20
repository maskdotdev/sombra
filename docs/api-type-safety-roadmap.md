# Typed Client Roadmap

This document translates the aspirational `SombraDB<MyGraphSchema>` demo into an actionable plan for the Node and Python bindings. The goal is a first-class, type-safe surface that models nodes, edges, and traversal helpers directly instead of forcing every flow through the low-level JSON query builder.

---

## Target experience

```ts
import { SombraDB } from 'sombradb/typed'
import type { GraphSchema } from 'sombradb/typed'

interface MyGraphSchema extends GraphSchema {
  nodes: {
    Person: { name: string; age: number }
    Company: { name: string; employees: number }
  }
  edges: {
    WORKS_AT: {
      from: 'Person'
      to: 'Company'
      properties: { role: string }
    }
    KNOWS: {
      from: 'Person'
      to: 'Person'
      properties: { since: number }
    }
  }
}

const db = new SombraDB<MyGraphSchema>('graph.db')
const fabian = db.addNode('Person', { name: 'Fabian', age: 32 })
db.addEdge(fabian, auroraTech, 'WORKS_AT', { role: 'Staff Software Engineer' })
const neighbors = db.getIncomingNeighbors(auroraTech, ['WORKS_AT'])
```

The IDE should autocomplete labels, enforce edge directions, and narrow return types (e.g., `neighbors` is `Array<NodeId<'Person'>>`). Python should offer the same ergonomics via `TypedDict`/`Protocol` hints so users who do not rely on TypeScript get validation errors and editor help, too.

---

## Current gaps

- **Schema depth stops at nodes.** `NodeSchema` only captures properties per label (`bindings/node/main.d.ts:21-31`), so edge constraints, directionality, and cross-label validation never surface in the editor or at runtime.
- **High-level helpers are stringly typed.** `Database.createNode`, `createEdge`, and traversal helpers accept raw strings everywhere (`bindings/node/README.md:35-95`), keeping the API far from the polished snippet above.
- **Python parity is even thinner.** The fluent builder normalizes a runtime map of `{ label -> props }` and has zero static typing hooks (`bindings/python/sombra/query.py:51-169`), so even basic property name mistakes slip through until runtime.
- **Docs talk about typed queries but not typed CRUD.** The fluent query plan in `docs/build/fluent-query.md` already describes how ASTs and schemas should work, yet neither binding composes those ideas into a cohesive `SombraDB` experience.

---

## Guiding principles

1. **Single schema contract.** Define `GraphSchema` once (nodes + edges) and reuse it in both bindings and docs so the runtime validator and type checker agree.
2. **Zero-cost sugar.** The expressive API should compile down to the existing FFI JSON payloads and mutation scripts—no divergent execution paths.
3. **Runtime validation everywhere.** Even vanilla JS/Python callers (no TS/mypy) must receive rich errors by injecting the schema into the existing runtime validation hooks described in `docs/build/fluent-query.md`.
4. **Incremental rollout.** Ship Node support first (stronger TS ecosystem), harden via examples/tests, then port the patterns to Python to avoid double work.

---

## Milestones

### Phase 0 — Schema + scaffolding

* Deliverables
  - Core traversal primitives (BFS/DFS, neighbor iteration, analytics counters) exposed from the Rust engine via FFI so every binding consumes the same implementation.
  - `GraphSchema` typing exported from both bindings with `nodes`/`edges` sections that mirror the snippet above.
  - Runtime schema normalization that enforces node/edge structure (labels, properties, `from`/`to` pairs, optional property metadata) before plumbing into `DatabaseConfig.schema`.
  - Shared helpers for ID branding (`type NodeId<L extends string> = number & { __node: L }`) and edge payloads so downstream APIs can express intent clearly.

* Supporting work
  - Add napi/pyo3 shims plus tests for the new traversal FFI calls so bindings can wrap them immediately.
  - Update `docs/build/fluent-query.md` to reference `GraphSchema` instead of `NodeSchema`.
  - Add fixture schemas + validator unit tests in both bindings.

### Phase 1 — Node typed surface

* Deliverables
  - New `packages` entry point (`sombradb/typed`) exporting `SombraDB`, schema helpers, and branded ID utilities.
  - `SombraDB` class that wraps the existing `Database` but exposes type-safe CRUD helpers: `addNode`, `addEdge`, `getNode`, neighbor traversals, `countNodesWithLabel`, BFS/DFS helpers, etc.
  - Edge-aware `query()` sugar mirroring the sample (`startFromLabel`, `traverse`, `getIds`) that emits the canonical JSON payload under the hood.
  - Type-level inference that maps schema definitions to props/directions (e.g., `addEdge` restricts `from`/`to` to compatible labels).
  - Exhaustive tests + doc examples that exercise nodes, edges, traversal helpers, analytics, and runtime schema validation.

* Supporting work
  - Update `bindings/node/index.d.ts` + builders to accept the richer schema metadata.
  - Provide migration notes in `bindings/node/README.md`.
  - Ship an example script mirroring the snippet in `bindings/node/examples/typed.ts`.

### Phase 2 — Python parity

* Deliverables
  - `TypedGraphSchema` protocol using `TypedDict`/`Literal` so mypy/pyright surface the same hints TypeScript users ride on (e.g., `TypedGraphSchema["nodes"]["User"]["properties"]["name"]` narrows to `str`).
  - High-level `SombraDB` facade implemented in Python that wraps the PyO3 `Database` object but enforces schema-defined label/edge usage. Methods mirror the Node API (`add_node`, `add_edge`, `find_node_by_property`, traversal helpers) and return `NodeId[Literal["Label"]]` newtypes so static checkers can reason about them.
  - Runtime validator shared with Node (possibly via generated JSON schema or a small Rust helper exposed through FFI) to guarantee consistent errors when a caller supplies an invalid schema or mismatched label.
  - Pythonic traversal helpers (`get_incoming_neighbors`, `bfs_traversal`, etc.) with optional async streams for long walks, plus a fluent query builder that piggybacks on the same typed AST and runtime schema.
  - `sombra/examples/typed.py` parity demo (matching the Node script) and `pytest`-backed regression coverage that exercises every helper end-to-end.

* Supporting work
  - Doc updates in `bindings/python/README.md` + new example under `bindings/python/examples/typed.py`.
  - mypy test suite covering schemas and helper functions (strict mode, CI-enforced).
  - Sphinx stub updates once the API settles so RTD surfaces the typed helpers prominently.

#### Python facade implementation blueprint

- **Module + typing layout.** Add a dedicated `bindings/python/sombra/typed` package that re-exports `TypedGraphSchema`, schema helper utilities, `NodeId`, and the `SombraDB` facade. Keep schema protocols in `schema.py` (`TypedGraphSchema`, `NodeSchema`, `EdgeSchema`, `EdgeConstraints`). Express nodes and edges as `TypedDict` objects so mypy/pyright can introspect property names and their value types. Model branded node IDs via `NodeId[Literal["Label"]]` newtypes so API signatures restrict inputs/outputs to the correct label family. Import `typing_extensions` for `Literal`, `TypeVar`, and `Protocol` helpers on Python 3.8.
- **Facade contract.** Implement `SombraDB(Generic[SchemaT])` in `bindings/python/sombra/typed/db.py` that wraps the existing `Database` binding. Provide constructors (`__init__`, `open`, `from_database`) that accept the schema object, validate it via the shared runtime normalizer, and stash the fully resolved metadata (nodes, edges, property validators). Each CRUD helper funnels into the existing mutation/query builders:
  - `add_node(label: NodeLabel, props: NodeProps[label]) -> NodeId[...]` uses `Database.create()` with runtime validation that every supplied prop matches the schema type.
  - `add_edge(from_node: NodeId[from_label], to_node: NodeId[to_label], edge_label: EdgeLiteral, props: EdgeProps[edge_label])` checks the declared `from`/`to` directions before dispatching a `createEdge` op.
  - `get_node`, `find_nodes`, `delete_node`, `update_node`, and `count_nodes_with_label` call into `QueryBuilder` / `_native.database_*` helpers but always guard label usage through the schema metadata.
  - Convenience traversal helpers (`get_incoming_neighbors`, `get_outgoing_neighbors`, `get_neighbors`, `bfs_traversal`, `dfs_traversal`) downcast to the existing `_native.database_neighbors` / `_native.database_bfs_traversal` calls and return properly typed node ID arrays.
- **Fluent query sugar.** Introduce a typed version of the builder (`TypedQueryBuilder`) that composes the current JSON AST plan under the hood but uses schema metadata to constrain `.start_from_label`, `.where`, `.traverse`, `.project`, and `.get_ids`. The builder should share predicates and projection utilities with the untyped `query.py` implementation to avoid drift (e.g., wrap the existing builder rather than cloning its entire implementation). Typed helper APIs must surface strongly typed result payloads (e.g., `TypedQueryResult[Literal["Person"]]`) so editors can autocomplete property accesses.
- **Runtime validation + ergonomics.** Extend `_normalize_runtime_schema` (or a new helper invoked from both Node and Python) to cover edges, `from`/`to` labels, and property maps. Provide descriptive error messages that include the offending label/property when validation fails. Ensure every facade entry point performs consistent runtime validation even when type hints are ignored (plain Python callers). Bidirectional conversion helpers should translate typed props into the dictionaries expected by the mutation engine without mutating the caller's objects.
- **Tests + docs.** Add regression tests under `bindings/python/tests/test_typed_facade.py` that seed a sample schema, exercise every public helper, and assert runtime validation errors for mismatched labels/props. Include mypy fixtures (e.g., `tests/mypy/test_typed_schema.py`) that verify Literal narrowing works for nodes, edges, and traversal outputs. Provide a parity example in `bindings/python/examples/typed.py` mirroring the TypeScript snippet plus README documentation that shows how to declare schemas, open the typed DB, and run CRUD/traversal flows.

### Phase 3 — Cross-binding polish

* Deliverables
  - Feature parity checklist (counts, BFS/DFS, query builder sugar, analytics) kept in `docs/api-type-safety-roadmap.md` and referenced from release notes.
  - CI coverage that runs the typed examples for both languages.
  - Telemetry hooks (optional) or feature flags to gate early adopters before defaulting `sombradb` to the typed surface.

* Supporting work
  - Align `CHANGELOG-js.md` / `CHANGELOG-python.md` entries and publish migration notes.
  - File follow-up issues for stretch ideas (codegen from schema files, editor plugins, etc.).

---

## Next actions

1. Draft the `GraphSchema` TypeScript definitions + validator (Phase 0).
2. Expose traversal helpers from the Rust core through napi/pyo3 and add smoke tests in both bindings.
3. Spike the `SombraDB` wrapper in Node to validate ergonomics with the demo snippet.
4. Mirror the schema contract in Python once the Node pieces solidify.
5. Backfill docs + examples as each milestone lands.

Tracking issue: _TBD once the initial PR opens; link it here so the roadmap stays discoverable._
