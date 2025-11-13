# 🧱 Create Builder Plan

Create a fluent `db.create()` API that lets developers stage nodes/edges with inline references or aliases (Patterns 1 & 2) across Rust + Node + Python. The builder must run inside a single transaction, return useful identifiers, and expose ergonomics consistent with Stage 8 query builders.

---

## Phase 0 — Requirements & Spec (design + alignment)

- [ ] **Finalize return contract**: Recommend `CreateResult { nodes: Vec<NodeId>, edges: Vec<EdgeId>, aliases: HashMap<String, NodeId> }` so bindings can surface `{ nodes, edges, aliases }`.
- [ ] **Define reference semantics**: decide on `NodeRef` tokens for pattern 1 plus alias strings (e.g., `$alice`) for pattern 2; clarify mixing with existing numeric ids.
- [ ] **Document failure policy**: builder execution wraps a write transaction; any failure aborts everything (no partial inserts). Capture this in docs.
- [ ] **Plan FFI surface**: choose between augmenting `MutationSpec` vs. introducing a new `CreateScript` payload; align on JSON schema now to avoid double work for bindings.

## Phase 1 — Core Rust builder (`sombra-query` or new crate)

- [ ] **Implement `CreateBuilder`** struct with fluent methods:
  - `node(labels: &[impl Into<String>], props: HashMap<_, _>) -> NodeRef`
  - `node_with_alias(..., alias: impl Into<String>)`
  - `edge(from: NodeRefOrAliasOrId, ty: impl Into<String>, to: NodeRefOrAliasOrId, props)`
- [ ] **Track staged entities**: store `DraftNode { labels, props, alias, token }` and `DraftEdge { src, dst, ty, props }`, ensuring aliases are unique and references resolve.
- [ ] **Execution engine**:
  - open transaction (via pager write guard),
  - resolve labels/types/props once (reuse Stage 7 dictionaries),
  - insert nodes first, collect mapping `token/alias -> NodeId`,
  - insert edges using resolved ids,
  - on error, drop transaction to ensure atomicity.
- [ ] **Return summary** built from created ids + alias map; unify with new struct decided in Phase 0.
- [ ] **Unit tests** covering:
  - both patterns (token chaining + alias chaining),
  - mixing staged refs with existing ids,
  - invalid alias (duplicate/unknown) errors,
  - failure rollback (e.g., missing label) leaves graph untouched.

## Phase 2 — FFI plumbing (`sombra-ffi`)

- [ ] **Expose new entrypoint** `database_create(CreateScript) -> CreateResult`.
- [ ] **Serde models**:
  - `CreateScript { nodes: Vec<NodeSpec>, edges: Vec<EdgeSpec> }`,
  - `NodeSpec { labels: Vec<String>, props: Map<String, Value>, alias: Option<String> }`,
  - `EdgeSpec { src: RefOrId, dst: RefOrId, ty: String, props: Map<_, _> }`,
  - `RefOrId = { kind: 'alias' | 'ref' | 'id', value }`.
- [ ] **Map script → builder**: parse JSON, feed into Rust builder so all validation is centralized.
- [ ] **Wire up result struct** to JSON response (`{ createdNodes, createdEdges, aliases }`), keeping camelCase to match existing bindings.
- [ ] **Add integration tests** that call FFI directly (without bindings) to assert API contract.

## Phase 3 — Bindings (Node & Python)

### Node (`bindings/node/main.js`)
- [ ] **Builder facade**:
  - `db.create()` returns `CreateBuilder`.
  - `.node(labels, props = {}, alias?)` returns opaque token for pattern 1 while also accepting alias for pattern 2.
  - `.edge(srcRef | alias | id, type, dstRef | alias | id, props = {})`.
  - `.execute()` serializes staged script, calls native `databaseCreate`, resolves tokens to ids in result for ergonomic return.
- [ ] **Validation**: ensure labels arrays, alias format, props plain objects; throw helpful errors before hitting FFI.
- [ ] **Examples/tests**: extend `bindings/node/__test__/index.spec.ts` to cover both provided patterns + result shape.

### Python (`bindings/python/sombra/query.py`)
- [ ] **Mirror API**:
  - `db.create()` returning `CreateBuilder`.
  - Fluent `.node(...)`, `.edge(...)`, `.execute()` returning dict `{ "nodes": [...], "edges": [...], "aliases": {...} }`.
- [ ] **Type hints + docs**: update module docstring / README snippet with both usage patterns.
- [ ] **Unit tests** in `bindings/python/tests` demonstrating alias chaining & transactional rollback.

## Phase 4 — Docs, Samples, QA

- [ ] **Docs**:
  - Update `docs/STAGE_8` (or new section) describing creation builder, return payload, and transactional semantics.
  - Add cookbook snippets replicating Pattern 1 & Pattern 2 in both JS & Python.
- [ ] **Benchmarks**:
  - Optionally extend CRUD benchmarks to use new builder to validate performance parity with legacy mutation batches.
- [ ] **Release notes / migration guide**: highlight that `db.mutate` still works but `db.create()` is the preferred ergonomic path.
- [ ] **Manual QA**: run smoke tests across bindings (create sample graph, ensure rollback on error, verify alias map).

---

**Success criteria**

1. Developers can express both provided patterns without manually juggling numeric ids.
2. `.execute()` returns `{ nodes, edges, aliases }` (or equivalent) and leaves the graph untouched if any part fails.
3. Node & Python bindings share the same JSON wire format, keeping surface APIs ergonomic and documented.
