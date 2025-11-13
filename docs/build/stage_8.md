# 📄 STAGE 8 — Fluent Query (Builder + Operators + Rule‑Based Planner)

**Crates:** `sombra-query` (planner/executor/operators), integrates `sombra-storage` + `sombra-index`
**Outcome:** a fluent, ergonomic API in **TypeScript** and **Python** that compiles to a small set of physical operators:

* `LabelScan`, `PropIndexScan`, `Expand(dir,type)`, `Filter`, `Intersect`, `HashJoin`, `Project`
* Rule‑based plan selection + **EXPLAIN** output.

---

## 0) Fluent API (TypeScript focus; Python parity)

**Goal:** match this style:

```ts
const mutualFollows = await db
  .query()
  .match('User')                   // starting label
  .where('FOLLOWS', 'User')        // edge type + right label
  .bidirectional()                 // treat edge as undirected (mutual)
  .execute();                      // [{ a: NodeId, b: NodeId }]
```

### 0.1 TypeScript shape

```ts
// bindings/node/src/query.ts (facade)
type Dir = 'out' | 'in' | 'both';

export class QueryBuilder {
  match(labelOrVar: string | { var: string; label: string }): this;
  where(edgeType: string, right: string | { var: string; label: string }): this;
  // Predicate overload: when the second argument is a callback, .where() switches to predicate mode.
  where(varName: string, build: (pred: PredicateBuilder) => void): this;

  // Graph semantics
  direction(dir: Dir): this;        // default 'out'
  bidirectional(): this;            // sugar for direction('both') AND reciprocal
  distinct(on?: 'nodes' | 'edges'): this; // default 'nodes'

  // Projections
  select(fields: Array<string | { as: string; expr: string }>): this;

  // Execution
  explain(): Promise<PlanJson>;
  execute(): Promise<Array<Record<string, any>>>;
  stream(): AsyncIterable<Record<string, any>>;
}

// Calling where(varName) without a callback returns a PredicateBuilder; invoke .done()
// to hand control back to the QueryBuilder before chaining more clauses.
```

**Sane defaults for the common case:**

* `.match('User')` creates variable `a:User`.
* `.where('FOLLOWS', 'User')` creates variable `b:User` and edge pattern `(a)-[:FOLLOWS]->(b)`.
* `.bidirectional()` compiles to **intersect** between FWD and REV neighbor sets (mutuality).
* Repeating `.where(var, …)` predicates on the same variable (or chaining `.predicate(var).eq(...).done()`) builds a stack of posting streams that the planner will intersect via the Stage‑7 streaming helper, keeping results lazy.
* Property projections (`{ var, prop, as }`) keep results flat (one scalar column per projection) so analytics-style queries skip large entity payloads.
* Literals accept timezone-aware ISO 8601 strings or language-native `Date`/`datetime` values and automatically convert them to UTC nanoseconds; naive datetimes are rejected.

### 0.2 Python parity

```python
# bindings/python/sombra/query.py
q = db.query().match('User').where('FOLLOWS', 'User').bidirectional()
rows = list(q.execute())         # list of dicts {'a': int, 'b': int}
plan = q.explain()
```

### 0.3 Quickstart (bindings)

**TypeScript**

```ts
import { Database } from 'sombradb'

type Schema = {
  User: { _id: string; name: string; country: string; bio?: string }
}

const db = Database.open<Schema>('/tmp/sombra.db').seedDemo()
const rows = await db
  .query()
  .match('User')
  .where('n0', (pred) => pred.eq('name', 'Ada'))
  .select([{ var: 'n0', prop: 'name', as: 'label' }])
  .execute()
```

**Python**

```python
from sombra import Database

db = Database.open("/tmp/sombra.db")
db.seed_demo()

rows = db.query().match("User").where_var("n0", lambda pred: pred.eq("name", "Ada")).select(["n0"]).execute()
```

`seedDemo`/`seed_demo` materialises a small sample graph (Ada, Grace, Alan) so the fluent API can be exercised without bootstrapping a full dataset.

Bindings **don’t** implement the executor; they forward the built AST to `sombra-query` in Rust via FFI.

## Metadata lookups

- The planner talks to a `MetadataProvider` that resolves labels, properties, and edge types by name, caching those ids for each `plan()` invocation so Stage‑5 dictionary lookups happen once per symbol.
- `CatalogMetadata` additionally exposes the property index catalog via `property_index(label, prop) -> Option<IndexDef>` by delegating to the storage graph, letting the planner see whether predicates such as `User.name` are indexed.
- `InMemoryMetadata` gained `with_property_index`/`with_property_index_def` helpers for tests and docs to advertise indexes without touching disk files.
- The root match clause now inspects equality/range predicates per variable and swaps the initial `LabelScan` for a `PropIndexScan` whenever metadata reports a matching property index, otherwise it falls back to the scan + filter path.
- When patterns contain multiple hops, the planner re-anchors the chain on the most selective variable (preferring indexed equality → range → label scan) and expands outward in either direction, inverting edge directions as needed so multi-hop reads still start from the cheapest binding.
- If multiple indexed predicates apply to the anchor variable, the planner creates parallel `PropIndexScan` nodes and intersects them before expanding, so only node ids that satisfy *all* indexed filters flow downstream.

---

## 1) Internal logical model

* **AST** (simplified):

  * `Match(var_a: Var, label_a: Label)`
  * `Edge(pattern: (var_a)-[type, dir]->(var_b: Label))`
  * `Predicates` on vars/properties
  * `Distinct` flag
  * `Projection` list

* **Logical plan operators**:

  * `LabelScan(label)` → stream(`node_id`)
  * `PropIndexScan(label, prop, op, value)` → stream(`node_id`)
  * `Expand(input_stream, from_var, dir, type)` → stream of bindings
  * `Filter(predicate)` on properties (fetches blobs lazily)
  * `Join(left,right,on=var)` → `HashJoin` or `Intersect`
  * `Project(vars/exprs)`

> **Row / binding:** small struct mapping `Var` → `NodeId` (+ optional `EdgeId` if requested).

---

## 2) Physical operators (Rust)

```rust
pub enum Operator {
    LabelScan { label: LabelId, as_var: Var },
    PropIndexScan { label: LabelId, prop: PropId, pred: Pred, as_var: Var },
    Expand { from: Var, to: Var, dir: Dir, ty: Option<TypeId>, distinct_nodes: bool },
    Filter { pred: PredExpr },                  // property access triggers node fetch
    Intersect { vars: Vec<Var> },               // streaming k-way intersect on NodeId
    HashJoin { left: Var, right: Var },
    Project { fields: Vec<FieldExpr> },
}
```

* `Expand` uses Stage‑6 FWD/REV iterators.
* `PropIndexScan` yields sorted `NodeId`s from Stage‑7 posting streams.
* `Filter` pulls property blobs as needed, no prefetch.
* `Intersect` relies on Stage‑7 `intersect_k` to merge one or more sorted posting streams without buffering.
* `HashJoin` used when left stream is small or when sortedness is unknown.

---

## 3) Rule‑based planning

1. **Choose a starting stream**:

   * Prefer the **most selective** of: `PropIndexScan` (if present) > `LabelScan`.
   * Estimate selectivity using label counts and (optional) index histograms.
2. **Expand ordering**:

   * For each `where(edgeType, rightLabel)` step:

     * If `.bidirectional()` is set, plan:

       * `Expand(dir='out', type=T)` → `to=b`
       * `Expand(dir='in',  type=T)` → `to=b'`
       * `Intersect` on `[b, b']` for mutuality.
     * Else, single `Expand` with chosen direction.
3. **Predicate combination**:

   * Group property predicates per variable. For equality/range predicates backed by indexes, build multiple posting streams and emit a single `Intersect` node configured with all participating vars; execute it via Stage‑7 `intersect_k`.
   * Fall back to a single `PropIndexScan` if only one indexed predicate remains.

4. **Filters**:

   * Push property filters down as `PropIndexScan` when index exists; otherwise **Filter** after binding.
5. **Join strategy**:

   * If both inputs are sorted on the same `Var`, prefer `Intersect` (k=2 streaming).
   * Else use `HashJoin` with smaller stream as build side (threshold-based).
6. **Projection**:

   * Last step; prune unused variables early.

---

## 4) EXPLAIN output

* **JSON** tree with:

  * operator name, vars, estimated input/output cardinalities,
  * chosen indexes (with label/prop/value or ranges),
  * cost components (IO ops, cpu),
  * example:

```json
{
  "plan": {
    "op": "Project",
    "fields": ["a","b"],
    "input": {
      "op": "Intersect",
      "vars": ["b"],
      "inputs": [{
        "op": "Expand",
        "dir": "out",
        "type": "FOLLOWS",
        "from": "a",
        "to": "b",
        "input": { "op": "LabelScan", "label": "User", "as": "a", "est": 1000000 }
      },{
        "op": "Expand",
        "dir": "in",
        "type": "FOLLOWS",
        "from": "a",
        "to": "b",
        "input": { "op": "LabelScan", "label": "User", "as": "a", "est": 1000000 }
      }]
    }
  }
}
```

Expose via `.explain()` in bindings.

---

## 5) Examples (compiled plans)

### 5.1 Mutual Follows (requested form)

```ts
const rows = await db
  .query()
  .match('User')
  .where('FOLLOWS', 'User')
  .bidirectional()
  .select(['n0','n1'])
  .execute();
```

* **Plan (high‑level)**:
  `LabelScan(User as n0)`
  → `Expand(n0 -> n1, dir=out, type=FOLLOWS)`
  → `Expand(n0 -> n1', dir=in, type=FOLLOWS)`
  → `Intersect([n1, n1'])`
  → `Project(n0,n1)`

### 5.2 People named “Ada” following someone named “Grace”

```ts
const rows = await db
  .query()
  .match({ var: 'a', label: 'Person' })
  .where('a', (pred) => pred.eq('name', 'Ada'))
  .where('KNOWS', { var: 'b', label: 'Person' })
  .where('b', (pred) => pred.eq('name', 'Grace'))
  .select(['a','b'])
  .execute();
```

* Uses two `PropIndexScan` operators (if indexes exist), joined with `Expand`.

---

## 6) Execution & Memory

* Operators stream rows in **batches** (configurable). No large materializations unless a `HashJoin` build side is chosen (then limited by threshold with spill‑to‑temp on overflow in the future).
* `Intersect` uses the Stage‑7 streaming helper (`intersect_k`) so even multi-predicate intersections stay incremental.
* `Expand` prefetches adjacency leaf pages sequentially; batch size exposed to users via `.execute({ batchSize })` (bindings).
* Property blob fetches (`Filter`) are **on demand**; avoid if plan picked `PropIndexScan`.

---

## 7) Tests & Acceptance

**End‑to‑end**

* Mutual follows query returns the same set as “self‑join on FOLLOWS” validation query.
* Name filter queries (with and without property indexes) return identical results.

**Operator‑level**

* `LabelScan` counts match label index.
* `PropIndexScan` matches Stage‑7 results.
* `Expand` matches adjacency; `Intersect` equals set intersection.

**Planner**

* With/without indexes, the rule‑based planner picks expected shapes; EXPLAIN stable.

**Performance**

* Mutual follows on power‑law dataset executes within X ms for Y edges; document baseline.

**Acceptance (Stage 8)**

* The demonstrated fluent queries compile & run correctly with EXPLAIN.
* Rule‑based plans choose `PropIndexScan` when available; fall back gracefully.
* Streaming execution keeps memory bounded under configured batch sizes.

---

## 8) Step‑by‑Step Checklist (coding agent)

* [ ] Define AST & builder structs in `sombra-query`.
* [ ] Implement operator traits (`open/next/close`) with batch streaming.
* [ ] Implement `LabelScan`, `PropIndexScan`, `Expand`, `Filter`, `Intersect`, `HashJoin`, `Project`.
* [ ] Hook `Intersect` to `sombra-index::intersect_k` and cover multi-stream predicates in tests.
* [ ] Implement simple rule‑based planner & cardinality estimates (label counts + optional histograms).
* [ ] Implement EXPLAIN (JSON).
* [ ] Bindings: add `db.query()` fluent façade (TS/Python); marshal AST to Rust.
* [ ] Tests: unit operators, integration (examples above), performance smoke.
* [ ] Document planner rules & EXPLAIN fields.

---

## Cross‑Stage Integration Notes

* **Stage 6** must expose `neighbors` & property accessors used by Stage 8 operators.
* **Stage 7** provides posting streams for `PropIndexScan`, label scans for `LabelScan`, and the streaming `intersect_k` helper used by `Intersect`.
* **Diagnostics:** propagate `tracing` spans through operators for per‑op timing.
* **Config knobs:** batch sizes for `Expand` and result streaming; degree cache toggle; index kind defaults.

---
