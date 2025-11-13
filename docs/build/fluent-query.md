# Fluent Query Evolution Plan

End-state blueprint for the typed fluent query API spanning Rust, Node, and Python once we delete `whereProp` and the legacy flat `predicates: []` schema. Backwards compatibility is **not** a goal—bindings, FFI, planner, and executor will all switch to the new representation together.

---

## Phase 0 — Goals & Guardrails

- **Typed predicates**: helpers such as `where('n0').eq('name', 'Ada')` replace stringly operators. Bindings fail fast on invalid combinations.
- **Boolean structure**: predicates become trees (`and`/`or`/`not` + comparison leaves) so we can model arbitrary boolean logic and plan OR/AND branches.
- **Single wire format**: one canonical JSON payload that bindings emit, FFI validates, and the planner consumes.
- **Consistent semantics**: null handling, collation, and type coercions are defined once and enforced everywhere.
- **No raw expression strings**: projections and predicates use typed ASTs only; text expressions return later behind a feature flag.

---

## Phase 1 — Canonical Query Schema & AST

### 1.1 JSON payload

```json
{
  "$schemaVersion": 1,
  "request_id": "req-123",
  "matches": [
    { "var": "a", "label": "User" },
    { "var": "b", "label": "User" }
  ],
  "edges": [
    { "from": "a", "to": "b", "type": "FOLLOWS", "direction": "out" }
  ],
  "predicate": {
    "op": "and",
    "args": [
      { "op": "eq", "var": "a", "prop": "country", "value": { "t": "string", "v": "US" } },
      {
        "op": "or",
        "args": [
          {
            "op": "in",
            "var": "a",
            "prop": "name",
            "values": [
              { "t": "string", "v": "Ada" },
              { "t": "string", "v": "Grace" }
            ]
          },
          {
            "op": "between",
            "var": "a",
            "prop": "age",
            "low": { "t": "int", "v": 21 },
            "high": { "t": "int", "v": 65 },
            "inclusive": [true, false]
          }
        ]
      }
    ]
  },
  "projections": [
    { "kind": "var", "var": "a" },
    { "kind": "prop", "var": "a", "prop": "name", "alias": "label" }
  ],
  "distinct": true
}
```

Key points:
- Every variable referenced anywhere must be declared **once** in `matches`. FFI rejects undeclared vars and duplicate declarations (error code `DuplicateVariable`), even if labels match.
- `edges[*].direction` is per-hop (`"out" | "in" | "both"`). Bidirectional expansion comes from `direction: "both"`; there is no query-level flag.
- `edges[*].type` is optional; when `null`/omitted it means “any edge type”. Non-null values are validated against the catalog.
- For predicates that mix typed and wildcard edges (e.g., inside `or`), the planner keeps typed expansions where possible and uses wildcard adjacency only for the branches lacking a specific type.
- For OR predicates mixing typed and wildcard edge clauses, the planner emits typed expansions where type is known and falls back to wildcard adjacency only for the branches that require it.
- `predicate` is optional; omit for “match everything”.
- Optional `request_id` strings let callers correlate/abort work later (documented but cancellation API is future work).
- Execute/explain responses always wrap data inside an envelope: `{ "request_id": "...?", "features": [], "rows": [...] }` for `execute` and `{ "request_id": "...?", "features": [], "plan_hash": "0x...", "plan": { ... } }` for `explain`.
- `projections.kind` is either `"var"` (return the entire bound entity) or `"prop"` (single property). Multiple `"prop"` projections are returned in request order.
- Property projections produce scalar columns and coerce their literals according to the property type. They are ideal for selective queries that only need a couple of values instead of whole nodes.
- Bindings inject runtime schema metadata (`DatabaseConfig.schema`) when constructing `Database<Schema>` so runtime callers (plain JS/Python) still get high-quality validation errors even without TypeScript types.
- If the builder never calls `.select(...)`, we implicitly behave as though every matched variable were projected as `{ kind: "var" }`, so `execute()` returns full rows by default. Callers must opt into property-only payloads explicitly.
- Single-label sugar such as `.nodes('Person')` auto-generates deterministic variable names (`n0`, `n1`, …); multi-entity `.match({ p: 'Person' })` honors the caller-supplied keys so cross-entity filters stay stable.
- Language bindings expose typed `Database<Schema>` surfaces so label/property mismatches are caught at compile time even though the on-the-wire representation stays the same.
- Payloads include `$schemaVersion` for future evolvability. Unknown or missing versions throw `UnsupportedSchemaVersion`.

#### Fluent builder examples

**Node (TypeScript)**

```ts
const rows = await db
  .query()
  .match({ var: 'a', label: 'User' })
  .where('FOLLOWS', { var: 'b', label: 'User' })
  .where('a', (pred) => pred.eq('country', 'US'))
  .where('b', (pred) => pred.between('name', 'Ada', 'Grace'))
  .select([{ var: 'a', as: 'source' }, { var: 'b', as: 'target' }])
  .execute();

console.log(rows[0]?.source._id, rows[0]?.target.props.name);
// Pass true if you need metadata such as request_id:
const { rows: withMeta, request_id } = await db
  .query()
  .nodes('User')
  .requestId('demo')
  .select('name')
  .execute(true);
console.log(request_id, withMeta.length);
```

**Python**

```python
rows = (
    db.query()
    .match({"var": "a", "label": "User"})
    .where("FOLLOWS", {"var": "b", "label": "User"})
    .where_var("a", lambda pred: pred.eq("country", "US"))
    .where_var("b", lambda pred: pred.between("name", "Ada", "Grace"))
    .select(["a", "b"])
    .execute()
)

print(rows[0]["a"]["_id"], rows[0]["b"]["props"]["name"])

# Pass with_meta=True for the envelope (rows + request_id + features):
payload = (
    db.query()
    .nodes("User")
    .request_id("demo")
    .select("name")
    .execute(with_meta=True)
)
print(payload.request_id(), len(payload.rows()))

Python’s `.execute()` now returns the row list by default; use `.execute(with_meta=True)` when you need the full `QueryResult` helper (`rows()`, `request_id()`, `features()`, …).
```

### 1.2 Scalars & predicate AST

**File**: `src/query/value.rs`

```rust
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", content = "v")]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    DateTime(i128), // nanoseconds since Unix epoch
}
```

**File**: `src/query/ast.rs`

```rust
#[derive(Clone, Debug)]
pub enum BoolExpr {
    Cmp(Comparison),
    And(Vec<BoolExpr>),
    Or(Vec<BoolExpr>),
    Not(Box<BoolExpr>),
}

#[derive(Clone, Debug)]
pub enum Comparison {
    Eq { var: Var, prop: String, value: Value },
    Ne { var: Var, prop: String, value: Value },
    Lt { var: Var, prop: String, value: Value },
    Le { var: Var, prop: String, value: Value },
    Gt { var: Var, prop: String, value: Value },
    Ge { var: Var, prop: String, value: Value },
    Between { var: Var, prop: String, low: Bound<Value>, high: Bound<Value> },
    In { var: Var, prop: String, values: Vec<Value> },
    Exists { var: Var, prop: String }, // property present regardless of null
    IsNull { var: Var, prop: String },
    IsNotNull { var: Var, prop: String },
}
```

`QueryAst` gains `predicate: Option<BoolExpr>`.

### 1.3 QuerySpec & validation

**File**: `src/ffi/mod.rs`

```rust
#[derive(Deserialize)]
pub struct QuerySpec {
    pub matches: Vec<MatchSpec>,
    #[serde(default)]
    pub edges: Vec<EdgeSpec>,
    #[serde(default)]
    pub predicate: Option<BoolExprSpec>,
    #[serde(default)]
    pub projections: Vec<ProjectionSpec>,
    #[serde(default)]
    pub distinct: bool,
}
```

Validation rules enforced before planning:
- All variables in edges/projections/predicate exist in `matches` and each `var` is declared exactly once.
- `edges[*].from`/`to` must reference declared vars (no anonymous endpoints).
- `edges[*].type` may be `null` to mean “any type”; non-null values must resolve via the catalog or trigger `UnknownEdgeType`.
- Each `(label, prop)` resolves via catalog; unknown properties fail fast.
- `Between` requires ordered property types (`Int`, `Float`, `DateTime`, collated `String`).
- `In` values must be homogeneous (same `Value` tag—ints may not mix with floats), non-empty, and within a configured max (default 10 000). Empty `values` → compile-time error (`InListEmpty`).
- Predicate trees may not exceed 10 000 nodes; FFI returns `PredicateTooLarge` when the limit is hit.
- Predicate tree depth may not exceed 256; deeper trees fail with `PredicateTooDeep`.
- Literal `Float` values must be finite (`NaN`/`±∞` rejected with `NonFiniteFloat`).
- `Bytes` values serialize as base64 strings (max 1 MiB per literal, aggregated across `In` lists as well). Oversize payloads return `BytesTooLarge`. Only `Eq`/`Ne` operators are permitted on `Bytes`; other operators raise `TypeMismatch`.
- Total JSON payload size per query ≤ 8 MiB; oversized payloads raise `PayloadTooLarge`.
- Bounds obey the property’s ordering (`low <= high` after coercion).
- Direction defaults to `"out"`; `"both"` expands to two logical hops internally.

### 1.4 Null, collation, and predicate semantics

- **Nulls**:
  - `Eq(value, Null)` ≡ `IS NULL` (`true` when property missing or stored as null).
  - `Ne(value, Null)` ≡ `IS NOT NULL` (`true` when property present and non-null).
  - Range comparisons (`Lt/Le/Gt/Ge/Between`) against `Null` evaluate to `false`.
  - `In[..]` ignores `Null` members; `Null` never matches via `In`. Callers must use `isNull` explicitly.
- **Constant predicates**: omitting `predicate` or supplying `{ "op": "and", "args": [] }` denotes TRUE. `{ "op": "or", "args": [] }` denotes FALSE. Analyzer normalizes any other degenerate forms to these canonical encodings.
- Analyzer also collapses singleton boolean nodes: `And([X]) → X`, `Or([X]) → X`. Tests cover all corner cases.
- **Exists** is true if the property key is present, even when value is `Null`. `IsNull` is true if the property is missing or explicitly null; `IsNotNull` requires presence + non-null value.
- **Collation**: every string property stores its collation (binary default) in the catalog. Comparisons operate on raw UTF-8 bytes (no normalization). Analyzer attaches collation metadata to predicate leaves; planner only picks range scans when the index collation matches the predicate collation.
- **Type coercion**: bindings emit typed literals; FFI coerces JSON numbers into `Int` when value fits `i64`, otherwise `Float`. It rejects lossy coercions (e.g., `1e100` → `Int`). DateTime literals must be UTC nanoseconds since Unix epoch—bindings convert zone-aware inputs to UTC before serialization, reject timezone-naive datetimes, and enforce the supported calendar range (1900-01-01 through 2100-01-01). Values outside storage bounds raise `DateTimeInvalid`.
- **Bytes**: only `Eq`/`Ne` comparisons are legal. Analyzer rejects range/in-list predicates on `Bytes` with `TypeMismatch`.

### 1.5 Projection semantics

- `kind: "var"` returns `{ "_id": <NodeId>, "props": { <prop>: <value>, ... } }`. Property key order inside `props` is unspecified; consumers must not rely on insertion order. This projection materializes every stored property, so docs flag it as convenience-only.
- `kind: "prop"` returns a single column. More projection kinds (aggregates, computed expressions) arrive later as typed AST nodes; raw strings are intentionally excluded in v1.

### 1.6 Edge semantics

- Each edge clause specifies `{ from, to, type: Option<String>, direction: "out"|"in"|"both" }`.
- `direction: "both"` lowers to two separate expansions (forward + reverse). If the query marks `distinct: true`, duplicates eliminated in `Distinct`; otherwise duplicates may flow downstream.
- When `type` is `null`, planner uses the generic adjacency iterator (“all types”) which is more expensive than typed expansions; docs warn callers to specify types for better plans.
- Anonymous targets are not allowed; every endpoint variable must have a `match` clause so predicates and projections can reference it safely.
- Reflexive edges (`from == to`) are only allowed when explicitly enabled; otherwise FFI emits `EdgeReflexiveNotAllowed`. When allowed, deduplication keys off the edge id so the same edge is not emitted twice.

---

## Phase 2 — Builder Surfaces

### Rust (`src/query/builder.rs`)

- Replace `.where_prop` with `.where_var(var, |pred| { ... })`.
- Helpers emit comparison leaves; boolean helpers (`and_group`, `or_group`, `not_group`) nest closures.
- Builder enforces type discipline early (e.g., `between` requires two values, `in_list` rejects empty arrays).

### Node (`bindings/node/main.js` + `main.d.ts`)

- `Database<Schema>(cfg: DatabaseConfig<Schema>)` now requires a runtime `schema` map describing every label/property pair so runtime callers get the same validation experience as TS users. The config object is also where pools/auth live.
- `QueryBuilder.where(var: string): PredicateBuilder`.
- Predicate builder methods: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `between(low, high, opts?)`, `in(values)`, `exists(prop)`, `isNull`, `isNotNull`, `custom()` (omitted in v1), plus `and(cb)`, `or(cb)`, `not(cb)`.
- TypeScript definitions bake in literal types and raise compile-time errors for missing callbacks.
- `.where()` and `.andWhere()` always AND the new predicate with the scope-local root; `.orWhere()` ORs the entire accumulated predicate with the new clause. Reach for the explicit `and()`/`or()` helpers when you need finer grouping.
- Runtime validation throws `TypeError` for heterogeneous or empty `in()` values, invalid directions, etc.
- `in(values)` rejects nested arrays/objects immediately with `TypeError` before deeper validation to protect planner assumptions.
- Literal tagging: `Number.isSafeInteger` plus range check `|value| <= i64::MAX` → `Int`; other finite numbers → `Float`; `true/false` → `Bool`. Non-finite numbers throw immediately.
- `DateTime` helpers accept JS `Date` or ISO 8601 strings and emit nanosecond integers so callers never hand-roll epochs.
- Result typing: when projections are property-only, `execute()` returns `Promise<Array<Record<string, Value>>>`; when `kind:"var"` appears (including the implicit default when `.select()` is omitted), the return type widens to `Array<Record<string, unknown>>` (documented in typings).

#### TypeScript typing sketch

```ts
type RuntimeSchema<S extends Record<string, Record<string, unknown>>> = {
  [L in keyof S & string]: Record<keyof S[L] & string, { type: string }>;
};

interface DatabaseConfig<S extends Record<string, Record<string, unknown>>> {
  schema: RuntimeSchema<S>;
  [extra: string]: unknown;
}

export type Expr = { readonly __expr: unique symbol; _node: any };

export type ScopedExpr<S, L extends keyof S & string> = Expr & {
  __scope?: { label: L };
};

interface NodeScope<S, L extends keyof S & string> {
  where(expr: ScopedExpr<S, L> | ((ctx: NodeScope<S, L>) => ScopedExpr<S, L>)): this;
  andWhere(expr: ScopedExpr<S, L>): this;
  orWhere(expr: ScopedExpr<S, L>): this;
  select(...keys: Array<keyof S[L] & string>): this;
}
```

Operator helpers exported from `@sombra/query` (`and`, `or`, `eq`, `between`, etc.) return opaque `Expr` objects lacking label context. `.where()`/`.select()` stamp them with the active var name and validate each key/value pair against `cfg.schema` before serializing to the canonical predicate tree.

### Python (`bindings/python/sombra/query.py`)

- `_PredicateBuilder` mirrors Node surface: `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `between`, `in_`, `exists`, `is_null`, `is_not_null`, `and_`, `or_`, `not_`.
- Raises `ValueError`/`TypeError` immediately on invalid combinations (including empty/heterogeneous/nested `in_` lists).
- Builder serializes to the JSON schema before calling `_native.database_execute`.
- Literal tagging distinguishes `bool` (handled before `int`), `int`, `float`. Tests assert `True` serializes as `Bool`, never `Int(1)`. Naive `datetime` inputs are rejected unless timezone info present; helpers convert aware datetimes to epoch ns and enforce range limits.

---

## Phase 3 — Analyzer & Normalization

**Files**: `src/query/analyze.rs`, `src/query/errors.rs`, `src/catalog/*`

- Pipeline = **`QueryAst → normalize → analyze → AnalyzedQuery`**.
  - `normalize()` stays side-effect free: canonicalizes predicates/projections so identical trees hash identically.
  - `analyze()` resolves every symbol against the catalog and emits typed structures the planner can consume directly.
- Resolver details:
  - Assign `VarId` to each match clause; store `(VarId, Var, LabelId, label_name)` inside `VarBinding`.
  - Edge clauses become `AnalyzedEdge` with `VarId` endpoints + optional `TypeId`.
  - Projection nodes become `AnalyzedProjection::{Var,Prop}` where prop includes `(PropId, name, type_hint, collation)`.
  - Boolean predicates lower to `AnalyzedExpr` trees whose leaves are `AnalyzedComparison::{Eq,Range,In,...}` with resolved `VarId` + fully annotated property metadata (id/name/type/collation) so planner & executor can reason about string ordering later.
  - Analyzer enforces resource budgets (`MAX_MATCHES`, predicate depth/node limits, `MAX_IN_VALUES`, bytes budgets) before planning ever runs.
- Predicate normalization rules:
  1. Flatten nested `And`/`Or`.
  2. Push/absorb `Not` when legal (`Not(Eq)` → `Ne`, `Not(IsNull)` → `IsNotNull`).
  3. Sort `And`/`Or` children deterministically for plan caching.
  4. Deduplicate identical children.
  5. Enforce `Between` ordering (`low <= high`) using literal ordering rules.
  6. Canonicalize `In` lists: drop `Null`, dedupe via value sort key, then sort per property collation so fingerprints match across platforms.
  7. Replace empty `And` with TRUE and empty `Or` with FALSE (canonical constants).
- Reject `In` lists that normalize to empty after deduplication with `InListEmpty`.
- **Exists semantics**: true iff the property key is physically present (even if value is null). `IsNotNull` couples `Exists` + non-null value; `IsNull` is true when the property is missing or explicitly null.
- Analyzer output (`AnalyzedQuery`):
  - `vars: Vec<VarBinding>` (each includes `LabelId` + human-readable label)
  - `edges: Vec<AnalyzedEdge>` (VarIds + optional `TypeId`)
  - `predicate: Option<AnalyzedExpr>` (normalized, leaves carry `PropId`, `prop_name`, `type_hint`, and `collation`, defaulting to binary collation until catalog exposes more detail)
  - `projections: Vec<AnalyzedProjection>`
  - `distinct` flag forwarded unchanged
  - Callers (planner, FFI) no longer hit dictionaries—every node already carries ids and string labels for explain output.
- `Planner::plan` now just delegates to `plan_analyzed(&AnalyzedQuery)`; `Database::plan` (FFI) runs the analyzer first and bubbles errors as `FfiError::Analyzer`.
- **Error model**: `AnalyzerError` enumerates structured variants (`UnknownVariable`, `UnknownProperty`, `TypeMismatch`, `InvalidBounds`, `InListEmpty`, `VarNotMatched`, `DirectionInvalid`, etc.). Bindings surface both the code + message so developers can branch on the failure cause.

### Error codes surfaced by FFI

| Code | Meaning |
| --- | --- |
| `UnsupportedSchemaVersion` | Payload omitted `$schemaVersion` or used an unknown version. |
| `UnknownVariable` | Predicate/edge referenced a var not declared in `matches`. |
| `VarNotMatched` | Var was referenced in an edge/predicate/projection without a prior `match`. |
| `UnknownProperty` | `(label, prop)` absent from catalog. |
| `DuplicateVariable` | Same `var` declared more than once with conflicting metadata. |
| `TooManyMatches` | Number of `matches` clauses exceeds configured maximum (default 1 000). |
| `DirectionInvalid` | Edge direction outside `"out"|"in"|"both"`. |
| `UnknownEdgeType` | Edge type string not present in dictionary. |
| `EdgeReflexiveNotAllowed` | Reflexive edge (`from == to`) rejected when feature disabled. |
| `TypeMismatch` | Operator not supported for the property type (e.g., `Gt` on string). |
| `InvalidBounds` | `Between` bounds or inclusive flags invalid under collation ordering. |
| `InListEmpty` / `InListTooLarge` | `in(values)` empty or exceeds configured max. |
| `PredicateTooLarge` | Predicate tree exceeds node budget. |
| `NonFiniteFloat` | Literal float is `NaN`/`±∞`. |
| `BytesEncoding` | `Bytes` literal not valid base64. |
| `DateTimeInvalid` | Datetime literal cannot be parsed, lacks timezone info, or falls outside supported UTC range. |
| `PredicateTooDeep` | Predicate nesting depth exceeds limit (256). |

---

## Phase 4 — Planner & Execution

### 4.1 Sargability & lowering rules

| Predicate form | Requirement | Logical plan |
| --- | --- | --- |
| `Eq(var.prop = c)` | property indexed | `PropIndexScan [c,c]` |
| `Lt/Le/Gt/Ge` | ordered property + index | `PropIndexScan` with open/closed bound |
| `Between` | both bounds valid | `PropIndexScan [low, high]` (respect inclusive flags) |
| `In` (k ≤ 8) | indexed property | `k` point scans unioned; else fallback to scan + Filter |
| `Exists` | property indexed (optional) | property bitmap scan or Filter (phase 2) |
| `IsNull/IsNotNull` | property supports nulls | `PropIndexScan` if null bitmap exists, else Filter |

- `And` of multiple sargable predicates on the **same** variable: choose the most selective single index (using stats) and keep the rest as residual filters in v1. Future work may intersect posting lists.
- `And` across different variables uses joins/expand semantics already present in Stage 8.
- `Or` across predicates on the **same** variable becomes `Union` of any sargable children. Each branch may itself come from an `Eq`/`Range` comparison or a small `In` list—`In` expands to multiple point scans before unioning. Residual boolean nodes that are not fully pushdownable stay as `BoolFilter` so semantics match legacy behavior. With `distinct:false`, overlapping ranges may emit duplicate rows; `distinct:true` cleans them up either inside the union (when all children share the same var) or via the outer `Distinct`. `Or` spanning different variables still materializes separate pipelines joined upstream (no change from Stage 8 semantics).
- `explain_json` calls surface the union shape — deduped unions show `"dedup": "true"` so callers can reason about duplicate elimination. Example for `SELECT DISTINCT a FROM User WHERE name='Ada' OR name='Grace'`:

```json
{
  "plan": [
    {
      "op": "Project",
      "props": { "fields": "a" },
      "inputs": [
        {
          "op": "Union",
          "props": { "vars": "a", "dedup": "true" },
          "inputs": [
            { "op": "PropIndexScan", "props": { "predicate": "a.name = \"Ada\"" } },
            { "op": "PropIndexScan", "props": { "predicate": "a.name = \"Grace\"" } }
          ]
        }
      ]
    }
  ],
  "plan_hash": "0x…"
}
```

**Range bound encoding**

- `<` ⇒ `(upper = Bound::Excluded(value))`
- `<=` ⇒ `(upper = Bound::Included(value))`
- `>` ⇒ `(lower = Bound::Excluded(value))`
- `>=` ⇒ `(lower = Bound::Included(value))`
- `Between` uses `(lower, upper)` plus `inclusive: [bool,bool]` flags. Unit tests assert `[true,false]` maps to `[low, high)`.

### 4.2 Stats & selectivity

- Extend catalog stats with per-(label, prop):
  - row count, NDV, null fraction, min/max (numeric/date), histogram buckets (optional).
- Use stats to reorder conjuncts (evaluate most selective first) and pick indexes.
- Store stats alongside dictionary entries so planner has O(1) access per plan call.

Selectivity heuristics (v1):
- `Eq` ≈ `1 / max(1, NDV)`
- `In(k)` ≈ `min(1.0, k / max(1, NDV))`
- `Between` ≈ `(high - low) / max(1, max_value - min_value)` clamped to `(0,1]`
- `IsNull` ≈ `null_fraction`
- Residual filters default to `0.25` when no stats available

Analyzer/planner attach these estimates to predicate leaves and propagate descending order hints to `And` nodes so evaluators short-circuit most selective predicates first (micro-benchmarks verify the win).

### 4.3 Execution operators

- Logical operators: `LabelScan`, `PropIndexScan`, `Expand`, `Filter`, `Union`, `Intersect`, `Distinct`, `Project`.
- OR semantics: when all OR children reference the same `(var, label)` the planner emits a `Union` of the child scans (flattening nested ORs and expanding `In` leaves). `Union` streams children in order and, when marked `dedup:true`, owns duplicate removal so the planner can skip a redundant top-level `Distinct`. OR across different variables still lowers to multiple pipelines combined via join/expand operators.
- Physical operators mirror them with iterator-style APIs.
- `Union` and `Intersect` stream results without buffering where possible; fallback to HashSet for dedup if needed.
- Predicate evaluator short-circuits boolean expressions using selectivity hints. `In` uses small-vector comparisons for ≤8 entries else pre-builds an `FxHashSet<ValueKey>` per operator instance.
- **Concurrency**: all scans/evaluators run on a consistent MVCC read snapshot (same behavior as existing executor). Range scans respect visibility rules (exclude uncommitted rows, include committed ≤ snapshot).

### 4.4 Explain output

- `explain_json` returns `{ "plan": [ ... ], "plan_hash": "<xxHash64>" }` where nodes are emitted parent-first (topological) for stable diffs.
- Each node includes operator-specific data (chosen index, bounds, residual filters, selectivity hints).
- `plan_hash` is computed from `schemaVersion`, normalized AST, and catalog epoch (same value used for the plan cache).
- Tests snapshot explain output and `plan_hash` to ensure planner changes are intentional.
- Optional future flag `explain_json(redact_literals=true)` will zero out literal values while preserving operator structure for sensitive deployments (documented here for forward compatibility).

---

## Phase 5 — FFI & Binding Plumbing

- `Database::execute_json`, `explain_json`, `stream_json` accept only the new `QuerySpec`. Legacy payloads fail with `invalid query spec`.
- Bindings convert language-native literals into tagged `Value` objects (e.g., Node builder distinguishes `Number.isInteger` vs float, Python builder inspects `int` vs `float` vs `bool`).
- Date/time helpers convert JS `Date`, Python `datetime`, or ISO strings into `DateTime(i128)` consistently.
- Error propagation: FFI bubbles structured analyzer/planner errors up to bindings; Node throws `Error`, Python raises `ValueError`.
- Resource guardrails at FFI:
  - Predicate tree node budget (10 000).
  - Max `in(values)` length (10 000 configurable).
  - Query timeout/cancellation hooks pass through from bindings to executor (future work but stub documented here).
  - Hard cap on per-query memory for `Distinct`/`Union` dedup; spill to temp storage once exceeded.
- Deterministic serialization: analyzer sorts predicate children so identical logical queries produce identical plan cache keys.
- Optional `request_id` field (string) lets clients correlate/cancel queries; future cancellation APIs will target this identifier.
- Plan cache key = hash(`schemaVersion`, normalized `QueryAst`, catalog epoch) computed via xxHash64, ensuring identical logical queries (under the same collation) hash identically across platforms. Tests assert identical logical queries hit the same key.

### Resource limits & guardrails (defaults)

- `$schemaVersion` must be `1`.
- `matches` clauses ≤ 1 000 entries.
- Predicate node budget 10 000; depth ≤ 256.
- `in(values)` length ≤ 10 000 after dedup.
- `Bytes` literal ≤ 1 MiB.
- Total serialized payload ≤ 8 MiB.
- Execution memory for `Distinct`/`Union` dedup capped (spill to disk when exceeded).
- Optional `request_id` reserved for cancellation APIs.
- Predicate evaluation uses an explicit stack instead of recursion to honor depth limits safely.
- FFI responses reserve a `"features": []` list for capability flags (empty in v1).

---

## Phase 6 — Testing & Benchmarks

### Rust

- Serde round-trips for every `BoolExpr` variant.
- Analyzer tests covering null semantics, `Between` bounds (including `low == high`), heterogeneous `In`, `NonFiniteFloat`, `Bytes` base64 rejection, and Exists/IsNull truth tables (missing/present/null).
- Analyzer tests for constant predicate normalization (empty AND/OR, singleton AND/OR) and Bytes-operator rejection.
- Planner tests verifying OR → Union (same var vs different vars), AND residual filters, index selection, and undeclared vars rejection.
- Executor tests for `isNull` vs `exists`, `In` performance (≤8 vs >8), nested boolean trees, union/intersection streaming (with and without `distinct`), string range scans honoring collation, and MVCC visibility.
- Plan-stability tests: identical logical predicates (even with reordered inputs) yield the same normalized AST + plan hash.
- Edge-plan tests: compare explain output for typed vs wildcard edges to ensure planner picks adjacency scans only when `type` is null.
- Criterion benchmarks: indexed equality, range scans with varying selectivity, OR unions of small vs large `In` lists.

### Bindings

- Node + Python tests for fluent builder ergonomics, runtime validation errors, JSON payload snapshots, and EXPLAIN verification.
- Manual QA scripts replicating Stage 8 sample queries using the new API.

---

## File Checklist

- `src/query/value.rs` (new): scalar Value enum.
- `src/query/ast.rs`: add `BoolExpr`, `Comparison`, update `QueryAst`.
- `src/query/builder.rs`: fluent predicate builder + helpers.
- `src/query/analyze.rs` & `src/query/errors.rs`: resolution, normalization, diagnostics.
- `src/query/logical.rs`, `planner.rs`, `physical.rs`, `executor/*`: predicate-aware operators, `Union`/`Intersect`.
- `src/ffi/mod.rs`: new `QuerySpec`, serde, validation, error plumbing.
- `bindings/node/main.js` & `main.d.ts`: builder surface + typings.
- `bindings/python/sombra/query.py`: builder parity + type hints.
- Docs (`docs/build/stage_8.md`, language READMEs) updated with new API examples.

---

## Milestones

1. **M1 — Schema & serde**: implement `Value`, `BoolExpr`, new `QuerySpec`, serde tests, validation of variables/props.
2. **M2 — Builders**: Rust + bindings expose `where(var)` predicate builders; legacy methods removed.
3. **M3 — Analyzer & planner**: normalization, catalog lookups, stats integration, sargability rules, Union/Intersect lowering.
4. **M4 — Executor & runtime**: predicate evaluator, new physical operators, MVCC enforcement, benchmarks.
5. **M5 — Docs & rollout**: documentation updates, binding samples, QA sign-off.

Achieving M5 means the old string-based predicate API is fully retired and every query flows through the typed boolean tree.
