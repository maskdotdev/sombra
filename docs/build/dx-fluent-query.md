
# Phase 0 — Target DX & Schema

## DX we’re shipping (TypeScript)

```ts
type Schema = {
  Person: { id: number; name: string; age: number; country: 'US'|'CA'|'MX'; active: boolean };
  Movie:  { id: number; title: string; year: number };
};

import { and, or, not, eq, gt, ge, lt, le, between, inList, isNull, isNotNull } from '@sombra/query';

const db = Database<Schema>(cfg);

const rows = await db.query()
  .nodes('Person')                 // context = Person
  .where(
    and(
      eq('id', 42),                // 'id' is type-checked from Schema.Person.id
      between('age', 18, 65),
      inList('country', ['US','CA']),
      not(eq('active', false))
    )
  )
  .select('id', 'name')            // → Promise<Array<Pick<Schema['Person'],'id'|'name'>>>
  .execute();
```

## Python parity

```py
from sombra.query import and_, or_, not_, eq, gt, ge, lt, le, between, in_list, is_null, is_not_null

res = db.query() \
  .nodes("Person") \
  .where(and_(
    eq("id", 42),
    between("age", 18, 65),
    in_list("country", ["US","CA"]),
    not_(eq("active", False))
  )) \
  .select("id", "name") \
  .execute()
```

---

# Phase 1 — Wire Format & Rust AST (single canonical tree)

* Keep **one** boolean predicate tree (`root_predicate`)—no legacy arrays.
* Each leaf carries `{ var, prop, op, … }`.

```rust
enum PropOp { Eq, Lt, Le, Gt, Ge, Between, In /* + string ops later */ }

enum BoolExpr {
    Cmp { var: VarId, prop: String, op: PropOp,
          value: Option<Value>, low: Option<Value>, high: Option<Value>,
          values: Option<Vec<Value>>, inclusive: Option<(bool,bool)> },
    And(Vec<BoolExpr>),
    Or(Vec<BoolExpr>),
    Not(Box<BoolExpr>),
}

struct QuerySpec {
    matches: Vec<MatchSpec>,       // { var, label }
    root_predicate: Option<BoolExpr>,
    select: Vec<Projection>,       // { var, prop }
    distinct: bool,
    direction: Direction,
    bidirectional: bool,
}
```

**Planner**: honor AND/OR/NOT precedence; inclusive bounds; IN lists.
**Tests**: serde round-trips + golden planner tests.

---

# Phase 2 — TypeScript API (context-aware + importable operators)

## 2.1 Public surfaces

```ts
declare function Database<S>(cfg?: unknown): {
  query(): {
    nodes<L extends keyof S & string>(label: L): NodeScope<S, L>;
    // (edges/patterns in Phase 3)
  };
};

interface NodeScope<S, L extends keyof S & string> {
  where(expr: Expr | ((ctx: NodeScope<S, L>) => Expr)): this; // callback form optional
  andWhere(expr: Expr): this;
  orWhere(expr: Expr): this;

  select(...keys: Array<keyof S[L] & string>): this;
  distinct(): this;
  direction(dir: 'out'|'in'|'any'): this;
  bidirectional(flag?: boolean): this;

  execute(): Promise<any>;
  explain(): Promise<any>;
  stream(): AsyncIterable<any>;
}
```

> Note: `NodeScope` **does not** expose `eq/and/...` methods. Those come from **imported helpers** for a clean call-site.

## 2.2 Type-safe, importable operators

We’ll ship a tiny operator module `@sombra/query`:

```ts
// Opaque expression built from operator calls
export type Expr = { readonly __expr: unique symbol; _node: any };

// Operator functions (type-checked later by the scope that consumes them)
export const and: (...xs: Expr[]) => Expr;
export const or:  (...xs: Expr[]) => Expr;
export const not: (x: Expr) => Expr;

export const eq:       <K extends string, V>(key: K, value: V) => Expr;
export const ne:       <K extends string, V>(key: K, value: V) => Expr; // sugar: not(eq)
export const gt:       <K extends string, V extends number|Date>(key: K, value: V) => Expr;
export const ge:       <K extends string, V extends number|Date>(key: K, value: V) => Expr;
export const lt:       <K extends string, V extends number|Date>(key: K, value: V) => Expr;
export const le:       <K extends string, V extends number|Date>(key: K, value: V) => Expr;
export const between:  <K extends string, V extends number|Date>(key: K, low: V, high: V, opts?: { inclusive?: [boolean,boolean] }) => Expr;
export const inList:   <K extends string, V>(key: K, values: readonly V[]) => Expr;
export const isNull:   <K extends string>(key: K) => Expr;
export const isNotNull:<K extends string>(key: K) => Expr;
```

### How do these stay **type-safe** without `qb`?

We make operator calls produce **structure with key names only** (no var/label).
When a scope consumes an `Expr` (in `.where(...)`), it **contextualizes** it:

* It knows the active label (`Person`) and var name (e.g., `'n'`).
* It **stamps** every leaf that lacks `var` with `{ var:'n' }` and validates each `key` against `keyof S[Person]`.
* It also validates the **value type** against `S[Person][key]` via TypeScript generics on `.where()`.

#### `.where()` typing (keyed to the label)

```ts
type KeyOf<S, L extends keyof S> = keyof S[L] & string;
type KeyType<S, L extends keyof S, K extends KeyOf<S,L>> = S[L][K];

// Narrow Expr so keys must be valid for L, and values must match S[L][K]:
type ScopedExpr<S, L extends keyof S> = Expr & {
  // branded type used only at compile time to enforce key/value compatibility
  __scope?: { label: L }
};

// Overload: .where(ScopedExpr<...>) enforces that eq('id', 42) is valid for Person.
interface NodeScope<S, L extends keyof S & string> {
  where(expr: ScopedExpr<S, L> | ((ctx: NodeScope<S, L>) => ScopedExpr<S, L>)): this;
}
```

**Implementation detail:** during `.where()`, recursively walk `_node`:

* For `Cmp` nodes missing `var`, set `var` to the scope var.
* Validate `prop` is in `schema[label]`; throw clear runtime error if not.
* Optionally coerce IN lists to canonical `{ op:'in', values:[...] }`.

## 2.3 Selection typing

* `.select('id', 'name')` → TS infers `Array<Pick<S[L], 'id'|'name'>>`.
* If no `.select()`, return `Array<S[L]>` or `Array<Partial<S[L]>>` (choose one and document).

## 2.4 Runtime checks (for JS callers)

* Unknown property: “Unknown property 'foo' on label 'Person'.”
* Type mismatch where detectable: e.g., `between('age', 'a', 'z')` in JS.

**Deliverables**

* `Database<S>` + `NodeScope` implementation.
* `@sombra/query` operators.
* Type tests (tsd) proving:

  * `eq('id','oops')` fails if `id: number`.
  * `between('name',1,5)` fails (not number/date).
  * `select('bogus')` fails.
* Snapshot tests for JSON generated by typical queries.

---

# Phase 3 — Patterns & Multiple Entities (ergonomic, still operator-based)

Add optional pattern querying while keeping the operator style.

```ts
await db.query()
  .match({ p: 'Person', m: 'Movie' })     // binds both
  .on('p', scope =>
    scope.where(and(eq('active', true), ge('age', 21))).select('name')
  )
  .on('m', scope =>
    scope.where(ge('year', 2015)).select('title')
  )
  .execute();
```

* `.match({ p:'Person', m:'Movie' })` binds variables.
* `.on('p', fn)` hands a **Person-scoped** `NodeScope<Schema,'Person'>` to `fn`.
* The same **imported operators** (`eq`, `and`, …) work; keys are validated against the active label.

(You can introduce edges later in the same style: `.edges('LIKES')` → edge-scoped select/filters.)

---

# Phase 4 — Python Parity (runtime validation)

* Same importable operator functions (`and_`, `eq`, `between`, …).
* `.nodes("Person").where(and_(...)).select(...).execute()`.
* `.match({...}).on("p", lambda s: s.where(...).select(...))`.

Runtime validates:

* Key existence in label.
* Type categories for range ops (number/date).
* Helpful messages on mismatch.

---

# Phase 5 — Planner & Explain

* Ensure planner supports `And/Or/Not/Between/In`.
* Implement IN via set/bitmap or index expansion, as appropriate.
* Add `explain()` formatting that shows the boolean tree and chosen access paths (e.g., index range on `age`).

Benchmarks:

* Eq/range/IN over representative datasets.
* Make sure contextualization step is negligible (it’s just JSON decoration).

---

# Phase 6 — Tests

**Rust**

* Serde for each op variant (Eq/Lt/Le/Gt/Ge/Between/In/And/Or/Not).
* Planner precedence & inclusivity tests.

**TypeScript**

* Type tests (tsd): wrong keys/values fail at compile time.
* Runtime tests: clear errors for bad keys when run without TS.
* JSON snapshots: stable wire for canonical examples.

**Python**

* JSON parity with TS for the same logical queries.
* Error messaging tests.

---

# Phase 7 — Nice-to-Haves (post-ship)

1. **String ops**: `startsWith`, `endsWith`, `contains`, `regex` (typed to `string` keys).
2. **Parameters**: `param('minAge')` usable in ops (`ge('age', param('minAge'))`); bind at `execute({ minAge: 21 })`.
3. **Cross-entity comparisons** (advanced): `eqCol({ var:'p', key:'age' }, { var:'m', key:'year' })`.
4. **Edges scope**: `.edges('LIKES').where(ge('weight', 0.8)).select('from','to','weight')`.
5. **QoL**: `.filter(expr)` alias for `.where(expr)`; better error diffs listing available keys.

---

# Phase 8 — Docs & Examples

**One focused page**:

* “Write filters with importable operators, not strings.”
* Quick reference for each operator and its allowed types.
* Copy-paste examples:

  * Basic equality + select
  * Range + IN + NOT
  * Null checks
  * Distinct/direction/bidirectional
  * Multi-entity with `.match().on()`

---

# Phase 9 — Milestones

* **M1**: Rust `BoolExpr` + serde + planner + tests.
* **M2**: TS `Database<S>`, `NodeScope`, operator contextualization + type tests + snapshots.
* **M3**: Python parity + parity tests + error messages.
* **M4**: Benchmarks + `explain()` + docs + examples + CI green.

---

## Minimal TS Implementation Sketch (operators + contextualization)

```ts
// @sombra/query
export type Expr = { readonly __expr: unique symbol; _node: any };

const n = (op: string, payload: any): Expr =>
  ({ __expr: Symbol() as any, _node: { op, ...payload } });

export const and    = (...xs: Expr[]): Expr => n('and', { args: xs.map(x => x._node) });
export const or     = (...xs: Expr[]): Expr => n('or',  { args: xs.map(x => x._node) });
export const not    = (x: Expr): Expr      => n('not', { args: [x._node] });

export const eq     = <K extends string, V>(k: K, v: V): Expr => n('eq', { prop: k, value: v });
export const ne     = <K extends string, V>(k: K, v: V): Expr => not(eq(k, v));
export const gt     = <K extends string, V extends number|Date>(k: K, v: V): Expr => n('gt', { prop: k, value: v });
export const ge     = <K extends string, V extends number|Date>(k: K, v: V): Expr => n('ge', { prop: k, value: v });
export const lt     = <K extends string, V extends number|Date>(k: K, v: V): Expr => n('lt', { prop: k, value: v });
export const le     = <K extends string, V extends number|Date>(k: K, v: V): Expr => n('le', { prop: k, value: v });
export const between= <K extends string, V extends number|Date>(k: K, lo: V, hi: V, opts?: { inclusive?: [boolean,boolean] }) =>
  n('between', { prop: k, low: lo, high: hi, inclusive: opts?.inclusive ?? [true,true] });
export const inList = <K extends string, V>(k: K, values: readonly V[]) =>
  n('in', { prop: k, values });
export const isNull = <K extends string>(k: K) => eq(k, null as any);
export const isNotNull = <K extends string>(k: K) => not(isNull(k));
```

```ts
// Database<S> (Node scope; stamping + validation)
function Database<S extends Record<string, Record<string, unknown>>>(cfg?: unknown) {
  const schema = /* your schema metadata for runtime checks */;
  return {
    query() {
      const matches: Array<{ var: string; label: string }> = [];
      let root: any | undefined;
      let select: Array<{ var: string; prop: string }> = [];

      const stamp = (label: keyof S & string, varName: string, node: any): any => {
        // Recursively add {var:varName} to leaves, validate props against schema[label]
        if (node.op === 'and' || node.op === 'or') {
          return { op: node.op, args: node.args.map((a: any) => stamp(label, varName, a)) };
        }
        if (node.op === 'not') return { op: 'not', args: [stamp(label, varName, node.args[0])] };
        // leaf
        const prop = node.prop;
        if (!schema[label] || !(prop in schema[label])) {
          throw new Error(`Unknown property '${prop}' on label '${label}'`);
        }
        return { ...node, var: varName, prop };
      };

      return {
        nodes<L extends keyof S & string>(label: L) {
          const varName = 'n'; // internal
          matches.push({ var: varName, label });
          return {
            where(exprOrFn: any) {
              const expr = typeof exprOrFn === 'function' ? exprOrFn(this) : exprOrFn;
              const stamped = stamp(label, varName, expr._node);
              root = root ? { op: 'and', args: [root, stamped] } : stamped;
              return this;
            },
            andWhere(expr: any) { return this.where(expr); },
            orWhere(expr: any)  {
              const stamped = stamp(label, varName, expr._node);
              root = root ? { op: 'or', args: [root, stamped] } : stamped;
              return this;
            },
            select(...keys: any[]) {
              for (const k of keys) {
                if (!schema[label] || !(k in schema[label])) {
                  throw new Error(`Unknown property '${k}' on label '${label}'`);
                }
                select.push({ var: varName, prop: String(k) });
              }
              return this;
            },
            distinct() { /* set flag */ return this; },
            direction(_d: 'out'|'in'|'any') { /* set flag */ return this; },
            bidirectional(_b?: boolean) { /* set flag */ return this; },

            async execute() { return nativeExecute({ matches, root_predicate: root, select }); },
            async explain() { return nativeExplain({ matches, root_predicate: root, select }); },
            async *stream() { yield* nativeStream({ matches, root_predicate: root, select }); },
          } as NodeScope<S, L>;
        },
      };
    }
  };
}
```

---

This keeps the call-site **minimal and readable**, preserves **strong typing** from your schema, cleanly separates **operators** (imported helpers) from **scopes** (nodes/patterns), and maps 1:1 to a robust boolean predicate tree on the backend.
