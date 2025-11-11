# ⚡ Indexed Read Planner Upgrade

Speed up point/range lookups by teaching the Stage 8 planner to choose `PropIndexScan` whenever an indexed predicate is available, instead of always doing `LabelScan → Filter`. The goal is that queries like `read_user_by_name` touch only the matching node, not the entire label.

---

## Phase 0 — Metadata plumbing

- [x] **Expose index lookup**: extend `MetadataProvider` with a way to ask “does label X have an index on prop Y?” (or return the matching `IndexDef`). Implement it for both `CatalogMetadata` and `InMemoryMetadata`.
- [x] **Cache resolutions**: resolve label/prop names once per planning session and reuse the ids for predicate checks to avoid repeated dictionary lookups.
- [x] **Document contract**: make sure Stage 7/8 docs explain how metadata advertises available property indexes (label + prop id, kind, cardinality later).

## Phase 1 — Logical plan selection

- [x] **Predicate bucketing**: while grouping AST predicates by variable, mark which ones are index-eligible (`Eq` or bounded `Range`).
- [x] **Choose best predicate**: when building the initial match (and future anchor points), prefer `PropIndexScan` if any eligible predicate targets an indexed property; fall back to `LabelScan` otherwise.
- [x] **Consume predicate**: once a predicate is used for the index scan, remove it from the pending filter list so the plan doesn’t emit redundant `Filter` operators.
- [x] **Fallback filters**: keep `apply_var_predicates` for leftover predicates or vars without indexes so semantics stay identical.
- [x] **Re-anchor multi-hop patterns**: pick the most selective variable as the starting stream (even mid-chain) and expand outward, flipping edge directions when traversing “backwards”.
- [x] **Intersect indexed predicates**: when multiple indexed filters exist on the anchor variable, build parallel `PropIndexScan`s and wrap them in `Intersect` so only overlapping nodes feed the rest of the plan.

## Phase 2 — Physical lowering + executor sanity

- [ ] **Lowering wiring**: ensure `LogicalOp::PropIndexScan` carries the selected predicate + prop id into the existing `PhysicalOp::PropIndexScan` arm (lowering code already supports it, but add tests).
- [ ] **Executor invariants**: double-check the executor’s `PropIndexScan` path still receives concrete predicates and no longer expects an upstream filter on the same prop.
- [ ] **Error handling**: emit clear planner errors when metadata claims an index but resolution fails (missing label/prop).

## Phase 3 — Tests & validation

- [ ] **Planner unit tests**: add cases where `match("User").where_var("a", |pred| pred.eq("name","foo"))` yields a physical `PropIndexScan`, plus range predicates and “no index available” fallbacks.
- [ ] **Integration/bench smoke**: run or add a microbenchmark proving indexed reads only touch the expected nodes.
- [ ] **Explain output**: assert the explain tree now shows `PropIndexScan` for indexed queries.

## Phase 4 — Docs & release notes

- [ ] **Stage 8 docs**: update `docs/build/stage_8.md` with the new planning rule, selection priority (Eq > Range > LabelScan), and any caveats.
- [ ] **Changelog**: call out “super fast indexed reads” as a perf highlight for the release.
- [ ] **Follow-ups**: leave breadcrumbs for expanding the same logic to later match clauses (post-expand anchor selection) and eventually intersecting multiple prop indexes.

---

**Success criteria**

1. Planner emits `PropIndexScan` whenever an indexed predicate exists on the anchoring variable, eliminating full-label scans for indexed lookups.
2. Physical/executor layers need no behavioral hacks; predicates are consumed exactly once.
3. Docs/tests capture the optimization so regressions are obvious, and benchmarks show improved read latency.
