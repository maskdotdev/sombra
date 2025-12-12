# Code Graph Bulk Load & Scaling Plan

## 1. Goals and Target Workloads

**Scale target**
- Nodes: 100–200k code entities (functions, methods, classes, etc.).
- Edges: ≥1M `LINKS`‑style relationships (call, reference, etc.).

**Workloads**
- Bulk ingestion:
  - Initial import from an indexer / LSP / analysis pipeline.
  - Periodic rebuilds (e.g., after big repo updates).
- Steady‑state reads/traversals:
  - Lookups by symbol/identifier (`Node.name`, file+position).
  - Neighbor expansions (one hop: callers/callees, references).
  - Multi-hop BFS / path‑like traversals (e.g. dependency chains).

**Constraints**
- It’s acceptable to have a **non‑atomic bulk‑ingest mode** as long as it’s clearly separated from normal transactional APIs.

---

## 2. What the 50k/200k Run Is Telling Us

For `realistic_bench` with a code‑graph schema (`src/bin/realistic_bench.rs`):

- At **5k nodes / 20k edges**:
  - Writes: ~360 ms → ~13.8k nodes/sec, ~55k edges/sec.
  - Reads (10k random `get_nodes`): ~120 ms → ~84k reads/sec.
  - DB size: ~95 MB.
- At **50k nodes / 200k edges**:
  - Writes: ~50.8 s → ~985 nodes/sec, ~3.9k edges/sec.
  - Reads (10k random `get_nodes`): ~42.2 s → ~237 reads/sec.
  - DB size: ~880 MB.

So:

- Ingestion gets ~14× slower per node/edge.
- Read throughput collapses by ~350×.
- Space usage is far higher than the logical payload.

This strongly suggests:

- A **hot path that is effectively O(N²) or at least super‑linear** in batch size (big single transaction, index building, vstore, or both).
- A **disk layout/cache locality problem** once the DB gets big (~1M+ pages of stuff to page through).
- Almost certainly **WAL + vstore + index overhead** amplifying once we hit tens of thousands of nodes.

---

## 3. Bulk Ingestion: Make Big Batches Actually Scale

The main ingestion path for the code graph is `Database::create_typed_batch`:

- `src/ffi/mod.rs`:
  - One `begin_write()` for all nodes + edges.
  - Per‑node:
    - `resolve_or_cache_label` (dictionary lookup).
    - `ensure_label_index` (may recheck per node).
    - `typed_props_to_storage` converting each `TypedPropEntry`.
    - `graph.create_node(...)`.
  - Per‑edge:
    - `resolve_or_cache_type`.
    - `TypedNodeRef::resolve` for src/dst.
    - `typed_props_to_storage` again.
    - `graph.create_edge(...)`.
  - `graph.flush_deferred_writes` then `commit`.

### 3.1. Pre‑index and Label Handling

Today:

- `ensure_label_index` is invoked **for every node** (and in `mutate`, for every create op).
  - `src/ffi/mod.rs` wraps `graph.has_label_index` + `graph.create_label_index`.

Plan:

- Inside `create_typed_batch`:
  - Maintain a small `HashSet<LabelId>` of labels we’ve already ensured within this batch.
  - Before calling `ensure_label_index`, check that set; only do the expensive `has_label_index` once per label.
- For known schemas (like the code graph):
  - Use `ensure_label_indexes(&[String])` once up front (e.g., `['Node']`) instead of per-node checks.

Expected effect:

- Turn label-index maintenance from `O(#nodes)` into `O(#labels)` per batch.
- Remove a huge number of redundant catalog lookups and index existence checks.

### 3.2. Schema‑aware Property ID Resolution

Current behavior:

- `typed_props_to_storage` (`src/ffi/mod.rs`) for every node and edge:
  - For each property:
    - `resolve_or_cache_prop` lookup in `HashMap<String, PropId>`.
    - `TypedPropEntry::to_prop_value_owned` converts to a value type.

For the code graph:

- The node schema is fixed and small:
  - `name`, `filePath`, `startLine`, `endLine`, `codeText`, `language`, `metadata`.
- Edge schema is also fixed:
  - `weight`, `linkKind`.

Plan:

- Add an internal “schema descriptor” for typed batches:
  - First time we see a `TypedNodeSpec` for a given label with a given `props` key set, resolve all `PropId`s and keep them in a compact structure (e.g., `Vec<(PropId, TypedPropType)>`).
  - For subsequent nodes with that same key set/order:
    - Skip string → `PropId` lookup; just use pre‑resolved IDs.
  - Similarly for edges.
- Integration:
  - `create_typed_batch` can detect repeating schemas easily (the code‑graph bench uses uniform specs).

Expected effect:

- For code graphs (where node schemas are uniform and simple), property resolution cost becomes almost purely value conversion, not name lookup + map overhead.

### 3.3. Trusted Endpoints for Handle-Based Edges

Observation:

- There is already a `GraphWriter` (`src/storage/graph/writer.rs`) that supports:
  - `trusted_endpoints: bool` to skip endpoint existence checks when validated in bulk.
  - An `exists_cache` LRU for non-trusted mode.
- However, `create_typed_batch` currently calls `graph.create_edge`, which always runs `ensure_node_exists` for src/dst per edge.

Plan:

- In `create_typed_batch`:
  - Detect when all edges use handle-based references to nodes within the same batch.
  - Pre-resolve handles to `NodeId`s after node creation.
  - Use `GraphWriter::try_new` with `CreateEdgeOptions { trusted_endpoints: true, .. }` and a simple `BulkEdgeValidator` over those IDs.
  - Call `writer.create_edge` instead of `graph.create_edge` so per-edge endpoint checks are skipped.
- For mixed batches (some edges reference external IDs), keep current behavior initially and treat mixed support as future work.

Expected effect:

- Eliminates 2 B-tree lookups per edge for same-batch edges.
- For 200k edges this saves ~400k existence probes in the hot ingest path.

### 3.4. Chunked Bulk Ingestion (Non‑atomic Mode)

The biggest structural issue at larger scales is doing 50k/200k in a single transaction. That pushes:

- WAL size way up.
- BTree/index updates into a worst‑case behavior regime.
- Memory usage and vstore extent management into stress territory.

We want a dedicated **bulk‑ingest API**, separate from the normal atomic API.

Plan:

- Keep `create_typed_batch(&TypedBatchSpec)` exactly as it is (single txn, atomic).
- Introduce a bulk-load handle API:

  ```rust
  pub struct BulkLoadHandle { .. }
  pub struct BulkLoadStats { .. }

  impl Database {
      pub fn begin_bulk_load(&self) -> Result<BulkLoadHandle>;
  }

  impl BulkLoadHandle {
      pub fn load_nodes(&mut self, nodes: &[TypedNodeSpec]) -> Result<Vec<NodeId>>;
      pub fn load_edges(&mut self, edges: &[TypedEdgeSpec]) -> Result<Vec<EdgeId>>;
      pub fn finish(self) -> Result<BulkLoadStats>;
  }
  ```

- Behavior:
  - Internally chunk nodes into e.g. 5–10k per txn, edges into e.g. 50–100k per txn.
  - Within each chunk:
    - `begin_write`, insert chunk, `flush_deferred_writes`, `commit`.
  - Fsync strategy:
    - Auto-detect based on total size and allow batching multiple chunks before fsync when safe.

- Semantics:
  - Explicitly non-atomic: if a crash happens mid-ingest, some chunks are committed, some not.
  - DB remains consistent but partially ingested.
  - Higher-level tooling can drop/recreate or verify completeness if it needs all-or-nothing.

Expected impact:

- Writes scale much closer to linearly: 10× more data → ~10× time, not 100×.
- WAL files are smaller and easier to checkpoint.
- Indexes see many medium‑size updates instead of one huge barrage.

---

## 4. Storage + WAL / Pager Tuning for 1M-Edge Graphs

Key components:

- Pager options (page size, cache, WAL coalescing, autocheckpoint).
- VStore for variable‑length properties (codeText, metadata).
- Index store’s use of BTree/Chunked indexes (`src/storage/index/store.rs`).

### 4.1. WAL and Checkpoint Configuration for Bulk Loads

Plan:

- For bulk‑ingest workloads:
  - Expose or reuse higher‑level tunables as “pragmas” (`Database::pragma` in `src/ffi/mod.rs`):
    - `synchronous`.
    - `wal_coalesce_ms`.
    - `autocheckpoint_ms`.
  - Recommend a bulk‑ingest profile:
    - Larger `autocheckpoint_ms` or disabled auto checkpoints during ingest, followed by a manual checkpoint.
    - Moderate WAL coalesce interval to batch fsyncs in `Normal` mode.
- Ensure `Database` drop behavior (best-effort checkpoint) doesn’t unexpectedly add huge latency at the end of big loads when bulk APIs are used.

### 4.2. VStore Tuning and Inline Thresholds

Current behavior:

- Small values are stored inline in property blobs.
- Larger values spill into vstore pages and carry per-value overhead.
- Defaults (from `graph_types`):
  - `DEFAULT_INLINE_PROP_BLOB = 128` bytes.
  - `DEFAULT_INLINE_PROP_VALUE = 48` bytes.

Plan:

- Instrument vstore usage via `VStoreMetricsSnapshot` and correlate with:
  - Bytes logically stored vs bytes actually written (`bytes_written`, `extent_pages`).
- If overhead is high at code-graph scales:
  - Increase `inline_prop_value` threshold modestly for this workload.
  - Consider compressing or differently storing large `codeText` fields if they dominate space.

Goal:

- Keep DB size for a 200k-node, 1M-edge code graph well under hundreds of MB, not drifting toward multi‑GB.

---

## 5. Read/Traversal Path for Steady-State Workloads

Steady‑state code-graph workloads need:

- Fast lookups by name / file / position.
- Fast neighbors and BFS traversals.

Key APIs:

- `get_nodes`, `get_node_data`, `get_node_prop_counts` (`src/ffi/mod.rs`).
- `neighbors_with_options` and `bfs_traversal` (`src/ffi/mod.rs`).
- Query engine for indexed lookups (`execute()` and `src/query/*`).

### 5.1. Indexed Lookups for Code Graphs

Typical key lookups:

- `Node.name` (symbol name).
- Possibly `(filePath, startLine, endLine)`.

Plan:

- Treat `Node.name` as a first‑class property index:
  - Use `ensure_property_index("Node", "name", "chunked"|"btree", "string")` during schema setup.
- Use realistic index configurations in benchmarks:
  - In both `realistic_bench` and `graph_ops_bench`, ensure these indexes exist before the ingest phase and don’t rebuild them repetitively.
- For position‑based lookups:
  - Consider composite index style or encoded key combining `(filePath, startLine)` to avoid scanning.

Expected effect:

- Keep indexed lookups at sub‑millisecond latency even at 1M+ edges.

### 5.2. Batched Reads and Locality

Current `SNAPSHOT_BATCH` mode in `realistic_bench`:

- Precomputes a list of node IDs, then calls `db.get_nodes(batch)` in chunks.
- `get_nodes` creates a single snapshot then iterates `graph.get_node` in ID order.
- The access pattern is pseudo-random (`(i * 17) % len`).

Plan:

- For batched workloads, reorder read IDs by storage locality:
  - e.g., sort by ID (if IDs roughly reflect storage location) or by a page/extent ID before the fetch.
  - Reassemble results back into original order when returning to the client.

Expected effect:

- Reduce page faults and dramatically improve read throughput with large DBs.

### 5.3. Optimized BFS / Neighbor Traversals

Existing APIs (`neighbors_with_options`, `bfs_traversal`) are already low-level and use latest-committed snapshots.

Plan:

- Ensure traversals:
  - Avoid materializing full node records unless necessary (prefer IDs + minimal props).
  - Use adjacency structures efficiently.
- Once ingestion and space are fixed, measure neighbors/BFS at ~1M edges using the code-graph schema to ensure they remain in healthy ranges.

---

## 6. Profiling and Space Breakdown

We need to profile the 880MB footprint for 50k/200k to understand where space is going.

Plan:

- Add detailed per-tree and per-component size reporting:
  - `nodes` B-tree pages and bytes.
  - `edges` B-tree pages and bytes.
  - `adj_fwd` / `adj_rev` adjacency trees.
  - `version_log`.
  - `vstore` extents.
  - Label and property indexes.
- Expose this via an extended stats path (e.g., `sombra stats --detailed`).

Once we have the breakdown:

- If vstore dominates → tune inline thresholds and/or storage for `codeText`.
- If adjacency dominates → review key encoding and fanout.
- If version log dominates → review MVCC and checkpoint strategy for bulk loads.

---

## 7. API Design and Branching Strategy

**API split:**

- **Atomic path:** `create_typed_batch` (current behavior), single transaction, strongly consistent.
- **Non‑atomic bulk path:** new begin/end bulk load APIs for large code-graph loads.

**Semantics:**

- Bulk API explicitly documents that:
  - Crash mid-ingest may leave partially ingested data.
  - DB remains consistent.
  - Higher layers can drop/recreate if they need all-or-nothing semantics.

**Bindings (Node/Python):**

- Expose bulk load as a dedicated API (e.g., `beginBulkLoad`, `bulkLoadNodes`, `bulkLoadEdges`, `endBulkLoad`).
- Internally auto-detect optimal chunk sizes and fsync strategies based on workload size.

---

## 8. Suggested Order of Implementation

1. **Profiling infrastructure**
   - Add detailed per-tree and vstore size breakdown.
   - Wire into stats tooling for easy toggling.

2. **Ingestion correctness & scaling (quick wins)**
   - Batch‑local label index caching in `create_typed_batch` / `mutate`.
   - Use `GraphWriter` trusted endpoints for handle-based edges.
   - Schema‑aware property ID resolution so typed batch cost is linear in (#nodes + #edges).

3. **Bulk load API**
   - Implement `begin_bulk_load` / `load_nodes` / `load_edges` / `finish`.
   - Internally chunk, batch fsyncs, and apply all quick-win optimizations.

4. **Space and layout**
   - Use profiling to target vstore/adjacency/version-log optimizations.
   - Tune inline thresholds and index configurations for code-graph workloads.

5. **Read/traversal performance**
   - Improve `get_nodes` batched locality.
   - Re‑benchmark neighbors and BFS at code-graph scale; ensure they’re healthy.

6. **Polish and documentation**
   - Provide clear “bulk load” and “steady‑state” configuration recipes (pager settings, indexes).
   - Wire these into `realistic_bench` and `graph_ops_bench` so we have repeatable perf baselines at 1M+ edges.
