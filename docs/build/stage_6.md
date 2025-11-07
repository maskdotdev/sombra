# 📄 STAGE 6 — Node/Edge Catalogs + Adjacency Indexes

**Crates:** `sombra-storage` (new), uses `sombra-btree`, `sombra-vstore`, `sombra-catalog`, `sombra-concurrency`, `sombra-pager`, `sombra-types`
**Outcome:** durable CRUD for nodes and edges with typed property blobs (inline or overflow via VStore) plus forward and reverse adjacency indexes that power neighborhood expansion APIs and degree counting.
**Audience:** engine developers & coding agent

---

## 0) Goals & Non-Goals

**Goals**

* Ship a single-writer storage crate (`sombra-storage`) that exposes node/edge CRUD backed by Stage 4 B+ trees and Stage 5 VStore/dictionaries.
* Define compact node/edge row encodings with predictable scan costs and the ability to spill oversized property values to the VStore.
* Maintain forward (`(src,type,dst,edge)`) and reverse (`(dst,type,src,edge)`) adjacency indexes in lock-step with the edge catalog.
* Provide neighbor iteration and degree APIs that the Stage 8 query layer can call from bindings.
* Enforce optional endpoint existence checks and surface rich errors when referential integrity fails.
* Remain crash-safe by routing every dirty page through the Stage 3 WAL.

**Non-Goals**

* Secondary indexes on labels or properties (Stage 7).
* Multi-writer MVCC (Stage 10+).
* Advanced graph constraints (unique edges, conditional triggers, etc.).

---

## 1) Crate Layout & Catalog Metadata

### 1.1 `sombra-storage` modules

* `lib.rs` — `Graph` facade, option structs, and crate re-exports.
* `node.rs` — encode/decode for node rows plus view helpers.
* `edge.rs` — edge row codec and validation utilities.
* `props.rs` — property map encoder/decoder shared by nodes and edges.
* `adjacency.rs` — wrappers around the FWD/REV B+ trees and neighbor cursor plumbing.
* `degree.rs` — optional degree cache support (`cfg(feature = "degree-cache")`).
* `metrics.rs` — counters, gauges, and tracing hooks wired into operations.
* `ffi.rs` — C-compatible adapters consumed by `sombra-ffi` and the bindings.

Support files: `tests/` for crate-level integration scenarios, `benches/` for expand throughput, and documentation comments referencing this stage doc.

### 1.2 Catalog trees and metadata records

| Tree name  | Key encoding                                       | Value encoding                         | Purpose |
| ---------- | -------------------------------------------------- | -------------------------------------- | ------- |
| `nodes`    | `NodeId` (`u64` big-endian)                        | `NodeRow` (see §2.1)                   | Node catalog |
| `edges`    | `EdgeId` (`u64` big-endian)                        | `EdgeRow` (see §2.2)                   | Edge catalog |
| `adj_fwd`  | `(src: u64, ty: u32, dst: u64, edge: u64)` (BE)    | empty (`()`)                           | Forward adjacency |
| `adj_rev`  | `(dst: u64, ty: u32, src: u64, edge: u64)` (BE)    | empty (`()`)                           | Reverse adjacency |
| `degree`*  | `(node: u64, dir: u8, ty: u32)` (BE)               | `u64`                                  | Degree cache (`feature = "degree-cache"`) |

*Stored in the Stage 3 catalog meta page:*

* `next_node_id: u64` and `next_edge_id: u64` counters (monotonic, never reuse IDs).
* `config_flags: u32` bit-pack (e.g., enforce endpoints, distinct default mode).
* Inline thresholds and value limits (`inline_prop_blob`, `inline_prop_value`).
* `degree_cache_enabled: bool` (mirrors build feature for quicker startup checks).

### 1.3 External dependencies

* Dictionaries from Stage 5 (`LabelId`, `TypeId`, `PropId`) are read via `sombra-catalog` handles.
* VStore (Stage 5) supplies `VRef` write/read/free for large property payloads.
* Concurrency manager (Stage 3) still enforces single-writer invariants; Stage 6 does not add new latching.

---

## 2) On-Disk Layout & Encodings

### 2.1 Node rows

| Field               | Type/Encoding                       | Notes |
| ------------------- | ----------------------------------- | ----- |
| `label_count`       | `u8`                                | Up to 255 inline labels (spill via VStore otherwise). |
| `labels[]`          | `label_id (u32 BE)` × `label_count` | Sorted ascending and deduplicated before encode. |
| `prop_repr_tag`     | `u8` (`0=Inline`, `1=VRef`)         | Controls which payload follows. |
| `prop_inline_len`   | `u16` (present if `prop_repr_tag=0`) | Length in bytes of the property blob. |
| `prop_inline_bytes` | raw                                 | Property map (§2.3). |
| `prop_vref`         | `VRef` (present if `prop_repr_tag=1`) | Overflow handle into VStore. |

Inline blob threshold defaults to **128 bytes** and is configurable. Rows are packed without padding; multi-byte integers are big-endian to preserve B+ tree ordering. Debug assertions verify `label_count` matches the encoded array length.

### 2.2 Edge rows

| Field             | Type/Encoding     | Notes |
| ----------------- | ----------------- | ----- |
| `src_id`          | `u64 BE`          | Must reference an existing node when endpoint enforcement is enabled. |
| `dst_id`          | `u64 BE`          | Same rules as `src_id`. |
| `type_id`         | `u32 BE`          | Label dictionary ID for the edge type. |
| `prop_repr_tag`   | `u8`              | Mirrors node encoding. |
| `prop_inline_len` | `u16` (if inline) | Length of inline property blob. |
| `prop_inline`     | raw               | Property map when inline. |
| `prop_vref`       | `VRef`            | Overflow handle when not inline. |

Edge rows intentionally duplicate `type_id` to keep adjacency keys compact (no extra dictionary lookups in hot paths).

### 2.3 Property map encoding

* Header: `entry_count (varuint)`.
* Each entry is encoded in property ID order:
  * `prop_id (varuint)` — dictionary ID from Stage 5.
  * `type_tag (u8)` — `0=null, 1=bool, 2=int64, 3=float64, 4=str, 5=bytes, 6=datetime, 7=date`.
  * `repr_tag (u8)` — `0=InlineValue`, `1=VRefValue` (only for `str`/`bytes`).
  * `value payload`:
    * `null` → no payload.
    * `bool` → `u8` (`0` or `1`).
    * `int64` → zigzag varint (Stage 1 codec).
    * `float64` → 8 bytes little-endian IEEE 754.
    * `str`/`bytes` inline → `len (varuint)` + raw bytes (limited by `INLINE_PROP_VAL_MAX`, default 48).
    * `str`/`bytes` via VStore → embed `VRef` after emitting the `VRefValue` tag.
    * `datetime`/`date` → `i64` (epoch millis/days) encoded as zigzag varint.

Encoders validate that entries arrive sorted by `prop_id` with no duplicates. Decoders support skip/binary search to accelerate `PropPatch` application.

### 2.4 Adjacency key encoding

* FWD key = `(src_id: u64 BE, type_id: u32 BE, dst_id: u64 BE, edge_id: u64 BE)` → value is empty.
* REV key = `(dst_id: u64 BE, type_id: u32 BE, src_id: u64 BE, edge_id: u64 BE)` → value is empty.

Including `edge_id` makes parallel edges distinguishable and gives deterministic ordering for cursor pagination. Keys reuse the same order-preserving codecs introduced in Stage 4.

### 2.5 Degree cache (optional)

When the `degree-cache` feature is enabled, the degree tree stores `(node_id, dir, type_id)` as the key where `dir` is `0=Out`, `1=In`, `2=Both` and the value is `u64 degree`. The cache updates within the same transaction as edge insert/delete. A background validator (debug builds) can recompute degrees from adjacency trees to assert cache correctness.

---

## 3) Transactions, IDs & Concurrency

* `Graph::open` accepts handles to the Stage 3 pager, WAL, and catalog. On first open it creates missing trees and seeds `next_node_id`/`next_edge_id` to `1`. Metrics observers can be injected via `GraphOptions::metrics`.
* ID allocation occurs inside a write transaction: fetch, increment, persist to the meta record, and return the previous value.
* The Stage 3 concurrency layer still enforces **single-writer, multi-reader** semantics. Stage 6 does not introduce additional locks; it relies on B+ tree page pins and the WAL for safety.
* Referential integrity checks run at **edge create/update** and optionally just before commit. Configuration decides whether missing endpoints produce a hard error or a logged warning.
* Read transactions keep stable snapshots by opening B+ tree cursors at the current durable LSN (from Stage 3).

---

## 4) CRUD & Adjacency Algorithms

### 4.1 Node creation

1. Normalize labels: convert to `LabelId`, deduplicate, and sort.
2. Validate properties (sorted keys, supported types) and encode via `props::encode` choosing inline or VRef.
3. Build `NodeRow` into a scratch buffer; if overflow occurs, free intermediate VRefs and return an error.
4. Insert `(node_id, row)` into the `nodes` B+ tree.
5. (Stage 7) Hooks will update label/property secondary indexes.

### 4.2 Node updates

* Accept `PropPatch`, defined as ordered operations (`Set`, `Delete`) keyed by `PropId`.
* Decode the existing property blob (inline or VRef) into a temporary structure.
* Apply patch while maintaining sorted order; reuse inline buffer if the result fits, otherwise migrate to VStore.
* Update the row in place (B+ tree `put` overwrites leaf slot) and free any orphaned VRefs.

### 4.3 Node deletion

* `DeleteNodeOpts` exposes `mode: Restrict|Cascade` plus `force_free_props` (debug guard).
* `Restrict` scans FWD and REV ranges for non-empty iterators; returning an error if edges remain.
* `Cascade` walks FWD then REV adjacency ranges, deleting each edge via the edge delete path (dedupe by `edge_id` when Both direction surfaces the same edge twice).
* Remove node row, free property VRefs, and release dictionary references (Stage 5 caches observe label/type usage counts).
* **Implementation note:** current single-writer plumbing only exposes read-range cursors against a stable snapshot. Cascade deletes therefore rely on the last durable view and will miss edges created earlier in the same write transaction. Once the B+ tree exposes range iteration over `WriteGuard`, revisit this to surface in-flight adjacency mutations.

### 4.4 Edge creation

1. Optionally verify endpoints by probing `nodes[src]` and `nodes[dst]`.
2. Encode edge properties and build `EdgeRow` (inline or VRef).
3. Insert `(edge_id, row)` into `edges` tree.
4. Insert adjacency keys into `adj_fwd` and `adj_rev` in the same transaction.
5. Update degree cache counters if enabled.

### 4.5 Edge updates

* Properties patching mirrors node update flow.
* Changes to `src`, `dst`, or `type` are **not** allowed in-place for Stage 6; requesters must delete and recreate. If future work enables this, adjacency keys must be updated atomically.

### 4.6 Edge deletion

* Remove keys from `adj_fwd` and `adj_rev` (search by composite key).
* Delete `edges[edge_id]` and free any property VRefs.
* Update degree cache.

### 4.7 Neighbor iteration and degree

* `neighbors(id, Dir::Out, ty?)` — open a range cursor over `adj_fwd` spanning `(src=id, ty=*|ty, dst=*, edge=*)`.
* `Dir::In` mirrors using `adj_rev`. `Dir::Both` merges two cursors, emitting neighbors in sorted order (tie-breaking on `(neighbor, edge_id)`).
* `ExpandOpts.distinct_nodes` collapses duplicates by neighbor ID (needed when multiple edge types are scanned concurrently). `GraphOptions::distinct_neighbors_default` toggles this behaviour globally.
* `NeighborCursor` batches results (`batch` option) to reduce FFI crossings.
* `degree` uses the cache if present; otherwise it counts keys through the adjacency cursor. For `Dir::Both`, compute Out + In (minus duplicates when `src == dst`).

---

## 5) Public API & FFI Surface

### 5.1 Rust API (core)

```rust
pub enum PropValue<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(Cow<'a, str>),
    Bytes(Cow<'a, [u8]>),
    Date(Date),
    DateTime(DateTime),
}

pub struct PropEntry<'a> {
    pub prop: PropId,
    pub value: PropValue<'a>,
}

pub enum PropPatchOp<'a> {
    Set { prop: PropId, value: PropValue<'a> },
    Delete { prop: PropId },
}

pub struct PropPatch<'a> {
    pub ops: &'a [PropPatchOp<'a>], // sorted by prop id
}

pub struct NodeSpec<'a> { pub labels: &'a [LabelId], pub props: &'a [PropEntry<'a>] }
pub struct EdgeSpec<'a> { pub src: NodeId, pub dst: NodeId, pub ty: TypeId, pub props: &'a [PropEntry<'a>] }

pub struct DeleteNodeOpts { pub mode: DeleteMode }
pub enum DeleteMode { Restrict, Cascade }

pub struct NodeView<'a> { pub labels: SmallVec<[LabelId; 4]>, pub props: PropMapView<'a> }
pub struct EdgeView<'a> { pub src: NodeId, pub dst: NodeId, pub ty: TypeId, pub props: PropMapView<'a> }

pub struct Neighbor { pub neighbor: NodeId, pub edge: EdgeId, pub ty: TypeId }
pub struct NeighborCursor<'a> { /* internal state, implements Iterator */ }

impl Graph {
    pub fn open(opts: GraphOptions) -> Result<Self>;
    pub fn create_node(&self, tx: &mut WriteTx, spec: NodeSpec) -> Result<NodeId>;
    pub fn update_node(&self, tx: &mut WriteTx, id: NodeId, patch: PropPatch) -> Result<()>;
    pub fn delete_node(&self, tx: &mut WriteTx, id: NodeId, opts: DeleteNodeOpts) -> Result<()>;
    pub fn create_edge(&self, tx: &mut WriteTx, spec: EdgeSpec) -> Result<EdgeId>;
    pub fn update_edge(&self, tx: &mut WriteTx, id: EdgeId, patch: PropPatch) -> Result<()>;
    pub fn delete_edge(&self, tx: &mut WriteTx, id: EdgeId) -> Result<()>;
    pub fn get_node(&self, tx: &ReadTx, id: NodeId) -> Result<Option<NodeView<'_>>>;
    pub fn get_edge(&self, tx: &ReadTx, id: EdgeId) -> Result<Option<EdgeView<'_>>>;
    pub fn neighbors(&self, tx: &ReadTx, id: NodeId, dir: Dir, ty: Option<TypeId>, opts: ExpandOpts) -> Result<NeighborCursor<'_>>;
    pub fn degree(&self, tx: &ReadTx, id: NodeId, dir: Dir, ty: Option<TypeId>) -> Result<u64>;
}
```

All views borrow decoded buffers valid for the lifetime of the transaction. `PropMapView` exposes iterators plus `get(prop_id)` helpers.

### 5.2 FFI surface (`sombra-ffi`)

* Export C ABI functions for each CRUD call, plus cursor helpers (`graph_neighbors_next`).
* Represent properties as flat arrays of `{prop_id, type_tag, union value}` mirroring the on-disk encoding to minimize copies.
* Provide error codes for constraint violations: `SOMBRA_ERR_NODE_NOT_FOUND`, `SOMBRA_ERR_EDGE_ENDPOINT_MISSING`, etc.
* Neighbor cursor batches marshalled into structs `{neighbor_id, edge_id, type_id}` for bindings.

### 5.3 Language bindings

* **Node/TypeScript:** asynchronous APIs returning `Promise<NodeHandle>`/`AsyncIterable<Neighbor>`. `batchSize` maps onto `ExpandOpts.batch`.
* **Python:** synchronous wrappers with context-managed transactions; iterators yield `Neighbor` dataclasses. Degree exposed as `tx.degree(node_id, dir='out', type_id=None)`.
* Bindings handle dictionary resolution (Stage 5) by converting between `StrId` and strings on the client boundary.

---

## 6) Durability, Recovery & Integrity

* All dirty pages (catalog, adjacency, degree cache, VStore chains) go through the Stage 3 WAL. Flush policy remains "write-ahead": WAL frame fsync precedes page cache writes.
* On recovery:
  1. Replay WAL to reconstruct `nodes`, `edges`, and adjacency trees.
  2. Rebuild in-memory caches (next IDs, config flags, degree presence) from meta.
  3. If the degree cache is enabled, optionally recompute entries lazily to avoid long boot times.
* Referential integrity is validated during recovery: scan `edges` and assert both adjacency entries exist; missing entries trigger corrective rebuild or panic (configurable).
* VStore leaks are detected by comparing live VRefs in node/edge rows against the VStore freelist; discrepancies log diagnostics.

---

## 7) Configuration & Observability

* `GraphOptions` fields:
  * `inline_prop_blob: usize` (default 128).
  * `inline_prop_value: usize` (default 48).
  * `enforce_endpoints: bool` (default true).
  * `default_distinct: bool` (affects `ExpandOpts`).
  * `degree_cache: bool` (mirrors feature flag).
  * `distinct_neighbors_default: bool` (controls `ExpandOpts.distinct_nodes` default).
  * `metrics: Arc<dyn StorageMetrics>` optional observer hook.
* Metrics (emitted via `metrics.rs`):
  * Counters: `nodes.created`, `nodes.deleted`, `edges.created`, `edges.deleted`, `props.inline_bytes`, `props.vref_bytes`.
  * Gauges: `degree_cache.entries`, `adjacency.pages_pinned`.
  * Histograms: encode/decode duration, neighbor batch sizes.
* Debug tooling: `graph.dump_node(id)` and `graph.dump_edge(id)` behind `cfg(test)` for inspecting encoded rows; `validate_degree_cache()` asserts invariants.

---

## 8) Tests & Benchmarks

**Correctness**

* Deterministic unit tests for encoder/decoder round-trips (inline + VRef paths).
* Randomized graph builder (N nodes, M edges) verifying:
  * Edge↔adjacency bijection and FWD/REV symmetry.
  * Degree calculations match key counts.
  * Cascade deletes remove all incident edges and free VRefs.
  * `tests/stress.rs` drives randomized patch workloads and validates adjacency/degree invariants.
* Property patch tests covering Set/Delete combinations, inline→VRef transitions, and reusing inline buffers.

**Crash & Recovery**

* Fault-injection harness forcing crashes between adjacency updates; after replay, invariants hold.
* WAL truncation fuzzing to ensure partially written transactions roll back cleanly.

**Performance**

* Expand throughput micro-benchmark on synthetic power-law graphs (Zipf α≈1.1) for Out/In/Both and various `batch` sizes.
* Degree cache A/B benchmark: compare cached vs uncached degree queries under high parallel read load.
* Property encoding benchmark to determine default thresholds.
* `cargo bench -p sombra-storage --bench expand` measures neighbor/degree throughput (Criterion).

Acceptance criteria: no invariant failures across 10M-edge stress, expand throughput meets team baseline, VStore freelist reconciles after massive deletes.

---

## 9) Implementation Phases (Stage 6 roll-out)

1. **Foundation & Metadata**
   * Extend pager/catalog metadata with Stage 6 counters and tree roots.
   * Scaffold the `sombra-storage` crate (module skeletons, feature flags, options structs).
   * Wire initial `Graph::open` plumbing that locates/creates catalog trees and persistent counters.
2. **Property Encoding & Catalog Rows**
   * Implement property map encoder/decoder (inline vs VRef) and supporting codecs.
   * Define `NodeRow`/`EdgeRow` packing plus helpers for label normalization and property lifecycle.
   * Land node/edge CRUD stubs that read/write catalog rows without adjacency updates.
3. **Adjacency Indexes & Degree Accounting**
   * Build FWD/REV adjacency B+ tree wrappers, key codecs, and referential-integrity checks.
   * Integrate adjacency maintenance into edge create/delete/update flows; add cascade delete logic.
   * Implement optional degree cache maintenance paths and validation hooks.
4. **Public API, Neighbor Cursor & FFI Prep**
   * Flesh out the `Graph` facade APIs (CRUD, `neighbors`, `degree`, options).
   * Implement neighbor cursor batching/merge logic and expose degree helpers.
   * Thread configuration knobs, error types, and metrics plumbing needed by bindings/FFI.
5. **Testing, Benchmarks & Observability**
* Author unit/integration tests, recovery tests, and randomized stress verifying invariants.
* Add performance benches (expand throughput, degree cache A/B) and document baseline targets.
* Finalize metrics, debug tooling, and docs updates before bindings consume the crate.

---

## 10) Step-by-Step Checklist (coding agent)

* [x] Scaffold `sombra-storage` crate, register trees in catalog metadata, and expose `Graph::open`.
* [x] Implement node and edge row codecs plus property map encoder/decoder with inline/VRef support.
* [x] Wire CRUD helpers for `nodes` and `edges`; add ID allocation logic.
* [x] Build FWD/REV adjacency wrappers and ensure inserts/deletes stay symmetric.
* [x] Implement `PropPatch` application and ensure VRefs are freed or reused correctly.
* [x] Implement optional degree cache and configuration plumbing.
* [x] Expose neighbor cursor and degree APIs; surface through `sombra-ffi` for bindings.
* [x] Add metrics hooks and configuration knobs.
* [x] Author tests (unit + integration + recovery) and performance benchmarks, documenting results.

---
