
# Insert-Path Optimization Plan (Sombra DB)

Below is a concrete, implementation-ready plan that turns your bullets into code, invariants, and tests. It’s structured so you can land work in small, safe PRs behind feature flags, with clear success metrics.

---

## Goals & Success Metrics

* **Primary:** Reduce single-row insert latency and bulk ingest throughput time.
* **Targets (first pass):**

  * **CRUD bench:** ≥3× faster `create_node`/`create_edge` paths.
  * **BTREE micro bench:** ≥5× faster leaf inserts on pages with slack; no regression on point lookups.
  * **WAL throughput:** ≥2× fewer syscalls per commit; ≥1.3× faster commit wall time at 10–100k dirty pages.
* **Non-goals for this batch:** query planner changes, cross-page rebalancing algorithms beyond what’s necessary for correctness.

---

## A. Graph Layer

### A1) Property delta & single-pass encoding

**Problem:** We clone/encode props multiple times; updates rewrite even when no changes.

**Design (real storage types):**

```rust
pub enum PropValue<'a> {
    Inline(&'a [u8]),
    Spill(VRef),
    Decoded(Value<'a>), // optional slow-path helper
}

pub struct IndexKey {
    pub index_id: IndexId,
    pub key: Vec<u8>,
}

pub struct PropDelta<'a> {
    pub sorted: SmallVec<[(PropId, PropValue<'a>); 8]>, // prop-id sorted
    pub encoded_row: Vec<u8>,                           // canonical bytes
    pub spill_vrefs: SmallVec<[VRef; 4]>,
    pub index_keys: SmallVec<[IndexKey; 8]>,
    pub row_hash64: u64,                                // SipHash64(encoded_row)
}

pub fn build_prop_delta<'a>(
    old_row: Option<&[u8]>,        // legacy or hash-appended rows
    patch: &Patch<'a>,
    labels: &[LabelId],
    idx_cache: &GraphIndexCache,
    enc: &mut RowEncoder,
) -> Result<PropDelta<'a>>;
```

* **Equality fast-path:** Pull the stored row hash (see next subsection) once, compare to `row_hash64`, and short-circuit `update_node`/`update_edge` when identical.
* **Feed indexes once:** `index_keys` flow straight into `insert_indexed_props`/`update_indexed_props_for_node`, eliminating the extra `materialize_props_owned` call inside write paths.
* **Determinism:** `encoded_row` is canonical for identical logical inputs, which keeps hashing and WAL bytes stable.

**Landing order:**

1. Add row-hash support to node/edge rows (feature `row_hash_header`).
2. Ship `PropDelta` + fast-path and gate via `prop_delta_path`.
3. Replace remaining write-path `materialize_props_owned` calls with the data already inside `PropDelta`.

**Tests:**

* Property-preserving updates emit zero WAL frames and leave page bytes untouched.
* A single inline prop change doesn’t churn spill pages unless it actually crosses the inline threshold.
* Index maintenance consumes `index_keys` only (assert no catalog scans during inserts).

#### Row hash compatibility (no reader breakage)

Appending an 8-byte hash must not force a global format bump. Two rollout options:

* **Header flag (preferred):** reuse an unused bit (`HAS_ROW_HASH`) so readers know whether a row ends with a SipHash64 suffix. Feature flag `row_hash_header` controls writes; lazy migration rewrites rows opportunistically.
* **Catalog capability:** add `CatalogFeature::RowHashV1` before emitting hashes. Readers detect the optional trailing bytes (checksum verified) and accept both layouts until the capability is flipped.

Both paths keep legacy readers working, allow mixed rows per page, and require tests covering mixed-format decoding plus no-op updates with the feature disabled.

---

---

### A2) DdlEpoch + per-transaction label→indexes cache

**Problem:** We scan catalog B-trees on every mutation and have no way to invalidate caches when DDL happens.

**Design:**

```rust
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct DdlEpoch(pub u64);

pub struct CatalogEpoch {
    mem_epoch: AtomicU64,
}

impl CatalogEpoch {
    pub fn current(&self) -> DdlEpoch;
    pub fn bump_in_txn(tx: &mut WriteGuard<'_>) -> Result<DdlEpoch>; // call at every DDL
}

#[derive(Clone)]
pub struct GraphIndexCache {
    epoch: DdlEpoch,
    map: FxHashMap<LabelId, Arc<Vec<IndexDef>>>,
}

impl GraphIndexCache {
    pub fn new(epoch: DdlEpoch) -> Self;
    pub fn get_or_load(
        &mut self,
        label: LabelId,
        loader: impl Fn(LabelId) -> Result<Vec<IndexDef>>,
    ) -> Result<Arc<Vec<IndexDef>>>;
}
```

* Persist `DdlEpoch` inside catalog meta (init `0` if absent) and mirror it in memory (`CatalogEpoch`).
* Any DDL mutation (create/drop index, rebuild, etc.) must call `CatalogEpoch::bump_in_txn` after committing catalog changes.
* Each write transaction snapshots `txn.ddl_epoch = catalog_epoch.current()` and owns a fresh `GraphIndexCache`. If the global epoch changes mid-txn, the next `get_or_load` detects the mismatch, clears the map, and reloads definitions.
* Optional: keep a read-only shared cache for read txns, but write txns always have a private cache to avoid races.

**Tests / metrics:**

* Inject DDL while a workload runs; verify subsequent writes reload definitions (epoch mismatch).
* Expose `idx_cache_hits/misses`; hits per label per txn should drop to ≤1 once the cache lands.

---

### A3) Avoiding extra read txns in write path

**Problem:** `materialize_props_owned` opens a **read** txn even though we have a **write** txn.

**Design options:**

* **Option 1:** `VStore::read_with_write(&mut WriteGuard, vref) -> &[u8]` uses the pager snapshot bound to the write txn, no extra lock.
* **Option 2:** Change `materialize_props_owned` to accept `&mut WriteGuard`.

**Risks:** Reentrancy/deadlocks if `VStore` tries to start nested txns. Ensure it only borrows the pager from the write guard.

**Tests:**

* Assert only one txn active in thread during `update_*`.
* Overflow read count drops for updates.

---

### A4) Trusted endpoints for edge creation (bulk import)

**Problem:** Two node existence probes per edge.

**Design:**

```rust
pub struct CreateEdgeOptions {
    pub trusted_endpoints: bool, // default false
}

pub struct GraphWriter<'a> {
    pub opts: CreateEdgeOptions,
    pub exists_cache: LruCache<NodeId, bool>,
    pub validator: Option<BulkEdgeValidator<'a>>,
}

pub struct BulkEdgeValidator<'a> {
    pub exists_snapshot: Box<dyn Fn(NodeId) -> bool + 'a>,
    pub sample_rate: f32, // optional back-check percentage
}

impl<'a> BulkEdgeValidator<'a> {
    pub fn validate_batch(&self, edges: &[(NodeId, NodeId)]) -> Result<()>;
}
```

* **Bulk loaders** must run `validate_batch` (or supply an equivalent snapshot/Bloom filter) before flipping `trusted_endpoints`. The CLI/ETL tooling can enforce this so “trust” is earned, not assumed.
* **OLTP default:** `trusted_endpoints=false`, but `exists_cache` plus `preload_nodes` still reduce redundant probes.

**Tests:**

* With validator + `trusted_endpoints=true`, adjacency inserts must skip redundant node lookups (check pager metrics).
* Without validation, enabling `trusted_endpoints` should fail fast at the tool boundary.
* Error semantics remain unchanged when `trusted_endpoints=false`.

---

### A5) Batched adjacency / index writes

**Problem:** Point-wise `BTree::put` causes extra splits and cursor churn.

**Design (B-tree API):**

```rust
pub struct PutItem<'a> {
    pub key: &'a [u8],
    pub val: &'a [u8],
}

pub struct PutManyStats {
    pub leaf_rebuilds: u64,
    pub in_place: u64,
    pub splits: u64,
}

impl BTree {
    /// Keys must be non-decreasing. Reuses a cursor and does in-place edits when possible.
    pub fn put_many<'a, I>(&mut self, it: I) -> Result<PutManyStats>
    where I: IntoIterator<Item = PutItem<'a>>;
}
```

* **Usage:** Group adjacency keys by source (already naturally sorted in many ingest flows). Same API used by property-index maintenance.
* **Cursor reuse:** Maintain current leaf page; avoid binary search per key; append or near-insert when ordered.

**Tests:**

* Insert N sorted adjacency pairs → ≤1 split per leaf (vs many).
* Validate error on unsorted input (debug builds).

---

## B. B-Tree Engine

### B1) In-place inserts with slot directory

**Current:** Rebuild whole leaf into `Vec<(key,val)>` on every insert.

**Leaf layout (target):**

```
[ header | cell area (records) ... free gap ... | slot directory (u16 offsets) ]
```

**Algorithm (happy path, no split):**

1. **Search:** Binary-search slot directory for insert position.
2. **Size check:** Compute `need = key_sz + val_sz + cell_hdr` and `have = free_gap`.
3. **If have ≥ need:**

   * Move the contiguous suffix of the **cell area** using `copy_within` to open `need` bytes.
   * Write cell bytes at the gap start.
   * Insert one `u16` into slot directory (shift higher slots with `copy_within`).
   * Update header free-gap pointer & counts.
4. **Else:** Fall back to rebuild (existing path), or split if under split threshold.

**Engineering notes:**

* **Scratch buffer:** One per tree (e.g., 8–16 KiB) for the rebuild path to avoid alloc churn.
* **Safety:** Use `debug_assert!`s for no overlap moves; add fuzz tests.

**Tests:**

* In-place insert leaves page CRC stable except changed regions (verified via WAL bytes).
* Random key insertions produce same logical contents as old algorithm.

---

### B2) In-place deletes & merges

**Algorithm (delete):**

1. Find slot, get cell bounds, `copy_within` to close the hole.
2. Remove slot entry, shift directory left.
3. If page under `min_fill`, attempt **borrow from siblings**; if not possible, **merge**:

   * Use existing merge code, but prefer **in-place** concatenation when both leaves have enough contiguous free gap.

**Tests:**

* Delete single key doesn’t rewrite entire page.
* Borrow/merge correctness with varied neighbor sizes.

---

## C. Persistence Layer

### C1) Commit path: zero-copy WAL frames where possible

**Current:** Sort dirty pages, clone each into `Vec<u8>`, recompute CRC on whole page, then write.

**Design:**

* **Dirty tracking:** Maintain `dirty_pages: SmallVec<[PageId; N]>` ordered by mark-time (already close to write order for coalescing).
* **Borrowed buffers:** Instead of cloning into `Vec<u8>`, WAL frames borrow `Arc<PageBuf>` (or guarded slices) until the commit completes.
* **CRC policy:** Recompute the **full-page** CRC in place (required by the existing pager/meta layout) immediately before enqueueing frames. No format change.

**Tests:**

* Byte-for-byte identical WAL/database images compared to the legacy path.
* Syscall count per commit drops thanks to buffer reuse + the coalesced `pwritev` worker in §C2.

---

### C2) WAL coalescing and parallelism

**Current:** One `append_frame` per page, single worker.

**Design:**

* **Coalescing:** Group consecutive pageIds (or file offsets) into a single `pwritev`. Bound groups by `wal_commit_max_frames` and `wal_commit_coalesce_ms`.
* **Parallelism:** Multiple in-flight I/O groups, capped by `numa_cores` or `IO_SQEs` hint. Preserve order constraints within a txn, but allow groups to be queued in parallel.
* **Fsync strategy:** Single `fdatasync` at group tail (or range fsync if available). Keep `Synchronous::Full` semantics.

**Metrics:** record `(frames, coalesced_writes, avg_group_len, commit_ms)`.

**Tests:**

* Crash-consistency: kill-the-process tests mid-commit; recovery must succeed.
* Coalescing improves `(frames / writes)` by ≥3× on large dirty sets.

---

### C3) Overflow (VStore) contiguous extents

**Current:** Allocate a pager page per 8 KiB chunk; one lock/unlock and WAL entry per chunk.

**Design:**

* **Extent allocator:** Pager gains `allocate_extent(n_pages) -> Extent { start, len }` with best-fit or next-fit policy; keeps a free-list of extents.
* **Streaming write:** `VStore::write_extent(Extent, reader)` maps or streams the entire payload through a single guard, emitting WAL frames over the contiguous range (which coalescing will pack into few writes).
* **Fallback:** If not enough contiguous space, chain extents; still reuse a single guard per segment.

**Tests:**

* Large blob write emits ≪ frames than chunked path.
* Read correctness with chained extents.

---

## D. Concurrency, MVCC & Safety

* **MVCC visibility:** In-place page edits happen **within** a write txn; readers see the version pinned by their snapshot. Ensure copy-on-write semantics at page level for concurrent readers (WAL already serializes).
* **Latch discipline:** In-place modifications only under exclusive page latch. No nested txn acquisition.
* **DDL epoch:** Txn begins capture the `DdlEpoch`; invalidate caches on mismatch.

---

## E. Instrumentation & Guardrails

* **Counters:**

  * `btree_in_place_inserts`, `btree_leaf_rebuilds`, `btree_splits`
  * `wal_frames`, `wal_coalesced_writes`, `wal_commit_ms_p50/p95`
  * `idx_cache_hits`, `idx_cache_misses`
* **Trace probes:** mark `put_many` groups and commit phases.
* **Feature flags:**

  * `row_hash_header`, `prop_delta_path`, `btree_inplace`, `wal_coalesce`, `vstore_extents`, `adjacency_put_many`.
* **Fallbacks:** If any feature panics under `RUSTFLAGS='-Zpanic_abort_tests'`, auto-disable via env to keep server usable.

---

## F. Tests & Benches

### Unit & property tests

* Row encoding is canonical and stable (same inputs → same bytes).
* In-place insert/delete round-trips arbitrary key/value bytes (proptest).
* Index cache respects epoch invalidation.

### Crash & durability

* Kill mid-commit at random frame boundaries; recover and verify checksum & contents.
* Fuzzy workload (mixed inserts/updates/deletes) with periodic crashes.

### Microbenches (existing + add)

* `btree_insert_sequential`, `btree_insert_random`, `btree_delete_random` with varied value sizes (inline vs overflow).
* `put_many_sorted` vs `put_pointwise`.
* WAL commit with 1k/10k/100k dirty pages: frames, syscalls, wall-time.

### End-to-end benches

* `sombra-bench crud` (nodes/edges, properties, labels).
* Bulk ingest: 1M edges, sorted by `src`, with `trusted_endpoints=true` vs false.

---

## G. Rollout Plan (PR sequence)

1. **DdlEpoch plumbing + txn-local GraphIndexCache**.
2. **PropDelta (PropId/PropValue) + no-op update fast-path** (`prop_delta_path`).
3. **Row-hash storage** (`row_hash_header`) with mixed-row compatibility tests.
4. **Write-path materialization via write guard** (`VStore::read_with_write`).
5. **BTree::put_many()`** + adjacency/property-index batching.
6. **In-place leaf insert** (feature `btree_inplace`), retaining rebuild fallback.
7. **In-place delete/borrow/merge**.
8. **WAL borrowed buffers + coalesced `pwritev`**; metrics & crash tests.
9. **VStore contiguous extents + streaming writes**.
10. **GraphWriter trusted endpoints + validator tooling**.

Each step lands with metrics turned on and a gate to disable at runtime.

---

## H. Code Sketches (key bits)

### H1) Update fast-path

```rust
pub fn update_node(tx: &mut WriteGuard<'_>, id: NodeId, patch: Patch) -> Result<()> {
    let old_blob = storage::read_row_blob(tx, id)?;
    let old_hash = storage::read_row_hash64(&old_blob);
    let labels = labels_of(tx, id)?;
    let mut enc = RowEncoder::borrow(tx.arena());
    let mut idx_cache = tx.index_cache();

    let delta = build_prop_delta(Some(&old_blob), &patch, &labels, &idx_cache, &mut enc)?;
    if old_hash == Some(delta.row_hash64) {
        return Ok(()); // no-op
    }
    storage::write_row_blob(tx, id, &delta.encoded_row, &delta.spill_vrefs)?;
    index::apply_delta(tx, id, &labels, &delta.index_keys)?;
    Ok(())
}
```

### H2) Batched adjacency

```rust
pub fn insert_adjacencies(tx: &mut WriteGuard, items: &mut [AdjKeyVal]) -> Result<()> {
    items.sort_unstable_by(|a,b| a.key.cmp(&b.key)); // ensure order
    let put_iter = items.iter().map(|it| PutItem { key: &it.key, val: &it.val });
    tx.adj_tree.put_many(put_iter)?;
    Ok(())
}
```

### H3) In-place insert (leaf)

```rust
fn insert_in_place(leaf: &mut [u8], pos: usize, cell: &[u8]) -> Result<bool> {
    let hdr = Header::parse(leaf)?;
    let have = hdr.free_gap();
    let need = cell.len() + SLOT_SIZE;
    if have < need { return Ok(false); } // fallback
    // shift cell area suffix
    let insert_at = hdr.gap_start();
    let suffix = insert_at .. hdr.slot_dir_start();
    leaf.copy_within(suffix.clone(), insert_at + cell.len());
    // write cell
    leaf[insert_at .. insert_at + cell.len()].copy_from_slice(cell);
    // shift slots and add new slot
    shift_slots_right(leaf, pos)?;
    write_slot(leaf, pos, insert_at as u16)?;
    Header::update_after_insert(leaf, cell.len())?;
    Ok(true)
}
```

---

## I. Risks & Mitigations

* **Subtle page-layout bugs:** Heavy fuzzing + differential testing against rebuild path; assert invariants in debug.
* **Cache invalidation errors:** Centralize `DdlEpoch` bump at all DDL call sites; add tests that force DDL mid-workload.
* **I/O semantics drift:** Keep `Synchronous::Full` default; coalescing must not reorder frames across txns.

---

## J. What to measure after each PR

* p50/p95 for:

  * `create_node`, `create_edge`, `update_node(no-op)`, `update_node(1 small prop)`
  * WAL `commit_ms` and `writes_per_commit`
  * B-tree `splits/insert`
* Allocation profile (`jemalloc`/`malloc`) deltas: total bytes and alloc count in CRUD bench.
* CPU profile hot spots shifting out of `search_leaf_bytes` and `insert_into_leaf` rebuilds.

---

## K. Quick Checklist (copy into your issue tracker)

* [ ] `DdlEpoch` persisted, loaded, and bumped at every DDL site
* [ ] Txn-local `GraphIndexCache` (hits/misses metrics)
* [ ] `PropDelta` (PropId/PropValue) + no-op update fast-path
* [ ] Row-hash storage feature + mixed-row compatibility tests
* [ ] `VStore::read_with_write` / write-path materialization reuse
* [ ] `BTree::put_many` wired into adjacency/property indexes
* [ ] In-place insert (+ scratch buffer + fuzz/property tests)
* [ ] In-place delete/merge w/ borrow fallback
* [ ] WAL borrowed buffers + full-page CRC + coalescing metrics
* [ ] VStore contiguous extents / streaming overflow writes
* [ ] Bulk `GraphWriter` with `trusted_endpoints` + validator enforcement
* [ ] Bench matrix updated & baselined; feature flags + dashboards

---

If you want, I can tailor the first PR diff outline (files, function signatures, and minimal tests) for `PropDelta + no-op update fast-path` so you can start coding immediately.
