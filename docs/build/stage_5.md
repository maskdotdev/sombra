# 📄 STAGE 5 — Var-Len Store & Dictionaries

**Crates:** `sombra-vstore`, `sombra-catalog` (dictionary module), reuse `sombra-pager`, `sombra-btree`
**Outcome:** durable variable-length storage for oversized values plus reusable string dictionaries for labels, types, property names, and optionally string values. This stage shrinks B+ tree payloads, removes duplicate strings, and centralizes overflow management.
**Audience:** engine developers & coding agent

---

## 0) Goals & non-goals

**Goals**

* Provide an overflow heap for values that do not fit inline in Stage 6 node/edge catalogs.
* Deduplicate high-cardinality strings via `str <-> id` dictionaries with crash-safe updates.
* Keep handles small (`VRef`, `StrId`) so catalogs and indexes remain compact.

**Non-goals**

* Compression or encryption of overflow pages (future enhancement).
* Multi-writer MVCC semantics (arrive in Stage 10).
* Application-layer caches—bindings may add them, but core crates only expose metrics.

---

## 1) Var-Len store (`sombra-vstore`)

### 1.1 Value handle

```rust
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct VRef {
    pub start_page: PageId,  // first overflow page in the chain
    pub n_pages: u32,        // number of overflow pages
    pub len: u32,            // logical value length in bytes
    pub checksum: u32,       // CRC32 of the logical payload
}
```

* `PageId` comes from the pager (Stage 2/3).
* Fixed-size handles keep catalog rows small and make serialization trivial.

### 1.2 Overflow page payload layout

After the standard pager header, each overflow page stores:

| Off | Size | Field          | Notes                                   |
| --: | ---: | -------------- | --------------------------------------- |
|   0 |    8 | `next_page_id` | `0` terminates the chain                |
|   8 |    4 | `used_bytes`   | payload length written into `data`      |
|  12 |    4 | `reserved`     | future compression/encryption flags     |
|  16 |  N-16| `data`         | raw payload bytes                       |

* `data_capacity = page_size - stage1_header - 16`.
* `used_bytes` must never exceed `data_capacity`; zero the remainder on free for diagnostics.

### 1.3 Allocation & free-list strategy

* Compute `needed_pages = ceil(len / data_capacity)`.
* Prefer contiguous extents from the pager free-list; fall back to single-page allocations chained through `next_page_id`.
* Freeing walks the chain and releases contiguous runs back to the pager in one call.
* Pager maintains counters so tests can assert `pages_allocated - pages_freed == live_pages`.

### 1.4 I/O paths

* **write(tx, bytes)**: split into capacity-sized chunks, allocate pages, write header + chunk, compute CRC32C over the full logical value, return `VRef`.
* **read(tx, vref)**: follow the chain, append each chunk into a scratch buffer, verify CRC matches, return `Vec<u8>` (or stream into caller-owned buffer).
* **read_into(tx, vref, dst)**: reuse `dst` to avoid repeated allocations; clear and reserve based on `vref.len`.
* **update(tx, vref, new_bytes)**:
  * If `new_bytes.len()` ≤ `vref.n_pages * data_capacity`, rewrite in place, updating `used_bytes`, `len`, and `checksum`; zero trailing bytes on the final page.
  * Otherwise allocate a new chain, write, free the old chain, and update the caller’s `VRef`.
* **free(tx, vref)**: release each page in the chain; checksum mismatches encountered during free bubble up as corruption errors.

### 1.5 Checksums & corruption handling

* Use CRC32C (hardware-accelerated on x86/ARM). Store in `VRef.checksum`.
* Verification happens on every read and optional background scrubbing.
* Tests intentionally flip payload bits to ensure corruption is detected and reported with the offending `PageId`.

---

## 2) Dictionaries (`sombra-catalog::dict`)

### 2.1 Identifiers & trees

* **Identifier type:** `StrId(u32)`; encoded as big-endian when stored on disk. `0` is reserved.
* **Trees:**
  * `dict_str_to_id`: key = Stage 1 string key encoding (`[varuint len][utf8 bytes]`); value = raw `StrId` (4 bytes BE).
  * `dict_id_to_str`: key = `StrId` (BE `u32`); value = `StrVal` (`Inline` or `VRef`).

### 2.2 Value record layout (`dict_id_to_str`)

```
repr_tag: u8     // 0 = Inline, 1 = VRef
inline_len: u8   // only present when repr_tag == 0
inline_bytes: [] // UTF-8 payload when inline
vref: VRef       // when repr_tag == 1
```

* Default inline threshold: **60 bytes**; configurable via `DictOptions`.
* Validate that inline data is UTF-8 before storing.

### 2.3 Catalog metadata

* Persist `next_str_id: u32` in the catalog meta page (close to Stage 2 free-list metadata).
* Optionally store a `dict_epoch: u64` counter for binding caches.
* Record stats such as `inline_bytes` vs `vref_bytes` for observability.

### 2.4 Intern/resolve flow

* **intern(tx, s)**:
  1. Search `dict_str_to_id`; if found, return existing `StrId`.
  2. Else allocate new `StrId = next_str_id; next_str_id += 1`.
  3. Choose inline vs VRef storage; for VRef, call `VStore::write`.
  4. Insert `(s → id)` and `(id → value)` in one transaction; failure must roll back both sides.
* **resolve(tx, id)**:
  * Fetch `(id → value)`; decode inline bytes or dereference VRef.
  * Optional per-transaction cache keyed by `StrId` for repeated lookups.

### 2.5 Optional extensions

* String property **values** can use a parallel dictionary (`dict_propval_*`) if deduplication proves worthwhile.
* For binary blobs, consider a future `BlobId` dictionary sharing the same VStore.

---

## 3) Crate layout & public API

### 3.1 `sombra-vstore`

```rust
pub struct VStore {
    pager: Arc<PageStore>,
    page_size: u32,
    metrics: Arc<VStoreMetrics>,
}

impl VStore {
    pub fn open(pager: Arc<PageStore>) -> Result<Self>;
    pub fn write(&self, tx: &mut WriteTx, bytes: &[u8]) -> Result<VRef>;
    pub fn read(&self, tx: &ReadTx, r: VRef) -> Result<Vec<u8>>;
    pub fn read_into(&self, tx: &ReadTx, r: VRef, dst: &mut Vec<u8>) -> Result<()>;
    pub fn free(&self, tx: &mut WriteTx, r: VRef) -> Result<()>;
    pub fn update(&self, tx: &mut WriteTx, r: &mut VRef, new: &[u8]) -> Result<()>;
}
```

* Internally maintain helpers for chain traversal and extent allocation.
* Expose `metrics` to the higher layers (Stage 6 storage crate).

### 3.2 `sombra-catalog::dict`

```rust
pub struct Dict {
    s2i: BTree<StringKey, StrId>,
    i2s: BTree<StrIdKey, StrVal>,
    vstore: VStore,
    opts: DictOptions,
}

impl Dict {
    pub fn open(
        pager: Arc<PageStore>,
        s2i_tree: TreeId,
        i2s_tree: TreeId,
        opts: DictOptions,
    ) -> Result<Self>;

    pub fn intern(&self, tx: &mut WriteTx, s: &str) -> Result<StrId>;
    pub fn resolve(&self, tx: &ReadTx, id: StrId) -> Result<String>;
}
```

* `DictOptions` toggles inline threshold, checksum verification, and optional metrics callbacks.
* `StrVal` enum stores inline payload (`SmallVec<[u8; 64]>`) or `VRef`.

---

## 4) Integration with catalogs & properties

* Node/edge catalogs (Stage 6) store labels, types, and property keys as `StrId`.
* Property values remain inline until they exceed the configured `INLINE_THRESHOLD` (48–128 bytes). When exceeded, encode property entry as a `VRef`.
* Bindings may maintain caches (`StrId → String`) invalidated via `dict_epoch`.
* Meta page augments Stage 2/3 fields with `next_str_id`, `vstore_page_count`, and optional stats counters.

---

## 5) Invariants & validation

* `vref.n_pages * data_capacity >= vref.len`.
* Every page in a chain belongs to the same allocation owner; `next_page_id` is either another overflow page or `0`.
* CRC32C matches the concatenated payload; mismatch yields a `Corruption` error carrying `PageId`.
* Freeing a `VRef` releases exactly `n_pages`; double free triggers assertions in debug builds.
* `resolve(intern(s)) == s` for all UTF-8 inputs; `StrId` allocation is strictly monotonic.
* `dict_str_to_id` keys remain byte-wise sorted; `dict_id_to_str` keys follow numeric order.

---

## 6) Tests & acceptance

**VStore**

* Round-trip write/read for sizes: 1 byte, 4 KB, 8 KB (exact page), 64 KB, 1 MB.
* Update-in-place vs reallocate path; ensure old chains are freed.
* Inject corruption (flip bits mid-chain) and assert reads detect failure.

**Dictionaries**

* Intern ~100k unique strings, record IDs, and assert sequential allocation.
* Persistence test: close/open cycle, verify `resolve` still returns originals.
* Optional stress: multi-threaded reads with single writer to confirm determinism.

**Performance smoke**

* Measure sequential write/read throughput for 1 MB values (baseline for Stage 6).
* Measure latency around the inline threshold (e.g., 48–80 bytes).

**Acceptance**

* All invariants upheld across unit, property, and fuzz tests.
* Pager freelist counters reconcile after deleting all entries (no leaks).
* No unexpected allocations or checksum failures under randomized workloads.

---

## 7) Observability & tooling

* Counters: `vstore.pages_allocated`, `vstore.pages_freed`, `vstore.bytes_written`, `vstore.bytes_read`.
* Gauges: `vstore.live_pages`, `dict.inline_bytes`, `dict.vref_bytes`.
* Dictionary metrics: `dict.intern_calls`, `dict.cache_hits`, `dict.cache_misses` (when caches exist).
* Debug helper: `dump_vref(vref)` prints chain layout, sizes, and checksum state; compile behind `debug_assertions`.

---

## 8) Step-by-step checklist (coding agent)

* [ ] Implement overflow page helpers and integrate with pager allocation/free.
* [ ] Implement `VStore::{write, read, read_into, update, free}` with CRC verification.
* [ ] Wire up metrics for allocations, frees, and bytes moved.
* [ ] Build `Dict` on top of Stage 4 B+ trees with inline/VRef storage.
* [ ] Implement `intern`/`resolve`, persisting `next_str_id`.
* [ ] Add tests for VStore round-trips, reallocations, corruption, and dictionary bijection + persistence.
* [ ] Validate observability hooks via unit tests or smoke harness.

---

## 9) Looking ahead

* Stage 10 MVCC treats `VRef` as immutable; updates allocate new chains, allowing copy-on-write without breaking readers.
* Compression/encryption can flip bits in the overflow header while keeping the `VRef` format stable.
* String property value dictionaries can participate in global string GC once retention policies land.
