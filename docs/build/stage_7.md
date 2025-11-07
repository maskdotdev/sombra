# 📄 STAGE 7 — Label & Property Indexes

**Crates:** `sombra-index` (this stage), uses `sombra-btree`, `sombra-bytes`, `sombra-storage`, `sombra-vstore`
**Outcome:** fast scans by label and property predicates with intersection.

---

## 0) Components

1. **Label index** — `B+` tree keyed by `(label_id, node_id)` → `()`.
2. **Property index** — value→postings for `(label, property)` pairs. Two interchangeable backends behind a trait:

   * **Chunked postings** (default): delta‑encoded `node_id`s with skips (8 KiB chunks).
   * **BTree postings** (fallback): `B+` tree keyed `(label, prop, value, node_id)` → `()`; simpler and handles out‑of‑order inserts naturally. Useful for tests and edge cases.

---

## 1) Index Catalog & Definitions

* **Tree:** `index_catalog`
* **Record:** `(label_id, prop_id) → { index_kind: enum, type_tag: u8 }`
* `index_kind`: `0=Disabled`, `1=Chunked`, `2=BTree`
* Creation API validates `type_tag` (e.g., numeric/string/date). Strings use lexicographic order; NaNs disallowed.

**Public API**

```rust
pub enum IndexKind { Chunked, BTree }
pub struct IndexDef { pub label: LabelId, pub prop: PropId, pub kind: IndexKind, pub ty: TypeTag }

pub fn create_label_index(graph: &Graph, tx: &mut WriteTx, label: LabelId) -> Result<()>;
pub fn drop_label_index(graph: &Graph, tx: &mut WriteTx, label: LabelId) -> Result<()>;

pub fn create_property_index(graph: &Graph, tx: &mut WriteTx, def: IndexDef) -> Result<()>;
pub fn drop_property_index(graph: &Graph, tx: &mut WriteTx, label: LabelId, prop: PropId) -> Result<()>;
```

Indexes are **online**: inserts/updates maintain them inside the same txn as catalog changes.

---

## 2) Label Index

* **Key:** `(label_id: u32 BE, node_id: u64 BE)` → **Val:** `()`
* **Operations:**

  * On node create: insert one entry per label.
  * On node delete: remove entries.
  * On label set update: add/remove accordingly.
* **Scan:** range on `(label_id, node_id=[lo..hi])`.

---

## 3) Property Index — Chunked Postings (default)

### 3.1 Structures

* **Key (posting head/segment):**
  `(label_id: u32 BE, prop_id: u32 BE, value_key: bytes, segment_id: u32 BE)`
  → **Val** = `PostingSegment` (variable length; stored in page(s))

* **`value_key` encoding:** order‑preserving from Stage‑1:

  * `int64` → sign‑flipped + BE
  * `float64` → normalized sign + BE
  * `string` → `len(u32 BE) + bytes`
  * `date/datetime/bool` similar, with explicit type tags embedded at the front to prevent cross‑type mixing:

    * `value_key = [type_tag:u8 | encoded_value...]`

* **Segment payload format (8 KiB target):**

| Field              | Type/Encoding                                    |
| ------------------ | ------------------------------------------------ |
| `codec_version`    | `u8`                                             |
| `n`                | `u32` (#postings)                                |
| `base`             | `u64` (first node_id)                            |
| `delta_varints[n]` | varint deltas from previous id                   |
| `skip_stride`      | `u16` (e.g., 32)                                 |
| `skip_table`       | array of `(idx:u32, id:u64, off:u32)` for gallop |
| `tombstones?`      | optional bitmap or RLE                           |

* **Append policy:** grow current tail segment for a `(label,prop,value)` until near page full; then start a new `segment_id += 1`.

### 3.2 Edge cases & maintenance

* **Out‑of‑order inserts** (updating old nodes): place into a small **overflow segment** (segment_id = `u32::MAX` backwards). Query merges tail+overflow via two iterators. Periodic **repack** compacts overflow into main segments.
* **Deletes/updates:** mark tombstones in the segment; if density < threshold, trigger asynchronous **repack** (or build‑offline tool).

### 3.3 Streaming API

```rust
pub trait PostingStream {
    fn next_batch(&mut self, out: &mut Vec<NodeId>, max: usize) -> Result<bool>; // returns false when done
}

pub fn intersect_sorted<A: PostingStream, B: PostingStream>(a: A, b: B, out: &mut Vec<NodeId>) -> Result<()>;
pub fn intersect_k(streams: &mut [&mut dyn PostingStream], out: &mut Vec<NodeId>) -> Result<()>;
```

* `intersect_sorted` walks both inputs with a two-pointer merge.
* `intersect_k` advances the slowest streams towards the current max head, emitting matches on the fly so no intermediate vectors are materialised.

---

## 4) Property Index — BTree Postings (fallback)

* **Key:** `(label_id, prop_id, value_key, node_id)` → **Val:** `()`
* **Pros:** simpler mutation path, perfect order, easy deletes.
* **Cons:** more B+ tree overhead vs chunked segments.
* Behind `IndexKind::BTree`; same query API (adapter yields `PostingStream` over key range).

---

## 5) Query operations exposed by Stage 7

* `label_scan(label_id) -> PostingStream` (actually a simple range over label index)
* `prop_eq(label_id, prop_id, value) -> PostingStream`
* `prop_range(label_id, prop_id, lo..hi) -> PostingStream` (inclusive/exclusive bounds)
* `intersect(streams…) -> NodeId set`
* **Join with adjacency** handled in Stage 8’s query pipeline.

---

## 6) Integration with Stage 6 CRUD

* On **node create/update/delete**, `sombra-storage` calls into `sombra-index`:

  * For each **label**: update label index.
  * For each **indexed property** per label:

    * If **new value** exists: insert posting (append or BTree key).
    * If **old value** existed: delete posting (tombstone or key delete).
* These operations occur inside the same **write txn** to ensure atomicity.

---

## 7) Tests & Acceptance

**Correctness**

* Build index for random nodes/props; validate:

  * `prop_eq` and `prop_range` return exactly the set found by scanning `nodes`.
  * Label scans match set of nodes carrying the label.
  * Intersections equal set intersections produced by Rust `BTreeSet`.

**Performance**

* Compare `prop_eq` vs full scan on datasets with selectivity 0.1%, 1%, 10% (expect strong win).
* Intersection speed: two streams of 1M ids each with 10% overlap; ensure skip/gallop outperforms linear.

**Durability**

* Fault injection during segment append; recover & verify consistent last valid prefix.

**Acceptance (Stage 7)**

* Equality & range filters return correct sets.
* Intersections faster than scanning at selectivity ≤ 10%.
* Repack/overflow mechanics maintain correctness after churn.

---

## 8) Step‑by‑Step Checklist (coding agent)

* [ ] Implement label index B+ tree, integrate with node CRUD.
* [ ] Define `value_key` encoding (with type tag).
* [ ] Implement `PostingSegment` codec + skip table; stream reader/writer.
* [ ] Implement chunked postings tree & append logic; overflow segments.
* [ ] Implement BTree postings fallback; adapter to `PostingStream`.
* [ ] Implement `prop_eq`, `prop_range`, `intersect_sorted`.
* [ ] Wire to Stage 6 mutations.
* [ ] Tests: correctness vs scans; performance microbenches; crash/recovery on partial segments.

---
