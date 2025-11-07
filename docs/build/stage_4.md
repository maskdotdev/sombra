# 📄 STAGE 4 — Generic B+ Tree (KV on PageStore)

**Outcome:** a reusable, ordered **B+ tree** supporting point lookup, range scan, insert, delete, split/merge, and stable iterators. Keys use **order‑preserving encodings** from Stage 1. The tree is built on Stage 3 `PageStore` (pages are versioned by WAL at commit).

---

## 0) Design principles

* **B+ tree** (all values in leaves; internal nodes hold separators/fence keys).
* **Variable‑length records** with **slot directory** per page.
* **Prefix compression** of keys within a leaf page to increase fan‑out.
* **Single writer** (no latch coupling required in Stage 4).
* **Stable cursors** across page boundaries and splits (within a read txn snapshot).

---

## 1) Crate & public API

**Crate:** `sombra-btree`

```rust
pub trait KeyCodec: Sized {
    fn encode_key(&self, k: &Self, out: &mut Vec<u8>);
    fn compare_encoded(a: &[u8], b: &[u8]) -> std::cmp::Ordering; // lexicographic
    fn decode_key(bytes: &[u8]) -> Result<Self>;
}

pub trait ValCodec: Sized {
    fn encode_val(&self, v: &Self, out: &mut Vec<u8>);
    fn decode_val(&self, src: &[u8]) -> Self;
}

pub struct BTree<K: KeyCodec, V: ValCodec> {
    root: PageId,
    // references to PageStore, options, stats...
}

impl<K: KeyCodec, V: ValCodec> BTree<K, V> {
    pub fn open_or_create(ps: &Arc<dyn PageStore>, opts: BTreeOptions) -> Result<Self>;
    pub fn get(&self, tx: &ReadTx, key: &K) -> Result<Option<V>>;
    pub fn put(&self, tx: &mut WriteTx, key: &K, val: &V) -> Result<()>;
    pub fn delete(&self, tx: &mut WriteTx, key: &K) -> Result<bool>;
    pub fn range<'a>(&'a self, tx: &'a ReadTx, lo: Bound<K>, hi: Bound<K>)
       -> Result<Cursor<'a, K, V>>;
}
```

**Options:**

* `page_fill_target: u8` (e.g., 85% for leaves)
* `internal_min_fill: u8` (e.g., 40%)
* `prefix_compress: bool`
* `checksum_verify_on_read: bool` (delegated to pager)

---

## 2) Page layouts

**Common header within page payload (after Stage‑1 header):**

| Off | Size | Field          | Notes                       |
| --: | ---: | -------------- | --------------------------- |
|   0 |    1 | kind           | 1=leaf, 2=internal          |
|   1 |    1 | flags          | reserved                    |
|   2 |    2 | nslots         | number of entries           |
|   4 |    2 | free_start     | start of free space         |
|   6 |    2 | free_end       | end of free space (grows ↓) |
|   8 |    8 | parent_page    | 0 for root                  |
|  16 |    8 | right_sibling  | 0 if last (B‑link ptr)      |
|  24 |    8 | left_sibling   | 0 if first                  |
|  32 |    8 | low_fence_len  | length of low fence key     |
|  40 |    8 | high_fence_len | length of high fence key    |
|  48 |    ? | fence keys …   | serialized fence keys       |
|   … |    … | slot directory | `nslots * 2` bytes at tail  |
|   … |    … | var records    | in the middle (grow ↑)      |

**Slot directory:** array of `u16` offsets (from page start) to each record.

**Leaf record format:**

* `[prefix_len:u16][key_suffix_len:u16][key_suffix bytes][val bytes]`
  Prefix is relative to **previous key** (or fence key); enable/disable via option.

**Internal record format:**

* `[sep_key_len:u16][sep_key bytes][child_page_id:u64]`
  Internal node fan‑out targets ~100–200 children (depends on page size & key lengths).

---

## 3) Algorithms

### 3.1 Search (point)

* Descend from root:

  * In internal pages, binary search on separator keys.
  * In leaves, binary search with prefix decode to locate the key.

### 3.2 Insert

1. Find target leaf.
2. If enough free space → insert record (apply prefix compression).
3. Else **split**:

   * Allocate new right sibling page.
   * Redistribute records ~50/50 (keep sort order).
   * Set B‑link pointers (`left`/`right`).
   * Promote **separator key** (the first key of the right page) to parent.
   * If parent full, split up recursively; if root splits, create new root.

### 3.3 Delete

1. Find leaf; if key exists, remove slot and reclaim space.
2. If utilization < `internal_min_fill` and not root:

   * Try **borrow** from left/right sibling (prefer borrow over merge).
   * Else **merge** with sibling and delete separator from parent; cascade upward if needed.
3. Update fence keys and possibly parent separator keys.

### 3.4 Range scan / iterators

* Start at first key ≥ `lo`.
* **Cursor** holds `(page_id, slot_index, generation)` and a small decode buffer for prefix recovery.
* On reaching end of page, follow `right_sibling`.
* Cursors are valid across splits because of B‑link right pointers (they may skip to the correct sibling if the current page’s upper bound moved).

---

## 4) Concurrency (Stage 4)

* **Single writer**: during mutation, pages are pinned; no read latches.
* Readers use the snapshot at `last_checkpoint_lsn` (Stage 3); trees are consistent due to copy‑of‑page in WAL at commit.
* B‑link pointers ensure range scans won’t loop or miss keys during concurrent splits by the writer.

---

## 5) Invariants

* Internal pages: `low_fence < sep_0 ≤ child_0 ≤ sep_1 ≤ child_1 … < high_fence`.
* Leaves: keys strictly increasing.
* Page free space: `free_start ≤ free_end`.
* Utilization after insert/split: both pages ≥ ~50% (target fill).
* Root special‑cases: may be leaf; if internal and becomes empty on merge, collapse height.

---

## 6) Error handling

* Detect and report **corruption** if binary search encounters out‑of‑order slots.
* Slot directory bounds checking (no offset overlaps, offsets ≥ header size).
* Page kind must match expected (`leaf` vs `internal`).

---

## 7) Tests & acceptance

**Unit tests**

* Put/Get/Delete on small key spaces (integers, short strings).
* Splits at boundaries (key at page end, smallest/largest key).
* Merge/borrow correctness with random operations.
* Cursor across splits and merges (never miss/duplicate keys).

**Property tests**

* Mirror against `BTreeMap` for random op sequences; after each op, compare full contents.

**Fuzz**

* Fuzz decode logic for prefix compression and record parsing.

**Performance smoke**

* Measure insert throughput for 1M sequential and 1M random keys; range scan MB/s.

**Acceptance (Stage 4)**

* No mismatches vs `BTreeMap` on 100k‑op randomized workloads (multiple seeds).
* No panics; corruption errors only when pages are intentionally damaged in tests.
* Throughput meets your internal baseline (document target).

---

## 8) Step‑by‑step checklist (coding agent)

* [ ] Implement page payload headers and slot directory helpers.
* [ ] Implement key prefix compression encode/decode.
* [ ] Implement search in internal + leaf pages.
* [ ] Implement insert with split, separator promotion, root split.
* [ ] Implement delete with borrow/merge and root collapse.
* [ ] Implement cursor for forward iteration (and optionally reverse).
* [ ] Tests: unit, property against `BTreeMap`, split/merge edge cases.
* [ ] Hooks for `tracing` and stats (node visits, splits, merges).

---
