
## Targets & guardrails

**Primary goals (on the current benchmark set):**

* **Random/sequential reads:** close the ~20× gap to within **2–4× of SQLite**.
* **Inserts (10k, no splits):** close the ~150× gap to within **3–6× of SQLite**.
* **Mixed workload:** reduce commit cost so mixed loop overhead is dominated by reads, not fsyncs.

**Non-goals for this round:** secondary indexes, complex collations, vacuuming strategies beyond basic compaction. (We’ll leave these for a “v2 perf” track.)

**Engineering guardrails:**

* No loss of crash safety under the current durability mode semantics.
* No format changes without a version-gated migration strategy (backwards read-compat or block with clear error).

---

## Phase 0 — Baseline & Observability (before any changes)

**Why:** Make the perf work measurable and repeatable.

**Work items**

1. **Benchmark profile snapshots**

   * Run `cargo run --release --bin compare-bench` on a fixed dataset/seed.
   * Record: ops/sec, p95 latency for each section, fsync count, bytes written to WAL, page reads.
2. **Instrumentation**

   * Add counters + histograms (feature-gated `metrics` or custom):

     * `btree.leaf.key_decodes`, `btree.leaf.key_cmps`, `btree.leaf.memcopies_bytes`
     * `pager.wal.frames`, `pager.wal.bytes`, `pager.fsync.count`, `pager.commit.ms`
   * Scope timers around `search_leaf_bytes`, `insert_into_leaf`, `pager.commit`.
3. **Flamegraph**

   * Capture perf/flamegraph on (a) random finds, (b) 10k inserts, (c) mixed.
4. **Benchmark modes (small code change, no behavior change)**

   * Parameterize compare-bench runs:

     * `--mode reads-only`, `--mode inserts-only`, `--mode mixed`
     * `--commit-every N` (default 1 for current behavior)
     * `--tx-mode read-with-write` (for later A/B)

**Acceptance criteria**

* A single “baseline.md” with numbers and graphs checked into `bench/` alongside a `perf.data` or SVG flamegraph.
* CI job runs the microbench and publishes metrics artifacts.

---

## Phase 1 — **Leaf binary search without full key reconstruction**

> Goal: Replace O(n) linear scans + heap allocations with O(log n) binary search over the slot directory using borrowed bytes.

### 1A. Introduce a **BorrowedKey** view and streaming comparator

* **Type:** `struct KeyCursor<'a> { buf: &'a [u8], pos: usize }`
* **APIs:**

  ```rust
  impl<'a> KeyCursor<'a> {
      fn new(buf: &'a [u8]) -> Self;
      fn read_varint_len(&mut self) -> usize; // minimal decode
      fn slice(&mut self, n: usize) -> &'a [u8]; // borrow
  }
  pub trait KeyComparator {
      fn compare(a: &[u8], b: &[u8]) -> Ordering; // default memcmp
  }
  ```
* **Change:** Rewrite comparisons to operate on **borrowed slices** from the page; do not `Vec::from`.

### 1B. **Binary-search** the slot directory

* Keep slot directory sorted by key (current invariant).
* Implement:

  ```rust
  fn search_leaf_bytes(target_key: &[u8]) -> Result<SearchResult, Error> {
      // binary search over slot offsets; for each mid:
      //   decode minimal header (varint key_len), borrow `key_bytes`,
      //   compare via KeyComparator::compare
  }
  ```
* Optimize **prefix decoding** only as needed: read `key_len` and the first `min(target.len, key_len)` bytes.

### 1C. Optional micro-optimization: **first-byte table**

* Maintain a 256-entry count/histogram for first key byte per page to skip ranges quickly.
* Only if measurements show >5–8% wins; otherwise defer.

**Tests**

* Unit tests for `KeyCursor` (boundary conditions, malformed varints).
* Property tests (proptest) with random keys: ensure search returns the same result as the old linear scan.
* Fuzz: arbitrary byte pages flagged as invalid must not panic.

**Acceptance criteria**

* **Allocations during lookup** drop ~to zero (except when returning value).
* **Key decodes per get** ~O(log n) (from ~150 to ~8 for 4 KiB pages).
* Bench: random/sequential read time improves ~10–25× (gap to SQLite ≤ 2–4×).

**Risks & mitigations**

* **Comparator semantics** (collation): keep raw byte compare for now; document.
* **Slot sort invariant:** add debug assertion post-insert.

---

## Phase 2 — **In-place insert/delete on slotted pages (memmove)**

> Goal: Stop re-materializing the entire leaf. Shift bytes in-page and update slot directory.

### 2A. Lock down/extend the **page format**

* **Header (example):**

  ```
  u16 n_slots
  u16 free_start   // grows upwards
  u16 free_end     // grows downwards
  u16 reserved     // future flags; bit for prefix-compression later
  [slot_dir: n_slots * u16 offsets] // sorted by key
  [payload region between free_start and free_end]
  ```
* **Record layout:** `varint key_len | varint val_len | key_bytes | val_bytes`
* **Invariant:** `free_start <= free_end`. Fragmentation tolerated; compaction routine provided.

> If this differs from today’s layout, bump **file format minor** and gate reading with a version check. For now keep it compatible if possible.

### 2B. Core operations

* `insert_record(leaf, key, value)`:

  1. **Search position** via Phase 1 binary search.
  2. **Capacity check:** `needed = header + varint(key_len) + varint(val_len) + key_len + val_len`.

     * If `needed` > `free_contiguous`, run **page_compact(leaf)`** (memmove live records downward, rewrite slot offsets) else proceed.
  3. **Make room:** decrement `free_end` by `needed`, write record at `free_end`.
  4. **Shift slot_dir:** `memmove(&mut slots[idx+1..], &slots[idx..])`, write new offset at `slots[idx]`.
  5. Update `n_slots`, `free_start` if compaction occurred, checksums if any.
* `delete_record(leaf, slot_idx)`:

  * Remove slot; **lazy** reclaim: do not fill hole in payload; just leave a tomb (space reclaimed by next compaction).
* `update_record(...)`:

  * If new size fits, in-place; else delete+insert.

**Implementation notes**

* Use `ptr::copy` / `copy_nonoverlapping` for memmoves.
* Encapsulate all unsafe into `page_ops.rs` with invariants documented.
* Avoid temporary `Vec` for record rewrite.

### 2C. Splits/merges unchanged (but cheaper)

* During split, **do not decode/re-encode all records**; move a contiguous byte range and adjust slot offsets for each side.

**Tests**

* Insert/delete/update property tests comparing page state with a simple model.
* Fragmentation stress: many churn operations then compaction; verify all records retrievable and ordered.
* Crash-safety (see Phase 4 testing): page images with holes must still be readable.

**Acceptance criteria**

* Insert microbench shows **O(log n)** leaf touches and **O(record + shifted_slot_dir)** bytes copied.
* 10k insert test improves by **10–100×** (target gap ≤ 3–6× vs SQLite).
* Allocation count during inserts ~0 aside from comparator scratch.

**Risks & mitigations**

* **Fragmentation:** compaction policy can be “if free_contiguous < needed and (free_total ≥ needed) then compact”.
* **Large values:** define threshold for overflow pages later; for now ensure correctness.

---

## Phase 3 — **Cheaper commits for short transactions**

> Goal: Reduce per-commit cost to microseconds territory or avoid commits in the mixed loop.

We’ll pursue **two tracks**; you can land either or both.

### 3A. **Batch WAL frame serialization + deferred fsync**

* **Change:** Within a transaction, gather dirty pages in a contiguous staging buffer and issue a **single writev** (or sequential writes) at commit. Compute checksums once, sequentially.
* **fsync policy:**

  * Add a durability knob (`Synchronous::Full|Normal|Off` exists already). For `Full`, keep fsync at commit but **coalesce to one fsync** per commit; never multiple.
  * Add **group commit window** (e.g., coalesce fsyncs that arrive while one is in flight). Implementation without background threads: a spin-wait of a fixed number of microseconds is not acceptable; instead keep it simple initially—single fsync, nothing else.
* **Avoid copies:** Do not “copy every dirty page into a new buffer” if page frames are already contiguous; otherwise, pre-size one buffer and serialize directly into it.

**Acceptance criteria**

* In mixed benchmark with commit per loop, `pager.commit` time drops by **>5×** and fsync count == loop count (1 per commit), not >1.
* WAL bytes written per commit equals `dirty_pages * page_size + headers` (no extra copies).

### 3B. **Read-through-write (avoid commits in the loop)**

* Expose/standardize `tree.get_with_write` and update the mixed benchmark to use it when measuring read-dominant workloads that don’t require durability after each iteration.
* **Correctness:** reads under the write guard see uncommitted changes; this is acceptable for the benchmark mode but document the semantics clearly.

**Acceptance criteria**

* Mixed workload where reads interleave writes **without** committing should perform within **2–4×** of the “reads-only” mode after Phase 1, since commits are out of the hot path.

**Risks**

* **Durability semantics:** Document precisely; do not change defaults.

---

## Phase 4 — Validation, Safety & Tooling

### 4A. Crash-safety harness (WAL)

* Fault injection: at deterministic points in `pager.commit`, simulate a crash (truncate WAL after N frames, drop after header write, etc.), then attempt recovery.
* Property: after recovery the database is in a valid state and contains either pre- or post-commit state, never a hybrid.

### 4B. Corpus & fuzzing

* AFL/libFuzzer target on page ops: mutate bytes, ensure read path either rejects cleanly or returns consistent state.

### 4C. CI gates

* Run microbench and fail the PR if **regression > 10%** on any section from the last main baseline.
* Run sanitizer configs on page ops (`-Zsanitizer=address` / Miri where feasible).

---

## Optional “Phase 5” accelerators (do **after** core wins land)

These are add-ons that can produce additional gains but add complexity:

1. **Short-key cache in slot directory**

   * Store a fixed-length prefix (e.g., first 8 bytes + key_len varint) next to each slot offset for faster binary search with fewer payload touches.

2. **Prefix compression per leaf**

   * Group records into “restart blocks” (LevelDB-style): `shared_prefix_len | unshared_key_len | value_len | unshared_key | value`.
   * Keeps on-page binary search workable if restart points are frequent enough. (Requires a page format bump.)

3. **Overflow pages for large values**

   * Prevents single records from blowing up memmove costs and split logic.

4. **CRC acceleration**

   * Use hardware CRC (e.g., `crc32c` intrinsic) for WAL frame checksums.

---

## Detailed work breakdown (PR-sized chunks)

### PR1 — KeyCursor + binary search

* Introduce `KeyCursor`, rewrite `search_leaf_bytes` to borrowed slices.
* Bench wins recorded; no format change.

### PR2 — Page ops module

* Encapsulate unsafe memmove utilities and slot-dir manipulation in `page_ops.rs`.
* Add tests & property tests.

### PR3 — In-place insert/delete (leaf)

* Replace `insert_into_leaf` materialize/rebuild path with in-place ops.
* Implement compaction.
* Ensure split path moves byte ranges.

### PR4 — WAL serialization batching

* Serialize N dirty pages into a contiguous buffer; single write (or writev).
* One fsync per commit.

### PR5 — Mixed benchmark: read-through-write mode

* Add mode to compare_bench and document.

*(You can interleave PR3 & PR4 depending on who picks them up.)*

---

## Bench methodology & success checklist

**Run matrix**

* Page sizes: 4 KiB (current), 8 KiB (optional A/B).
* Data shapes: fixed-size keys/values vs variable-size.
* Modes: reads-only, inserts-only (no splits), mixed (commit every 1, 10, 100), mixed with read-through-write.

**Report (to update after each PR)**

* Ops/sec & p50/p95 for each test.
* Allocation counts (e.g., via `dhat` or custom counter).
* `pager.commit.ms` distribution (p50/p95/p99).
* `fsync.count`, `wal.bytes`, `memcopies.bytes`.

**Stop conditions**

* If a change improves inserts but regresses reads ≥10%, revisit before merging (most common cause: comparator or slot-dir invariant bugs).
* If fsync optimization changes crash-safety semantics, keep it behind a feature flag until proven safe by crash harness.

---

## Implementation sketches

**Binary search (conceptual):**

```rust
fn cmp_at_slot(page: &Page, slot_idx: usize, target: &[u8]) -> Ordering {
    let off = page.slot_offset(slot_idx);
    let mut cur = KeyCursor::new(page.bytes_at(off));
    let klen = cur.read_varint_len();
    let vlen = cur.peek_varint_len_skip_only(); // or read and advance; value unused here
    let key = cur.slice(klen.min(target.len())); // borrow only what we need to compare
    match key.cmp(&target[..key.len()]) {
        Ordering::Equal if klen == target.len() => Ordering::Equal,
        Ordering::Equal => klen.cmp(&target.len()),
        other => other,
    }
}
```

**Insert in place (core):**

```rust
fn insert_record(page: &mut Page, idx: usize, key: &[u8], val: &[u8]) -> Result<()> {
    let rec_sz = varint_len(key.len()) + varint_len(val.len()) + key.len() + val.len();
    if page.free_contig() < rec_sz && page.free_total() >= rec_sz {
        page.compact()?; // memmove live region downward; rewrite slot offsets
    }
    ensure!(page.free_contig() >= rec_sz, Error::PageFull);

    let write_off = page.free_end() - rec_sz;
    page.write_record_at(write_off, key, val); // encode varints + bytes
    page.insert_slot(idx, write_off);          // memmove slot_dir, bump n_slots
    Ok(())
}
```

**WAL batching outline:**

```rust
fn commit(tx: &mut Tx) -> Result<()> {
    // collect dirty pages in page order
    let pages = tx.dirty_pages_sorted();
    // pre-size buffer to sum(serialized_frame_len(p))
    let mut buf = Vec::with_capacity(estimated_total);
    for p in pages { serialize_frame_into(&mut buf, p); }
    wal_write_all(&buf)?; // ideally single write; fall back to writev where supported
    wal_fsync()?;         // exactly one fsync for Synchronous::Full
    mark_commit();        // commit marker / index update
    Ok(())
}
```

---

## Risks & mitigations (summary)

* **Format changes:** Version gating and a one-time migration path (read old → rewrite page with new slot-dir invariant during vacuum or on first write).
* **Unsafe memmoves:** Encapsulate and thoroughly test `page_ops` with Miri/ASan.
* **Comparator surprises:** Today we assume binary collation; document and keep the trait pluggable for future collations.
* **Mixed-mode semantics:** Clearly document that `get_with_write` yields read-your-writes but not durability unless committed.

---

## What you’ll likely see after each phase

* **Phase 1:** Random/sequential reads become competitive (within ~2–4× of SQLite), allocations plummet; flamegraph shifts away from key decode.
* **Phase 2:** Inserts speed up dramatically; flamegraph shows time in `ptr::copy` instead of re-encoding the entire leaf.
* **Phase 3:** Mixed workload no longer dominated by `pager.commit`; either commits are fewer (read-through-write) or each commit is much cheaper.

---

## Concrete next actions (you can assign today)

1. **PR1**: Land `KeyCursor` + replace `search_leaf_bytes` with binary search over borrowed slices; wire metrics.
2. **PR2**: Create `page_ops.rs` with safe wrappers for slot-dir shifts and contiguous region writes + exhaustive tests.
3. **PR3**: Rewrite `insert_into_leaf` to in-place + compaction; keep split logic byte-range based.
4. **PR4**: Batch WAL serialization, single fsync per commit; add counters for `wal.frames`/`wal.bytes`/`fsync.count`.
5. **PR5**: Add compare-bench modes (`--commit-every`, `--tx-mode read-with-write`) and update the README with the measurement protocol.
6. **Crash harness**: Add a fault-injection test suite for commit steps (serialize frames, write, fsync, commit mark), then turn it on in CI (nightly job is fine).

If you want, I can turn this into task issues with checklists and initial skeleton patches for PR1/PR2.
