# 📄 STAGE 10 — MVCC Prototype (Feature: `mvcc`) - NOT MVP

**Crates:**

* `sombra-concurrency` (new impl: `MvccCow`)
* `sombra-pager` (version‑aware reads)
* `sombra-wal` (reused as **MVCC log** with visibility records) or new `sombra-mvl`
* uses: everything above

**Outcome:** **multi‑writer** prototype using **copy‑on‑write pages** with version visibility by `LSN`. Readers pin an **epoch** (snapshot LSN) and read the newest page version ≤ their snapshot. Writers commit in parallel when touching **disjoint pages**; conflicts at page granularity abort.

---

## 0) Goals & Non‑Goals

**Goals**

* Multiple concurrent writers with page‑level conflict detection.
* Append‑only version log; readers never block writers.
* Epoch manager + GC of old versions.
* Keep the existing **traits stable** (`TxnManager`, `PageStore`).

**Non‑Goals**

* Key‑range conflict detection or predicate locking (page granularity only, for now).
* Distributed transactions or 2PC.
* Full index vacuuming policies (only GC by page version).

---

## 1) Files & structures

### 1.1 Version Log (MVL)

Either reuse WAL with small additions or add `graph.sombra-mvl`. We’ll describe as **MVL**:

**MVL header** (same as WAL header with a different magic, e.g., `b"SOMV"`).
**MVL frame** = WAL frame + an extra **vis** flag in a small **txn commit record**:

* Append **all page frames** with `frame_lsn = commit_lsn` (payload = full page image).
* After frames, append **Commit Record**:

  * `{commit_lsn, n_pages, page_ids[ ], checksum}`
  * Marks visibility for all frames with that `commit_lsn`.

On recovery, only frames whose `commit_lsn` has a **Commit Record** are visible.

### 1.2 Versioned Page Table (VPT)

In‑memory structure built at open and maintained online:

```
VPT: HashMap<PageId, Vec<(Lsn, file_offset)>> sorted by Lsn ascending
LastWriter: HashMap<PageId, Lsn> // for conflict detection
```

* On startup, scan MVL; for each committed txn, push `(lsn, off)` entries.
* Optionally persist a **checkpointed VPT** into a B+ tree at checkpoints, so warm starts avoid full scans.

---

## 2) Txn lifecycle (MvccCow)

```rust
pub trait TxnManager {
    type R<'a>: ReadTxn;
    type W<'a>: WriteTxn;

    fn begin_read(&self)  -> Result<Self::R<'_>>;
    fn begin_write(&self) -> Result<Self::W<'_>>;
}
```

**Begin read**

* Assign `snapshot_lsn = durable_lsn()` (or latest committed LSN).
* Acquire **EpochGuard** (increments reader epoch counter).
* Reads call `read_page_at(page_id, snapshot_lsn)`.

**Begin write**

* Allocate temporary `txn_id`, capture `begin_lsn = current_lsn()` for conflict checks.
* Maintain a **private dirty map**: `HashMap<PageId, Vec<u8>>` (new page images).
* On first write to page `p`, record `read_version[p] = LastWriter[p]` (or 0).

**Read for write (RFW)**

* To modify page `p`, read **visible version** at `snapshot_lsn` into the txn’s buffer; apply mutations → new image buffered.

**Commit**

1. Acquire **commit slot** to assign `commit_lsn = fetch_add(1)`. (Short critical section.)
2. **Validate**: for each page `p` in dirty set, check `LastWriter[p] <= read_version[p]`:

   * If `LastWriter[p] > read_version[p]` → someone committed a newer version since we started → **conflict → abort**.
3. **Append** all dirty page frames with `frame_lsn = commit_lsn` to MVL (parallel write OK).
4. Append **Commit Record** `{commit_lsn, n_pages, page_ids...}` and **fsync** depending on `Synchronous` mode.
5. Update `LastWriter[p] = commit_lsn` for all pages (protected by a lightweight lock or atomic CAS loop).
6. Release write txn.

**Abort**

* Drop dirty buffers; nothing appended to MVL.

**Readers**

* `read_page_at(id, lsn)`:

  * Look up `VPT[id]`, binary search last `(≤ lsn)`. If found, `pread` from MVL offset. Else `pread` from **base DB file**.
  * Buffer into cache; standard page checks apply.

---

## 3) Checkpoint & GC

### 3.1 Checkpoint

* Choose a **stable_lsn** = min(snapshot_lsn of active readers).
* For each page with versions `≤ stable_lsn` and newer than base:

  * Apply **latest ≤ stable_lsn** version into base DB at `page_id * page_size`.
* Update meta: `last_checkpoint_lsn = stable_lsn`.
* Truncate or recycle MVL frames `≤ stable_lsn`.
* Persist a compact **VPT checkpoint** (optional): a B+ tree mapping `PageId → (lsn, file_offset)` for the **latest** version > base (helps warm start).

### 3.2 GC of old versions

* Versions with `lsn < stable_lsn` can be reclaimed (removed as part of truncation).
* Keep a **small tail** for debugging (configurable N recent commits).

---

## 4) Conflict detection strategies

* **Stage 10 (prototype): page‑level**:

  * Maintains `LastWriter[page]`.
  * Writers record per‑page `read_version` when first touching the page.
  * Validate `LastWriter[page] == read_version[page]` at commit; otherwise abort.

* **Next step (optional): key‑range coarse tracking**:

  * For B+ tree pages, track `(tree_id, page_id)`; for hot spots, split pages earlier to reduce conflicts.
  * Future: install **intent metadata** on internal nodes to widen write set across splits.

---

## 5) PageStore & read path changes

Add **version‑aware read** in pager layer under `mvcc` feature:

```rust
pub trait VersionedReads {
    fn read_page_at(&self, id: PageId, lsn: Lsn) -> Result<PageRef<'_>>;
}
```

* `PageStore` uses `VPT` to locate newest ≤ `lsn`.
* Cache key becomes `(page_id, lsn_bucket)` to avoid mixing snapshots; simple strategy is to cache latest committed image and re‑validate against requests.

---

## 6) Locks & writer coordination

* **No global writer lock.**
* A small **commit counter lock** assigns `commit_lsn`.
* Updates to `LastWriter[p]` are via sharded mutexes or atomics (e.g., `DashMap<PageId, AtomicU64>` or shard by `page_id % S` to keep contention low).
* Commit publishing is **append‑only**; multiple writers can append concurrently using an atomic file offset allocator (e.g., `fetch_add` with `pwrite`).

---

## 7) Recovery

On open:

1. Read meta from base DB (for page size, salts, `last_checkpoint_lsn`).
2. Scan MVL:

   * Validate header; iterate frames; collect offsets per `commit_lsn`.
   * Apply only frames belonging to **committed** LSNs (those with Commit Record).
   * Populate `VPT` and `LastWriter` from latest LSN per page.
3. Readers can now read at latest committed LSN; writers begin at `current_lsn = max_committed + 1`.

---

## 8) Observability

* `mvcc.current_lsn`, `mvcc.active_readers`, `mvcc.active_writers`
* `mvcc.commit_bytes`, `mvcc.frames`, `mvcc.conflicts` (counter + reasons)
* `mvcc.gc_versions_removed`, `mvcc.gc_last_stable_lsn`
* Expose via `PRAGMA stats` and a new `PRAGMA mvcc`.

---

## 9) Tests & Acceptance

**Correctness**

* Two writers on **disjoint pages** both commit successfully; results visible to subsequent readers.
* Two writers on **same page**: one commits, the other **aborts** with conflict.
* Readers started before commit see **old** data; after commit (new read tx) see new data.
* Checkpoint keeps behavior: after checkpoint to `stable_lsn`, MVL truncated ≤ `stable_lsn`.

**Crash safety**

* Kill between appending frames and commit record: frames are **not visible** on recovery (no commit record).
* Kill after commit record but before `LastWriter` in‑memory update: recovery rebuilds `LastWriter` from MVL.

**Performance**

* Measure commit latency under 1, 2, 4 concurrent writers with random non‑overlapping key ranges; demonstrate >1 writer throughput scaling (even if commit record serialization exists).
* Read amplification: ensure `read_page_at` adds ≤ 1 extra seek vs baseline on warm cache.

**Acceptance (Stage 10)**

* “Disjoint writers commit independently” test passes repeatedly (hundreds of runs).
* “Conflict abort” test deterministically aborts loser; no torn data.
* Readers see consistent snapshots at selected LSN.
* GC reclaims old versions without impacting active readers.

---

## 10) Hooks you should already have (and how they’re used)

1. **Stable traits (`PageStore`, `TxnManager`)**

   * `MvccCow` plugs into `TxnManager` without changing storage/query code.
2. **Commit LSN everywhere**

   * B+ tree page mutations stamped with LSN; VPT indexes by `PageId, Lsn`.
3. **Meta root indirection**

   * Root table (array) in meta; MVCC publishes root changes via commit record; checkpoint folds latest root ≤ `stable_lsn` back into meta.
4. **Order‑preserving key encodings**

   * Enables range partitioning later if you shard trees (reduce conflicts).
5. **No global singletons**

   * `Db` holds `Arc`s to managers; swapping `SingleWriter` → `MvccCow` is a constructor choice.
6. **Epoch plumbing**

   * Read txns hold `EpochGuard`; `stable_lsn = min_epoch_lsn()` drives GC; MVCC simply implements the real epoch manager where Stage 3 used a no‑op.

---

## 11) Step‑by‑Step Checklist (coding agent)

**A. MVL & VPT**

* [ ] Implement `sombra-mvl` (or extend `sombra-wal`) with **commit record**.
* [ ] Implement `VPT` (in‑memory) + builder from MVL scan on open.
* [ ] Implement append allocator for MVL (atomic offset).

**B. TxnManager::MvccCow**

* [ ] Read tx: `snapshot_lsn`, `EpochGuard`, versioned reads.
* [ ] Write tx: dirty map, `read_version` capture on first touch.
* [ ] Commit: assign `commit_lsn`, validate, append frames, append commit, fsync per mode, publish `LastWriter` updates.
* [ ] Abort: drop dirty set.

**C. Pager integration**

* [ ] `read_page_at(id, lsn)` path; cache policy; checksums.

**D. Checkpoint & GC**

* [ ] Compute `stable_lsn` from epochs; fold versions ≤ stable to base; truncate MVL.
* [ ] Optional: persist compact VPT checkpoint state.

**E. Tests**

* [ ] Disjoint vs overlapping writer scenarios.
* [ ] Crash windows around commit and checkpoint.
* [ ] Reader snapshot consistency.

**F. Observability & CLI**

* [ ] `PRAGMA mvcc` to expose counters.
* [ ] `sombra checkpoint` honors MVCC path.

---

## 12) Migration & Feature Flagging

* Build `sombra` with `--features mvcc` to enable MVCC; default remains single‑writer.
* On a DB created without MVCC, enabling MVCC creates `graph.sombra-mvl` on first write; disabling MVCC requires a `VACUUM INTO` to fold all versions.

---

These two documents align with your earlier stages and keep interfaces narrow so you can evolve safely. If you want, I can also generate **crate/file skeletons** (CLI parsing, admin library traits, fuzz targets, and MVCC struct scaffolds) so your coding agent can start coding immediately.
