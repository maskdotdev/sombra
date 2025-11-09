# 📄 STAGE 10 — MVCC Prototype (Feature: `mvcc`) - NOT MVP

**Modules:**

* `primitives::concurrency` (new impl: `MvccCow` alongside existing `SingleWriter`)
* `primitives::pager` (add version‑aware reads via `VersionedPageStore` trait)
* `primitives::wal` (extend existing WAL or add new `mvl` module for **MVCC log** with commit records)
* `admin` (update checkpoint, stats to support MVCC)
* `storage` (integrate MVCC transaction manager)

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

Either extend existing `primitives::wal` module with MVCC support or add new `primitives::mvl` module. For separate file, use `graph.sombra-mvl`. We'll describe as **MVL**:

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

Implement a new `MvccCow` type in `primitives::concurrency` that provides multi-writer capability alongside the existing `SingleWriter`. Add a transaction manager interface:

```rust
// In primitives::concurrency::mvcc
pub trait TxnManager {
    type R<'a>: ReadTxn;
    type W<'a>: WriteTxn;

    fn begin_read(&self)  -> Result<Self::R<'_>>;
    fn begin_write(&self) -> Result<Self::W<'_>>;
}

pub struct MvccCow {
    // Epoch management, VPT, LastWriter tracking
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

* To modify page `p`, read **visible version** at `snapshot_lsn` into the txn's buffer; apply mutations → new image buffered.

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

Extend existing `admin::checkpoint` to support MVCC mode:

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

Add **version‑aware read** in `primitives::pager` module under `mvcc` feature:

```rust
// In primitives::pager::mod.rs (behind #[cfg(feature = "mvcc")])
pub trait VersionedPageStore {
    fn read_page_at(&self, id: PageId, lsn: Lsn) -> Result<PageRef<'_>>;
}
```

* Implement `VersionedPageStore` for the existing `PageStore` type
* Uses `VPT` to locate newest ≤ `lsn`.
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

Extend existing `admin::stats` module to include MVCC metrics:

* `mvcc.current_lsn`, `mvcc.active_readers`, `mvcc.active_writers`
* `mvcc.commit_bytes`, `mvcc.frames`, `mvcc.conflicts` (counter + reasons)
* `mvcc.gc_versions_removed`, `mvcc.gc_last_stable_lsn`
* Add to `StatsReport` struct and expose via CLI `stats` command.

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

* "Disjoint writers commit independently" test passes repeatedly (hundreds of runs).
* "Conflict abort" test deterministically aborts loser; no torn data.
* Readers see consistent snapshots at selected LSN.
* GC reclaims old versions without impacting active readers.

---

## 10) Existing architecture & integration points

1. **Concurrency foundation**

   * Existing `primitives::concurrency::SingleWriter` provides the single-writer baseline
   * `MvccCow` will be a new implementation in the same module, selected via feature flag or at database open time

2. **WAL infrastructure**

   * Existing `primitives::wal` module has frame-based structure with LSN tracking (`WalFrame`, `WalIterator`)
   * Extend with commit records or create parallel `primitives::mvl` module
   * `WalCommitter` already provides batching infrastructure

3. **Pager system**

   * `primitives::pager::PageStore` manages pages and cache
   * Add `VersionedPageStore` trait for MVCC reads
   * Meta system in `primitives::pager::meta` tracks root pages

4. **Storage layer**

   * `storage::graph::Graph` uses `Arc<dyn PageStore>` 
   * Can swap to MVCC-aware pager without changing storage code
   * B+ trees in `storage::btree` already work with page abstraction

5. **Admin operations**

   * `admin::checkpoint` handles existing WAL checkpointing
   * Extend for MVCC stable_lsn and version GC
   * `admin::stats` provides `StatsReport` structure for metrics

6. **Feature flag system**

   * Use existing `Cargo.toml` feature mechanism (currently has `degree-cache`, `ffi-benches`)
   * Add `mvcc` feature to conditionally compile MVCC code

---

## 11) Step‑by‑Step Checklist (coding agent)

**A. MVL & VPT**

* [ ] Extend `primitives::wal` or create `primitives::mvl` module with **commit record**.
* [ ] Implement `VPT` (in‑memory) + builder from MVL scan on open in `primitives::concurrency::mvcc`.
* [ ] Implement append allocator for MVL (atomic offset) - can reuse patterns from existing `Wal::append_frame`.

**B. TxnManager::MvccCow**

* [ ] Create `primitives::concurrency::mvcc` submodule.
* [ ] Implement `MvccCow` struct with VPT, LastWriter, epoch tracking.
* [ ] Read tx: `snapshot_lsn`, `EpochGuard`, versioned reads.
* [ ] Write tx: dirty map, `read_version` capture on first touch.
* [ ] Commit: assign `commit_lsn`, validate, append frames, append commit, fsync per mode, publish `LastWriter` updates.
* [ ] Abort: drop dirty set.

**C. Pager integration**

* [ ] Add `VersionedPageStore` trait to `primitives::pager`.
* [ ] Implement `read_page_at(id, lsn)` path; cache policy; checksums.
* [ ] Update `PageStore` to optionally support versioned reads behind `mvcc` feature.

**D. Checkpoint & GC**

* [ ] Extend `admin::checkpoint::checkpoint()` for MVCC mode.
* [ ] Compute `stable_lsn` from epochs; fold versions ≤ stable to base; truncate MVL.
* [ ] Optional: persist compact VPT checkpoint state.

**E. Tests**

* [ ] Add `tests/integration/mvcc_*.rs` test files.
* [ ] Disjoint vs overlapping writer scenarios.
* [ ] Crash windows around commit and checkpoint.
* [ ] Reader snapshot consistency.

**F. Observability & CLI**

* [ ] Extend `admin::stats::StatsReport` with MVCC section.
* [ ] Update `cli` commands to show MVCC stats.
* [ ] Ensure `checkpoint` command works with MVCC databases.

---

## 12) Migration & Feature Flagging

* Add `mvcc` feature to `Cargo.toml` features section
* Build `sombra` with `--features mvcc` to enable MVCC; default remains single‑writer (`SingleWriter`)
* Database opening can detect MVCC mode from file metadata and instantiate appropriate transaction manager
* On a DB created without MVCC, enabling MVCC creates `graph.sombra-mvl` on first write; disabling MVCC requires using `admin::vacuum::vacuum_into()` to fold all versions
* Update `storage::graph::Graph` constructor to accept either `SingleWriter` or `MvccCow` based on configuration

---

## 13) File Structure

Expected new/modified files in the monolithic `sombra` crate:

```
src/
  primitives/
    concurrency/
      mod.rs              # Update with mvcc module export
      mvcc.rs             # New: MvccCow, VPT, TxnManager trait
    wal/
      mod.rs              # Potentially extend for commit records
    mvl/                  # Alternative: separate MVL module
      mod.rs              # New MVL implementation
    pager/
      mod.rs              # Add VersionedPageStore trait
      versioned.rs        # New: version-aware page reads
  storage/
    graph.rs              # Update to support MVCC transaction manager
  admin/
    checkpoint.rs         # Extend for MVCC checkpointing
    stats.rs              # Add MVCC metrics to StatsReport
tests/
  integration/
    mvcc_concurrent.rs    # New: concurrent writer tests
    mvcc_recovery.rs      # New: crash recovery tests
    mvcc_snapshot.rs      # New: snapshot isolation tests
```

---

This document aligns with the current monolithic architecture while maintaining the detailed MVCC design from the original. The implementation can now reference actual existing modules and extend them appropriately.
