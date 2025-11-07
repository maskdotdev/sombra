# 📄 STAGE 3 — WAL + Recovery + Locking

**Outcome:** single‑writer / multi‑reader durability via a Write‑Ahead Log (WAL), crash recovery, and cross‑platform file locking. Readers always see a consistent snapshot; writers never corrupt the DB. (Readers see new commits after checkpoint; writer is never blocked by readers to commit.)

---

## 0) Summary & non‑goals

**You will build:**

* An append‑only **WAL file** (`graph.sombra-wal`) with checksummed frames.
* A **commit protocol** with group commit and configurable durability.
* A **recovery** path that replays the WAL on startup.
* An **auto‑checkpoint** that merges WAL frames back into the main DB file.
* A **lock file** (`graph.sombra-lock`) implementing multi‑process single‑writer + multi‑reader semantics.

**Non‑goals (Stage 3):**

* Readers overlaying the WAL directly (SQLite’s wal-index).
  *MVP behavior:* readers see the last checkpoint; new commits become visible after checkpoint.
* Multi‑writer (arrives with MVCC later) - **NOT MVP - IGNORE**.

---

## 1) Terms

* **Page**: fixed-size block (default 8 KiB) managed by the pager.
* **LSN**: increasing `u64` **log sequence number** assigned at commit time.
* **Frame**: one WAL record containing a full page image (not a delta).
* **Checkpoint**: copying committed frames from WAL to the main file, advancing the durable snapshot.

---

## 2) Files & naming

* Main DB: `graph.sombra`
* WAL file: `graph.sombra-wal`
* Lock file: `graph.sombra-lock` (small, separate file to avoid platform quirks)

---

## 3) WAL on‑disk format

**File header (32 bytes):**

| Off | Size | Field           | Notes                                      |
| --: | ---: | --------------- | ------------------------------------------ |
|   0 |    4 | magic `b"SOMW"` | WAL magic                                  |
|   4 |    2 | format_version  | `1`                                        |
|   6 |    2 | reserved        |                                            |
|   8 |    4 | page_size       | must equal DB page size                    |
|  12 |    8 | wal_salt        | random per‑DB (copy from meta.salt or new) |
|  20 |    8 | start_lsn       | LSN of first commit stored in this WAL     |
|  28 |    4 | crc32           | header CRC (with this field zeroed)        |

**Frame (fixed size = page_size + 32 bytes):**

| Off | Size | Field            | Notes                                   |
| --: | ---: | ---------------- | --------------------------------------- |
|   0 |    8 | frame_lsn        | commit LSN for this page image          |
|   8 |    8 | page_id          | absolute page id                        |
|  16 |    8 | prev_crc32_chain | rolling CRC of prior frame              |
|  24 |    4 | payload_crc32    | CRC of the page payload                 |
|  28 |    4 | header_crc32     | CRC of this 32‑byte header (zeroed crc) |
|  32 |    N | payload          | **full page image** after the header    |

**CRC policy:**

* `payload_crc32 = crc32(payload)`.
* `header_crc32 = crc32(header with header_crc32=0)`.
* `prev_crc32_chain` allows detecting torn appends across frames.

**Rationale:** Full‑page images simplify correctness and recovery. Compression is a future optimization.

---

## 4) Meta page additions (main DB page 0)

Add/repurpose fields (payload area of page 0):

* `last_checkpoint_lsn: u64` — the durable LSN in the main file.
* `wal_salt: u64` — copied into WAL headers for validation.
* `wal_policy_flags: u32` — synchronous mode, autocheckpoint thresholds.
* (Keep `next_page`, `free_head`, and future root slots as in Stage 2.)

**Invariant:** `last_checkpoint_lsn` **never decreases**.

---

## 5) Locking model (cross‑process)

Locking is implemented on `graph.sombra-lock` via **byte‑range locks**:

| Byte range | Mode          | Purpose                                   |
| ---------- | ------------- | ----------------------------------------- |
| 0..1       | **shared**    | **Reader lock**. Readers hold shared.     |
| 1..2       | **exclusive** | **Writer lock**. Single writer at a time. |
| 2..3       | **exclusive** | **Checkpoint lock**. Excl vs readers.     |

**Rules:**

* **Readers**: acquire **shared** on [0..1); release after finishing the read txn. No blocking of writer commits to WAL.
* **Writer**: acquire **exclusive** on [1..2) when a write transaction begins. Commits append to WAL; readers unaffected.
* **Checkpoint**: needs exclusive on [2..3) **and** no reader lock on [0..1). If readers are present, postpone. (We’ll do “best effort” autocheckpoint.)

**Windows/Unix:** implement both using platform file locking (`LockFileEx` / `fcntl`). Locks are **advisory**; code must respect them.

---

## 6) Transaction lifecycle

### 6.1 Begin read

* Take **shared** reader lock.
* Snapshot at `meta.last_checkpoint_lsn` (returned for introspection).
* Reads use **main file only**.

### 6.2 Begin write

* Take **exclusive** writer lock.
* Build a private **dirty set** of `(page_id → new_page_image)` in memory.
* When first mutation occurs, allocate a **provisional LSN** (monotonic counter in memory, not persisted).

### 6.3 Commit (single transaction)

1. **Serialize and append** one frame per dirty page: `(frame_lsn, page_id, payload, CRCs)`.
2. **fsync(WAL)** depending on synchronous mode (see §7).
3. Release writer lock (other writers may start) **without checkpointing**.
4. Optionally trigger **auto‑checkpoint** in the background (same process) if:

   * WAL size > `autocheckpoint_pages` * page_size; or
   * Time since last checkpoint > `autocheckpoint_ms`.

### 6.4 Checkpoint

* Try to acquire **exclusive checkpoint lock** and verify **no readers** (no shared locks on [0..1)).
* For each frame **with frame_lsn > last_checkpoint_lsn** (in WAL order):

  * Write payload to `db_file[page_id * page_size ..]`.
* **fsync(DB)**.
* Update **meta.last_checkpoint_lsn = max_frame_lsn_applied** in page 0 and write page 0.
* **fsync(DB)** again.
* **Truncate or reset WAL** if fully checkpointed (set file length to WAL header only).
* Release checkpoint lock.

**Note:** If readers exist, skip checkpoint; retry next time.

---

## 7) Durability & performance modes

Expose through `OpenOptions` / PRAGMA‑like API:

* `Synchronous::Full` — fsync WAL at commit; fsync DB at checkpoint; safest.
* `Synchronous::Normal` — fsync WAL at group‑commit intervals (e.g., every 10 ms or N pages); DB fsync at checkpoint.
* `Synchronous::Off` — no fsync; fastest; unsafe (for testing).

**Group commit:** batch frames from concurrent (queued) writers under the same fsync to amortize cost. (Stage 3 still has single writer; “concurrent writers” here means back‑to‑back transactions coalesced by the commit loop.)

---

## 8) Recovery at startup

1. Open DB; read and validate **meta**.
2. If `graph.sombra-wal` exists and is non‑empty:

   * Validate WAL header (magic, version, page_size, wal_salt).
   * Scan frames sequentially:

     * Validate header CRC, payload CRC, and chain CRC.
     * Keep **largest valid prefix** (stop at first corruption).
   * For valid frames with `frame_lsn > meta.last_checkpoint_lsn`:

     * Apply to main DB file offset (`page_id * page_size`).
   * **fsync(DB)**.
   * Update meta `last_checkpoint_lsn`.
   * Truncate or reset WAL (optional: keep header).
3. Open ready.

**Idempotence:** replaying the same WAL frames again is safe (pages are overwritten with identical images).

---

## 9) Pager integration (differences vs Stage 2)

* `PageStore::get_page_mut` marks pages dirty in the **txn’s private dirty set**, not written to DB directly.
* On `commit`: serialize frames from the dirty set and **append to WAL** via `sombra-wal`.
* Cache still holds modified pages; mark them **clean** post‑commit to avoid re‑writing them again before checkpoint.
* Reads always read from DB file (snapshot at last checkpoint) in Stage 3.

**New/updated traits:**

```rust
pub trait PageStore {
    fn begin_write(&self) -> Result<WriteGuard>;
    fn commit(&self, guard: WriteGuard) -> Result<Lsn>;
    fn checkpoint(&self, mode: CheckpointMode) -> Result<()>;
    fn last_checkpoint_lsn(&self) -> Lsn;
}
pub enum CheckpointMode { Force, BestEffort }
```

---

## 10) Observability

* `wal.bytes_appended`, `wal.frames_appended`
* `wal.autocheckpoint_triggered`, `wal.truncated`
* `recovery.frames_replayed`, `recovery.max_lsn`
* `locks.readers`, `locks.writer_waits`, `locks.checkpoint_skips`

---

## 11) Tests & acceptance

**Unit/integration**

* Commit 1‑page, 10‑page, 1k‑page txns; verify WAL frames and CRCs.
* Crash‑monkey (kill between steps of commit/checkpoint): always recover to a consistent DB with `last_checkpoint_lsn` monotonic.
* Readers running during writer commit continue uninterrupted (return old snapshot).
* Autocheckpoint triggers at size threshold; WAL shrinks.

**Property tests**

* Random sequences of writes with random page ids; after recovery, DB contents equal to the last fully committed sequence.

**Performance smoke**

* Measure txns/sec & fsync rate across FULL/NORMAL/OFF; WAL append MB/s.

**Acceptance (Stage 3)**

* All tests green on Linux/macOS/Windows.
* No data loss on power‑off simulation with FULL mode.
* Readers are never blocked by writer commit; writer is blocked only by lock acquisition at **begin_write**, not by readers.

---

## 12) Step‑by‑step checklist (coding agent)

* [ ] Implement `sombra-wal` with file header, frame serialization, CRCs.
* [ ] Add `sombra-concurrency::SingleWriter` with writer/reader/checkpoint locks.
* [ ] Integrate WAL into `sombra-pager` write path (`begin_write/commit`).
* [ ] Implement recovery at open.
* [ ] Implement autocheckpoint and manual checkpoint.
* [ ] Add tests (unit, property, crash‑monkey).
* [ ] Expose PRAGMAs / options for sync and autocheckpoint.


