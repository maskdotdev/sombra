# Stage8_1 — WAL Durability & Throughput Plan

Great questions. Two key points up front:

1. **FULL isn’t “slow” in production** when you use it the way it’s meant to be used: *many logical writes per transaction*. The ~8–12 ms you’re seeing is the cost of a **single fsync per commit**. If you commit once for 500 ops, that cost amortizes to ~16–24 µs/op, which is perfectly fine.
2. **Yes—SQLite WAL does a form of group commit.** Multiple connections that reach COMMIT around the same time can share a single WAL flush/sync. In `FULL` it still fsyncs **before** acknowledging the commit; the trick is that **one fsync covers many txns**.

Below is how to make this work in our engine (what to change, where, and what it buys you).

---

## Why FULL is viable in prod

* **One fsync per transaction** is the core invariant of FULL. The fsync is expensive (~1–10 ms depending on filesystem/hardware).
* Production systems **batch work**—explicitly (enqueue writes) or naturally (per-request transactions that touch many rows). With 100–10,000 ops per txn, FULL becomes competitive because the fsync is amortized.
* Systems that truly need **many tiny commits** either:
  * accept `NORMAL` (small risk: last few ms of commits can be lost on power loss), or
  * run FULL but rely on **group commit** so multiple tiny transactions share one fsync.

---

## What SQLite does (high level)

* **WAL append:** all writers append page images to a single WAL file.
* **Commit point:** a commit is durable when the WAL is synced. If multiple commits are queued, **one sync** can make all those frames durable at once → *group commit*.
* **NORMAL vs FULL:** in `NORMAL`, SQLite can avoid some fsyncs (the OS may flush later); in `FULL`, it still fsyncs but can **coalesce** many commits into one fsync.

---

## Our equivalent (concrete plan)

You already have up to Stage 8. We’ll augment Stage 3’s WAL path and bindings so BOTH FULL and NORMAL are fast in practice.

### 1) Implement **group commit** in the WAL

> Works in **FULL** and **NORMAL**. Preserves FULL semantics (each COMMIT returns only after the fsync that covered it).

**Design (“commit barrier” / “leader fsync”)**

* A per‑DB **WalCommitQueue** collects commit requests: `{frames, completion_tx}`.
* The **first** request that finds the queue empty becomes the **leader**:
  1. Drain additional pending requests for a tiny window (e.g., up to `N` frames or `<= 1–2 ms`).
  2. Append frames for **all** drained commits.
  3. **fsync(WAL) once**.
  4. Acknowledge all completion channels.
* Followers just enqueue; they’re completed when the leader fsyncs.

**Where to put it**

* `crates/sombra-wal`: add `WalCommitter` with a lock‑free (or mpsc) queue and a small draining loop.
* `Pager::commit_txn` switches from “append + immediate fsync” to “enqueue and wait”.

**Semantics**

* **FULL:** each COMMIT waits for the leader’s fsync; durable on return.
* **NORMAL:** same logic, but the fsync can be *time-based* (or disabled entirely); still one fsync can cover many commits.

**Why this helps even with our single-writer design?** We serialize page writes anyway, but many independent callers can hit COMMIT back‑to‑back. The barrier lets their commits share a single fsync instead of doing one fsync per COMMIT.

---

### 2) Give callers the right **knobs** (end-to-end)

* Add **`synchronous: 'full' | 'normal' | 'off'`** to open options in Node/Python and plumb it through FFI → pager → WAL.
* Add **coalesce parameters**: `commitCoalesceMs` and/or `coalesceMaxFrames`.
* Add `PRAGMA synchronous`, `PRAGMA wal_coalesce_ms` to change these at runtime.

**Defaults**

* Keep **`FULL`** as the library default (production safety).
* For **benchmarks** and high-throughput ingestion, recommend `NORMAL` with `commitCoalesceMs ≈ 2–10`.

**Runtime PRAGMAs**

* `PRAGMA synchronous = 'normal' | 'full' | 'off'` — `db.pragma("synchronous", "normal")` in Node/Python updates the live pager without reopening.
* `PRAGMA wal_coalesce_ms = <u64>` — `db.pragma("wal_coalesce_ms", 5)` adjusts the group-commit wait window (ms) at runtime; omit the value to read the current setting.
* `PRAGMA autocheckpoint_ms = <u64 | null>` — `db.pragma("autocheckpoint_ms", null)` disables the time-based trigger; setting a number arms the timer immediately.

---

### 3) **Batch writes** (the biggest practical win)

FULL or NORMAL, if you commit per row, you pay per row.

* Expose real write transactions in bindings: `db.beginWrite / tx.commit / tx.rollback`.
* Add a `db.mutateMany(ops[])` helper that keeps one txn for a batch.
* In the engine, sort adjacency inserts by key within the txn to reduce B+‑tree churn (cheap and effective).

> This alone usually takes you from **~9 ms/op** to **hundreds of µs/op**.

---

### 4) **Warm caches & avoid catalog round-trips**

* Cache label/type/property **IDs** in bindings; pass numeric IDs to FFI to skip dictionary lookups in the hot path.
* Keep a process-local **IndexSet** so we aren’t “ensuring” indexes on every call.

---

## Minimal code you need (summary)

* **WAL group commit** (FULL & NORMAL):
  * Add `WalCommitter` queue and a leader-drain-fsync cycle.
  * `Pager::commit_txn` enqueues and waits.
* **Knobs**: `OpenOptions { synchronous, commit_coalesce_ms }` → Node/Python `ConnectOptions`, runtime PRAGMAs.
* **Transactions in bindings**: `db.transaction(async tx => { … })`, plus `mutateMany`.
* **Fast ID path**: `createNodeIds(labels: number[], propsFast)` in FFI & bindings.

If you want exact signatures and where to paste them: see my previous message’s drop-in snippets (OpenOptions, WalCommitter, Node/Python APIs, FFI mappings). They already match your Stage‑8 layout.

---

## What to use in production

Here’s a realistic playbook we see work well:

* **Mission-critical durability**:
  * `synchronous = FULL`
  * **Batch** logical writes: e.g., per request or per micro-batch (100–10k ops).
  * **Group commit** enabled (the WAL barrier described above).
  * Large page cache; WAL preallocation (64–256 MiB) to stabilize latency.
* **High-throughput ingestion** (okay to lose last ~10 ms on power loss):
  * `synchronous = NORMAL, commitCoalesceMs = 2–10`
  * Big batches, sorted adjacency inserts.
  * Disable property indexes during load; build offline; then re-enable.
* **Synthetic engine benchmarks** (no durability):
  * `synchronous = OFF` on tmpfs/RAM disk to measure CPU/storage code, not disk.

---

## FAQ

**Q: Can we do group commit while keeping FULL semantics?**  
**A:** Yes. The leader fsync happens **before** any COMMIT returns. You get one fsync that covers many transactions.

**Q: Why not always rely on NORMAL?**  
**A:** NORMAL is great, but some customers demand *“no committed data loss on power failure”*. That’s what FULL guarantees. With batching and group commit, FULL can still be very fast.

**Q: Is this exactly like SQLite?**  
**A:** Conceptually, yes: **one WAL, many frames, one sync covering many commits**. In our codebase, that’s the `WalCommitter` barrier. The differences are mechanical (names, lock layout), not semantic.

---

## Action list (order of operations)

1. Implement **WalCommitter** + barrier (leader fsync) and switch `Pager::commit_txn` to enqueue/wait.
2. Expose **`synchronous` + coalesce** options (FFI + Node/Python + PRAGMAs).
3. Ship **bindings transactions** (`db.transaction`, `mutateMany`) and **ID fast-paths**.
4. Update your bench to use `NORMAL` + batches + persistent DB.
5. (Optional) Preallocate WAL and sort adjacency writes within a txn.

Do these and you’ll have **SQLite-class** ergonomics: FULL is safe *and* fast when used with transactions, and NORMAL gives you easy extra headroom.
