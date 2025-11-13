## 0) Model, assumptions, and goals

> **NOTE**: This plan uses **XID-based MVCC** with a **Transaction Status Table (TST)** instead of timestamp-based visibility. This design avoids in-place page updates at commit time and simplifies WAL/recovery. See §1 and §3 for details.

**Key design decisions (updated)**:

1. **XIDs not timestamps in tuples**: Store `xmin`/`xmax` (TxIDs) in version headers; commit state lives in TST.
2. **No page rewrites at commit**: Visibility determined by TST lookup; commit atomically updates TST only.
3. **Adjacency extent chains**: `next_ptr` in segment headers for high-degree nodes (>100 neighbors per page).
4. **32-byte aligned MVCC headers**: Cache-friendly, consistent sizing across all versioned structures.
5. **Atomic edge + adjacency visibility**: Both use same `xmin`; visibility flips together at TST commit.

**Graph model (assumed)**

* **Nodes**: `NodeID (u64)`, labels (`SET<LabelID>`), properties (`MAP<string, value>`).
* **Edges**: `EdgeID (u64)`, `src: NodeID`, `dst: NodeID`, `type: EdgeTypeID`, direction (OUT/IN), properties, optional multi‑edges.
* **Adjacency**: OUT and IN lists per node, grouped by edge type.

**Concurrency/isolation goals**

* Default **Snapshot Isolation (SI)** for entire graph queries (e.g., traversals run on a fixed snapshot).
* Optional **Read Committed (RC)** per statement; **Serializable** via SSI/SSN extension with graph‑aware read markers (see §10).
* Readers never block writers; writers minimally block writers.
* No torn traversals: a single multi‑step traversal runs with one `read_ts`.

**File‑based assumptions**

* Single‑host to start; later replication optional.
* Direct I/O or mmap files; page size tunable (e.g., 8–16 KiB).
* WAL + fuzzy checkpoints.

---

## 1) MVCC invariants for a graph

We version **Nodes**, **Edges**, and **Adjacency entries**. Every version uses **XID-based** visibility (not timestamps in the tuple). Commit status is tracked in a **Transaction Status Table (TST)**.

**Key design: XIDs not timestamps**

* Each version stores `xmin` (creator TxID) and `xmax` (deleter/overwriter TxID, or 0 for ∞).
* **No in-place timestamp updates at commit**—visibility is determined by looking up `xmin`/`xmax` state in the TST.
* This avoids random page writes on commit and simplifies WAL/recovery.

**Visibility rules**

A node/edge/adj-entry version `V` is visible to snapshot `S(read_ts)` iff:

1. `V.xmin` is COMMITTED in TST with `commit_ts ≤ read_ts`,
2. `V.xmax == 0` OR `V.xmax` is ABORTED OR `V.xmax` committed with `commit_ts > read_ts`,
3. For edges: **both endpoints** are visible at `read_ts`.

Adjacency visibility must imply edge visibility and vice versa (no dangling neighbor entries for a visible edge at a given timestamp).

---

## 2) On‑disk layout (files and record formats)

> Keep records append‑only; update by adding a new version and “closing” the previous one. Use indirection tables for fast head lookups.

### 2.1 File set

* `nodes.dat` – append‑only **NodeVersion** records.
* `nodes.head` – fixed array mapping `NodeID → FilePtr` (offset to head version).
* `edges.dat` – append‑only **EdgeVersion** records.
* `edges.head` – `EdgeID → FilePtr` to head version.
* `adj-out.dat` – append‑only **AdjSegment** pages for OUT adjacency.
* `adj-in.dat` – append‑only **AdjSegment** pages for IN adjacency.
* `labels.idx` – label→NodeID postings (MVCC‑aware).
* `props.idx` – property (key,typed_value,entityKind)→ID postings (MVCC‑aware).
* `freemap.dat` – free space map for segment reuse (post‑GC).
* `txn.tbl` – persisted transaction table snapshot (for recovery).
* `wal.log` – write‑ahead log (physical redo).

All file pointers are 64‑bit offsets; pages have checksums.

### 2.2 Version headers

**MVCC Header (32 bytes, shared prefix)**

All versioned structures (Node, Edge, AdjEntry) start with a common 32-byte MVCC header for cache-friendly scans:

```rust
// src/storage/mvcc.rs
#[repr(C)]
pub struct MvccHdr {
    pub xmin: u64,        // creator TxID
    pub xmax: u64,        // 0 == ∞ (no deleter/successor yet)
    pub prev_ptr: u64,    // file offset of previous version (0 if none)
    pub flags: u16,       // TOMBSTONE=0x1, HINT_XMIN_COMMITTED=0x2, HINT_XMAX_COMMITTED=0x4
    pub _pad: [u8; 6],    // keep hdr 32 bytes; no misalignment in scans
}
```

**NodeVersion (NV)** — embeds NodeRow right after MvccHdr:

```
Offset  Size  Field
0       8     xmin
8       8     xmax
16      8     prev_ptr
24      2     flags
26      6     _pad                  // ---- 32 bytes MVCC header
32      8     node_id               // NodeRow starts
40      1     label_count
41      4N    labels[N]
41+4N   4     props_len
45+4N   ...   props_blob (inline or VRef)
...     4     crc32
```

**EdgeVersion (EV)**:

```
Offset  Size  Field
0       8     xmin
8       8     xmax
16      8     prev_ptr
24      2     flags
26      6     _pad                  // ---- 32 bytes MVCC header
32      8     edge_id               // EdgeRow starts
40      8     src
48      8     dst
56      4     type
60      4     props_len
64      ...   props_blob
...     4     crc32
```

**Adjacency entry (stored inside segments)**

```rust
#[repr(C)]
pub struct AdjEntry {
    pub edge_id: u64,
    pub neighbor: u64,
    pub ty: u32,
    pub _pad_align: u4,
    pub xmin: u64,           // creator TxID
    pub xmax: u64,           // 0==∞
    pub flags: u16,          // TOMBSTONE, etc.
    pub _pad: u16,
} // 40 bytes
```

**Adjacency segment page** (64 bytes header with extent chain support):

```
Offset  Size  Field
0       8     owner_node
8       1     direction (OUT=0, IN=1)
9       3     _pad_dir
12      4     type
16      8     base_ptr        // previous version (MVCC chain)
24      8     next_ptr        // next extent within the same version
32      4     entries
36      4     free_bytes
40      8     seg_xmin        // creator XID
48      8     seg_xmax        // 0==∞ (superseded version)
56      4     crc32
60      4     _pad
// 64 bytes total
// Followed by packed AdjEntry[] (sorted by neighbor, then xmin desc)
```

With 4 KiB pages and a 64-byte header, capacity ≈ `(4096-64)/40 = 100` entries per extent.

> **Why version adjacency segments?** So a traversal can open the **segment head as of `read_ts`** and iterate entries, checking visibility via TST. Copy‑on‑write at update time avoids rewriting large historic segments. The **`next_ptr`** enables extent chains for high-degree nodes (>100 neighbors per version).

---

## 3) Transaction Status Table (TST) & timestamping

**Key insight**: Commit state lives in the **TST**, not in tuples. Tuples store immutable XIDs; visibility checks consult the TST.

**Global timestamp counter**

* `GLOBAL_TS = atomic_u64` (commit counter).
* On BEGIN: `read_ts = atomic_fetch_add(GLOBAL_TS, 1)`.
* On COMMIT: `commit_ts = atomic_fetch_add(GLOBAL_TS, 1)`.

**Transaction Status Table (TST)** — in-memory hash table, persisted via TxLog WAL:

```rust
// src/storage/txn_status.rs
pub enum TxnState {
    Active { read_ts: u64 },
    Committed { commit_ts: u64 },
    Aborted,
}

pub struct TxnStatusTable {
    states: DashMap<u64, TxnState>,  // TxID -> state
}
```

**TxLog records** (persisted to WAL):

```rust
pub enum TxLog {
    Begin { tid: u64, read_ts: u64 },
    Commit { tid: u64, commit_ts: u64 },
    Abort { tid: u64 },
}
```

**Visibility check** (shared for all versioned structures):

```rust
#[inline(always)]
fn visible(h: &MvccHdr, snap_ts: u64, tst: &TxnStatusTable) -> bool {
    // xmin must be committed with commit_ts <= snapshot_ts
    match tst.state(h.xmin) {
        TxnState::Committed { commit_ts } if commit_ts <= snap_ts => {}
        TxnState::Aborted => return false,
        _ => return false, // in-progress or committed after snapshot
    }

    if h.xmax == 0 {
        return true; // no deleter
    }

    match tst.state(h.xmax) {
        TxnState::Aborted => true, // deletion never took effect
        TxnState::Committed { commit_ts } => commit_ts > snap_ts,
        _ => true, // deleter still in-progress at snapshot
    }
}
```

**Active snapshots registry**: lock‑free set of current `read_ts` values (for GC safe point calculation).

---

## 4) Indexing (graph‑specific) with MVCC

* **Head maps** (`nodes.head`, `edges.head`) give O(1) to the newest version chain head for a Node/Edge.
* **Label index**: postings for (LabelID → NodeID) with entry headers holding begin/end ts. On node label add/remove, append posting entries with MVCC metadata; purge at GC.
* **Property index**: (key, typed_value, kind[NODE|EDGE]) → ID; same MVCC scheme.
* **Adjacency**: OUT and IN per `(owner_node, edge_type)` segmented lists (see §2.2).

> **Rule:** secondary index entries must never make invisible entities appear. Readers validate visibility of base Node/Edge versions by `read_ts` before returning results.

---

## 5) Locking & conflict policy

Granularity:

* **Node locks**: write‑intent on `NodeID` (for property/label updates, delete).
* **Edge locks**: write‑intent on `EdgeID`.
* **Adjacency lock**: **per (owner_node, direction, edge_type)** segment lock for **update** (copy‑on‑write); reads are lock‑free.
* **Lock ordering**: To add/remove an edge, lock `min(src,dst)` node first, then `max(src,dst)` to avoid deadlocks; then the new edge id.

Policy:

* **First‑committer‑wins** on the same node/edge chain (SI). Detect at validation if another committed version was installed after our `read_ts`.
* Use **wound‑wait** or **wait‑die** for writer collisions to reduce deadlocks.

---

## 6) Read algorithms

### 6.1 Node read

```rust
node_read(id, read_ts, tst: &TxnStatusTable):
  v = nodes.head[id]
  while v:
    if visible(&v.mvcc_hdr, read_ts, tst): return v
    v = v.prev_ptr
  return NOT_FOUND
```

### 6.2 Edge read

* Same, plus ensure endpoints are visible at `read_ts`. If an edge is visible but an endpoint isn't, treat edge as invisible (topology consistency).

### 6.3 Adjacency scan (core for traversal)

**With extent chain support via `next_ptr`:**

```rust
adj_iter(owner, dir, type, read_ts, tst: &TxnStatusTable):
  seg = adj_head(owner, dir, type)
  
  // Walk version chain via base_ptr to find visible segment version
  while seg:
    seg_visible = check_segment_visible(seg, read_ts, tst)
    if seg_visible:
      break
    seg = seg.base_ptr
  
  // Iterate extent chain via next_ptr within this version
  while seg:
    for e in seg.entries:
      if visible_adjentry(&e, read_ts, tst):
        // Optionally validate edge visibility (endpoints) lazily
        yield (e.neighbor, e.edge_id)
    seg = seg.next_ptr  // follow extent chain

fn check_segment_visible(seg: &AdjSegment, read_ts: u64, tst: &TxnStatusTable) -> bool {
    // Check segment-level MVCC header
    match tst.state(seg.seg_xmin) {
        TxnState::Committed { commit_ts } if commit_ts <= read_ts => {}
        _ => return false,
    }
    if seg.seg_xmax == 0 {
        return true;
    }
    match tst.state(seg.seg_xmax) {
        TxnState::Aborted => true,
        TxnState::Committed { commit_ts } => commit_ts > read_ts,
        _ => true,
    }
}

fn visible_adjentry(e: &AdjEntry, read_ts: u64, tst: &TxnStatusTable) -> bool {
    // Check entry-level MVCC (xmin/xmax)
    match tst.state(e.xmin) {
        TxnState::Committed { commit_ts } if commit_ts <= read_ts => {}
        _ => return false,
    }
    if e.xmax == 0 {
        return true;
    }
    match tst.state(e.xmax) {
        TxnState::Aborted => true,
        TxnState::Committed { commit_ts } => commit_ts > read_ts,
        _ => true,
    }
}
```

**Traversal** (BFS/DFS) holds a **single `read_ts`** across the whole query, preventing fractured reads. The `TxnStatusTable` reference provides consistent visibility for all checks.

---

## 7) Write algorithms

### 7.1 Create/Update/Delete Node

1. Lock node.
2. Load head; create new NV with `xmin=my_tid`, `xmax=0`, `prev_ptr=head`, set flags/tombstone as needed; append to `nodes.dat`.
3. **Immediately set `xmax=my_tid` on the old version** (if any) while page is latched—this is a single in-place write pre-commit.
4. CAS `nodes.head[id] = new_ptr`.
5. Update `labels.idx` and `props.idx` with entries containing `xmin=my_tid`.
6. On commit: write `TxLog::Commit{tid, commit_ts}` to WAL, fsync, then update `TST[my_tid] = Committed{commit_ts}`. **No page rewrites needed**—visibility flips via TST lookup.

### 7.2 Create Edge

1. Lock `min(src,dst)` then `max(src,dst)` (deadlock prevention), then lock new `edge_id`.
2. Validate endpoints **visible at my `read_ts`** (for SI semantics) and not tombstoned.
3. Create EV with `xmin=my_tid`, `xmax=0` in `edges.dat`.
4. Append **AdjEntry** for `(src, OUT, type)` and `(dst, IN, type)` in their segment heads with `xmin=my_tid`, `xmax=0`.

   * If current segment has insufficient free space or extent grows beyond capacity, **allocate new extent page** with `next_ptr` linking to it, or **copy‑on‑write** a new segment version.
5. On commit: write `TxLog::Commit{tid, commit_ts}`, fsync, update TST. Edge and adjacency entries become visible atomically via TST state change.

### 7.3 Delete Edge

* Same locks as create. Create EV tombstone with `xmin=my_tid`; set old EV `xmax=my_tid`. Append AdjEntry tombstones or set `xmax=my_tid` on existing entries. On commit: TST update makes deletions visible; GC will purge entries once safe.

### 7.4 Update Edge (properties only)

* New EV version with `xmin=my_tid`, set old EV `xmax=my_tid`; adjacency unchanged.

---

## 8) Adjacency storage strategies

Two viable designs (choose one; both MVCC‑safe):

**A. CoW Segments (recommended initially)**

* Each `(owner,dir,type)` has a segment chain. On mutation, write a fresh segment that **rewrites just changed entries + copies header**; link `base_ptr` to previous segment.
* Pros: simple visibility; fast scan at given `read_ts`.
* Cons: potentially higher write amplification for large neighbor sets (mitigate by partial segments per hash bucket).

**B. Delta Pages**

* Keep a stable base segment and append **delta pages** (insert/delete entries with begin/end ts). Periodic **segment compaction** merges delta into new base and retires old base.
* Pros: better for high‑degree nodes.
* Cons: traversal must check base + deltas; slightly more CPU.

**Heuristic**: Nodes with degree ≤ 1024 use CoW; above that switch to Delta Pages (tracked per node).

---

## 9) Validation & commit (single‑node)

**For SI**

1. **Validate write set**:

   * For each modified Node/Edge/AdjEntry, ensure **no committed version** of the same object has `xmin` committed in the range `(my.read_ts, now]` via TST lookup. (First‑committer‑wins.)
   * Topology check for edges: endpoints must not be deleted with commit happening in `(my.read_ts, now]`.
2. **Assign CID**: `commit_ts = atomic_fetch_add(GLOBAL_TS, 1)`.
3. **WAL**:

   * Write redo for all new NV/EV/Adj page changes (physical page-level WAL).
   * Write `TxLog::Commit { tid: my_tid, commit_ts }` record.
   * Fsync WAL before publish (group commit).
4. **Publish** (TST state change only):

   * Update `TST[my_tid] = Committed { commit_ts }`.
   * **No data page rewrites**—all versions with `xmin=my_tid` instantly become visible to snapshots with `read_ts >= commit_ts` via TST lookup.
   * Old versions with `xmax=my_tid` become invisible (their deletion/overwrite is now committed).
5. Release locks, mark txn COMMITTED.

**Abort**

* Write `TxLog::Abort { tid: my_tid }` to WAL.
* Update `TST[my_tid] = Aborted`.
* Versions with `xmin=my_tid` become invisible; old versions with `xmax=my_tid` remain visible.
* GC will eventually remove aborted versions and clear hint bits.

---

## 10) Serializable option for graph workloads

Serializable in graphs is hard due to **path phantoms** (new edges creating new paths). Offer two modes:

**Mode 1: SSI with graph‑aware SIREAD markers (practical)**

* On traversal, for each expanded `(owner,dir,type)` **record a SIREAD marker** on the **adjacency segment id (page id)** and optionally **the set of node IDs visited** (compact using Bloom filters or roaring bitmaps per txn).
* Writers (edge inserts) check at prepare/commit:

  * If inserting an edge `(u→v,type)` and there is a **concurrent SIREAD** on `adj(u, OUT, type)` or `adj(v, IN, type)` by a transaction that started before the writer and the reader will commit after the writer’s `begin_ts`, **abort one** (standard SSI dangerous structure policy).
* This prevents many practical anomalies (e.g., “no outgoing friend edge” invariant checks). It **does not guarantee serializability for arbitrary “no path exists” predicates spanning many hops**—document as a limitation unless users take locks via explicit constraints.

**Mode 2: Next‑Key/Predicate locks (heavy)**

* Expose API to lock an **adjacency partition** `(owner,dir,type)` or a **label/property index range** for the txn lifetime. Use for constraint transactions.

> Recommend **Mode 1 (SSI)** as default serializable implementation; provide explicit predicate locks for rare cases.

---

## 11) Garbage collection (GC) & compaction

**Safe point**: `gc_ts = min(active_read_ts)` (and ≥ last durable checkpoint snapshot). For optional replicas, also consider follower safe time.

**Eligibility** (TST-based):

* NV/EV with `xmax` where `TST[xmax] = Committed{commit_ts}` and `commit_ts ≤ gc_ts` → reclaimable.
* AdjEntry with `xmax` committed at `≤ gc_ts` → remove from segment during compaction.
* Versions with `xmin` where `TST[xmin] = Aborted` → prune (after grace window).

**Process**:

* **Node/Edge chain GC**: walk head→older via `prev_ptr`; unlink and free reclaimable versions (epoch/RCU to avoid UAF).
* **Adjacency compaction**:

  * CoW segments: when chain length or dead ratio exceeds thresholds, **rewrite a new compacted segment** containing only entries visible-at-gc_ts or later. Create new segment with `seg_xmin=compactor_tid`, link via `base_ptr`. Set old segment `seg_xmax=compactor_tid`; visibility flips when compactor commits via TST. Update head pointer.
  * Extent chains: merge/rewrite extents connected by `next_ptr` to eliminate dead entries, maintaining `next_ptr` linkage for large neighbor sets.
  * Delta pages: merge base + deltas into new base; retire old pages.

**Hint bits** (optional optimization):

* Set `HINT_XMIN_COMMITTED` and `HINT_XMAX_COMMITTED` flags in `MvccHdr` during GC passes to reduce TST lookups on hot tuples.
* Hints are never required for correctness—always validate against TST if hint is missing.

**Config knobs**:

* `mvcc.retention_ms` (time‑travel window).
* `adj.compact.dead_ratio_threshold` (e.g., 0.3).
* `max_versions_per_entity` (soft cap triggering compaction).
* Backoff when IO/CPU pressure is high.

---

## 12) Recovery

**WAL records**

Page-level WAL (physical redo) plus dedicated **TxLog** for transaction state:

* **Data WAL** (page-level):
  * `PAGE_APPEND(file_id, offset, page_data, lsn)`
  * `PAGE_UPDATE(file_id, offset, delta, lsn)`
  
* **TxLog WAL** (transaction status):
  ```rust
  pub enum TxLog {
      Begin { tid: u64, read_ts: u64 },
      Commit { tid: u64, commit_ts: u64 },
      Abort { tid: u64 },
  }
  ```

> Can persist TxLog to same `wal.log` with distinct record types, or use dedicated `tx.log`. Both are replayed in order.

**Startup**

1. **Redo** data pages to latest LSN: replay NV/EV/AdjEntry appends and updates.
2. **Rebuild TST**: replay TxLog records in order:
   * `Begin{tid, read_ts}` → `TST[tid] = Active{read_ts}` (transient during recovery).
   * `Commit{tid, commit_ts}` → `TST[tid] = Committed{commit_ts}`.
   * `Abort{tid}` → `TST[tid] = Aborted`.
3. Any version with `xmin=tid` where `TST[tid] = Active` or missing → treat as **Aborted** (txn did not complete).
4. Any version with `xmax=tid` where `TST[tid] = Aborted` → deletion never took effect (version remains visible).
5. Rebuild head maps (`nodes.head`, `edges.head`) from data if needed, or restore from checkpoint.
6. Recompute `GLOBAL_TS = max(all commit_ts) + 1`.

**Checkpoints**

* Fuzzy checkpoint includes:
  * Dirty page images (data files).
  * `nodes.head`/`edges.head` snapshots.
  * `gc_ts` (safe point).
  * TST snapshot with committed/aborted states.
  * Checkpoint LSN marker.
* Recovery can start from checkpoint LSN and replay only subsequent WAL.

---

## 13) Observability & admin

**Metrics**

* `mvcc.active_txns`, `mvcc.safe_point_age_ms`, `mvcc.snapshot_oldest_ts`
* `graph.versions_per_node_p50/p95/p99`, `graph.versions_per_edge_*`
* `adj.segment_chain_len_p95`, `adj.dead_ratio`, `adj.compact_ops/s`
* `abort_rate.{ww,ssi,timeout}`, `commit_latency_ms_p50/p99`
* `wal.bytes_s`, `checkpoint.interval_ms`, `recovery.estimated_time_ms`

**Debug tools**

* `DUMP NODE <id>` → print version chain with timestamps.
* `DUMP EDGE <id>`
* `DUMP ADJ <node,dir,type> @ts`
* `EXPLAIN SNAPSHOT` for a traversal: which segments/pages and why edges were/weren’t visible.
* `LIST LONG_SNAPSHOTS` with killers (clients pinning snapshots).

---

## 14) API surface (engine‑internal)

```c
Txn* begin_txn(Isolation iso);           // pins read_ts for SI/SER
bool commit(Txn*); void abort(Txn*);

Node get_node(Txn*, NodeID);
Edge get_edge(Txn*, EdgeID);

AdjIter adj_iter(Txn*, NodeID owner, Dir dir, EdgeType type);

void put_node(Txn*, NodeID, Labels, Props);      // upsert
void del_node(Txn*, NodeID);

void put_edge(Txn*, EdgeID, NodeID src, NodeID dst, EdgeType, Props);
void del_edge(Txn*, EdgeID);

bool add_label(Txn*, NodeID, LabelID);
bool set_prop(Txn*, Entity e, Key, Value);
```

> Traversals (Cypher/Gremlin‑like) compile down to repeated `adj_iter` + `get_node/edge`, all under one `read_ts`.

---

## 15) Testing plan (graph‑centric)

**Unit**

* Visibility permutations for NV/EV/AdjEntry (begin/end/abort/tombstone).
* Adjacency iterator correctness under chains of CoW segments and delta pages.
* Endpoint‑visibility rule for edges.

**Property‑based/fuzz**

* Random interleavings of node/edge creates/deletes/updates with concurrent traversals; assert SI invariants and absence of use‑after‑free.

**Workloads**

* Star graph, power‑law degree graphs (to stress adjacency CoW vs delta mode).
* BFS/SSSP traversals under write churn; verify **consistent snapshot** semantics.
* Unique constraints (e.g., username unique): race to create nodes; ensure first‑committer wins.

**Serializable (SSI)**

* Construct “dangerous structure” patterns (reader scans adjacency while concurrent edge insert happens) and verify one side aborts.
* Negative test: “no k‑hop path exists” remains a documented limitation unless predicate locks are taken.

**Crash/recovery**

* Crash before/after WAL fsync; between adjacency CoW write and head pointer swap; ensure no torn segments and that ABORTED entries are invisible.

**GC**

* Long‑running traversal while writers churn; measure GC backlog, ensure memory/space bound and correctness.

---

## 16) Performance considerations

* **Hot rows/nodes**: Keep version chains short; trigger compaction when chain len > K (e.g., 8).
* **High‑degree nodes**: Prefer delta pages; batch edge insertions into a single delta page.
* **Traversal CPU**: Store `segment.begin_ts` and `end_ts` to quickly skip whole segments outside snapshot.
* **Edge/property blobs**: For large properties, store off‑page blobs referenced from EV/NV; version the pointer, not the entire blob, when unchanged.

---

## 17) Configuration knobs

```
isolation.default = SI | RC | SERIALIZABLE
mvcc.retention_ms = 600000          // time-travel window
mvcc.gc.threads = N
adj.mode = COW | DELTA | AUTO
adj.cow.max_entries = 2048
adj.delta.compact_dead_ratio = 0.3
serializable.impl = SSI | SSN
writer.conflict_policy = FIRST_COMMITTER_WINS | WOUND_WAIT
checkpoint.interval_ms = 30000
```

---

## 18) Rollout (by feature, not timeline)

1. **A: Foundations**

   * NV/EV chains; head maps; WAL; RC reads; adjacency CoW segments.
2. **B: Full SI**

   * Single `read_ts` per query; write validation (first‑committer‑wins); GC v1 (safe point); label/prop indexes MVCC.
3. **C: Adjacency delta mode + compaction**

   * Heuristic switching; background compactor; free‑space reuse.
4. **D: Serializable (SSI)**

   * SIREAD markers on adjacency segments; commit‑time dangerous structure detection; metrics.
5. **E: Hardening & Ops**

   * Checkpoints; recovery speed; dashboards; admin tools.
6. **F: (Optional) Replication**

   * HLC timestamps; follower‑reads at safe time; 2PC for cross‑partition writes.

**Definition of Done per phase**: all tests passing, perf budgets set/achieved, runbook updated.

---

## 19) Key risks & mitigations

* **Path‑level serializability**: SSI covers local adjacency phantoms but not arbitrary “no path” checks. Provide **predicate locks** API and docs.
* **High‑degree adjacency churn**: Use delta pages; batch; periodic compaction.
* **Long snapshots pin GC**: observability + guardrails; throttle long queries or route to a snapshot copy (future: follower reads).
* **Edge endpoint races**: Validate endpoints at **commit**; abort edge create if endpoint deleted since reader’s snapshot.

---

## 20) Core helpers (pseudo‑code)

**Visibility** (XID-based with TST):

```rust
#[inline(always)]
fn visible(h: &MvccHdr, read_ts: u64, tst: &TxnStatusTable) -> bool {
    // Check xmin (creator)
    match tst.state(h.xmin) {
        TxnState::Committed { commit_ts } if commit_ts <= read_ts => {}
        TxnState::Aborted => return false,
        _ => return false, // in-progress or committed after snapshot
    }

    // Check xmax (deleter/overwriter)
    if h.xmax == 0 {
        return true; // no deleter
    }

    match tst.state(h.xmax) {
        TxnState::Aborted => true, // deletion never took effect
        TxnState::Committed { commit_ts } => commit_ts > read_ts,
        _ => true, // deleter still in-progress at snapshot
    }
}
```

**Commit publish** (TST-based, no page rewrites):

```rust
// All new versions already have xmin=my_tid, old versions have xmax=my_tid
// set during the write phase (pre-commit)

// 1. Write data WAL for all modified pages
for page in write_set.modified_pages {
    wal.append(PageRedo(page));
}

// 2. Assign commit timestamp and write TxLog
let commit_ts = GLOBAL_TS.fetch_add(1);
wal.append(TxLog::Commit { tid: my_tid, commit_ts });
wal.fsync();  // Group commit

// 3. Update TST (makes all xmin=my_tid visible, xmax=my_tid invisible)
tst.set_state(my_tid, TxnState::Committed { commit_ts });

// No page rewrites needed—visibility flips atomically via TST lookup
```

**Adjacency compaction trigger**

```rust
if segment.dead_ratio(tst, gc_ts) > cfg.adj.delta.compact_dead_ratio ||
    segment.version_chain_len() > cfg.adj.max_chain ||
    segment.extent_chain_len() > cfg.adj.max_extents {
    compact_segment(owner, dir, type, gc_ts, tst);
}
```

---

### What to implement first (tickets)

1. **MVCC Headers & TST**: `MvccHdr` (32B) with xmin/xmax; `TxnStatusTable` with `Begin/Commit/Abort` TxLog persistence.
2. **WAL layer**: Page-level WAL + TxLog records; CRC'd pages; group commit.
3. **Node/Edge CRUD**: MVCC chains with XID-based versioning; set `xmax` on old version at write time.
4. **Head maps**: `nodes.head`, `edges.head` (fixed arrays or B-tree backed); fast head pointer lookups.
5. **OUT/IN adjacency segments**: CoW with extent chains (`next_ptr`); AdjEntry with xmin/xmax; iterator with TST visibility checks.
6. **SI transaction manager**: begin/read_ts, commit/commit_ts, TST-based validation (first-committer-wins).
7. **Basic GC**: safe point calculation; TST-based eligibility; prune aborted versions; unlink dead versions from chains.
8. **Label/prop indexes**: MVCC entries with xmin/xmax; TST-based cleanup.
9. **Adjacency delta mode + compaction**: Extent chain rewriting; dead entry removal; hint bit optimization.
10. **SSI read markers**: SIREAD markers on adjacency segments; commit validation for dangerous structures.
11. **Checkpoint + recovery acceleration**: Fuzzy checkpoints with TST snapshots; fast recovery from checkpoint LSN.
12. **Observability + admin tools**: Metrics, debug dumps, version chain inspection.

---

## 22) Engineering checklist (actionable)

This checklist captures the key implementation tasks for the XID-based MVCC system:

* [ ] Implement `MvccHdr` (32B) struct in `src/storage/mvcc.rs` and migrate Node/Edge/AdjEntry encoders.
* [ ] Implement `TxnStatusTable` with `Begin/Commit/Abort` TxLog persistence in `src/storage/txn_status.rs`.
* [ ] Replace all `begin_ts/end_ts` references with `xmin/xmax` throughout codebase; refactor all `visible()` calls to use TST.
* [ ] Add `next_ptr` field to `AdjSegmentHeader`; update allocator logic and iterator to follow extent chains.
* [ ] Unify storage pattern: heap files (`nodes.dat`, `edges.dat`) + `*.head` maps (B-tree or HAMT) for id→ptr lookups.
* [ ] Update all write paths to set **old.xmax = my_tid** at overwrite time (pre-commit in-place write).
* [ ] Wire `ReadGuard{ read_ts, tst_view: &TxnStatusTableView }` into query execution; use `visible()` in all cursors.
* [ ] Introduce GC pass that consults TST commit_ts to reclaim versions and remove dead adjacency entries.
* [ ] Add SSI scaffolding: in-memory SIREAD maps for adjacency/index range markers; commit-time dangerous structure detection.
* [ ] Expand test coverage: high-degree nodes (>100 neighbors), parallel edge insert/delete, crash scenarios (after edge WAL before adj WAL, after page write before TxLog commit), long-running snapshots pinning GC.
* [ ] Implement hint bits (`HINT_XMIN_COMMITTED`, `HINT_XMAX_COMMITTED`) in GC to reduce TST lookup overhead.
* [ ] Add dual head pointer optimization (optional): `{latest_ptr, committed_ptr}` per entity for hot write paths.

---
