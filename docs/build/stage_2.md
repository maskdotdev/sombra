## Stage 2: Bytes/Checksum (S1) and IO + Pager(Cache) + Free‑List (S2)

### Part B — Stage 2: IO + Pager(Cache) + Free‑List


#### B0. Goals

* Cross‑platform **random‑access IO** (`read_at`/`write_at`).
* **Pager** with page cache (Clock‑Pro or 2Q), **pin/unpin**, dirty tracking.
* Persisted **Meta Page** and **Free‑List** pages to allocate/release pages.
* **No WAL yet**: direct writes to main file with `sync_all()` for now (WAL/locks arrive in Stage 3).

---

#### B1. Public APIs

**Crate `sombra-io`**

```rust
#![forbid(unsafe_code)]
use sombra_types::Result;

pub trait FileIo: Send + Sync + 'static {
    fn read_at(&self, off: u64, dst: &mut [u8]) -> Result<()>;
    fn write_at(&self, off: u64, src: &[u8]) -> Result<()>;
    fn sync_all(&self) -> Result<()>;
    fn len(&self) -> Result<u64>;
}

#[cfg(unix)]
pub mod stdio_unix { /* FileExt::read_at/write_at */ }
#[cfg(windows)]
pub mod stdio_win { /* FileExt::seek_read/seek_write */ }

pub struct StdFileIo { /* platform-specific impl */ }
impl FileIo for StdFileIo { /* ... */ }
```

**Crate `sombra-pager`** (core types)

```rust
use sombra_types::{PageId, Result};
use std::sync::Arc;

pub struct PageRef<'a> { pub id: PageId, pub data: &'a [u8] }
pub struct PageMut<'a> { pub id: PageId, pub data: &'a mut [u8] }

pub trait PageStore: Send + Sync {
    fn page_size(&self) -> u32;
    fn get_page(&self, id: PageId) -> Result<PageRef<'_>>;
    fn get_page_mut(&self, id: PageId) -> Result<PageMut<'_>>; // pins & marks dirty
    fn allocate_page(&self) -> Result<PageId>;                  // from free-list or append
    fn free_page(&self, id: PageId) -> Result<()>;              // records into free-list
    fn flush(&self) -> Result<()>;                              // writeback dirty pages
    fn sync(&self) -> Result<()>;                               // fsync file
    fn meta(&self) -> Result<Meta>;                             // read meta snapshot
}

#[derive(Clone, Debug)]
pub struct Meta {
    pub page_size: u32,
    pub salt: u64,
    pub format_version: u16,
    pub free_head: PageId,         // first FreeList page (0 = none)
    pub next_page: PageId,         // first unused PageId (append-end)
}
```

---

#### B2. On‑Disk Formats (Meta & Free‑List)

**Meta Page (page 0)** — serialized into the page body after header:

| Offset | Size | Field          | Notes             |
| -----: | ---: | -------------- | ----------------- |
|      0 |    8 | salt           | random at create  |
|      8 |    4 | page_size      | must equal header |
|     12 |    2 | format_version | 1                 |
|     14 |    2 | reserved       |                   |
|     16 |    8 | free_head      | PageId (0 = none) |
|     24 |    8 | next_page      | first unused id   |
|     32 |   32 | future roots   | placeholders      |
|   64.. |    … | reserved       | zeroed            |

* Meta page’s **crc32** covers header+payload (with crc zeroed).

**Free‑List Page** — a singly‑linked list of **extents** (runs of contiguous free pages).

| Offset | Size | Field                                                   |
| -----: | ---: | ------------------------------------------------------- |
|      0 |    8 | next_free_page (PageId, 0 = end)                        |
|      8 |    4 | n_extents                                               |
|     12 |    4 | reserved                                                |
|   16.. |    N | entries: repeated (start_page: u64, len: u32, pad: u32) |

* **Entry (16 bytes):** `start (u64) | len (u32) | pad (u32)` → 16‑byte aligned.
* Each Free‑List page holds `(page_size - header - 16)/16` entries.

**Why extents?**
They reduce metadata size and speed allocation/free by coalescing contiguous ranges, while keeping Stage 2 simple (no B‑tree required yet).

---

#### B3. Pager Design

**Cache algorithm:** **Clock‑Pro** (compact & robust).
**Data structures (in memory):**

* `HashMap<PageId, FrameIdx>` page table.
* `Vec<Frame>` frames with:

  * `buf: Box<[u8]>`  (page sized)
  * `state: Hot | Cold | Test`
  * `refcnt: u32` (pin count)
  * `dirty: bool`
  * `id: PageId`
* `clock_hand_hot`, `clock_hand_cold`, `target_cold`: standard Clock‑Pro pointers.

**Operations**

* `get_page(id)`:

  1. Lookup in table → hit: set reference bit (implicit), return `PageRef`.
  2. Miss: choose victim via Clock‑Pro (skipping `refcnt>0` frames), if `dirty` write back, load from disk, set `Cold`, insert into table.
* `get_page_mut(id)`:

  * Same as `get_page` plus `mark dirty` and increment `refcnt`.
* `allocate_page()`:

  1. Try from in‑memory free cache (fast path).
  2. If empty, **pull** one extent from on‑disk Free‑List page into memory cache.
  3. If no extents available, **append**: `pid = meta.next_page; meta.next_page += 1;` and **persist meta** on next `flush()`.
* `free_page(id)`:

  * Insert into **pending_free** set; on `flush()` coalesce into extents and write a Free‑List page if necessary (or update existing page), then mark cache frame as invalid if resident.

**Writeback & Sync**

* `flush()` writes **all dirty frames** to file at offset `id * page_size`.
* After data pages, write updated **Free‑List pages** and **Meta page** (page 0).
* `sync()` calls `io.sync_all()`.

> Stage 2 has **no WAL**, so correctness under crashes is limited to “best effort”. This is acceptable for Stage 2; Stage 3 adds WAL + recovery.

---

#### B4. File IO Details

* Use platform `FileExt` traits to avoid seek races:

  * Unix: `std::os::unix::fs::FileExt::{read_at, write_at}`.
  * Windows: `std::os::windows::fs::FileExt::{seek_read, seek_write}`.
* Open with `read | write | create` and exclusive mode when possible.
* Ensure the file size is grown to contain page 0 on create.

---

#### B5. Meta Lifecycle

* **Create DB**:

  1. Generate `salt` (rand 64‑bit).
  2. Initialize `free_head = 0`, `next_page = 1` (page 0 is meta).
  3. Write page 0 header + payload; compute crc32; write; `sync_all()`.
* **Open DB**:

  1. Read page 0; validate magic, version, crc; read `salt`, `next_page`, `free_head`.
  2. Initialize pager cache with configured capacity (page frames).

---

#### B6. Public Options & Stats (S2)

```rust
pub struct PagerOptions {
    pub page_size: u32,        // default 8192
    pub cache_pages: usize,    // number of frames
    pub prefetch_on_miss: bool, // optional
    pub synchronous: Synchronous, // Stage 3+ durability
    pub autocheckpoint_pages: usize,
    pub autocheckpoint_ms: Option<u64>,
    pub wal_commit_max_commits: usize,   // Stage 8.1 tuning
    pub wal_commit_max_frames: usize,
    pub wal_commit_coalesce_ms: u64,
}

pub struct PagerStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub dirty_writebacks: u64,
}
```

Expose `pager.stats()` and `pager.reset_stats()` for tests & tuning.

---

#### B7. Error Handling & Invariants

* `get_page`/`get_page_mut` validate page header (`magic`, `page_size`, `crc32` if enabled).
* Writes **always** update header fields consistently; `crc32` computed with its own field zeroed.
* `free_page(0)` is invalid (meta not freeable).
* `allocate_page()` must never return an id `< 1` or `>= meta.next_page` unless drawn from freelist.
* **When computing CRC:** use `sombra-checksum::page_crc32(page_no, salt, payload)`; header must contain `page_no` and `salt` already.

---

#### B8. Tests (Stage 2)

**Unit tests**

* Create/open DB; write/read an arbitrary page; verify round‑trip.
* Allocate/free thousands of random pages; persist/close/reopen; verify freelist persistence and coalescing.
* Cache tests: assert hits/misses under known access sequences (e.g., loop over working set > cache).

**Property tests**

* Random sequences of `alloc/free` that never free page 0; after persistence, all allocated pages are distinct and not in freelist.

**Corruption tests**

* Flip a byte in a page on disk; on next read, mismatch should raise `Corruption`.
* Truncate file mid‑page; read should error.

**Performance smoke**

* Fill page cache; stream‑read `N` pages; ensure evictions & writebacks counters make sense.

**Acceptance (Stage 2)**

* All tests green on Linux/macOS/Windows.
* Pager sustains at least `X` MB/s sequential write/read (set your internal baseline).
* Allocation from freelist vs append shows expected behavior in metrics.

---

#### B9. Implementation Sketches

**`sombra-io` (Unix example)**

```rust
#[cfg(unix)]
mod stdio_unix {
    use std::{fs::File, os::unix::fs::FileExt, sync::Arc};
    use super::FileIo;
    use sombra_types::{Result, SombraError};

    pub struct StdFileIo { f: Arc<File> }
    impl StdFileIo {
        pub fn open(path: &std::path::Path) -> Result<Self> {
            let f = std::fs::OpenOptions::new().read(true).write(true).create(true).open(path)?;
            Ok(Self { f: Arc::new(f) })
        }
    }
    impl FileIo for StdFileIo {
        fn read_at(&self, off: u64, dst: &mut [u8]) -> Result<()> { self.f.read_at(dst, off).map(|_|()).map_err(Into::into) }
        fn write_at(&self, off: u64, src: &[u8]) -> Result<()> { self.f.write_at(src, off).map(|_|()).map_err(Into::into) }
        fn sync_all(&self) -> Result<()> { self.f.sync_all().map_err(Into::into) }
        fn len(&self) -> Result<u64> { self.f.metadata().map(|m| m.len()).map_err(Into::into) }
    }
}
```

**Pager (frame & table snippets)**

```rust
struct Frame {
    id: PageId,
    buf: Box<[u8]>,
    dirty: bool,
    refcnt: u32,
    state: State, // Hot | Cold | Test
}
enum State { Hot, Cold, Test }

pub struct Pager<I: FileIo> {
    io: I,
    opts: PagerOptions,
    meta: Meta,
    table: hashbrown::HashMap<PageId, usize>,
    frames: Vec<Frame>,
    hand_hot: usize,
    hand_cold: usize,
    stats: PagerStats,
}

impl<I: FileIo> Pager<I> {
    pub fn get_page(&mut self, id: PageId) -> Result<PageRef<'_>> { /* lookup or load, set state */ }
    pub fn get_page_mut(&mut self, id: PageId) -> Result<PageMut<'_>> { /* mark dirty, pin */ }
    pub fn allocate_page(&mut self) -> Result<PageId> { /* freelist or append */ }
    pub fn free_page(&mut self, id: PageId) -> Result<()> { /* add to pending_free */ }
    pub fn flush(&mut self) -> Result<()> { /* write dirty frames, freelist, meta */ }
}
```

**Free‑List Manager (in pager)**

```rust
struct FreeCache { extents: std::collections::BinaryHeap<Extent> } // choose by length desc
#[derive(Copy, Clone, Eq, PartialEq)]
struct Extent { start: PageId, len: u32 }

impl FreeCache {
    fn alloc(&mut self) -> Option<PageId> { /* pop biggest extent; return start; push back if len>1 */ }
    fn free(&mut self, id: PageId) { /* coalesce if adjacent to existing extents */ }
}
```

**Persisting Extents to Free‑List Pages**

* On `flush()`, if `free_cache` changed:

  1. Serialize up to `K` extents per Free‑List page.
  2. Link them: write each page’s `next_free_page` to the next page id (or 0).
  3. Update `meta.free_head` to first page id.
  4. Write and `sync_all()` in Stage 2 (WAL will change ordering in Stage 3).

---

#### B10. Observability (Stage 2)

Provide `Debug`/`Display` for `Meta` and `PagerStats`. Add optional `tracing` spans:

* `pager.get_page` (hit/miss)
* `pager.evict`
* `pager.flush` (pages written, ms elapsed)

---

#### B11. Integration Points for Later Stages

* **WAL hook**: `flush()` and `get_page_mut()` sites are the only places that will change to “write WAL frame instead of main file”. Keep them centralized.
* **Locks**: Stage 3 will add a small lock file or region; Stage 2 code must not assume multi‑writer.
* **Async**: `FileIo` is sync; a future `FileIoAsync` can be added behind a feature flag without touching storage APIs.

---

#### B12. Step‑By‑Step Checklist

**Stage 2**

* [ ] Implement `sombra-io::StdFileIo` (Unix & Windows backends) + tests (read_at/write_at).
* [ ] Define page header helpers (write/read; compute/verify crc using `salt`).
* [ ] Implement `Meta` read/write; `create_db()` initializes page 0.
* [ ] Implement `FreeCache` (in‑memory extents) and serialize to Free‑List pages.
* [ ] Build `Pager`:

  * [ ] Page table & frames with Clock‑Pro state.
  * [ ] `get_page`/`get_page_mut` with pin/unpin and dirty bits.
  * [ ] `allocate_page` (freelist → append).
  * [ ] `free_page` (pending list → free cache).
  * [ ] `flush()` (dirty frames → disk; freelist pages; meta; then `sync()`).
* [ ] Tests: unit + property tests + corruption tests.
* [ ] Wire basic stats and `tracing` spans.

**Done** when all acceptance criteria in S1 and S2 are met.

---

### Appendix — Example: Page Header Encode/Decode

```rust
pub fn write_page_header(buf: &mut [u8], page_no: u64, kind: PageKind, page_size: u32, salt: u64) {
    use sombra_bytes::ord::*;
    buf[0..4].copy_from_slice(b"SOMB");
    buf[4..6].copy_from_slice(&1u16.to_be_bytes());       // version
    buf[6] = kind as u8;
    buf[7] = 0;
    buf[8..12].copy_from_slice(&page_size.to_be_bytes());
    buf[12..20].copy_from_slice(&page_no.to_be_bytes());
    buf[20..28].copy_from_slice(&salt.to_be_bytes());
    // crc at [28..32] filled after payload is written
}

pub fn finalize_page_crc(buf: &mut [u8]) {
    use sombra_checksum::Crc32Fast;
    let mut tmp = buf.to_vec();
    tmp[28..32].fill(0);
    let crc = {
        let mut h = Crc32Fast::default();
        h.update(&tmp);
        h.finalize()
    };
    buf[28..32].copy_from_slice(&crc.to_be_bytes());
}
```

---

### Appendix — Example: Free‑List Extent Coalescing

```rust
fn coalesce(extents: &mut Vec<Extent>) {
    extents.sort_by_key(|e| e.start.0);
    let mut out = Vec::with_capacity(extents.len());
    for e in extents.drain(..) {
        if let Some(last) = out.last_mut() {
            let last_end = last.start.0 + last.len as u64;
            if e.start.0 == last_end {
                last.len += e.len;
                continue;
            }
        }
        out.push(e);
    }
    *extents = out;
}
```
