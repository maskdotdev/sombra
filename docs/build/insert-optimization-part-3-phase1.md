# Incremental Leaf Allocator – Phase 1 Design

This document delivers Phase 1 of `build/insert-optimization-part-3.md`: a concrete design for replacing `build_leaf_layout` rebuilds with an always-on incremental allocator. The intent is to unblock implementation work (Phase 2) and reviews by spelling out the storage model, metadata, fence rules, and APIs we will ship.

## Objectives & Guardrails
- **Eliminate rebuild-only fallbacks** on the hot insert/delete paths in `src/storage/btree/tree/definition/leaf.rs:152` without changing the on-disk format.
- **Preserve existing corruption detectors** (`SlotExtents`, fence overlap checks, free pointer invariants) and add new ones when we introduce allocator metadata.
- **Keep the allocator heap-free** so every edit reuses stack storage (`SmallVec`) and the page buffer; no `Vec` sized to `page_size`.
- **Make compaction deterministic** so repeated operations produce identical layouts (good for WAL diffs and tests).

Non-goals for this phase: instrumentation counters, mutation rewrites, or split/merge changes (they land in Phases 2–4).

## Per-Page Storage Model

We continue using the existing physical layout:

```
[ Stage-1 header ][ BTree header ][ low/high fences ][ record arena ][ padding ][ slot dir ]
                    ^             ^                  ^               ^          ^
                    |             |                  |               |          |
                    |             |                  |               |       payload.len()
                    |             |                  |         free_end ----+
                    |             |               arena_lo                   |
                    |          fences_end                                     |
                 PAYLOAD_HEADER_LEN                                    slot_count*4 bytes
```

The allocator reinterprets the area between `arena_lo = PAYLOAD_HEADER_LEN + low_len + high_len` and `arena_hi = header.free_end` as a **log-structured arena**:

1. Slot directories stay sorted by key order; offsets continue to point inside the arena.
2. Records may leave *holes* anywhere in the arena after deletes/overwrites. Those holes are tracked as `FreeRegion`s.
3. The allocator services requests in three passes:
   - **Reuse existing slot extent** when overwriting and the encoded record still fits.
   - **Place into an existing hole** (first-fit, then best-fit) by copying bytes downward/upward with `copy_within`.
   - **Pack-and-reserve**: deterministically stream all live records toward `arena_lo`, optionally leaving a gap at the insertion point. This is the universal fallback that replaces `build_leaf_layout`.

Because gaps can exist before *any* slot, the allocator must be able to move a suffix of the arena while keeping key order intact. Pack-and-reserve achieves this without an auxiliary buffer: we iterate slots in key order, keep a running cursor, and use `payload.copy_within(old_start..old_end, cursor)` to close each gap. When an insertion requests `N` bytes at slot `i`, we simply advance the cursor by `N` before moving slot `i` and continue streaming the remainder; the cursor ends exactly at the point where `free_start` should land.

## Metadata Structures (heap-free)

```rust
const INLINE_SLOTS: usize = 192;          // ~3/4 of a 16 KiB leaf
const INLINE_FREE_REGIONS: usize = 24;    // enough for worst observed fragmentation

pub struct LeafAllocator<'page, 'scratch> {
    header: page::Header,
    payload: &'page mut [u8],
    slots: page::SlotDirectory<'page>,
    extents: page::SlotExtents,
    arena: LeafArena,
    slot_meta: SmallVec<[SlotMeta; INLINE_SLOTS]>,
    free_regions: SmallVec<[FreeRegion; INLINE_FREE_REGIONS]>,
    dirty_slots: SmallVec<[usize; INLINE_SLOTS]>,
    stats: LeafAllocatorStats<'scratch>,
}

struct LeafArena {
    start: u16, // arena_lo
    end: u16,   // header.free_end before slot dir
}

struct SlotMeta {
    slot_idx: usize,
    start: u16,
    len: u16,
    key_hash: u32, // quick equality check when overwriting
}

struct FreeRegion {
    start: u16,
    end: u16, // exclusive
}

struct LeafAllocatorStats<'a> {
    compactions: u32,
    bytes_moved: u32,
    payload_scratch: &'a mut SmallVec<[u8; 256]>, // reused for encoding new records
}
```

Key points:
- `LeafAllocator::build(page)` borrows the mutable payload once, constructs a `SlotView` (which already caches `SlotExtents` from Part 2), and derives both `slot_meta` and `free_regions` by scanning the ordered extents. This scan also re-validates monotonic slots/fences; any overlap or OOB extent is immediately mapped to `SombraError::Corruption`.
- `free_regions` are stored sorted by `start`. Adjacent free regions are coalesced to keep the inline capacity bounded. If fragmentation ever exceeds `INLINE_FREE_REGIONS`, we fall back to pack-and-reserve immediately (the page is effectively full).
- `dirty_slots` records which slot indices need new offsets after an edit. This lets us avoid rewriting the entire slot directory when we only touched a suffix.
- `LeafAllocatorStats` lives in a scratch buffer owned by `BTree` so Phase 2 instrumentation can read it; in Phase 1 we only increment counters for validation.

### Allocation workflow

```
LeafAllocator::prepare_insert(key, value) -> LeafEditPlan
LeafAllocator::commit(plan) -> LeafEditOutcome
```

1. Encode the record (`plain_leaf_record_encoded_len` + `encode_leaf_record`) into `stats.payload_scratch`.
2. Binary-search `slot_meta` for the insertion point. If the key exists, we branch to overwrite handling (no rebuilding).
3. Call `allocator.allocate(bytes_needed, insert_idx)`:
   - **Overwrite fits**: reuse the same extent, adjust `len` if the encoded length shrinks, and track the freed tail as a new `FreeRegion`.
   - **Hole fits**: pick the first region `>= bytes_needed`, move the bytes that currently occupy that region (if any) to open the gap, and return the start offset.
   - **Pack-and-reserve**: stream through slot order to close all holes while leaving `bytes_needed` at `insert_idx`.
4. Update `slot_meta` with the new offset/len and push `slot_idx` to `dirty_slots`.
5. `commit(plan)` copies the encoded bytes into place, writes the slot directory entries listed in `dirty_slots`, and recomputes `free_start` (`arena.start + total_live_bytes + reserved_gap`). `free_end` stays `arena.end` minus `slot_count * 4`.

All mutations happen against the page buffer; no temporary Vec sized to the entire payload is needed.

## Fence Maintenance Rules

Fences live ahead of the arena, so any length change shifts `arena.start`. The allocator takes ownership of fence updates so the mutation paths no longer need bespoke checks like the length equality guard in `try_insert_leaf_in_place` (`src/storage/btree/tree/definition/leaf.rs:187`).

Rules:

1. **Low fence** always matches the first key in the leaf.
   - `LeafAllocator::update_low_fence(new_key)` writes the bytes via `page::set_low_fence` and recalculates `arena.start`.
   - If `new_key.len()` increases, we ensure `free_start - arena.start` has enough slack. If not, we run pack-and-reserve to push records downward before rewriting the fence.
2. **High fence** mirrors either the parent separator or stays empty on the rightmost leaf.
   - `update_high_fence` only runs when splits/merges adjust it. The allocator treats it the same as low fence: rewrite bytes, recompute `arena.start`, and, if needed, compact before committing.
3. **Fence writes happen before record moves** so `arena.start` is stable while we copy records.
4. **Validation**: every allocator build rechecks that `arena.start <= header.free_start <= header.free_end <= payload.len()` and that no record extent overlaps `FENCE_DATA_OFFSET..arena.start`. Any violation converts to `SombraError::Corruption`.

By centralizing fence updates, mutation paths can simply pass `FenceIntent` hints:

```rust
enum FenceIntent<'a> {
    NoChange,
    Low(&'a [u8]),          // replace low fence with `new_key`
    LowHigh { low: &'a [u8], high: &'a [u8] },
}
```

`LeafAllocator::commit` consumes the intent, applies fence writes, recomputes `arena.start`, and ensures the reserved gap for inserts still lines up with the first slot’s new position.

## API Sketches & Invariants

```rust
impl<'page, 'scratch> LeafAllocator<'page, 'scratch> {
    pub fn build(page: &'page mut PageMut<'_>, header: &page::Header, scratch: &'scratch mut LeafScratch)
        -> Result<Self>;

    pub fn prepare_insert(&mut self, key: &[u8], val: &[u8]) -> Result<LeafEditPlan>;
    pub fn prepare_delete(&mut self, key: &[u8]) -> Result<LeafEditPlan>;
    pub fn prepare_overwrite(&mut self, key: &[u8], val: &[u8]) -> Result<LeafEditPlan>;

    pub fn commit(&mut self, plan: LeafEditPlan, fences: FenceIntent) -> Result<LeafEditOutcome>;
}

pub enum LeafEditPlan {
    Insert { slot_idx: usize, record_len: u16, offset: u16 },
    Overwrite { slot_idx: usize, old_len: u16, new_len: u16, offset: u16 },
    Delete { slot_idx: usize, freed: FreeRegion },
}

pub struct LeafEditOutcome {
    pub slot_idx: usize,
    pub new_first_key: Option<Vec<u8>>,
    pub page_full: bool,              // true when pack-and-reserve still could not make room
    pub stats: LeafAllocatorStats,
}
```

Invariants enforced on every build/commit:

- `slot_meta` stays sorted by key order and by on-page offset after we pack/allocate.
- `free_regions` are disjoint and never overlap with any slot extent.
- `arena.start >= PAYLOAD_HEADER_LEN + low_len + high_len` and `arena.start <= slot_meta[i].start` for every slot.
- `free_start` always equals `arena.start + sum(slot_meta.len)` (i.e., no phantom bytes between `free_start` and the last record).
- `free_end` equals `payload.len() - slot_count * SLOT_ENTRY_LEN`.
- Pack-and-reserve is idempotent: running it twice without other mutations produces the same layout, which simplifies corruption checks during fuzzing.

If any invariant fails we emit `SombraError::Corruption` so fuzz/property tests can flag regressions immediately in Phase 2.

## Integration Notes

- `LeafAllocator` lives in a new module (e.g., `src/storage/btree/tree/definition/leaf_allocator.rs`) and is reused by inserts, deletes, borrows, and split-prep.
- `try_insert_leaf_in_place` shrinks to the thin wrapper that builds the allocator, calls `prepare_insert`, and commits with `FenceIntent::Low(new_first_key)` when inserting before slot 0.
- Delete/borrow helpers reuse the same allocator so they can recycle freed space instead of calling `build_leaf_layout` (handled in later phases).
- Phase 2 will plug in instrumentation via `LeafAllocatorStats` (counts for compactions, bytes moved, allocator failures).

With this design in place we can implement Phase 2 directly: the allocator gives us a deterministic, heap-free way to service any mutation without falling back to `build_leaf_layout`, satisfying the Part 3 goals while preserving the existing on-disk format.
