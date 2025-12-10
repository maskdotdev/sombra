# Graphite Page Layout and Allocation Strategy

This document describes how Graphite organizes data within fixed-size pages for the MVP release. The design keeps the layout simple while already supporting free-list driven page recycling and leaving room for future features such as overflow pages and WAL integration.

## Page Size and Addressing

- Default page size: 8192 bytes (configurable in code later).
- Pages are identified by a zero-based `PageId` (`u32`).
- Page 0 is reserved for the database header. All other pages may store records.

## Page Types (MVP)

1. **Header Page (Page 0)**
2. **Record Pages (Pages ≥ 1)**

Future versions can introduce dedicated free-list, overflow, or index pages.

## Header Page Layout

The first 64 bytes of page 0 store metadata required to bootstrap the database. Remaining bytes are reserved.

| Offset | Size | Field                      | Notes                                   |
|--------|------|---------------------------|-----------------------------------------|
| 0      | 8    | Magic value `b"GRPHITE\0"`| Identifies file format                  |
| 8      | 2    | Format major version      | Start at `0x0001`                       |
| 10     | 2    | Format minor version      | Start at `0x0000`                       |
| 12     | 4    | Page size                 | Stored as `u32`, must match runtime     |
| 16     | 8    | Next node id counter     | `u64`, first allocation uses this value |
| 24     | 8    | Next edge id counter     | `u64`                                   |
| 32     | 4    | Free page head            | `PageId`; `0` indicates none            |
| 36     | 4    | Last record page          | `PageId`; `0` indicates no records yet  |
| 40     | 24   | Reserved                  | Allow future metadata without migration |

All header integers are stored little-endian.

`Last record page` is maintained as an insertion hint so the storage layer can try packing subsequent records into the most recently used page before allocating a new one.

## Record Page Structure

Each record page uses a slotted layout to pack variable-sized records while enabling in-place updates.

```
+----------------------------+
| Page Header (16 bytes)     |
+----------------------------+
| Record Directory (down)    |
| ...                        |
+----------------------------+
| Free Space (grows/shrinks) |
+----------------------------+
| Record Payloads (up)       |
| ...                        |
+----------------------------+
```

### Record Page Header

| Offset | Size | Field             | Notes                                   |
|--------|------|-------------------|-----------------------------------------|
| 0      | 2    | Record count      | `u16`, number of directory entries      |
| 2      | 2    | Free space offset | `u16`, start of free region from top    |
| 4      | 4    | Free list next    | `PageId`; `0` indicates tail            |
| 8      | 8    | Reserved          | For per-page generation numbers, etc.   |

After the header comes the **Record Directory**, an array of `u16` offsets (entry per record) pointing to payload starts relative to the page base. Directory entries are stored consecutively; entry `i` corresponds to record `i`.

### Record Placement

- Record payloads grow downward from the end of the page.
- Directory entries grow upward immediately after the header.
- Free space resides between directory and payload regions.

**Insertion Algorithm (MVP)**

1. Compute record size: `RECORD_HEADER_SIZE + payload_length`, rounded up to the next 8-byte multiple.
2. Ensure `free_space >= record_size + 2` (extra `u16` for directory offset).
3. Decrement `free_space_offset` by `record_size`, write record starting there.
4. Append new `u16` offset to record directory, increment record count.

**Deletion**

- Records are deleted in place by rewriting the record header to `RecordKind::Free` and zeroing the payload bytes.
- Free slots retain their original directory entry and capacity. When inserting, the storage layer first searches for a free slot large enough to hold the incoming record. If found, it overwrites the slot without moving other records.
- If no free slot is large enough, the engine appends to the page as before. Only when the page has no remaining free space does it allocate a new page.

## Free Page Management

- The header maintains `free_page_head`, a singly-linked list of reusable pages.
- After every deletion the storage layer counts live records on the page. When it reaches zero, the page is cleared, linked into the header free list, and its directory/payload regions are reset.
- During insertion the engine first tries to place the record on a preferred page (typically the last one that received an insert). If that fails, it pops a page from the free list, reinitializes it, and attempts to insert there before allocating a fresh page.
- Pages linked through `free_list_next` may be reused multiple times; the list persists across restarts because the header is flushed with the database.

## Record Lookup

- To scan a page, iterate over the directory (0..record_count):
  1. Read offset `o`.
  2. Read `RecordHeader` at `o`.
  3. Use header to interpret payload.

Directory entries remain valid even if records are updated in place (as long as new size ≤ existing size). Larger updates require copy-on-write semantics, deferred to later phases.

## Overflow Handling

- MVP rejects records that exceed page free space.
- Future design: allow overflow pages chained per record, or external property store for large payloads.

## Concurrency and Consistency Notes

- Pager is responsible for marking pages dirty when modified.
- WAL and fine-grained locking are future work; current layout assumes single-writer semantics.

This layout balances implementation simplicity with enough structure to evolve toward a more robust storage engine.
