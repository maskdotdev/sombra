# Inline Adjacency in Node Rows – Implementation Plan

You are helping optimize the Index-Free Adjacency (IFA) write path in the Sombra graph database. The goal is to make IFA “IfaOnly” adjacency inserts as fast as (or close to) the existing B-tree-based adjacency, especially for bulk edge inserts.

This document captures the current architecture, prior work, decisions made, and a concrete implementation plan.

---

## 1. Project Context

- **Database**: Sombra (Rust)
- **Feature**: Index-Free Adjacency (IFA) for graph edges
- **Alternative**: B-tree adjacency
- **Benchmark**: ~5000 edge inserts on a small graph

**Current numbers**:

- B-tree adjacency: ~118ms / 5000 edges
- IFA adjacency: ~950ms / 5000 edges (≈7–8x slower)
- Target: bring IFA within ≈90% of B-tree write speed

The core issue: write amplification in IFA due to storing adjacency in per-node adjacency pages with poor locality, vs. B-tree leaves which handle sorted bulk updates efficiently.

---

## 2. Relevant Files

Workspace-relative paths:

- **Node storage / MVCC**
  - `src/storage/node.rs`
  - `src/storage/mvcc.rs`
- **Adjacency / graph-level operations**
  - `src/storage/graph/adjacency_ops.rs`
- **IFA internals**
  - `src/storage/graph/ifa/adjacency.rs`
  - `src/storage/graph/ifa/types.rs`
  - `src/storage/graph/ifa/node_adj_page.rs`
  - `src/storage/graph/ifa/segment.rs`
- **B-tree implementation**
  - `src/storage/btree/tree/definition/api.rs`
  - `src/storage/btree/...` (general B-tree code)

---

## 3. Prior Work Already Done

Before this plan we made three important optimizations.

### 3.1 Removed meta_salt() bottleneck

- Previously, each adjacency page write called `meta_salt()` which:
  - Re-read page 0
  - Started a new read transaction
- We added a cached salt accessor on `SegmentManager` and used it in `ifa::adjacency` instead of rereading metadata.

Files:

- `src/storage/graph/ifa/segment_manager.rs`
- `src/storage/graph/ifa/adjacency.rs`

### 3.2 Inline-first IFA strategy inside adjacency pages

- We improved `insert_edges_batch_true_ifa_preallocated` to:
  - Try storing small numbers of edges per type inline in `NodeAdjHeader` using `InlineAdjEntry`.
  - Only allocate external segments (32-byte `AdjEntry`) when inline capacity is exceeded.
- This reduced page allocations for low-degree nodes but IFA remained ~7–8x slower than B-tree.

File:

- `src/storage/graph/ifa/adjacency.rs`

### 3.3 Batched tree-root persist

- We changed adjacency operations to avoid persisting B-tree roots per-node and instead batch persist at the end of the IFA insertion.

File:

- `src/storage/graph/adjacency_ops.rs`

Even with these, IFA is significantly slower than B-tree, hence the deeper redesign.

---

## 4. Existing Node Row Format

Node rows are encoded in `src/storage/node.rs` with an MVCC `VersionHeader` followed by a payload.

### 4.1 MVCC flags (src/storage/mvcc.rs)

```rust
pub mod flags {
    pub const TOMBSTONE: u16       = 0x0001;
    pub const PAYLOAD_EXTERNAL: u16 = 0x0002;
    pub const PENDING: u16         = 0x0004;
    pub const INLINE_HISTORY: u16  = 0x0008;
    pub const HAS_ADJ_PAGE: u16    = 0x0010;
    // New: HAS_INLINE_ADJ: 0x0020
}
```

`VersionHeader` includes `flags: u16` and `payload_len: u16`.

### 4.2 NodeRow and payload layout

Conceptual `NodeRow` today:

```rust
#[derive(Clone, Debug)]
pub struct NodeRow {
    pub labels: Vec<LabelId>,
    pub props: PropStorage,
    pub row_hash: Option<u64>,
    pub adj_page: Option<PageId>,
    // New: inline_adj: Option<InlineNodeAdj>
}
```

Payload encoding order (current):

1. `label_count: u8`
2. `labels: label_count * 4` bytes
3. `tag: u8` (storage kind + hash flag)
4. Properties (inline or `VRef` external)
5. Optional `row_hash: u64` if hash flag set
6. Optional `adj_page: u64` if `HAS_ADJ_PAGE` flag set
7. Optional `inline_history` if `INLINE_HISTORY` flag set

We will insert inline adjacency between `adj_page` and `inline_history`.

---

## 5. Existing IFA Inline Structures (in Adj Pages)

From `src/storage/graph/ifa/types.rs` and related:

- `InlineAdjEntry` (for header-level inline storage):
  - 16 bytes: neighbor (8) + edge (8)
- `AdjEntry` (for segment storage):
  - 32 bytes: neighbor (8) + edge (8) + xmin (8) + xmax (8)
- `TypeBucket` and `NodeAdjHeader` encode per-type inline buckets:
  - 6 buckets per node; each bucket can store 1–3 inline entries before spilling.
- `NodeAdjPage` layout:
  - Owner `NodeId`
  - `NodeAdjHeader` for OUT
  - `NodeAdjHeader` for IN

This inline machinery currently applies *inside adjacency pages*, not node rows.

---

## 6. New Design: Inline Adjacency in Node Rows

We introduce an inline adjacency representation directly in node rows, in the node B-tree. The design goals:

- Use node B-tree locality for low-degree nodes (turn edge inserts into node row updates).
- Maintain IFA’s existing external adjacency pages for heavy nodes.
- Promote from inline → external once a node exceeds a small inline capacity.
- Never automatically demote (avoid thrashing).

### 6.1 New MVCC flag: HAS_INLINE_ADJ

In `src/storage/mvcc.rs`:

```rust
pub const HAS_INLINE_ADJ: u16 = 0x0020; // Bit 5
```

This flag signals that the node row payload contains inline adjacency data.

### 6.2 New inline types in node.rs

We add a compact inline representation:

```rust
pub const DIR_OUT: u8 = 0;
pub const DIR_IN: u8  = 1;

/// Single inline adjacency entry (20 bytes)
/// Layout: [dir:1][type_id:3][neighbor:8][edge:8]
#[derive(Clone, Debug, PartialEq)]
pub struct InlineAdjEntry {
    pub direction: u8,    // 0=OUT, 1=IN
    pub type_id: u32,     // 24 bits used in encoding
    pub neighbor: NodeId,
    pub edge: EdgeId,
}

impl InlineAdjEntry {
    pub const ENCODED_LEN: usize = 20;

    pub fn encode(&self, buf: &mut [u8]) {
        buf[0] = self.direction;
        // Store type_id as 3 bytes (big-endian, drop high byte)
        buf[1..4].copy_from_slice(&self.type_id.to_be_bytes()[1..4]);
        buf[4..12].copy_from_slice(&self.neighbor.0.to_be_bytes());
        buf[12..20].copy_from_slice(&self.edge.0.to_be_bytes());
    }

    pub fn decode(buf: &[u8]) -> Self {
        let direction = buf[0];
        let type_id = u32::from_be_bytes([0, buf[1], buf[2], buf[3]]);
        let neighbor = NodeId(u64::from_be_bytes(buf[4..12].try_into().unwrap()));
        let edge = EdgeId(u64::from_be_bytes(buf[12..20].try_into().unwrap()));
        Self { direction, type_id, neighbor, edge }
    }
}

pub const MAX_INLINE_ADJ_ENTRIES: usize = 8;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct InlineNodeAdj {
    pub entries: Vec<InlineAdjEntry>,
}

impl InlineNodeAdj {
    pub fn new() -> Self { Self { entries: Vec::new() } }
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }
    pub fn len(&self) -> usize { self.entries.len() }

    pub fn needs_promotion(&self, additional: usize) -> bool {
        self.entries.len() + additional > MAX_INLINE_ADJ_ENTRIES
    }

    pub fn add(&mut self, entry: InlineAdjEntry) {
        self.entries.push(entry);
    }
}
```

We extend `NodeRow`:

```rust
#[derive(Clone, Debug)]
pub struct NodeRow {
    pub labels: Vec<LabelId>,
    pub props: PropStorage,
    pub row_hash: Option<u64>,
    pub adj_page: Option<PageId>,
    pub inline_adj: Option<InlineNodeAdj>, // NEW
}
```

### 6.3 Encoding / decoding inline adjacency

**Payload order (updated)**:

1. `label_count`
2. `labels[...]`
3. `tag` and props
4. optional `row_hash`
5. optional `adj_page` (if `HAS_ADJ_PAGE`)
6. optional `inline_adj` (if `HAS_INLINE_ADJ`)
7. optional `inline_history` (if `INLINE_HISTORY`)

**Encoding in `encode()`** (conceptual):

```rust
if let Some(inline_adj) = opts.inline_adj {
    if !inline_adj.is_empty() {
        version.flags |= flags::HAS_INLINE_ADJ;
        payload.push(inline_adj.entries.len() as u8);
        for entry in &inline_adj.entries {
            let mut buf = [0u8; InlineAdjEntry::ENCODED_LEN];
            entry.encode(&mut buf);
            payload.extend_from_slice(&buf);
        }
    }
}
```

**Decoding in `decode()`** (conceptual):

```rust
let inline_adj = if (header.flags & flags::HAS_INLINE_ADJ) != 0 {
    if offset >= payload.len() {
        return Err(SombraError::Corruption("node inline adj truncated"));
    }
    let count = payload[offset] as usize;
    offset += 1;

    let needed = count * InlineAdjEntry::ENCODED_LEN;
    if offset + needed > payload.len() {
        return Err(SombraError::Corruption("node inline adj entries truncated"));
    }

    let mut entries = Vec::with_capacity(count);
    for i in 0..count {
        let start = offset + i * InlineAdjEntry::ENCODED_LEN;
        let end = start + InlineAdjEntry::ENCODED_LEN;
        entries.push(InlineAdjEntry::decode(&payload[start..end]));
    }
    offset += needed;
    Some(InlineNodeAdj { entries })
} else {
    None
};
```

**EncodeOpts update**:

```rust
#[derive(Clone, Copy, Debug, Default)]
pub struct EncodeOpts<'a> {
    pub append_row_hash: bool,
    pub adj_page: Option<PageId>,
    pub inline_adj: Option<&'a InlineNodeAdj>, // NEW
}
```

---

## 7. Behavioral Decisions

### 7.1 Inline capacity & promotion

- `MAX_INLINE_ADJ_ENTRIES = 8`.
- When inserting edges for a node:
  - If `inline_adj.len() + new_edges.len() <= 8`, we stay inline.
  - Otherwise, we **promote**:
    - Allocate a new external adjacency page.
    - Move all inline entries + new incoming edges into this page.
    - Clear `inline_adj` (set to `None`).
    - Set `adj_page = Some(page_id)`.

### 7.2 No demotion (hysteresis)

- Once `adj_page` is set, we do not demote back to inline on the hot path.
- Nodes that become “small” again are still treated as external adjacency nodes.
- If needed, a future VACUUM/COMPACT process may handle demotion, but it is out of scope here.

### 7.3 Mutual exclusivity on reads

A node row should be in one of these states:

1. `inline_adj = Some(...)`, `adj_page = None`  → inline-only
2. `inline_adj = None`, `adj_page = Some(...)`  → external-only
3. `inline_adj = None`, `adj_page = None`       → no adjacency

With correct promotion logic, there is never a state with both inline and external data.

Read logic:

```rust
if let Some(inline) = &row.inline_adj {
    // Read from inline entries only (filter by direction)
} else if let Some(adj_page) = row.adj_page {
    // Read from external adjacency page only
} else {
    // No neighbors
}
```

No merging of inline + external is required.

---

## 8. B-tree API Constraints & Opportunities

From `src/storage/btree/tree/definition/api.rs` and related:

- `put_many` API:
  - Accepts an iterator of `PutItem<'a, K, V>`.
  - Requires items in **sorted key order**.
  - Internally uses a `LeafCache` to avoid retraversing the tree when consecutive keys fall into the same leaf.
- No `entry()`-style or write cursor API.
- `get_with_write()` and `put()` each do a separate B-tree traversal.

Therefore, per-node read-modify-write as `get_with_write() + put()` costs ~2N traversals. Using `put_many` on a sorted batch with pre-encoded node rows can significantly reduce traversals via `LeafCache`.

---

## 9. New Write Path for insert_adjacencies_true_ifa

We redesign `insert_adjacencies_true_ifa` in `src/storage/graph/adjacency_ops.rs` to:

1. Group edges by node using `BTreeMap` for sorted iteration.
2. Batch read all affected node rows via `get_with_write`, in sorted order.
3. Modify rows in-memory, applying inline adjacency or promotion.
4. Allocate adjacency pages and write external adjacency data as needed.
5. Batch write all node rows via `put_many`.

### 9.1 Group edges by node using BTreeMap

Instead of using `HashMap<NodeId, Vec<_>>`, we use:

```rust
use std::collections::BTreeMap;

enum EdgeOp {
    Out(NodeId, TypeId, EdgeId),
    In(NodeId, TypeId, EdgeId),
}

let mut node_edges: BTreeMap<NodeId, Vec<EdgeOp>> = BTreeMap::new();

for (src, dst, ty, edge) in entries {
    node_edges.entry(*src).or_default().push(EdgeOp::Out(*dst, *ty, *edge));
    node_edges.entry(*dst).or_default().push(EdgeOp::In(*src, *ty, *edge));
}
```

This ensures we process nodes in ascending `NodeId` order, aligning with B-tree key order and improving locality.

### 9.2 Batch read phase

```rust
let nodes_tree = self.nodes_tree();
let mut node_rows: BTreeMap<NodeId, VersionedNodeRow> = BTreeMap::new();

for node_id in node_edges.keys() {
    let row = nodes_tree
        .get_with_write(tx, node_id)?
        .ok_or_else(|| SombraError::NotFound("node not found"))?;
    node_rows.insert(*node_id, row);
}
```

Reads are also performed in sorted order, improving cache behavior.

### 9.3 In-memory modification phase

We walk `node_edges` and update `node_rows`:

- Path A: node already has `adj_page` → treat as external-only; queue ops for external append.
- Path B: node has inline and will exceed capacity → promotion.
- Path C: node has inline and stays under capacity → append inline.

Conceptual logic:

```rust
let mut promoted_nodes: Vec<(NodeId, Vec<InlineAdjEntry>, Vec<EdgeOp>)> = Vec::new();

for (node_id, ops) in &node_edges {
    let row = node_rows.get_mut(node_id).unwrap();

    if row.row.adj_page.is_some() {
        // Path A: already external; collect for external update
        promoted_nodes.push((*node_id, vec![], ops.clone()));
    } else {
        let inline = row.row.inline_adj.get_or_insert_with(InlineNodeAdj::new);

        if inline.needs_promotion(ops.len()) {
            // Path B: promotion
            let existing = std::mem::take(&mut inline.entries);
            row.row.inline_adj = None; // clear inline data (no ghosts)

            promoted_nodes.push((*node_id, existing, ops.clone()));
        } else {
            // Path C: stay inline
            for op in ops {
                inline.add(op.to_inline_entry());
            }
        }
    }
}
```

Where `EdgeOp::to_inline_entry()` converts an `EdgeOp` into an `InlineAdjEntry`:

```rust
impl EdgeOp {
    fn to_inline_entry(&self) -> InlineAdjEntry {
        match self {
            EdgeOp::Out(neighbor, ty, edge) => InlineAdjEntry {
                direction: DIR_OUT,
                type_id: ty.0,
                neighbor: *neighbor,
                edge: *edge,
            },
            EdgeOp::In(neighbor, ty, edge) => InlineAdjEntry {
                direction: DIR_IN,
                type_id: ty.0,
                neighbor: *neighbor,
                edge: *edge,
            },
        }
    }
}
```

### 9.4 Promotion & external adjacency updates

For each entry in `promoted_nodes`:

- If `adj_page` is `None`:
  - Allocate a new adjacency page via IFA APIs (e.g. `ifa.allocate_adj_page(tx, node_id)`).
  - Update `row.row.adj_page = Some(page_id)`.
  - Move `existing_inline` entries + `new_ops` into that page using an appropriate IFA write function.
- If `adj_page` is already `Some(page_id)`:
  - Append `new_ops` edges to that existing page.

Crucially, promotion logic always clears `inline_adj` for promoted nodes. There is never stale inline data after promotion.

### 9.5 Batch write via put_many

After modifying all `node_rows` and completing promotions:

```rust
let items: Vec<PutItem<NodeId, EncodedNodeRow>> = node_rows
    .iter()
    .map(|(id, row)| {
        let encoded = encode_node_row(row); // helper to call node::encode
        PutItem { key: id, value: &encoded }
    })
    .collect();

nodes_tree.put_many(tx, items)?;
```

Because `node_rows` is a `BTreeMap`, keys are sorted and `put_many` can use its internal `LeafCache` for efficient batched writes.

---

## 10. Read Path Changes

Read-side behavior for IFA adjacency ("true IFA" path):

1. Load node row from the nodes tree.
2. If `inline_adj.is_some()`:
   - Iterate over `inline_adj.entries`.
   - Filter by direction (`DIR_OUT` or `DIR_IN`).
   - Return neighbors.
3. Else if `adj_page.is_some()`:
   - Use existing IFA page read functions to fetch neighbors.
4. Else:
   - Return empty adjacency.

Example pattern:

```rust
pub fn get_neighbors(
    &self,
    tx: &ReadGuard,
    node_id: NodeId,
    dir: Dir,
) -> Result<Vec<(NodeId, TypeId, EdgeId)>> {
    let row = self.nodes_tree()
        .get(tx, &node_id)?
        .ok_or_else(|| SombraError::NotFound("node not found"))?;

    if let Some(inline_adj) = &row.row.inline_adj {
        let dir_byte = match dir { Dir::Out => DIR_OUT, Dir::In => DIR_IN };
        return Ok(
            inline_adj
                .entries
                .iter()
                .filter(|e| e.direction == dir_byte)
                .map(|e| (e.neighbor, TypeId(e.type_id), e.edge))
                .collect(),
        );
    }

    if let Some(adj_page) = row.row.adj_page {
        return self.ifa.read_adj_page(tx, adj_page, dir);
    }

    Ok(vec![])
}
```

---

## 11. Testing & Benchmarking

After implementation:

1. **Unit tests**:
   - `cargo test --lib`
2. **IFA-specific tests**:
   - `cargo test --test ifa_validation`
3. **Benchmark**:
   - `cargo run --release --bin ifa_bench`
   - Optionally filter output:
     ```bash
     cargo run --release --bin ifa_bench 2>&1 | \
       grep -E "(Operation|create_|full_graph|Speedup)"
     ```

We expect:

- IFA write performance to move from ~950ms towards ~100–150ms for the 5000-edge benchmark, i.e. much closer to the B-tree baseline (~118ms).
- Reads to be either unchanged (for promoted nodes) or slightly faster for low-degree nodes due to inline adjacency.

---

## 12. Implementation Checklist

1. Add `HAS_INLINE_ADJ` flag to `src/storage/mvcc.rs`.
2. Define `InlineAdjEntry` and `InlineNodeAdj` in `src/storage/node.rs`.
3. Extend `NodeRow` with `inline_adj: Option<InlineNodeAdj>`.
4. Update `EncodeOpts` to carry `inline_adj: Option<&InlineNodeAdj>`.
5. Implement encode/decode of inline adjacency (between `adj_page` and `inline_history`).
6. Switch `HashMap` → `BTreeMap` in `insert_adjacencies_true_ifa` grouping.
7. Implement the three-phase write path: batch read, in-memory modifications (inline/promotion), batch write via `put_many`.
8. Ensure promotion always clears `inline_adj` and sets `adj_page` (no ghost data).
9. Update read path to prefer `inline_adj` over `adj_page`, treating them as mutually exclusive.
10. Run tests and benchmarks, and iterate if needed.
