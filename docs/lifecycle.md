# Storage Lifecycle (Nodes & Edges)

```
create_node
  ↓
normalize labels & own props
  ↓
encode_property_map ── spill large → vstore.write → VRef
  ↓
build MVCC header (commit vs pending)
  ↓
node::encode(row bytes)
  ↓
allocate NodeId & update txn meta
  ↓
nodes.put (B-Tree)
  ├─ find_leaf_mut
  ├─ try_insert_leaf_in_place (leaf allocator: slots/free regions/compact)
  ├─ if full: split leaf → new page → update parents
  └─ persist tree root
  ↓
stage_label_inserts / insert_indexed_props (buffered for deferred flush)
  ↓
[if non-deferred] finalize_node_head (drop pending flag)
  ↓
commit phase
  ├─ flush_deferred_indexes (batched label/prop put_many → B-Tree inserts)
  ├─ pager collects dirty pages → WAL frames → fsync policy
  └─ persist root pointers

create_edge
  ↓
ensure_node_exists(src), ensure_node_exists(dst)  // B-Tree lookups on nodes
  ↓
encode_property_map ── spill large → vstore.write → VRef
  ↓
build MVCC header (commit vs pending)
  ↓
edge::encode(row bytes)
  ↓
allocate EdgeId & update txn meta
  ↓
edges.put (B-Tree insert/split like nodes) → persist edges root
  ↓
stage_adjacency_inserts (fwd src→dst, rev dst→src) with commit_id
  ↓
[if non-deferred] finalize_edge_head
  ↓
commit phase
  ├─ flush_deferred_writes (adjacency)
  │    ├─ encode+sort adj keys
  │    ├─ fwd put_many (B-Tree inserts/splits)
  │    └─ rev put_many (B-Tree inserts/splits)
  ├─ flush_deferred_indexes (labels/props) via put_many
  ├─ pager WAL commit (dirty pages → WAL frames → fsync policy)
  └─ persist root pointers

Shared B-Tree internals
  put / put_many
    ├─ find_leaf_mut (reuse leaf cache when possible)
    ├─ insert_into_leaf
    │    ├─ encode record (key/value)
    │    ├─ binary search slots
    │    ├─ leaf allocator insert (may compact)
    │    └─ if overflow: split leaf (allocate page, redistribute, update parents)
    └─ propagate splits up the tree as needed

Value store (vstore)
  - write: store large prop blobs, returns VRef
  - read: load by VRef
  - free on error/overwrite paths

MVCC/versioning
  - Rows carry commit_id/flags/pointers
  - Deferred-flush path writes committed headers; non-deferred uses pending then finalizes
```
