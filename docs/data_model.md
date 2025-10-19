# Graphite Data Model and Serialization

This specification defines the in-memory structures and on-disk serialization format for the Graphite MVP. The goal is to keep the design simple, deterministic, and future-proof while enabling efficient graph traversal.

## Core Identifiers

- `NodeId` and `EdgeId` are unsigned 64-bit integers (`u64`).
- IDs are allocated monotonically from counters stored in the database header.
- `0` is reserved as a sentinel meaning "no reference".

## Supported Property Types

Property values are serialized using a tagged, length-prefixed encoding. The MVP supports a focused set of types:

| Tag | Type        | Encoding details                                                      |
|-----|-------------|-----------------------------------------------------------------------|
| 0x01| Boolean     | Single byte: `0x00` for false, `0x01` for true                         |
| 0x02| Int64       | Signed 64-bit little-endian integer                                    |
| 0x03| Float64     | IEEE-754 double precision, little-endian                               |
| 0x04| String      | `u32` byte length (little-endian) followed by UTF-8 bytes              |
| 0x05| Bytes       | `u32` length + raw bytes                                               |

Future versions can extend the tag set without breaking compatibility.

## In-Memory Structures

### `Node`

```rust
pub struct Node {
    pub id: NodeId,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, PropertyValue>,
    pub first_outgoing_edge_id: EdgeId,
    pub first_incoming_edge_id: EdgeId,
}
```

- `labels` use `Vec<String>` to preserve insertion order and allow duplicates filtering at higher layers.
- `properties` uses `BTreeMap` to provide deterministic iteration order for serialization.
- `first_outgoing_edge_id` and `first_incoming_edge_id` default to `0` when no edges exist.

### `Edge`

```rust
pub struct Edge {
    pub id: EdgeId,
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub type_name: String,
    pub properties: BTreeMap<String, PropertyValue>,
    pub next_outgoing_edge_id: EdgeId,
    pub next_incoming_edge_id: EdgeId,
}
```

- `type_name` is stored as a UTF-8 string and serialized like a string property value.
- `next_*` pointers default to `0` when they terminate an adjacency list.

### `PropertyValue`

```rust
pub enum PropertyValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
}
```

## Record Serialization

All records are written in little-endian byte order. Each record is prefixed with a compact header that enables page scans.

### Generic Record Header

| Offset | Size | Field               | Notes                                                 |
|--------|------|---------------------|-------------------------------------------------------|
| 0      | 1    | record kind         | `0x00` Free slot, `0x01` Node, `0x02` Edge            |
| 1      | 3    | reserved            | Zero for MVP, reserved for future flags               |
| 4      | 4    | payload length (B)  | Length of payload, rounded up to 8-byte alignment     |

- The payload immediately follows the header.
- Payload length does not include the 8-byte header.
- Records are padded to an 8-byte boundary to keep alignment simple.

### Free Slot Payload

`RecordKind::Free` marks a slot that previously held a node or edge. The payload bytes are zeroed for hygiene, and the recorded payload length matches the slot capacity minus the 8-byte header. When inserting, the storage layer compares the padded payload length with the slot capacity to decide whether in-place reuse is possible. If it is, the entire slot (header + payload) is overwritten with the new record.

### Node Payload Layout

| Offset | Type                | Field                                  |
|--------|---------------------|----------------------------------------|
| 0      | `u64`               | `id`                                   |
| 8      | `u64`               | `first_outgoing_edge_id`               |
| 16     | `u64`               | `first_incoming_edge_id`               |
| 24     | `u32`               | label count `L`                        |
| 28     | variable            | label entries                          |
| ...    | `u32`               | property count `P`                     |
| ...    | variable            | property entries                       |

Label entry encoding:

1. `u32` byte length
2. UTF-8 bytes

Property entry encoding:

1. `u32` key byte length
2. UTF-8 key bytes
3. `u8` value tag (from table above)
4. Value payload (depends on tag)

### Edge Payload Layout

| Offset | Type    | Field                        |
|--------|---------|-----------------------------|
| 0      | `u64`   | `id`                        |
| 8      | `u64`   | `source_node_id`            |
| 16     | `u64`   | `target_node_id`            |
| 24     | `u64`   | `next_outgoing_edge_id`     |
| 32     | `u64`   | `next_incoming_edge_id`     |
| 40     | string  | `type_name` (length + bytes)|
| ...    | `u32`   | property count `P`          |
| ...    | variable| property entries            |

Strings and property entries use the same encoding as in the node payload.

## Validation Rules

- `labels` must be valid UTF-8 and non-empty when serialized.
- Property keys must be valid UTF-8, non-empty, and unique per node/edge.
- Property values must match the supported tag set.
- `source_node_id` and `target_node_id` must be non-zero.
- `next_outgoing_edge_id` and `next_incoming_edge_id` may be `0` or reference existing edges.
- The serialized payload must not exceed the host page's free space; the pager enforces this constraint.

## Deserialization Considerations

- Fail fast on malformed inputs (unexpected tags, length overruns) with descriptive errors.
- The deserializer should accept payloads with reserved header bits set to zero; future versions can introduce flags.
- Maintain deterministic ordering when reconstructing `labels` and `properties`.

## Backward Compatibility

- Reserved header bytes give room for flags such as "tombstone" or "overflow".
- Tags above `0x7F` are reserved for future value types (e.g., arrays, temporal types).
- New fields should append to payloads; existing offsets remain stable.

This document will guide the implementation of serializers, deserializers, and validation logic inside the storage layer.
