# Sombra Architecture

This document describes the architecture of the Sombra graph database and how its components interact to provide ACID transactions, MVCC isolation, and high performance.

## Overview

Sombra is an embedded, single-file graph database written in Rust with bindings for Node.js and Python. It uses a layered architecture inspired by SQLite:

```
┌─────────────────────────────────────────────────────────────────┐
│                      Language Bindings                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  Node.js    │  │   Python    │  │   Rust (native)         │  │
│  │  (napi-rs)  │  │   (PyO3)    │  │                         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                        FFI Layer                                │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Database    │  QueryStream  │  CreateBuilder  │ Errors │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                     Query Engine (Stage 8)                      │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐    │
│  │    AST    │  │  Planner  │  │ Executor  │  │  Builder  │    │
│  │           │→ │ (logical/ │→ │ (streaming│  │  (fluent) │    │
│  │           │  │ physical) │  │  results) │  │           │    │
│  └───────────┘  └───────────┘  └───────────┘  └───────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                      Storage Engine                             │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐    │
│  │   Graph   │  │  B-Tree   │  │   Index   │  │   MVCC    │    │
│  │  (nodes,  │  │  (sorted  │  │  (label,  │  │ (versions,│    │
│  │   edges)  │  │   keys)   │  │  property)│  │  commits) │    │
│  └───────────┘  └───────────┘  └───────────┘  └───────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                       Pager Layer                               │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐    │
│  │   Page    │  │    WAL    │  │Checkpoint │  │  Freelist │    │
│  │   Cache   │  │ (segments)│  │  Manager  │  │           │    │
│  └───────────┘  └───────────┘  └───────────┘  └───────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                     File System                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  database.sombra  │  database.sombra-wal/  (segments)   │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Source Code Layout

```
sombra-db/
├── src/
│   ├── ffi/              # High-level FFI for bindings (Database, queries)
│   ├── query/            # Stage 8 query engine
│   │   ├── ast.rs        # Query abstract syntax tree
│   │   ├── builder.rs    # Fluent query builder
│   │   ├── planner.rs    # Logical → physical planning
│   │   ├── executor.rs   # Streaming query execution
│   │   └── analyze.rs    # Query normalization
│   ├── storage/          # Storage engine
│   │   ├── btree/        # B-tree implementation
│   │   ├── index/        # Label & property indexes
│   │   ├── catalog/      # Dictionary (labels, types, props)
│   │   ├── vstore/       # Overflow value storage
│   │   ├── graph.rs      # Graph operations
│   │   ├── mvcc.rs       # MVCC version control
│   │   └── adjacency.rs  # Edge adjacency lists
│   ├── primitives/
│   │   ├── pager/        # Page cache and I/O
│   │   └── wal/          # Write-ahead logging
│   ├── admin/            # CLI admin commands
│   └── cli/              # CLI interface
├── bindings/
│   ├── node/             # Node.js bindings (napi-rs)
│   └── python/           # Python bindings (PyO3)
└── packages/
    ├── api-server/       # Dashboard REST API (Axum)
    └── dashboard/        # React dashboard UI
```

## Layer Details

### Language Bindings

All language bindings expose the same core functionality through the FFI layer:

- **Node.js** (`sombradb` on npm): Uses napi-rs for native bindings with TypeScript definitions. Provides a fluent query builder, typed schema support, and async streaming.

- **Python** (`sombra` on PyPI): Uses PyO3 for native bindings. Mirrors the Node.js API with Pythonic naming conventions.

- **Rust**: Direct access to the core library via the `sombra` crate.

### FFI Layer (`src/ffi/`)

The FFI layer provides a safe, high-level interface for language bindings:

```rust
pub struct Database {
    graph: Graph,
    pager: Arc<Pager>,
    dict: Dict,
    // ...
}

impl Database {
    pub fn open(path: impl AsRef<Path>, opts: DatabaseOptions) -> Result<Self>;
    pub fn execute_json(&self, spec: &Value) -> Result<Value>;
    pub fn create_node(&self, labels: &[&str], props: &Value) -> Result<u64>;
    pub fn query_stream(&self, spec: &Value) -> Result<QueryStream>;
    // ...
}
```

Key responsibilities:

- JSON serialization/deserialization of queries and results
- Type conversion between language-native types and storage types
- Error translation to language-specific exceptions
- Request cancellation via request IDs
- Connection pooling and resource management

### Query Engine (`src/query/`)

The Stage 8 query engine processes queries through multiple phases:

#### AST (`ast.rs`)

Defines the query structure:

```rust
pub struct QueryAst {
    pub schema_version: u32,
    pub request_id: Option<String>,
    pub matches: Vec<MatchClause>,    // Node patterns
    pub edges: Vec<EdgeClause>,       // Edge traversals
    pub predicate: Option<BoolExpr>,  // Filter conditions
    pub projections: Vec<Projection>, // Output fields
    pub distinct: bool,
}
```

#### Planner (`planner.rs`)

Converts AST to executable plans:

1. **Logical Plan**: Describes what to compute
2. **Physical Plan**: Describes how to compute it (index scans, filters, joins)

#### Executor (`executor.rs`)

Executes physical plans with streaming output:

- Returns results as an iterator of rows
- Supports early termination and cancellation
- Enforces row limits for streaming safety

### Storage Engine (`src/storage/`)

#### Graph (`graph.rs`)

Core graph operations:

- Node CRUD with multi-label support
- Edge CRUD with typed relationships
- Adjacency list traversal (forward/reverse)
- BFS traversal with depth limits

#### B-Tree (`btree/`)

Persistent B-tree for sorted key-value storage:

- 8 KiB pages with variable-length records
- Leaf-level compression and allocation caching
- Used for node/edge storage and indexes

#### Index System (`index/`)

Multiple index types for fast lookups:

| Index Type     | Purpose                      | Implementation             |
| -------------- | ---------------------------- | -------------------------- |
| Label Index    | Find nodes by label          | Chunked postings lists     |
| Property Index | Find nodes by property value | B-tree with value encoding |
| Type Index     | Find edges by type           | Chunked postings lists     |

#### MVCC (`mvcc.rs`)

Multi-Version Concurrency Control:

- Snapshot isolation for readers
- Version chains for historical data
- Commit table for transaction coordination
- Garbage collection of old versions

#### Catalog (`catalog/`)

Dictionary for string interning:

- Maps strings ↔ integer IDs for labels, types, property names
- Reduces storage overhead and speeds comparisons

### Pager Layer (`src/primitives/pager/`)

#### Page Cache

LRU cache of 8 KiB pages:

- Configurable cache size
- Dirty page tracking
- Reference counting for pinned pages

#### WAL (Write-Ahead Logging)

Segmented WAL for durability:

```
database.sombra-wal/
├── 00000001.wal
├── 00000002.wal
└── 00000003.wal
```

Features:

- Group commit for batching writes
- Async fsync option for performance
- Automatic segment rotation
- Checkpoint triggers (time or frame count)

#### Checkpoint Manager

Flushes WAL to main database file:

- Force or best-effort modes
- Concurrent with reads
- WAL truncation after checkpoint

## Key Mechanisms

### Query Execution Flow

```
JSON Query Spec
      │
      ▼
┌─────────────┐
│   Analyze   │  Validate, normalize, extract predicates
└─────────────┘
      │
      ▼
┌─────────────┐
│   Planner   │  Choose indexes, build execution plan
└─────────────┘
      │
      ▼
┌─────────────┐
│  Executor   │  Stream results from storage
└─────────────┘
      │
      ▼
JSON Result Rows
```

### Transaction Model

Sombra uses MVCC for concurrent access:

1. **Readers**: Acquire a snapshot; see consistent view of data
2. **Writers**: Exclusive write lock; create new versions
3. **Commits**: Atomic visibility via commit table

```rust
// Concurrent reads don't block
let snapshot = db.begin_read()?;
let node = snapshot.get_node(id)?;

// Writes are serialized
let writer = db.begin_write()?;
writer.create_node(&["User"], &props)?;
writer.commit()?;
```

### Crash Recovery

1. Open database file
2. Check for dirty shutdown flag
3. Replay WAL segments in order
4. Rebuild in-memory indexes
5. Clear dirty flag

### Data Flow for Writes

```
createNode(labels, props)
        │
        ▼
┌───────────────┐
│ Allocate IDs  │  Node ID, property IDs from sequences
└───────────────┘
        │
        ▼
┌───────────────┐
│ Encode Record │  Serialize to B-tree key/value
└───────────────┘
        │
        ▼
┌───────────────┐
│ Update Index  │  Add to label index, property indexes
└───────────────┘
        │
        ▼
┌───────────────┐
│  WAL Write    │  Append to current WAL segment
└───────────────┘
        │
        ▼
┌───────────────┐
│    fsync      │  Durability guarantee (if sync mode)
└───────────────┘
```

## Performance Characteristics

### Memory Usage

| Component     | Typical Size | Notes                          |
| ------------- | ------------ | ------------------------------ |
| Page Cache    | 32-256 MB    | Configurable via `cachePages`  |
| WAL Buffer    | 1-4 MB       | Buffered before fsync          |
| Index Cache   | ~10% of data | Label/property posting lists   |
| Query Buffers | Per-query    | Streaming keeps memory bounded |

### I/O Patterns

- **WAL**: Sequential append (fast)
- **B-tree reads**: Random access (page cache mitigates)
- **Checkpoint**: Sequential write of dirty pages
- **Index scans**: Sequential within posting lists

### Throughput (typical developer machine)

| Operation            | Throughput        |
| -------------------- | ----------------- |
| Point reads          | ~20,000/sec       |
| Node + edge creation | ~9,000/sec        |
| Bulk import          | ~50,000 nodes/sec |
| Query (indexed)      | ~10,000/sec       |

## Configuration Options

### Sync Modes

| Mode     | Durability           | Performance                |
| -------- | -------------------- | -------------------------- |
| `full`   | Every commit fsynced | Slowest, safest            |
| `normal` | Periodic fsync       | Balanced                   |
| `off`    | No fsync             | Fastest, risk of data loss |

### Key Options

```typescript
Database.open(path, {
    createIfMissing: true,
    pageSize: 8192, // 8 KiB pages
    cachePages: 4096, // 32 MB cache
    synchronous: "normal",
    autocheckpointMs: 5000, // Checkpoint every 5s
    groupCommitMaxWaitMs: 10, // Batch commits for 10ms
});
```

## Safety Features

### Data Integrity

- CRC32 checksums on all pages
- WAL checksums for recovery validation
- Header validation on open

### Error Handling

- All errors surfaced as typed exceptions
- No silent data corruption
- Graceful degradation on resource exhaustion

### Resource Limits

- Maximum payload size (8 MiB)
- Streaming row limits (1,000 per batch)
- Query cancellation via request IDs
