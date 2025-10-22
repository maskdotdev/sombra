# Sombra Architecture

This document describes the layered architecture of the Sombra graph database and how its components interact to provide ACID transactions, durability, and high performance.

## Overview

Sombra uses a layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                    API Layer                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Rust API  │  │ Python API  │  │ Node.js API │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                  Database Layer                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   GraphDB   │  │Transaction  │  │   Config    │         │
│  │             │  │   Manager   │  │             │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                   Index Layer                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ BTree Index │  │Label Index  │  │Property     │         │
│  │             │  │             │  │Index        │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                    Pager Layer                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ Page Cache  │  │  WAL Mgr    │  │Checkpoint   │         │
│  │             │  │             │  │   Manager    │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                  Storage Layer                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Pages     │  │   Records   │  │  Checksums  │         │
│  │             │  │             │  │             │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

## Layer Details

### API Layer

The API layer provides language-specific interfaces to the database:

- **Rust API**: Native Rust interface with full feature support
- **Python API**: Python bindings using PyO3 for Python integration
- **Node.js API**: Node.js bindings using NAPI for JavaScript/TypeScript

All APIs provide the same core functionality:
- Database opening and configuration
- Transaction management
- CRUD operations on nodes and edges
- Index management
- Health checks and metrics

### Database Layer

The database layer implements the core graph database logic:

#### GraphDB
- Main database interface
- Manages transactions and their lifecycle
- Coordinates between all other layers
- Provides high-level graph operations

#### Transaction Manager
- ACID transaction implementation
- Isolation between concurrent transactions
- Rollback and commit logic
- Dirty page tracking

#### Configuration
- Database behavior settings
- Performance tuning parameters
- Resource limits and safety constraints

### Index Layer

The index layer provides fast data access through multiple index types:

#### BTree Index
- Primary index for node and edge lookups by ID
- Ordered structure for range queries
- Persistent storage with crash recovery

#### Label Index
- Index nodes by their labels
- Supports fast label-based queries
- Maps label → set of node IDs

#### Property Index
- Index nodes by property values
- Supports equality queries on indexed properties
- Only indexable types (bool, int, string)

### Pager Layer

The pager layer manages memory and disk I/O:

#### Page Cache
- LRU cache of frequently accessed pages
- Configurable size for performance tuning
- Dirty page tracking for transactions

#### WAL Manager
- Write-Ahead Logging for durability
- Append-only log of all modifications
- Crash recovery through log replay

#### Checkpoint Manager
- Periodic flushing of dirty pages
- WAL truncation after successful checkpoint
- Configurable thresholds and intervals

### Storage Layer

The storage layer handles on-disk data representation:

#### Pages
- Fixed-size blocks (typically 4KB or 8KB)
- Basic unit of I/O and caching
- CRC32 checksums for corruption detection

#### Records
- Variable-length storage within pages
- Node and edge data serialization
- Free space management

#### Checksums
- CRC32 validation for all pages
- Corruption detection on read
- Configurable (can be disabled for benchmarks)

## Key Mechanisms

### Transaction Lifecycle

1. **Begin**: Transaction starts, dirty page tracking enabled
2. **Operations**: All modifications tracked in memory
3. **Commit**: 
   - Header updated with transaction ID
   - Changes written to WAL
   - Dirty pages marked for flushing
4. **Checkpoint**: Dirty pages flushed to main database file

### Crash Recovery

1. **Database Open**: Check if clean shutdown occurred
2. **WAL Replay**: If not clean, replay WAL frames
3. **Index Rebuild**: Rebuild in-memory indexes from disk
4. **Ready**: Database ready for operations

### Data Flow

```
Application Request
       ↓
   GraphDB API
       ↓
   Transaction
       ↓
   Index Layer
       ↓
   Pager Layer
       ↓
   Storage Layer
       ↓
   Disk I/O
```

## Performance Considerations

### Memory Usage

- **Page Cache**: Largest memory consumer, configurable
- **Index Memory**: BTree indexes in memory
- **Transaction Memory**: Dirty pages and operation tracking

### I/O Patterns

- **Sequential WAL**: Append-only writes are fast
- **Random Page Access**: Page cache mitigates disk latency
- **Checkpoint I/O**: Batched writes for efficiency

### Concurrency Model

Sombra implements a **multi-reader, single-writer** concurrency model using `RwLock` for optimal read performance:

- **Multiple Concurrent Readers**: Read operations can execute simultaneously without blocking each other
- **Single Writer**: Write operations (including transactions) require exclusive access
- **Lock Granularity**: Database-level locking via `Arc<RwLock<GraphDB>>`

#### Read vs Write Operations

**Read Operations** (acquire shared read lock):
- `get_node()`, `get_edge()`
- `get_node_properties()`, `get_edge_properties()`
- `find_nodes_by_label()`, `find_nodes_by_property()`
- `get_outgoing_edges()`, `get_incoming_edges()`
- Range queries and ordered iteration
- Graph traversals

**Write Operations** (acquire exclusive write lock):
- `create_node()`, `create_edge()`
- `update_node_properties()`, `update_edge_properties()`
- `delete_node()`, `delete_edge()`
- Transaction commit/rollback
- Index modifications
- Checkpoint operations

#### Transaction Isolation

Transactions hold an exclusive write lock for their entire duration:
1. **Begin**: Acquire write lock
2. **Operations**: All modifications tracked in memory
3. **Commit/Rollback**: Write to WAL, then release lock

This ensures **serializable isolation** - transactions are effectively executed one at a time.

#### Performance Characteristics

- **Read Throughput Scaling**: 3-4x improvement with 4 concurrent readers
- **No Read Blocking**: Readers never block other readers
- **Writer Priority**: Writers may need to wait for active readers to complete
- **Lock Contention**: Minimal for read-heavy workloads (typical graph queries)

#### Implementation Details

**Rust API**: Direct access to `Arc<RwLock<GraphDB>>`
**Python/Node.js Bindings**: Transparent `RwLock` wrapping with automatic lock acquisition

The RwLock choice optimizes for graph database query patterns, where reads typically outnumber writes 10:1 or more.

## Configuration Impact

### Sync Modes

- **Full**: Maximum durability, lowest performance
- **Normal**: Balanced approach with periodic fsync
- **Group Commit**: High throughput with batching
- **Off**: Maximum performance, no durability

### Cache Sizes

- **Page Cache**: Larger = better read performance
- **Transaction Limits**: Prevents excessive memory usage
- **WAL Thresholds**: Controls checkpoint frequency

## Safety Features

### Corruption Detection

- **Page Checksums**: Detects disk corruption
- **Header Validation**: Ensures database integrity
- **Record Bounds Checking**: Prevents buffer overflows

### Error Handling

- **Graceful Degradation**: Lock poisoning handled safely
- **Comprehensive Errors**: Detailed error messages
- **Recovery Options**: Repair tools for corruption

This architecture provides a solid foundation for a production-ready graph database with the right balance of performance, durability, and safety.