# Sombra Core

Core Rust library for Sombra, a high-performance graph database.

> **Note:** This is alpha software under active development. APIs may change between minor versions.

## Overview

This package contains the core graph database implementation for Sombra. It provides the foundational data structures, storage engine, and graph algorithms that power the Node.js and Python bindings.

## Features

- **Property Graph Model**: Nodes, edges, and flexible properties
- **Single File Storage**: SQLite-style database files
- **ACID Transactions**: Full transactional support with rollback
- **MVCC (Multi-Version Concurrency Control)**: Snapshot isolation for concurrent reads/writes
- **Write-Ahead Logging**: Crash-safe operations
- **Page-Based Storage**: Efficient memory-mapped I/O
- **B-tree Primary Index**: Memory-efficient indexing with better cache locality
- **Label Index**: Fast label-based queries with O(1) lookup
- **LRU Node Cache**: High hit rate for repeated reads
- **Performance Metrics**: Real-time monitoring of cache, queries, and traversals

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
sombra = "0.4.0"
```

### Example

```rust
use sombra::prelude::*;

let mut db = GraphDB::open("my_graph.db")?;

let mut tx = db.begin_transaction()?;

let user = tx.add_node(Node::new(0))?;
let post = tx.add_node(Node::new(1))?;
tx.add_edge(Edge::new(user, post, "AUTHORED"))?;

tx.commit()?;

let neighbors = db.get_neighbors(user)?;
println!("User {} authored {} posts", user, neighbors.len());
```

### MVCC Mode (Concurrent Transactions)

Enable MVCC for snapshot isolation and concurrent read-write transactions:

```rust
use sombra::db::{Config, GraphDB};

let mut config = Config::default();
config.mvcc_enabled = true;
config.max_concurrent_transactions = Some(100);

let mut db = GraphDB::open_with_config("my_graph.db", config)?;

// Concurrent transactions with snapshot isolation
let tx1 = db.begin_transaction()?; // Gets snapshot at T1
let tx2 = db.begin_transaction()?; // Gets snapshot at T2

// tx1 and tx2 see consistent snapshots
// Writes don't block reads
```

**Performance Trade-offs**:
- MVCC adds ~357μs per transaction (timestamp allocation)
- Reads are ~3.6μs slower (visibility checks)
- Storage overhead: ~33% for update-heavy workloads
- Benefits: Non-blocking reads, snapshot isolation

See `MVCC_IMPLEMENTATION_STATUS.md` for detailed performance benchmarks.

## Benchmarks

Run performance benchmarks:

```bash
# Standard benchmarks (read, write, traversal)
cargo bench --features benchmarks

# MVCC performance comparison
cargo bench --bench mvcc_performance --features benchmarks
```

## Documentation

See the [main Sombra documentation](https://docs.rs/sombra) for full API reference.

## Repository

[GitHub](https://github.com/maskdotdev/sombra)

## License

MIT
