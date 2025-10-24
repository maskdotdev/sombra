# Sombra Core

Core Rust library for Sombra, a high-performance graph database.

> **Note:** This is alpha software under active development. APIs may change between minor versions.

## Overview

This package contains the core graph database implementation for Sombra. It provides the foundational data structures, storage engine, and graph algorithms that power the Node.js and Python bindings.

## Features

- **Property Graph Model**: Nodes, edges, and flexible properties
- **Single File Storage**: SQLite-style database files
- **ACID Transactions**: Full transactional support with rollback
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

## Documentation

See the [main Sombra documentation](https://docs.rs/sombra) for full API reference.

## License

MIT
