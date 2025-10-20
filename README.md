# Sombra - Production-Ready Graph Database

[![Crates.io](https://img.shields.io/crates/v/sombra)](https://crates.io/crates/sombra)
[![Documentation](https://docs.rs/sombra/badge.svg)](https://docs.rs/sombra)
[![CI](https://github.com/maskdotdev/sombra/workflows/CI/badge.svg)](https://github.com/maskdotdev/sombra/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Sombra is a file-based graph database inspired by SQLite's single-file architecture. Built in Rust with a focus on **reliability**, **performance**, and **ACID transactions**.

**Version 0.2.0 - Production Ready** ✅
- Zero panic paths in production code
- Comprehensive error handling with graceful degradation
- Battle-tested with 10,000+ corruption scenarios
- Structured logging and health monitoring
- Complete API documentation and operational guides

## Features

### Core Features
- **Property Graph Model**: Nodes, edges, and flexible properties
- **Single File Storage**: SQLite-style database files
- **ACID Transactions**: Full transactional support with rollback
- **Write-Ahead Logging**: Crash-safe operations
- **Page-Based Storage**: Efficient memory-mapped I/O

### Performance Features ✨ NEW
- **Label Index**: Fast label-based queries with O(1) lookup
- **LRU Node Cache**: 90% hit rate for repeated reads
- **B-tree Primary Index**: 25-40% memory reduction, better cache locality
- **Optimized Graph Traversals**: 18-23x faster than SQLite for graph operations
- **Performance Metrics**: Real-time monitoring of cache, queries, and traversals
- **Scalability Testing**: Validated for 100K+ node graphs

### Language Support
- **Rust API**: Core library with full feature support
- **TypeScript/Node.js API**: Complete NAPI bindings for JavaScript/TypeScript
- **Python API**: PyO3 bindings with native performance (build with `maturin -F python`)
- **Cross-Platform**: Linux, macOS, and Windows support

### Reliability & Production Features (v0.2.0) ✨ NEW
- **Zero Panic Paths**: All errors handled gracefully with `Result` types
- **Corruption Resistance**: Safe deserialization with comprehensive validation
- **Structured Logging**: Full tracing support with `tracing` crate
- **Health Monitoring**: Built-in health checks and extended metrics
- **Graceful Shutdown**: Clean database closure with WAL checkpoint
- **Resource Limits**: Configurable limits for database size, WAL, and transactions
- **Database Inspector**: CLI tools for inspection and repair

### Testing & Quality
- **100+ Comprehensive Tests**: Unit, integration, stress, and fuzz tests
- **Corruption Fuzzing**: 10,000+ scenarios tested without crashes
- **Multi-Platform CI**: Linux, macOS, Windows with full test coverage
- **Zero Clippy Warnings**: Strict linting with `-D warnings`
- **Benchmark Suite**: Performance regression testing

## Quick Start

### Rust API

```rust
use sombra::prelude::*;

// Open or create a database
let mut db = GraphDB::open("my_graph.db")?;

// Use transactions for safe operations
let mut tx = db.begin_transaction()?;

// Add nodes and edges
let user = tx.add_node(Node::new(0))?;
let post = tx.add_node(Node::new(1))?;
tx.add_edge(Edge::new(user, post, "AUTHORED"))?;

// Commit to make changes permanent
tx.commit()?;

// Query the graph
let neighbors = db.get_neighbors(user)?;
println!("User {} authored {} posts", user, neighbors.len());

// Create property indexes for fast queries
db.create_property_index("User", "age")?;
let users_age_30 = db.find_nodes_by_property("User", "age", &PropertyValue::Int(30))?;
println!("Found {} users aged 30", users_age_30.len());
```

### TypeScript/Node.js API

```typescript
import { SombraDB, SombraPropertyValue } from 'sombradb';

const db = new SombraDB('./my_graph.db');

const createProp = (type: 'string' | 'int' | 'float' | 'bool', value: any): SombraPropertyValue => ({
  type,
  value
});

const alice = db.addNode(['Person'], {
  name: createProp('string', 'Alice'),
  age: createProp('int', 30)
});

const bob = db.addNode(['Person'], {
  name: createProp('string', 'Bob'),
  age: createProp('int', 25)
});

const knows = db.addEdge(alice, bob, 'KNOWS', {
  since: createProp('int', 2020)
});

const aliceNode = db.getNode(alice);
console.log('Alice:', aliceNode);

const neighbors = db.getNeighbors(alice);
console.log(`Alice has ${neighbors.length} connections`);

const bfsResults = db.bfsTraversal(alice, 3);
console.log('BFS traversal:', bfsResults);

const tx = db.beginTransaction();
try {
  const charlie = tx.addNode(['Person'], {
    name: createProp('string', 'Charlie')
  });
  tx.addEdge(alice, charlie, 'KNOWS');
  tx.commit();
} catch (error) {
  tx.rollback();
  throw error;
}

db.flush();
db.checkpoint();
```

### Python API

```python
from sombra import SombraDB

db = SombraDB("./my_graph.db")

alice = db.add_node(["Person"], {"name": "Alice", "age": 30})
bob = db.add_node(["Person"], {"name": "Bob", "age": 25})

db.add_edge(alice, bob, "KNOWS", {"since": 2020})

node = db.get_node(alice)
print(f"Alice -> {node.labels}, properties={node.properties}")

neighbors = db.get_neighbors(alice)
print(f"Alice has {len(neighbors)} connections")

tx = db.begin_transaction()
try:
    charlie = tx.add_node(["Person"], {"name": "Charlie"})
    tx.add_edge(alice, charlie, "KNOWS")
    tx.commit()
except Exception:
    tx.rollback()
    raise
```

## Installation

### Rust
```bash
cargo add sombra
```

### TypeScript/Node.js
```bash
npm install sombradb
```

### Python
```bash
# Install from PyPI (coming soon)
pip install sombra

# Or build from source
pip install maturin
maturin build --release -F python
pip install target/wheels/sombra-*.whl
```

### CLI Tools

Install the unified CLI for database inspection, repair, and verification:

```bash
# Via Cargo (recommended)
cargo install sombra

# The 'sombra' command will be available system-wide
sombra --help
```

The CLI is also bundled with npm and pip installations:
```bash
# Via npm
npm install -g sombradb
sombra inspect mydb.db info

# Via pip
pip install sombra
sombra verify mydb.db
```

See the [CLI documentation](docs/cli.md) for complete usage guide.

## Architecture

Sombra is built in layers:

1. **Storage Layer**: Page-based file storage with 8KB pages
2. **Pager Layer**: In-memory caching and dirty page tracking
3. **WAL Layer**: Write-ahead logging for crash safety
4. **Transaction Layer**: ACID transaction support
5. **Graph API**: High-level graph operations
6. **NAPI Bindings**: TypeScript/Node.js interface layer

## Documentation

### Getting Started
- [Getting Started Guide](docs/getting-started.md) - Quick start tutorial
- [Configuration Guide](docs/configuration.md) - Configuration options and tuning
- [Operations Guide](docs/operations.md) - Production deployment and monitoring
- [Migration Guide](docs/migration-0.1-to-0.2.md) - Upgrading from 0.1.x to 0.2.0

### Language-Specific Guides
- [Python Guide](docs/python-guide.md) - Using Sombra from Python
- [Node.js Guide](docs/nodejs-guide.md) - Using Sombra from TypeScript/JavaScript
- [CLI Tools](docs/cli.md) - Command-line tools for inspection, repair, and verification

### Technical Documentation
- [Architecture](docs/architecture.md) - System architecture and design
- [Transaction Design](docs/transactions.md) - ACID transaction implementation
- [Data Model](docs/data_model.md) - Graph data structure details
- [B-tree Index Implementation](docs/btree_index_implementation.md) - Primary index details
- [Performance Metrics](docs/performance_metrics.md) - Monitoring and observability

### Development
- [Contributing](docs/contributing.md) - Development guidelines
- [Roadmap](docs/roadmap.md) - Future development plans
- [API Documentation](https://docs.rs/sombra) - Complete API reference

## Testing

```bash
# Run all tests
cargo test

# Run transaction tests specifically
cargo test transactions

# Run smoke tests
cargo test smoke

# Run stress tests
cargo test stress
```

## Performance

### Phase 1 Optimizations ✅ COMPLETE

Sombra now includes production-ready performance optimizations:

| Optimization | Improvement | Status |
|--------------|-------------|--------|
| Label Index | Fast O(1) label queries | ✅ Complete |
| Node Cache | 90% hit rate for repeated reads | ✅ Complete |
| B-tree Index | 25-40% memory reduction | ✅ Complete |
| Metrics System | Real-time monitoring | ✅ Complete |

**Benchmark Results** (100K nodes):
```
Node Lookups:    ~1.5M ops/sec
Neighbor Queries: ~9.9M ops/sec  
Index Memory:    25% reduction (3.2MB → 2.4MB)
Cache Hit Rate:  90% after warmup
```

**Graph Traversal Performance** (vs SQLite):
- Medium Dataset: 7,778 ops/sec vs 452 ops/sec (18x faster)
- Large Dataset: 1,092 ops/sec vs 48 ops/sec (23x faster)

### Running Benchmarks

```bash
# Index performance comparison
cargo bench --bench index_benchmark --features benchmarks

# BFS traversal performance
cargo bench --bench small_read_benchmark --features benchmarks

# Scalability testing (50K-500K nodes)
cargo bench --bench scalability_benchmark --features benchmarks

# Performance metrics demo
cargo run --example performance_metrics_demo --features benchmarks
```

## Current Status

### ✅ Version 0.2.0 - Production Ready

**Core Features:**
- Core graph operations (nodes, edges, properties)
- Page-based storage with B-tree indexing (25-40% memory savings)
- Write-ahead logging (WAL) for crash recovery
- ACID transactions with rollback support
- Label secondary index with O(1) lookup
- LRU node cache (90% hit rate)
- Adjacency indexing for fast traversals (18-23x faster than SQLite)
- Property-based indexes for O(log n) queries

**Production Hardening (v0.2.0):**
- ✅ Zero panic paths - All errors handled gracefully
- ✅ Corruption resistance - 10,000+ fuzzing scenarios
- ✅ Structured logging - Full `tracing` support
- ✅ Health monitoring - Extended metrics and health checks
- ✅ Graceful shutdown - Clean `close()` method
- ✅ Resource limits - Configurable size/timeout limits
- ✅ CLI tools - Inspector and repair utilities
- ✅ 100+ tests passing - Unit, integration, stress, fuzz
- ✅ Complete documentation - API docs, guides, examples
- ✅ Multi-platform CI - Linux, macOS, Windows

**Language Bindings:**
- ✅ Rust API (native)
- ✅ Python bindings (PyO3)
- ✅ TypeScript/Node.js bindings (NAPI)

### 🚀 Future Roadmap

**Planned Features:**
- Page-level checksums for data integrity validation
- Concurrent readers with MVCC
- Query planner with cost-based optimization
- Replication and high availability
- Backup/restore utilities
- Performance dashboard

See [CHANGELOG.md](CHANGELOG.md) for detailed release notes and [docs/roadmap.md](docs/roadmap.md) for future plans.

## Examples

See the `tests/` directory for comprehensive examples:
- `tests/smoke.rs` - Basic usage patterns
- `tests/stress.rs` - Performance and scalability
- `tests/transactions.rs` - Transaction usage examples

## License

This project is open source. See [LICENSE](LICENSE) for details.

## Contributing

See [Contributing Guidelines](docs/contributing.md) for information on how to contribute to Sombra.
