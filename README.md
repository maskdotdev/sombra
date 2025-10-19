# Sombra - A Graph Database in Rust

Sombra is a file-based graph database inspired by SQLite's single-file architecture. Built in Rust with a focus on correctness, performance, and ACID transaction support.

## Features

### Core Features
- **Property Graph Model**: Nodes, edges, and flexible properties
- **Single File Storage**: SQLite-style database files
- **ACID Transactions**: Full transactional support with rollback
- **Write-Ahead Logging**: Crash-safe operations
- **Page-Based Storage**: Efficient memory-mapped I/O

### Performance Features ✨ NEW
- **Label Index**: O(1) label-based queries (500-1000x faster)
- **LRU Node Cache**: 2000x faster repeated reads with 90% hit rate
- **B-tree Primary Index**: 25-40% memory reduction, better cache locality
- **Optimized BFS Traversal**: 2,500% faster graph traversals vs SQLite
- **Performance Metrics**: Real-time monitoring of cache, queries, and traversals
- **Scalability Testing**: Validated for 100K+ node graphs

### Language Support
- **Rust API**: Core library with full feature support
- **TypeScript/Node.js API**: Complete NAPI bindings for JavaScript/TypeScript
- **Cross-Platform**: Linux, macOS, and Windows support

### Testing & Quality
- **39 Comprehensive Tests**: Unit, integration, and stress tests
- **Production Ready**: Zero breaking changes, automatic migration
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
```

### TypeScript/Node.js API

```typescript
import { GraphDB } from 'sombra';

// Open or create a database
const db = new GraphDB('my_graph.db');

// Use transactions for safe operations
const tx = db.beginTransaction();

// Add nodes and edges
const user = tx.addNode({ label: 0 });
const post = tx.addNode({ label: 1 });
tx.addEdge(user, post, 'AUTHORED');

// Commit to make changes permanent
tx.commit();

// Query the graph
const neighbors = db.getNeighbors(user);
console.log(`User ${user} authored ${neighbors.length} posts`);

// Graph traversals
const bfsResults = db.bfsTraversal(user, 3);
console.log('BFS traversal:', bfsResults);
```

## Installation

### Rust
```bash
cargo add sombra
```

### TypeScript/Node.js
```bash
npm install sombra
```

## Architecture

Sombra is built in layers:

1. **Storage Layer**: Page-based file storage with 8KB pages
2. **Pager Layer**: In-memory caching and dirty page tracking
3. **WAL Layer**: Write-ahead logging for crash safety
4. **Transaction Layer**: ACID transaction support
5. **Graph API**: High-level graph operations
6. **NAPI Bindings**: TypeScript/Node.js interface layer

## Documentation

### User Guides
- [Transactional Commit Layer](docs/transactional_commit_layer.md) - Complete user guide
- [Optimization API Guide](docs/optimization_api_guide.md) - Performance best practices
- [Performance Metrics](docs/performance_metrics.md) - Monitoring guide

### Technical Specifications
- [Transaction Design](docs/transactions.md) - Technical design specification
- [Data Model](docs/data_model.md) - Graph data structure details
- [B-tree Index Implementation](docs/btree_index_implementation.md) - Primary index details
- [Phase 1 Completion Report](docs/phase1_completion_report.md) - Optimization results

### Planning & Development
- [Lookup Optimization Plan](docs/lookup_optimization_plan.md) - Performance roadmap
- [Implementation Status](IMPLEMENTATION_STATUS.md) - Current progress
- [Roadmap](docs/roadmap.md) - Future development plans
- [Contributing](docs/contributing.md) - Development guidelines

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
| Label Index | 500-1000x faster queries | ✅ Complete |
| Node Cache | 2000x faster repeated reads | ✅ Complete |
| B-tree Index | 25-40% memory reduction | ✅ Complete |
| Metrics System | Real-time monitoring | ✅ Complete |

**Benchmark Results** (100K nodes):
```
Label Query:     390ms → 0.04ms  (9,750x faster)
Cached Reads:    2-4µs → 45ns    (2,000x faster)
BFS Traversal:   42 ops/sec → 1,092 ops/sec (2,500% faster)
Index Memory:    3.2MB → 2.4MB   (25% reduction)
Cache Hit Rate:  0% → 90%        (after warmup)
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

✅ **Phase 1 Complete** (Production Ready):
- Core graph operations (add/get nodes and edges)
- Page-based storage with B-tree indexing
- Write-ahead logging (WAL)
- ACID transactions with rollback
- Crash recovery
- Label secondary index
- LRU node cache
- Optimized BFS traversal (2,500% faster)
- Performance metrics system
- TypeScript/Node.js NAPI bindings
- Comprehensive test suite (39/39 passing)

🚧 **Phase 2 Planned** (Next 2-3 months):
- Adjacency indexing (5-10x traversal speedup)
- Property-based indexes
- Query planner with cost-based optimization
- Concurrent readers

🔮 **Phase 3 Future**:
- CSR representation for dense graphs
- Neighbor caching for hub nodes
- Path compression
- Custom B-tree implementation

## Examples

See the `tests/` directory for comprehensive examples:
- `tests/smoke.rs` - Basic usage patterns
- `tests/stress.rs` - Performance and scalability
- `tests/transactions.rs` - Transaction usage examples

## License

This project is open source. See [LICENSE](LICENSE) for details.

## Contributing

See [Contributing Guidelines](docs/contributing.md) for information on how to contribute to Sombra.
