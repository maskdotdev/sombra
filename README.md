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
npm install sombra
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
- [Python Usage](docs/python_usage.md) - Building and calling the PyO3 bindings

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

✅ **Phase 1 Complete** (Production Ready):
- Core graph operations (add/get nodes and edges)
- Page-based storage with B-tree indexing
- Write-ahead logging (WAL)
- ACID transactions with rollback
- Crash recovery
- Label secondary index
- LRU node cache
- Adjacency indexing for fast traversals
- Property-based indexes
- Optimized graph traversals (18-23x faster than SQLite)
- Performance metrics system
- TypeScript/Node.js NAPI bindings
- Comprehensive test suite (44/44 passing)

🚧 **Phase 2 In Progress**:
- ✅ Adjacency indexing (implemented - 18-23x faster traversals)
- ✅ Property-based indexes (implemented - O(log n) property queries)
- ⏳ Query planner with cost-based optimization (planned)
- ⏳ Concurrent readers (deferred - complex refactor needed)

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
