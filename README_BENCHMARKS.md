# Sombra Database Benchmarks

This directory contains a comprehensive benchmark suite for comparing Sombra against SQLite-based graph databases.

## Overview

The benchmark suite includes:

1. **Realistic Data Generation** - Functions to generate various types of graph data:
   - Social networks (users and friendships)
   - Product catalogs (products and categories)
   - Knowledge graphs (entities and relationships)

2. **SQLite Adapter** - A SQLite-based graph database implementation for comparison

3. **Performance Measurement** - Utilities for timing, memory tracking, and result analysis

4. **Benchmark Suite** - Comprehensive tests across different data sizes

## Running Benchmarks

### Prerequisites

Make sure you have the required dependencies installed:

```bash
# Install SQLite development libraries if needed
# On macOS:
brew install sqlite

# On Ubuntu/Debian:
sudo apt-get install libsqlite3-dev
```

### Run the full benchmark suite:

```bash
cargo bench --bench benchmark_main
```

This will run:
- Small dataset benchmarks (100 nodes, ~1000 edges)
- Medium dataset benchmarks (1000 nodes, ~25000 edges) 
- Large dataset benchmarks (10000 nodes, ~500000 edges)
- 30-second stress test

### Results

The benchmark will:
- Print a summary to the console
- Export detailed results to `benchmark_results.csv`
- Show memory usage and operations per second

## Benchmark Categories

### Insert Performance
- **Node insertion**: Time to insert all nodes
- **Edge insertion**: Time to insert all edges
- Measured in operations per second

### Query Performance  
- **Node lookup**: Time to retrieve nodes by ID
- **Neighbor queries**: Time to find adjacent nodes
- Each query is run multiple times for accuracy

### Stress Testing
- Continuous insertion for a fixed time period
- Measures sustained performance under load

## Data Generation

### Social Network
- User nodes with properties: name, age, active status, join date, score
- Friendship edges with properties: since date, strength
- Configurable network size and connectivity

### Product Catalog
- Product nodes with properties: name, price, stock, rating, availability
- Category nodes with hierarchical structure
- Belongs-to relationships with relevance scores

### Knowledge Graph
- Multiple entity types: Person, Organization, Location, Event, Concept
- Various relationship types: WORKS_FOR, LOCATED_IN, PARTICIPATES_IN, etc.
- Confidence scores and verification status

## Understanding Results

### Metrics
- **Duration**: Total time taken for the operation
- **Ops/sec**: Operations performed per second (higher is better)
- **Memory (MB)**: Memory usage during the operation
- **Count**: Total number of operations performed

### Expected Patterns
- Sombra should excel at:
  - Large-scale insertions due to efficient storage
  - Graph traversal queries with optimized adjacency lists
  - Memory efficiency for sparse graphs

- SQLite may excel at:
  - Complex property-based queries
  - ACID transaction overhead
  - Mature query optimization

## Custom Benchmarks

You can create custom benchmarks by:

1. Using the `DataGenerator` to create test data
2. Implementing your own benchmark logic in `benchmark_suite.rs`
3. Adding new benchmark categories

Example:
```rust
let mut runner = BenchmarkRunner::new();
let (nodes, edges) = runner.data_generator.generate_custom_data();
runner.benchmark_sombra_insert("custom_test", &nodes, &edges);
```

## Troubleshooting

### Memory Issues
For large datasets, ensure you have sufficient RAM:
- Small dataset: ~10MB
- Medium dataset: ~100MB  
- Large dataset: ~1GB

### SQLite Errors
Make sure SQLite development libraries are installed and the database file path is writable.

### Performance Variations
Benchmark results can vary based on:
- Hardware specifications
- System load
- Disk I/O performance
- Rust compiler optimizations

Run benchmarks multiple times and average the results for more reliable data.