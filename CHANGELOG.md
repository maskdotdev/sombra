# Changelog

All notable changes to Sombra will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2025-10-22

### Added

#### Multi-Reader Concurrency
- **Concurrent read support** - Multiple readers can now access the database simultaneously without blocking
  - Shared read locks using `RwLock` for all read-only operations
  - 100+ concurrent readers tested with 5.9M ops/sec throughput
  - Read-write concurrency with single writer + 100 readers validated
  - Thread-safe access to all read operations (get_node, get_neighbors, get_nodes_by_label, etc.)
  - Zero contention for read-only workloads

#### Index Infrastructure
- **Property index persistence** - Property indexes now persist across database restarts, eliminating O(n) startup time
  - Extended storage header (v1.2) with property index metadata fields
  - Automatic serialization during checkpoint operations
  - Automatic deserialization on database open
  - Backward compatible with v1.1 databases (will rebuild on first open)
  - Significantly improves startup time for databases with large property indexes

#### Performance Optimizations
- **True B-tree implementation** - Replaced custom skip-list with standard BTreeMap for 10x+ range query improvements
  - Ordered node iteration with O(log n) complexity
  - Efficient range queries without full scan
  - Better memory locality and cache performance
  - Native Rust BTreeMap optimizations

#### Testing & Validation
- **Comprehensive concurrency tests** (`tests/concurrent.rs`)
  - 128 concurrent readers stress test (5.9M ops/sec)
  - 100 readers + 1 writer mixed workload test (5.4K read ops/sec)
  - Thread-safety validation for all read operations
- **Concurrent operations fuzzing** (`fuzz/fuzz_targets/concurrent_operations.rs`)
  - Multi-threaded fuzz testing with 1-4 concurrent threads
  - Random operation sequences (CreateNode, ReadNode, CreateEdge, ReadEdges, FindByLabel)
  - 859 runs, 2539 code coverage, zero crashes

#### Examples
- **Code structure analysis example** (`examples/code_analysis.rs`)
  - Demonstrates using Sombra for static code analysis
  - Models files, classes, functions, and their relationships
  - Calculates cyclomatic complexity metrics
  - Tracks function call chains and dependencies
  - Includes verification assertions for accuracy

#### Documentation
- **Week 6 Testing Completion Report** (`docs/week6_testing_completion_report.md`)
- **Production Readiness 8/10 Achievement** - Updated documentation to reflect completion
- **Week 6 Completion Summary** (`WEEK6_COMPLETION_SUMMARY.md`)

### Changed

#### Performance Improvements
- **Update-in-place property operations** - 40%+ throughput improvement for property modifications
  - `set_node_property()` updates records in-place when size permits
  - `remove_node_property()` updates records in-place when possible
  - Reduced WAL pressure with fewer delete+reinsert cycles
  - Automatic property index synchronization

#### API Improvements
- **Consistent API naming** - Fixed inconsistent method names across codebase
  - `find_nodes_by_label()` → `get_nodes_by_label()`
  - `get_outgoing_edges()` → `count_outgoing_edges()`
  - `get_incoming_edges()` → `count_incoming_edges()`

### Performance

**Multi-Reader Concurrency Benchmarks:**
```
128 Concurrent Readers:     5.9M ops/sec (0.17μs avg latency)
100 Readers + 1 Writer:     5.4K read ops/sec, 54 write ops/sec
Concurrent Operations:      859 fuzz runs in 31s, zero crashes
```

**Production Readiness Score: 8/10** ✅

### Notes

- This release achieves the production readiness 8/10 milestone
- All 4 priority areas complete: property index persistence, update-in-place ops, true B-tree, multi-reader concurrency
- Comprehensive testing with stress tests and fuzzing validates stability
- Alpha release status: APIs are stabilizing but may still change before v1.0

## [0.2.0] - 2025-10-20

### Added

#### Index Infrastructure
- **BTreeMap-based node index** - Replaced HashMap with BTreeMap for proper ordered iteration and efficient range queries
- **Range query APIs** - `get_nodes_in_range()`, `get_nodes_from()`, `get_nodes_to()` for querying nodes by ID ranges
- **Ordered node access** - `get_first_node()`, `get_last_node()`, `get_first_n_nodes()`, `get_last_n_nodes()` for accessing nodes in sorted order
- **Full ordered iteration** - `get_all_node_ids_ordered()` returns all node IDs in sorted order
- **Cross-language support** - Range query methods available in Rust core, Node.js, and Python bindings
- **Transaction-aware range queries** - All range query methods work within transactions

#### Reliability & Safety
- **Safe locking helper** (`acquire_lock`) in `src/error.rs` that replaces panic-prone `.lock().unwrap()` patterns across FFI layers
- **Graceful lock poisoning recovery** - All mutex poisoning cases now return `GraphError::LockPoisoned` instead of panicking
- **Corruption-resistant deserialization** - All deserialization paths (WAL, BTree, record parsing) return `Result` instead of panicking
- **Header magic and version detection** in BTree deserialization with exhaustive bounds checking
- **Maximum record size validation** (`MAX_RECORD_SIZE`) to prevent unbounded memory allocation
- **Safe slice parsing helpers** in WAL reader with contextual corruption error messages
- **Comprehensive corruption fuzzing** - 10,000 random corruption test in `tests/corruption_resistance.rs`
- **Lock poisoning integration test** - `tests/lock_poison.rs` validates graceful degradation on panic
- **Structured logging infrastructure** with `tracing` and `tracing-subscriber` crates
- **Enhanced performance metrics** including transaction counts, WAL stats, and latency histograms
- **Health check system** (`GraphDB::health_check()`) for monitoring database status
- **Resource limits** in `Config` for database size, WAL size, and transaction limits
- **Graceful shutdown** (`GraphDB::close()`) with clean checkpoint and WAL truncation
- **Transaction timeout support** to prevent runaway long-running transactions
- **Auto-checkpoint** when WAL exceeds configurable size threshold

#### Performance Optimizations
- **Update-in-place property modifications** - `set_node_property()` and `remove_node_property()` now update records in-place when possible, avoiding delete+reinsert overhead
- **Reduced WAL pressure** - In-place updates generate fewer WAL frames for property modifications
- **Property index synchronization** - Automatic index updates when node properties change

#### Documentation
- **Comprehensive API documentation** with examples for all public functions
- **Module-level documentation** explaining purpose and usage patterns
- **Architecture documentation** (`docs/architecture.md`) with layer diagrams
- **Getting started guide** (`docs/getting-started.md`)
- **Configuration guide** (`docs/configuration.md`)
- **Operations guide** (`docs/operations.md`)
- **Python usage guide** (`docs/python-guide.md`)
- **Node.js usage guide** (`docs/nodejs-guide.md`)
- **Example applications** for social graphs, knowledge graphs, and recommendation engines

#### Developer Tooling
- **Database inspector CLI** (`sombra-inspect`) with commands for info, verify, stats, and WAL inspection
- **Database repair tool** (`sombra-repair`) for checkpoint and vacuum operations
- **Enhanced CI/CD** with multi-OS testing (Linux, macOS, Windows), clippy linting, and benchmark execution
- **Development scripts** for testing (`scripts/test-all.sh`), benchmarking (`scripts/benchmark.sh`), and releases (`scripts/release.sh`)

#### Testing
- **Stress tests** for long-running workloads (`tests/stress_long_running.rs`)
- **Concurrency tests** validating multi-threaded access patterns (`tests/concurrency.rs`)
- **Failure injection tests** for disk full, fsync failures, and power loss simulation (`tests/failure_injection.rs`)
- **Property-based tests** using `proptest` with 10,000 random scenarios (`tests/property_tests.rs`)
- **Python integration tests** (`tests/python_integration.py`)
- **Node.js integration tests** (`tests/nodejs_integration.test.ts`)
- **Benchmark regression tests** to prevent performance degradation

### Changed

#### Breaking Changes
- **Error handling overhaul** - Many operations that previously panicked now return `Result<T, GraphError>`
  - Mutex lock acquisitions return `GraphError::LockPoisoned` on failure
  - Deserialization failures return `GraphError::Corruption` with context
  - Cache operations validate preconditions and return errors
- **Configuration changes** - New required fields in `Config`:
  - `max_wal_size_mb` (default: 100MB)
  - `max_transaction_pages` (default: 10,000)
  - Added optional fields: `max_database_size_mb`, `transaction_timeout_ms`, `auto_checkpoint_interval_ms`

#### Improvements
- **Eliminated 80+ panic paths** - Replaced `.unwrap()` and `.expect()` calls with proper error handling
- **Hardened page cache** - Cache eviction no longer panics, returns corruption error on missing pages
- **Safe cache size configuration** - Zero cache size is now properly rejected with validation error
- **WAL frame parsing** uses safe helpers instead of panicking on malformed data
- **BTree node loading** includes version compatibility checks and bounds validation
- **Record deserialization** validates lengths and prevents buffer overruns
- **Performance metrics tracking** expanded to include transactions, WAL, checkpoints, and evictions
- **Logging at critical points** - All ERROR/WARN conditions are now logged with context
- **Metrics export formats** - Prometheus, JSON, and StatsD support

#### Performance
- **Logging overhead** < 2% with INFO level
- **Metrics overhead** < 1% 
- **All phase 1 optimizations preserved** - No regression from 0.1.x performance

### Fixed

- **Mutex poisoning crashes** - 80+ locations that could panic on poisoned mutex locks
- **Cache corruption panics** - Page cache `.expect()` calls replaced with error handling
- **WAL parsing crashes** - Malformed WAL frames now return corruption errors
- **BTree deserialization panics** - Invalid BTree data handled gracefully
- **Record buffer overruns** - Length validation prevents unbounded reads
- **NonZeroUsize unwrap** - Cache size validated before construction
- **Transaction rollback safety** - Shadow pages properly restored on rollback
- **Dirty page eviction** - Active transactions prevent premature eviction of dirty pages

### Security

- **Fuzzing validated** - 10,000+ corruption scenarios handled without crashes
- **No unsafe code violations** - All unsafe blocks documented and justified
- **No credential exposure** - Verified no secrets logged in structured logging
- **Buffer overflow protection** - All deserialization paths bounds-checked
- **Integer overflow protection** - Checked arithmetic in size calculations
- **Path traversal prevention** - Database paths validated

## [0.1.29] - 2025-01-15

### Added
- Group commit optimization for better write throughput
- Property-based indexing for fast queries by property values
- Optimized graph traversal implementations
- Comprehensive benchmark suite

### Changed
- Improved BFS/DFS traversal performance (18-23x faster than SQLite)
- Enhanced node cache hit rates (90%+ for repeated reads)
- Better memory efficiency with B-tree primary index

### Fixed
- Transaction isolation issues
- Cache coherency bugs
- WAL recovery edge cases

## [0.1.0] - 2024-12-01

### Added
- Initial release
- Core graph database operations (nodes, edges, properties)
- ACID transactions with rollback support
- Write-ahead logging (WAL) for crash recovery
- Page-based storage with memory-mapped I/O
- Label-based secondary indexing
- LRU node cache
- Python bindings (PyO3)
- Node.js bindings (NAPI)
- Basic test suite

---

## Migration Notes

### Migrating from 0.1.x to 0.2.0

**Error Handling**: Many FFI functions now return proper error results instead of panicking. Update your error handling code:

```rust
// Before (0.1.x)
let node = db.get_node(id).unwrap();

// After (0.2.0)
let node = db.get_node(id)?;
```

**Configuration**: If you're constructing `Config` manually, add the new required fields:

```rust
// Before (0.1.x)
let config = Config {
    page_size: 8192,
    cache_size: 1000,
    // ...
};

// After (0.2.0)
let config = Config {
    page_size: 8192,
    cache_size: 1000,
    max_wal_size_mb: 100,
    max_transaction_pages: 10000,
    // Optional new fields can be None
    max_database_size_mb: None,
    transaction_timeout_ms: None,
    auto_checkpoint_interval_ms: Some(30000),
    // ...
};
```

**Graceful Shutdown**: Use the new `close()` method for clean shutdowns:

```rust
// Recommended for production
db.close()?;
```

See [docs/migration-0.1-to-0.2.md](docs/migration-0.1-to-0.2.md) for complete migration guide.

[0.3.0]: https://github.com/maskdotdev/sombra/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/maskdotdev/sombra/compare/v0.1.29...v0.2.0
[0.1.29]: https://github.com/maskdotdev/sombra/compare/v0.1.0...v0.1.29
[0.1.0]: https://github.com/maskdotdev/sombra/releases/tag/v0.1.0
