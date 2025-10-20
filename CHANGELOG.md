# Changelog

All notable changes to Sombra will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2025-10-20

### Added

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

[0.2.0]: https://github.com/maskdotdev/sombra/compare/v0.1.29...v0.2.0
[0.1.29]: https://github.com/maskdotdev/sombra/compare/v0.1.0...v0.1.29
[0.1.0]: https://github.com/maskdotdev/sombra/releases/tag/v0.1.0
