# Sombra DB: Multi-Crate to Monolithic Migration Guide

## Overview

This document outlines the complete migration plan from a 20-crate workspace structure to a single monolithic `sombra` crate. This consolidation will:

- Reduce maintenance overhead (19 fewer Cargo.toml files)
- Improve compile times (fewer build units = better parallelization)
- Simplify dependency management
- Make refactoring easier
- Better suited for publishing a single unified crate

**Status**: Planning phase  
**Complexity**: Medium (clean dependency graph, no circular deps)  
**Estimated Duration**: 2-3 weeks depending on team size

---

## Target Structure

```
sombra-db/
├── src/
│   ├── lib.rs                 # Main library root
│   ├── types/                 # Base types & errors (sombra-types, sombra-checksum)
│   │   ├── mod.rs
│   │   ├── error.rs
│   │   └── ...
│   ├── primitives/            # Low-level utilities
│   │   ├── mod.rs
│   │   ├── bytes/            # (sombra-bytes)
│   │   ├── io/               # (sombra-io)
│   │   ├── concurrency/      # (sombra-concurrency)
│   │   ├── wal/              # (sombra-wal)
│   │   └── pager/            # (sombra-pager)
│   ├── storage/               # Storage engine
│   │   ├── mod.rs
│   │   ├── core/             # (sombra-core)
│   │   ├── btree/            # (sombra-btree)
│   │   ├── vstore/           # (sombra-vstore)
│   │   ├── catalog/          # (sombra-catalog)
│   │   └── index/            # (sombra-index)
│   ├── query/                 # Query processing (sombra-query)
│   │   ├── mod.rs
│   │   ├── ast.rs
│   │   ├── planner.rs
│   │   └── ...
│   └── admin/                 # Admin utilities (sombra-admin)
│       ├── mod.rs
│       └── ...
├── src/bin/
│   └── cli.rs                # CLI binary (sombra-cli logic)
├── tests/
│   ├── integration/          # Integration tests
│   └── fixtures/             # Test data
├── benches/
│   └── ...                   # Benchmark code (from sombra-bench)
├── bindings/
│   ├── node/                 # Keep as separate NAPI crate
│   └── python/               # Keep as separate PyO3 crate
├── Cargo.toml                # Single root Cargo.toml (not workspace)
└── Cargo.lock
```

---

## Phase Breakdown

### Phase 1: Planning & Preparation ✓ CURRENT

**Goal**: Document current state and prepare for migration

**Tasks**:
- [x] Analyze dependency graph (no circular deps confirmed!)
- [x] Document API boundaries for each crate
- [ ] Create detailed module layout
- [ ] Identify all feature flags and conditional compilation
- [ ] List all external dependencies per crate

**Deliverables**:
- Comprehensive dependency map
- Feature flag inventory
- Module layout specification

**Duration**: 1-2 days

---

### Phase 2: Create Base Monolithic Structure

**Goal**: Set up new monolithic src/ structure without migrating code yet

**Tasks**:
1. Create directory structure in `src/`
   ```bash
   mkdir -p src/{types,primitives/{bytes,io,concurrency,wal,pager},storage/{core,btree,vstore,catalog,index},query,admin}
   ```

2. Create `mod.rs` files for each module with appropriate visibility
3. Create initial `src/lib.rs` with module declarations
4. Create `src/bin/cli.rs` as entry point for CLI binary
5. Update root `Cargo.toml`:
   - Remove `[workspace]` section
   - Move all dependencies into root
   - Remove workspace members
   - Add lib and bin target definitions

6. Verify project compiles (should fail with missing modules - that's okay)

**Dependencies**: None

**Validation**:
```bash
cargo check  # Should fail with missing module definitions (expected)
```

**Duration**: 1 day

---

### Phase 3: Migrate Base Layer (Depth 0)

**Goal**: Migrate foundation types that have no internal dependencies

**Crates**: `sombra-types`, `sombra-checksum`

**Tasks**:
1. Copy `crates/sombra-types/src/**` → `src/types/`
2. Copy `crates/sombra-checksum/src/**` → `src/types/checksum/`
3. Remove all internal `use sombra_*` imports (won't exist)
4. Update `src/types/mod.rs` to export all types
5. Run tests
   ```bash
   cargo test types::
   ```

**Dependencies**: None internal

**Validation**:
```bash
cargo test types:: checksum::
```

**Duration**: 1 day

---

### Phase 4: Migrate Primitives Layer (Depth 1-2)

**Goal**: Migrate low-level utilities that depend only on types

**Crates**: `sombra-bytes`, `sombra-io`, `sombra-concurrency`, `sombra-wal`, `sombra-pager`

**Key Points**:
- **sombra-pager** is the most complex - depends on all lower layers
- Migrate in order: bytes → io → concurrency → wal → pager

**For each crate**:
1. Copy source code to appropriate module
2. Replace `use sombra_types::` with `use crate::types::`
3. Replace `use sombra_bytes::` with `use crate::primitives::bytes::`
4. Update `mod.rs` to re-export public items
5. Run tests

**Example replacements**:
```rust
// Before
use sombra_types::Error;
use sombra_bytes::Bytes;

// After
use crate::types::Error;
use crate::primitives::bytes::Bytes;
```

**Testing**:
```bash
cargo test primitives::bytes::
cargo test primitives::io::
cargo test primitives::concurrency::
cargo test primitives::wal::
cargo test primitives::pager::
```

**Duration**: 3-4 days

---

### Phase 5: Migrate Storage Layer (Depth 3-6)

**Goal**: Migrate storage engine components

**Crates** (in order):
1. `sombra-core` - depends on: types, bytes, checksum, io, pager
2. `sombra-btree` - depends on: types, pager
3. `sombra-vstore` - depends on: types, checksum, pager
4. `sombra-catalog` - depends on: types, bytes, pager, btree, vstore
5. `sombra-index` - depends on: types, pager, btree
6. `sombra-storage` - depends on all above

**For each crate**:
1. Copy code to `src/storage/{module}/`
2. Update all import paths:
   ```rust
   sombra_pager::* → crate::primitives::pager::*
   sombra_core::* → crate::storage::core::*
   ```
3. Pay attention to circular dependencies (there shouldn't be any)
4. Run tests frequently

**Common issues to watch**:
- Re-exports between modules
- Test utilities that depend on multiple modules
- Build.rs scripts or proc macros

**Testing**:
```bash
cargo test storage::core::
cargo test storage::btree::
cargo test storage::vstore::
cargo test storage::catalog::
cargo test storage::index::
cargo test storage::    # All storage tests
```

**Duration**: 4-5 days

---

### Phase 6: Migrate Query & High-Level Components

**Goal**: Migrate query engine and admin tools

**Crates** (in order):
1. `sombra-query` - depends on: types, pager, storage (catalog, index), storage
2. `sombra-admin` - depends on: types, pager, storage, catalog
3. `sombra-cli` → move to `src/bin/cli.rs`

**Tasks**:
1. Migrate `sombra-query` to `src/query/`
2. Migrate `sombra-admin` to `src/admin/`
3. Move CLI implementation:
   - Copy `crates/sombra-cli/src/main.rs` logic to `src/bin/cli.rs`
   - Move argument parsing and main logic
   - Import from `sombra::` crate
4. Update all import paths
5. Ensure CLI still works
6. Run integration tests

**Testing**:
```bash
cargo test query::
cargo test admin::
cargo build --bin cli
./target/debug/cli --help
```

**Duration**: 2-3 days

---

### Phase 7: Reorganize Tests & Utilities

**Goal**: Consolidate testing infrastructure

**Tasks**:
1. Identify all test files:
   - Unit tests (in `#[cfg(test)]` modules within src/)
   - Integration tests (in crates/*/tests/)
   - Benchmark code (sombra-bench)

2. Move integration tests to `tests/`:
   ```
   tests/
   ├── common/mod.rs          # Shared test utilities
   ├── storage_integration.rs
   ├── query_integration.rs
   └── ...
   ```

3. Convert `sombra-testkit` utilities:
   - Option A: Move to `src/testkit.rs` or `src/testkit/` (internal module)
   - Option B: Create `tests/common/mod.rs` and re-export

4. Move benchmarks to `benches/`:
   ```
   benches/
   ├── storage.rs
   ├── query.rs
   └── ...
   ```

5. Ensure all tests still pass:
   ```bash
   cargo test --all
   cargo test --test '*'
   cargo bench --no-run
   ```

**Duration**: 2 days

---

### Phase 8: Update Language Bindings

**Goal**: Update FFI layers to use monolithic crate

**For Node.js (bindings/node/)**:
1. Update `Cargo.toml`:
   ```toml
   [dependencies]
   sombra = { path = "../.." }  # Point to root
   ```

2. Update `src/lib.rs` - import from root sombra crate:
   ```rust
   use sombra::storage;
   use sombra::query;
   ```

3. Test build:
   ```bash
   cd bindings/node && cargo build --release
   npm install
   npm test
   ```

**For Python (bindings/python/)**:
1. Update `Cargo.toml` similarly
2. Update imports in `_native.rs`
3. Test build:
   ```bash
   cd bindings/python && cargo build --release
   python -m pytest tests/
   ```

**Testing**:
```bash
cargo test --all
cd bindings/node && npm test
cd bindings/python && python -m pytest tests/
```

**Duration**: 1-2 days

---

### Phase 9: Clean Up & Remove Old Structure

**Goal**: Remove old crates/ directory and finalize structure

**Tasks**:
1. **Backup**: Create a git commit with the old crates/ before deletion
   ```bash
   git add -A
   git commit -m "backup: save old multi-crate structure before consolidation"
   ```

2. **Delete old crates**:
   ```bash
   rm -rf crates/
   ```

3. **Update project files**:
   - Update `.gitignore` if needed
   - Update CI/CD workflows (`.github/workflows/`)
   - Update `README.md` if it references crate structure
   - Update CONTRIBUTING guide

4. **Update CI/CD**:
   - `.github/workflows/ci.yml` - should just do `cargo test`
   - `.github/workflows/lint.yml` - should just do `cargo clippy`
   - Remove any multi-crate-specific logic

**Duration**: 1 day

---

### Phase 10: Final Validation & Testing

**Goal**: Comprehensive testing to ensure migration is complete and correct

**Tasks**:
1. Full test suite:
   ```bash
   cargo test --all --all-features
   ```

2. Clippy linting:
   ```bash
   cargo clippy --all --all-targets -- -D warnings
   ```

3. Release build:
   ```bash
   cargo build --release
   ```

4. Benchmarks:
   ```bash
   cargo bench --no-run
   ```

5. CLI verification:
   ```bash
   ./target/release/cli --help
   ./target/release/cli [test operations]
   ```

6. FFI verification:
   ```bash
   cd bindings/node && npm test
   cd bindings/python && python -m pytest tests/
   ```

7. Documentation check:
   ```bash
   cargo doc --no-deps --open
   ```

**Acceptance Criteria**:
- All tests pass ✓
- No clippy warnings ✓
- Release build completes ✓
- CLI works correctly ✓
- FFI bindings work ✓
- All benchmarks run ✓

**Duration**: 1-2 days

---

## Key Considerations

### Module Visibility Strategy

Use Rust's module system to maintain API boundaries:

```rust
// src/lib.rs
pub mod types;
pub mod primitives;
pub mod storage;
pub mod query;
pub mod admin;

// src/storage/mod.rs
pub mod core;
pub mod btree;
pub mod catalog;
pub mod index;
// internal only
mod vstore;

// Re-export key items for convenience
pub use storage::catalog::Catalog;
pub use storage::index::Index;
```

### Handling Re-exports

Keep re-exports explicit and minimal:

```rust
// src/primitives/mod.rs
pub mod bytes;
pub mod io;
pub mod pager;

// Make commonly used items available
pub use pager::Pager;
pub use io::Reader;
```

### Managing Test Dependencies

For shared test utilities:

```rust
// src/testkit.rs (marked #[cfg(test)] if truly internal only)
pub struct TestFixture { ... }
pub fn setup_test_db() -> ... { ... }

// Or in tests/common/mod.rs for integration tests
pub fn shared_setup() { ... }
```

### Feature Flags

If any crates had feature flags:

```toml
[features]
default = []
benchmarks = []
fuzzing = []
```

Update conditional compilation:

```rust
#[cfg(feature = "benchmarks")]
mod bench_utils;
```

---

## Rollback Plan

If issues arise during migration:

1. **Before major phases**: Create git commits
   ```bash
   git commit -m "phase X: [description]"
   ```

2. **If critical issues found**:
   ```bash
   git revert [commit]  # Revert to last working state
   ```

3. **The old crates/ backup commit** allows easy reference to original code

---

## Expected Benefits

After migration, you should see:

1. **Simpler build process**:
   - `cargo test` instead of managing multiple crates
   - Faster clean builds (fewer codegen passes)

2. **Easier development**:
   - Move code between modules freely
   - Shared test utilities without pub-in-pub issues

3. **Cleaner dependencies**:
   - Single Cargo.toml to maintain
   - Clear module hierarchy

4. **Better for publishing**:
   - Single `sombra` crate on crates.io
   - Unified versioning

---

## Estimated Timeline

| Phase | Duration | Status |
|-------|----------|--------|
| 1. Planning | 1-2 days | In Progress |
| 2. Base Structure | 1 day | Pending |
| 3. Base Layer | 1 day | Pending |
| 4. Primitives | 3-4 days | Pending |
| 5. Storage | 4-5 days | Pending |
| 6. Query & CLI | 2-3 days | Pending |
| 7. Tests | 2 days | Pending |
| 8. Bindings | 1-2 days | Pending |
| 9. Cleanup | 1 day | Pending |
| 10. Validation | 1-2 days | Pending |
| **Total** | **17-25 days** | **Pending** |

**Realistic estimate with team**: 2-3 weeks

---

## Getting Help

If stuck on a specific phase:

1. Check git history for similar module migrations
2. Review Rust module system docs: https://doc.rust-lang.org/book/ch07-00-managing-growing-projects-with-packages-modules-and-paths.html
3. Use `cargo tree` to debug dependency issues
4. Check `rg` for import patterns to replace

---

## Post-Migration Improvements

After successful migration, consider:

1. **Reduce public API surface** - mark internal modules as private
2. **Consolidate feature flags** - remove if unnecessary
3. **Add workspace lint config** to workspace instead of per-crate
4. **Update benchmarks** to use built-in `benches/` directory
5. **Consider feature-gating** heavy optional dependencies
