# 🏗️ Sombra Monorepo Restructuring Plan

## Executive Summary

This plan outlines a complete restructuring of the Sombra repository from a **mixed-language root layout** to a **proper monorepo structure** with clear package boundaries. This will enable:

- ✅ Independent versioning via release-please (currently broken)
- ✅ Clear separation of concerns
- ✅ Better IDE/tooling support
- ✅ Simplified CI/CD workflows
- ✅ Easier onboarding for contributors

**Estimated Effort:** 2-3 days of work + 1-2 weeks user transition period  
**Breaking Change:** Yes - this is a major version bump (v0.4.0 or v1.0.0)

---

## 1. Current State Analysis

### Current Structure
```
sombra/                         # Root directory (2.3G)
├── Cargo.toml                  # Rust crate (sombra v0.3.3)
├── package.json                # Node.js package (sombradb v0.3.3)
├── pyproject.toml              # Python package (sombra v0.3.3)
├── src/                        # 113 Rust source files
│   ├── bindings.rs             # NAPI bindings for Node.js
│   ├── python.rs               # PyO3 bindings for Python
│   ├── lib.rs                  # Core Rust library
│   └── ...
├── npm/                        # Platform-specific npm packages
├── python/                     # Python wrapper module
├── examples/                   # Mixed-language examples
├── tests/                      # Rust tests
├── test/                       # Node.js tests
├── docs/                       # Shared documentation
└── .github/workflows/          # CI/CD pipelines
```

### Problems with Current Structure
1. **Release-please can't differentiate packages** - All at root path `.`
2. **Unclear ownership** - Which files belong to which package?
3. **Mixed language concerns** - Rust bindings code mixed with core library
4. **Confusing for contributors** - Where do I add Python tests?
5. **Build conflicts** - `npm install` runs at root, affects Rust builds

---

## 2. Proposed Monorepo Structure

### Target Structure
```
sombra/                         # Monorepo root
├── packages/
│   ├── core/                   # Rust core library
│   │   ├── Cargo.toml          # name = "sombra"
│   │   ├── src/
│   │   │   ├── lib.rs          # Pure Rust API (no bindings)
│   │   │   ├── db/
│   │   │   ├── storage/
│   │   │   ├── index/
│   │   │   └── ...
│   │   ├── benches/
│   │   ├── tests/              # Rust integration tests
│   │   ├── examples/           # Rust-only examples
│   │   ├── docs/               # Rust-specific docs
│   │   └── README.md
│   │
│   ├── nodejs/                 # Node.js bindings
│   │   ├── Cargo.toml          # name = "sombradb-native"
│   │   ├── package.json        # name = "sombradb"
│   │   ├── src/
│   │   │   └── lib.rs          # NAPI bindings (cdylib)
│   │   ├── npm/                # Platform packages
│   │   ├── test/               # Node.js tests
│   │   ├── examples/           # Node.js examples
│   │   ├── index.js
│   │   ├── sombra.d.ts
│   │   └── README.md
│   │
│   └── python/                 # Python bindings
│       ├── Cargo.toml          # name = "sombra-python"
│       ├── pyproject.toml      # name = "sombra"
│       ├── src/
│       │   └── lib.rs          # PyO3 bindings (cdylib)
│       ├── python/             # Python wrapper code
│       ├── tests/              # Python tests
│       ├── examples/           # Python examples
│       └── README.md
│
├── docs/                       # Shared documentation
│   ├── architecture.md
│   ├── data_model.md
│   └── ...
│
├── scripts/                    # Build scripts
│   ├── build-all.sh
│   ├── release.sh
│   └── ...
│
├── .github/
│   └── workflows/
│       ├── ci-rust.yml
│       ├── ci-nodejs.yml
│       ├── ci-python.yml
│       ├── release-please.yml
│       ├── publish-rust.yml
│       ├── publish-nodejs.yml
│       └── publish-python.yml
│
├── Cargo.toml                  # Workspace root
├── README.md                   # Monorepo overview
├── COMPATIBILITY.md
├── release-please-config.json
├── .release-please-manifest.json
└── LICENSE
```

### Key Design Decisions

#### Cargo Workspace Configuration
```toml
# Root Cargo.toml
[workspace]
members = [
    "packages/core",
    "packages/nodejs",
    "packages/python",
]
resolver = "2"

[workspace.package]
version = "0.4.0"
edition = "2021"
license = "MIT"
authors = ["mask <maskdotdev@gmail.com>"]
repository = "https://github.com/maskdotdev/sombra"

[workspace.dependencies]
# Shared dependencies for all packages
sombra = { path = "packages/core", version = "0.4.0" }
```

#### Package Dependencies
```toml
# packages/nodejs/Cargo.toml
[package]
name = "sombradb-native"
version.workspace = true

[dependencies]
sombra = { workspace = true }
napi = "3.0.0"
napi-derive = "3.0.0"

# packages/python/Cargo.toml
[package]
name = "sombra-python"
version.workspace = true

[dependencies]
sombra = { workspace = true }
pyo3 = "0.27"
```

---

## 3. Migration Steps

### Phase 1: Preparation (Day 1, Morning)

#### 1.1 Create Feature Branch
```bash
git checkout -b feat/monorepo-restructure
```

#### 1.2 Document Current State
```bash
# Backup critical files
cp Cargo.toml Cargo.toml.bak
cp package.json package.json.bak
cp pyproject.toml pyproject.toml.bak

# Document current file structure
tree -L 3 > structure-before.txt
```

#### 1.3 Create Directory Structure
```bash
mkdir -p packages/core packages/nodejs packages/python
```

### Phase 2: Move Core Rust Library (Day 1, Afternoon)

#### 2.1 Move Core Files
```bash
# Move source files (excluding bindings)
git mv src packages/core/src

# Move core Rust files
git mv Cargo.toml packages/core/
git mv build.rs packages/core/ 2>/dev/null || true

# Move core docs
git mv benches packages/core/
git mv tests packages/core/

# Create core examples (Rust only)
mkdir packages/core/examples
mv examples/social_graph.rs packages/core/examples/
mv examples/code_analysis.rs packages/core/examples/
mv examples/transaction_handling.rs packages/core/examples/
mv examples/performance_metrics_demo.rs packages/core/examples/
```

#### 2.2 Split Bindings from Core
```bash
# Extract NAPI bindings to nodejs package
mkdir -p packages/nodejs/src
# bindings.rs will be moved to packages/nodejs/src/lib.rs

# Extract PyO3 bindings to python package  
mkdir -p packages/python/src
# python.rs will be moved to packages/python/src/lib.rs
```

#### 2.3 Update Core Cargo.toml
- Remove `napi`, `pyo3` dependencies (core is pure Rust)
- Remove `cdylib` from `crate-type`
- Remove `napi` and `python` features
- Keep only `["rlib"]` for library builds

### Phase 3: Setup Node.js Package (Day 2, Morning)

#### 3.1 Move Node.js Files
```bash
# Move Node.js specific files
git mv package.json packages/nodejs/
git mv package-lock.json packages/nodejs/
git mv npm packages/nodejs/
git mv test packages/nodejs/
git mv index.js packages/nodejs/
git mv index-napi.js packages/nodejs/ 2>/dev/null || true
git mv sombra.d.ts packages/nodejs/
git mv bin packages/nodejs/

# Move Node.js examples
mkdir packages/nodejs/examples
mv examples/nodejs-example.js packages/nodejs/examples/
mv examples/typescript-example.ts packages/nodejs/examples/
mv examples/code-analysis packages/nodejs/examples/
```

#### 3.2 Create Node.js Cargo.toml
```toml
[package]
name = "sombradb-native"
version = "0.4.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
sombra = { path = "../core" }
napi = { version = "3.0.0", features = ["serde-json"] }
napi-derive = "3.0.0"

[build-dependencies]
napi-build = "2"
```

#### 3.3 Create Node.js src/lib.rs
Move content from `src/bindings.rs` to `packages/nodejs/src/lib.rs`

#### 3.4 Update package.json
```json
{
  "name": "sombradb",
  "version": "0.4.0",
  "scripts": {
    "build": "napi build --platform --release --manifest-path Cargo.toml",
    "prepublishOnly": "napi prepublish -t npm --skip-gh-release"
  }
}
```

### Phase 4: Setup Python Package (Day 2, Afternoon)

#### 4.1 Move Python Files
```bash
# Move Python specific files
git mv pyproject.toml packages/python/
git mv python packages/python/
git mv uv.lock packages/python/ 2>/dev/null || true

# Move Python examples
mkdir packages/python/examples
mv examples/social_network.py packages/python/examples/

# Move Python tests
mkdir packages/python/tests
mv tests/python_*.py packages/python/tests/
```

#### 4.2 Create Python Cargo.toml
```toml
[package]
name = "sombra-python"
version = "0.4.0"
edition = "2021"

[lib]
name = "sombra"
crate-type = ["cdylib"]

[dependencies]
sombra = { path = "../core" }
pyo3 = { version = "0.27", features = ["extension-module"] }
```

#### 4.3 Create Python src/lib.rs
Move content from `src/python.rs` to `packages/python/src/lib.rs`

#### 4.4 Update pyproject.toml
```toml
[tool.maturin]
features = []  # Remove "python" feature
manifest-path = "Cargo.toml"
module-name = "sombra.sombra"
```

### Phase 5: Create Workspace Root (Day 3, Morning)

#### 5.1 Create Root Cargo.toml
```toml
[workspace]
members = ["packages/core", "packages/nodejs", "packages/python"]
resolver = "2"

[workspace.package]
version = "0.4.0"
edition = "2021"
license = "MIT"
authors = ["mask <maskdotdev@gmail.com>"]
repository = "https://github.com/maskdotdev/sombra"

[workspace.dependencies]
sombra = { path = "packages/core", version = "0.4.0" }
```

#### 5.2 Update Documentation
```bash
# Update README.md with new structure
# Update COMPATIBILITY.md with v0.4.0 references
# Create packages/*/README.md for each package
```

#### 5.3 Update Release-Please Config
```json
{
  "packages": {
    "packages/core": {
      "component": "sombra",
      "package-name": "sombra",
      "release-type": "rust"
    },
    "packages/nodejs": {
      "component": "sombradb",
      "package-name": "sombradb",
      "release-type": "node"
    },
    "packages/python": {
      "component": "sombra-py",
      "package-name": "sombra",
      "release-type": "python"
    }
  }
}
```

### Phase 6: Update CI/CD Workflows (Day 3, Afternoon)

#### 6.1 Split CI Workflows

**`.github/workflows/ci-rust.yml`**
```yaml
name: CI - Rust Core

on:
  pull_request:
    paths:
      - 'packages/core/**'
      - 'Cargo.toml'
      - '.github/workflows/ci-rust.yml'

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy, rustfmt
      - name: Cargo fmt
        run: cargo fmt --manifest-path packages/core/Cargo.toml -- --check
      - name: Clippy
        run: cargo clippy --manifest-path packages/core/Cargo.toml

  test:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v5
      - uses: dtolnay/rust-toolchain@stable
      - name: Run tests
        run: cargo test --manifest-path packages/core/Cargo.toml
```

**`.github/workflows/ci-nodejs.yml`**
```yaml
name: CI - Node.js Bindings

on:
  pull_request:
    paths:
      - 'packages/nodejs/**'
      - 'packages/core/**'
      - '.github/workflows/ci-nodejs.yml'

jobs:
  build:
    strategy:
      matrix:
        settings:
          - host: macos-13
            target: x86_64-apple-darwin
          - host: windows-latest
            target: x86_64-pc-windows-msvc
          - host: ubuntu-latest
            target: x86_64-unknown-linux-gnu
          - host: macos-latest
            target: aarch64-apple-darwin
    runs-on: ${{ matrix.settings.host }}
    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-node@v5
        with:
          node-version: 22
          cache: npm
          cache-dependency-path: packages/nodejs/package-lock.json
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: ${{ matrix.settings.target }}
      - name: Install dependencies
        working-directory: packages/nodejs
        run: npm ci
      - name: Build
        working-directory: packages/nodejs
        run: npm run build -- --target ${{ matrix.settings.target }}
      - uses: actions/upload-artifact@v4
        with:
          name: bindings-${{ matrix.settings.target }}
          path: packages/nodejs/*.node

  test:
    needs: build
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
        node: ['20', '22']
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-node@v5
        with:
          node-version: ${{ matrix.node }}
      - uses: actions/download-artifact@v5
        with:
          path: packages/nodejs
      - name: Install dependencies
        working-directory: packages/nodejs
        run: npm ci
      - name: Test
        working-directory: packages/nodejs
        run: npm test
```

**`.github/workflows/ci-python.yml`**
```yaml
name: CI - Python Bindings

on:
  pull_request:
    paths:
      - 'packages/python/**'
      - 'packages/core/**'
      - '.github/workflows/ci-python.yml'

jobs:
  test:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
        python: ['3.11', '3.12', '3.13']
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python }}
      - uses: dtolnay/rust-toolchain@stable
      - name: Build and test
        working-directory: packages/python
        run: |
          pip install maturin pytest
          maturin develop
          pytest tests/
```

#### 6.2 Update Publish Workflows

**`.github/workflows/publish-rust.yml`**
```yaml
name: Publish Rust Crate

on:
  push:
    tags:
      - 'sombra-v*'

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: dtolnay/rust-toolchain@stable
      - name: Publish to crates.io
        working-directory: packages/core
        run: cargo publish --token ${{ secrets.CARGO_REGISTRY_TOKEN }}
```

**`.github/workflows/publish-nodejs.yml`** - Update paths:
```yaml
working-directory: packages/nodejs
cache-dependency-path: packages/nodejs/package-lock.json
```

**`.github/workflows/publish-python.yml`** - Update paths:
```yaml
working-directory: packages/python
```

#### 6.3 Update Release-Please Workflow
```yaml
- uses: googleapis/release-please-action@v4
  with:
    config-file: release-please-config.json
    manifest-file: .release-please-manifest.json
```

---

## 4. Breaking Changes & Migration Guide

### For Rust Users

**Before (v0.3.3):**
```toml
[dependencies]
sombra = "0.3.3"
```

**After (v0.4.0):**
```toml
[dependencies]
sombra = "0.4.0"  # No change to API!
```

**Impact:** ✅ **No code changes required** - Package name and API unchanged

### For Node.js Users

**Before (v0.3.3):**
```bash
npm install sombradb@0.3.3
```

**After (v0.4.0):**
```bash
npm install sombradb@0.4.0  # No change!
```

**Impact:** ✅ **No code changes required** - Package name and API unchanged

### For Python Users

**Before (v0.3.3):**
```bash
pip install sombra==0.3.3
```

**After (v0.4.0):**
```bash
pip install sombra==0.4.0  # No change!
```

**Impact:** ✅ **No code changes required** - Package name and API unchanged

### For Contributors

**Before:**
```bash
git clone https://github.com/maskdotdev/sombra
cd sombra
cargo test        # Tests Rust
npm test          # Tests Node.js
```

**After:**
```bash
git clone https://github.com/maskdotdev/sombra
cd sombra

# Test Rust core
cargo test --manifest-path packages/core/Cargo.toml

# Test Node.js bindings
cd packages/nodejs && npm test

# Test Python bindings
cd packages/python && maturin develop && pytest

# Or test everything
./scripts/test-all.sh
```

---

## 5. Rollout Strategy

### Timeline

**Week 1: Preparation & Migration**
- **Day 1-3:** Execute migration steps (Phases 1-6)
- **Day 4:** Local testing of all packages
- **Day 5:** Fix any issues, create PR

**Week 2: Testing & Review**
- **Day 1-2:** Community review of PR
- **Day 3-4:** Address feedback, update docs
- **Day 5:** Merge to main, create pre-release

**Week 3: Soft Launch**
- Publish `v0.4.0-beta.1` to all registries
- Ask early adopters to test
- Monitor for issues

**Week 4: Production Release**
- Publish `v0.4.0` to all registries
- Announce on GitHub, social media, etc.
- Update documentation sites

### Risk Mitigation

#### Risk 1: Build Breakage
**Mitigation:**
- Test all platforms locally with Docker/VMs
- Keep comprehensive test suite
- CI must pass before merge

#### Risk 2: User Confusion
**Mitigation:**
- Clear migration guide in CHANGELOG
- Pin v0.3.3 docs as "legacy"
- Respond quickly to GitHub issues

#### Risk 3: CI/CD Issues
**Mitigation:**
- Test workflows on feature branch first
- Keep old workflows as backup (`.bak`)
- Can rollback tags if publish fails

#### Risk 4: Lost Git History
**Mitigation:**
- Use `git mv` (preserves history)
- Test with `git log --follow`
- Document file movements in commit

### Rollback Plan

If major issues discovered after release:

1. **Immediately:** Yank broken versions from registries
   ```bash
   cargo yank --vers 0.4.0 sombra
   npm unpublish sombradb@0.4.0
   # (PyPI doesn't support unpublish, but can yank)
   ```

2. **Short-term:** Publish hotfix `v0.3.4` with critical fixes

3. **Long-term:** Create `v0.3.x` maintenance branch for legacy support

---

## 6. Success Metrics

### Technical Metrics
- ✅ All CI workflows pass
- ✅ Release-please creates separate PRs per component
- ✅ Each package publishes independently
- ✅ No increase in binary sizes
- ✅ Test coverage remains ≥90%

### User Metrics
- ✅ Zero breaking API changes (except imports for contributors)
- ✅ Migration guide clarity (measured by GitHub issues)
- ✅ Download counts remain stable or increase
- ✅ No significant bug reports related to restructuring

---

## 7. Next Steps - Action Items

### Immediate Actions (Before Starting)

1. **Create tracking issue on GitHub**
   ```markdown
   Title: [RFC] Restructure repository into proper monorepo
   - Link to this plan
   - Request community feedback
   - Set 1-week feedback period
   ```

2. **Communicate with users**
   - Post in discussions
   - Tag major contributors
   - Explain benefits

3. **Backup critical state**
   ```bash
   git tag v0.3.3-final-pre-monorepo
   git push origin v0.3.3-final-pre-monorepo
   ```

### Implementation Checklist

Use this checklist when executing:

- [ ] Create `feat/monorepo-restructure` branch
- [ ] Document current state
- [ ] Create `packages/` directory structure
- [ ] Move core Rust library to `packages/core/`
- [ ] Split out Node.js bindings to `packages/nodejs/`
- [ ] Split out Python bindings to `packages/python/`
- [ ] Create workspace root `Cargo.toml`
- [ ] Update all `Cargo.toml` files with workspace deps
- [ ] Update CI workflows (3 separate files)
- [ ] Update publish workflows (3 files)
- [ ] Update release-please config
- [ ] Update all READMEs
- [ ] Update COMPATIBILITY.md
- [ ] Create MIGRATION.md guide
- [ ] Test Rust: `cargo test -p sombra`
- [ ] Test Node.js: `cd packages/nodejs && npm test`
- [ ] Test Python: `cd packages/python && maturin develop && pytest`
- [ ] Run benchmarks to verify no performance regression
- [ ] Create comprehensive PR description
- [ ] Request reviews from maintainers
- [ ] Address feedback
- [ ] Merge to main
- [ ] Publish beta versions
- [ ] Monitor for issues
- [ ] Publish stable v0.4.0

---

## 8. Alternative: Gradual Migration

If full restructure is too risky, consider **gradual approach**:

### Step 1: Add Workspace (No File Moves)
```toml
# Root Cargo.toml
[workspace]
members = ["."]  # Keep everything at root temporarily
```

### Step 2: Move One Package at a Time
- **Month 1:** Move Python to `packages/python/`
- **Month 2:** Move Node.js to `packages/nodejs/`
- **Month 3:** Move core to `packages/core/`

**Pros:** Less risky, easier to rollback  
**Cons:** Takes longer, confusing intermediate state

---

## 9. Recommendation

**I recommend proceeding with the full restructure because:**

1. ✅ **Clean break** - v0.4.0 is a natural breaking point
2. ✅ **Solves release-please issue** - Primary goal
3. ✅ **Better long-term** - Clear structure for years to come
4. ✅ **Low user impact** - No API changes
5. ✅ **Current state already confusing** - Can't get worse

**The alternative (Option D - custom tagging) would be technical debt that needs solving eventually.**

---

## 10. Questions & Decisions Needed

Before proceeding, please decide:

1. **Version number:** Use `v0.4.0` or `v1.0.0`?
2. **Timeline:** Start immediately or wait for community feedback?
3. **Communication:** Create RFC issue first or announce after?
4. **Testing:** Beta release first or direct to stable?

---

**Document Status:** Draft  
**Last Updated:** 2025-10-23  
**Owner:** @maskdotdev
