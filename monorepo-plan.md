## 📋 Sombra Monorepo Migration Plan

### Current State Analysis

**Current versions (all synced):** `0.3.3`
- **Rust crate** (`sombra`): Published to crates.io
- **Node.js package** (`sombradb`): Published to npm with platform-specific packages
- **Python package** (`sombra`): Published to PyPI with maturin

**Current commit style:** You're already using conventional commits! (e.g., `fix: use @sombradb scoped packages`)

**Current workflow:** Tag-based releases (`v*`) trigger all three ecosystems to publish simultaneously

---

### Phase 1: Transition to Independent Versioning

#### 1.1 Create `release-please-config.json`

```json
{
  "$schema": "https://raw.githubusercontent.com/googleapis/release-please/main/schemas/config.json",
  "packages": {
    ".": {
      "component": "sombra",
      "package-name": "sombra",
      "release-type": "rust",
      "changelog-path": "CHANGELOG-rust.md"
    },
    "js": {
      "component": "sombradb",
      "package-name": "sombradb", 
      "release-type": "node",
      "changelog-path": "CHANGELOG-js.md"
    },
    "python": {
      "component": "sombra-py",
      "package-name": "sombra",
      "release-type": "python",
      "changelog-path": "CHANGELOG-python.md"
    }
  },
  "bootstrap-sha": "e89886d5c98e13a51b270940557d7c0aef5b2eec",
  "release-search-depth": 500,
  "pull-request-title-pattern": "chore: release ${component} ${version}",
  "changelog-sections": [
    {"type": "feat", "section": "Features"},
    {"type": "fix", "section": "Bug Fixes"},
    {"type": "perf", "section": "Performance Improvements"},
    {"type": "docs", "section": "Documentation"},
    {"type": "chore", "section": "Miscellaneous", "hidden": true}
  ]
}
```

#### 1.2 Create `.release-please-manifest.json`

```json
{
  ".": "0.3.3",
  "js": "0.3.3",
  "python": "0.3.3"
}
```

This tracks the **current** version of each package.

#### 1.3 Update Conventional Commit Scopes

Moving forward, use these scopes in commits:

- `feat(core):` / `fix(core):` → Bumps **Rust crate** version
- `feat(js):` / `fix(js):` → Bumps **Node.js package** version  
- `feat(py):` / `fix(py):` → Bumps **Python package** version
- `feat(all):` → Bumps **all three** (use sparingly)

**Examples:**
```bash
git commit -m "feat(py): add async transaction support"
git commit -m "fix(js): memory leak in query builder"
git commit -m "perf(core): optimize B-tree node splitting"
git commit -m "feat(core)!: remove deprecated transaction API

BREAKING CHANGE: The old sync transaction API has been removed. Use async transactions."
```

---

### Phase 2: Setup Release Please Workflow

#### 2.1 Create `.github/workflows/release-please.yml`

```yaml
name: Release Please

on:
  push:
    branches:
      - main

permissions:
  contents: write
  pull-requests: write

jobs:
  release-please:
    runs-on: ubuntu-latest
    outputs:
      releases_created: ${{ steps.release.outputs.releases_created }}
      rust_release: ${{ steps.release.outputs['--release_created'] }}
      js_release: ${{ steps.release.outputs['js--release_created'] }}
      py_release: ${{ steps.release.outputs['python--release_created'] }}
      rust_tag: ${{ steps.release.outputs['--tag_name'] }}
      js_tag: ${{ steps.release.outputs['js--tag_name'] }}
      py_tag: ${{ steps.release.outputs['python--tag_name'] }}
    steps:
      - uses: google-github-actions/release-please-action@v4
        id: release
        with:
          token: ${{ secrets.GITHUB_TOKEN }}
          config-file: release-please-config.json
          manifest-file: .release-please-manifest.json
```

**What this does:**
- Monitors commits on `main`
- Opens a PR like "chore: release sombradb 0.3.4" when it detects `feat(js):` or `fix(js):` commits
- Updates `package.json`, creates `CHANGELOG-js.md`
- When merged, creates a GitHub Release with tag `sombradb-v0.3.4`

---

### Phase 3: Create Component-Specific Publishing Workflows

#### 3.1 Publish Rust Crate: `.github/workflows/publish-rust.yml`

```yaml
name: Publish Rust Crate

on:
  release:
    types: [created]

jobs:
  publish-crate:
    if: startsWith(github.ref, 'refs/tags/sombra-v')
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Install Rust
        uses: dtolnay/rust-toolchain@stable
      
      - name: Run tests
        run: cargo test --lib
      
      - name: Publish to crates.io
        run: cargo publish --token ${{ secrets.CARGO_REGISTRY_TOKEN }}
        env:
          CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}
```

**Trigger:** Tag format `sombra-v0.3.4`

---

#### 3.2 Publish Node.js Package: `.github/workflows/publish-npm.yml`

```yaml
name: Publish npm Package

on:
  release:
    types: [created]

env:
  DEBUG: napi:*
  APP_NAME: sombradb
  MACOSX_DEPLOYMENT_TARGET: '10.13'

jobs:
  build:
    if: startsWith(github.ref, 'refs/tags/sombradb-v')
    strategy:
      fail-fast: false
      matrix:
        settings:
          - host: macos-13
            target: x86_64-apple-darwin
            build: npm run build -- --target x86_64-apple-darwin
          - host: windows-latest
            target: x86_64-pc-windows-msvc
            build: npm run build -- --target x86_64-pc-windows-msvc
          - host: ubuntu-latest
            target: x86_64-unknown-linux-gnu
            build: npm run build -- --target x86_64-unknown-linux-gnu --use-napi-cross
          - host: macos-latest
            target: aarch64-apple-darwin
            build: npm run build -- --target aarch64-apple-darwin
    runs-on: ${{ matrix.settings.host }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 22
          cache: npm
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: ${{ matrix.settings.target }}
      - run: npm ci
      - run: ${{ matrix.settings.build }}
      - uses: actions/upload-artifact@v4
        with:
          name: bindings-${{ matrix.settings.target }}
          path: |
            *.node
            index.js
            index.d.ts

  publish:
    needs: build
    runs-on: ubuntu-latest
    permissions:
      contents: write
      id-token: write
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 22
          registry-url: 'https://registry.npmjs.org'
      - run: npm ci
      - run: npx napi create-npm-dirs
      - uses: actions/download-artifact@v4
        with:
          path: artifacts
      - run: npm run artifacts
      - name: Publish to npm
        run: |
          npm config set provenance true
          echo "//registry.npmjs.org/:_authToken=$NPM_TOKEN" >> ~/.npmrc
          npm publish --access public
        env:
          NPM_TOKEN: ${{ secrets.NPM_TOKEN }}
```

**Trigger:** Tag format `sombradb-v0.3.4`

---

#### 3.3 Publish Python Package: `.github/workflows/publish-python.yml`

```yaml
name: Publish Python Package

on:
  release:
    types: [created]

jobs:
  build-wheels:
    if: startsWith(github.ref, 'refs/tags/sombra-py-v')
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
        python-version: ['3.11', '3.12', '3.13', '3.14']
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
      
      - name: Install Rust
        uses: dtolnay/rust-toolchain@stable
      
      - name: Build wheel
        run: |
          python -m pip install maturin
          python -m maturin build --release -F python
      
      - uses: actions/upload-artifact@v4
        with:
          name: wheel-${{ matrix.os }}-py${{ matrix.python-version }}
          path: target/wheels/*.whl

  publish-pypi:
    needs: build-wheels
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v4
        with:
          path: wheels
          pattern: wheel-*
      
      - name: Flatten wheels
        run: find wheels -name "*.whl" -exec cp {} . \;
      
      - uses: pypa/gh-action-pypi-publish@release/v1
        with:
          password: ${{ secrets.PYPI_API_TOKEN }}
          packages-dir: .
```

**Trigger:** Tag format `sombra-py-v0.3.4`

---

### Phase 4: Documentation

#### 4.1 Create `COMPATIBILITY.md`

```markdown
# 📦 Sombra Version Compatibility Matrix

The Sombra ecosystem consists of three independently-versioned packages:

- **`sombra`** (Rust crate on crates.io)
- **`sombradb`** (Node.js/TypeScript on npm)
- **`sombra`** (Python on PyPI)

## Current Versions

| Rust (`sombra`) | Node.js (`sombradb`) | Python (`sombra`) | Release Date | Notes |
|:----------------|:---------------------|:------------------|:-------------|:------|
| `0.3.3`         | `0.3.3`              | `0.3.3`           | 2024-XX-XX   | Initial independent versioning |

## Compatibility Rules

- **Breaking changes in Core (Rust)** require updates to BOTH bindings
- **Non-breaking Core changes** do NOT require binding updates
- **Binding-specific features** can be released independently

## Finding Compatible Versions

1. **If using Rust directly:** Use the latest `sombra` version from crates.io
2. **If using Node.js:** Check the "Supported Core Version" in the npm package README
3. **If using Python:** Check the "Supported Core Version" in the PyPI package description

## Version History

### `sombra` (Rust Core)
- `0.3.3` - Current stable release

### `sombradb` (Node.js)
- `0.3.3` - Matches core `0.3.3`

### `sombra` (Python)
- `0.3.3` - Matches core `0.3.3`
```

#### 4.2 Update `README.md`

Add this section after installation instructions:

```markdown
## 📦 Version Compatibility

Sombra uses **independent versioning** for each language binding. See the [Compatibility Matrix](COMPATIBILITY.md) to ensure you're using compatible versions across ecosystems.

**Quick Reference:**
- Rust: `cargo add sombra@0.3.3`
- Node.js: `npm install sombradb@0.3.3`
- Python: `pip install sombra==0.3.3`
```

---

### Phase 5: Migration Workflow

#### Step-by-step migration process:

**Week 1: Setup (No Breaking Changes)**
1. ✅ Create `release-please-config.json`
2. ✅ Create `.release-please-manifest.json`
3. ✅ Create `COMPATIBILITY.md`
4. ✅ Update `README.md` with versioning notice
5. ✅ Commit: `docs: add independent versioning documentation`

**Week 2: Workflow Creation**
1. ✅ Create `.github/workflows/release-please.yml`
2. ✅ Merge to `main`
3. ✅ Wait for release-please bot to scan history (it will create initial PRs)
4. ✅ **Do NOT merge** these PRs yet—they're just for testing

**Week 3: Split Publishing Workflows**
1. ✅ Create `publish-rust.yml`, `publish-npm.yml`, `publish-python.yml`
2. ✅ **Keep** existing `release.yml` as backup (rename to `release-legacy.yml.bak`)
3. ✅ Test by making a `feat(js):` commit
4. ✅ Verify release-please opens a PR
5. ✅ Manually add new row to `COMPATIBILITY.md` in that PR
6. ✅ Merge the PR
7. ✅ Verify the tag triggers only the npm workflow

**Week 4: Full Cutover**
1. ✅ Delete `release-legacy.yml.bak`
2. ✅ Update team docs/CONTRIBUTING.md about new commit format
3. ✅ Announce in project Discord/Slack

---

### Phase 6: Secrets Configuration

Add these to **Settings → Secrets and variables → Actions**:

- `CARGO_REGISTRY_TOKEN` (from crates.io)
- `NPM_TOKEN` (from npmjs.com)
- `PYPI_API_TOKEN` (from pypi.org)

---

### Benefits of This Approach

1. **No more version lock-step:** Fix a Python-only bug without cutting new Rust + Node.js releases
2. **Clear changelog per ecosystem:** `CHANGELOG-python.md` only shows Python changes
3. **Automated version bumping:** Conventional Commits → automatic `package.json`/`Cargo.toml` updates
4. **GitHub Releases per component:** Users can subscribe to just Node.js releases
5. **Better semver adherence:** A new JS method is `minor` for JS, but doesn't force Rust to bump

---

### Key Differences from Generic Plan

**Sombra-specific adaptations:**

1. **Keep your existing CI (`ci.yml`)** for building/testing—it's solid
2. **Use scoped npm packages** (`@sombradb/*`) if you want to avoid spam detection (already handling this)
3. **Python package name collision:** Both Rust crate and Python package are `sombra`—use `component` names to differentiate
4. **Complex npm publish:** Your NAPI multi-platform build is preserved in `publish-npm.yml`
5. **Bootstrap SHA:** Set to your latest commit (`e89886d`) so release-please starts fresh

---

### Sample Commit Flow (After Migration)

```bash
# Python-only feature
git commit -m "feat(py): add async query execution"
# → Opens PR: "chore: release sombra-py 0.3.4"
# → Merging creates tag: sombra-py-v0.3.4
# → Publishes only to PyPI

# Node.js bug fix  
git commit -m "fix(js): correct TypeScript types for SombraDB.query()"
# → Opens PR: "chore: release sombradb 0.3.4"
# → Merging creates tag: sombradb-v0.3.4
# → Publishes only to npm

# Core breaking change affecting all bindings
git commit -m "feat(core)!: change transaction commit to return Result

BREAKING CHANGE: commit() now returns Result<(), Error> instead of ()
Bindings must update error handling."
# → Opens PR: "chore: release sombra 0.4.0"
# → You manually update bindings in same commit
# → Tag: sombra-v0.4.0 (Rust only)
# → Then commit `feat(js): adapt to core 0.4.0` → bumps sombradb to 0.4.0
# → Then commit `feat(py): adapt to core 0.4.0` → bumps sombra-py to 0.4.0
```

---
