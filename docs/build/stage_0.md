
## 📄 Document 1 — Stage 0: Repository & Monorepo Setup

**Outcome:** a reproducible, multi‑language monorepo skeleton with Rust workspace and binding package stubs for Python and Node/TypeScript, CI wired, and conventions set.

### 0. Goals & Non‑Goals

**Goals**

* Single monorepo with a **Cargo workspace** and sibling **Python**/**Node** bindings folders.
* Deterministic builds: pinned toolchains, lints, formatting, pre‑commit hooks.
* Cross‑platform CI for Rust + Python + Node.
* Ready for Stage 1 and Stage 2 development in parallel.

**Non‑Goals**

* No engine functionality yet (beyond compiling stubs).
* No publishing to PyPI/npm yet (release workflows may be stubbed but disabled).

---

### 1. Repository Layout

```
sombra/
├─ Cargo.toml                      # workspace + shared deps & lints
├─ rust-toolchain.toml             # pin Rust version
├─ .cargo/config.toml              # RUSTFLAGS/LTO and per-target tweaks
├─ crates/
│  ├─ sombra-types/                # ids, errors (stage 1 uses this)
│  ├─ sombra-bytes/                # varint/zigzag/BE helpers (stage 1)
│  ├─ sombra-checksum/             # checksum trait + crc32fast impl (stage 1)
│  ├─ sombra-io/                   # sync IO trait + std impl (stage 2)
│  ├─ sombra-pager/                # pager/cache + meta structs (stage 2)
│  ├─ sombra-core/                 # public Rust API surface (thin veneer)
│  ├─ sombra-ffi/                  # C ABI (for future languages)
│  ├─ sombra-cli/                  # REPL/CLI, import/export (later)
│  ├─ sombra-testkit/              # fixtures, golden tests, generators
│  ├─ sombra-bench/                # criterion benches (later)
│  └─ sombra-fuzz/                 # fuzz targets (later)
├─ bindings/
│  ├─ python/
│  │  ├─ pyproject.toml            # maturin + pyo3 (abi3)
│  │  ├─ README.md
│  │  └─ sombra/                   # package root
│  │     ├─ __init__.py            # re-exports, small helpers
│  │     └─ _native.rs             # Rust module compiled by maturin (stub)
│  └─ node/
│     ├─ package.json              # napi-rs + TypeScript façade
│     ├─ tsconfig.json
│     ├─ README.md
│     ├─ src/index.ts              # re-exports, types (stub)
│     └─ napi/                     # Rust crate for N-API addon
│        ├─ Cargo.toml
│        └─ src/lib.rs             # #[napi] stubs
├─ docs/
│  ├─ STAGE0_repo_setup.md         # this document
│  ├─ STAGE1_2_bytes_io_pager.md   # the next document
│  └─ CONTRIBUTING.md
├─ .github/
│  └─ workflows/
│     ├─ ci.yml                    # build+test matrix
│     ├─ lint.yml                  # rustfmt/clippy/ruff/eslint
│     └─ (release workflows later)
├─ .editorconfig
├─ .gitignore
└─ .pre-commit-config.yaml
```

---

### 2. Root Cargo Workspace

**`Cargo.toml` (root):**

```toml
[workspace]
members = [
  "crates/sombra-*",
  "bindings/python",
]

[workspace.package]
edition = "2021"
version = "0.0.0"
license = "MIT"

[workspace.dependencies]
thiserror = "1"
tracing = "0.1"
parking_lot = "0.12"
crc32fast = "1.4"
bytes = "1"
sombra-types = { path = "crates/sombra-types" }
sombra-bytes = { path = "crates/sombra-bytes" }
sombra-checksum = { path = "crates/sombra-checksum" }
sombra-io = { path = "crates/sombra-io" }
sombra-pager = { path = "crates/sombra-pager" }
sombra-core = { path = "crates/sombra-core" }
sombra-ffi = { path = "crates/sombra-ffi" }
sombra-cli = { path = "crates/sombra-cli" }
sombra-testkit = { path = "crates/sombra-testkit" }
sombra-bench = { path = "crates/sombra-bench" }
sombra-fuzz = { path = "crates/sombra-fuzz" }
sombra-py-native = { path = "bindings/python" }

[workspace.lints.rust]
unsafe_code = "deny"
missing_docs = "warn"
```

**`rust-toolchain.toml`:**

```toml
[toolchain]
channel = "1.80.0"
components = ["rustfmt", "clippy"]
profile = "minimal"
```

**`.cargo/config.toml`:**

```toml
[build]
rustflags = ["-Ctarget-cpu=native"]

[target.'cfg(target_os = "linux")']
rustflags = ["-Ctarget-cpu=native", "-Clink-arg=-Wl,-O1"]

[profile.release]
lto = "thin"
codegen-units = 1
panic = "abort"

[env]
PYO3_USE_ABI3_PY3 = "1"
```

---

### 3. Initial Crate Stubs

**`crates/sombra-types/src/lib.rs`**

```rust
#![forbid(unsafe_code)]
use std::fmt;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct NodeId(pub u64);
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct EdgeId(pub u64);
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct PageId(pub u64);
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Lsn(pub u64);

#[derive(thiserror::Error, Debug)]
pub enum SombraError {
    #[error("IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("corruption: {0}")]
    Corruption(&'static str),
    #[error("invalid argument: {0}")]
    Invalid(&'static str),
    #[error("not found")]
    NotFound,
}

pub type Result<T> = std::result::Result<T, SombraError>;

impl fmt::Display for NodeId { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) } }
```

Create empty/lib stubs for `sombra-bytes`, `sombra-checksum`, `sombra-io`, `sombra-pager`, and a thin `sombra-core` with just a `Db` placeholder that compiles.

---

### 4. Bindings Stubs

**Python (`bindings/python/pyproject.toml`)**

```toml
[build-system]
requires = ["maturin>=1.4,<2"]
build-backend = "maturin"

[project]
name = "sombra-py"
version = "0.0.0"
description = "Python bindings for Sombra Graph DB (experimental)"
requires-python = ">=3.8"
classifiers = ["Programming Language :: Python :: 3"]
readme = "README.md"

[tool.maturin]
module-name = "sombra._native"
bindings = "pyo3"
manifest-path = "Cargo.toml"
```

**Python Rust module stub (`bindings/python/sombra/_native.rs`)**

```rust
use pyo3::{prelude::*, wrap_pyfunction};

#[pyfunction]
fn version() -> PyResult<&'static str> { Ok("0.0.0-dev") }

#[pymodule]
fn _native(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    Ok(())
}
```

> ⚠️ **Node bindings** already live in `bindings/node/` and can be iterated on
> separately from Stage 0. No additional scaffold required here.

---

### 5. CI & Linting

**`.github/workflows/ci.yml`**
name: CI

on:
  push:
    branches: [main]
  pull_request:

env:
  CARGO_TERM_COLOR: always
  PYO3_USE_ABI3_PY3: "1"

jobs:
  rust:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: 1.80.0
          components: rustfmt, clippy
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --all -- --check
      - run: cargo clippy --all-targets --all-features -- -D warnings
      - run: cargo test --all --all-features

  python:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.10"
      - uses: PyO3/maturin-action@v1
        with:
          command: develop
          args: --release
          working-directory: bindings/python
      - run: python -c "import sombra; print(sombra.version())"
        working-directory: bindings/python
```

**`.github/workflows/lint.yml`** — lightweight pre-commit enforcement (ruff, typos, etc.).

---

### 6. Conventions

* **Commits**: Conventional Commits (`feat:`, `fix:`, `chore:`).
* **Code style**: `rustfmt`, `clippy -D warnings`, `ruff`, `eslint`.
* **Pre‑commit** (`.pre-commit-config.yaml`): run formatters & linters.

---

### 7. Acceptance Criteria (Stage 0)

* `cargo test --all` compiles and runs on Linux/macOS/Windows.
* `maturin develop --release` builds the Python wheel locally (dev mode).
* (Optional) `npm run build` succeeds for the Node bindings if touched.
* CI passes on Rust + Python jobs in GitHub Actions.
* No `unsafe` outside allowed low‑level crates (none yet).

---

### 8. Step‑By‑Step Checklist (for the coding agent)

* [ ] Create repo with the tree above.
* [ ] Add root Cargo workspace with lints and pinned toolchain.
* [ ] Add stub crates (`sombra-types`, `sombra-bytes`, `sombra-checksum`, `sombra-io`, `sombra-pager`, `sombra-core`).
* [ ] Add Python binding stub with maturin+pyo3; verify import works.
* [ ] (Optional) Add/refresh Node binding stub with napi-rs; verify `version()` works.
* [ ] Wire CI, ensure all jobs are green. ( NOT IMPORTANT YET )
* [ ] Commit `docs/` with this document and Stage 1/2 spec.
