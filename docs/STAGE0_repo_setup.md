# Stage 0 — Repository & Monorepo Setup

This document mirrors the living specification tracked in
`docs/build/stage_0.md`. Refer to that file for the detailed roadmap and status
of Stage 0 tasks. The repository layout created in this stage includes:

- A Cargo workspace containing all Rust crates (`crates/sombra-*`) and the
  Python binding crate (`bindings/python`).
- Toolchain pinning and shared configuration (`rust-toolchain.toml`,
  `.cargo/config.toml`).
- Tooling defaults such as `.editorconfig`, `.pre-commit-config.yaml`, and CI
  workflows under `.github/workflows/`.
- Stub implementations for the foundational crates (`sombra-types`, `sombra-bytes`,
  `sombra-checksum`, `sombra-io`, `sombra-pager`, `sombra-core`, `sombra-ffi`,
  `sombra-cli`, `sombra-testkit`, `sombra-bench`, and `sombra-fuzz`).
- A minimal Python package scaffold in `bindings/python/` built with `maturin`
  and `pyo3`.

Updates to the Stage 0 plan should be made in `docs/build/stage_0.md` and
summarised here as needed for contributors.
