# Contributing Guidelines

This document codifies the expectations for code quality and architecture so that new changes slot cleanly into the Graphite codebase.

## Coding Standards

- **Edition & Style:** Use Rust 2021 with default `rustfmt` formatting. When `rustfmt` cannot express a layout decision, prefer clarity over line-length perfection.
- **Naming:** Favor explicit module and type names (`RecordPage`, `FreeListHead`) over abbreviations. Local variables can be shorter when scoped tightly.
- **Documentation:** Add `///` docs for public APIs and brief inline comments only when the control flow or data manipulation is non-obvious.
- **Testing:** All behavioral changes require unit tests or integration tests. For bug fixes, add a regression test that fails before the fix.
- **Unsafe Code:** Avoid `unsafe` unless absolutely necessary; consult the maintainers before introducing it. Every `unsafe` block must be documented.

## Error Handling Strategy

- Use the crate-wide `Result<T>` alias and `GraphError` enum (`src/error.rs`) for all fallible operations.
- Propagate lower-level errors with `?` and enrich them with context at module boundaries using `GraphError::Io`, `GraphError::Corruption`, etc.
- Reserve `panic!` for unrecoverable programmer errors (e.g., logic bugs), not for I/O or user input issues.
- When adding new error variants, ensure they round-trip through `Display` and update any pattern matches that enumerate existing variants.

## Module Boundaries

- `src/model.rs` owns the in-memory graph primitives (`Node`, `Edge`) and should remain serialization-agnostic.
- `src/storage` encapsulates on-disk representation concerns (record layout, serializers, header). Keep it free of graph semantics.
- `src/pager` handles file I/O, page caching, and durability plumbing. Higher layers should not access raw file handles directly.
- `src/db.rs` orchestrates graph workflows (ID allocation, adjacency maintenance) by composing `model`, `storage`, and `pager`.
- New functionality should fit into this layering; if adding cross-cutting features (e.g., WAL), prefer creating a dedicated module that `db` orchestrates rather than collapsing boundaries.

## Workflow Expectations

- Run `cargo fmt` and `cargo test` before opening a change.
- Keep commits focused: one logical change per commit with a clear message.
- Document user-visible changes in `plan.md` or a dedicated doc under `docs/` when appropriate.

