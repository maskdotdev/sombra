# Contributing Guidelines

This document codifies the expectations for code quality and architecture so that new changes slot cleanly into the Sombra codebase.

## Release Process

Sombra uses [release-please](https://github.com/googleapis/release-please) for automated version management across three independent packages:
- **Rust core** (`sombra`) on crates.io
- **Node.js bindings** (`sombradb`) on npm  
- **Python bindings** (`sombra-py`) on PyPI

### Commit Message Format

Use [Conventional Commits](https://www.conventionalcommits.org/) with package-specific scopes:

- `feat(core): <description>` - New feature in Rust core → bumps Rust version
- `fix(js): <description>` - Bug fix in Node.js bindings → bumps npm version
- `feat(py): <description>` - New feature in Python bindings → bumps PyPI version
- `docs: <description>` - Documentation changes (no version bump)
- `chore: <description>` - Maintenance tasks (no version bump)

For breaking changes, add `!` after scope or include `BREAKING CHANGE:` in commit body:
```
feat(core)!: redesign transaction API

BREAKING CHANGE: Transaction.commit() now returns Result
```

### How Releases Work

1. **Make commits** with conventional format
2. **Release-please bot** automatically opens/updates a PR with:
   - Version bumps in `Cargo.toml`/`package.json`/`pyproject.toml`
   - Updated changelog
3. **Review the PR** and manually update `COMPATIBILITY.md` if bindings are affected
4. **Merge the PR** → release-please:
   - Creates a GitHub release and tag (e.g., `sombradb-v0.3.4`)
   - Automatically triggers the appropriate publish workflow(s) for changed packages
5. **Publishing workflows** build and publish to crates.io/npm/PyPI in parallel

Note: The publish workflows are triggered automatically by the release-please workflow using reusable workflow calls, not by tag push events. Each package (Rust, npm, Python) publishes independently based on which packages had changes.

### Version Compatibility

See [COMPATIBILITY.md](../COMPATIBILITY.md) for the version compatibility matrix between packages.

## Coding Standards

- **Edition & Style:** Use Rust 2021 with default `rustfmt` formatting. When `rustfmt` cannot express a layout decision, prefer clarity over line-length perfection.
- **Naming:** Favor explicit module and type names (`RecordPage`, `FreeListHead`) over abbreviations. Local variables can be shorter when scoped tightly.
- **Documentation:** Add `///` docs for public APIs and brief inline comments only when the control flow or data manipulation is non-obvious.
- **Testing:** All behavioral changes require unit tests or integration tests. For bug fixes, add a regression test that fails before the fix.
- **Unsafe Code:** Avoid `unsafe` unless absolutely necessary; consult the maintainers before introducing it. Every `unsafe` block must be documented.

## Error Handling Strategy

- Use the crate-wide `Result<T>` alias and `GraphError` enum (`packages/core/src/error.rs`) for all fallible operations.
- Propagate lower-level errors with `?` and enrich them with context at module boundaries using `GraphError::Io`, `GraphError::Corruption`, etc.
- Reserve `panic!` for unrecoverable programmer errors (e.g., logic bugs), not for I/O or user input issues.
- When adding new error variants, ensure they round-trip through `Display` and update any pattern matches that enumerate existing variants.

## Module Boundaries

- `packages/core/src/model.rs` owns the in-memory graph primitives (`Node`, `Edge`) and should remain serialization-agnostic.
- `packages/core/src/storage` encapsulates on-disk representation concerns (record layout, serializers, header). Keep it free of graph semantics.
- `packages/core/src/pager` handles file I/O, page caching, and durability plumbing. Higher layers should not access raw file handles directly.
- `packages/core/src/db.rs` orchestrates graph workflows (ID allocation, adjacency maintenance) by composing `model`, `storage`, and `pager`.
- New functionality should fit into this layering; if adding cross-cutting features (e.g., WAL), prefer creating a dedicated module that `db` orchestrates rather than collapsing boundaries.

## Workflow Expectations

- Run `cargo fmt` and `cargo test` before opening a change.
- Use conventional commit format with appropriate scope (`core`, `js`, `py`)
- Keep commits focused: one logical change per commit with a clear message.
- Document user-visible changes in the appropriate changelog via conventional commits.

