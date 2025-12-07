# Changelog (Python bindings)

All notable changes to the Python bindings will be documented in this file by Release Please.

## Unreleased

- Remove the duplicate `QueryResult` definition and rely on a single helper with safe accessors.
- Wrap all native entry points (execute/explain/stream/mutate/create/pragma/neighbors/BFS) with `wrap_native_error` so callers consistently receive typed exceptions.
- Add `QueryStream.close()` plus async context-manager support to release native stream handles early.
- Expand pytest coverage for stream disposal, native error wrapping, and basic parallel read/write usage on a shared database handle.
