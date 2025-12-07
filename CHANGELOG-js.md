# Changelog (Node bindings)

All notable changes to the Node bindings will be documented in this file by Release Please.

## Unreleased

- Wrap all native calls in `Database`/`QueryStream` with `wrapNativeError` so public methods always raise typed `SombraError` subclasses.
- Add `QueryStream.close()`, `return()`, and `Symbol.dispose`/`Symbol.asyncDispose` plus `Database[Symbol.dispose]` to release native handles during `using`/`for await`.
- Align optional native binary dependency versions to `0.6.2` and tighten `engines` to Node 18+.
- Extend AVA coverage for stream disposal, native error wrapping, and parallel read/write usage on a single handle.
