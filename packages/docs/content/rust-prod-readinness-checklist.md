## Panic Prevention

**Unwrap/Expect Usage**
- Audit all `.unwrap()` and `.expect()` calls - replace with proper error handling using `Result` or `Option` patterns
- Search for: `panic!`, `unwrap()`, `expect()`, `unreachable!`, `todo!()`, `unimplemented!()`
- Use `unwrap_or()`, `unwrap_or_else()`, `ok_or()`, or `?` operator instead

**Integer Overflow**
- In release mode, integer overflow wraps by default - use checked arithmetic for critical operations
- Replace `+`, `-`, `*` with `.checked_add()`, `.saturating_mul()`, etc. where overflow matters
- Consider enabling overflow checks in release: `overflow-checks = true` in Cargo.toml

**Array/Slice Indexing**
- Direct indexing `arr[i]` panics on out-of-bounds - use `.get(i)` which returns `Option`
- For graph traversal, this is critical when accessing neighbor lists

## Memory & Performance Issues

**Unnecessary Cloning**
- Search for excessive `.clone()` calls - graph data structures can be clone-heavy
- Use references (`&`), `Rc`/`Arc`, or `Cow` where appropriate
- Profile with `cargo clippy -- -W clippy::clone_on_copy`

**String Allocations**
- Prefer `&str` over `String` in function parameters
- Use `format!` judiciously - it allocates
- For graph node/edge labels, consider interning strings

**Large Stack Allocations**
- Graph algorithms with deep recursion can overflow stack
- Use iterative approaches or `Box` for large structs
- Default stack size is ~2MB on Linux, ~1MB on Windows

**Drop Order & Circular References**
- Graph structures with `Rc`/`Arc` can create reference cycles
- Use `Weak` pointers to break cycles
- Implement `Drop` carefully if manual cleanup is needed

## Concurrency Issues

**Send/Sync Bounds**
- Ensure thread-safe types when using parallel graph algorithms
- `Rc` is not `Send` - use `Arc` for multi-threaded access
- `RefCell` is not `Sync` - use `Mutex`/`RwLock`

**Data Races with Interior Mutability**
- `RefCell` panics if borrow rules violated at runtime
- Use `try_borrow()` for fallible borrowing
- For concurrent access, use `Mutex`/`RwLock` instead

**Deadlocks**
- Lock acquisition order matters - document and enforce ordering
- Avoid holding locks across await points in async code
- Consider using `parking_lot` for better lock performance/diagnostics

## Type System Pitfalls

**Lifetime Annotations**
- Graph structures often need explicit lifetimes for node/edge references
- Watch for "lifetime may not live long enough" errors
- Consider arena allocation patterns for graph nodes

**Generic Bounds**
- Missing trait bounds can cause confusing errors
- Use `where` clauses for complex bounds readability
- Graph generic over node/edge types needs careful bound design

**Enum Exhaustiveness**
- Use `#[non_exhaustive]` for public enums that might grow
- Don't use catch-all `_` patterns where you want compile-time exhaustiveness checks

## Error Handling

**Error Propagation**
- Use `thiserror` or `anyhow` for consistent error handling
- Graph operations should return `Result` not panic
- Distinguish between recoverable and fatal errors

**Lossy Conversions**
- `.try_into()` over `.into()` where conversion can fail
- Watch for integer truncation with `as` casts

## Unsafe Code

**Unsafe Blocks**
- Minimize unsafe - audit every usage
- Document invariants that must be upheld
- Consider using battle-tested unsafe abstractions instead

**Raw Pointers**
- Validate alignment and null checks
- Ensure lifetime validity manually

## Testing & Tooling

**Run These Regularly**
```bash
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt -- --check
cargo test --all-features
cargo miri test  # for undefined behavior detection
cargo deny check # for dependency security/licenses
RUSTFLAGS="-Zsanitizer=address" cargo +nightly test # address sanitizer
```

**Graph-Specific Tests**
- Fuzz test graph algorithms with random graphs
- Test with empty graphs, single nodes, cycles
- Stress test with large graphs for memory issues
- Property-based testing with `proptest` or `quickcheck`

## Dependencies

**Audit Dependencies**
- Use `cargo audit` for security vulnerabilities
- Pin critical dependencies with exact versions
- Check for unmaintained crates (especially for graph algorithms)
- Review transitive dependencies

**Feature Flags**
- Disable default features you don't need to reduce attack surface
- Use workspace dependencies for version consistency

## Documentation

**Public API Documentation**
- All public functions should have doc comments
- Include complexity analysis (O notation) for graph algorithms
- Document panic conditions explicitly with `# Panics` section

Would you like me to create a checklist artifact or dive deeper into any specific area like implementing a custom linter for panic detection?
