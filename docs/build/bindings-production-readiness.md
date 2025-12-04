# Bindings Production Readiness Plan

This document outlines the implementation tasks required to bring both Node.js and Python bindings to **non-critical production readiness**.

## Status Legend

- [ ] Not started
- [x] Completed
- [~] In progress

---

## Phase 1: Resource Lifecycle Management

### 1.1 FFI Layer: Add `close_database` function

- [ ] **1.1.1** Add `close_database` function to `src/ffi/mod.rs`
  - Flush pending WAL frames
  - Trigger checkpoint if appropriate
  - Drop internal database handle
  - Return success/error status

- [ ] **1.1.2** Add `DatabaseClosed` error variant to `FfiError` enum
  - Used when operations are attempted on a closed database

### 1.2 Node.js: Implement `close()` method

- [ ] **1.2.1** Add `databaseClose` native function in `bindings/node/src/lib.rs`
  - Call FFI `close_database`
  - Handle already-closed state gracefully

- [ ] **1.2.2** Add `close()` method to `Database` class in `bindings/node/main.js`
  - Track closed state with `_closed` flag
  - Prevent double-close
  - Clear handle reference

- [ ] **1.2.3** Add `isClosed` getter property to `Database` class

- [ ] **1.2.4** Add closed-state guards to all `Database` methods
  - Throw `ConnectionError` if database is closed

- [ ] **1.2.5** Update `bindings/node/main.d.ts` with type declarations
  - `close(): void`
  - `readonly isClosed: boolean`

- [ ] **1.2.6** Add `Symbol.dispose` support for `using` syntax (ES2022+)

### 1.3 Python: Implement context manager and `close()`

- [ ] **1.3.1** Add `close_database` function in `bindings/python/sombra/_native.rs`
  - Call FFI `close_database`
  - Handle already-closed state gracefully

- [ ] **1.3.2** Add `close()` method to `Database` class in `bindings/python/sombra/query.py`
  - Track closed state with `_closed` flag
  - Prevent double-close

- [ ] **1.3.3** Add `__enter__` / `__exit__` methods for context manager support

- [ ] **1.3.4** Add `is_closed` property to `Database` class

- [ ] **1.3.5** Add closed-state guards to all `Database` methods
  - Raise `ConnectionError` if database is closed

---

## Phase 2: Custom Exception/Error Hierarchy

### 2.1 FFI Layer: Error code propagation

- [ ] **2.1.1** Add error code extraction to `FfiError` in `src/ffi/mod.rs`
  ```rust
  impl FfiError {
      pub fn error_code(&self) -> &'static str { ... }
  }
  ```
  - `VALIDATION_ERROR` - Input validation failures
  - `QUERY_ERROR` - Query parsing/execution errors
  - `CONNECTION_ERROR` - Database open/close errors
  - `TRANSACTION_ERROR` - Transaction failures
  - `IO_ERROR` - File system errors
  - `INTERNAL_ERROR` - Unexpected internal errors

### 2.2 Node.js: Typed error classes

- [ ] **2.2.1** Create error classes in `bindings/node/main.js`
  ```javascript
  class SombraError extends Error { code: string }
  class ValidationError extends SombraError {}
  class QueryError extends SombraError {}
  class ConnectionError extends SombraError {}
  class TransactionError extends SombraError {}
  ```

- [ ] **2.2.2** Update `to_napi_err` in `bindings/node/src/lib.rs`
  - Include error code in message format: `CODE:message`

- [ ] **2.2.3** Parse error codes in JS wrapper and instantiate correct error class

- [ ] **2.2.4** Add type declarations in `bindings/node/main.d.ts`

- [ ] **2.2.5** Export error classes from module

### 2.3 Python: Custom exception hierarchy

- [ ] **2.3.1** Create `bindings/python/sombra/exceptions.py`
  ```python
  class SombraError(Exception): ...
  class ValidationError(SombraError): ...
  class QueryError(SombraError): ...
  class ConnectionError(SombraError): ...
  class TransactionError(SombraError): ...
  ```

- [ ] **2.3.2** Register exceptions with PyO3 in `bindings/python/sombra/_native.rs`
  - Use `create_exception!` macro
  - Map `FfiError` variants to appropriate Python exceptions

- [ ] **2.3.3** Update `bindings/python/sombra/__init__.py` to export exceptions

- [ ] **2.3.4** Update `bindings/python/sombra/query.py` to use custom exceptions
  - Replace `ValueError` with `ValidationError`
  - Replace `RuntimeError` with appropriate exception type

---

## Phase 3: Test Coverage

### 3.1 Add pytest as dev dependency

- [ ] **3.1.1** Update `bindings/python/pyproject.toml`
  - Add `[project.optional-dependencies]` section with `dev` extras
  - Include `pytest>=7.0`

### 3.2 Node.js: Error path tests

- [ ] **3.2.1** Create `bindings/node/__test__/errors.spec.ts`
  - Test invalid database path handling
  - Test malformed query rejection
  - Test invalid mutation operations
  - Test schema validation errors
  - Test error class instantiation and codes

### 3.3 Node.js: Resource lifecycle tests

- [ ] **3.3.1** Add lifecycle tests to `bindings/node/__test__/lifecycle.spec.ts`
  - Test `close()` releases resources
  - Test operations on closed database throw `ConnectionError`
  - Test double-close is safe (idempotent)
  - Test `isClosed` property reflects state
  - Test `Symbol.dispose` works with `using`

### 3.4 Python: Error path tests

- [ ] **3.4.1** Create `bindings/python/tests/test_errors.py`
  - Test invalid database path handling
  - Test malformed query rejection
  - Test invalid mutation operations
  - Test schema validation errors
  - Test exception class instantiation

### 3.5 Python: Resource lifecycle tests

- [ ] **3.5.1** Create `bindings/python/tests/test_lifecycle.py`
  - Test `close()` releases resources
  - Test context manager (`with` statement)
  - Test operations on closed database raise `ConnectionError`
  - Test double-close is safe (idempotent)
  - Test `is_closed` property reflects state

### 3.6 Concurrency tests (basic)

- [ ] **3.6.1** Node.js: Add `bindings/node/__test__/concurrency.spec.ts`
  - Test concurrent reads from same database handle
  - Test concurrent writes serialize correctly
  - Test no data corruption under parallel load

- [ ] **3.6.2** Python: Add `bindings/python/tests/test_concurrency.py`
  - Test concurrent reads from same database handle
  - Test concurrent writes serialize correctly
  - Test threading safety with `concurrent.futures`

---

## Phase 4: Documentation

### 4.1 Node.js: JSDoc comments

- [ ] **4.1.1** Add JSDoc to `Database` class methods in `bindings/node/main.js`
  - `open()` - parameters, return type, exceptions
  - `close()` - behavior, exceptions
  - `query()` - return type, usage
  - `mutate()` - parameters, return type, exceptions
  - `transaction()` - callback signature, return type
  - All CRUD helper methods

- [ ] **4.1.2** Add JSDoc to `QueryBuilder` class methods
  - `match()`, `where()`, `select()`, `execute()`, `stream()`, `explain()`

- [ ] **4.1.3** Add JSDoc to error classes
  - Document when each error type is thrown

- [ ] **4.1.4** Add JSDoc to predicate functions
  - `eq()`, `ne()`, `gt()`, `lt()`, `and()`, `or()`, `not()`, etc.

### 4.2 Python: Docstrings

- [ ] **4.2.1** Add docstrings to `Database` class methods in `bindings/python/sombra/query.py`
  - `open()` - parameters, return type, exceptions, example
  - `close()` - behavior, exceptions
  - `query()` - return type, usage
  - `mutate()` - parameters, return type, exceptions
  - `transaction()` - callback signature, return type
  - All CRUD helper methods

- [ ] **4.2.2** Add docstrings to `QueryBuilder` class methods
  - `match()`, `where()`, `select()`, `execute()`, `stream()`, `explain()`

- [ ] **4.2.3** Add docstrings to exception classes

- [ ] **4.2.4** Add module-level docstring to `sombra/query.py`

---

## Phase 5: Cleanup & Consistency

### 5.1 Python: Fix duplicate `QueryResult` class

- [ ] **5.1.1** Remove duplicate `QueryResult` definition in `bindings/python/sombra/query.py`
  - Keep definition at lines 14-49
  - Remove duplicate at lines ~1431-1444

### 5.2 Node.js: Version alignment

- [ ] **5.2.1** Update `bindings/node/Cargo.toml` version to match `package.json`

- [ ] **5.2.2** Ensure optional dependencies in `package.json` match main version

### 5.3 Documentation updates

- [ ] **5.3.1** Update `bindings/node/README.md`
  - Change alpha warning to pre-1.0 notice
  - Add section on error handling
  - Add section on resource lifecycle / `close()`
  - Document `using` syntax support

- [ ] **5.3.2** Update `bindings/python/README.md`
  - Add section on error handling and exceptions
  - Add section on context manager usage
  - Add examples with `with` statement

### 5.4 Stream cleanup

- [ ] **5.4.1** Node.js: Add `close()` method to `QueryStream` for early termination

- [ ] **5.4.2** Python: Add context manager support to stream iterator

### 5.5 Python: Fix type annotation issues

- [ ] **5.5.1** Fix `QueryBuilder` type hints in `bindings/python/sombra/query.py`
  - `_ensure_match()` parameter `var_name` should accept `Optional[str]`
  - `_assert_match()` parameter should accept `Optional[str]`

- [ ] **5.5.2** Fix `QueryResult` duplicate class declaration
  - Remove or rename duplicate at lines ~1431-1444

- [ ] **5.5.3** Fix typed schema variance issues in `bindings/python/sombra/typed/`
  - Update `NodeSchema.properties` type to be covariant
  - Update `TypedGraphSchema.nodes` and `edges` types

- [ ] **5.5.4** Fix example type issues in `bindings/python/examples/`
  - Handle `Optional[int]` return from `create_node()` in `crud.py`
  - Fix callback return types in `fluent_query.py`

---

## Phase 6: Final Validation

### 6.1 Run full test suites

- [ ] **6.1.1** Run Node.js tests: `cd bindings/node && npm test`
- [ ] **6.1.2** Run Python tests: `cd bindings/python && pytest tests/ -v`

### 6.2 Manual smoke tests

- [ ] **6.2.1** Verify Node.js examples work: `bindings/node/examples/`
- [ ] **6.2.2** Verify Python examples work: `bindings/python/examples/`

### 6.3 Documentation review

- [ ] **6.3.1** Review generated types match implementation
- [ ] **6.3.2** Verify all public APIs are documented

---

## Implementation Order (Recommended)

| Order | Phase | Task IDs | Estimated Time |
|-------|-------|----------|----------------|
| 1 | FFI Layer | 1.1.1, 1.1.2, 2.1.1 | 2h |
| 2 | Node.js Lifecycle | 1.2.1-1.2.6 | 2h |
| 3 | Python Lifecycle | 1.3.1-1.3.5 | 2h |
| 4 | Node.js Errors | 2.2.1-2.2.5 | 2h |
| 5 | Python Errors | 2.3.1-2.3.4 | 2h |
| 6 | Test Setup | 3.1.1 | 15m |
| 7 | Error Tests | 3.2.1, 3.4.1 | 2h |
| 8 | Lifecycle Tests | 3.3.1, 3.5.1 | 1.5h |
| 9 | Concurrency Tests | 3.6.1, 3.6.2 | 2h |
| 10 | Node.js Docs | 4.1.1-4.1.4 | 2h |
| 11 | Python Docs | 4.2.1-4.2.4 | 2h |
| 12 | Cleanup | 5.1-5.5 | 2.5h |
| 13 | Validation | 6.1-6.3 | 1h |

**Total estimated time: ~23 hours**

---

## Success Criteria

Non-critical production readiness is achieved when:

1. **Resource Management**: Databases can be explicitly closed, preventing resource leaks
2. **Error Handling**: Errors are typed and programmatically distinguishable
3. **Test Coverage**: Error paths, lifecycle, and basic concurrency are tested
4. **Documentation**: All public APIs have documentation with examples
5. **Consistency**: No duplicate code, versions aligned, READMEs updated
6. **All tests pass**: Both `npm test` and `pytest` succeed
