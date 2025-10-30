# MVCC API Refactor Plan: Enable Concurrent Transactions

## Goal
Change `Transaction` from holding `&mut GraphDB` to `&GraphDB` to enable true concurrent snapshot isolation with multiple overlapping transactions.

## Current Blockers

### 1. Transaction API
**File:** `packages/core/src/db/transaction.rs`
- **Current:** `pub struct Transaction<'db> { db: &'db mut GraphDB, ... }`
- **Target:** `pub struct Transaction<'db> { db: &'db GraphDB, ... }`
- **Blocker:** Rust borrow checker prevents multiple mutable borrows

### 2. GraphDB Methods Requiring `&mut self`
**File:** `packages/core/src/db/core/nodes.rs`
- `delete_node_internal(&mut self, ...)` - line 272
  
**File:** `packages/core/src/db/core/edges.rs`
- `delete_edge_internal(&mut self, ...)` - line 131

**Analysis:** These methods don't actually need `&mut self` - they use:
- `self.node_index.remove()` - DashMap (lock-free)
- `self.label_index.get_mut()` - DashMap (lock-free)
- `self.edge_index.remove()` - DashMap (lock-free)
- `self.node_cache.pop()` - ConcurrentLruCache
- `self.free_record()` - Uses pager with interior mutability

**Solution:** Change signatures to `&self`

### 3. begin_transaction Method
**File:** `packages/core/src/db/core/graphdb.rs`
- **Current:** `pub fn begin_transaction(&mut self) -> Result<Transaction<'_>>`
- **Target:** `pub fn begin_transaction(&self) -> Result<Transaction<'_>>`

## Implementation Steps

### Phase 1: Fix Delete Methods (LOW RISK)
These changes are isolated and don't affect the API surface:

1. **Change `delete_node_internal` signature**
   - File: `packages/core/src/db/core/nodes.rs:272`
   - Change: `pub fn delete_node_internal(&mut self, ...)` → `pub fn delete_node_internal(&self, ...)`
   - Verify all internal calls use concurrent data structures

2. **Change `delete_edge_internal` signature**
   - File: `packages/core/src/db/core/edges.rs:131`
   - Change: `pub fn delete_edge_internal(&mut self, ...)` → `pub fn delete_edge_internal(&self, ...)`
   - Verify all internal calls use concurrent data structures

3. **Update public `delete_node` and `delete_edge` methods**
   - File: Look for public wrappers
   - Change signatures from `&mut self` to `&self`

### Phase 2: Change Transaction to Hold &GraphDB (MEDIUM RISK)
This is the core change that enables concurrent transactions:

1. **Update Transaction struct**
   - File: `packages/core/src/db/transaction.rs:58`
   - Change: `db: &'db mut GraphDB` → `db: &'db GraphDB`

2. **Update Transaction::new**
   - File: `packages/core/src/db/transaction.rs:77`
   - Change parameter: `db: &'db mut GraphDB` → `db: &'db GraphDB`

3. **Update all Transaction methods**
   - Remove `&mut` from `self.db` accesses where they're no longer needed
   - Most methods already call `&self` methods on GraphDB

### Phase 3: Change begin_transaction API (HIGH IMPACT)
This changes the public API and affects all tests:

1. **Update GraphDB::begin_transaction**
   - File: `packages/core/src/db/core/graphdb.rs`
   - Change: `pub fn begin_transaction(&mut self) -> Result<Transaction<'_>>`
   - To: `pub fn begin_transaction(&self) -> Result<Transaction<'_>>`

2. **Update all test code** (237+ files potentially affected)
   - Change: `let mut db = GraphDB::open(...)?;`
   - To: `let db = GraphDB::open(...)?;` (remove `mut`)
   - Change: `let mut tx = db.begin_transaction()?;`
   - Keep `mut` on `tx` (still needed for transaction operations)

### Phase 4: Verify MVCC Snapshot Visibility (CRITICAL)
This ensures reads use snapshot timestamps correctly:

1. **Audit all read operations**
   - `get_node_with_snapshot` - Already implemented ✓
   - `load_edge_with_snapshot` - Check implementation
   - `get_nodes_by_label` - Needs snapshot filtering?
   - `find_nodes_by_property` - Needs snapshot filtering?
   - `get_neighbors_with_snapshot` - Already implemented ✓

2. **Test index visibility**
   - Label index should respect snapshots
   - Property index should respect snapshots
   - BTree index should respect snapshots

## Verification Strategy

### 1. Compile-time Verification
```bash
cargo build --all-features
cargo clippy --all-features
```

### 2. Existing Test Suite
```bash
cargo test --all-features
```

### 3. New MVCC Concurrent Tests
Create `packages/core/tests/mvcc_concurrent.rs` with:
- Multiple concurrent readers with different snapshots
- Reader/writer isolation (writer doesn't block readers)
- Snapshot visibility (old snapshot sees old data)
- Index snapshot visibility

## Risk Assessment

### Low Risk Changes
- ✅ Delete method signatures (`&mut self` → `&self`)
- ✅ These use concurrent data structures already

### Medium Risk Changes
- ⚠️ Transaction struct modification
- ⚠️ Need to verify all method calls compile

### High Risk Changes  
- 🔴 Public API change (`begin_transaction`)
- 🔴 Requires updating ALL test code
- 🔴 Breaking change for external users

## Rollback Plan

If issues are discovered:
1. All changes are in a feature branch
2. Can revert commits individually (Phase 1 → Phase 4)
3. Existing tests provide regression safety
4. No data format changes (safe to rollback)

## Success Criteria

1. ✅ All existing tests pass
2. ✅ New concurrent transaction tests pass
3. ✅ Multiple transactions can exist simultaneously
4. ✅ Snapshot isolation works correctly
5. ✅ No performance regression
6. ✅ Clippy and compiler warnings clean

## Timeline Estimate

- Phase 1 (Delete methods): 30 minutes
- Phase 2 (Transaction struct): 1 hour
- Phase 3 (Public API + tests): 2-3 hours
- Phase 4 (MVCC verification): 2 hours
- **Total:** ~6-7 hours

## Next Steps

1. Start with Phase 1 (lowest risk)
2. Compile and test after each phase
3. Write TDD tests for concurrent transactions FIRST
4. Then implement changes to make tests pass
