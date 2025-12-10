# Edge Creation Bug Fix Implementation Plan

## Overview

This document outlines the implementation plan for fixing the edge creation deadlock/busy-loop issue that occurs during concurrent edge creation when checkpoint thresholds are reached. The fix involves two main strategies:

1. **Deferred Free-Page Cleanup**: Move free-page cleanup from index persistence to transaction commit to fix lock-order issues
2. **Smart Checkpoint Skipping**: Skip checkpoint when other transactions are active to avoid deadlocks

---

## PHASE 1: Fix Lock-Order & Free-Page Deferred Cleanup

### Task 1.1: Defer Free-Page Cleanup Until Transaction Commit

**File: `packages/core/src/db/core/index.rs`**

**Changes Needed:**
1. Line 127-130: Remove immediate `push_free_page()` calls
2. Store `old_pages` in transaction context for deferred cleanup
3. Add new field to TransactionContext or use existing dirty page tracking

**Implementation:**

```rust
// In persist_btree_index, after line 126:
// Instead of:
//   for page_id in old_pages {
//       self.push_free_page(tx_id, page_id)?;
//   }
// Do:
if tx_id != 0 {
    // For real transactions, defer until commit
    self.defer_free_pages(tx_id, old_pages);
} else {
    // For system transactions (tx_id=0), free immediately
    for page_id in old_pages {
        self.push_free_page(tx_id, page_id)?;
    }
}
```

**File: `packages/core/src/db/core/graphdb.rs`**
- Add `deferred_free_pages: Arc<DashMap<TxId, Vec<PageId>>>` field (similar to `recent_dirty_pages`)

**File: `packages/core/src/db/core/transaction_support.rs`**
- Add `defer_free_pages(tx_id, pages)` method
- Modify `commit_to_wal()` to call `push_free_page()` for deferred pages AFTER WAL commit

---

### Task 1.2: Fix Duplicate Header Update

**File: `packages/core/src/db/core/index.rs:137-139`**

**Changes:**
- Remove duplicate header update (lines 137-139)
- Keep only the first update (lines 121-125)
- This is already after pager work completes, correct ordering

---

### Task 1.3: Make push_free_page Non-Panicking

**File: `packages/core/src/db/core/records.rs:496-536`**

**Changes:**
- Change panic to return error or retry logic
- Add similar retry mechanism as `write_header()` in `transaction_support.rs:257-289`

---

## PHASE 2: Add Smart Checkpoint Skipping

### Task 2.1: Add Active Transaction Check

**File: `packages/core/src/db/core/transaction_support.rs:92-98`**

**Implementation:**

```rust
// Around line 92, replace:
if self.transactions_since_checkpoint.load(Ordering::Relaxed)
    >= self.config.checkpoint_threshold
{
    self.checkpoint()?;
    self.transactions_since_checkpoint.store(0, Ordering::Relaxed);
}

// With:
if self.transactions_since_checkpoint.load(Ordering::Relaxed)
    >= self.config.checkpoint_threshold
{
    // Skip checkpoint if other transactions are active to avoid deadlock
    // The transaction that makes active_count() drop to 0 will trigger checkpoint
    if self.mvcc_tx_manager.active_count() <= 1 {
        self.checkpoint()?;
        self.transactions_since_checkpoint.store(0, Ordering::Relaxed);
    } else {
        // Defer checkpoint - will be picked up by next transaction
        // Log warning if threshold is significantly exceeded
        let count = self.transactions_since_checkpoint.load(Ordering::Relaxed);
        if count % 1000 == 0 {
            warn!(count, "Checkpoint deferred due to active transactions");
        }
    }
}
```

---

### Task 2.2: Add Deferred Checkpoint Trigger

**File: `packages/core/src/db/core/transaction_support.rs`**

**Implementation:**
- In `exit_transaction()` (line 234-237), check if checkpoint was deferred
- If `active_count() == 0` and checkpoint threshold exceeded, trigger checkpoint

```rust
pub fn exit_transaction(&self, tx_id: TxId) {
    let _ = self.mvcc_tx_manager.end_transaction(tx_id);
    
    // Check if we should run deferred checkpoint now that this tx completed
    if self.mvcc_tx_manager.active_count() == 0 {
        if self.transactions_since_checkpoint.load(Ordering::Relaxed) 
            >= self.config.checkpoint_threshold 
        {
            // Safe to checkpoint now - no active transactions
            if let Err(e) = self.checkpoint() {
                warn!(error = ?e, "Deferred checkpoint failed");
            } else {
                self.transactions_since_checkpoint.store(0, Ordering::Relaxed);
            }
        }
    }
}
```

---

### Task 2.3: Apply Same Logic to WAL Size Check

**File: `packages/core/src/db/core/transaction_support.rs:113-121`**

**Changes:**
- Apply same active transaction check before forced checkpoint
- This prevents WAL size trigger from causing busy-loop

---

## PHASE 3: Testing & Cleanup

### Task 3.1: Verify concurrent_edge_creation Test

**File: `packages/core/tests/concurrent.rs:68`**

**Test Plan:**
1. Run test with `RUST_LOG=debug` to capture detailed logs
2. Verify no deadlocks or busy-loops during checkpoint
3. Check that all edges are created successfully
4. Verify neighbor count matches expected (line 114-115)

**Command:**
```bash
RUST_LOG=debug cargo test --package sombra concurrent_edge_creation -- --nocapture
```

---

### Task 3.2: Add Targeted Stress Test

**New Test File: `packages/core/tests/checkpoint_concurrency.rs`**

**Test Scenarios:**
1. Heavy autocommit workload hitting checkpoint threshold with active transactions
2. Verify checkpoint is deferred until transactions complete
3. Verify deferred free pages don't cause conflicts
4. Verify checkpoint eventually runs after transactions complete

---

### Task 3.3: Clean Up Debug Logging

**Files to Review:**
- `packages/core/src/db/core/index.rs`
- `packages/core/src/db/core/records.rs`
- `packages/core/src/db/core/transaction_support.rs`
- `packages/core/src/pager/mod.rs`

**Search for:**
- Excessive `tracing::debug!()` or `println!()` statements
- Temporary logging added during debugging
- Keep essential `info!()` and `warn!()` logs

---

## DETAILED FILE CHANGES SUMMARY

### Files to Modify:

1. **`packages/core/src/db/core/graphdb.rs`**
   - Add `deferred_free_pages: Arc<DashMap<TxId, Vec<PageId>>>`
   - Initialize in constructor

2. **`packages/core/src/db/core/index.rs`**
   - Line 127-130: Replace immediate free with deferred free
   - Line 137-139: Remove duplicate header update
   - Line 231: Same for `persist_property_indexes()`

3. **`packages/core/src/db/core/records.rs`**
   - Line 496-536: Make `push_free_page()` non-panicking with retry logic

4. **`packages/core/src/db/core/transaction_support.rs`**
   - Add `defer_free_pages()` method
   - Add `take_deferred_free_pages()` method
   - Line 92-98: Add active transaction check before checkpoint
   - Line 113-121: Add active transaction check before forced checkpoint
   - Modify `commit_to_wal()` to process deferred free pages
   - Modify `exit_transaction()` to trigger deferred checkpoint

5. **`packages/core/tests/concurrent.rs`**
   - Line 68: Verify `concurrent_edge_creation()` completes without issues

6. **`packages/core/tests/checkpoint_concurrency.rs` (NEW)**
   - Add comprehensive checkpoint concurrency tests

---

## IMPLEMENTATION ORDER

### Step 1: Infrastructure (No Behavior Change)
- Add `deferred_free_pages` field to GraphDB
- Add `defer_free_pages()` and `take_deferred_free_pages()` methods
- Run existing tests to ensure no regression

### Step 2: Free-Page Deferred Cleanup
- Modify `persist_btree_index()` to defer free pages
- Modify `persist_property_indexes()` to defer free pages
- Update `commit_to_wal()` to process deferred free pages
- Run tests

### Step 3: Checkpoint Skipping Logic
- Add active transaction check in `commit_to_wal()` checkpoint triggers
- Add deferred checkpoint in `exit_transaction()`
- Run tests

### Step 4: Testing & Verification
- Run `concurrent_edge_creation` test
- Add new checkpoint concurrency tests
- Verify all tests pass

### Step 5: Cleanup
- Remove excessive debug logging
- Update documentation if needed
- Final test sweep

---

## RISK MITIGATION

### Risk 1: Deferred Free Pages Memory Growth

**Mitigation:**
- Track total deferred pages across all transactions
- Add warning if exceeds threshold (e.g., 10,000 pages)
- Force cleanup if critical threshold reached

### Risk 2: Checkpoint Never Triggers

**Mitigation:**
- Add maximum defer count (e.g., 5x `checkpoint_threshold`)
- Force checkpoint even with active transactions if critical
- Log warnings when deferring

### Risk 3: Deferred Free Pages Not Released

**Mitigation:**
- Ensure `rollback_transaction()` also clears deferred free pages
- Add periodic audit in health check

---

## SUCCESS CRITERIA

1. ✅ `concurrent_edge_creation` test passes consistently
2. ✅ No deadlocks or busy-loops during checkpoint
3. ✅ Free pages are properly released after transaction commit
4. ✅ Checkpoint logic respects active transactions
5. ✅ All existing tests still pass
6. ✅ Debug logging is cleaned up

---

## TIMELINE ESTIMATE

- **Phase 1:** 4-6 hours (infrastructure + free-page deferred cleanup)
- **Phase 2:** 2-3 hours (checkpoint skipping logic)
- **Phase 3:** 3-4 hours (testing, verification, cleanup)
- **Total:** 9-13 hours

---

## Related Documentation

- [Concurrency Documentation](./CONCURRENCY.md)
- [Transaction Documentation](./transactions.md)
- [Architecture Overview](./architecture.md)
