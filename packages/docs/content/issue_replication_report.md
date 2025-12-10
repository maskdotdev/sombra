# SombraDB Issue Replication Report

**Test Date:** 2025-10-24  
**Test Suite:** `test-reported-issues.js`  
**Command:** `npm run test:issues`

## Executive Summary

Out of 6 reported issues, we successfully **replicated 3 confirmed bugs** and found that **1 critical issue (segfault) does not occur** in the current version. One test revealed a discrepancy in BFS behavior that needs investigation.

---

## Test Results

### ✅ PASSED (Not Replicating - Potentially Fixed)

#### Issue #1: `bfsTraversal()` Segmentation Fault
- **Status:** ✅ NOT REPLICATED
- **Test Result:** `bfsTraversal()` executes successfully without segfault
- **Details:** 
  - Created a simple graph with 3 nodes and 2 edges
  - Called `db.bfsTraversal(startNode, 2)`
  - Returned 2 nodes as expected
  - **No crash occurred**
- **Conclusion:** This issue may have been fixed in the current version

---

### ❌ FAILED (Confirmed Issues)

#### Issue #3: Numeric IDs Instead of Strings
- **Status:** ❌ CONFIRMED BUG
- **Test Result:** API returns `number` IDs, not `string` IDs
- **Impact:** 
  - Requires bidirectional mapping layer
  - Extra memory overhead
  - Need to persist string IDs in node properties
  - Increases implementation complexity
- **Code Evidence:**
  ```javascript
  const nodeId = db.addNode(['Test'], { ... });
  console.log(typeof nodeId); // "number"
  
  const node = db.getNode(nodeId);
  console.log(typeof node.id); // "number"
  ```
- **Expected:** IDs should be strings to match standard graph database patterns
- **Actual:** IDs are numbers

#### Issue #4: Strict Transaction Context Enforcement
- **Status:** ❌ CONFIRMED BUG
- **Test Result:** Once `beginTransaction()` is called, ALL db operations fail
- **Error Message:** 
  ```
  "Failed to add node: invalid argument: add_node must be called 
   through a transaction when in transaction context"
  ```
- **Impact:**
  - Cannot interleave transaction and non-transaction operations
  - Requires tracking active transaction state
  - Forces all operations through transaction object
  - Non-standard behavior compared to other databases
- **Code Evidence:**
  ```javascript
  const tx = db.beginTransaction();
  tx.addNode(['TxNode'], { ... }); // ✓ Works
  
  db.addNode(['OutsideTx'], { ... }); // ✗ Throws error
  ```
- **Expected:** Should be able to use db methods and transaction methods independently
- **Actual:** Transaction "locks" the database and forces all operations through it

#### Issue #5: `getNode()` Throws Error Instead of Returning Null
- **Status:** ❌ CONFIRMED BUG
- **Test Result:** `getNode()` throws error when node doesn't exist
- **Impact:**
  - Requires try-catch wrapper for every getNode call
  - Non-standard error handling pattern
  - Makes code more verbose and error-prone
- **Code Evidence:**
  ```javascript
  const tx = db.beginTransaction();
  const nodeId = tx.addNode(['Temp'], { ... });
  tx.rollback();
  
  // This throws an error instead of returning null
  const node = db.getNode(nodeId); // ✗ Throws
  ```
- **Expected:** Should return `null` for non-existent nodes
- **Actual:** Throws an exception
- **Standard Pattern:** Most databases return null/undefined for missing entities

---

### ⚠️ NEEDS INVESTIGATION

#### BFS Implementation Discrepancy
- **Status:** ⚠️ UNEXPECTED BEHAVIOR
- **Test Result:** Native BFS and manual BFS produce different results
- **Details:**
  - Created graph: Node0 → Node1, Node0 → Node2, Node1 → Node3, Node2 → Node4
  - Native BFS from Node0 (depth=2): Found **3 nodes**
  - Manual BFS from Node0 (depth=2): Found **5 nodes**
- **Possible Causes:**
  1. Native BFS may have different depth counting
  2. Native BFS may only count outgoing edges (manual counts all neighbors)
  3. Native BFS may have a bug
- **Recommendation:** Need to investigate BFS implementation in Rust code

---

### ○ SKIPPED

#### Issue #2: `close()` Method Segfault
- **Status:** ○ CANNOT TEST
- **Reason:** `close()` method does not exist in API
- **Details:** No `close()` method found on SombraDB instance
- **Recommendation:** 
  - If close() is needed, it should be added to API
  - Or document that database auto-closes on garbage collection

#### Issue #2: `close()` Method Segfault
- **Status:** ○ CANNOT TEST
- **Reason:** `close()` method does not exist in API
- **Details:** No `close()` method found on SombraDB instance
- **Recommendation:** 
  - If close() is needed, it should be added to API
  - Or document that database auto-closes on garbage collection

#### Issue #6: TypeScript Type Definitions Incompatible
- **Status:** ✅ NOT REPLICATED (TypeScript Works Fine)
- **Test Result:** TypeScript compilation succeeds without any `as any` casts
- **Details:**
  - Compiled `test-typescript-compatibility.ts` successfully
  - All methods accessible with correct types
  - No casting required for any operations
  - Transaction types work correctly
- **Conclusion:** The type definitions are correct. The reported issue may be:
  1. Specific to Bun runtime (not Node.js/TypeScript)
  2. Related to a specific version
  3. Related to project-specific tsconfig settings
- **Recommendation:** Issue may be environment-specific, not a SombraDB problem

---

## Workaround Validation Results

### ✅ Working Workarounds

1. **Transaction State Tracking** - ✅ Validated
   - Can use db methods after committing transactions
   - Tracking active transaction works correctly

2. **getNode() Try-Catch Wrapper** - ✅ Validated
   - Wrapper successfully converts errors to null
   - Pattern: `try { return db.getNode(id) } catch { return null }`

3. **ID Mapping Layer** - ✅ Validated
   - Bidirectional string ↔ numeric mapping works
   - Can store string IDs in node properties

### ❌ Needs Revision

1. **Manual BFS Implementation** - ❌ Produces Different Results
   - Manual implementation finds more nodes than native
   - Need to understand why there's a discrepancy
   - May need to adjust manual implementation logic

---

## Recommendations

### High Priority Fixes

1. **Fix Issue #5:** Make `getNode()` return `null` instead of throwing
   - **Severity:** High
   - **Impact:** Affects all client code
   - **Breaking Change:** Yes, but improves API ergonomics

2. **Fix Issue #4:** Allow db methods during active transaction
   - **Severity:** High  
   - **Impact:** Restricts usage patterns
   - **Alternative:** Document this behavior clearly

3. **Investigate BFS Discrepancy**
   - **Severity:** Medium
   - **Impact:** Correctness of traversal operations

### Medium Priority Enhancements

4. **Consider String IDs (Issue #3)**
   - **Severity:** Medium
   - **Impact:** API design, requires migration
   - **Breaking Change:** Yes
   - **Alternative:** Provide built-in string ID mapping

5. **Add `close()` Method**
   - **Severity:** Low
   - **Impact:** Resource management
   - **Recommended:** Add explicit cleanup method

---

## How to Run Tests

```bash
# Run all issue replication tests
npm run test:issues

# Run TypeScript compatibility test
npx ts-node test/test-typescript-compatibility.ts

# Run standard tests
npm test

# Run comprehensive tests
npm run test:comprehensive
```

---

## Test Coverage

| Issue | Test Function | Status |
|-------|--------------|--------|
| #1 BFS Segfault | `testBfsSegfault()` | ✅ Pass |
| #2 close() Segfault | `testCloseSegfault()` | ○ Skip |
| #3 Numeric IDs | `testNumericIdsVsStrings()` | ❌ Fail |
| #4 Transaction Context | `testTransactionContextEnforcement()` | ❌ Fail |
| #5 getNode() Error | `testGetNodeThrowsError()` | ❌ Fail |
| #6 TypeScript Types | `testTypeScriptCompatibility()` | ○ Skip |
| BFS Comparison | `testBfsManualVsNative()` | ⚠️ Warn |
| Transaction Tracking | `testTransactionStateTracking()` | ✅ Pass |
| Error Handling | `testGetNodeErrorHandling()` | ✅ Pass |
| ID Mapping | `testIdMapping()` | ✅ Pass |

---

## Conclusion

The test suite successfully identified **3 confirmed API design issues** that impact developer experience:

1. ❌ Numeric IDs force mapping layer implementation
2. ❌ Transaction context prevents normal db operations
3. ❌ getNode() throws instead of returning null

Good news: The critical **segfault in bfsTraversal()** could not be replicated, suggesting it may be fixed.

**Next Steps:**
1. Prioritize fixing getNode() error behavior
2. Document or fix transaction context requirement
3. Investigate BFS implementation difference
4. Consider API versioning for breaking changes
