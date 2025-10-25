# SombraDB Issue Test Suite

This directory contains tests to replicate and validate reported issues with SombraDB.

## Quick Start

```bash
# Install dependencies and build
npm install
npm run build

# Run issue replication tests
npm run test:issues

# Run TypeScript compatibility test
node test/test-typescript-compatibility.js
```

## Test Files

- **`test-reported-issues.js`** - Main test suite for replicating reported issues
- **`test-typescript-compatibility.ts`** - TypeScript type compatibility validation
- **`../docs/issue_replication_report.md`** - Detailed test results and analysis

## Test Results Summary

### ✅ CONFIRMED WORKING (Issues NOT replicated)
- ✓ Issue #1: bfsTraversal() does **NOT** cause segfault
- ✓ Issue #6: TypeScript types work **correctly** (no `as any` needed)

### ❌ CONFIRMED BUGS (Issues replicated)
- ✗ Issue #3: API uses numeric IDs instead of strings
- ✗ Issue #4: Strict transaction context enforcement  
- ✗ Issue #5: getNode() throws error instead of returning null

### ⚠️ NEEDS INVESTIGATION
- ⚠️ BFS native vs manual implementation produces different results

### ○ CANNOT TEST
- ○ Issue #2: close() method not available in API

## Issue Details

### Issue #3: Numeric IDs
**Problem:** API returns `number` IDs instead of `string` IDs  
**Impact:** Requires mapping layer for applications expecting string IDs  
**Workaround:** Store string IDs in node properties and maintain bidirectional map

### Issue #4: Transaction Context Enforcement
**Problem:** After calling `beginTransaction()`, all db operations must use transaction object  
**Error:** `"add_node must be called through a transaction when in transaction context"`  
**Impact:** Cannot interleave transaction and non-transaction operations  
**Workaround:** Track active transaction state and route operations accordingly

### Issue #5: getNode() Error Handling
**Problem:** `getNode()` throws exception for non-existent nodes instead of returning null  
**Impact:** Requires try-catch wrapper for every getNode call  
**Workaround:** Wrap in try-catch and return null on error

## Running Individual Tests

```javascript
const { SombraDB } = require('../index');

// Test Issue #3: Numeric IDs
const db = new SombraDB('test.db');
const id = db.addNode(['Test'], {});
console.log(typeof id); // "number" - confirms issue

// Test Issue #4: Transaction Context
const tx = db.beginTransaction();
db.addNode(['Test'], {}); // Throws error - confirms issue

// Test Issue #5: getNode() Error
const tx2 = db.beginTransaction();
const nodeId = tx2.addNode(['Temp'], {});
tx2.rollback();
db.getNode(nodeId); // Throws error instead of returning null - confirms issue
```

## Validated Workarounds

The following workarounds have been tested and confirmed working:

1. ✅ **ID Mapping Layer** - Bidirectional string ↔ numeric mapping works
2. ✅ **Transaction State Tracking** - Can track and manage active transactions
3. ✅ **getNode() Try-Catch Wrapper** - Successfully converts errors to null
4. ⚠️ **Manual BFS Implementation** - Works but produces different results than native

## TypeScript Compatibility

The TypeScript type definitions work correctly:

```typescript
import { SombraDB, SombraTransaction } from 'sombradb';

const db = new SombraDB('test.db');
const nodeId = db.addNode(['Test'], { name: { type: 'string', value: 'test' } });
const node = db.getNode(nodeId); // No casting needed
const tx: SombraTransaction = db.beginTransaction(); // Types are correct
```

**Conclusion:** Issue #6 (TypeScript incompatibility) is **NOT** a SombraDB problem. 
It may be specific to Bun runtime or project-specific configuration.

## Next Steps

1. Fix Issue #5 (high priority) - Make getNode() return null
2. Document or fix Issue #4 - Transaction context behavior  
3. Consider Issue #3 - Evaluate string ID support
4. Investigate BFS discrepancy

See `docs/issue_replication_report.md` for full analysis and recommendations.
