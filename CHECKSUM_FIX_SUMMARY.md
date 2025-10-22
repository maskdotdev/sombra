# Property Index Persistence - Checksum Mismatch Fix

## Problem Summary
Property index stress tests were failing with checksum mismatch errors when reopening databases after checkpoint. The error occurred on page 26 with stored checksum `0x00000000` but computed checksum `0xAAC184F9`, indicating the page existed in the file but was never properly written.

## Root Cause
**Bug in `persist_btree_index()` at `src/db/core/index.rs:63`**

The function calculated it needed multiple pages (e.g., 4 pages for a 32KB BTree index), but only allocated ONE page with `allocate_page()`. It then assumed the next pages would exist contiguously and tried to write to them using `start_page + i`.

### Example of the Bug:
```rust
// Before fix - INCORRECT
let start_page = self.pager.allocate_page()?;  // Only allocates page 26
for i in 0..pages_needed {                      // pages_needed = 4
    let page_id = start_page + i as u32;       // Assumes 26, 27, 28, 29 exist
    // Write to page_id...
}
```

This meant:
- Page 26: Allocated but checksummed with default zeros
- Pages 27-29: Never allocated, assumed to exist
- File size extended on allocation, creating "ghost" pages with invalid checksums
- On reopen: Checksum verification failed because pages existed in file but were never initialized

## The Fix
**Modified `persist_btree_index()` to allocate ALL needed pages upfront**

### Location: `src/db/core/index.rs:52-68`

```rust
// After fix - CORRECT
let start = self.pager.allocate_page()?;
for i in 1..pages_needed {
    let expected_page = start + i as u32;
    let allocated = self.pager.allocate_page()?;
    if allocated != expected_page {
        return Err(GraphError::Corruption(
            format!("Expected contiguous page allocation: got {}, expected {}", 
                    allocated, expected_page)
        ));
    }
}
```

This ensures:
1. All pages are explicitly allocated
2. Pages are verified to be contiguous
3. Each allocated page gets a valid checksum applied
4. File metadata (`file_len`, dirty page tracking) is properly maintained

## Changes Made

### `src/db/core/index.rs`
- Lines 52-68: Fixed allocation in the "need more pages than old" branch
- Lines 70-85: Fixed allocation in the "no old pages" branch  
- Both branches now allocate all needed pages and verify contiguity

### `src/pager/mod.rs`
- Line 224: Already applies checksum on page allocation (added in previous session)
- This ensures allocated pages have valid checksums when persisted

### `src/db/core/property_index_persistence.rs`
- Line 567: Fixed test assertion bug (expected count=1, should be count=3)
- This was a pre-existing bug in a new test file

## Test Results
All tests pass:
- ✅ `property_index_stress::test_property_index_stress_string_values` - Previously failing, now passes
- ✅ All 9 property index stress tests pass
- ✅ Full test suite passes (72 tests)

## Key Insights
1. **Page allocation assumptions are dangerous**: Never assume pages exist contiguously without explicitly allocating them
2. **File size ≠ initialized pages**: The pager's `file_len` extends on allocation, but pages must be written to be valid
3. **Dirty page tracking is separate**: Only pages with `record_page_write()` called are tracked for flushing
4. **Checksum on allocation helps but doesn't solve**: While we checksum on allocation, we still need to write the page to disk

## Prevention
Future page allocation code should:
1. Always explicitly allocate all pages needed
2. Verify contiguity if assuming sequential page IDs
3. Mark pages dirty AND call `record_page_write()` for proper tracking
4. Write pages to disk before checkpoint/close

## Related Issues
This fix resolves the checksum mismatch issue that only occurred with:
- 1000-2000 nodes (large enough to need multiple BTree index pages)
- Database reopen after checkpoint
- BTree index spanning multiple pages
