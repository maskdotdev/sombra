# Phase 2 Task 10: MVCC Write Operations Implementation Plan

## Goal
Implement version-aware write operations so that updating existing nodes/edges creates new versions in version chains instead of creating new records with new IDs.

## Current State

### What Works ✅
- **Read path**: `get_node_with_snapshot()` uses `VersionChainReader` to read versioned records
- **Timestamp allocation**: `TimestampOracle` allocates snapshot and commit timestamps
- **Version storage**: `store_new_version()` function exists and can write versioned records
- **Version metadata**: 25-byte version metadata struct is defined and serializable
- **Version chain reading**: `VersionChainReader` can traverse version chains

### What's Missing ❌
- **Write path**: `add_node_internal()` does NOT create version chains
- **Update detection**: No logic to distinguish between new node creation vs. updating existing node
- **Version linking**: `version_chain` field in Node model is not populated during writes
- **Index updates**: Indexes point to single versions, not heads of version chains

## Problem Analysis

### Current Behavior (BROKEN)
```rust
// TX1: Create node
let mut tx1 = db.begin_transaction()?;
let mut node = Node::new(1);
node.id = 0; // Not set yet
let id1 = tx1.add_node(node)?; // Returns id=1
tx1.commit()?;

// TX2: "Update" the node
let mut tx2 = db.begin_transaction()?;
let mut node = tx2.get_node(id1)?.unwrap(); // Get node id=1
node.properties.insert("new_prop", PropertyValue::Int(42));
let id2 = tx2.add_node(node)?; // Returns id=2 (NEW ID!)
tx2.commit()?;

// TX3: Read original node
let mut tx3 = db.begin_transaction()?;
let node = tx3.get_node(id1)?; // Still sees OLD version (no update!)
```

**Root cause**: `add_node()` ALWAYS allocates a new ID and creates a new record. It never updates existing records.

### Expected Behavior (CORRECT)
```rust
// TX1: Create node
let mut tx1 = db.begin_transaction()?;
let mut node = Node::new(1);
let id = tx1.add_node(node)?; // Returns id=1
tx1.commit()?;

// TX2: Update the node (creates new version)
let mut tx2 = db.begin_transaction()?;
let mut node = tx2.get_node(id)?.unwrap(); // Get node id=1
node.properties.insert("new_prop", PropertyValue::Int(42));
tx2.add_node(node)?; // Returns SAME id=1, creates version 2
tx2.commit()?;

// TX3: Read updated node
let mut tx3 = db.begin_transaction()?;
let node = tx3.get_node(id)?; // Sees NEW version with updated property
```

## Design Decisions

### Decision 1: How to detect updates vs. new nodes?

**Option A**: Check if `node.id != 0` (node has existing ID)
- ✅ Simple
- ✅ Already used in current codebase pattern
- ❌ Fragile - relies on caller setting ID correctly

**Option B**: Check if `node_index.contains(node.id)`
- ✅ Robust - verifies node actually exists
- ✅ Catches errors if caller passes invalid ID
- ❌ Extra index lookup

**Recommendation**: Use **Option B** - Check if ID exists in index

### Decision 2: Update API or keep existing?

**Option A**: Keep `add_node()` for both create and update
- ✅ No API changes needed
- ✅ Backwards compatible
- ❌ Confusing name for "update" operation

**Option B**: Add separate `update_node()` method
- ✅ Clear intent
- ❌ Breaking API change
- ❌ Need to update all callers

**Recommendation**: Use **Option A** for now - Keep existing API, add separate methods later if needed

### Decision 3: Transaction integration

**Current state**: Transaction wrapper exists but doesn't integrate with MVCC
- `Transaction::add_node()` calls `GraphDB::add_node_internal()`
- No commit timestamp passed to write operations
- No write-set tracking

**Plan**: 
1. Pass transaction context (tx_id, commit_ts) to write operations
2. Track written records in transaction
3. Use commit timestamp from prepare phase

## Implementation Steps

### Step 1: Modify `add_node_internal()` signature
**File**: `src/db/core/nodes.rs`

Add parameters for MVCC context:
```rust
pub fn add_node_internal(
    &mut self, 
    mut node: Node,
    tx_id: TxId,           // NEW: Transaction ID
    commit_ts: u64,        // NEW: Commit timestamp (0 if not committed yet)
) -> Result<NodeId>
```

### Step 2: Detect update vs. create
**File**: `src/db/core/nodes.rs`

```rust
pub fn add_node_internal(&mut self, mut node: Node, tx_id: TxId, commit_ts: u64) -> Result<NodeId> {
    let is_update = node.id != 0 && self.node_index.contains_key(&node.id);
    
    if is_update {
        // Update existing node - create new version
        self.update_node_version(node, tx_id, commit_ts)
    } else {
        // Create new node - existing logic
        self.create_new_node(node, tx_id, commit_ts)
    }
}
```

### Step 3: Implement `create_new_node()`
**File**: `src/db/core/nodes.rs`

Extract existing `add_node_internal()` logic into this method:
```rust
fn create_new_node(&mut self, mut node: Node, tx_id: TxId, commit_ts: u64) -> Result<NodeId> {
    let node_id = self.header.next_node_id;
    self.header.next_node_id += 1;
    
    node.id = node_id;
    node.first_outgoing_edge_id = NULL_EDGE_ID;
    node.first_incoming_edge_id = NULL_EDGE_ID;
    node.created_tx_id = tx_id;
    node.modified_tx_id = tx_id;
    node.version_chain = None; // First version has no previous
    
    let payload = serialize_node(&node)?;
    
    // Use store_new_version if MVCC enabled
    let pointer = if self.config.mvcc_enabled {
        use crate::storage::version_chain::store_new_version;
        store_new_version(
            &mut self.heap,
            None,  // No previous version
            node_id,
            RecordKind::Node,
            &payload,
            tx_id,
            commit_ts,
        )?
    } else {
        // Legacy non-versioned record
        let record = encode_record(RecordKind::Node, &payload)?;
        let preferred = self.header.last_record_page;
        self.insert_record(&record, preferred)?
    };
    
    self.node_index.insert(node_id, pointer);
    
    // Update label indexes
    for label in &node.labels {
        self.label_index
            .entry(label.clone())
            .or_default()
            .insert(node_id);
    }
    
    self.update_property_indexes_on_node_add(node_id)?;
    self.node_cache.put(node_id, node.clone());
    self.header.last_record_page = Some(pointer.page_id);
    
    Ok(node_id)
}
```

### Step 4: Implement `update_node_version()`
**File**: `src/db/core/nodes.rs`

NEW method to create a new version of existing node:
```rust
fn update_node_version(&mut self, mut node: Node, tx_id: TxId, commit_ts: u64) -> Result<NodeId> {
    let node_id = node.id;
    
    // Get pointer to current version (head of version chain)
    let prev_pointer = self.node_index.get(&node_id)
        .ok_or_else(|| GraphError::NodeNotFound(node_id))?
        .clone();
    
    // Update version metadata
    node.modified_tx_id = tx_id;
    node.version_chain = Some(prev_pointer); // Link to previous version
    
    let payload = serialize_node(&node)?;
    
    // Create new version in version chain
    use crate::storage::version_chain::store_new_version;
    let new_pointer = store_new_version(
        &mut self.heap,
        Some(prev_pointer),  // Link to previous version
        node_id,
        RecordKind::Node,
        &payload,
        tx_id,
        commit_ts,
    )?;
    
    // Update index to point to NEW head of version chain
    self.node_index.insert(node_id, new_pointer);
    
    // Update label indexes if labels changed
    // TODO: Need to read old node to compute diff
    for label in &node.labels {
        self.label_index
            .entry(label.clone())
            .or_default()
            .insert(node_id);
    }
    
    // Update property indexes
    // TODO: Need proper property index update logic for versions
    self.update_property_indexes_on_node_add(node_id)?;
    
    // Update cache with new version
    self.node_cache.put(node_id, node.clone());
    self.header.last_record_page = Some(new_pointer.page_id);
    
    Ok(node_id)
}
```

### Step 5: Update Transaction integration
**File**: `src/db/transaction.rs`

Modify `Transaction::add_node()` to pass MVCC context:
```rust
pub fn add_node(&mut self, node: Node) -> Result<NodeId> {
    let tx_id = self.tx_id;
    let commit_ts = 0; // Not committed yet - will be set at commit time
    
    self.db.add_node_internal(node, tx_id, commit_ts)
}
```

### Step 6: Handle commit timestamp
**File**: `src/db/transaction.rs`

At commit time, need to:
1. Allocate commit timestamp
2. Update all written records with commit timestamp
3. Or: Track written records and update them in commit()

**Challenge**: Records are already written with commit_ts=0. Need to either:
- **Option A**: Write records at commit time (2-phase: collect writes, then commit)
- **Option B**: Store records with ts=0, update them at commit
- **Option C**: Write records directly with allocated commit_ts

**Recommendation**: Use **Option C** - Allocate commit timestamp early

### Step 7: Update callers
**Files**: Multiple

Update all callers of `add_node_internal()` to pass tx_id and commit_ts:
- `GraphDB::add_node()` - Public API (allocates tx_id)
- `GraphDB::add_nodes_bulk()` - Bulk insert
- Any test code

### Step 8: Add tests
**File**: `tests/mvcc_writes.rs` (NEW)

Create tests to verify:
- Creating new node allocates new ID
- Updating existing node reuses same ID
- Version chain is created with prev_version pointer
- Index points to latest version
- Old snapshots can still read old versions
- Commit timestamp is set correctly

## Open Questions

### Q1: What about edges?
Similar changes needed for `add_edge_internal()` and edge operations.
**Answer**: Implement after nodes are working.

### Q2: How to handle property indexes?
Property indexes currently point to node IDs. With versioning, need to:
- Store which version the index entry refers to?
- Or: Always point to latest version and check visibility?
**Answer**: For Phase 2, keep simple - always point to latest. Fix in Phase 2 Task 13.

### Q3: What about deletes?
Should `delete_node()` create a tombstone version?
**Answer**: Yes - create version with `deleted` flag set. Implement separately.

### Q4: Label index updates on version changes?
If node labels change, need to update label index. But need to read old version to compute diff.
**Answer**: For now, keep it simple - add labels on update. Remove old labels separately. Optimize later.

### Q5: Concurrent writes to same node?
Phase 2 Task 12 (write-write conflicts) handles this.
**Answer**: Defer to Task 12.

## Testing Strategy

### Unit Tests
1. Test `create_new_node()` creates version with no prev_version
2. Test `update_node_version()` creates version with prev_version link
3. Test index always points to latest version
4. Test node ID doesn't change on update

### Integration Tests
1. Create node in TX1, update in TX2, verify same ID
2. Verify version chain is created (2 versions)
3. Verify old snapshot reads old version
4. Verify new snapshot reads new version
5. Test multiple updates create chain of 3+ versions

### Regression Tests
1. Verify non-MVCC mode still works (mvcc_enabled=false)
2. Verify backward compatibility with existing databases

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Breaking existing tests | High | Run full test suite, fix failures |
| Performance regression | Medium | Benchmark before/after |
| Index inconsistency | High | Add index validation tests |
| Version chain corruption | Critical | Add integrity checks |
| Backward compatibility | High | Test with old databases |

## Success Criteria

- [x] `add_node()` with existing ID creates new version (not new node) - **VERIFIED with mvcc_debug test**
- [x] Version chain is linked via `version_chain` field - **Implemented in version_chain.rs**
- [x] Index points to latest version - **Index update logic in place**
- [x] Versioned records can be written and read back - **mvcc_debug test passing**
- [x] RecordHeader::from_bytes() accepts versioned record kinds - **Lenient parsing implemented**
- [x] verify_integrity() handles versioned records - **Updated to use VersionedRecordKind**
- [ ] Old snapshots read old versions - **Infrastructure ready, needs integration**
- [ ] New snapshots read new versions - **Infrastructure ready, needs integration**
- [ ] All mvcc_isolation.rs tests pass
- [ ] All mvcc_version_chain.rs tests pass
- [ ] Existing non-MVCC tests still pass

## Recent Progress (October 27, 2025)

### Completed: Core Record Handling for Versioned Records ✅

**Problem Solved**: The "unknown record kind: 0x03" error was blocking all MVCC write operations.

**Root Cause**: 
- `RecordHeader::from_bytes()` used strict `RecordKind::from_byte()` which rejected versioned kinds
- `verify_integrity()` function also used strict parsing
- Low-level page operations couldn't handle versioned records

**Solution Implemented**:

1. **`src/storage/record.rs`**: Added lenient parsing
   - New method: `RecordKind::from_byte_lenient()` maps versioned kinds to legacy equivalents:
     - 0x03 (VersionedNode) → RecordKind::Node
     - 0x04 (VersionedEdge) → RecordKind::Edge
   - Updated `RecordHeader::from_bytes()` to use lenient parser
   - This allows all page-level operations to work transparently with both formats

2. **`src/db/core/graphdb.rs`**: Fixed integrity verification
   - Added import: `use crate::storage::version::{VersionedRecordKind, VERSION_METADATA_SIZE};`
   - Rewrote `verify_integrity()` function to:
     - Use `VersionedRecordKind::from_byte()` for kind detection
     - Calculate correct payload offsets for versioned records (skip 25-byte metadata)
     - Handle all 5 record types properly

3. **Test Results**:
   - `cargo test --test mvcc_debug` now **PASSES** ✅
   - Versioned node records can be written and read back successfully
   - Database integrity verification works with mixed legacy/versioned records

**Key Design Decision**: 
The lenient parsing approach allows gradual migration - legacy and MVCC records can coexist in the same database without requiring format migration.

## Next Steps After This Task

1. **Task 11**: Add commit validation logic
2. **Task 12**: Handle write-write conflicts
3. **Task 13**: Convert indexes to multi-version aware
4. Implement edge versioning (similar to nodes)
5. Implement delete operations (tombstone versions)
