# Concurrent API Design for Sombra Graph Database

## Problem Statement

The current MVCC implementation has all the infrastructure for concurrent transactions but the API prevents it:

```rust
pub fn begin_transaction(&mut self) -> Result<Transaction<'_>>
```

The `&mut self` requirement means only one transaction can be active at a time in the same process, defeating the purpose of MVCC.

## Current Architecture

### What We Have
- ✅ `MvccTransactionManager` - tracks multiple concurrent transactions
- ✅ `TimestampOracle` - allocates snapshot/commit timestamps
- ✅ Version chains - MVCC storage layer with snapshot isolation
- ✅ File locking - prevents multi-process corruption

### What's Blocking Concurrency
- ❌ API requires `&mut GraphDB` for all operations
- ❌ `Transaction<'db>` holds exclusive borrow of `GraphDB`
- ❌ No interior mutability patterns (Mutex/RwLock)

## Design Options

### Option 1: Arc<Mutex<GraphDB>> - Simple but Coarse-Grained

```rust
pub struct ConcurrentGraphDB {
    inner: Arc<Mutex<GraphDB>>,
}

impl ConcurrentGraphDB {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(GraphDB::open(path)?)),
        })
    }
    
    pub fn begin_transaction(&self) -> Result<ConcurrentTransaction> {
        let mut db = self.inner.lock().unwrap();
        let tx_id = db.allocate_tx_id()?;
        // Create transaction context in MVCC manager
        let context = db.mvcc_tx_manager.as_mut()
            .ok_or(GraphError::InvalidArgument("MVCC not enabled"))?
            .begin_transaction(tx_id)?;
        
        Ok(ConcurrentTransaction {
            db: Arc::clone(&self.inner),
            tx_id,
            snapshot_ts: context.snapshot_ts,
            state: TxState::Active,
        })
    }
}

pub struct ConcurrentTransaction {
    db: Arc<Mutex<GraphDB>>,
    tx_id: TxId,
    snapshot_ts: u64,
    state: TxState,
}

impl ConcurrentTransaction {
    pub fn add_node(&mut self, node: Node) -> Result<NodeId> {
        let mut db = self.db.lock().unwrap();
        db.add_node_internal(node, self.tx_id, 0) // commit_ts=0 until commit
    }
    
    pub fn get_node(&self, node_id: NodeId) -> Result<Option<Node>> {
        let mut db = self.db.lock().unwrap();
        db.get_node_with_snapshot(node_id, self.snapshot_ts, Some(self.tx_id))
    }
    
    pub fn commit(mut self) -> Result<()> {
        let mut db = self.db.lock().unwrap();
        // Allocate commit timestamp and update all version records
        // ...
    }
}
```

**Pros:**
- Simple implementation
- Works with existing GraphDB code
- No changes to core GraphDB needed

**Cons:**
- Coarse-grained locking - only one operation at a time
- No true concurrency benefit
- Still serializes reads and writes

### Option 2: Arc<RwLock<GraphDB>> - Allow Concurrent Reads

```rust
pub struct ConcurrentGraphDB {
    inner: Arc<RwLock<GraphDB>>,
}
```

**Pros:**
- Multiple concurrent readers
- Single writer

**Cons:**
- Still blocks all reads during writes
- MVCC allows concurrent reads DURING writes - we're not exploiting this

### Option 3: Fine-Grained Interior Mutability (Recommended)

Wrap individual mutable components with interior mutability:

```rust
pub struct GraphDB {
    // Immutable state
    path: PathBuf,
    config: Config,
    
    // Shared read-only access
    pager: Arc<Pager>,  // Already has internal locking
    
    // Protected mutable state with interior mutability
    node_index: Arc<RwLock<BTreeIndex>>,
    edge_index: Arc<RwLock<HashMap<EdgeId, RecordPointer>>>,
    label_index: Arc<RwLock<HashMap<String, BTreeSet<NodeId>>>>,
    
    // Caches - can use lock-free structures or RwLock
    node_cache: Arc<Mutex<LruCache<NodeId, Node>>>,
    edge_cache: Arc<Mutex<LruCache<EdgeId, Edge>>>,
    
    // MVCC components - already designed for concurrency
    mvcc_tx_manager: Arc<Mutex<MvccTransactionManager>>,
    timestamp_oracle: Arc<TimestampOracle>,  // Already has AtomicU64 internally
    
    // Transaction state
    next_tx_id: Arc<AtomicU64>,
}
```

**Pros:**
- True concurrent reads and writes
- Exploits MVCC's snapshot isolation
- Best performance
- Most scalable

**Cons:**
- Requires significant refactoring
- More complex
- Lock ordering must be carefully managed

### Option 4: Hybrid Approach (RECOMMENDED FOR NOW)

Use Option 1 (Mutex) but structure the API to enable future migration to Option 3:

```rust
// Public concurrent API
pub struct ConcurrentGraphDB {
    inner: Arc<Mutex<GraphDB>>,
}

// Keep existing GraphDB for backward compatibility
pub struct GraphDB { /* unchanged */ }

// New concurrent transaction type
pub struct ConcurrentTransaction {
    db: Arc<Mutex<GraphDB>>,
    tx_id: TxId,
    snapshot_ts: u64,
    // Buffer operations locally, apply in batch
    pending_writes: Vec<Write>,
}
```

**Why This Works:**
1. ✅ Provides concurrent API immediately
2. ✅ Backward compatible - existing code unchanged
3. ✅ Can be optimized later with fine-grained locks
4. ✅ Simple to implement and test
5. ✅ Still better than `&mut` - multiple threads can hold transactions

## Proposed Implementation Plan

### Phase 1: Concurrent Wrapper (This PR)

1. Create `ConcurrentGraphDB` wrapper around `Arc<Mutex<GraphDB>>`
2. Create `ConcurrentTransaction` that acquires lock per operation
3. Implement basic CRUD operations
4. Write tests demonstrating concurrent transactions
5. Document the API

### Phase 2: Optimizations (Future PR)

1. Add read-only transaction type that uses RwLock read guard
2. Batch operations within transaction to reduce lock acquisitions
3. Profile and identify lock contention hotspots

### Phase 3: Fine-Grained Locking (Future PR - Optional)

1. Add interior mutability to individual components
2. Lock-free caches using evmap or similar
3. Fine-grained index locking

## API Design

### Opening Database

```rust
use sombra::{ConcurrentGraphDB, Config};

// Open in concurrent mode
let db = ConcurrentGraphDB::open("my.db")?;
let db = ConcurrentGraphDB::open_with_config("my.db", config)?;

// Database handle is cloneable and thread-safe
let db2 = db.clone();
```

### Transactions

```rust
// Start a transaction (non-blocking)
let mut tx1 = db.begin_transaction()?;
let mut tx2 = db.begin_transaction()?;  // Can have multiple!

// Concurrent writes
std::thread::scope(|s| {
    s.spawn(|| {
        tx1.add_node(Node::new(1).with_label("Person"))?;
        tx1.commit()?;
    });
    
    s.spawn(|| {
        tx2.add_node(Node::new(2).with_label("Person"))?;
        tx2.commit()?;
    });
});

// Snapshot isolation guarantees each sees consistent view
```

### Read-Your-Own-Writes

```rust
let mut tx = db.begin_transaction()?;
let node_id = tx.add_node(Node::new(1))?;
let node = tx.get_node(node_id)?;  // Can see own uncommitted writes
assert!(node.is_some());
tx.commit()?;
```

### Concurrent Reads During Writes

```rust
// Long-running transaction
let mut writer = db.begin_transaction()?;

// Reader can start even while writer is active
let reader = db.begin_transaction()?;

// Writer modifies data
writer.add_node(Node::new(1))?;

// Reader sees snapshot from before writer started
let node = reader.get_node(1)?;
assert!(node.is_none());  // Writer hasn't committed yet

writer.commit()?;
reader.commit()?;
```

## Implementation Details

### Transaction Lifecycle

1. **Begin**: Acquire tx_id, allocate snapshot_ts from oracle
2. **Operations**: Each operation acquires lock, performs work, releases lock
3. **Commit**: 
   - Acquire lock
   - Allocate commit_ts from oracle
   - Update all version records with commit_ts
   - Release lock
4. **Cleanup**: Unregister transaction from MVCC manager

### Concurrency Guarantees

- **Snapshot Isolation**: Each transaction sees consistent snapshot
- **Read-Your-Own-Writes**: Transactions see their own uncommitted changes
- **Serializability**: NOT guaranteed (write-write conflicts not detected)
- **Durability**: WAL ensures durability on commit

### Performance Characteristics

- **Lock Duration**: Short - only held during individual operations
- **Contention**: Single lock point, but operations are fast
- **Scalability**: Good for read-heavy workloads, moderate for write-heavy
- **Memory**: Each transaction has small overhead (~100 bytes)

## Testing Strategy

### Unit Tests
- Multiple transactions can be created
- Concurrent reads see consistent snapshots
- Read-your-own-writes works
- Commit timestamp ordering

### Integration Tests
- Spawn 10 threads, each running transactions
- Verify no data corruption
- Verify snapshot isolation
- Verify WAL recovery with concurrent transactions

### Stress Tests
- 100+ concurrent transactions
- Mix of reads and writes
- Measure throughput and latency

## Migration Path

### For Existing Code

```rust
// Old single-threaded code - still works
let mut db = GraphDB::open("my.db")?;
let mut tx = db.begin_transaction()?;
tx.add_node(node)?;
tx.commit()?;

// New concurrent code
let db = ConcurrentGraphDB::open("my.db")?;
let mut tx = db.begin_transaction()?;
tx.add_node(node)?;
tx.commit()?;
```

APIs are similar, migration is straightforward.

### Compatibility

- ✅ Can open database created with `GraphDB` using `ConcurrentGraphDB`
- ✅ MVCC must be enabled in config
- ✅ File locking prevents mixing processes

## Open Questions

1. **Should we add write-write conflict detection?**
   - Current MVCC doesn't detect conflicts
   - Last writer wins
   - Could add optimistic locking later

2. **Should we support long-running read transactions?**
   - Current design allows it
   - But blocks GC from reclaiming old versions
   - Need transaction timeout?

3. **Should we add transaction priorities?**
   - Could help with scheduling
   - Adds complexity

4. **Auto-commit single operations?**
   ```rust
   db.add_node(node)?;  // Auto-commit
   ```
   - Convenient but hides cost
   - Decision: NO - explicit transactions only

## Success Criteria

1. ✅ Multiple threads can create transactions concurrently
2. ✅ Snapshot isolation works correctly
3. ✅ Read-your-own-writes works
4. ✅ No data corruption under concurrent load
5. ✅ Performance is acceptable (>1000 ops/sec with 10 concurrent threads)
6. ✅ Existing tests still pass
7. ✅ Documentation is clear

## Conclusion

The Hybrid Approach (Option 4) provides the best balance of:
- **Simplicity**: Easy to implement and understand
- **Functionality**: Enables concurrent transactions immediately
- **Performance**: Good enough for most use cases
- **Future-proof**: Can be optimized later without API changes

Let's proceed with implementation!
