# Transaction Layer Design

**📖 For a complete user guide, see [Transactional Commit Layer](transactional_commit_layer.md)**

This document contains the original technical design specification for Graphite's transaction system. The implementation is now complete and fully tested.

## Implementation Status ✅

The Transactional Commit Layer has been fully implemented with the following features:

- ✅ **ACID Transactions**: Atomic, Consistent, Isolated, Durable operations
- ✅ **Write-Ahead Logging**: All changes logged before being applied
- ✅ **Crash Recovery**: Automatic recovery on database restart
- ✅ **Rollback Support**: Complete transaction rollback capability
- ✅ **Comprehensive Testing**: 32 tests covering all scenarios

## Quick API Overview

```rust
// Begin a transaction
let mut tx = db.begin_transaction()?;

// Make changes
let node1 = tx.add_node(Node::new(0))?;
let node2 = tx.add_node(Node::new(1))?;
tx.add_edge(Edge::new(node1, node2, "KNOWS"))?;

// Commit or rollback
tx.commit()?;  // Makes changes permanent
// tx.rollback()?;  // Discards all changes
```

## Technical Architecture

### Core Components

1. **Transaction Manager**: Handles transaction lifecycle and state
2. **WAL Integration**: Extends WAL with transaction IDs and commit markers
3. **Page Tracking**: Tracks dirty pages per transaction for rollback
4. **Crash Recovery**: Replays committed transactions from WAL

### Key Design Decisions

- **Single Writer Model**: Only one active transaction per database connection
- **Deferred Checkpointing**: Changes go to WAL first, main file updated during checkpoint
- **Page-Level Tracking**: Efficient rollback by tracking modified pages
- **Deterministic Ordering**: Sorted page IDs ensure reproducible behavior

### WAL Frame Format

Extended WAL frame header with transaction metadata:

| Offset | Size | Field        | Purpose                     |
|--------|------|--------------|-----------------------------|
| 12     | 4    | transaction  | Monotonic transaction ID    |
| 16     | 4    | flags        | `0x1` = commit marker       |

## Implementation Details

### Transaction State Machine

```
Active → Committed
   ↓
RolledBack
```

### Commit Process

1. Validate transaction is active
2. Gather dirty pages in deterministic order
3. Write WAL frames with transaction ID
4. Write commit marker frame
5. Fsync WAL for durability
6. Mark transaction as committed

### Rollback Process

1. Validate transaction is active
2. Reload tracked pages from disk
3. Clear dirty flags
4. Mark transaction as rolled back

### Crash Recovery

1. Scan WAL for transaction frames
2. Group frames by transaction ID
3. Apply only transactions with commit markers
4. Truncate WAL after successful recovery

## Testing Coverage

The implementation includes comprehensive tests:

- **Basic Operations**: Commit, rollback, error handling
- **WAL Behavior**: Frame writing, checkpointing, truncation
- **Isolation**: Single-writer enforcement, state consistency
- **Persistence**: Transaction ID survival across restarts
- **Crash Recovery**: Incomplete transaction handling
- **Scalability**: Large transaction performance

Run tests with: `cargo test transactions`

## Performance Characteristics

- **Memory Usage**: O(dirty_pages) per transaction
- **Disk I/O**: Sequential WAL writes during commit
- **Recovery Time**: O(WAL_size) on database open
- **Checkpoint Overhead**: O(committed_transactions) during flush

## Future Enhancements

Planned improvements for future versions:

- **Concurrent Readers**: Allow reads during active transactions
- **Nested Transactions**: Savepoints and partial rollback
- **Optimistic Concurrency**: Multiple writers with conflict detection
- **Transaction Isolation Levels**: Different consistency guarantees

---

*For detailed usage instructions, examples, and troubleshooting, see the [Transactional Commit Layer guide](transactional_commit_layer.md).*
