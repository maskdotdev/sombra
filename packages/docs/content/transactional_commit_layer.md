# Transactional Commit Layer

The Transactional Commit Layer provides ACID-style transaction support for the Graphite graph database. This document explains how transactions work, how to use them, and what guarantees they provide.

## Overview

The Transactional Commit Layer sits on top of the existing Pager and WAL system to provide:
- **Atomicity**: Transactions are all-or-nothing
- **Consistency**: Database always remains in a valid state
- **Isolation**: Transactions don't interfere with each other
- **Durability**: Committed transactions survive crashes

## Quick Start

```rust
use graphite::prelude::*;

// Open or create a database
let mut db = GraphDB::open_arc("my_graph.db")?;

// Begin a transaction
let mut tx = db.begin_transaction()?;

// Add nodes and edges within the transaction
let node1 = tx.add_node(Node::new(0))?;
let node2 = tx.add_node(Node::new(1))?;
tx.add_edge(Edge::new(node1, node2, "KNOWS"))?;

// Commit the transaction (makes changes permanent)
tx.commit()?;

// Or rollback if something went wrong
// tx.rollback()?;
```

## Core Concepts

### Transaction States

Every transaction follows a simple state machine:

```
Active → Committed
   ↓
RolledBack
```

- **Active**: You can make changes to the database
- **Committed**: Changes are permanently saved to the WAL
- **RolledBack**: All changes are discarded

### Write-Ahead Logging (WAL)

When you commit a transaction:
1. All changes are written to the WAL file first
2. The WAL is synced to disk (fsync)
3. A special "commit marker" is written
4. Changes are applied to the main database during checkpointing

This ensures that even if the application crashes, committed transactions can be recovered.

## API Reference

### GraphDB Methods

```rust
impl GraphDB {
    // Begin a new transaction
    pub fn begin_transaction(&mut self) -> Result<Transaction<'_>>;
    
    // Apply all committed transactions from WAL to main database
    pub fn checkpoint(&mut self) -> Result<()>;
    
    // Flush is equivalent to checkpoint()
    pub fn flush(&mut self) -> Result<()>;
}
```

### Transaction Methods

```rust
impl<'db> Transaction<'db> {
    // Add a node within the transaction
    pub fn add_node(&mut self, node: Node) -> Result<NodeId>;
    
    // Add an edge within the transaction
    pub fn add_edge(&mut self, edge: Edge) -> Result<EdgeId>;
    
    // Get a node (reads work within transactions)
    pub fn get_node(&self, id: NodeId) -> Result<Option<Node>>;
    
    // Get neighbors (reads work within transactions)
    pub fn get_neighbors(&self, id: NodeId) -> Result<Vec<NodeId>>;
    
    // Commit the transaction (makes changes permanent)
    pub fn commit(self) -> Result<()>;
    
    // Rollback the transaction (discards all changes)
    pub fn rollback(self) -> Result<()>;
}
```

## Usage Patterns

### Basic Transaction

```rust
let mut tx = db.begin_transaction()?;

let user = tx.add_node(Node::new(0))?;
let post = tx.add_node(Node::new(1))?;
tx.add_edge(Edge::new(user, post, "AUTHORED"))?;

tx.commit()?;
```

### Error Handling with Rollback

```rust
let mut tx = db.begin_transaction()?;

match perform_complex_operation(&mut tx) {
    Ok(_) => tx.commit()?,
    Err(e) => {
        tx.rollback()?;
        return Err(e);
    }
}
```

### Read Operations During Transactions

```rust
let mut tx = db.begin_transaction()?;

let node1 = tx.add_node(Node::new(0))?;
let node2 = tx.add_node(Node::new(1))?;

// You can read your own uncommitted changes
let retrieved = tx.get_node(node1)?;
assert!(retrieved.is_some());

tx.commit()?;
```

## Transaction Guarantees

### Atomicity
- Either all changes in a transaction are applied, or none are
- If commit fails partway through, the database is unchanged
- Rollback completely discards all transaction changes

### Consistency
- Database always remains in a valid state
- Foreign key relationships are maintained
- No partial updates or corrupted data

### Isolation
- Only one transaction can be active at a time (single-writer model)
- Transactions see a consistent view of the database
- No interference between concurrent transactions

### Durability
- Once `commit()` returns successfully, the transaction is permanently saved
- Committed transactions survive application crashes
- Recovery happens automatically on database reopen

## Crash Recovery

When you open a database after a crash:

1. The WAL file is automatically scanned
2. Transactions with commit markers are replayed
3. Transactions without commit markers are discarded
4. The main database is updated with committed changes
5. The WAL is truncated after successful recovery

This happens automatically - you don't need to do anything special.

## Performance Considerations

### Memory Usage
- Transactions track dirty pages in memory
- Large transactions may consume more memory
- Consider breaking very large operations into multiple transactions

### Checkpointing
- Committed transactions live in the WAL until checkpoint
- Call `db.checkpoint()` periodically to control WAL size
- `db.flush()` is equivalent to checkpoint

### Batch Operations
```rust
// Good: Batch related operations in one transaction
let mut tx = db.begin_transaction()?;
for i in 0..1000 {
    tx.add_node(Node::new(i))?;
}
tx.commit()?;

// Avoid: Many small transactions
for i in 0..1000 {
    let mut tx = db.begin_transaction()?;
    tx.add_node(Node::new(i))?;
    tx.commit()?; // Much slower due to overhead
}
```

## Limitations

### Single Writer
- Only one transaction can be active at a time per database connection
- Multiple connections to the same database file are not supported yet

### Nested Transactions
- Nested transactions are not supported
- Attempting to begin a transaction while one is already active returns an error

### Rollback Implementation
- Currently reloads pages from disk for rollback
- Future versions may implement more efficient rollback mechanisms

## Testing

The transaction layer includes comprehensive tests:

```bash
# Run all transaction tests
cargo test transactions

# Run specific test categories
cargo test transactions::basic_commit
cargo test transactions::rollback
cargo test transactions::crash_recovery
```

Test coverage includes:
- Basic commit and rollback operations
- WAL behavior and checkpointing
- Transaction isolation
- Crash recovery scenarios
- Large transaction handling
- Error conditions

## Migration from Non-Transactional Code

If you have existing code using the direct GraphDB API:

```rust
// Old way (direct mutations)
let node1 = db.add_node(Node::new(0))?;
let node2 = db.add_node(Node::new(1))?;
db.add_edge(Edge::new(node1, node2, "KNOWS"))?;
db.flush()?;
```

```rust
// New way (transactional)
let mut tx = db.begin_transaction()?;
let node1 = tx.add_node(Node::new(0))?;
let node2 = tx.add_node(Node::new(1))?;
tx.add_edge(Edge::new(node1, node2, "KNOWS"))?;
tx.commit()?;
```

The transactional approach provides better safety guarantees and is the recommended way to write new code.

## Troubleshooting

### Common Errors

**"Transaction already active"**
- You're trying to begin a transaction while one is already in progress
- Complete or rollback the current transaction first

**"Transaction not active"**
- You're trying to commit or rollback a transaction that's already finished
- Each transaction can only be committed or rolled back once

**WAL grows too large**
- Call `db.checkpoint()` periodically to apply committed transactions
- Consider smaller transactions for better control

### Performance Tips

1. **Batch related operations** in single transactions
2. **Checkpoint periodically** to control WAL size
3. **Avoid very large transactions** that modify many pages
4. **Handle errors properly** to ensure rollback when needed

## Future Enhancements

Planned improvements to the transaction system:

- **Concurrent readers**: Allow multiple threads to read during transactions
- **Savepoints**: Partial rollback within transactions
- **Optimistic concurrency**: Multiple writers with conflict detection
- **Transaction isolation levels**: Different consistency guarantees
- **Nested transactions**: Transactions within transactions

## Conclusion

The Transactional Commit Layer provides robust ACID guarantees for Graphite while maintaining simplicity and performance. By understanding the concepts and patterns outlined in this document, you can effectively use transactions to build reliable graph database applications.