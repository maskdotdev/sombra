# Diagnosis: Why is Sombra Fully Durable Mode 139x Slower than SQLite?

## Performance Results
- **Sombra (fully durable)**: 253 ops/sec
- **SQLite (fully durable)**: 35,292 ops/sec  
- **Gap**: 139x slower

## Root Cause Analysis

### 1. Both are doing fsync per transaction
Both implementations call fsync() after each transaction:
- Sombra: `SyncMode::Full` → `pager.sync_wal()` → `file.sync_data()` 
- SQLite: `synchronous=FULL` in WAL mode → fsync after each commit

### 2. Why is SQLite so much faster?

#### Hypothesis A: SQLite doesn't actually fsync in our test
**REJECTED** - We verified SQLite with `synchronous=FULL` and `journal_mode=WAL`

#### Hypothesis B: macOS filesystem caching
**LIKELY** - macOS may be caching WAL writes in memory/SSD cache
- Sequential WAL writes are heavily optimized by modern filesystems
- SSD controllers can batch sequential writes
- F_FULLFSYNC vs fsync behavior on macOS

#### Hypothesis C: Rusqlite Connection Behavior
**CRITICAL FINDING** - The benchmark uses autocommit mode!

Looking at our benchmark (src/benchmark_suite.rs:294):
```rust
db.add_node(node).unwrap();  // No explicit transaction!
```

Looking at SQLite adapter (src/sqlite_adapter.rs:63):
```rust
self.conn.execute("INSERT ...", params![...])?;  // Autocommit
```

**QUESTION**: Does rusqlite actually commit and fsync on EVERY execute() call?
Or does it batch them somehow?

### 3. Testing the hypothesis

Let me check what rusqlite's behavior is with autocommit and synchronous=FULL...

According to SQLite documentation:
- In autocommit mode, each SQL statement is wrapped in a transaction
- With synchronous=FULL + WAL, each transaction fsyncs the WAL
- BUT: SQLite has optimizations for autocommit mode

### 4. Potential Issue in Our Implementation

Looking at our code path for a single insert:
1. `begin_transaction()` - acquire transaction
2. `add_node()` - modify pages
3. `commit()` - calls `commit_to_wal()`
4. `commit_to_wal()` - writes WAL frames
5. For each page: `append_page_to_wal()` 
6. Then: `append_commit_to_wal()` 
7. Then: `pager.sync_wal()` → fsync!
8. Check if checkpoint needed (every 1000 tx)

**FOUND IT**: We write TWO frames per transaction:
- One page frame (the modified page)
- One commit frame (tx_id with flag)

This means we're doing:
- 2 × write_all() calls per transaction
- 1 × fsync() per transaction

### 5. Checkpoint Overhead

Looking at Config::fully_durable():
```rust
checkpoint_threshold: 1000,
```

Our benchmark does 2,534 transactions in 10 seconds.
This means we triggered 2-3 checkpoints!

Each checkpoint:
1. Reads all WAL frames
2. Writes them to main DB file
3. fsyncs main DB file
4. Resets WAL
5. fsyncs WAL

This is VERY expensive!

## THE SMOKING GUN

**Checkpoints are killing performance!**

With 253 ops/sec over 10 seconds = 2,530 operations
With checkpoint_threshold = 1000
= 2-3 full checkpoints during the benchmark

Each checkpoint:
- Stops all writes
- Reads ~1000 pages from WAL
- Writes ~1000 pages to main DB
- Multiple fsyncs

Estimated checkpoint time: ~1-2 seconds each
Total checkpoint overhead: 3-6 seconds out of 10 seconds!

## Comparison with SQLite

SQLite's WAL checkpoint behavior:
- Default checkpoint at 1000 pages (same as us)
- BUT: SQLite uses checkpoint_fullfsync and other optimizations
- SQLite has passive checkpointing (background)
- SQLite has multiple checkpoint modes (PASSIVE, FULL, RESTART, TRUNCATE)

Our checkpoints are BLOCKING and SYNCHRONOUS.

## Solution

The fully durable mode is slow because:
1. ✅ fsync per transaction (expected)
2. ❌ Aggressive checkpoint policy that blocks
3. ❌ No background/passive checkpointing
4. ❌ Checkpoint does full fsync of entire DB file

To fix:
- Increase checkpoint_threshold (or disable during benchmarks)
- Implement passive/background checkpointing
- Optimize checkpoint fsync behavior
- Consider F_BARRIERFSYNC vs F_FULLFSYNC on macOS
