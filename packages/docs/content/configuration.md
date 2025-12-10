# Configuration Guide

Sombra provides flexible configuration options to optimize performance for different use cases. This guide covers all configuration options and their tradeoffs.

## Configuration Overview

### Node.js

```typescript
import { Database } from "sombradb";

const db = Database.open("./data.db", {
    createIfMissing: true,
    pageSize: 4096,
    cachePages: 1024,
    synchronous: "normal",
    autocheckpointMs: 30000,
    // Advanced WAL / MVCC settings
    pagerGroupCommitMaxWriters: 10,
    pagerAsyncFsync: true,
    versionCodec: "snappy",
    snapshotPoolSize: 4
});
```

### Python

```python
from sombra import Database

db = Database.open('./data.db',
    create_if_missing=True,
    page_size=4096,
    cache_pages=1024,
    synchronous='normal',
    autocheckpoint_ms=30000,
    # Advanced WAL / MVCC settings
    pager_group_commit_max_writers=10,
    pager_async_fsync=True,
    version_codec='snappy',
    snapshot_pool_size=4
)
```

## Connection Options

| Option                     | Type      | Default  | Description                                    |
| -------------------------- | --------- | -------- | ---------------------------------------------- |
| `createIfMissing`          | `boolean` | `true`   | Create database if it doesn't exist            |
| `pageSize`                 | `number`  | `4096`   | Page size in bytes                             |
| `cachePages`               | `number`  | `1024`   | Number of pages to cache                       |
| `distinctNeighborsDefault` | `boolean` | `true`   | Default distinct behavior for neighbor queries |
| `synchronous`              | `string`  | `'full'` | Sync mode: `'full'`, `'normal'`, `'off'`       |

## MVCC Configuration

| Option                      | Type           | Default | Description                                  |
| --------------------------- | -------------- | ------- | -------------------------------------------- |
| `inlineHistory`             | `boolean`      | -       | Embed newest history on page (true/false)    |
| `inlineHistoryMaxBytes`     | `number`       | -       | Max bytes for inline history                 |
| `versionCodec`              | `string`       | `'none'`| Compression for history: `'none'`, `'snappy'`|
| `versionCodecMinBytes`      | `number`       | -       | Min payload size to attempt compression      |
| `snapshotPoolSize`          | `number`       | -       | Cached snapshots to reuse for reads          |
| `snapshotPoolMaxAgeMs`      | `number`       | -       | Max age for pooled read snapshots            |

## WAL Configuration

| Option                   | Type             | Default | Description                                |
| ------------------------ | ---------------- | ------- | ------------------------------------------ |
| `walSegmentBytes`        | `number`         | -       | WAL segment size in bytes                  |
| `walPreallocateSegments` | `number`         | -       | Number of segments to preallocate          |
| `autocheckpointMs`       | `number \| null` | `30000` | Auto-checkpoint interval (null to disable) |

## Commit Coalescing

| Option             | Type     | Default | Description                                |
| ------------------ | -------- | ------- | ------------------------------------------ |
| `commitCoalesceMs` | `number` | -       | Milliseconds to wait for commit coalescing |
| `commitMaxFrames`  | `number` | -       | Maximum frames per coalesced commit        |
| `commitMaxCommits` | `number` | -       | Maximum commits to coalesce                |

## Group Commit

| Option                  | Type     | Default | Description                                 |
| ----------------------- | -------- | ------- | ------------------------------------------- |
| `groupCommitMaxWriters` | `number` | -       | Maximum concurrent writers for group commit |
| `groupCommitMaxFrames`  | `number` | -       | Maximum frames per group commit             |
| `groupCommitMaxWaitMs`  | `number` | -       | Maximum wait time for group commit          |

## Async Fsync

| Option                | Type      | Default | Description                       |
| --------------------- | --------- | ------- | --------------------------------- |
| `asyncFsync`          | `boolean` | -       | Enable async fsync                |
| `asyncFsyncMaxWaitMs` | `number`  | -       | Maximum wait time for async fsync |

## Synchronous Modes

The `synchronous` option controls durability vs. performance:

| Mode       | Description             | Durability | Performance |
| ---------- | ----------------------- | ---------- | ----------- |
| `'full'`   | Sync after every commit | Highest    | Lowest      |
| `'normal'` | Sync at critical points | Good       | Moderate    |
| `'off'`    | No explicit syncs       | None       | Highest     |

### When to Use Each Mode

**`'full'` (Default)**

- Production systems requiring data integrity
- Financial applications
- Any system where data loss is unacceptable

**`'normal'`**

- Development and testing
- Applications with moderate durability requirements
- Systems with battery-backed storage

**`'off'`**

- Benchmarking and performance testing
- Temporary/throwaway data
- Systems with external replication

## Runtime Configuration via Pragmas

You can query and modify some settings at runtime using pragmas:

### Node.js

```typescript
import { Database } from "sombradb";

const db = Database.open("./data.db");

// Get current synchronous mode
const mode = db.pragma("synchronous");
console.log("Current mode:", mode);

// Change synchronous mode
db.pragma("synchronous", "normal");

// Get/set autocheckpoint interval
const interval = db.pragma("autocheckpoint_ms");
db.pragma("autocheckpoint_ms", 60000); // 1 minute

// Disable autocheckpoint
db.pragma("autocheckpoint_ms", null);
```

### Python

```python
from sombra import Database

db = Database.open('./data.db')

# Get current synchronous mode
mode = db.pragma('synchronous')
print(f'Current mode: {mode}')

# Change synchronous mode
db.pragma('synchronous', 'normal')

# Get/set autocheckpoint interval
interval = db.pragma('autocheckpoint_ms')
db.pragma('autocheckpoint_ms', 60000)  # 1 minute

# Disable autocheckpoint
db.pragma('autocheckpoint_ms', None)
```

## Performance Tuning

### Cache Size

The page cache is the most important performance setting. Larger cache = more data in memory = fewer disk reads.

```typescript
// Small cache (~4MB) - memory-constrained environments
const db = Database.open("./data.db", { cachePages: 1024 });

// Medium cache (~40MB) - general use
const db = Database.open("./data.db", { cachePages: 10000 });

// Large cache (~400MB) - memory-rich servers
const db = Database.open("./data.db", { cachePages: 100000 });
```

**Guidelines:**

- Each page is typically 4KB
- `cachePages * pageSize` = total cache memory
- Start with 10-20% of available RAM
- Monitor cache hit rates and adjust

### Checkpoint Interval

Auto-checkpoint controls how often WAL frames are merged into the main database:

```typescript
// Frequent checkpoints - lower WAL size, more I/O
const db = Database.open("./data.db", { autocheckpointMs: 10000 });

// Infrequent checkpoints - larger WAL, less I/O
const db = Database.open("./data.db", { autocheckpointMs: 300000 });

// Disable auto-checkpoint (manual checkpointing only)
const db = Database.open("./data.db", { autocheckpointMs: null });
```

**Tradeoffs:**

- Shorter interval: Smaller WAL, faster recovery, more I/O overhead
- Longer interval: Larger WAL, longer recovery, better write throughput

### Write-Heavy Workloads

For applications with many writes:

```typescript
const db = Database.open("./data.db", {
    synchronous: "normal",
    autocheckpointMs: 60000,
    commitCoalesceMs: 10,
    pagerGroupCommitMaxWriters: 16,
    pagerAsyncFsync: true
});
```

### Read-Heavy Workloads

For applications with mostly reads:

```typescript
const db = Database.open("./data.db", {
    cachePages: 50000, // Large cache
    synchronous: "full", // Full durability
    autocheckpointMs: 30000, // Standard checkpointing
});
```

### Benchmarking

For maximum performance during benchmarks (data loss acceptable):

```typescript
const db = Database.open("./data.db", {
    synchronous: "off",
    autocheckpointMs: null,
    cachePages: 100000,
});
```

## Environment-Specific Configuration

### Development

```typescript
const db = Database.open("./dev.db", {
    createIfMissing: true,
    synchronous: "full", // Catch durability issues early
    cachePages: 2048, // Small cache
    autocheckpointMs: 10000, // Frequent checkpoints
});
```

### Testing

```typescript
import { tmpdir } from "node:os";
import { join } from "node:path";

const db = Database.open(join(tmpdir(), "test.db"), {
    createIfMissing: true,
    synchronous: "off", // Speed over durability
    cachePages: 1024, // Minimal cache
    autocheckpointMs: null, // No auto-checkpoint
});
```

### Production

```typescript
const db = Database.open("/var/lib/myapp/production.db", {
    createIfMissing: false, // Fail if missing (explicit creation)
    synchronous: "full", // Maximum durability
    cachePages: 50000, // ~200MB cache
    autocheckpointMs: 30000, // 30-second checkpoint interval
});
```

## Schema Configuration

Runtime schema validation can be enabled to catch property errors:

```typescript
const db = Database.open("./data.db", {
    schema: {
        User: {
            name: { type: "string" },
            age: { type: "number" },
            email: { type: "string" },
        },
        Post: {
            title: { type: "string" },
            content: { type: "string" },
        },
    },
});

// This will throw an error - 'unknown' is not in the schema
db.query().nodes("User").where(eq("unknown", "value"));
```

## Monitoring and Diagnostics

### Check Database Health

Use the CLI to inspect database state:

```bash
# Show database info
sombra inspect mydb.db info

# Show WAL status
sombra inspect mydb.db wal-info

# Verify integrity
sombra verify mydb.db
```

### Database Lifecycle

```typescript
const db = Database.open('./data.db')

// Check if database is open
console.log(db.isClosed)  // false

// Close the database
db.close()
console.log(db.isClosed)  // true

// Context manager pattern (Python)
with Database.open('./data.db') as db:
    # operations
# Auto-closed here
```

## Best Practices

### 1. Always Close Databases

```typescript
const db = Database.open("./data.db");
try {
    // operations
} finally {
    db.close();
}
```

Or use context managers in Python:

```python
with Database.open('./data.db') as db:
    # operations
```

### 2. Choose Synchronous Mode Based on Requirements

- Production: Start with `'full'`
- If performance is critical and you have replication: Consider `'normal'`
- Never use `'off'` in production

### 3. Monitor WAL Size

Large WAL files can indicate:

- Too infrequent checkpointing
- Long-running read transactions holding snapshots
- High write throughput

### 4. Size Cache Appropriately

Too small: Poor read performance, frequent disk I/O
Too large: Memory pressure, potential swapping

Start with 10% of available RAM and adjust based on workload.

### 5. Test Configuration Changes

Always benchmark configuration changes with representative workloads before deploying to production.

## Troubleshooting

### Slow Queries

1. Increase `cachePages`
2. Check if queries are hitting indexes (use `explain()`)
3. Consider query restructuring

### High Memory Usage

1. Decrease `cachePages`
2. Check for long-running queries holding snapshots
3. Enable more frequent checkpointing

### WAL File Growing Large

1. Decrease `autocheckpointMs`
2. Check for stuck readers
3. Manually trigger checkpoint via CLI

### Data Integrity Issues

1. Ensure `synchronous: 'full'` in production
2. Run `sombra verify` regularly
3. Check for storage hardware issues

## Next Steps

- [CLI Tools](cli.md) - Database inspection and maintenance
- [Architecture](architecture.md) - Internal implementation details
- [Performance](performance.md) - Benchmarks and optimization
