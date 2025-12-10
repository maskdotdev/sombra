# Sombra Data Model

This document describes the data model for Sombra, a property graph database optimized for code intelligence and general-purpose graph workloads.

## Overview

Sombra implements a labeled property graph model where:

- **Nodes** have zero or more labels and key-value properties
- **Edges** connect two nodes with a type and optional properties
- Both nodes and edges can have arbitrary properties

## Core Identifiers

- `NodeId` and `EdgeId` are unsigned 64-bit integers (`u64`)
- IDs are allocated monotonically from counters stored in the database header
- `0` is reserved as a sentinel meaning "no reference"

## Nodes

Nodes are the primary entities in the graph:

```typescript
// TypeScript representation
interface Node {
    id: number; // Unique identifier
    labels: string[]; // Zero or more labels (e.g., ["User", "Admin"])
    properties: Record<string, PropertyValue>;
}
```

```python
# Python representation
@dataclass
class Node:
    id: int
    labels: list[str]
    properties: dict[str, PropertyValue]
```

### Labels

- Labels are strings that categorize nodes
- A node can have multiple labels (e.g., a User who is also an Admin)
- Labels are indexed for efficient lookup
- Label names must be valid UTF-8 and non-empty

### Common Node Patterns

```typescript
// Single label
const user = db.createNode("User", { name: "Alice", age: 30 });

// Multiple labels
const admin = db.createNode(["User", "Admin"], {
    name: "Bob",
    level: 10,
});
```

## Edges

Edges connect nodes and have a single type:

```typescript
// TypeScript representation
interface Edge {
    id: number; // Unique identifier
    src: number; // Source node ID
    dst: number; // Destination node ID
    type: string; // Edge type (e.g., "FOLLOWS")
    properties: Record<string, PropertyValue>;
}
```

### Edge Types

- Each edge has exactly one type
- Edge types are strings (e.g., "FOLLOWS", "WORKS_AT", "AUTHORED")
- Edge types are indexed for efficient traversal
- Type names must be valid UTF-8 and non-empty

### Directionality

Edges in Sombra are **directed** but can be traversed in both directions:

```typescript
// Create a directed edge from alice to bob
db.createEdge(alice, bob, "FOLLOWS", { since: 2024 });

// Query outgoing edges (alice follows who?)
db.neighbors(alice, { direction: "out", edgeType: "FOLLOWS" });

// Query incoming edges (who follows bob?)
db.neighbors(bob, { direction: "in", edgeType: "FOLLOWS" });

// Query both directions
db.neighbors(alice, { direction: "both", edgeType: "FOLLOWS" });
```

## Property Values

Properties are key-value pairs where keys are strings and values are typed:

| Type     | TypeScript   | Python     | Description                        |
| -------- | ------------ | ---------- | ---------------------------------- |
| Boolean  | `boolean`    | `bool`     | `true` or `false`                  |
| Integer  | `number`     | `int`      | Signed 64-bit integer              |
| Float    | `number`     | `float`    | IEEE-754 double precision          |
| String   | `string`     | `str`      | UTF-8 encoded text                 |
| Bytes    | `Uint8Array` | `bytes`    | Raw byte array                     |
| DateTime | `Date`       | `datetime` | Nanosecond precision UTC timestamp |
| Null     | `null`       | `None`     | Absence of value                   |

### Property Constraints

- Property keys must be non-empty UTF-8 strings
- Property keys must be unique within a node or edge
- Integer values must fit in signed 64-bit range
- Float values must be finite (no NaN or Infinity)
- DateTime values must include timezone information
- String values must be valid UTF-8

### Property Examples

```typescript
import { Database } from "sombradb";

const db = Database.open("./data.db");

// Various property types
db.createNode("Example", {
    active: true, // Boolean
    count: 42, // Integer
    score: 3.14159, // Float
    name: "Alice", // String
    created_at: new Date(), // DateTime
    data: new Uint8Array([1, 2, 3]), // Bytes
    optional: null, // Null
});
```

```python
from sombra import Database
from datetime import datetime, timezone

db = Database.open('./data.db')

# Various property types
db.create_node('Example', {
    'active': True,                              # Boolean
    'count': 42,                                 # Integer
    'score': 3.14159,                            # Float
    'name': 'Alice',                             # String
    'created_at': datetime.now(timezone.utc),   # DateTime
    'data': b'\x01\x02\x03',                     # Bytes
    'optional': None,                            # Null
})
```

## Schema

Sombra supports optional runtime schema validation:

```typescript
// Define schema for validation
const schema = {
    User: {
        name: { type: "string" },
        age: { type: "number" },
        email: { type: "string" },
    },
    Post: {
        title: { type: "string" },
        content: { type: "string" },
        created_at: { type: "datetime" },
    },
};

const db = Database.open("./data.db", { schema });

// Valid - matches schema
db.createNode("User", { name: "Alice", age: 30, email: "alice@example.com" });

// Invalid - unknown property 'unknown'
db.query().nodes("User").where(eq("unknown", "value")); // throws error
```

### TypeScript Typed Schema

For compile-time type safety, use the typed API:

```typescript
import { SombraDB } from "sombradb/typed";
import type { GraphSchema } from "sombradb/typed";

interface MySchema extends GraphSchema {
    nodes: {
        User: { properties: { name: string; age: number } };
        Post: { properties: { title: string; content: string } };
    };
    edges: {
        AUTHORED: {
            from: "User";
            to: "Post";
            properties: { at: number };
        };
    };
}

const db = new SombraDB<MySchema>("./data.db");

// Full type safety and autocomplete
const user = db.addNode("User", { name: "Alice", age: 30 });
```

## Indexing

Sombra maintains automatic indexes for efficient queries:

### Built-in Indexes

| Index           | Purpose                          |
| --------------- | -------------------------------- |
| Node ID         | O(1) node lookup by ID           |
| Edge ID         | O(1) edge lookup by ID           |
| Label Index     | Nodes by label                   |
| Edge Type Index | Edges by type                    |
| Adjacency Index | Outgoing/incoming edges per node |

### Property Indexing

Property-based queries use scan operations. For queries like:

```typescript
db.query().nodes("User").where(eq("email", "alice@example.com"));
```

Sombra scans all nodes with the `User` label and filters by property. For large datasets with frequent property queries, consider:

1. Using the label index to narrow results first
2. Caching frequently-accessed property lookups at the application layer

## Query Model

Sombra provides a fluent query builder for graph traversal:

### Node Queries

```typescript
// Find nodes by label and property
const users = await db
    .query()
    .nodes("User")
    .where(eq("name", "Alice"))
    .execute();
```

### Pattern Matching

```typescript
// Match multi-hop patterns
const results = await db
    .query()
    .match({ user: "User", post: "Post" })
    .where("AUTHORED", { var: "post", label: "Post" })
    .on("user", (scope) => scope.where(eq("name", "Alice")))
    .execute();
```

### Traversal

```typescript
// BFS traversal from a node
const reachable = db.bfsTraversal(startNodeId, 3, {
    direction: "out",
    edgeTypes: ["FOLLOWS", "KNOWS"],
});
```

## Data Integrity

### MVCC (Multi-Version Concurrency Control)

Sombra uses MVCC for concurrent access:

- Readers never block writers
- Writers never block readers
- Consistent snapshots for queries
- Automatic conflict detection for concurrent writes

### WAL (Write-Ahead Logging)

All writes go through a write-ahead log:

- Atomic transactions
- Crash recovery
- Configurable sync modes for durability/performance tradeoff

### Checksums

- Pages are checksummed to detect corruption
- Integrity verification available via CLI tools

## Best Practices

### Model Design

1. **Use meaningful labels**: Labels should represent entity types (`User`, `Document`, `File`)
2. **Use meaningful edge types**: Edge types should describe relationships (`FOLLOWS`, `CONTAINS`, `AUTHORED`)
3. **Normalize carefully**: Balance between query performance and data redundancy
4. **Consider query patterns**: Design your model around your most common queries

### Property Guidelines

1. **Use consistent naming**: Pick a convention (camelCase, snake_case) and stick to it
2. **Avoid deeply nested data**: Sombra properties are flat key-value pairs
3. **Use appropriate types**: Don't store numbers as strings
4. **Handle null explicitly**: Use null for optional properties

### Performance Considerations

1. **Leverage label indexes**: Query by label first, then filter by properties
2. **Batch creates**: Use the create builder for bulk inserts
3. **Limit traversal depth**: BFS traversals can be expensive; set reasonable limits
4. **Use streaming for large results**: Stream large result sets instead of loading all into memory

## Next Steps

- [Getting Started Guide](getting-started.md) - Basic usage patterns
- [Architecture](architecture.md) - Internal implementation details
- [Configuration](configuration.md) - Tuning options
