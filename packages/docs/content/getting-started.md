# Getting Started with Sombra

Welcome to Sombra, a high-performance embedded graph database for Rust, Python, and Node.js applications. Sombra stores your data in a single file with no external dependencies or daemon processes.

## Installation

### Node.js / TypeScript

```bash
npm install sombradb
# or
pnpm add sombradb
# or
bun add sombradb
```

### Python

```bash
pip install sombra
```

### Rust

```toml
[dependencies]
sombra = "0.3"
```

## Quick Start

### Node.js / TypeScript

```typescript
import { Database, eq } from "sombradb";

// Open or create a database
const db = Database.open("/tmp/sombra.db");

// Seed with demo data (optional)
db.seedDemo();

// Query nodes
const users = await db
    .query()
    .nodes("User")
    .where(eq("name", "Ada Lovelace"))
    .select("name", "bio")
    .execute();

console.log(users);

// Create a node
const userId = db.createNode("User", { name: "New User", bio: "Hello!" });

// Update a node
db.updateNode(userId, { set: { bio: "Updated bio" } });

// Delete a node (cascade removes connected edges)
db.deleteNode(userId, true);

// Close the database
db.close();
```

### Python

```python
from sombra import Database
from sombra.query import eq

# Open or create a database
db = Database.open("/tmp/sombra.db")

# Seed with demo data (optional)
db.seed_demo()

# Query nodes
users = (
    db.query()
    .nodes("User")
    .where(eq("name", "Ada Lovelace"))
    .select("name", "bio")
    .execute()
)

print(users)

# Create a node
user_id = db.create_node("User", {"name": "New User", "bio": "Hello!"})

# Update a node
db.update_node(user_id, set_props={"bio": "Updated bio"})

# Delete a node
db.delete_node(user_id, cascade=True)
```

### Rust

```rust
use sombra::ffi::{Database, DatabaseOptions};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open or create a database
    let db = Database::open("my_graph.sombra", DatabaseOptions::default())?;

    // Seed demo data
    db.seed_demo()?;

    // Execute a query
    let query = json!({
        "$schemaVersion": 1,
        "request_id": "example",
        "matches": [{ "var": "u", "label": "User" }],
        "predicate": {
            "op": "eq",
            "var": "u",
            "prop": "name",
            "value": { "t": "String", "v": "Ada Lovelace" }
        },
        "projections": [
            { "kind": "var", "var": "u" },
            { "kind": "prop", "var": "u", "prop": "name", "alias": "name" }
        ]
    });

    let results = db.execute_json(&query)?;
    println!("{}", serde_json::to_string_pretty(&results)?);

    Ok(())
}
```

## Core Concepts

### Nodes

Nodes represent entities in your graph. Each node has:

- A unique numeric ID (auto-generated)
- One or more labels (e.g., "User", "Post")
- Properties (key-value pairs)

```typescript
// Create a node with a single label
const userId = db.createNode("User", {
    name: "Alice",
    email: "alice@example.com",
    age: 30,
});

// Create a node with multiple labels
const adminId = db.createNode(["User", "Admin"], {
    name: "Bob",
    role: "superadmin",
});
```

### Edges

Edges represent relationships between nodes. Each edge has:

- A source node ID
- A destination node ID
- A type (e.g., "FOLLOWS", "AUTHORED")
- Optional properties

```typescript
// Create an edge
const edgeId = db.createEdge(aliceId, bobId, "FOLLOWS", {
    since: new Date("2024-01-01"),
    strength: 0.8,
});
```

### Properties

Supported property value types:

- `null`
- `boolean` (true/false)
- `integer` (safe 53-bit integers)
- `float` (finite numbers)
- `string`
- `bytes` (Buffer or Uint8Array)
- `datetime` (Date objects or ISO 8601 strings)

```typescript
db.createNode("Example", {
    isActive: true, // boolean
    count: 42, // integer
    score: 3.14, // float
    name: "Test", // string
    data: Buffer.from([1, 2]), // bytes
    created: new Date(), // datetime
    metadata: null, // null
});
```

## Querying Data

### Fluent Query Builder

The query builder provides a type-safe way to construct queries:

```typescript
import { eq, and, between, inList } from "sombradb";

// Simple query
const users = await db
    .query()
    .nodes("User")
    .where(eq("active", true))
    .select("name", "email")
    .execute();

// Complex predicates
const results = await db
    .query()
    .nodes("User")
    .where(and(inList("role", ["admin", "moderator"]), between("age", 18, 65)))
    .select("name", "role", "age")
    .execute();
```

### Available Predicates

| Predicate                  | Description           | Example                       |
| -------------------------- | --------------------- | ----------------------------- |
| `eq(prop, value)`          | Equals                | `eq('name', 'Alice')`         |
| `ne(prop, value)`          | Not equals            | `ne('status', 'deleted')`     |
| `lt(prop, value)`          | Less than             | `lt('age', 30)`               |
| `le(prop, value)`          | Less than or equal    | `le('age', 30)`               |
| `gt(prop, value)`          | Greater than          | `gt('score', 100)`            |
| `ge(prop, value)`          | Greater than or equal | `ge('score', 100)`            |
| `between(prop, low, high)` | Range (inclusive)     | `between('age', 18, 65)`      |
| `inList(prop, values)`     | Set membership        | `inList('role', ['a', 'b'])`  |
| `and(...preds)`            | Logical AND           | `and(eq('a', 1), eq('b', 2))` |
| `or(...preds)`             | Logical OR            | `or(eq('a', 1), eq('a', 2))`  |
| `not(pred)`                | Logical NOT           | `not(eq('deleted', true))`    |
| `exists(prop)`             | Property exists       | `exists('email')`             |
| `isNull(prop)`             | Property is null      | `isNull('deletedAt')`         |

### Traversing Edges

Query across relationships:

```typescript
// Find users that Alice follows
const following = await db
    .query()
    .match({ follower: "User", followee: "User" })
    .where("FOLLOWS", { var: "followee", label: "User" })
    .on("follower", (scope) => scope.where(eq("name", "Alice")))
    .select([{ var: "followee", as: "user" }])
    .execute();
```

### Streaming Results

For large result sets, use streaming to avoid memory issues:

```typescript
const stream = db.query().nodes("User").stream();

for await (const row of stream) {
    console.log(row);
}

stream.close();
```

## CRUD Operations

### Create

```typescript
// Single node
const nodeId = db.createNode("User", { name: "Alice" });

// Single edge
const edgeId = db.createEdge(srcId, dstId, "FOLLOWS", { since: 2024 });

// Bulk creation
const summary = db
    .create()
    .node(["User"], { name: "Alice" })
    .node(["User"], { name: "Bob" })
    .edge(0, "FOLLOWS", 1, { since: 2024 })
    .execute();

console.log(summary.nodes); // [id1, id2]
console.log(summary.edges); // [edgeId]
```

### Read

```typescript
// Get by ID
const node = db.getNodeRecord(nodeId);
const edge = db.getEdgeRecord(edgeId);

// Count
const userCount = db.countNodesWithLabel("User");
const followCount = db.countEdgesWithType("FOLLOWS");

// Neighbors
const neighbors = db.neighbors(nodeId, {
    direction: "out",
    edgeType: "FOLLOWS",
});
```

### Update

```typescript
// Update node
db.updateNode(nodeId, {
    set: { bio: "New bio", level: 5 },
    unset: ["temporary"],
});

// Update edge
db.updateEdge(edgeId, {
    set: { strength: 10 },
});
```

### Delete

```typescript
// Delete node (with cascade to remove edges)
db.deleteNode(nodeId, true);

// Delete node (fail if has edges)
db.deleteNode(nodeId, false);

// Delete edge
db.deleteEdge(edgeId);
```

## Transactions

Group multiple operations into an atomic transaction:

```typescript
await db.transaction(async (tx) => {
    const alice = tx.createNode(["User"], { name: "Alice" });
    const bob = tx.createNode(["User"], { name: "Bob" });
    tx.createEdge(alice, bob, "KNOWS", { since: 2024 });
    // All operations commit together or roll back on error
});
```

## Database Options

```typescript
const db = Database.open("/path/to/db", {
    createIfMissing: true, // Create if doesn't exist
    pageSize: 8192, // Page size in bytes
    cachePages: 4096, // Number of cached pages
    synchronous: "normal", // 'full' | 'normal' | 'off'
    autocheckpointMs: 5000, // Auto-checkpoint interval
});
```

## Error Handling

```typescript
import { SombraError, NotFoundError, ConflictError } from "sombradb";

try {
    const node = db.getNodeRecord(invalidId);
} catch (err) {
    if (err instanceof NotFoundError) {
        console.log("Node not found");
    } else if (err instanceof SombraError) {
        console.log(`Sombra error: ${err.code}`);
    }
}
```

## Next Steps

- [Node.js Guide](nodejs-guide.md) - Deep dive into TypeScript patterns
- [Python Guide](python-guide.md) - Python-specific features
- [Configuration](configuration.md) - Performance tuning
- [CLI Guide](cli.md) - Command-line administration
- [Architecture](architecture.md) - Technical deep dive
