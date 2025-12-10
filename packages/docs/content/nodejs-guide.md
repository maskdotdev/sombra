# Node.js Binding Guide

This guide covers using Sombra from Node.js and TypeScript, including installation, basic operations, and integration with the Node.js ecosystem.

## Installation

### From npm (Recommended)

```bash
npm install sombradb
```

### From Source

```bash
git clone https://github.com/sombra-db/sombra
cd sombra/bindings/node
npm install
npm run build
npm pack
npm install ./sombradb-*.tgz
```

## Quick Start

### TypeScript

```typescript
import { Database, eq } from "sombradb";

// Open a database (creates if missing by default)
const db = Database.open("./example.db", { createIfMissing: true });

// Create nodes
const alice = db.createNode("User", { name: "Alice", age: 30 });
const bob = db.createNode("User", { name: "Bob", age: 25 });

// Create an edge
db.createEdge(alice, bob, "FOLLOWS", { since: 2024 });

// Query nodes
const users = await db
    .query()
    .nodes("User")
    .where(eq("name", "Alice"))
    .execute();

console.log(users);

// Close when done
db.close();
```

### JavaScript

```javascript
const { Database, eq } = require("sombradb");

const db = Database.open("./example.db", { createIfMissing: true });

const alice = db.createNode("User", { name: "Alice", age: 30 });
const bob = db.createNode("User", { name: "Bob", age: 25 });

db.createEdge(alice, bob, "FOLLOWS", { since: 2024 });

const users = await db
    .query()
    .nodes("User")
    .where(eq("name", "Alice"))
    .execute();

console.log(users);
db.close();
```

## TypeScript Support

Sombra provides two APIs for TypeScript users:

1. **Standard API**: Direct bindings with TypeScript definitions
2. **Typed API** (Recommended): Type-safe wrapper with schema validation at compile time

### Standard API with Schema

```typescript
import { Database, eq } from "sombradb";

// Define a schema for runtime validation
const schema = {
    User: {
        name: { type: "string" },
        age: { type: "number" },
        email: { type: "string" },
    },
    Post: {
        title: { type: "string" },
        content: { type: "string" },
    },
};

const db = Database.open("./blog.db", { schema });

// Runtime validation will catch unknown properties
db.query().nodes("User").where(eq("name", "Alice")); // OK
db.query().nodes("User").where(eq("unknown", "value")); // Error!
```

### Typed API (Full Type Safety)

For enhanced type safety with autocomplete and compile-time validation:

```typescript
import { SombraDB } from "sombradb/typed";
import type { GraphSchema } from "sombradb/typed";

interface MyGraphSchema extends GraphSchema {
    nodes: {
        Person: { properties: { name: string; age: number } };
        Company: { properties: { name: string; employees: number } };
    };
    edges: {
        WORKS_AT: {
            from: "Person";
            to: "Company";
            properties: { role: string; since: number };
        };
        KNOWS: {
            from: "Person";
            to: "Person";
            properties: { since: number };
        };
    };
}

const db = new SombraDB<MyGraphSchema>("./example.db");

// Full type safety on node creation
const alice = db.addNode("Person", { name: "Alice", age: 30 });
const acme = db.addNode("Company", { name: "ACME", employees: 100 });

// Type-safe edge creation (validates from/to labels match schema)
db.addEdge(alice, acme, "WORKS_AT", { role: "Engineer", since: 2020 });

// Property lookup with type inference
const found = db.findNodeByProperty("Company", "name", "ACME");
const node = db.getNode(found!, "Company");
console.log(node?.properties.employees); // Type: number

// Type-safe neighbors
const employees = db.getIncomingNeighbors(acme, "WORKS_AT");
```

Benefits of the Typed API:

- **Autocomplete**: Full IntelliSense for labels, edge types, and properties
- **Type Safety**: Compile-time validation of property types
- **Edge Validation**: Ensures edges connect valid label pairs
- **Cleaner Code**: Works with plain JavaScript objects

## Opening and Closing Databases

```typescript
import { Database } from "sombradb";

// Open with options
const db = Database.open("./data.db", {
    createIfMissing: true, // Create database if it doesn't exist
    pageSize: 4096, // Page size in bytes
    cachePages: 1024, // Number of pages to cache
    synchronous: "normal", // 'full' | 'normal' | 'off'
    autocheckpointMs: 30000, // Auto-checkpoint interval (null to disable)
});

// Check if database is closed
console.log(db.isClosed); // false

// Close explicitly
db.close();
console.log(db.isClosed); // true

// Operations on closed database throw ClosedError
db.query(); // throws ClosedError
```

## Basic Operations

### Creating Nodes

```typescript
// Single label
const userId = db.createNode("User", {
    username: "john_doe",
    email: "john@example.com",
    active: true,
    score: 95.5,
});

// Multiple labels
const adminId = db.createNode(["User", "Admin"], {
    username: "admin",
    level: 10,
});

// Using the create builder (returns IDs)
const result = db
    .create()
    .node("User", { name: "Alice" }, "$alice")
    .node("User", { name: "Bob" }, "$bob")
    .edge("$alice", "FOLLOWS", "$bob")
    .execute();

console.log(result.nodes); // [1, 2]
console.log(result.edges); // [1]
console.log(result.alias("$alice")); // 1
```

### Updating Nodes

```typescript
// Set properties
db.updateNode(userId, { set: { score: 98.2, verified: true } });

// Unset properties
db.updateNode(userId, { unset: ["score"] });

// Both at once
db.updateNode(userId, {
    set: { lastLogin: Date.now() },
    unset: ["temporaryFlag"],
});
```

### Deleting Nodes

```typescript
// Delete node (fails if has edges)
db.deleteNode(userId);

// Delete node and all connected edges (cascade)
db.deleteNode(userId, true);
```

### Creating Edges

```typescript
const edgeId = db.createEdge(alice, bob, "FOLLOWS", {
    since: 2021,
    strength: 0.8,
});
```

### Deleting Edges

```typescript
db.deleteEdge(edgeId);
```

### Reading Nodes and Edges

```typescript
// Get node record
const node = db.getNodeRecord(userId);
// { labels: ['User'], properties: { name: 'Alice', age: 30 } }

// Get edge record
const edge = db.getEdgeRecord(edgeId);
// { src: 1, dst: 2, type: 'FOLLOWS', properties: { since: 2021 } }

// List nodes by label
const userIds = db.listNodesWithLabel("User");

// Count nodes/edges
const userCount = db.countNodesWithLabel("User");
const followsCount = db.countEdgesWithType("FOLLOWS");
```

## Query Builder

Sombra provides a powerful fluent query builder for complex graph queries.

### Simple Node Queries

```typescript
import { eq, and, or, not, between, inList } from "sombradb";

// Find users by name
const users = await db
    .query()
    .nodes("User")
    .where(eq("name", "Alice"))
    .execute();

// Complex predicates
const results = await db
    .query()
    .nodes("User")
    .where(
        and(
            inList("name", ["Alice", "Bob", "Charlie"]),
            between("age", 18, 65),
            not(eq("status", "inactive")),
        ),
    )
    .execute();

// Select specific properties (returns scalar values)
const names = await db.query().nodes("User").select("name").execute();
// [{ name: 'Alice' }, { name: 'Bob' }]
```

### Pattern Matching with match()

```typescript
// Match nodes connected by edges
const followers = await db
    .query()
    .match({ user: "User", follower: "User" })
    .where("FOLLOWS", { var: "user", label: "User" })
    .on("user", (scope) => scope.where(eq("name", "Alice")))
    .select([
        { var: "follower", as: "follower" },
        { var: "user", as: "user" },
    ])
    .execute();
```

### Predicate Functions

| Function                   | Description           | Example                                   |
| -------------------------- | --------------------- | ----------------------------------------- |
| `eq(prop, value)`          | Equal to              | `eq('name', 'Alice')`                     |
| `ne(prop, value)`          | Not equal to          | `ne('status', 'deleted')`                 |
| `lt(prop, value)`          | Less than             | `lt('age', 30)`                           |
| `le(prop, value)`          | Less than or equal    | `le('score', 100)`                        |
| `gt(prop, value)`          | Greater than          | `gt('followers', 1000)`                   |
| `ge(prop, value)`          | Greater than or equal | `ge('rating', 4.0)`                       |
| `between(prop, low, high)` | Range (inclusive)     | `between('age', 18, 65)`                  |
| `inList(prop, values)`     | In list               | `inList('status', ['active', 'pending'])` |
| `exists(prop)`             | Property exists       | `exists('email')`                         |
| `isNull(prop)`             | Is null               | `isNull('deletedAt')`                     |
| `isNotNull(prop)`          | Is not null           | `isNotNull('verifiedAt')`                 |
| `and(...exprs)`            | Logical AND           | `and(eq('a', 1), eq('b', 2))`             |
| `or(...exprs)`             | Logical OR            | `or(eq('a', 1), eq('a', 2))`              |
| `not(expr)`                | Logical NOT           | `not(eq('status', 'deleted'))`            |

### Query Execution Options

```typescript
// Basic execute - returns array of rows
const rows = await db.query().nodes("User").execute();

// Execute with metadata
const { rows, request_id, features } = await db
    .query()
    .nodes("User")
    .requestId("my-query-id")
    .execute(true);

// Streaming for large results
const stream = db.query().nodes("User").stream();
for await (const row of stream) {
    console.log(row);
}

// Explain query plan
const plan = await db
    .query()
    .nodes("User")
    .where(eq("name", "Alice"))
    .explain();
```

### Query Direction

```typescript
// Follow edges in specific direction
const outgoing = await db
    .query()
    .match("User")
    .where("FOLLOWS", "User")
    .direction("out")
    .execute();

const incoming = await db
    .query()
    .match("User")
    .where("FOLLOWS", "User")
    .direction("in")
    .execute();

// Bidirectional traversal
const both = await db
    .query()
    .match("User")
    .where("FOLLOWS", "User")
    .bidirectional()
    .execute();
```

## Graph Traversal

### Neighbors

```typescript
// Get outgoing neighbors
const following = db.getOutgoingNeighbors(userId, "FOLLOWS");

// Get incoming neighbors
const followers = db.getIncomingNeighbors(userId, "FOLLOWS");

// Get detailed neighbor info
const neighbors = db.neighbors(userId, {
    direction: "out", // 'out' | 'in' | 'both'
    edgeType: "FOLLOWS", // optional filter
    distinct: true, // deduplicate
});
// [{ nodeId: 2, edgeId: 1, typeId: 5 }, ...]
```

### BFS Traversal

```typescript
// Breadth-first traversal
const visited = db.bfsTraversal(startNodeId, 3, {
    direction: "out",
    edgeTypes: ["FOLLOWS", "KNOWS"],
    maxResults: 100,
});

for (const { nodeId, depth } of visited) {
    console.log(`Node ${nodeId} at depth ${depth}`);
}
```

## Mutations

### Mutation Script

```typescript
const summary = db.mutate({
    ops: [
        { op: "createNode", labels: ["User"], props: { name: "Alice" } },
        { op: "createNode", labels: ["User"], props: { name: "Bob" } },
        { op: "createEdge", src: 1, dst: 2, ty: "FOLLOWS", props: {} },
        { op: "updateNode", id: 1, set: { verified: true } },
        { op: "deleteEdge", id: 1 },
        { op: "deleteNode", id: 2, cascade: true },
    ],
});

console.log(summary.createdNodes); // [1, 2]
console.log(summary.createdEdges); // [1]
console.log(summary.deletedNodes); // 1
```

### Batched Mutations

```typescript
// Multiple operations in one commit
const summary = db.mutateMany([
    { op: "createNode", labels: ["User"], props: { name: "User1" } },
    { op: "createNode", labels: ["User"], props: { name: "User2" } },
]);

// Batched with chunk size (for very large operations)
const ops = users.map((u) => ({
    op: "createNode" as const,
    labels: ["User"],
    props: u,
}));
const summary = db.mutateBatched(ops, { batchSize: 1000 });
```

### Transactions

```typescript
const { summary, result } = await db.transaction(async (tx) => {
    tx.createNode("User", { name: "Alice" });
    tx.createNode("User", { name: "Bob" });
    tx.createEdge(1, 2, "FOLLOWS", {});

    // Async operations allowed
    await someAsyncOperation();

    return "success";
});

console.log(summary.createdNodes); // [1, 2]
console.log(result); // 'success'
```

## Error Handling

Sombra provides typed error classes for different failure modes:

```typescript
import {
    SombraError,
    AnalyzerError,
    IoError,
    CorruptionError,
    ConflictError,
    ClosedError,
    NotFoundError,
    ErrorCode,
} from "sombradb";

try {
    await db.query().nodes("User").where(eq("invalid", "query")).execute();
} catch (err) {
    if (err instanceof AnalyzerError) {
        console.log("Query syntax error:", err.message);
    } else if (err instanceof IoError) {
        console.log("Database I/O error:", err.message);
    } else if (err instanceof ClosedError) {
        console.log("Database was closed:", err.message);
    } else if (err instanceof SombraError) {
        console.log(`Error [${err.code}]:`, err.message);
    }
}

// Error codes
ErrorCode.ANALYZER; // Query analysis failed
ErrorCode.IO; // I/O operation failed
ErrorCode.CORRUPTION; // Data corruption detected
ErrorCode.CONFLICT; // Write-write conflict
ErrorCode.SNAPSHOT_TOO_OLD; // MVCC snapshot evicted
ErrorCode.CANCELLED; // Operation cancelled
ErrorCode.INVALID_ARG; // Invalid argument
ErrorCode.NOT_FOUND; // Resource not found
ErrorCode.CLOSED; // Database closed
```

## Database Configuration

### Pragmas

```typescript
// Get pragma value
const syncMode = db.pragma("synchronous");

// Set pragma value
db.pragma("synchronous", "normal");

// Available pragmas
db.pragma("synchronous"); // 'full' | 'normal' | 'off'
db.pragma("autocheckpoint_ms"); // number | null
```

### Request Cancellation

```typescript
// Start a long-running query with a request ID
const promise = db.query().nodes("User").requestId("slow-query").execute();

// Cancel it from another context
setTimeout(() => {
    db.cancelRequest("slow-query");
}, 1000);

try {
    await promise;
} catch (err) {
    if (err instanceof CancelledError) {
        console.log("Query was cancelled");
    }
}
```

## Integration with Express.js

```typescript
import express from "express";
import { Database, eq, ClosedError } from "sombradb";

const app = express();
app.use(express.json());

const db = Database.open("./social_network.db", { createIfMissing: true });

// Graceful shutdown
process.on("SIGTERM", () => {
    console.log("Closing database...");
    db.close();
    process.exit(0);
});

app.post("/users", async (req, res) => {
    try {
        const { name, email } = req.body;
        const userId = db.createNode("User", { name, email });
        res.status(201).json({ id: userId, name, email });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get("/users/:id", async (req, res) => {
    try {
        const userId = Number(req.params.id);
        const user = db.getNodeRecord(userId);
        if (!user) {
            return res.status(404).json({ error: "User not found" });
        }
        res.json({ id: userId, ...user });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get("/users/:id/followers", async (req, res) => {
    try {
        const userId = Number(req.params.id);
        const followerIds = db.getIncomingNeighbors(userId, "FOLLOWS");
        const followers = followerIds.map((id) => ({
            id,
            ...db.getNodeRecord(id),
        }));
        res.json(followers);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get("/users/search", async (req, res) => {
    try {
        const { name } = req.query;
        const users = await db
            .query()
            .nodes("User")
            .where(eq("name", name as string))
            .execute();
        res.json(users);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.listen(3000, () => {
    console.log("Server running on port 3000");
});
```

## Testing with Ava/Jest

```typescript
import test from "ava";
import { Database, eq } from "sombradb";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

function tempPath(): string {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "sombra-test-"));
    return path.join(dir, "db");
}

test("create and retrieve node", async (t) => {
    const db = Database.open(tempPath());

    const userId = db.createNode("User", { name: "Alice", age: 30 });
    const node = db.getNodeRecord(userId);

    t.truthy(node);
    t.deepEqual(node?.labels, ["User"]);
    t.is(node?.properties.name, "Alice");
    t.is(node?.properties.age, 30);

    db.close();
});

test("query with predicates", async (t) => {
    const db = Database.open(tempPath());

    db.createNode("User", { name: "Alice", age: 30 });
    db.createNode("User", { name: "Bob", age: 25 });

    const results = await db
        .query()
        .nodes("User")
        .where(eq("name", "Alice"))
        .execute();

    t.is(results.length, 1);

    db.close();
});

test("graph traversal", async (t) => {
    const db = Database.open(tempPath());

    const alice = db.createNode("User", { name: "Alice" });
    const bob = db.createNode("User", { name: "Bob" });
    db.createEdge(alice, bob, "FOLLOWS", {});

    const following = db.getOutgoingNeighbors(alice, "FOLLOWS");

    t.deepEqual(following, [bob]);

    db.close();
});

test("data persists after close", async (t) => {
    const dbPath = tempPath();

    // Create and close
    const db1 = Database.open(dbPath);
    db1.createNode("User", { name: "Alice" });
    db1.close();

    // Reopen and verify
    const db2 = Database.open(dbPath, { createIfMissing: false });
    const users = await db2.query().nodes("User").execute();
    t.is(users.length, 1);
    db2.close();
});
```

## Performance Tips

1. **Use the create builder for batch inserts**: Group related creates together

    ```typescript
    db.create()
      .node('User', { name: 'A' })
      .node('User', { name: 'B' })
      .edge(...)
      .execute()  // Single commit
    ```

2. **Use mutateBatched for large operations**: Chunk large imports

    ```typescript
    db.mutateBatched(thousandsOfOps, { batchSize: 1000 });
    ```

3. **Stream large result sets**: Avoid loading everything into memory

    ```typescript
    for await (const row of db.query().nodes("User").stream()) {
        process.row(row);
    }
    ```

4. **Use distinct neighbors**: Deduplicate when traversing multi-edges

    ```typescript
    db.getOutgoingNeighbors(id, "FOLLOWS", true); // distinct=true
    ```

5. **Close databases when done**: Release resources explicitly

    ```typescript
    db.close();
    ```

6. **Use request IDs for cancellation**: Cancel long-running queries
    ```typescript
    db.query().requestId("my-query").execute();
    db.cancelRequest("my-query");
    ```

## Next Steps

- Read the [Getting Started Guide](getting-started.md) for basic concepts
- Check the [Configuration Guide](configuration.md) for tuning options
- Review the [Architecture Guide](architecture.md) for internals
- Browse the [examples](https://github.com/sombra-db/sombra/tree/main/bindings/node/examples) directory for complete applications
