# Node.js / TypeScript Usage

Sombra provides native Node.js bindings through NAPI-RS, offering high-performance graph database operations from JavaScript and TypeScript.

## Installation

```bash
npm install sombra
```

## Basic Usage

```javascript
const { SombraDB } = require('sombra');

const db = new SombraDB('./my-graph.db');

const alice = db.addNode(['Person'], {
  name: { type: 'string', value: 'Alice' },
  age: { type: 'int', value: 30 }
});

const bob = db.addNode(['Person'], {
  name: { type: 'string', value: 'Bob' },
  age: { type: 'int', value: 25 }
});

const edge = db.addEdge(alice, bob, 'KNOWS', {
  since: { type: 'int', value: 2020 }
});

console.log(db.getNode(alice));
console.log(db.getEdge(edge));
```

## TypeScript Support

Full TypeScript definitions are included:

```typescript
import { SombraDB, SombraPropertyValue, SombraNode, SombraEdge } from 'sombra';

const db = new SombraDB('./my-graph.db');

const node: SombraNode = db.getNode(1);
const edge: SombraEdge = db.getEdge(1);
```

## Property Types

Sombra supports multiple property types:

```javascript
db.addNode(['Example'], {
  text: { type: 'string', value: 'Hello World' },
  count: { type: 'int', value: 42 },
  score: { type: 'float', value: 3.14 },
  active: { type: 'bool', value: true },
  data: { type: 'bytes', value: Buffer.from([1, 2, 3]) }
});
```

## Working with Edges

### Adding Edges

```javascript
const edge = db.addEdge(sourceNodeId, targetNodeId, 'RELATIONSHIP_TYPE', {
  weight: { type: 'float', value: 0.85 }
});
```

### Querying Edges

```javascript
const edge = db.getEdge(edgeId);
console.log(edge.sourceNodeId);
console.log(edge.targetNodeId);
console.log(edge.typeName);
console.log(edge.properties);

const outgoing = db.getOutgoingEdges(nodeId);

const incoming = db.getIncomingEdges(nodeId);

const neighbors = db.getNeighbors(nodeId);
```

## Transactions

Sombra provides ACID transactions:

```javascript
const tx = db.beginTransaction();

try {
  const node = tx.addNode(['Person'], {
    name: { type: 'string', value: 'Charlie' }
  });
  
  tx.addEdge(alice, node, 'KNOWS');
  
  tx.commit();
} catch (error) {
  tx.rollback();
  throw error;
}
```

## Persistence

```javascript
db.flush();

db.checkpoint();
```

## Deleting Data

```javascript
db.deleteNode(nodeId);

db.deleteEdge(edgeId);
```

## API Reference

### SombraDB

#### Constructor
- `new SombraDB(path: string)` - Opens or creates a database at the specified path

#### Node Operations
- `addNode(labels: string[], properties?: Record<string, SombraPropertyValue>): number` - Adds a node and returns its ID
- `getNode(nodeId: number): SombraNode` - Retrieves a node by ID
- `deleteNode(nodeId: number): void` - Deletes a node

#### Edge Operations
- `addEdge(sourceNodeId: number, targetNodeId: number, label: string, properties?: Record<string, SombraPropertyValue>): number` - Adds an edge and returns its ID
- `getEdge(edgeId: number): SombraEdge` - Retrieves an edge by ID
- `getOutgoingEdges(nodeId: number): number[]` - Gets IDs of outgoing edges from a node
- `getIncomingEdges(nodeId: number): number[]` - Gets IDs of incoming edges to a node
- `getNeighbors(nodeId: number): number[]` - Gets IDs of neighboring nodes
- `deleteEdge(edgeId: number): void` - Deletes an edge

#### Transaction Operations
- `beginTransaction(): SombraTransaction` - Starts a new transaction

#### Persistence Operations
- `flush(): void` - Flushes in-memory changes to disk
- `checkpoint(): void` - Creates a checkpoint

### SombraTransaction

#### Methods
- `id(): number` - Returns the transaction ID
- `addNode(labels: string[], properties?: Record<string, SombraPropertyValue>): number` - Adds a node in the transaction
- `addEdge(sourceNodeId: number, targetNodeId: number, label: string, properties?: Record<string, SombraPropertyValue>): number` - Adds an edge in the transaction
- `getNode(nodeId: number): SombraNode` - Gets a node (sees transaction changes)
- `getEdge(edgeId: number): SombraEdge` - Gets an edge (sees transaction changes)
- `getOutgoingEdges(nodeId: number): number[]` - Gets outgoing edges (sees transaction changes)
- `getIncomingEdges(nodeId: number): number[]` - Gets incoming edges (sees transaction changes)
- `getNeighbors(nodeId: number): number[]` - Gets neighbors (sees transaction changes)
- `deleteNode(nodeId: number): void` - Deletes a node in the transaction
- `deleteEdge(edgeId: number): void` - Deletes an edge in the transaction
- `commit(): void` - Commits the transaction
- `rollback(): void` - Rolls back the transaction

### Types

#### SombraPropertyValue
```typescript
interface SombraPropertyValue {
  type: 'string' | 'int' | 'float' | 'bool' | 'bytes';
  value: any;
}
```

#### SombraNode
```typescript
interface SombraNode {
  id: number;
  labels: string[];
  properties: Record<string, SombraPropertyValue>;
}
```

#### SombraEdge
```typescript
interface SombraEdge {
  id: number;
  sourceNodeId: number;
  targetNodeId: number;
  typeName: string;
  properties: Record<string, SombraPropertyValue>;
}
```

## Error Handling

All methods that can fail will throw errors with descriptive messages:

```javascript
try {
  const node = db.getNode(999);
} catch (error) {
  console.error('Failed to get node:', error.message);
}
```

## Platform Support

Sombra provides prebuilt binaries for:
- Windows (x64, ia32, arm64)
- macOS (x64, arm64/Apple Silicon)
- Linux (x64, arm64, armv7 - glibc and musl)

## Performance Tips

1. Use transactions for batch operations
2. Call `flush()` periodically for long-running applications
3. Use `checkpoint()` before shutting down
4. Reuse the SombraDB instance across your application
