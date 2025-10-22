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

## Hierarchy Traversal

Sombra provides powerful methods for traversing hierarchical relationships in your graph. These are particularly useful for code analysis, organizational charts, file systems, and any tree-like structures.

### Find Ancestor by Label

Find the nearest ancestor with a specific label:

```javascript
const fileNode = db.findAncestorByLabel(statementNode, 'File', 'PARENT');

const functionNode = db.findAncestorByLabel(blockNode, 'Function', 'PARENT');

if (fileNode === null) {
  console.log('No ancestor with label "File" found');
}
```

### Get All Ancestors

Retrieve all ancestors up to a specified depth:

```javascript
const allAncestors = db.getAncestors(nodeId, 'PARENT');

const limitedAncestors = db.getAncestors(nodeId, 'PARENT', 3);
```

### Get All Descendants

Retrieve all descendants up to a specified depth:

```javascript
const allDescendants = db.getDescendants(nodeId, 'PARENT');

const limitedDescendants = db.getDescendants(nodeId, 'PARENT', 2);
```

### Get Containing File

Convenience method to find the File node containing a given node:

```javascript
const fileId = db.getContainingFile(statementId);
console.log('Statement is in file:', db.getNode(fileId));
```

### Example: Code Analysis Hierarchy

```javascript
const file = db.addNode(['File'], { name: { type: 'string', value: 'main.js' } });
const func = db.addNode(['Function'], { name: { type: 'string', value: 'processData' } });
const block = db.addNode(['Block'], { name: { type: 'string', value: 'if-block' } });
const stmt = db.addNode(['Statement'], { name: { type: 'string', value: 'return' } });

db.addEdge(func, file, 'PARENT');
db.addEdge(block, func, 'PARENT');
db.addEdge(stmt, block, 'PARENT');

const ancestors = db.getAncestors(stmt, 'PARENT');

const functionNode = db.findAncestorByLabel(stmt, 'Function', 'PARENT');
const fileNode = db.getContainingFile(stmt);
```

## Pattern Matching

Sombra provides a declarative pattern matching API for querying graph structures. This is ideal for code analysis (finding call patterns, import chains), dependency tracking, and complex relationship queries.

### Basic Pattern Matching

Find nodes and edges matching a specific pattern:

```javascript
const pattern = {
  nodes: [
    {
      varName: 'call',
      labels: ['CallExpr'],
      properties: {
        equals: { callee: { type: 'string', value: 'foo' } },
        notEquals: {},
        ranges: []
      }
    },
    {
      varName: 'func',
      labels: ['Function'],
      properties: {
        equals: { name: { type: 'string', value: 'foo' } },
        notEquals: {},
        ranges: []
      }
    }
  ],
  edges: [
    {
      fromVar: 'call',
      toVar: 'func',
      types: ['CALLS'],
      properties: { equals: {}, notEquals: {}, ranges: [] },
      direction: 'outgoing'
    }
  ]
};

const matches = db.matchPattern(pattern);
matches.forEach(match => {
  console.log('Call node:', match.nodeBindings.call);
  console.log('Function node:', match.nodeBindings.func);
  console.log('Edge IDs:', match.edgeIds);
});
```

### Property Filters

Pattern matching supports three types of property filters:

#### Equals Filter
```javascript
properties: {
  equals: { 
    status: { type: 'string', value: 'active' },
    count: { type: 'int', value: 5 }
  },
  notEquals: {},
  ranges: []
}
```

#### Not-Equals Filter
```javascript
properties: {
  equals: {},
  notEquals: { 
    visibility: { type: 'string', value: 'private' }
  },
  ranges: []
}
```

#### Range Filter
```javascript
properties: {
  equals: {},
  notEquals: {},
  ranges: [
    {
      propertyName: 'age',
      start: { bound: 'inclusive', value: { type: 'int', value: 30 } },
      end: { bound: 'inclusive', value: { type: 'int', value: 40 } }
    }
  ]
}
```

Bounds can be `'inclusive'` or `'exclusive'`.

### Edge Directions

Edges can match in three directions:

```javascript
{ direction: 'outgoing' }

{ direction: 'incoming' }

{ direction: 'both' }
```

### Multi-Hop Patterns

Match paths through multiple nodes:

```javascript
const pattern = {
  nodes: [
    {
      varName: 'a',
      labels: ['Person'],
      properties: {
        equals: { name: { type: 'string', value: 'Alice' } },
        notEquals: {},
        ranges: []
      }
    },
    {
      varName: 'b',
      labels: ['Person'],
      properties: { equals: {}, notEquals: {}, ranges: [] }
    },
    {
      varName: 'c',
      labels: ['Person'],
      properties: {
        equals: { name: { type: 'string', value: 'Charlie' } },
        notEquals: {},
        ranges: []
      }
    }
  ],
  edges: [
    {
      fromVar: 'a',
      toVar: 'b',
      types: ['KNOWS'],
      properties: { equals: {}, notEquals: {}, ranges: [] },
      direction: 'outgoing'
    },
    {
      fromVar: 'b',
      toVar: 'c',
      types: ['KNOWS'],
      properties: { equals: {}, notEquals: {}, ranges: [] },
      direction: 'outgoing'
    }
  ]
};

const matches = db.matchPattern(pattern);
```

### Example: Code Analysis Queries

Find all function calls to a specific function:

```javascript
const callPattern = {
  nodes: [
    { varName: 'call', labels: ['CallExpr'], properties: { equals: {}, notEquals: {}, ranges: [] } },
    { varName: 'func', labels: ['Function'], properties: { equals: { name: { type: 'string', value: 'dangerousAPI' } }, notEquals: {}, ranges: [] } }
  ],
  edges: [
    { fromVar: 'call', toVar: 'func', types: ['CALLS'], properties: { equals: {}, notEquals: {}, ranges: [] }, direction: 'outgoing' }
  ]
};

const dangerousCalls = db.matchPattern(callPattern);
console.log(`Found ${dangerousCalls.length} calls to dangerousAPI`);
```

Find import chains:

```javascript
const importChainPattern = {
  nodes: [
    { varName: 'file1', labels: ['File'], properties: { equals: {}, notEquals: {}, ranges: [] } },
    { varName: 'file2', labels: ['File'], properties: { equals: {}, notEquals: {}, ranges: [] } },
    { varName: 'file3', labels: ['File'], properties: { equals: {}, notEquals: {}, ranges: [] } }
  ],
  edges: [
    { fromVar: 'file1', toVar: 'file2', types: ['IMPORTS'], properties: { equals: {}, notEquals: {}, ranges: [] }, direction: 'outgoing' },
    { fromVar: 'file2', toVar: 'file3', types: ['IMPORTS'], properties: { equals: {}, notEquals: {}, ranges: [] }, direction: 'outgoing' }
  ]
};

const importChains = db.matchPattern(importChainPattern);
```

### Performance Considerations

- Specify labels and property filters to reduce the search space
- More selective patterns (with specific property values) will be faster
- The algorithm has O(P * (V + E)) worst-case complexity where P is pattern size, V is nodes, E is edges
- Use property indexes for frequently queried properties to improve performance

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

#### Hierarchy Operations
- `findAncestorByLabel(nodeId: number, label: string, edgeType: string): number | null` - Finds nearest ancestor with the specified label
- `getAncestors(nodeId: number, edgeType: string, maxDepth?: number): number[]` - Gets all ancestors up to max depth (unlimited if not specified)
- `getDescendants(nodeId: number, edgeType: string, maxDepth?: number): number[]` - Gets all descendants up to max depth (unlimited if not specified)
- `getContainingFile(nodeId: number): number` - Finds the File node containing the given node (convenience method)

#### Pattern Matching Operations
- `matchPattern(pattern: Pattern): Match[]` - Finds all matches of the specified pattern in the graph

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

#### Pattern Matching Types

```typescript
interface PropertyBound {
  bound: 'inclusive' | 'exclusive';
  value: SombraPropertyValue;
}

interface PropertyRangeFilter {
  propertyName: string;
  start: PropertyBound;
  end: PropertyBound;
}

interface PropertyFilters {
  equals: Record<string, SombraPropertyValue>;
  notEquals: Record<string, SombraPropertyValue>;
  ranges: PropertyRangeFilter[];
}

interface NodePattern {
  varName: string;
  labels: string[];
  properties: PropertyFilters;
}

interface EdgePattern {
  fromVar: string;
  toVar: string;
  types: string[];
  properties: PropertyFilters;
  direction: 'outgoing' | 'incoming' | 'both';
}

interface Pattern {
  nodes: NodePattern[];
  edges: EdgePattern[];
}

interface Match {
  nodeBindings: Record<string, number>;
  edgeIds: number[];
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
