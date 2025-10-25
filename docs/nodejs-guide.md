# Node.js Binding Guide

This guide covers using Sombra from Node.js and TypeScript, including installation, basic operations, and integration with the Node.js ecosystem.

## Installation

### From npm (Recommended)

```bash
npm install sombra
```

### From Source

```bash
git clone https://github.com/sombra-db/sombra
cd sombra
npm install
npm run build
npm pack
npm install ./sombra-*.tgz
```

## TypeScript Support

Sombra provides two APIs for TypeScript users:

1. **Standard API**: Direct bindings with basic TypeScript definitions
2. **Typed API** (Recommended): Type-safe wrapper with schema validation at compile time

### Standard API

```typescript
import { SombraDB } from 'sombradb';

const db = new SombraDB('example.db');
```

### Typed API (Type-Safe Schema)

For enhanced type safety with autocomplete and compile-time validation:

```typescript
import { createTypedDB } from 'sombradb/typed';

interface MyGraphSchema {
  nodes: {
    Person: {
      name: string;
      age: number;
    };
    Company: {
      name: string;
      employees: number;
    };
  };
  edges: {
    WORKS_AT: {
      from: 'Person';
      to: 'Company';
      properties: {
        role: string;
        since: number;
      };
    };
  };
}

const db = createTypedDB<MyGraphSchema>('./example.db');

const person = db.addNode('Person', { name: 'Alice', age: 30 });
const company = db.addNode('Company', { name: 'ACME', employees: 100 });
db.addEdge(person, company, 'WORKS_AT', { role: 'Engineer', since: 2020 });

const found = db.findNodeByProperty('Company', 'name', 'ACME');
const node = db.getNode(found!);
console.log(node?.properties.employees);
```

Benefits of the Typed API:
- **Autocomplete**: Full IntelliSense for labels, edge types, and properties
- **Type Safety**: Compile-time validation of property types
- **Automatic Conversion**: No manual property wrapper creation needed
- **Cleaner Code**: Works with plain JavaScript objects

## Quick Start

### TypeScript

```typescript
import { SombraDB } from 'sombra';

const db = new SombraDB('example.db');
const tx = db.beginTransaction();

const alice = tx.addNode(['Person'], {
    name: 'Alice',
    age: 30
});

const bob = tx.addNode(['Person'], {
    name: 'Bob',
    age: 25
});

tx.addEdge(alice, bob, 'KNOWS', {
    since: 2020
});

tx.commit();
```

### JavaScript

```javascript
const { SombraDB } = require('sombra');

const db = new SombraDB('example.db');
const tx = db.beginTransaction();

const alice = tx.addNode(['Person'], {
    name: 'Alice',
    age: 30
});

const bob = tx.addNode(['Person'], {
    name: 'Bob',
    age: 25
});

tx.addEdge(alice, bob, 'KNOWS', {
    since: 2020
});

tx.commit();
```

## Basic Operations

### Working with Nodes

```typescript
import { SombraDB } from 'sombra';

const db = new SombraDB('social.db');
const tx = db.beginTransaction();

const userId = tx.addNode(['User'], {
    username: 'john_doe',
    email: 'john@example.com',
    active: true,
    score: 95.5
});

const node = tx.getNode(userId);
console.log(`Node ${node.id}: ${node.labels}`);
console.log(`Properties:`, node.properties);

tx.setNodeProperty(userId, 'score', 98.2);

tx.removeNodeProperty(userId, 'score');

const users = tx.getNodesByLabel('User');
console.log(`Found ${users.length} users`);

const rangeNodes = tx.getNodesInRange(1, 100);
console.log(`Nodes in range: ${rangeNodes.length}`);

const firstNode = tx.getFirstNode();
const lastNode = tx.getLastNode();
const firstTen = tx.getFirstNNodes(10);
const lastTen = tx.getLastNNodes(10);

tx.commit();
```

### Working with Edges

```typescript
const tx = db.beginTransaction();

const edgeId = tx.addEdge(alice, bob, 'FOLLOWS', {
    since: 2021,
    strength: 0.8
});

const edge = tx.getEdge(edgeId);
console.log(`Edge ${edge.id}: ${edge.sourceNodeId} -> ${edge.targetNodeId}`);
console.log(`Label: ${edge.label}`);
console.log(`Properties:`, edge.properties);

const outgoing = tx.getOutgoingEdges(alice);
const incoming = tx.getIncomingEdges(bob);

console.log(`Alice follows ${outgoing.length} users`);
console.log(`Bob has ${incoming.length} followers`);

const outCount = tx.countOutgoingEdges(alice);
const inCount = tx.countIncomingEdges(bob);

tx.deleteEdge(edgeId);
tx.commit();
```

### Graph Traversal

```typescript
const neighbors = tx.getNeighbors(userId);
console.log(`Direct neighbors: ${neighbors.length}`);

const incoming = tx.getIncomingNeighbors(userId);
console.log(`Incoming neighbors: ${incoming.length}`);

const twoHops = tx.getNeighborsTwoHops(userId);
console.log(`Two-hop neighbors: ${twoHops.length}`);

const threeHops = tx.getNeighborsThreeHops(userId);
console.log(`Three-hop neighbors: ${threeHops.length}`);

const bfsResult = tx.bfsTraversal(userId, ['KNOWS', 'FOLLOWS'], 'Outgoing', 3);
console.log(`BFS found ${bfsResult.length} nodes`);
```

## Query Builder API

Sombra provides a powerful query builder for complex graph queries:

```typescript
const query = db.query()
    .startFrom([1, 2, 3])
    .traverse(['KNOWS'], 'Outgoing', 2)
    .limit(10)
    .execute();

console.log(`Found ${query.nodeIds.length} nodes`);

const labelQuery = db.query()
    .startFromLabel('User')
    .traverse(['FOLLOWS'], 'Outgoing', 1)
    .execute();

const propQuery = db.query()
    .startFromProperty('User', 'username', 'alice')
    .traverse(['KNOWS', 'FOLLOWS'], 'Both', 2)
    .limit(50)
    .execute();
```

## Pattern Matching

Match complex graph patterns:

```typescript
const pattern = {
    nodes: [
        { label: 'Person', alias: 'p1' },
        { label: 'Person', alias: 'p2' }
    ],
    edges: [
        {
            sourceAlias: 'p1',
            targetAlias: 'p2',
            label: 'KNOWS'
        }
    ]
};

const matches = db.matchPattern(pattern);
console.log(`Found ${matches.length} matches`);

for (const match of matches) {
    console.log(`Match:`, match.nodeBindings);
}
```

## Path Finding

```typescript
const path = db.shortestPath(sourceId, targetId, ['KNOWS', 'FOLLOWS']);
if (path) {
    console.log(`Shortest path: ${path.nodeIds.join(' -> ')}`);
    console.log(`Path length: ${path.length}`);
}

const allPaths = db.findPaths(
    sourceId,
    targetId,
    ['KNOWS'],
    5,
    10
);
console.log(`Found ${allPaths.length} paths`);
```

## Analytics

```typescript
const distrib = db.degreeDistribution();
console.log(`Avg in-degree: ${distrib.avgInDegree}`);
console.log(`Avg out-degree: ${distrib.avgOutDegree}`);
console.log(`Max in-degree: ${distrib.maxInDegree}`);
console.log(`Max out-degree: ${distrib.maxOutDegree}`);

const hubs = db.findHubs(10, 'Out');
console.log(`Found ${hubs.length} hub nodes`);

const isolated = db.findIsolatedNodes();
console.log(`Found ${isolated.length} isolated nodes`);

const leaves = db.findLeafNodes('Out');
console.log(`Found ${leaves.length} leaf nodes`);

const avgDegree = db.getAverageDegree();
const density = db.getDensity();
console.log(`Average degree: ${avgDegree}`);
console.log(`Graph density: ${density}`);
```

## Statistics

```typescript
const labelCounts = db.countNodesByLabel();
for (const [label, count] of Object.entries(labelCounts)) {
    console.log(`${label}: ${count} nodes`);
}

const edgeCounts = db.countEdgesByType();
for (const [type, count] of Object.entries(edgeCounts)) {
    console.log(`${type}: ${count} edges`);
}

const totalNodes = db.getTotalNodeCount();
const totalEdges = db.getTotalEdgeCount();

const userCount = db.countNodesWithLabel('User');
const followsCount = db.countEdgesWithType('FOLLOWS');
```

## Subgraph Extraction

```typescript
const subgraph = db.extractSubgraph([1, 2, 3, 4, 5], ['KNOWS', 'FOLLOWS']);
console.log(`Subgraph nodes: ${subgraph.nodeIds.length}`);
console.log(`Subgraph edges: ${subgraph.edgeIds.length}`);

const induced = db.extractInducedSubgraph([1, 2, 3, 4, 5]);
console.log(`Induced subgraph edges: ${induced.edgeIds.length}`);
```

## Hierarchical Queries

```typescript
const ancestor = db.findAncestorByLabel(nodeId, 'File', 'HAS_CHILD');
if (ancestor) {
    console.log(`Found ancestor file: ${ancestor}`);
}

const ancestors = db.getAncestors(nodeId, ['HAS_CHILD'], 10);
console.log(`Found ${ancestors.length} ancestors`);

const descendants = db.getDescendants(nodeId, ['HAS_CHILD'], 5);
console.log(`Found ${descendants.length} descendants`);

const fileId = db.getContainingFile(nodeId);
console.log(`Containing file: ${fileId}`);
```

## Transactions

```typescript
const tx = db.beginTransaction();

try {
    const user1 = tx.addNode(['User'], { name: 'Alice' });
    const user2 = tx.addNode(['User'], { name: 'Bob' });
    tx.addEdge(user1, user2, 'KNOWS', {});
    
    tx.commit();
} catch (error) {
    tx.rollback();
    throw error;
}
```

## Integration with Express.js

```typescript
import express from 'express';
import { SombraDB } from 'sombra';

const app = express();
app.use(express.json());

const db = new SombraDB('social_network.db');

app.post('/users', (req, res) => {
    try {
        const { name, email } = req.body;
        const tx = db.beginTransaction();
        const userId = tx.addNode(['User'], { name, email });
        tx.commit();
        res.status(201).json({ id: userId, name, email });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/users/:id', (req, res) => {
    try {
        const userId = Number(req.params.id);
        const tx = db.beginTransaction();
        const user = tx.getNode(userId);
        tx.commit();
        res.json({ id: user.id, labels: user.labels, properties: user.properties });
    } catch (error) {
        res.status(404).json({ error: 'User not found' });
    }
});

app.get('/users/:id/friends', (req, res) => {
    try {
        const userId = Number(req.params.id);
        const tx = db.beginTransaction();
        const friendIds = tx.bfsTraversal(userId, ['FRIENDS'], 'Outgoing', 1);
        const friends = friendIds.map(id => tx.getNode(id));
        tx.commit();
        res.json(friends);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.listen(3000, () => {
    console.log('Server running on port 3000');
});
```

## Batch Operations

```typescript
function bulkInsertUsers(userData: Array<{name: string, email: string}>) {
    const batchSize = 1000;
    const results: number[] = [];
    
    for (let i = 0; i < userData.length; i += batchSize) {
        const batch = userData.slice(i, i + batchSize);
        const tx = db.beginTransaction();
        
        try {
            for (const data of batch) {
                const id = tx.addNode(['User'], data);
                results.push(id);
            }
            tx.commit();
        } catch (error) {
            tx.rollback();
            throw error;
        }
    }
    
    return results;
}
```

## Testing with Jest

```typescript
import { SombraDB } from 'sombra';
import { existsSync, unlinkSync } from 'fs';

describe('Sombra Operations', () => {
    const testDbPath = 'test.db';
    let db: SombraDB;
    
    beforeEach(() => {
        if (existsSync(testDbPath)) {
            unlinkSync(testDbPath);
        }
        db = new SombraDB(testDbPath);
    });
    
    afterEach(() => {
        if (existsSync(testDbPath)) {
            unlinkSync(testDbPath);
        }
    });
    
    test('create and retrieve node', () => {
        const tx = db.beginTransaction();
        const userId = tx.addNode(['User'], { name: 'Alice' });
        tx.commit();
        
        const tx2 = db.beginTransaction();
        const node = tx2.getNode(userId);
        tx2.commit();
        
        expect(node.labels).toContain('User');
        expect(node.properties.name).toBe('Alice');
    });
    
    test('graph traversal', () => {
        const tx = db.beginTransaction();
        const alice = tx.addNode(['User'], { name: 'Alice' });
        const bob = tx.addNode(['User'], { name: 'Bob' });
        tx.addEdge(alice, bob, 'KNOWS', {});
        tx.commit();
        
        const tx2 = db.beginTransaction();
        const neighbors = tx2.getNeighbors(alice);
        tx2.commit();
        
        expect(neighbors).toHaveLength(1);
        expect(neighbors[0]).toBe(bob);
    });
});
```

## Performance Tips

1. **Use transactions for batches**: Group multiple operations in a single transaction
2. **Minimize transaction scope**: Keep transactions short-lived
3. **Use range queries**: For sequential access, use `getNodesInRange()` instead of individual `getNode()` calls
4. **Leverage BFS**: Use `bfsTraversal()` for efficient graph traversal
5. **Use query builder**: For complex queries, use the query builder API
6. **Checkpoint regularly**: Call `db.checkpoint()` periodically for long-running operations

## Database Maintenance

```typescript
db.flush();

db.checkpoint();
```

## Next Steps

- Read the [Getting Started Guide](getting-started.md) for basic concepts
- Check the [Configuration Guide](configuration.md) for tuning options
- Review the [Operations Guide](operations.md) for production deployment
- Browse the [examples](../examples/) directory for complete applications
