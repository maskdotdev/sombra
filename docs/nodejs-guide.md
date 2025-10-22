# Node.js Binding Guide

This guide covers using Sombra from Node.js and TypeScript, including installation, basic operations, async patterns, and integration with the Node.js ecosystem.

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

### Development Installation

```bash
git clone https://github.com/sombra-db/sombra
cd sombra
npm install
npm run build
npm link
```

## TypeScript Support

Sombra provides full TypeScript definitions out of the box:

```typescript
import { GraphDB, Node, Edge, Transaction, PropertyValue } from 'sombra';

// Full type safety and IDE support
const db: GraphDB = new GraphDB('example.db');
```

## Quick Start

### TypeScript

```typescript
import { GraphDB, PropertyValue } from 'sombra';

async function main() {
    // Open or create a database
    const db = new GraphDB('example.db');
    
    try {
        // Start a transaction
        const tx = db.beginTransaction();
        
        // Create nodes
        const alice = await tx.createNode('Person', {
            name: 'Alice',
            age: 30,
            email: 'alice@example.com'
        });
        
        const bob = await tx.createNode('Person', {
            name: 'Bob',
            age: 25,
            email: 'bob@example.com'
        });
        
        // Create an edge
        await tx.createEdge(alice, bob, 'KNOWS', {
            since: 2020,
            relationship: 'friend'
        });
        
        // Commit the transaction
        await tx.commit();
        
        console.log('Graph created successfully!');
    } catch (error) {
        console.error('Error:', error);
    }
}

main().catch(console.error);
```

### JavaScript

```javascript
const { GraphDB } = require('sombra');

async function main() {
    // Open or create a database
    const db = new GraphDB('example.db');
    
    try {
        // Start a transaction
        const tx = db.beginTransaction();
        
        // Create nodes
        const alice = await tx.createNode('Person', {
            name: 'Alice',
            age: 30,
            email: 'alice@example.com'
        });
        
        const bob = await tx.createNode('Person', {
            name: 'Bob',
            age: 25,
            email: 'bob@example.com'
        });
        
        // Create an edge
        await tx.createEdge(alice, bob, 'KNOWS', {
            since: 2020,
            relationship: 'friend'
        });
        
        // Commit the transaction
        await tx.commit();
        
        console.log('Graph created successfully!');
    } catch (error) {
        console.error('Error:', error);
    }
}

main().catch(console.error);
```

## Basic Operations

### Working with Nodes

```typescript
import { GraphDB, PropertyValue } from 'sombra';

const db = new GraphDB('social.db');
const tx = db.beginTransaction();

// Create a node with various property types
const user = await tx.createNode('User', {
    username: 'john_doe',
    email: 'john@example.com',
    active: true,
    created_at: new Date('2024-01-15'),
    score: 95.5,
    tags: ['developer', 'javascript', 'typescript']
});

// Get node by ID
const node = await tx.getNode(user.id);
console.log(`Node ${node.id}: ${node.label}`);

// Get node properties
const props = await tx.getNodeProperties(user.id);
console.log(`Name: ${props.name}`);
console.log(`Age: ${props.age}`);
console.log(`Created: ${props.created_at}`);

// Update node properties
await tx.updateNodeProperties(user.id, {
    last_login: new Date(),
    login_count: 5,
    score: 98.2
});

// Find nodes by label
const users = await tx.findNodesByLabel('User');
console.log(`Found ${users.length} users`);

// Find nodes by property
const john = await tx.findNodesByProperty('User', 'username', 'john_doe');
if (john.length > 0) {
    console.log(`Found user: ${john[0].id}`);
}

// Find nodes by property range
const youngUsers = await tx.findNodesByPropertyRange('User', 'age', 18, 25);
console.log(`Found ${youngUsers.length} young users`);

// Find nodes by multiple properties
const engineers = await tx.findNodesByProperties('User', {
    department: 'Engineering',
    level: ['Senior', 'Principal'],
    active: true
});
```

### Working with Edges

```typescript
// Create an edge
const followEdge = await tx.createEdge(alice, bob, 'FOLLOWS', {
    since: new Date('2021-01-01'),
    strength: 0.8,
    interactions: 42
});

// Get edges from a node
const outgoing = await tx.getOutgoingEdges(alice.id);
const incoming = await tx.getIncomingEdges(bob.id);

console.log(`Alice follows ${outgoing.length} users`);
console.log(`Bob has ${incoming.length} followers`);

// Get edge properties
const edgeProps = await tx.getEdgeProperties(followEdge.id);
console.log(`Follow strength: ${edgeProps.strength}`);
console.log(`Interactions: ${edgeProps.interactions}`);

// Update edge properties
await tx.updateEdgeProperties(followEdge.id, {
    strength: 0.9,
    last_interaction: new Date(),
    interactions: 50
});

// Delete an edge
await tx.deleteEdge(followEdge.id);
```

### Graph Traversal

```typescript
// Basic traversal
const friends = await tx.traverse()
    .fromNode(alice.id)
    .outgoing('KNOWS')
    .collect();

console.log(`Alice's friends: ${friends.map(f => f.id)}`);

// Multi-hop traversal
const friendsOfFriends = await tx.traverse()
    .fromNode(alice.id)
    .outgoing('KNOWS')
    .outgoing('KNOWS')
    .collect();

console.log(`Friends of friends: ${friendsOfFriends.map(f => f.id)}`);

// Traversal with filters
const activeFriends = await tx.traverse()
    .fromNode(alice.id)
    .outgoing('KNOWS')
    .filter(async (node) => {
        const props = await tx.getNodeProperties(node.id);
        return props.active === true;
    })
    .collect();

console.log(`Active friends: ${activeFriends.map(f => f.id)}`);

// Bidirectional traversal
const mutualFriends = await tx.traverse()
    .fromNode(alice.id)
    .outgoing('KNOWS')
    .incoming('KNOWS')
    .filter(async (node) => node.id === bob.id)
    .collect();

// Traversal with property access
const skilledFriends = await tx.traverse()
    .fromNode(alice.id)
    .outgoing('KNOWS')
    .filter(async (node) => {
        const props = await tx.getNodeProperties(node.id);
        return props.skills && props.skills.includes('typescript');
    })
    .collect();
```

## Advanced Patterns

### Async/Await Patterns

```typescript
// Sequential operations
async function createUserWithFriends(userName: string, friendNames: string[]) {
    const tx = db.beginTransaction();
    
    try {
        const user = await tx.createNode('User', { name: userName });
        
        for (const friendName of friendNames) {
            const friend = await tx.createNode('User', { name: friendName });
            await tx.createEdge(user, friend, 'FRIENDS', {});
        }
        
        await tx.commit();
        return user;
    } catch (error) {
        await tx.rollback();
        throw error;
    }
}

// Parallel operations
async function createManyUsers(userData: Array<{name: string, email: string}>) {
    const tx = db.beginTransaction();
    
    try {
        // Create all users in parallel
        const userPromises = userData.map(data => 
            tx.createNode('User', data)
        );
        const users = await Promise.all(userPromises);
        
        await tx.commit();
        return users;
    } catch (error) {
        await tx.rollback();
        throw error;
    }
}
```

### Error Handling

```typescript
import { GraphError, NodeNotFoundError, TransactionError } from 'sombra';

async function safeOperation() {
    try {
        const db = new GraphDB('/invalid/path/db.db');
        // ... operations
    } catch (error) {
        if (error instanceof GraphError) {
            console.error(`Database error: ${error.message}`);
            console.error(`Error code: ${error.code}`);
        } else {
            console.error(`Unexpected error: ${error}`);
        }
    }
}

async function transactionWithRetry<T>(
    operation: (tx: Transaction) => Promise<T>,
    maxRetries: number = 3
): Promise<T> {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        const tx = db.beginTransaction();
        
        try {
            const result = await operation(tx);
            await tx.commit();
            return result;
        } catch (error) {
            await tx.rollback();
            
            if (attempt === maxRetries) {
                throw error;
            }
            
            // Exponential backoff
            const delay = Math.pow(2, attempt) * 100;
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
    
    throw new Error('Max retries exceeded');
}

// Usage
const result = await transactionWithRetry(async (tx) => {
    const user = await tx.createNode('User', { name: 'Alice' });
    return user;
});
```

### Connection Pooling

```typescript
import { GraphDB } from 'sombra';

class DatabasePool {
    private pools: GraphDB[] = [];
    private available: number[] = [];
    private inUse: Set<number> = new Set();
    
    constructor(private dbPath: string, private poolSize: number = 5) {
        this.initializePool();
    }
    
    private initializePool() {
        for (let i = 0; i < this.poolSize; i++) {
            this.pools.push(new GraphDB(this.dbPath));
            this.available.push(i);
        }
    }
    
    async getConnection(): Promise<{ db: GraphDB, release: () => void }> {
        return new Promise((resolve, reject) => {
            const checkAvailable = () => {
                if (this.available.length > 0) {
                    const index = this.available.pop()!;
                    this.inUse.add(index);
                    
                    resolve({
                        db: this.pools[index],
                        release: () => {
                            this.inUse.delete(index);
                            this.available.push(index);
                        }
                    });
                } else {
                    setTimeout(checkAvailable, 10);
                }
            };
            
            checkAvailable();
        });
    }
    
    async closeAll() {
        for (const db of this.pools) {
            await db.close();
        }
    }
}

// Usage
const pool = new DatabasePool('production.db', 10);

async function processUser(userData: any) {
    const { db, release } = await pool.getConnection();
    
    try {
        const tx = db.beginTransaction();
        const user = await tx.createNode('User', userData);
        await tx.commit();
        return user;
    } finally {
        release();
    }
}
```

### Batch Operations

```typescript
async function bulkInsertUsers(userData: Array<{name: string, email: string}>) {
    const batchSize = 1000;
    const results: Node[] = [];
    
    for (let i = 0; i < userData.length; i += batchSize) {
        const batch = userData.slice(i, i + batchSize);
        const tx = db.beginTransaction();
        
        try {
            const batchResults = await Promise.all(
                batch.map(data => tx.createNode('User', data))
            );
            
            await tx.commit();
            results.push(...batchResults);
        } catch (error) {
            await tx.rollback();
            throw error;
        }
    }
    
    return results;
}

// Usage
const users = [
    { name: 'User1', email: 'user1@example.com' },
    { name: 'User2', email: 'user2@example.com' },
    // ... more users
];

const insertedUsers = await bulkInsertUsers(users);
console.log(`Inserted ${insertedUsers.length} users`);
```

## Integration with Node.js Ecosystem

### Express.js Integration

```typescript
import express from 'express';
import { GraphDB } from 'sombra';

const app = express();
app.use(express.json());

const db = new GraphDB('social_network.db');

// Create user endpoint
app.post('/users', async (req, res) => {
    try {
        const { name, email, age } = req.body;
        
        const tx = db.beginTransaction();
        const user = await tx.createNode('User', { name, email, age });
        await tx.commit();
        
        res.status(201).json({ id: user.id, name, email, age });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Get user endpoint
app.get('/users/:id', async (req, res) => {
    try {
        const userId = parseInt(req.params.id);
        const tx = db.beginTransaction();
        const user = await tx.getNode(userId);
        const properties = await tx.getNodeProperties(userId);
        await tx.commit();
        
        res.json({ id: user.id, label: user.label, properties });
    } catch (error) {
        res.status(404).json({ error: 'User not found' });
    }
});

// Get friends endpoint
app.get('/users/:id/friends', async (req, res) => {
    try {
        const userId = parseInt(req.params.id);
        const tx = db.beginTransaction();
        
        const friends = await tx.traverse()
            .fromNode(userId)
            .outgoing('FRIENDS')
            .collect();
        
        const friendData = await Promise.all(
            friends.map(async (friend) => {
                const props = await tx.getNodeProperties(friend.id);
                return { id: friend.id, ...props };
            })
        );
        
        await tx.commit();
        res.json(friendData);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.listen(3000, () => {
    console.log('Server running on port 3000');
});
```

### Stream Processing

```typescript
import { Transform } from 'stream';
import { GraphDB } from 'sombra';

class UserImportStream extends Transform {
    private db: GraphDB;
    private tx: Transaction;
    private batchSize: number = 100;
    private currentBatch: any[] = [];
    
    constructor(dbPath: string) {
        super({ objectMode: true });
        this.db = new GraphDB(dbPath);
        this.tx = this.db.beginTransaction();
    }
    
    async _transform(chunk: any, encoding: string, callback: Function) {
        this.currentBatch.push(chunk);
        
        if (this.currentBatch.length >= this.batchSize) {
            await this.processBatch();
        }
        
        callback();
    }
    
    async _flush(callback: Function) {
        if (this.currentBatch.length > 0) {
            await this.processBatch();
        }
        
        await this.tx.commit();
        callback();
    }
    
    private async processBatch() {
        try {
            await Promise.all(
                this.currentBatch.map(userData => 
                    this.tx.createNode('User', userData)
                )
            );
            
            this.currentBatch = [];
        } catch (error) {
            await this.tx.rollback();
            throw error;
        }
    }
}

// Usage with CSV parsing
import fs from 'fs';
import csv from 'csv-parser';

const userStream = fs.createReadStream('users.csv')
    .pipe(csv())
    .pipe(new UserImportStream('users.db'));

userStream.on('finish', () => {
    console.log('User import completed');
});

userStream.on('error', (error) => {
    console.error('Import failed:', error);
});
```

### Concurrent Read Operations

Sombra supports multiple concurrent read operations using a multi-reader, single-writer concurrency model:

```typescript
import { GraphDB } from 'sombra';

const db = new GraphDB('concurrent.db');

async function concurrentReads() {
    const userId1 = 1;
    const userId2 = 2;
    const userId3 = 3;
    
    const [user1Friends, user2Friends, user3Friends] = await Promise.all([
        db.getOutgoingEdges(userId1),
        db.getOutgoingEdges(userId2),
        db.getOutgoingEdges(userId3)
    ]);
    
    console.log(`User 1 has ${user1Friends.length} friends`);
    console.log(`User 2 has ${user2Friends.length} friends`);
    console.log(`User 3 has ${user3Friends.length} friends`);
}

async function parallelTraversals() {
    const userIds = [1, 2, 3, 4, 5];
    
    const allFriends = await Promise.all(
        userIds.map(userId => 
            db.rangeQuery(userId, userId, 'KNOWS')
        )
    );
    
    allFriends.forEach((friends, idx) => {
        console.log(`User ${userIds[idx]} friends: ${friends.length}`);
    });
}

async function concurrentPropertyReads() {
    const nodeIds = [1, 2, 3, 4, 5, 6, 7, 8];
    
    const properties = await Promise.all(
        nodeIds.map(id => db.getNodeProperties(id))
    );
    
    console.log(`Fetched ${properties.length} node properties concurrently`);
}

concurrentReads();
parallelTraversals();
concurrentPropertyReads();
```

### Worker Threads

```typescript
import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import { GraphDB } from 'sombra';

interface WorkerTask {
    type: 'process_users';
    userData: Array<{name: string, email: string}>;
    dbPath: string;
}

if (!isMainThread) {
    // Worker thread code
    const processUsers = async (task: WorkerTask) => {
        const db = new GraphDB(task.dbPath);
        const tx = db.beginTransaction();
        
        try {
            const results = await Promise.all(
                task.userData.map(data => tx.createNode('User', data))
            );
            
            await tx.commit();
            
            parentPort?.postMessage({
                success: true,
                count: results.length
            });
        } catch (error) {
            await tx.rollback();
            parentPort?.postMessage({
                success: false,
                error: error.message
            });
        }
    };
    
    parentPort?.on('message', (task: WorkerTask) => {
        processUsers(task);
    });
} else {
    // Main thread code
    async function processWithWorkers(userData: Array<{name: string, email: string}>) {
        const numWorkers = 4;
        const chunkSize = Math.ceil(userData.length / numWorkers);
        const workers: Worker[] = [];
        
        for (let i = 0; i < numWorkers; i++) {
            const chunk = userData.slice(i * chunkSize, (i + 1) * chunkSize);
            
            const worker = new Worker(__filename, {
                workerData: {
                    type: 'process_users',
                    userData: chunk,
                    dbPath: 'parallel_users.db'
                }
            });
            
            workers.push(worker);
        }
        
        const results = await Promise.all(
            workers.map(worker => 
                new Promise((resolve) => {
                    worker.on('message', resolve);
                })
            )
        );
        
        console.log('Worker results:', results);
        
        // Clean up workers
        workers.forEach(worker => worker.terminate());
    }
}
```

### GraphQL Integration

```typescript
import { GraphQLSchema, GraphQLObjectType, GraphQLString, GraphQLList, GraphQLID } from 'graphql';
import { GraphDB } from 'sombra';

const db = new GraphDB('graphql.db');

const UserType = new GraphQLObjectType({
    name: 'User',
    fields: {
        id: { type: GraphQLID },
        name: { type: GraphQLString },
        email: { type: GraphQLString },
        friends: {
            type: new GraphQLList(UserType),
            resolve: async (parent) => {
                const tx = db.beginTransaction();
                const friends = await tx.traverse()
                    .fromNode(parent.id)
                    .outgoing('FRIENDS')
                    .collect();
                
                const friendData = await Promise.all(
                    friends.map(async (friend) => {
                        const props = await tx.getNodeProperties(friend.id);
                        return { id: friend.id, ...props };
                    })
                );
                
                await tx.commit();
                return friendData;
            }
        }
    }
});

const QueryType = new GraphQLObjectType({
    name: 'Query',
    fields: {
        user: {
            type: UserType,
            args: {
                id: { type: GraphQLID }
            },
            resolve: async (_, { id }) => {
                const tx = db.beginTransaction();
                const user = await tx.getNode(parseInt(id));
                const properties = await tx.getNodeProperties(parseInt(id));
                await tx.commit();
                
                return { id: user.id, ...properties };
            }
        },
        users: {
            type: new GraphQLList(UserType),
            resolve: async () => {
                const tx = db.beginTransaction();
                const users = await tx.findNodesByLabel('User');
                
                const userData = await Promise.all(
                    users.map(async (user) => {
                        const props = await tx.getNodeProperties(user.id);
                        return { id: user.id, ...props };
                    })
                );
                
                await tx.commit();
                return userData;
            }
        }
    }
});

const schema = new GraphQLSchema({
    query: QueryType
});

export { schema };
```

## Testing

### Jest Testing

```typescript
import { GraphDB } from 'sombra';
import { existsSync, unlinkSync } from 'fs';
import { join } from 'path';

describe('Sombra Operations', () => {
    const testDbPath = join(__dirname, 'test.db');
    
    beforeEach(() => {
        // Clean up test database
        if (existsSync(testDbPath)) {
            unlinkSync(testDbPath);
        }
    });
    
    afterEach(() => {
        // Clean up test database
        if (existsSync(testDbPath)) {
            unlinkSync(testDbPath);
        }
    });
    
    test('create and retrieve node', async () => {
        const db = new GraphDB(testDbPath);
        const tx = db.beginTransaction();
        
        const user = await tx.createNode('User', { name: 'Alice' });
        await tx.commit();
        
        const tx2 = db.beginTransaction();
        const retrieved = await tx2.getNode(user.id);
        const props = await tx2.getNodeProperties(user.id);
        await tx2.commit();
        
        expect(retrieved.label).toBe('User');
        expect(props.name).toBe('Alice');
    });
    
    test('graph traversal', async () => {
        const db = new GraphDB(testDbPath);
        const tx = db.beginTransaction();
        
        const alice = await tx.createNode('User', { name: 'Alice' });
        const bob = await tx.createNode('User', { name: 'Bob' });
        await tx.createEdge(alice, bob, 'KNOWS', {});
        await tx.commit();
        
        const tx2 = db.beginTransaction();
        const friends = await tx2.traverse()
            .fromNode(alice.id)
            .outgoing('KNOWS')
            .collect();
        await tx2.commit();
        
        expect(friends).toHaveLength(1);
        expect(friends[0].id).toBe(bob.id);
    });
});
```

### Integration Testing

```typescript
import { GraphDB } from 'sombra';

describe('Social Network Integration', () => {
    let db: GraphDB;
    
    beforeAll(async () => {
        db = new GraphDB('integration_test.db');
    });
    
    afterAll(async () => {
        await db.close();
    });
    
    test('complete social network scenario', async () => {
        // Create users
        const tx = db.beginTransaction();
        const alice = await tx.createNode('User', { name: 'Alice', age: 30 });
        const bob = await tx.createNode('User', { name: 'Bob', age: 25 });
        const charlie = await tx.createNode('User', { name: 'Charlie', age: 35 });
        
        // Create friendships
        await tx.createEdge(alice, bob, 'FRIENDS', { since: new Date('2020-01-01') });
        await tx.createEdge(bob, charlie, 'FRIENDS', { since: new Date('2021-01-01') });
        await tx.createEdge(alice, charlie, 'FRIENDS', { since: new Date('2022-01-01') });
        await tx.commit();
        
        // Test Alice's friends
        const tx2 = db.beginTransaction();
        const aliceFriends = await tx2.traverse()
            .fromNode(alice.id)
            .outgoing('FRIENDS')
            .collect();
        await tx2.commit();
        
        expect(aliceFriends).toHaveLength(2);
        
        // Test friends of friends
        const tx3 = db.beginTransaction();
        const fofs = await tx3.traverse()
            .fromNode(alice.id)
            .outgoing('FRIENDS')
            .outgoing('FRIENDS')
            .collect();
        await tx3.commit();
        
        expect(fofs.length).toBeGreaterThanOrEqual(2);
    });
});
```

## Performance Optimization

### Connection Management

```typescript
class DatabaseManager {
    private static instance: DatabaseManager;
    private db: GraphDB;
    
    private constructor(dbPath: string) {
        this.db = new GraphDB(dbPath);
    }
    
    static getInstance(dbPath: string): DatabaseManager {
        if (!DatabaseManager.instance) {
            DatabaseManager.instance = new DatabaseManager(dbPath);
        }
        return DatabaseManager.instance;
    }
    
    getDatabase(): GraphDB {
        return this.db;
    }
    
    async close(): Promise<void> {
        await this.db.close();
    }
}

// Usage
const dbManager = DatabaseManager.getInstance('app.db');
const db = dbManager.getDatabase();
```

### Caching Layer

```typescript
import NodeCache from 'node-cache';

class CachedGraphDB {
    private db: GraphDB;
    private cache: NodeCache;
    
    constructor(dbPath: string, cacheTTL: number = 300) {
        this.db = new GraphDB(dbPath);
        this.cache = new NodeCache({ stdTTL: cacheTTL });
    }
    
    async getNodePropertiesCached(nodeId: number): Promise<any> {
        const cacheKey = `node_props_${nodeId}`;
        let props = this.cache.get<any>(cacheKey);
        
        if (!props) {
            const tx = this.db.beginTransaction();
            props = await tx.getNodeProperties(nodeId);
            await tx.commit();
            
            this.cache.set(cacheKey, props);
        }
        
        return props;
    }
    
    invalidateNodeCache(nodeId: number): void {
        const cacheKey = `node_props_${nodeId}`;
        this.cache.del(cacheKey);
    }
    
    async updateNodePropertiesWithCache(
        nodeId: number, 
        properties: any
    ): Promise<void> {
        const tx = this.db.beginTransaction();
        await tx.updateNodeProperties(nodeId, properties);
        await tx.commit();
        
        // Update cache
        const cacheKey = `node_props_${nodeId}`;
        const existingProps = this.cache.get<any>(cacheKey) || {};
        const updatedProps = { ...existingProps, ...properties };
        this.cache.set(cacheKey, updatedProps);
    }
}
```

## Next Steps

- Read the [Getting Started Guide](getting-started.md) for basic usage
- Check the [Configuration Guide](configuration.md) for performance tuning
- Review the [Operations Guide](operations.md) for production deployment
- Browse the [examples](../examples/) directory for complete applications