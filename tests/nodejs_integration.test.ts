import { SombraDB } from '../index';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

function createTempFile(): string {
  const tmpDir = os.tmpdir();
  const tmpFile = path.join(tmpDir, `sombra-test-${Date.now()}-${Math.random()}.db`);
  return tmpFile;
}

describe('SombraDB Basic Operations', () => {
  let dbPath: string;
  let db: SombraDB;

  beforeEach(() => {
    dbPath = createTempFile();
    db = new SombraDB(dbPath);
  });

  afterEach(() => {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
  });

  test('create and get node', () => {
    const nodeId = db.addNode(['Person'], {
      name: { type: 'string', value: 'Alice' },
      age: { type: 'int', value: 30 }
    });

    expect(nodeId).toBe(1);

    const node = db.getNode(nodeId);
    expect(node.id).toBe(nodeId);
    expect(node.labels).toEqual(['Person']);
    expect(node.properties.name.value).toBe('Alice');
    expect(node.properties.age.value).toBe(30);
  });

  test('create and get edge', () => {
    const node1 = db.addNode(['Person'], {
      name: { type: 'string', value: 'Alice' }
    });
    const node2 = db.addNode(['Person'], {
      name: { type: 'string', value: 'Bob' }
    });

    const edgeId = db.addEdge(node1, node2, 'KNOWS', {
      since: { type: 'int', value: 2020 }
    });

    expect(edgeId).toBe(1);

    const edge = db.getEdge(edgeId);
    expect(edge.id).toBe(edgeId);
    expect(edge.sourceNodeId).toBe(node1);
    expect(edge.targetNodeId).toBe(node2);
    expect(edge.typeName).toBe('KNOWS');
    expect(edge.properties.since.value).toBe(2020);
  });
});

describe('SombraDB Transactions', () => {
  let dbPath: string;
  let db: SombraDB;

  beforeEach(() => {
    dbPath = createTempFile();
    db = new SombraDB(dbPath);
  });

  afterEach(() => {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
  });

  test('transaction commit', () => {
    const tx = db.beginTransaction();
    const nodeId = tx.addNode(['Test'], {
      value: { type: 'int', value: 42 }
    });
    tx.commit();

    const node = db.getNode(nodeId);
    expect(node.properties.value.value).toBe(42);
  });

  test('transaction rollback', () => {
    const tx1 = db.beginTransaction();
    const committedId = tx1.addNode(['Committed'], {
      status: { type: 'string', value: 'committed' }
    });
    tx1.commit();

    const tx2 = db.beginTransaction();
    const rolledBackId = tx2.addNode(['RolledBack'], {
      status: { type: 'string', value: 'rollback' }
    });
    tx2.rollback();

    const committedNode = db.getNode(committedId);
    expect(committedNode).toBeDefined();
    expect(committedNode.properties.status.value).toBe('committed');

    expect(() => db.getNode(rolledBackId)).toThrow();
  });
});

describe('SombraDB Graph Traversal', () => {
  let dbPath: string;
  let db: SombraDB;

  beforeEach(() => {
    dbPath = createTempFile();
    db = new SombraDB(dbPath);
  });

  afterEach(() => {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
  });

  test('get outgoing edges', () => {
    const node1 = db.addNode(['Node'], {});
    const node2 = db.addNode(['Node'], {});
    const node3 = db.addNode(['Node'], {});

    db.addEdge(node1, node2, 'CONNECTS', {});
    db.addEdge(node1, node3, 'CONNECTS', {});

    const outgoing = db.getOutgoingEdges(node1);
    expect(outgoing.length).toBe(2);
  });

  test('get neighbors', () => {
    const center = db.addNode(['Center'], {});
    const n1 = db.addNode(['Neighbor'], { id: { type: 'int', value: 1 } });
    const n2 = db.addNode(['Neighbor'], { id: { type: 'int', value: 2 } });

    db.addEdge(center, n1, 'LINKS', {});
    db.addEdge(center, n2, 'LINKS', {});

    const neighbors = db.getNeighbors(center);
    expect(neighbors.length).toBe(2);
    expect(neighbors).toContain(n1);
    expect(neighbors).toContain(n2);
  });

  test('bfs traversal', () => {
    const nodes = [];
    for (let i = 0; i < 5; i++) {
      nodes.push(db.addNode(['Chain'], { index: { type: 'int', value: i } }));
    }

    for (let i = 0; i < nodes.length - 1; i++) {
      db.addEdge(nodes[i], nodes[i + 1], 'NEXT', {});
    }

    const results = db.bfsTraversal(nodes[0], 10);
    expect(results.length).toBeGreaterThanOrEqual(1);
    expect(results[0].nodeId).toBe(nodes[0]);
    expect(results[0].depth).toBe(0);
  });
});

describe('SombraDB Property Types', () => {
  let dbPath: string;
  let db: SombraDB;

  beforeEach(() => {
    dbPath = createTempFile();
    db = new SombraDB(dbPath);
  });

  afterEach(() => {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
  });

  test('integer properties', () => {
    const nodeId = db.addNode(['Test'], {
      count: { type: 'int', value: 100 },
      negative: { type: 'int', value: -50 }
    });

    const node = db.getNode(nodeId);
    expect(node.properties.count.value).toBe(100);
    expect(node.properties.negative.value).toBe(-50);
  });

  test('float properties', () => {
    const nodeId = db.addNode(['Test'], {
      value: { type: 'float', value: 3.14 },
      negative: { type: 'float', value: -2.5 }
    });

    const node = db.getNode(nodeId);
    expect(node.properties.value.value).toBeCloseTo(3.14);
    expect(node.properties.negative.value).toBeCloseTo(-2.5);
  });

  test('boolean properties', () => {
    const nodeId = db.addNode(['Test'], {
      active: { type: 'bool', value: true },
      deleted: { type: 'bool', value: false }
    });

    const node = db.getNode(nodeId);
    expect(node.properties.active.value).toBe(true);
    expect(node.properties.deleted.value).toBe(false);
  });

  test('string properties', () => {
    const nodeId = db.addNode(['Test'], {
      name: { type: 'string', value: 'Test Node' },
      description: { type: 'string', value: 'A test node with multiple properties' }
    });

    const node = db.getNode(nodeId);
    expect(node.properties.name.value).toBe('Test Node');
    expect(node.properties.description.value).toBe('A test node with multiple properties');
  });

  test('mixed properties', () => {
    const nodeId = db.addNode(['Test'], {
      name: { type: 'string', value: 'Mixed' },
      count: { type: 'int', value: 42 },
      ratio: { type: 'float', value: 0.75 },
      active: { type: 'bool', value: true }
    });

    const node = db.getNode(nodeId);
    expect(node.properties.name.value).toBe('Mixed');
    expect(node.properties.count.value).toBe(42);
    expect(node.properties.ratio.value).toBeCloseTo(0.75);
    expect(node.properties.active.value).toBe(true);
  });
});

describe('SombraDB Bulk Operations', () => {
  let dbPath: string;
  let db: SombraDB;

  beforeEach(() => {
    dbPath = createTempFile();
    db = new SombraDB(dbPath);
  });

  afterEach(() => {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
  });

  test('bulk node insertion', () => {
    const nodeCount = 100;
    for (let i = 0; i < nodeCount; i++) {
      db.addNode(['Bulk'], {
        index: { type: 'int', value: i }
      });
    }

    let count = 0;
    for (let i = 1; i <= nodeCount; i++) {
      try {
        const node = db.getNode(i);
        if (node) count++;
      } catch (e) {
        // Node doesn't exist
      }
    }

    expect(count).toBe(nodeCount);
  });

  test('bulk edge insertion', () => {
    const nodes = [];
    for (let i = 0; i < 20; i++) {
      nodes.push(db.addNode(['Node'], {
        id: { type: 'int', value: i }
      }));
    }

    for (let i = 0; i < nodes.length - 1; i++) {
      db.addEdge(nodes[i], nodes[i + 1], 'NEXT', {});
    }

    const firstNodeEdges = db.getOutgoingEdges(nodes[0]);
    expect(firstNodeEdges.length).toBe(1);
  });
});

describe('SombraDB Persistence', () => {
  test('database persistence', () => {
    const dbPath = createTempFile();

    let nodeId: number;
    {
      const db1 = new SombraDB(dbPath);
      nodeId = db1.addNode(['Persistent'], {
        value: { type: 'string', value: 'preserved' }
      });
      db1.checkpoint();
    }

    {
      const db2 = new SombraDB(dbPath);
      const node = db2.getNode(nodeId);
      expect(node).toBeDefined();
      expect(node.properties.value.value).toBe('preserved');
    }

    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
  });
});

describe('SombraDB Label Queries', () => {
  let dbPath: string;
  let db: SombraDB;

  beforeEach(() => {
    dbPath = createTempFile();
    db = new SombraDB(dbPath);
  });

  afterEach(() => {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
  });

  test('get nodes by label', () => {
    for (let i = 0; i < 5; i++) {
      db.addNode(['Person'], { id: { type: 'int', value: i } });
    }

    for (let i = 0; i < 3; i++) {
      db.addNode(['Company'], { id: { type: 'int', value: i } });
    }

    const persons = db.getNodesByLabel('Person');
    expect(persons.length).toBe(5);

    const companies = db.getNodesByLabel('Company');
    expect(companies.length).toBe(3);
  });
});

describe('SombraDB Edge Counts', () => {
  let dbPath: string;
  let db: SombraDB;

  beforeEach(() => {
    dbPath = createTempFile();
    db = new SombraDB(dbPath);
  });

  afterEach(() => {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
  });

  test('count outgoing and incoming edges', () => {
    const center = db.addNode(['Center'], {});
    const n1 = db.addNode(['Node'], {});
    const n2 = db.addNode(['Node'], {});
    const n3 = db.addNode(['Node'], {});

    db.addEdge(center, n1, 'OUT', {});
    db.addEdge(center, n2, 'OUT', {});
    db.addEdge(n3, center, 'IN', {});

    expect(db.countOutgoingEdges(center)).toBe(2);
    expect(db.countIncomingEdges(center)).toBe(1);
  });
});
