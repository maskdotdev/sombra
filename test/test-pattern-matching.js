const { SombraDB } = require('../index');
const path = require('path');
const fs = require('fs');
const assert = require('assert');

function cleanup(dbPath) {
  if (fs.existsSync(dbPath)) {
    fs.unlinkSync(dbPath);
  }
}

function testBasicCallPattern() {
  console.log('Test: Basic call pattern matching...');
  const dbPath = path.join(__dirname, 'test-pattern-calls.db');
  cleanup(dbPath);

  try {
    const db = new SombraDB(dbPath);

    const targetId = db.addNode(['Function'], {
      name: { type: 'string', value: 'foo' }
    });

    const callId = db.addNode(['CallExpr'], {
      callee: { type: 'string', value: 'foo' }
    });

    const otherFuncId = db.addNode(['Function'], {
      name: { type: 'string', value: 'bar' }
    });

    const otherCallId = db.addNode(['CallExpr'], {
      callee: { type: 'string', value: 'bar' }
    });

    const callEdgeId = db.addEdge(callId, targetId, 'CALLS');
    db.addEdge(otherCallId, otherFuncId, 'CALLS');

    db.flush();
    db.checkpoint();

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
          properties: {
            equals: {},
            notEquals: {},
            ranges: []
          },
          direction: 'outgoing'
        }
      ]
    };

    const matches = db.matchPattern(pattern);

    assert.strictEqual(matches.length, 1, 'Should find exactly one match');
    assert.strictEqual(matches[0].nodeBindings.call, callId, 'call variable should bind to callId');
    assert.strictEqual(matches[0].nodeBindings.func, targetId, 'func variable should bind to targetId');
    assert.deepStrictEqual(matches[0].edgeIds, [callEdgeId], 'Should capture edge ID');

    console.log('✓ Basic call pattern test passed');
  } finally {
    cleanup(dbPath);
  }
}

function testIncomingEdgePattern() {
  console.log('Test: Incoming edge pattern matching...');
  const dbPath = path.join(__dirname, 'test-pattern-incoming.db');
  cleanup(dbPath);

  try {
    const db = new SombraDB(dbPath);

    const parentId = db.addNode(['Module'], {
      name: { type: 'string', value: 'core' }
    });

    const childId = db.addNode(['File'], {
      path: { type: 'string', value: 'src/lib.rs' }
    });

    const siblingId = db.addNode(['File'], {
      path: { type: 'string', value: 'src/mod.rs' }
    });

    const containsEdgeId = db.addEdge(parentId, childId, 'CONTAINS');
    db.addEdge(parentId, siblingId, 'CONTAINS');

    db.flush();
    db.checkpoint();

    const pattern = {
      nodes: [
        {
          varName: 'child',
          labels: ['File'],
          properties: {
            equals: { path: { type: 'string', value: 'src/lib.rs' } },
            notEquals: {},
            ranges: []
          }
        },
        {
          varName: 'parent',
          labels: ['Module'],
          properties: {
            equals: {},
            notEquals: {},
            ranges: []
          }
        }
      ],
      edges: [
        {
          fromVar: 'child',
          toVar: 'parent',
          types: ['CONTAINS'],
          properties: {
            equals: {},
            notEquals: {},
            ranges: []
          },
          direction: 'incoming'
        }
      ]
    };

    const matches = db.matchPattern(pattern);

    assert.strictEqual(matches.length, 1, 'Should find exactly one match');
    assert.strictEqual(matches[0].nodeBindings.child, childId, 'child variable should bind to childId');
    assert.strictEqual(matches[0].nodeBindings.parent, parentId, 'parent variable should bind to parentId');
    assert.deepStrictEqual(matches[0].edgeIds, [containsEdgeId], 'Should capture edge ID');

    console.log('✓ Incoming edge pattern test passed');
  } finally {
    cleanup(dbPath);
  }
}

function testPropertyRangeFilter() {
  console.log('Test: Property range filter in pattern...');
  const dbPath = path.join(__dirname, 'test-pattern-range.db');
  cleanup(dbPath);

  try {
    const db = new SombraDB(dbPath);

    const user1Id = db.addNode(['User'], {
      name: { type: 'string', value: 'Alice' },
      age: { type: 'int', value: 25 }
    });

    const user2Id = db.addNode(['User'], {
      name: { type: 'string', value: 'Bob' },
      age: { type: 'int', value: 35 }
    });

    const user3Id = db.addNode(['User'], {
      name: { type: 'string', value: 'Charlie' },
      age: { type: 'int', value: 45 }
    });

    const post1Id = db.addNode(['Post'], {
      title: { type: 'string', value: 'Post1' }
    });

    const post2Id = db.addNode(['Post'], {
      title: { type: 'string', value: 'Post2' }
    });

    const post3Id = db.addNode(['Post'], {
      title: { type: 'string', value: 'Post3' }
    });

    db.addEdge(user1Id, post1Id, 'AUTHORED');
    const edge2Id = db.addEdge(user2Id, post2Id, 'AUTHORED');
    db.addEdge(user3Id, post3Id, 'AUTHORED');

    db.flush();
    db.checkpoint();

    const pattern = {
      nodes: [
        {
          varName: 'user',
          labels: ['User'],
          properties: {
            equals: {},
            notEquals: {},
            ranges: [
              {
                key: 'age',
                min: { value: { type: 'int', value: 30 }, inclusive: true },
                max: { value: { type: 'int', value: 40 }, inclusive: true }
              }
            ]
          }
        },
        {
          varName: 'post',
          labels: ['Post'],
          properties: {
            equals: {},
            notEquals: {},
            ranges: []
          }
        }
      ],
      edges: [
        {
          fromVar: 'user',
          toVar: 'post',
          types: ['AUTHORED'],
          properties: {
            equals: {},
            notEquals: {},
            ranges: []
          },
          direction: 'outgoing'
        }
      ]
    };

    const matches = db.matchPattern(pattern);

    assert.strictEqual(matches.length, 1, 'Should find exactly one match (Bob, age 35)');
    assert.strictEqual(matches[0].nodeBindings.user, user2Id, 'user variable should bind to user2Id (Bob)');
    assert.strictEqual(matches[0].nodeBindings.post, post2Id, 'post variable should bind to post2Id');
    assert.deepStrictEqual(matches[0].edgeIds, [edge2Id], 'Should capture edge ID');

    console.log('✓ Property range filter test passed');
  } finally {
    cleanup(dbPath);
  }
}

function testMultiHopPattern() {
  console.log('Test: Multi-hop pattern matching...');
  const dbPath = path.join(__dirname, 'test-pattern-multihop.db');
  cleanup(dbPath);

  try {
    const db = new SombraDB(dbPath);

    const aliceId = db.addNode(['Person'], {
      name: { type: 'string', value: 'Alice' }
    });

    const bobId = db.addNode(['Person'], {
      name: { type: 'string', value: 'Bob' }
    });

    const charlieId = db.addNode(['Person'], {
      name: { type: 'string', value: 'Charlie' }
    });

    const edge1Id = db.addEdge(aliceId, bobId, 'KNOWS');
    const edge2Id = db.addEdge(bobId, charlieId, 'KNOWS');

    db.flush();
    db.checkpoint();

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
          properties: {
            equals: {},
            notEquals: {},
            ranges: []
          }
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
          properties: {
            equals: {},
            notEquals: {},
            ranges: []
          },
          direction: 'outgoing'
        },
        {
          fromVar: 'b',
          toVar: 'c',
          types: ['KNOWS'],
          properties: {
            equals: {},
            notEquals: {},
            ranges: []
          },
          direction: 'outgoing'
        }
      ]
    };

    const matches = db.matchPattern(pattern);

    assert.strictEqual(matches.length, 1, 'Should find exactly one match');
    assert.strictEqual(matches[0].nodeBindings.a, aliceId, 'a should bind to Alice');
    assert.strictEqual(matches[0].nodeBindings.b, bobId, 'b should bind to Bob');
    assert.strictEqual(matches[0].nodeBindings.c, charlieId, 'c should bind to Charlie');
    assert.deepStrictEqual(matches[0].edgeIds, [edge1Id, edge2Id], 'Should capture both edge IDs');

    console.log('✓ Multi-hop pattern test passed');
  } finally {
    cleanup(dbPath);
  }
}

function testNotEqualsFilter() {
  console.log('Test: Not-equals property filter...');
  const dbPath = path.join(__dirname, 'test-pattern-notequals.db');
  cleanup(dbPath);

  try {
    const db = new SombraDB(dbPath);

    const fooId = db.addNode(['Function'], {
      name: { type: 'string', value: 'foo' },
      visibility: { type: 'string', value: 'public' }
    });

    db.addNode(['Function'], {
      name: { type: 'string', value: 'bar' },
      visibility: { type: 'string', value: 'private' }
    });

    db.flush();
    db.checkpoint();

    const pattern = {
      nodes: [
        {
          varName: 'func',
          labels: ['Function'],
          properties: {
            equals: {},
            notEquals: { visibility: { type: 'string', value: 'private' } },
            ranges: []
          }
        }
      ],
      edges: []
    };

    const matches = db.matchPattern(pattern);

    assert.strictEqual(matches.length, 1, 'Should find exactly one match');
    assert.strictEqual(matches[0].nodeBindings.func, fooId, 'func should bind to foo (public)');

    console.log('✓ Not-equals filter test passed');
  } finally {
    cleanup(dbPath);
  }
}

function runTests() {
  console.log('=== Pattern Matching Tests ===\n');

  try {
    testBasicCallPattern();
    testIncomingEdgePattern();
    testPropertyRangeFilter();
    testMultiHopPattern();
    testNotEqualsFilter();

    console.log('\n=== All tests passed! ===');
  } catch (err) {
    console.error('\n=== Test failed ===');
    console.error(err);
    process.exit(1);
  }
}

runTests();
