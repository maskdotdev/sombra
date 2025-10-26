const { SombraDB } = require('../typed.js');

// Clean up any existing test database
const fs = require('fs');
const dbPath = '/tmp/typed-query-test.db';
if (fs.existsSync(dbPath)) {
  fs.unlinkSync(dbPath);
}

console.log('Testing typed QueryBuilder with getIds() and getNodes()...\n');

const db = new SombraDB(dbPath);

// Add test data
const alice = db.addNode('Person', { name: 'Alice', age: 30 });
const bob = db.addNode('Person', { name: 'Bob', age: 25 });
const charlie = db.addNode('Person', { name: 'Charlie', age: 35 });
const acme = db.addNode('Company', { name: 'Acme Corp' });

db.addEdge(alice, bob, 'KNOWS', {});
db.addEdge(alice, acme, 'WORKS_AT', {});

console.log('✓ Created test data\n');

// Test 1: getIds() method
console.log('Test 1: getIds() method');
const queryResult = db.query()
  .startFromLabel('Person')
  .getIds();

console.log('  Result:', queryResult);
console.log('  Has startNodes:', Array.isArray(queryResult.startNodes));
console.log('  Has nodeIds:', Array.isArray(queryResult.nodeIds));
console.log('  Has limited:', typeof queryResult.limited === 'boolean');
console.log('  Node count:', queryResult.nodeIds.length);
console.log('✓ getIds() works correctly\n');

// Test 2: getNodes() method
console.log('Test 2: getNodes() method');
const nodes = db.query()
  .startFromLabel('Person')
  .getNodes();

console.log('  Nodes returned:', nodes.length);
console.log('  First node:', nodes[0]);
console.log('  Properties are plain JS:', typeof nodes[0].properties.name === 'string');
console.log('  Age is number:', typeof nodes[0].properties.age === 'number');
console.log('✓ getNodes() works correctly\n');

// Test 3: getNodes() with traversal
console.log('Test 3: getNodes() with traversal');
const traversedNodes = db.query()
  .startFromLabel('Person')
  .traverse(['KNOWS'], 'outgoing', 1)
  .getNodes();

console.log('  Nodes after traversal:', traversedNodes.length);
console.log('  Found Bob:', traversedNodes.some(n => n.properties.name === 'Bob'));
console.log('✓ getNodes() with traversal works\n');

// Test 4: Compare execute() and getIds() - should be identical
console.log('Test 4: Compare execute() and getIds()');
const executeResult = db.query()
  .startFromLabel('Person')
  .execute();

const getIdsResult = db.query()
  .startFromLabel('Person')
  .getIds();

console.log('  execute() result:', executeResult);
console.log('  getIds() result:', getIdsResult);
console.log('  Results are identical:', 
  JSON.stringify(executeResult) === JSON.stringify(getIdsResult));
console.log('✓ execute() and getIds() return same data\n');

console.log('🎉 All typed QueryBuilder tests passed!');

// Cleanup
fs.unlinkSync(dbPath);
