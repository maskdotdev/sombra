const { SombraDB } = require('../index.js');
const fs = require('fs');
const path = require('path');

const dbPath = path.join(__dirname, 'test-query.db');

// Clean up any existing test database
if (fs.existsSync(dbPath)) {
  fs.unlinkSync(dbPath);
}
if (fs.existsSync(dbPath + '-wal')) {
  fs.unlinkSync(dbPath + '-wal');
}

try {
  console.log('Testing new Query APIs...\n');
  
  const db = new SombraDB(dbPath);
  
  // Create test data
  console.log('Creating test data...');
  const user1 = db.addNode(['User'], { name: { type: 'string', value: 'Alice' } });
  const user2 = db.addNode(['User'], { name: { type: 'string', value: 'Bob' } });
  const user3 = db.addNode(['User'], { name: { type: 'string', value: 'Charlie' } });
  const post1 = db.addNode(['Post'], { title: { type: 'string', value: 'Hello World' } });
  const post2 = db.addNode(['Post'], { title: { type: 'string', value: 'Graph DBs' } });
  
  db.addEdge(user1, post1, 'WROTE');
  db.addEdge(user1, post2, 'WROTE');
  db.addEdge(user2, post1, 'LIKED');
  db.addEdge(user2, user1, 'FOLLOWS');
  db.addEdge(user3, user2, 'FOLLOWS');
  
  db.flush();
  
  // Test Analytics APIs
  console.log('\n=== Analytics APIs ===');
  
  console.log('\n1. Count nodes by label:');
  const nodeCounts = db.countNodesByLabel();
  console.log(JSON.stringify(nodeCounts, null, 2));
  
  console.log('\n2. Count edges by type:');
  const edgeCounts = db.countEdgesByType();
  console.log(JSON.stringify(edgeCounts, null, 2));
  
  console.log('\n3. Total node count:', db.getTotalNodeCount());
  console.log('4. Total edge count:', db.getTotalEdgeCount());
  
  console.log('\n5. Degree distribution:');
  const degDist = db.degreeDistribution();
  console.log('In-degree:', degDist.inDegree.slice(0, 3));
  console.log('Out-degree:', degDist.outDegree.slice(0, 3));
  console.log('Total-degree:', degDist.totalDegree.slice(0, 3));
  
  console.log('\n6. Find hubs (min degree 2, total):');
  const hubs = db.findHubs(2, 'total');
  console.log(hubs);
  
  console.log('\n7. Find isolated nodes:');
  const isolated = db.findIsolatedNodes();
  console.log('Isolated nodes:', isolated);
  
  console.log('\n8. Find leaf nodes (outgoing):');
  const leaves = db.findLeafNodes('outgoing');
  console.log('Leaf nodes:', leaves);
  
  console.log('\n9. Average degree:', db.getAverageDegree());
  console.log('10. Graph density:', db.getDensity());
  
  console.log('\n11. Count nodes with label "User":', db.countNodesWithLabel('User'));
  console.log('12. Count edges with type "FOLLOWS":', db.countEdgesWithType('FOLLOWS'));
  
  // Test Subgraph APIs
  console.log('\n\n=== Subgraph APIs ===');
  
  console.log('\n1. Extract subgraph from user1 (depth 2):');
  const subgraph1 = db.extractSubgraph([user1], 2);
  console.log('Nodes:', subgraph1.nodes.length);
  console.log('Edges:', subgraph1.edges.length);
  console.log('Boundary nodes:', subgraph1.boundaryNodes);
  
  console.log('\n2. Extract subgraph with edge type filter (FOLLOWS only):');
  const subgraph2 = db.extractSubgraph([user2], 2, ['FOLLOWS']);
  console.log('Nodes:', subgraph2.nodes.length);
  console.log('Edges:', subgraph2.edges.length);
  
  console.log('\n3. Extract induced subgraph (user1, user2, post1):');
  const subgraph3 = db.extractInducedSubgraph([user1, user2, post1]);
  console.log('Nodes:', subgraph3.nodes.length);
  console.log('Edges:', subgraph3.edges.length);
  console.log('Node IDs:', subgraph3.nodes.map(n => n.id));
  
  console.log('\n✅ All tests passed!');
  
} catch (error) {
  console.error('❌ Test failed:', error);
  process.exit(1);
} finally {
  // Clean up
  if (fs.existsSync(dbPath)) {
    fs.unlinkSync(dbPath);
  }
  if (fs.existsSync(dbPath + '-wal')) {
    fs.unlinkSync(dbPath + '-wal');
  }
}
