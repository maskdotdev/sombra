const { SombraDB } = require('../index');

const db = new SombraDB(':memory:');

const n1 = db.addNode(['Person'], { name: { type: 'string', value: 'Alice' } });
const n2 = db.addNode(['Person'], { name: { type: 'string', value: 'Bob' } });
db.addEdge(n1, n2, 'KNOWS', {});

// Test JavaScript can use the clean API
const degreeDistribution = db.degreeDistribution();
console.log('✓ degreeDistribution() works:', degreeDistribution.inDegree.length, 'nodes');

const subgraph = db.extractSubgraph([n1], 1);
console.log('✓ extractSubgraph() works:', subgraph.nodes.length, 'nodes');

const pattern = {
  nodes: [
    { varName: 'a', labels: ['Person'] },
    { varName: 'b', labels: ['Person'] }
  ],
  edges: [
    { fromVar: 'a', toVar: 'b', direction: 'outgoing' }
  ]
};

const matches = db.matchPattern(pattern);
console.log('✓ matchPattern() works:', matches.length, 'matches');

console.log('\n🎉 JavaScript API works perfectly!');
