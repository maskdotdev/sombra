import { SombraDB, DegreeDistribution, Subgraph, Pattern, Match } from '../index';

const db = new SombraDB(':memory:');

const n1 = db.addNode(['Person'], { name: { type: 'string', value: 'Alice' } });
const n2 = db.addNode(['Person'], { name: { type: 'string', value: 'Bob' } });
db.addEdge(n1, n2, 'KNOWS', {});

// Users can use the clean types!
const degreeDistribution: DegreeDistribution = db.degreeDistribution();
console.log('✓ DegreeDistribution type works');

const subgraph: Subgraph = db.extractSubgraph([n1], 1);
console.log('✓ Subgraph type works');

const pattern: Pattern = {
  nodes: [
    { varName: 'a', labels: ['Person'] },
    { varName: 'b', labels: ['Person'] }
  ],
  edges: [
    { fromVar: 'a', toVar: 'b', direction: 'outgoing' }
  ]
};

const matches: Match[] = db.matchPattern(pattern);
console.log('✓ Pattern and Match types work');

console.log('\n🎉 All clean type aliases work perfectly!');
