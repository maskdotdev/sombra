import { SombraDB, JsDegreeDistribution, JsSubgraph, JsPattern, JsMatch } from '../index';

const db = new SombraDB(':memory:');

const n1 = db.addNode(['Person'], { name: { type: 'string', value: 'Alice' } });
const n2 = db.addNode(['Person'], { name: { type: 'string', value: 'Bob' } });
db.addEdge(n1, n2, 'KNOWS', {});

const degreeDistribution: JsDegreeDistribution = db.degreeDistribution();
console.log('Degree distribution:', degreeDistribution.inDegree.length);

const subgraph: JsSubgraph = db.extractSubgraph([n1], 1);
console.log('Subgraph nodes:', subgraph.nodes.length);

const pattern: JsPattern = {
  nodes: [
    { varName: 'a', labels: ['Person'] },
    { varName: 'b', labels: ['Person'] }
  ],
  edges: [
    { fromVar: 'a', toVar: 'b', direction: 'outgoing' }
  ]
};

const matches: JsMatch[] = db.matchPattern(pattern);
console.log('Pattern matches:', matches.length);

console.log('✓ Type definitions test passed');
