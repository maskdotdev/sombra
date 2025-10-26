import { SombraDB } from '../index';

const db = new SombraDB('test.db');

// Test query builder types
const query = db.query();

// Test chainable methods
query
  .startFromLabel('Function')
  .traverse(['CALLS'], 'outgoing', 2)
  .limit(10);

const result = query.getIds();

// Test result type
console.log(result.startNodes);
console.log(result.nodeIds);
console.log(result.limited);

// Test other start methods
db.query()
  .startFrom([1, 2, 3])
  .getIds();

db.query()
  .startFromProperty('Function', 'name', 'main')
  .traverse(['CALLS'], 'both', 1)
  .getIds();

// Test getNodes method
const nodes = db.query()
  .startFromLabel('Function')
  .limit(5)
  .getNodes();

console.log(nodes.length);
console.log(nodes[0]?.id);
console.log(nodes[0]?.labels);
