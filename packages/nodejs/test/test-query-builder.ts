import { SombraDB } from '../index';

const db = new SombraDB('test.db');

// Test query builder types
const query = db.query();

// Test chainable methods
query
  .startFromLabel('Function')
  .traverse(['CALLS'], 'outgoing', 2)
  .limit(10);

const result = query.execute();

// Test result type
console.log(result.startNodes);
console.log(result.nodeIds);
console.log(result.limited);

// Test other start methods
db.query()
  .startFrom([1, 2, 3])
  .execute();

db.query()
  .startFromProperty('Function', 'name', 'main')
  .traverse(['CALLS'], 'both', 1)
  .execute();
