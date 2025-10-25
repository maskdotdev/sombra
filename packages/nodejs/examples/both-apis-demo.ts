import { SombraDB } from '../typed';
import * as fs from 'fs';

console.log('=== SombraDB: One Class, Two Ways to Use It ===\n');

const dbPath = '/tmp/demo-both-apis.db';
if (fs.existsSync(dbPath)) fs.rmSync(dbPath);

console.log('--- Approach 1: Type-Safe API (with Schema) ---\n');

interface MySchema {
  nodes: {
    Person: { name: string; age: number };
    Company: { name: string };
  };
  edges: {
    WORKS_AT: {
      from: 'Person';
      to: 'Company';
      properties: { since: number };
    };
  };
}

const typedDb = new SombraDB<MySchema>(dbPath);

const alice = typedDb.addNode('Person', { name: 'Alice', age: 30 });
const acme = typedDb.addNode('Company', { name: 'Acme Corp' });
typedDb.addEdge(alice, acme, 'WORKS_AT', { since: 2020 });

const aliceNode = typedDb.getNode<'Person'>(alice);
console.log('Retrieved (typed):', aliceNode?.properties);
console.log('Type:', typeof aliceNode?.properties.name, typeof aliceNode?.properties.age);

console.log('\n--- Approach 2: Raw API (without Schema) ---\n');

if (fs.existsSync(dbPath)) fs.rmSync(dbPath);

const rawDb = new SombraDB(dbPath);

const bob = rawDb.addNode(['Person'], {
  name: { type: 'string', value: 'Bob' },
  age: { type: 'int', value: 25 },
});

const bobNode = rawDb.getNode(bob);
console.log('Retrieved (raw):', bobNode?.properties);
console.log('Type:', typeof bobNode?.properties.name, typeof bobNode?.properties.age);

console.log('\n✅ Both approaches work with the same SombraDB class!');
console.log('   - Use generic <Schema> for autocomplete & type safety');
console.log('   - Omit generic for raw API (backwards compatible)');

if (fs.existsSync(dbPath)) fs.rmSync(dbPath);
