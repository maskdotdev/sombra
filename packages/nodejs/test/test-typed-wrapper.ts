import { SombraDB } from '../typed';
import * as fs from 'fs';
import * as assert from 'assert';

interface TestSchema {
  nodes: {
    Person: {
      name: string;
      age: number;
      active: boolean;
    };
    Company: {
      name: string;
      employees: number;
    };
  };
  edges: {
    WORKS_AT: {
      from: 'Person';
      to: 'Company';
      properties: {
        role: string;
        salary: number;
      };
    };
    KNOWS: {
      from: 'Person';
      to: 'Person';
      properties: {
        since: number;
      };
    };
  };
}

const dbPath = './test-typed.db';

if (fs.existsSync(dbPath)) {
  fs.unlinkSync(dbPath);
}

console.log('Testing TypedSombraDB...\n');

const db = new SombraDB<TestSchema>(dbPath);

console.log('1. Testing addNode with type-safe properties...');
const alice = db.addNode('Person', { name: 'Alice', age: 30, active: true });
const bob = db.addNode('Person', { name: 'Bob', age: 25, active: false });
const acme = db.addNode('Company', { name: 'ACME Corp', employees: 100 });
console.log('✓ Nodes created:', { alice, bob, acme });

console.log('\n2. Testing addEdge with type-safe properties...');
const edge1 = db.addEdge(alice, acme, 'WORKS_AT', { role: 'Engineer', salary: 120000 });
const edge2 = db.addEdge(bob, acme, 'WORKS_AT', { role: 'Designer', salary: 100000 });
const edge3 = db.addEdge(alice, bob, 'KNOWS', { since: 2020 });
console.log('✓ Edges created:', { edge1, edge2, edge3 });

console.log('\n3. Testing getNode with typed properties...');
const aliceNode = db.getNode<'Person'>(alice);
assert.strictEqual(aliceNode?.properties.name, 'Alice');
assert.strictEqual(aliceNode?.properties.age, 30);
assert.strictEqual(aliceNode?.properties.active, true);
console.log('✓ Alice node:', aliceNode);

console.log('\n4. Testing getEdge with typed properties...');
const worksAtEdge = db.getEdge<'WORKS_AT'>(edge1);
assert.strictEqual(worksAtEdge?.typeName, 'WORKS_AT');
assert.strictEqual(worksAtEdge?.properties.role, 'Engineer');
assert.strictEqual(worksAtEdge?.properties.salary, 120000);
console.log('✓ WORKS_AT edge:', worksAtEdge);

console.log('\n5. Testing findNodeByProperty...');
const foundAcme = db.findNodeByProperty('Company', 'name', 'ACME Corp');
assert.strictEqual(foundAcme, acme);
console.log('✓ Found company:', foundAcme);

console.log('\n6. Testing findNodesByProperty with string...');
const aliceByName = db.findNodesByProperty('Person', 'name', 'Alice');
assert.strictEqual(aliceByName.length, 1);
assert.strictEqual(aliceByName[0], alice);
console.log('✓ Found persons by name:', aliceByName);

console.log('\n7. Testing getIncomingNeighbors...');
const employees = db.getIncomingNeighbors(acme);
assert.strictEqual(employees.length, 2);
assert.ok(employees.includes(alice));
assert.ok(employees.includes(bob));
console.log('✓ Employees:', employees);

console.log('\n8. Testing query builder...');
const queryResult = db
  .query()
  .startFromProperty('Company', 'name', 'ACME Corp')
  .traverse(['WORKS_AT'], 'incoming', 1)
  .execute();
assert.strictEqual(queryResult.nodeIds.length, 3);
console.log('✓ Query result includes start node + employees:', queryResult.nodeIds.length, 'nodes');

console.log('\n9. Testing setNodeProperty...');
db.setNodeProperty<'Person', 'age'>(alice, 'age', 31);
const updatedAlice = db.getNode<'Person'>(alice);
assert.strictEqual(updatedAlice?.properties.age, 31);
console.log('✓ Updated Alice age to:', updatedAlice?.properties.age);

console.log('\n10. Testing countNodesWithLabel...');
const personCount = db.countNodesWithLabel('Person');
const companyCount = db.countNodesWithLabel('Company');
assert.strictEqual(personCount, 2);
assert.strictEqual(companyCount, 1);
console.log('✓ Person count:', personCount, 'Company count:', companyCount);

console.log('\n11. Testing countEdgesWithType...');
const worksAtCount = db.countEdgesWithType('WORKS_AT');
const knowsCount = db.countEdgesWithType('KNOWS');
assert.strictEqual(worksAtCount, 2);
assert.strictEqual(knowsCount, 1);
console.log('✓ WORKS_AT count:', worksAtCount, 'KNOWS count:', knowsCount);

console.log('\n12. Testing property conversion with numbers...');
const floatTest = db.addNode('Person', { name: 'Charlie', age: 25, active: true });
db.setNodeProperty<'Person', 'age'>(floatTest, 'age', 25);
const floatNode = db.getNode<'Person'>(floatTest);
assert.strictEqual(floatNode?.properties.age, 25);
console.log('✓ Number conversion works correctly');

console.log('\n13. Testing deleteEdge...');
db.deleteEdge(edge3);
const deletedEdge = db.getEdge(edge3);
assert.strictEqual(deletedEdge, null);
console.log('✓ Edge deleted successfully');

console.log('\n14. Testing deleteNode...');
db.deleteNode(bob);
const deletedNode = db.getNode(bob);
assert.strictEqual(deletedNode, null);
console.log('✓ Node deleted successfully');

console.log('\n15. Testing access to underlying db...');
const rawDb = db.db;
assert.ok(rawDb !== null);
assert.ok(typeof rawDb.getNode === 'function');
console.log('✓ Can access underlying SombraDB instance');

db.flush();

console.log('\n✅ All TypedSombraDB tests passed!\n');

if (fs.existsSync(dbPath)) {
  fs.unlinkSync(dbPath);
}
