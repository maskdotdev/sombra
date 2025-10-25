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
    Employee: {
      employeeId: string;
      department: string;
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

console.log('\n16. Testing multiple labels with combined properties...');
const charlie = db.addNode(['Person', 'Employee'], { 
  name: 'Charlie', 
  age: 28, 
  active: true,
  employeeId: 'E200',
  department: 'Engineering',
});
const charlieNode = db.db.getNode(charlie); // Use untyped API for multi-label nodes
assert.strictEqual(charlieNode?.labels.length, 2);
assert.ok(charlieNode?.labels.includes('Person'));
assert.ok(charlieNode?.labels.includes('Employee'));
assert.deepStrictEqual(charlieNode?.properties.name, { type: 'string', value: 'Charlie' });
assert.deepStrictEqual(charlieNode?.properties.age, { type: 'int', value: 28 });
assert.deepStrictEqual(charlieNode?.properties.employeeId, { type: 'string', value: 'E200' });
assert.deepStrictEqual(charlieNode?.properties.department, { type: 'string', value: 'Engineering' });
console.log('✓ Multi-label node with combined properties:', charlieNode);

console.log('\n17. Testing another multi-label node...');
const david = db.addNode(['Person', 'Employee'], { 
  name: 'David',
  age: 31,
  active: false,
  employeeId: 'E123', 
  department: 'Engineering',
});
const davidNode = db.db.getNode(david); // Use untyped API for multi-label nodes
assert.strictEqual(davidNode?.labels.length, 2);
assert.deepStrictEqual(davidNode?.properties.name, { type: 'string', value: 'David' });
assert.deepStrictEqual(davidNode?.properties.age, { type: 'int', value: 31 });
assert.deepStrictEqual(davidNode?.properties.active, { type: 'bool', value: false });
assert.deepStrictEqual(davidNode?.properties.employeeId, { type: 'string', value: 'E123' });
assert.deepStrictEqual(davidNode?.properties.department, { type: 'string', value: 'Engineering' });
console.log('✓ Multi-label node persisted with all properties:', davidNode);

console.log('\n18. Testing multiple labels with properties from both...');
const eve = db.addNode(['Person', 'Employee'], { 
  name: 'Eve', 
  age: 32, 
  active: true,
  employeeId: 'E456', 
  department: 'Sales',
});
const eveNode = db.db.getNode(eve); // Use untyped API for multi-label nodes
assert.strictEqual(eveNode?.labels.length, 2);
assert.deepStrictEqual(eveNode?.properties.name, { type: 'string', value: 'Eve' });
assert.deepStrictEqual(eveNode?.properties.age, { type: 'int', value: 32 });
assert.deepStrictEqual(eveNode?.properties.employeeId, { type: 'string', value: 'E456' });
assert.deepStrictEqual(eveNode?.properties.department, { type: 'string', value: 'Sales' });
console.log('✓ Multi-label node with both properties:', eveNode);

console.log('\n19. Testing query by label finds multi-label nodes...');
const allPeople = db.getNodesByLabel('Person');
assert.ok(allPeople.includes(charlie));
assert.ok(allPeople.includes(david));
assert.ok(allPeople.includes(eve));
const allEmployees = db.getNodesByLabel('Employee');
assert.ok(allEmployees.includes(charlie));
assert.ok(allEmployees.includes(david));
assert.ok(allEmployees.includes(eve));
console.log('✓ Multi-label nodes found by both labels');

db.flush();

console.log('\n✅ All TypedSombraDB tests passed!\n');

if (fs.existsSync(dbPath)) {
  fs.unlinkSync(dbPath);
}
