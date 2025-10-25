import { SombraDB } from '../typed';
import * as fs from 'fs';

interface MySchema {
  nodes: {
    Person: {
      name: string;
      age: number;
    };
    Company: {
      name: string;
      founded: number;
    };
  };
  edges: {
    WORKS_AT: {
      from: 'Person';
      to: 'Company';
      properties: {
        since: number;
      };
    };
    KNOWS: {
      from: 'Person';
      to: 'Person';
      properties: {};
    };
  };
}

const dbPath = '/tmp/test-unified-typed.db';
if (fs.existsSync(dbPath)) {
  fs.rmSync(dbPath);
}

const db = new SombraDB<MySchema>(dbPath);

const alice = db.addNode('Person', { name: 'Alice', age: 30 });
const bob = db.addNode('Person', { name: 'Bob', age: 25 });
const acme = db.addNode('Company', { name: 'Acme Corp', founded: 2000 });

const edge1 = db.addEdge(alice, acme, 'WORKS_AT', { since: 2020 });
const edge2 = db.addEdge(alice, bob, 'KNOWS', {});

const aliceNode = db.getNode<'Person'>(alice);
console.log('Alice:', aliceNode);
console.assert(aliceNode?.properties.name === 'Alice', 'Name should be Alice');
console.assert(aliceNode?.properties.age === 30, 'Age should be 30');

const worksAtEdge = db.getEdge<'WORKS_AT'>(edge1);
console.log('WORKS_AT edge:', worksAtEdge);
console.assert(worksAtEdge?.properties.since === 2020, 'Since should be 2020');

const people = db.getNodesByLabel('Person');
console.log('People:', people);
console.assert(people.length === 2, 'Should have 2 people');

const foundAlice = db.findNodeByProperty('Person', 'name', 'Alice');
console.log('Found Alice:', foundAlice);
console.assert(foundAlice === alice, 'Should find Alice');

console.log('✓ All typed API tests passed!');

if (fs.existsSync(dbPath)) {
  fs.rmSync(dbPath);
}
