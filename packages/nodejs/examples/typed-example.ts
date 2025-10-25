import { SombraDB } from '../typed';

interface MyGraphSchema {
  nodes: {
    Person: {
      name: string;
      age: number;
    };
    Company: {
      name: string;
      employees: number;
    };
    City: {
      name: string;
      state: string;
    };
    Pet: {
      name: string;
      species: string;
    };
  };
  edges: {
    KNOWS: {
      from: 'Person';
      to: 'Person';
      properties: {
        since: number;
      };
    };
    WORKS_AT: {
      from: 'Person';
      to: 'Company';
      properties: {
        role: string;
      };
    };
    LIVES_IN: {
      from: 'Person';
      to: 'City';
      properties: {};
    };
    OWNS: {
      from: 'Person';
      to: 'Pet';
      properties: {
        since: number;
      };
    };
    PARENT_OF: {
      from: 'Person';
      to: 'Person';
      properties: {
        since: number;
      };
    };
    MARRIED_TO: {
      from: 'Person';
      to: 'Person';
      properties: {
        since: number;
      };
    };
  };
}

const db = new SombraDB<MyGraphSchema>('./typed-example.db');

console.log('=== Type-Safe SombraDB Demo ===\n');

const fabian = db.addNode('Person', {
  name: 'Fabian',
  age: 32,
});

const michelle = db.addNode('Person', {
  name: 'Michelle',
  age: 33,
});

const levi = db.addNode('Person', {
  name: 'Levi',
  age: 4,
});

const sarah = db.addNode('Person', {
  name: 'Sarah',
  age: 29,
});

const daniel = db.addNode('Person', {
  name: 'Daniel',
  age: 31,
});

const carlos = db.addNode('Person', {
  name: 'Carlos',
  age: 28,
});

const auroraTech = db.addNode('Company', {
  name: 'AuroraTech',
  employees: 250,
});

const omniCorp = db.addNode('Company', {
  name: 'OmniCorp',
  employees: 1200,
});

const austin = db.addNode('City', {
  name: 'Austin',
  state: 'TX',
});

const mochi = db.addNode('Pet', {
  name: 'Mochi',
  species: 'Dog',
});

console.log('✅ Created nodes with full type safety!\n');

db.addEdge(fabian, michelle, 'KNOWS', { since: 2015 });
db.addEdge(fabian, levi, 'PARENT_OF', { since: 2021 });
db.addEdge(michelle, levi, 'PARENT_OF', { since: 2021 });
db.addEdge(fabian, michelle, 'MARRIED_TO', { since: 2018 });
db.addEdge(fabian, sarah, 'KNOWS', { since: 2022 });
db.addEdge(sarah, daniel, 'KNOWS', { since: 2023 });
db.addEdge(carlos, fabian, 'KNOWS', { since: 2024 });

db.addEdge(fabian, auroraTech, 'WORKS_AT', { role: 'Staff Software Engineer' });
db.addEdge(michelle, auroraTech, 'WORKS_AT', { role: 'Product Manager' });
db.addEdge(daniel, omniCorp, 'WORKS_AT', { role: 'Data Engineer' });

db.addEdge(fabian, austin, 'LIVES_IN', {});
db.addEdge(michelle, austin, 'LIVES_IN', {});
db.addEdge(carlos, austin, 'LIVES_IN', {});

db.addEdge(fabian, mochi, 'OWNS', { since: 2020 });

console.log('✅ Created edges with full type safety!\n');

console.log('=== Type-Safe Queries ===\n');

console.log('1. Find company by name (type-safe property query):');
const auroraId = db.findNodeByProperty('Company', 'name', 'AuroraTech');
const auroraNode = db.getNode<'Company'>(auroraId!);
console.log(`   Found: ${auroraNode?.properties.name} with ${auroraNode?.properties.employees} employees\n`);

console.log('2. Get all employees at AuroraTech (type-safe edge traversal):');
const employeeIds = db.getIncomingNeighbors(auroraId!);
console.log(`   Found ${employeeIds.length} employees:`);
for (const empId of employeeIds) {
  const emp = db.getNode<'Person'>(empId);
  console.log(`   - ${emp?.properties.name} (age ${emp?.properties.age})`);
}
console.log();

console.log('3. Using type-safe query builder:');
const result = db
  .query()
  .startFromLabel('Company')
  .traverse(['WORKS_AT'], 'incoming', 1)
  .execute();

console.log(`   Found ${result.nodeIds.length} total employees across all companies\n`);

console.log('4. Find all people in Austin:');
const austinResidents = db.getIncomingNeighbors(austin);
console.log(`   ${austinResidents.length} people live in Austin:`);
for (const personId of austinResidents) {
  const person = db.getNode(personId);
  console.log(`   - ${person?.properties.name}`);
}
console.log();

console.log('5. Analytics with type-safe labels:');
console.log(`   Total people: ${db.countNodesWithLabel('Person')}`);
console.log(`   Total companies: ${db.countNodesWithLabel('Company')}`);
console.log(`   Total WORKS_AT relationships: ${db.countEdgesWithType('WORKS_AT')}`);
console.log(`   Total KNOWS relationships: ${db.countEdgesWithType('KNOWS')}`);
console.log();

console.log('6. BFS traversal from Fabian (depth 2):');
const bfsResults = db.bfsTraversal(fabian, 2);
console.log(`   Reached ${bfsResults.length} nodes:`);
for (const { nodeId, depth } of bfsResults.slice(0, 5)) {
  const node = db.getNode(nodeId);
  console.log(`   - ${node?.properties.name || 'N/A'} (depth: ${depth})`);
}
console.log();

console.log('✅ All type-safe operations completed successfully!\n');

console.log('=== Type Safety Benefits ===');
console.log('- Autocomplete for all labels: Person, Company, City, Pet');
console.log('- Autocomplete for all edge types: KNOWS, WORKS_AT, LIVES_IN, OWNS, etc.');
console.log('- Property types enforced at compile time');
console.log('- No runtime type conversion needed from user perspective');
console.log('- Full IntelliSense support in VS Code/TypeScript editors');

db.flush();
