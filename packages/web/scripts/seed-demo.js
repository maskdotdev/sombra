const { SombraDB } = require('sombradb');
const path = require('path');
const fs = require('fs');

const dbPath = process.argv[2] || path.join(__dirname, '..', 'demo.db');

if (fs.existsSync(dbPath)) {
  fs.unlinkSync(dbPath);
  console.log(`Removed existing database: ${dbPath}`);
}

const db = new SombraDB(dbPath);

console.log('Seeding demo graph database...');

const alice = db.addNode(['Person'], { name: { type: 'string', value: 'Alice' }, age: { type: 'int', value: 30 }, role: { type: 'string', value: 'Engineer' } });
const bob = db.addNode(['Person'], { name: { type: 'string', value: 'Bob' }, age: { type: 'int', value: 28 }, role: { type: 'string', value: 'Designer' } });
const charlie = db.addNode(['Person'], { name: { type: 'string', value: 'Charlie' }, age: { type: 'int', value: 35 }, role: { type: 'string', value: 'Manager' } });
const diana = db.addNode(['Person'], { name: { type: 'string', value: 'Diana' }, age: { type: 'int', value: 32 }, role: { type: 'string', value: 'Product Manager' } });

const project1 = db.addNode(['Project'], { name: { type: 'string', value: 'Web App' }, status: { type: 'string', value: 'active' }, priority: { type: 'string', value: 'high' } });
const project2 = db.addNode(['Project'], { name: { type: 'string', value: 'Mobile App' }, status: { type: 'string', value: 'planning' }, priority: { type: 'string', value: 'medium' } });
const project3 = db.addNode(['Project'], { name: { type: 'string', value: 'API Service' }, status: { type: 'string', value: 'active' }, priority: { type: 'string', value: 'high' } });

const team1 = db.addNode(['Team'], { name: { type: 'string', value: 'Frontend Team' }, size: { type: 'int', value: 5 } });
const team2 = db.addNode(['Team'], { name: { type: 'string', value: 'Backend Team' }, size: { type: 'int', value: 4 } });

const file1 = db.addNode(['File'], { path: { type: 'string', value: '/src/main.js' }, language: { type: 'string', value: 'javascript' }, lines: { type: 'int', value: 250 } });
const file2 = db.addNode(['File'], { path: { type: 'string', value: '/src/utils.ts' }, language: { type: 'string', value: 'typescript' }, lines: { type: 'int', value: 180 } });
const file3 = db.addNode(['File'], { path: { type: 'string', value: '/api/server.rs' }, language: { type: 'string', value: 'rust' }, lines: { type: 'int', value: 420 } });

db.addEdge(alice, bob, 'WORKS_WITH', { since: { type: 'string', value: '2023-01-15' } });
db.addEdge(alice, charlie, 'REPORTS_TO', { since: { type: 'string', value: '2022-06-01' } });
db.addEdge(bob, charlie, 'REPORTS_TO', { since: { type: 'string', value: '2022-08-15' } });
db.addEdge(diana, charlie, 'COLLABORATES_WITH', { frequency: { type: 'string', value: 'daily' } });

db.addEdge(alice, team1, 'MEMBER_OF', { role: { type: 'string', value: 'lead' } });
db.addEdge(bob, team1, 'MEMBER_OF', { role: { type: 'string', value: 'member' } });
db.addEdge(charlie, team2, 'MEMBER_OF', { role: { type: 'string', value: 'lead' } });
db.addEdge(diana, team1, 'MEMBER_OF', { role: { type: 'string', value: 'member' } });

db.addEdge(alice, project1, 'WORKS_ON', { hours_per_week: { type: 'int', value: 40 } });
db.addEdge(bob, project1, 'WORKS_ON', { hours_per_week: { type: 'int', value: 30 } });
db.addEdge(charlie, project2, 'MANAGES', { budget: { type: 'int', value: 100000 } });
db.addEdge(diana, project2, 'WORKS_ON', { hours_per_week: { type: 'int', value: 20 } });
db.addEdge(alice, project3, 'WORKS_ON', { hours_per_week: { type: 'int', value: 10 } });

db.addEdge(project1, team1, 'OWNED_BY', {});
db.addEdge(project2, team1, 'OWNED_BY', {});
db.addEdge(project3, team2, 'OWNED_BY', {});

db.addEdge(file1, project1, 'PART_OF', {});
db.addEdge(file2, project1, 'PART_OF', {});
db.addEdge(file3, project3, 'PART_OF', {});

db.addEdge(alice, file1, 'AUTHORED', { commits: { type: 'int', value: 42 } });
db.addEdge(alice, file2, 'AUTHORED', { commits: { type: 'int', value: 18 } });
db.addEdge(charlie, file3, 'AUTHORED', { commits: { type: 'int', value: 67 } });
db.addEdge(bob, file1, 'REVIEWED', { reviews: { type: 'int', value: 12 } });

console.log('✅ Demo database seeded successfully!');
console.log(`📁 Database location: ${dbPath}`);
console.log('\n📊 Graph contents:');
console.log('   • 4 Person nodes (Alice, Bob, Charlie, Diana)');
console.log('   • 3 Project nodes (Web App, Mobile App, API Service)');
console.log('   • 2 Team nodes (Frontend, Backend)');
console.log('   • 3 File nodes (source code)');
console.log('   • 24+ edges with relationships');
console.log('\n🚀 Next steps:');
console.log(`   sombra web ${dbPath}`);
console.log('\n   Or set environment variable:');
console.log(`   SOMBRA_DB_PATH=${dbPath} npm run dev`);
