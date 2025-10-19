const { SombraDB } = require('../index');
const path = require('path');
const fs = require('fs');

async function test() {
  console.log('Testing Sombra SombraDB Node.js bindings...');
  
  // Create a temporary database file
  const dbPath = path.join(__dirname, 'test.db');
  
  // Clean up any existing test database
  if (fs.existsSync(dbPath)) {
    fs.unlinkSync(dbPath);
  }
  
  try {
    // Test basic operations
    console.log('1. Opening database...');
    const db = new SombraDB(dbPath);
    console.log('✓ Database opened successfully');
    
    console.log('2. Adding nodes...');
    const node1Id = db.addNode(['Person'], { 
      name: { type: 'string', value: 'Alice' }, 
      age: { type: 'int', value: 30 } 
    });
    const node2Id = db.addNode(['Person'], { 
      name: { type: 'string', value: 'Bob' }, 
      age: { type: 'int', value: 25 } 
    });
    const node3Id = db.addNode(['Company'], { 
      name: { type: 'string', value: 'TechCorp' } 
    });
    console.log(`✓ Added nodes: ${node1Id}, ${node2Id}, ${node3Id}`);
    
    console.log('3. Adding edges...');
    const edge1Id = db.addEdge(node1Id, node2Id, 'KNOWS');
    const edge2Id = db.addEdge(node1Id, node3Id, 'WORKS_FOR');
    console.log(`✓ Added edges: ${edge1Id}, ${edge2Id}`);
    
    console.log('4. Getting nodes...');
    const alice = db.getNode(node1Id);
    const bob = db.getNode(node2Id);
    console.log(`✓ Alice: ${JSON.stringify(alice, null, 2)}`);
    console.log(`✓ Bob: ${JSON.stringify(bob, null, 2)}`);
    
    console.log('5. Getting neighbors...');
    const aliceNeighbors = db.getNeighbors(node1Id);
    console.log(`✓ Alice's neighbors: ${aliceNeighbors}`);
    
    console.log('6. Testing transactions...');
    const tx = db.beginTransaction();
    console.log(`✓ Transaction ${tx.id()} started`);
    
    const txNodeId = tx.addNode(['City'], { name: { type: 'string', value: 'San Francisco' } });
    console.log(`✓ Added node in transaction: ${txNodeId}`);
    
    const txNode = tx.getNode(txNodeId);
    console.log(`✓ Retrieved node in transaction: ${JSON.stringify(txNode, null, 2)}`);
    
    tx.commit();
    console.log('✓ Transaction committed');
    
    console.log('7. Testing rollback...');
    const rollbackTx = db.beginTransaction();
    const rollbackNodeId = rollbackTx.addNode(['Temp'], { value: { type: 'string', value: 'test' } });
    rollbackTx.rollback();
    console.log('✓ Transaction rolled back');
    
    console.log('8. Flushing and checkpointing...');
    db.flush();
    db.checkpoint();
    console.log('✓ Database flushed and checkpointed');
    
    console.log('\n🎉 All tests passed! Sombra Node.js bindings are working correctly.');
    
  } catch (error) {
    console.error('❌ Test failed:', error);
    process.exit(1);
  } finally {
    // Clean up
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
  }
}

test();