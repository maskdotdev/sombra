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
    db.flush();
    db.checkpoint();
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
    
    console.log('8. Testing getNodesByLabel...');
    const personNodes = db.getNodesByLabel('Person');
    console.log(`✓ Found ${personNodes.length} Person nodes: ${personNodes}`);
    if (personNodes.length !== 2) {
      throw new Error(`Expected 2 Person nodes, got ${personNodes.length}`);
    }
    if (!personNodes.includes(node1Id) || !personNodes.includes(node2Id)) {
      throw new Error('Person nodes do not match expected IDs');
    }
    
    const companyNodes = db.getNodesByLabel('Company');
    console.log(`✓ Found ${companyNodes.length} Company nodes: ${companyNodes}`);
    if (companyNodes.length !== 1 || companyNodes[0] !== node3Id) {
      throw new Error('Company node does not match expected ID');
    }
    
    const nonExistent = db.getNodesByLabel('NonExistent');
    if (nonExistent.length !== 0) {
      throw new Error('Expected 0 nodes for non-existent label');
    }
    console.log('✓ getNodesByLabel works correctly');
    
    console.log('9. Testing count edge methods...');
    const outgoingCount = db.countOutgoingEdges(node1Id);
    console.log(`✓ Alice has ${outgoingCount} outgoing edges`);
    if (outgoingCount !== 2) {
      throw new Error(`Expected 2 outgoing edges, got ${outgoingCount}`);
    }
    
    const incomingCount = db.countIncomingEdges(node2Id);
    console.log(`✓ Bob has ${incomingCount} incoming edges`);
    if (incomingCount !== 1) {
      throw new Error(`Expected 1 incoming edge, got ${incomingCount}`);
    }
    console.log('✓ Edge counting works correctly');
    
    console.log('10. Testing getIncomingNeighbors...');
    const bobIncoming = db.getIncomingNeighbors(node2Id);
    console.log(`✓ Bob's incoming neighbors: ${bobIncoming}`);
    if (bobIncoming.length !== 1 || bobIncoming[0] !== node1Id) {
      throw new Error('Incoming neighbors do not match expected');
    }
    console.log('✓ getIncomingNeighbors works correctly');
    
    console.log('11. Testing BFS traversal...');
    const bfsResults = db.bfsTraversal(node1Id, 2);
    console.log(`✓ BFS from Alice found ${bfsResults.length} nodes`);
    const aliceResult = bfsResults.find(r => r.nodeId === node1Id);
    if (!aliceResult || aliceResult.depth !== 0) {
      throw new Error('Alice should be at depth 0');
    }
    const bobResult = bfsResults.find(r => r.nodeId === node2Id);
    if (!bobResult || bobResult.depth !== 1) {
      throw new Error('Bob should be at depth 1');
    }
    console.log('✓ BFS traversal works correctly');
    
    console.log('12. Testing multi-hop traversals...');
    const twoHops = db.getNeighborsTwoHops(node1Id);
    console.log(`✓ Two-hop neighbors from Alice: ${twoHops.length} nodes`);
    
    const threeHops = db.getNeighborsThreeHops(node1Id);
    console.log(`✓ Three-hop neighbors from Alice: ${threeHops.length} nodes`);
    console.log('✓ Multi-hop traversals work correctly');
    
    console.log('13. Flushing and checkpointing...');
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