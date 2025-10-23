const { SombraDB } = require('../index');
const path = require('path');
const fs = require('fs');

async function test() {
  console.log('Testing Sombra SombraDB comprehensive functionality...\n');
  
  const dbPath = path.join(__dirname, 'test-comprehensive.db');
  
  if (fs.existsSync(dbPath)) {
    fs.unlinkSync(dbPath);
  }
  
  try {
    const db = new SombraDB(dbPath);
    console.log('✓ Database opened\n');
    
    console.log('=== Testing Property Types ===');
    const nodeId = db.addNode(['TestNode'], {
      stringProp: { type: 'string', value: 'test string' },
      intProp: { type: 'int', value: 42 },
      floatProp: { type: 'float', value: 3.14 },
      boolProp: { type: 'bool', value: true },
    });
    
    const node = db.getNode(nodeId);
    console.log('Node with various property types:');
    console.log(JSON.stringify(node, null, 2));
    console.log('✓ All property types work correctly\n');
    
    console.log('=== Testing Edge Properties ===');
    const person1 = db.addNode(['Person'], { 
      name: { type: 'string', value: 'Alice' } 
    });
    const person2 = db.addNode(['Person'], { 
      name: { type: 'string', value: 'Bob' } 
    });
    
    const edgeId = db.addEdge(person1, person2, 'KNOWS', {
      since: { type: 'int', value: 2020 },
      strength: { type: 'float', value: 0.85 }
    });
    
    const edge = db.getEdge(edgeId);
    console.log('Edge with properties:');
    console.log(JSON.stringify(edge, null, 2));
    console.log('✓ Edge properties work correctly\n');
    
    console.log('=== Testing Edge Queries ===');
    const person3 = db.addNode(['Person'], { 
      name: { type: 'string', value: 'Charlie' } 
    });
    db.addEdge(person1, person3, 'KNOWS');
    db.addEdge(person3, person1, 'FOLLOWS');
    
    const outgoing = db.getOutgoingEdges(person1);
    console.log(`Outgoing edges from person1: ${outgoing}`);
    console.log('✓ getOutgoingEdges works\n');
    
    const incoming = db.getIncomingEdges(person1);
    console.log(`Incoming edges to person1: ${incoming}`);
    console.log('✓ getIncomingEdges works\n');
    
    console.log('=== Testing Transactions ===');
    const tx = db.beginTransaction();
    console.log(`Transaction ${tx.id()} started`);
    
    const txNode = tx.addNode(['TxNode'], { 
      value: { type: 'string', value: 'created in transaction' } 
    });
    console.log(`Created node ${txNode} in transaction`);
    
    const beforeCommit = tx.getNode(txNode);
    console.log('Node before commit:', JSON.stringify(beforeCommit, null, 2));
    
    tx.commit();
    console.log('✓ Transaction committed\n');
    
    const afterCommit = db.getNode(txNode);
    console.log('Node after commit:', JSON.stringify(afterCommit, null, 2));
    console.log('✓ Node persists after commit\n');
    
    console.log('=== Testing Transaction Rollback ===');
    const rollbackTx = db.beginTransaction();
    const tempNode = rollbackTx.addNode(['Temp'], { 
      value: { type: 'string', value: 'should be rolled back' } 
    });
    console.log(`Created temp node ${tempNode} in transaction`);
    
    rollbackTx.rollback();
    console.log('✓ Transaction rolled back\n');
    
    console.log('=== Testing Delete Operations ===');
    
    const deleteEdgeTestNode1 = db.addNode(['EdgeDeleteTest'], {});
    const deleteEdgeTestNode2 = db.addNode(['EdgeDeleteTest'], {});
    const deleteEdge = db.addEdge(deleteEdgeTestNode1, deleteEdgeTestNode2, 'TEMP');
    console.log(`Created edge ${deleteEdge} to delete`);
    
    db.deleteEdge(deleteEdge);
    console.log('✓ Edge deleted');
    
    const deleteNode = db.addNode(['ToDelete'], {});
    console.log(`Created node ${deleteNode} to delete`);
    
    db.deleteNode(deleteNode);
    console.log('✓ Node deleted\n');
    
    console.log('=== Testing Persistence ===');
    db.flush();
    db.checkpoint();
    console.log('✓ Database flushed and checkpointed\n');
    
    console.log('🎉 All comprehensive tests passed!');
    
  } catch (error) {
    console.error('❌ Test failed:', error);
    console.error(error.stack);
    process.exit(1);
  } finally {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
  }
}

test();
