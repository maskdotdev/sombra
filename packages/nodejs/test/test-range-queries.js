const { SombraDB } = require('../index');
const path = require('path');
const fs = require('fs');

async function test() {
  console.log('Testing BTree Range Query Features...\n');
  
  const dbPath = path.join(__dirname, 'test-range.db');
  
  if (fs.existsSync(dbPath)) {
    fs.unlinkSync(dbPath);
  }
  
  try {
    const db = new SombraDB(dbPath);
    
    console.log('1. Creating test nodes...');
    const nodeIds = [];
    for (let i = 0; i < 10; i++) {
      const id = db.addNode(['Test'], { 
        value: { type: 'int', value: i } 
      });
      nodeIds.push(id);
    }
    db.flush();
    console.log(`✓ Created 10 nodes with IDs: ${nodeIds.join(', ')}\n`);
    
    console.log('2. Testing get_nodes_in_range...');
    const node3 = nodeIds[2];
    const node7 = nodeIds[6];
    const rangeNodes = db.getNodesInRange(node3, node7);
    console.log(`✓ Nodes in range [${node3}, ${node7}]: ${rangeNodes.join(', ')}`);
    console.log(`  Expected to include nodes from index 2-6 (5 nodes)`);
    console.log(`  Got ${rangeNodes.length} nodes\n`);
    
    console.log('3. Testing get_nodes_from...');
    const fromNode = nodeIds[7];
    const nodesFrom = db.getNodesFrom(fromNode);
    console.log(`✓ Nodes from ${fromNode}: ${nodesFrom.join(', ')}`);
    console.log(`  Expected to include nodes from index 7+ (3 nodes)`);
    console.log(`  Got ${nodesFrom.length} nodes\n`);
    
    console.log('4. Testing get_nodes_to...');
    const toNode = nodeIds[3];
    const nodesTo = db.getNodesTo(toNode);
    console.log(`✓ Nodes to ${toNode}: ${nodesTo.join(', ')}`);
    console.log(`  Expected to include nodes from start to index 3 (4 nodes)`);
    console.log(`  Got ${nodesTo.length} nodes\n`);
    
    console.log('5. Testing get_first_node...');
    const firstNode = db.getFirstNode();
    console.log(`✓ First node: ${firstNode}`);
    console.log(`  Expected: ${nodeIds[0]}\n`);
    
    console.log('6. Testing get_last_node...');
    const lastNode = db.getLastNode();
    console.log(`✓ Last node: ${lastNode}`);
    console.log(`  Expected: ${nodeIds[9]}\n`);
    
    console.log('7. Testing get_first_n_nodes...');
    const first3 = db.getFirstNNodes(3);
    console.log(`✓ First 3 nodes: ${first3.join(', ')}`);
    console.log(`  Expected: ${nodeIds.slice(0, 3).join(', ')}\n`);
    
    console.log('8. Testing get_last_n_nodes...');
    const last3 = db.getLastNNodes(3);
    console.log(`✓ Last 3 nodes: ${last3.join(', ')}`);
    console.log(`  Expected (reversed): ${nodeIds.slice(-3).reverse().join(', ')}\n`);
    
    console.log('9. Testing get_all_node_ids_ordered...');
    const allOrdered = db.getAllNodeIdsOrdered();
    console.log(`✓ All nodes ordered: ${allOrdered.join(', ')}`);
    console.log(`  Total: ${allOrdered.length} nodes`);
    console.log(`  Nodes are in sorted order: ${allOrdered.every((id, i) => i === 0 || id > allOrdered[i-1])}\n`);
    
    console.log('10. Testing with transaction...');
    const tx = db.beginTransaction();
    const txNode = tx.addNode(['TxTest'], { value: { type: 'int', value: 100 } });
    console.log(`✓ Added node ${txNode} in transaction`);
    
    const txRangeNodes = tx.getNodesInRange(nodeIds[0], nodeIds[4]);
    console.log(`✓ Transaction range query returned ${txRangeNodes.length} nodes`);
    
    const txFirstNode = tx.getFirstNode();
    console.log(`✓ Transaction first node: ${txFirstNode}`);
    
    tx.commit();
    console.log('✓ Transaction committed\n');
    
    console.log('✅ All range query tests passed!');
    
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
    
  } catch (error) {
    console.error('❌ Test failed:', error);
    process.exit(1);
  }
}

test();
