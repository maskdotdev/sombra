import { SombraDB, SombraTransaction } from '../index';
import * as path from 'path';
import * as fs from 'fs';

function testTypeScriptCompatibility() {
  const dbPath = path.join(__dirname, 'test-ts-compatibility.db');
  
  if (fs.existsSync(dbPath)) {
    fs.unlinkSync(dbPath);
  }
  
  try {
    console.log('Testing TypeScript type compatibility...');
    
    const db = new SombraDB(dbPath);
    
    const nodeId = db.addNode(['Test'], { 
      name: { type: 'string', value: 'test' } 
    });
    
    console.log(`✓ Can call addNode() without 'as any' cast`);
    console.log(`✓ Node ID type: ${typeof nodeId}`);
    
    const node = db.getNode(nodeId);
    console.log(`✓ Can call getNode() without 'as any' cast`);
    console.log(`✓ Node: ${JSON.stringify(node, null, 2)}`);
    
    const neighbors = db.getNeighbors(nodeId);
    console.log(`✓ Can call getNeighbors() without 'as any' cast`);
    
    const tx: SombraTransaction = db.beginTransaction();
    console.log(`✓ Transaction type is correct`);
    
    const txNode = tx.addNode(['TxTest'], { 
      value: { type: 'int', value: 42 } 
    });
    console.log(`✓ Can call tx.addNode() without 'as any' cast`);
    
    tx.commit();
    console.log(`✓ Can call tx.commit() without 'as any' cast`);
    
    const bfsResults = db.bfsTraversal(nodeId, 2);
    console.log(`✓ Can call bfsTraversal() without 'as any' cast`);
    console.log(`✓ BFS results type is correct: Array length ${bfsResults.length}`);
    
    console.log('\n✓ All TypeScript type checks passed - no casting needed!');
    
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
    
  } catch (error) {
    console.error('✗ TypeScript compatibility test failed:', error);
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
    process.exit(1);
  }
}

testTypeScriptCompatibility();
