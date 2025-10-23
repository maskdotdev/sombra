const { SombraDB } = require('../index');
const path = require('path');
const fs = require('fs');

async function test() {
  console.log('Testing Sombra Hierarchy API Node.js bindings...');
  
  const dbPath = path.join(__dirname, 'test-hierarchy.db');
  
  if (fs.existsSync(dbPath)) {
    fs.unlinkSync(dbPath);
  }
  
  try {
    console.log('1. Opening database...');
    const db = new SombraDB(dbPath);
    console.log('✓ Database opened successfully');
    
    console.log('2. Creating hierarchy structure...');
    const file = db.addNode(['File'], { name: { type: 'string', value: 'main.js' } });
    const func1 = db.addNode(['Function'], { name: { type: 'string', value: 'processData' } });
    const func2 = db.addNode(['Function'], { name: { type: 'string', value: 'helper' } });
    const block1 = db.addNode(['Block'], { name: { type: 'string', value: 'if-block' } });
    const block2 = db.addNode(['Block'], { name: { type: 'string', value: 'loop-block' } });
    const stmt1 = db.addNode(['Statement'], { name: { type: 'string', value: 'return' } });
    const stmt2 = db.addNode(['Statement'], { name: { type: 'string', value: 'call' } });
    
    db.addEdge(func1, file, 'PARENT');
    db.addEdge(func2, file, 'PARENT');
    db.addEdge(block1, func1, 'PARENT');
    db.addEdge(block2, func1, 'PARENT');
    db.addEdge(stmt1, block1, 'PARENT');
    db.addEdge(stmt2, block2, 'PARENT');
    
    db.flush();
    db.checkpoint();
    console.log('✓ Created hierarchy structure');
    
    console.log('3. Testing findAncestorByLabel...');
    const foundFunc = db.findAncestorByLabel(stmt1, 'Function', 'PARENT');
    if (foundFunc === func1) {
      console.log('✓ findAncestorByLabel found Function from Statement');
    } else {
      console.error('✗ findAncestorByLabel failed: expected', func1, 'got', foundFunc);
      process.exit(1);
    }
    
    const foundFile = db.findAncestorByLabel(stmt1, 'File', 'PARENT');
    if (foundFile === file) {
      console.log('✓ findAncestorByLabel found File from Statement');
    } else {
      console.error('✗ findAncestorByLabel failed: expected', file, 'got', foundFile);
      process.exit(1);
    }
    
    const notFound = db.findAncestorByLabel(stmt1, 'NonExistent', 'PARENT');
    if (notFound === null) {
      console.log('✓ findAncestorByLabel returns null when not found');
    } else {
      console.error('✗ findAncestorByLabel should return null for non-existent label');
      process.exit(1);
    }
    
    console.log('4. Testing getAncestors...');
    const ancestors = db.getAncestors(stmt1, 'PARENT');
    if (ancestors.length === 3 && ancestors.includes(block1) && ancestors.includes(func1) && ancestors.includes(file)) {
      console.log('✓ getAncestors found all ancestors:', ancestors);
    } else {
      console.error('✗ getAncestors failed: expected [block1, func1, file], got', ancestors);
      process.exit(1);
    }
    
    const ancestorsDepth2 = db.getAncestors(stmt1, 'PARENT', 2);
    if (ancestorsDepth2.length === 2 && ancestorsDepth2.includes(block1) && ancestorsDepth2.includes(func1)) {
      console.log('✓ getAncestors with max_depth=2:', ancestorsDepth2);
    } else {
      console.error('✗ getAncestors with max_depth failed: expected [block1, func1], got', ancestorsDepth2);
      process.exit(1);
    }
    
    console.log('5. Testing getDescendants...');
    const descendants = db.getDescendants(func1, 'PARENT');
    if (descendants.length === 3 && descendants.includes(block1) && descendants.includes(block2) && descendants.includes(stmt1)) {
      console.log('✓ getDescendants found all descendants:', descendants);
    } else {
      console.error('✗ getDescendants failed, got', descendants);
      process.exit(1);
    }
    
    const descendantsDepth1 = db.getDescendants(func1, 'PARENT', 1);
    if (descendantsDepth1.length === 2 && descendantsDepth1.includes(block1) && descendantsDepth1.includes(block2)) {
      console.log('✓ getDescendants with max_depth=1:', descendantsDepth1);
    } else {
      console.error('✗ getDescendants with max_depth failed: expected [block1, block2], got', descendantsDepth1);
      process.exit(1);
    }
    
    console.log('6. Testing getContainingFile...');
    const containingFile = db.getContainingFile(stmt1);
    if (containingFile === file) {
      console.log('✓ getContainingFile found File from Statement');
    } else {
      console.error('✗ getContainingFile failed: expected', file, 'got', containingFile);
      process.exit(1);
    }
    
    console.log('\n✓ All hierarchy API tests passed!');
    
  } catch (error) {
    console.error('✗ Test failed:', error);
    process.exit(1);
  } finally {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
  }
}

test();
