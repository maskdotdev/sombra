const { SombraDB } = require('../index.js');
const fs = require('fs');
const path = require('path');

const dbPath = path.join(__dirname, 'test-query-builder.db');

// Clean up any existing database
if (fs.existsSync(dbPath)) {
    fs.unlinkSync(dbPath);
}

try {
    // Create a new database
    const db = new SombraDB(dbPath);

    // Create test data: Functions and Files
    const func1 = db.addNode(['Function'], { name: { type: 'string', value: 'main' } });
    const func2 = db.addNode(['Function'], { name: { type: 'string', value: 'helper' } });
    const func3 = db.addNode(['Function'], { name: { type: 'string', value: 'utils' } });
    const file1 = db.addNode(['File'], { path: { type: 'string', value: '/src/main.js' } });
    
    // Create relationships
    const edge1 = db.addEdge(func1, func2, 'CALLS');
    const edge2 = db.addEdge(func2, func3, 'CALLS');

    console.log('✓ Created test data');

    // Test 1: Query from label with traversal
    console.log('\nTest 1: Query from label with traversal');
    const query1 = db.query();
    query1.startFromLabel('Function');
    query1.traverse(['CALLS'], 'outgoing', 2);
    const result1 = query1.execute();
    
    console.log('  Result:', {
        startNodes: result1.startNodes,
        nodeIds: result1.nodeIds,
        limited: result1.limited
    });
    
    if (result1.nodeIds.length > 0) {
        console.log('✓ Query from label executed successfully');
    } else {
        console.error('✗ Query returned no results');
        process.exit(1);
    }

    // Test 2: Query from explicit nodes
    console.log('\nTest 2: Query from explicit nodes');
    const query2 = db.query();
    query2.startFrom([func1]);
    query2.traverse(['CALLS'], 'outgoing', 1);
    const result2 = query2.execute();
    
    console.log('  Node IDs:', result2.nodeIds);
    
    if (result2.nodeIds.length > 0) {
        console.log('✓ Query from explicit node executed');
    } else {
        console.error('✗ Query returned no results');
        process.exit(1);
    }

    // Test 3: Query from property
    console.log('\nTest 3: Query from property');
    const query3 = db.query();
    query3.startFromProperty('Function', 'name', 'main');
    query3.traverse(['CALLS'], 'outgoing', 2);
    const result3 = query3.execute();
    
    console.log('  Node IDs:', result3.nodeIds);
    
    if (result3.nodeIds.length > 0) {
        console.log('✓ Query from property executed');
    } else {
        console.error('✗ Query returned no results');
        process.exit(1);
    }

    // Test 4: Query with limit
    console.log('\nTest 4: Query with limit');
    const query4 = db.query();
    query4.startFromLabel('Function');
    query4.limit(2);
    const result4 = query4.execute();
    
    console.log('  Node count:', result4.nodeIds.length);
    console.log('  Limited:', result4.limited);
    
    if (result4.nodeIds.length <= 2) {
        console.log('✓ Query with limit executed');
    } else {
        console.error('✗ Limit not applied correctly');
        process.exit(1);
    }

    console.log('\n✓ All tests passed!');

    // Cleanup
    if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath);
    }

} catch (error) {
    console.error('Error:', error);
    
    // Cleanup on error
    if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath);
    }
    
    process.exit(1);
}
