// Minimal test to reproduce page corruption bug

use sombra::db::{Config, GraphDB};
use sombra::model::{Node, PropertyValue};
use tempfile::TempDir;

#[test]
fn test_page_corruption_scenario() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("corruption_test.db");

    let mut config = Config::benchmark();
    config.mvcc_enabled = true;
    config.max_concurrent_transactions = Some(200);
    config.gc_interval_secs = None;

    let mut db = GraphDB::open_with_config(path.to_str().unwrap(), config).unwrap();

    // Create initial graph
    let mut node_ids = Vec::new();
    for i in 0..100 {
        let mut node = Node::new(0);
        node.labels.push("TestNode".to_string());
        node.properties
            .insert("index".to_string(), PropertyValue::Int(i as i64));
        let node_id = db.add_node(node).unwrap();
        node_ids.push(node_id);
    }

    db.checkpoint().unwrap();
    eprintln!("Initial checkpoint done");

    // Create version chains - this is where corruption happens at depth=5
    for depth in 0..=5 {
        eprintln!("\nCreating version chain depth {depth}");

        let mut tx = db.begin_transaction().unwrap();
        for &node_id in &node_ids {
            if let Ok(Some(mut node)) = tx.get_node(node_id) {
                node.properties
                    .insert("counter".to_string(), PropertyValue::Int(depth as i64));
                tx.add_node(node).ok();
            }
        }
        tx.commit().unwrap();

        eprintln!("Committed version chain depth {depth}");
    }

    eprintln!("\nReading nodes after version chains created (1000 iterations)");

    // Try to read - this is where corruption is detected
    // Match the benchmark: 1000 iterations of 100 reads
    for iteration in 0..1000 {
        let mut tx = db.begin_transaction().unwrap();
        for &node_id in &node_ids {
            let result = tx.get_node(node_id);
            if result.is_err() {
                eprintln!("ERROR at iteration {iteration} reading node {node_id}: {result:?}");
                panic!("Corruption detected!");
            }
        }
        tx.commit().unwrap();
    }

    eprintln!("Test completed successfully - no corruption");
}

#[test]
fn test_mixed_page_types_with_indexes() {
    // This test verifies that the page corruption fix correctly handles
    // mixed page types (RecordPages, BTree index pages, Property index pages)
    // when insert_new_slot() scans for available space.

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("mixed_pages_test.db");

    let mut config = Config::benchmark();
    config.mvcc_enabled = true;

    let mut db = GraphDB::open_with_config(path.to_str().unwrap(), config).unwrap();

    // Create nodes with properties that will be indexed
    let mut node_ids = Vec::new();
    for i in 0..50 {
        let mut node = Node::new(0);
        node.labels.push("Person".to_string());
        node.properties
            .insert("age".to_string(), PropertyValue::Int(20 + (i % 30) as i64));
        node.properties.insert(
            "name".to_string(),
            PropertyValue::String(format!("Person_{i}")),
        );
        let node_id = db.add_node(node).unwrap();
        node_ids.push(node_id);
    }

    // Create property index - this will persist PIDX pages on checkpoint
    db.create_property_index("Person", "age").unwrap();

    // Checkpoint to persist indexes (creates PIDX pages and BIDX pages)
    db.checkpoint().unwrap();
    eprintln!("Checkpoint created - PIDX and BIDX pages now exist");

    // Now insert more nodes - this triggers insert_new_slot() which will scan
    // through pages including the PIDX and BIDX pages. Without the fix, this
    // would cause false corruption errors.
    for i in 50..100 {
        let mut node = Node::new(0);
        node.labels.push("Person".to_string());
        node.properties
            .insert("age".to_string(), PropertyValue::Int(20 + (i % 30) as i64));
        node.properties.insert(
            "name".to_string(),
            PropertyValue::String(format!("Person_{i}")),
        );

        // This should succeed without corruption errors
        let node_id = db.add_node(node).unwrap();
        node_ids.push(node_id);
    }

    // Verify property index still works after mixed page allocation
    let results = db
        .find_nodes_by_property("Person", "age", &PropertyValue::Int(25))
        .unwrap();

    eprintln!("Found {} nodes with age=25", results.len());
    assert!(!results.is_empty(), "Property index should return results");

    // Verify all nodes are readable
    for &node_id in &node_ids {
        let node = db.get_node(node_id).unwrap();
        assert!(node.is_some(), "Node {node_id} should be readable");
    }

    eprintln!("Test completed - mixed page types handled correctly");
}
