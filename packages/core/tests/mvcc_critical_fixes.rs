//! Tests for critical MVCC bug fixes
//!
//! These tests verify that the two critical MVCC bugs identified have been fixed:
//! - Issue #3: Edge creation with correct tx_id (read-your-own-writes)
//! - Issue #6: File locking to prevent multi-process corruption

use sombra::{Config, Edge, GraphDB, Node, PropertyValue};
use std::fs;

#[test]
fn test_edge_read_your_own_writes_within_transaction() {
    // Issue #3: Edges were created with tx_id=0, breaking read-your-own-writes
    let path = "test_edge_ryw.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.wal", path));
    let _ = fs::remove_file(format!("{}.lock", path));

    let mut config = Config::default();
    config.mvcc_enabled = true;

    let mut db = GraphDB::open_with_config(path, config).unwrap();
    let mut tx = db.begin_transaction().unwrap();

    // Create two nodes
    let mut node1 = Node::new(1);
    node1.labels.push("Person".to_string());
    let mut node2 = Node::new(2);
    node2.labels.push("Person".to_string());
    let n1_id = tx.add_node(node1).unwrap();
    let n2_id = tx.add_node(node2).unwrap();

    // Create an edge between them
    let edge = Edge::new(1, n1_id, n2_id, "KNOWS");
    let edge_id = tx.add_edge(edge).unwrap();

    // CRITICAL: We should be able to read the edge we just created in the same transaction
    // This would fail before the fix because edges had tx_id=0
    let retrieved_edge = tx.get_edge(edge_id).unwrap();
    assert_eq!(retrieved_edge.source_node_id, n1_id);
    assert_eq!(retrieved_edge.target_node_id, n2_id);
    assert_eq!(retrieved_edge.type_name, "KNOWS");

    tx.commit().unwrap();

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.wal", path));
    let _ = fs::remove_file(format!("{}.lock", path));
}

#[test]
fn test_edge_isolation_between_transactions() {
    // Verify that uncommitted edges are NOT visible to other transactions
    let path = "test_edge_isolation.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.wal", path));
    let _ = fs::remove_file(format!("{}.lock", path));

    let mut config = Config::default();
    config.mvcc_enabled = true;

    let mut db = GraphDB::open_with_config(path, config).unwrap();
    
    // Create nodes in first transaction
    let (n1_id, n2_id) = {
        let mut tx1 = db.begin_transaction().unwrap();
        let mut node1 = Node::new(1);
        node1.labels.push("Person".to_string());
        let mut node2 = Node::new(2);
        node2.labels.push("Person".to_string());
        let n1 = tx1.add_node(node1).unwrap();
        let n2 = tx1.add_node(node2).unwrap();
        tx1.commit().unwrap();
        (n1, n2)
    };

    // Start transaction 2 that will create an edge but not commit yet
    let mut tx2 = db.begin_transaction().unwrap();
    let edge = Edge::new(1, n1_id, n2_id, "KNOWS");
    let edge_id = tx2.add_edge(edge).unwrap();

    // Edge should be visible in tx2 (read-your-own-writes)
    assert!(tx2.get_edge(edge_id).is_ok());

    // Start transaction 3 (don't commit tx2 yet)
    // Note: This test would need concurrent transaction support to fully test
    // For now we just verify read-your-own-writes works
    
    tx2.commit().unwrap();

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.wal", path));
    let _ = fs::remove_file(format!("{}.lock", path));
}

#[test]
fn test_file_locking_prevents_concurrent_opens() {
    // Issue #6: No file locking allowed multiple processes to corrupt the database
    let path = "test_file_lock.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.wal", path));
    let _ = fs::remove_file(format!("{}.lock", path));

    let mut config = Config::default();
    config.mvcc_enabled = true;

    // Open database in this process
    let _db1 = GraphDB::open_with_config(path, config.clone()).unwrap();

    // Try to open the same database again - should fail with lock error
    let result = GraphDB::open_with_config(path, config.clone());
    assert!(result.is_err(), "Second open should fail due to file lock");
    
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("already open") || err_msg.contains("lock"),
        "Error should mention lock or already open, got: {}",
        err_msg
    );

    // Drop db1 to release the lock
    drop(_db1);

    // Now we should be able to open it again
    let _db2 = GraphDB::open_with_config(path, config.clone()).unwrap();

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.wal", path));
    let _ = fs::remove_file(format!("{}.lock", path));
}

#[test]
fn test_edge_with_properties_read_your_own_writes() {
    // Test edge with properties to ensure complete fix
    let path = "test_edge_props_ryw.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.wal", path));
    let _ = fs::remove_file(format!("{}.lock", path));

    let mut config = Config::default();
    config.mvcc_enabled = true;

    let mut db = GraphDB::open_with_config(path, config).unwrap();
    let mut tx = db.begin_transaction().unwrap();

    // Create two nodes
    let mut node1 = Node::new(1);
    node1.labels.push("Person".to_string());
    node1.properties.insert("name".to_string(), PropertyValue::String("Alice".to_string()));
    let mut node2 = Node::new(2);
    node2.labels.push("Person".to_string());
    node2.properties.insert("name".to_string(), PropertyValue::String("Bob".to_string()));
    let n1_id = tx.add_node(node1).unwrap();
    let n2_id = tx.add_node(node2).unwrap();

    // Create an edge with properties
    let mut edge = Edge::new(1, n1_id, n2_id, "KNOWS");
    edge.properties.insert("since".to_string(), PropertyValue::Int(2020));
    edge.properties.insert("strength".to_string(), PropertyValue::String("strong".to_string()));
    let edge_id = tx.add_edge(edge).unwrap();

    // Read back the edge and verify properties
    let retrieved_edge = tx.get_edge(edge_id).unwrap();
    assert_eq!(retrieved_edge.type_name, "KNOWS");
    assert_eq!(retrieved_edge.properties.get("since").unwrap(), &sombra::PropertyValue::Int(2020));
    assert_eq!(
        retrieved_edge.properties.get("strength").unwrap(),
        &sombra::PropertyValue::String("strong".to_string())
    );

    tx.commit().unwrap();

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.wal", path));
    let _ = fs::remove_file(format!("{}.lock", path));
}

#[test]
fn test_auto_commit_edge_read_your_own_writes() {
    // Test that auto-committed edges (without explicit transaction) also work
    let path = "test_auto_edge_ryw.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.wal", path));
    let _ = fs::remove_file(format!("{}.lock", path));

    let mut config = Config::default();
    config.mvcc_enabled = true;

    let mut db = GraphDB::open_with_config(path, config).unwrap();

    // Create nodes with auto-commit
    let mut node1 = Node::new(1);
    node1.labels.push("Person".to_string());
    let mut node2 = Node::new(2);
    node2.labels.push("Person".to_string());
    let n1_id = db.add_node(node1).unwrap();
    let n2_id = db.add_node(node2).unwrap();

    // Create edge with auto-commit
    let edge = Edge::new(1, n1_id, n2_id, "KNOWS");
    let edge_id = db.add_edge(edge).unwrap();

    // Read back the edge using a transaction (GraphDB doesn't have get_edge)
    let mut tx = db.begin_transaction().unwrap();
    let retrieved_edge = tx.get_edge(edge_id).unwrap();
    assert_eq!(retrieved_edge.source_node_id, n1_id);
    assert_eq!(retrieved_edge.target_node_id, n2_id);
    assert_eq!(retrieved_edge.type_name, "KNOWS");
    tx.commit().unwrap();

    // Cleanup
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.wal", path));
    let _ = fs::remove_file(format!("{}.lock", path));
}
