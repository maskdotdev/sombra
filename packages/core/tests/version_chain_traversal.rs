//! Tests for version chain traversal (Issue #1)
//!
//! These tests verify that the version chain implementation works correctly.
//! Note: Full testing of snapshot isolation with version chains requires concurrent
//! transactions, which the current API (&mut self) doesn't support. These tests
//! verify the version chain structure and basic version update behavior.

use sombra::{Config, GraphDB, Node, PropertyValue};
use std::fs;

fn cleanup_test_db(path: &str) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.wal", path));
    let _ = fs::remove_file(format!("{}.lock", path));
}

#[test]
fn test_node_updates_create_new_versions() {
    // Verify that updating a node creates a new version without errors
    let path = "test_version_updates.db";
    cleanup_test_db(path);

    let mut config = Config::default();
    config.mvcc_enabled = true;
    let mut db = GraphDB::open_with_config(path, config).unwrap();

    // Create initial node
    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(1);
        node.properties.insert("value".to_string(), PropertyValue::Int(100));
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // Update the node multiple times to create version chain
    for i in 1..=5 {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties.insert("value".to_string(), PropertyValue::Int(100 + i * 10));
        tx.add_node(node).unwrap(); // Creates new version
        tx.commit().unwrap();
    }

    // Verify final value
    {
        let mut tx = db.begin_transaction().unwrap();
        let node = tx.get_node(node_id).unwrap().unwrap();
        assert_eq!(
            node.properties.get("value"),
            Some(&PropertyValue::Int(150)),
            "Should see latest value after all updates"
        );
        tx.commit().unwrap();
    }

    cleanup_test_db(path);
}

#[test]
fn test_multiple_nodes_independent_updates() {
    // Verify that different nodes can be updated independently
    let path = "test_independent_updates.db";
    cleanup_test_db(path);

    let mut config = Config::default();
    config.mvcc_enabled = true;
    let mut db = GraphDB::open_with_config(path, config).unwrap();

    // Create two nodes
    let (node1_id, node2_id) = {
        let mut tx = db.begin_transaction().unwrap();
        let mut n1 = Node::new(1);
        n1.properties.insert("id".to_string(), PropertyValue::String("node1".to_string()));
        n1.properties.insert("value".to_string(), PropertyValue::Int(1));
        let mut n2 = Node::new(2);
        n2.properties.insert("id".to_string(), PropertyValue::String("node2".to_string()));
        n2.properties.insert("value".to_string(), PropertyValue::Int(1));
        let id1 = tx.add_node(n1).unwrap();
        let id2 = tx.add_node(n2).unwrap();
        tx.commit().unwrap();
        (id1, id2)
    };

    // Update node1
    {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node1_id).unwrap().unwrap();
        node.properties.insert("value".to_string(), PropertyValue::Int(10));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // Update node2
    {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node2_id).unwrap().unwrap();
        node.properties.insert("value".to_string(), PropertyValue::Int(20));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // Verify both nodes have correct values
    {
        let mut tx = db.begin_transaction().unwrap();
        let n1 = tx.get_node(node1_id).unwrap().unwrap();
        let n2 = tx.get_node(node2_id).unwrap().unwrap();
        assert_eq!(n1.properties.get("value"), Some(&PropertyValue::Int(10)));
        assert_eq!(n2.properties.get("value"), Some(&PropertyValue::Int(20)));
        tx.commit().unwrap();
    }

    cleanup_test_db(path);
}

#[test]
fn test_version_chain_after_db_reopen() {
    // Verify version chains persist across database close/reopen
    let path = "test_version_chain_persist.db";
    cleanup_test_db(path);

    let node_id = {
        let mut config = Config::default();
        config.mvcc_enabled = true;
        let mut db = GraphDB::open_with_config(path, config).unwrap();

        // Create node with initial value
        let id = {
            let mut tx = db.begin_transaction().unwrap();
            let mut node = Node::new(1);
            node.properties.insert("value".to_string(), PropertyValue::Int(1));
            let id = tx.add_node(node).unwrap();
            tx.commit().unwrap();
            id
        };

        // Update it multiple times
        for i in 2..=5 {
            let mut tx = db.begin_transaction().unwrap();
            let mut node = tx.get_node(id).unwrap().unwrap();
            node.properties.insert("value".to_string(), PropertyValue::Int(i));
            tx.add_node(node).unwrap();
            tx.commit().unwrap();
        }

        id
    };

    // Reopen database
    {
        let mut config = Config::default();
        config.mvcc_enabled = true;
        let mut db = GraphDB::open_with_config(path, config).unwrap();

        // Should see latest version
        let mut tx = db.begin_transaction().unwrap();
        let node = tx.get_node(node_id).unwrap().unwrap();
        assert_eq!(
            node.properties.get("value"),
            Some(&PropertyValue::Int(5)),
            "Should see latest version after reopen"
        );
        tx.commit().unwrap();
    }

    cleanup_test_db(path);
}

#[test]
fn test_read_your_own_writes_with_updates() {
    // Verify read-your-own-writes works with node updates
    let path = "test_ryw_updates.db";
    cleanup_test_db(path);

    let mut config = Config::default();
    config.mvcc_enabled = true;
    let mut db = GraphDB::open_with_config(path, config).unwrap();

    let mut tx = db.begin_transaction().unwrap();

    // Create a node
    let mut node = Node::new(1);
    node.properties.insert("value".to_string(), PropertyValue::Int(1));
    let node_id = tx.add_node(node).unwrap();

    // Read it back (read-your-own-writes)
    let node = tx.get_node(node_id).unwrap().unwrap();
    assert_eq!(node.properties.get("value"), Some(&PropertyValue::Int(1)));

    // Update it
    let mut node = tx.get_node(node_id).unwrap().unwrap();
    node.properties.insert("value".to_string(), PropertyValue::Int(2));
    tx.add_node(node).unwrap();

    // Read updated value (read-your-own-writes)
    let node = tx.get_node(node_id).unwrap().unwrap();
    assert_eq!(node.properties.get("value"), Some(&PropertyValue::Int(2)));

    tx.commit().unwrap();

    // Verify committed value
    {
        let mut tx = db.begin_transaction().unwrap();
        let node = tx.get_node(node_id).unwrap().unwrap();
        assert_eq!(node.properties.get("value"), Some(&PropertyValue::Int(2)));
        tx.commit().unwrap();
    }

    cleanup_test_db(path);
}
