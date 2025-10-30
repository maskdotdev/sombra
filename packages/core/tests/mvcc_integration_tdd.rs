//! Advanced MVCC Integration Tests (TDD Approach)
//!
//! These tests define the expected behavior for full MVCC integration:
//! - Snapshot isolation for concurrent readers
//! - Index visibility across snapshots
//! - Property index snapshot visibility
//! - Deleted node visibility
//! - GC preservation of active snapshots

use sombra::model::PropertyValue;
use sombra::{Config, GraphDB, Node};
use std::fs;

fn cleanup_test_db(path: &str) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}-wal"));
}

fn create_mvcc_db(path: &str) -> GraphDB {
    cleanup_test_db(path);
    let mut config = Config::default();
    GraphDB::open_with_config(path, config).unwrap()
}

#[test]
fn test_concurrent_readers_with_different_snapshots() {
    // Test multiple concurrent readers each seeing their own snapshot
    let path = "test_concurrent_readers_tdd.db";
    let mut db = create_mvcc_db(path);

    // Create initial node
    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(1);
        node.properties
            .insert("version".to_string(), PropertyValue::Int(1));
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // Start TX1 - it should see version 1
    let mut tx1 = db.begin_transaction().unwrap();
    let v1 = tx1.get_node(node_id).unwrap().unwrap();
    assert_eq!(
        v1.properties.get("version"),
        Some(&PropertyValue::Int(1))
    );

    // Update to version 2 (using add_node on existing node creates new version)
    {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("version".to_string(), PropertyValue::Int(2));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // Start TX2 - it should see version 2
    let mut tx2 = db.begin_transaction().unwrap();
    let v2 = tx2.get_node(node_id).unwrap().unwrap();
    assert_eq!(
        v2.properties.get("version"),
        Some(&PropertyValue::Int(2))
    );

    // Update to version 3
    {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("version".to_string(), PropertyValue::Int(3));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // Start TX3 - it should see version 3
    let mut tx3 = db.begin_transaction().unwrap();
    let v3 = tx3.get_node(node_id).unwrap().unwrap();
    assert_eq!(
        v3.properties.get("version"),
        Some(&PropertyValue::Int(3))
    );

    // Each reader should STILL see their original snapshot (snapshot isolation)
    let v1_check = tx1.get_node(node_id).unwrap().unwrap();
    assert_eq!(
        v1_check.properties.get("version"),
        Some(&PropertyValue::Int(1)),
        "TX1 should still see version 1 (snapshot isolation)"
    );

    let v2_check = tx2.get_node(node_id).unwrap().unwrap();
    assert_eq!(
        v2_check.properties.get("version"),
        Some(&PropertyValue::Int(2)),
        "TX2 should still see version 2 (snapshot isolation)"
    );

    let v3_check = tx3.get_node(node_id).unwrap().unwrap();
    assert_eq!(
        v3_check.properties.get("version"),
        Some(&PropertyValue::Int(3)),
        "TX3 should still see version 3 (snapshot isolation)"
    );

    tx1.commit().unwrap();
    tx2.commit().unwrap();
    tx3.commit().unwrap();

    cleanup_test_db(path);
}

#[test]
fn test_index_visibility_across_snapshots() {
    // Test that label indexes respect snapshot visibility
    let path = "test_index_visibility_tdd.db";
    let mut db = create_mvcc_db(path);

    // Create node with label "Person"
    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(1);
        node.labels.push("Person".to_string());
        node.properties
            .insert("name".to_string(), PropertyValue::String("Alice".to_string()));
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // T1 starts and queries by label
    let mut tx1 = db.begin_transaction().unwrap();
    let nodes_v1 = tx1.get_nodes_by_label("Person").unwrap();
    assert_eq!(nodes_v1.len(), 1);
    let node_v1 = tx1.get_node(nodes_v1[0]).unwrap().unwrap();
    assert_eq!(
        node_v1.properties.get("name"),
        Some(&PropertyValue::String("Alice".to_string()))
    );

    // T2 updates the node's property (creates new version)
    {
        let mut tx2 = db.begin_transaction().unwrap();
        let mut node = tx2.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("name".to_string(), PropertyValue::String("Bob".to_string()));
        tx2.add_node(node).unwrap();
        tx2.commit().unwrap();
    }

    // T1 should still see old version via index query (snapshot isolation)
    let nodes_v1_again = tx1.get_nodes_by_label("Person").unwrap();
    assert_eq!(nodes_v1_again.len(), 1);
    let node_v1_again = tx1.get_node(nodes_v1_again[0]).unwrap().unwrap();
    assert_eq!(
        node_v1_again.properties.get("name"),
        Some(&PropertyValue::String("Alice".to_string())),
        "Index query should return snapshot-visible version"
    );

    tx1.commit().unwrap();

    // T3 should see new version via index query
    let mut tx3 = db.begin_transaction().unwrap();
    let nodes_v2 = tx3.get_nodes_by_label("Person").unwrap();
    assert_eq!(nodes_v2.len(), 1);
    let node_v2 = tx3.get_node(nodes_v2[0]).unwrap().unwrap();
    assert_eq!(
        node_v2.properties.get("name"),
        Some(&PropertyValue::String("Bob".to_string()))
    );
    tx3.commit().unwrap();

    cleanup_test_db(path);
}

#[test]
fn test_deleted_node_visibility() {
    // Test that deleted nodes are not visible to new snapshots but visible to old ones
    let path = "test_deleted_node_visibility_tdd.db";
    let mut db = create_mvcc_db(path);

    // Create node
    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let node = Node::new(1);
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // T1 starts and sees the node
    let mut tx1 = db.begin_transaction().unwrap();
    let node_v1 = tx1.get_node(node_id).unwrap();
    assert!(node_v1.is_some(), "T1 should see the node");

    // T2 deletes the node
    {
        let mut tx2 = db.begin_transaction().unwrap();
        tx2.delete_node(node_id).unwrap();
        tx2.commit().unwrap();
    }

    // T1 should still see the node (snapshot isolation)
    let node_v1_again = tx1.get_node(node_id).unwrap();
    assert!(
        node_v1_again.is_some(),
        "T1 should still see deleted node in its snapshot (snapshot isolation)"
    );

    tx1.commit().unwrap();

    // T3 should NOT see the node
    let mut tx3 = db.begin_transaction().unwrap();
    let node_v2 = tx3.get_node(node_id).unwrap();
    assert!(
        node_v2.is_none(),
        "T3 should not see deleted node in new snapshot"
    );
    tx3.commit().unwrap();

    cleanup_test_db(path);
}

#[test]
fn test_gc_preserves_active_snapshots() {
    // Test that garbage collection doesn't remove versions visible to active snapshots
    let path = "test_gc_active_snapshots_tdd.db";
    let mut db = create_mvcc_db(path);

    // Create initial node
    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(1);
        node.properties
            .insert("value".to_string(), PropertyValue::Int(1));
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // T1 starts (snapshot at version 1)
    let mut tx1 = db.begin_transaction().unwrap();
    let v1 = tx1.get_node(node_id).unwrap().unwrap();
    assert_eq!(v1.properties.get("value"), Some(&PropertyValue::Int(1)));

    // Create many new versions (should trigger GC eventually)
    for i in 2..=10 {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("value".to_string(), PropertyValue::Int(i));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // T1 should STILL be able to read its snapshot (GC must preserve it)
    let v1_after_updates = tx1.get_node(node_id).unwrap().unwrap();
    assert_eq!(
        v1_after_updates.properties.get("value"),
        Some(&PropertyValue::Int(1)),
        "GC should preserve version 1 while T1 is active"
    );

    tx1.commit().unwrap();

    cleanup_test_db(path);
}
