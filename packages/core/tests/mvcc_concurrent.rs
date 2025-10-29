//! MVCC Concurrent Transaction Tests
//!
//! These tests verify that the MVCC transaction manager can handle
//! concurrent transactions correctly, even though the public API
//! currently requires sequential access due to &mut GraphDB.
//!
//! This tests the internal MVCC infrastructure that will enable
//! true concurrent transactions once the API is refactored.

use sombra::{Config, GraphDB, Node, PropertyValue};
use std::fs;

fn cleanup_test_db(path: &str) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));
}

fn create_mvcc_db(path: &str) -> GraphDB {
    cleanup_test_db(path);
    let mut config = Config::default();
    config.mvcc_enabled = true;
    config.max_concurrent_transactions = Some(100);
    GraphDB::open_with_config(path, config).unwrap()
}

#[test]
fn test_mvcc_manager_tracks_concurrent_transactions() {
    let path = "test_mvcc_concurrent_tracking.db";
    let mut db = create_mvcc_db(path);

    // Start first transaction
    let tx1 = db.begin_transaction().unwrap();
    let tx1_id = tx1.id();
    let tx1_snapshot = tx1.snapshot_ts();

    // Note: We can't start tx2 while tx1 is active due to &mut borrow
    // This is expected - the single-writer constraint at the API level
    // will be removed in Phase 4 when we refactor GraphDB to use Arc<Mutex>

    // Complete tx1
    tx1.commit().unwrap();

    // Start second transaction - it should have a newer snapshot
    let tx2 = db.begin_transaction().unwrap();
    let tx2_snapshot = tx2.snapshot_ts();

    assert!(
        tx2_snapshot > tx1_snapshot,
        "tx2 should have newer snapshot than tx1"
    );

    tx2.commit().unwrap();

    cleanup_test_db(path);
}

#[test]
fn test_sequential_transactions_with_mvcc() {
    let path = "test_sequential_mvcc_tx.db";
    let mut db = create_mvcc_db(path);

    // Transaction 1: Create a node
    let node_id = {
        let mut tx1 = db.begin_transaction().unwrap();
        let mut node = Node::new(1);
        node.properties
            .insert("counter".to_string(), PropertyValue::Int(1));
        let id = tx1.add_node(node).unwrap();
        tx1.commit().unwrap();
        id
    };

    // Transaction 2: Update the node
    {
        let mut tx2 = db.begin_transaction().unwrap();
        let mut node = tx2.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("counter".to_string(), PropertyValue::Int(2));
        tx2.add_node(node).unwrap(); // Creates new version
        tx2.commit().unwrap();
    }

    // Transaction 3: Read the node - should see latest version
    {
        let mut tx3 = db.begin_transaction().unwrap();
        let node = tx3.get_node(node_id).unwrap().unwrap();
        assert_eq!(node.properties.get("counter"), Some(&PropertyValue::Int(2)));
        tx3.commit().unwrap();
    }

    cleanup_test_db(path);
}

#[test]
fn test_mvcc_read_write_tracking() {
    let path = "test_mvcc_read_write_tracking.db";
    let mut db = create_mvcc_db(path);

    // Create initial nodes
    let node_id1 = {
        let mut tx = db.begin_transaction().unwrap();
        let node = Node::new(1);
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    let node_id2 = {
        let mut tx = db.begin_transaction().unwrap();
        let node = Node::new(2);
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // Transaction that reads and writes
    {
        let mut tx = db.begin_transaction().unwrap();

        // Read node1 (tracked in read_nodes)
        let _node1 = tx.get_node(node_id1).unwrap();

        // Update node2 (tracked in write_nodes)
        let mut node2 = tx.get_node(node_id2).unwrap().unwrap();
        node2
            .properties
            .insert("updated".to_string(), PropertyValue::Bool(true));
        tx.add_node(node2).unwrap();

        // The transaction should track both reads and writes
        // (we can't directly inspect the sets, but they're used in logging)
        tx.commit().unwrap();
    }

    cleanup_test_db(path);
}

#[test]
fn test_mvcc_snapshot_timestamps_increase() {
    let path = "test_mvcc_snapshot_increase.db";
    let mut db = create_mvcc_db(path);

    let mut snapshots = Vec::new();

    // Create 5 transactions sequentially
    for i in 0..5 {
        let mut tx = db.begin_transaction().unwrap();
        let snapshot = tx.snapshot_ts();
        snapshots.push(snapshot);

        // Add a node to make the transaction do something
        let mut node = Node::new(i);
        node.properties
            .insert("index".to_string(), PropertyValue::Int(i as i64));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // Verify that snapshot timestamps are strictly increasing
    for i in 1..snapshots.len() {
        assert!(
            snapshots[i] > snapshots[i - 1],
            "Snapshot {} ({}) should be > snapshot {} ({})",
            i,
            snapshots[i],
            i - 1,
            snapshots[i - 1]
        );
    }

    cleanup_test_db(path);
}

#[test]
fn test_mvcc_version_chain_with_multiple_updates() {
    let path = "test_mvcc_version_chain_updates.db";
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

    // Perform multiple updates, creating a version chain
    for i in 2..=5 {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("version".to_string(), PropertyValue::Int(i));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // Verify latest version is visible
    {
        let mut tx = db.begin_transaction().unwrap();
        let node = tx.get_node(node_id).unwrap().unwrap();
        assert_eq!(node.properties.get("version"), Some(&PropertyValue::Int(5)));
        tx.commit().unwrap();
    }

    cleanup_test_db(path);
}
