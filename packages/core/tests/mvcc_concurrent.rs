//! MVCC Concurrent Transaction Tests
//!
//! These tests verify that the MVCC transaction manager can handle
//! concurrent transactions correctly.

use sombra::{Config, GraphDB, Node, NodeId, PropertyValue};
use std::fs;
use std::sync::Arc;
use std::thread;

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

// ============================================================================
// CONCURRENT TRANSACTION TESTS
// These tests verify that multiple transactions can run concurrently with
// proper MVCC snapshot isolation.
// ============================================================================

#[test]
fn test_concurrent_readers_different_snapshots() {
    let path = "test_concurrent_readers.db";
    let db = Arc::new(create_mvcc_db(path));

    // Create initial node
    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(0);
        node.properties
            .insert("value".to_string(), PropertyValue::Int(100));
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // Start reader 1 with snapshot at time T1
    let mut tx1 = db.begin_transaction().unwrap();
    let snapshot1 = tx1.snapshot_ts();
    
    // Update the node (commits at time T2)
    {
        let mut tx_writer = db.begin_transaction().unwrap();
        let mut node = tx_writer.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("value".to_string(), PropertyValue::Int(200));
        tx_writer.add_node(node).unwrap();
        tx_writer.commit().unwrap();
    }

    // Start reader 2 with snapshot at time T3 (after update)
    let mut tx2 = db.begin_transaction().unwrap();
    let snapshot2 = tx2.snapshot_ts();

    // Verify snapshots are different
    assert!(snapshot2 > snapshot1, "Reader 2 should have newer snapshot");

    // Reader 1 should see old value (snapshot isolation)
    let node1 = tx1.get_node(node_id).unwrap().unwrap();
    assert_eq!(
        node1.properties.get("value"),
        Some(&PropertyValue::Int(100)),
        "Reader 1 should see old value from its snapshot"
    );

    // Reader 2 should see new value
    let node2 = tx2.get_node(node_id).unwrap().unwrap();
    assert_eq!(
        node2.properties.get("value"),
        Some(&PropertyValue::Int(200)),
        "Reader 2 should see new value from its snapshot"
    );

    tx1.commit().unwrap();
    tx2.commit().unwrap();

    cleanup_test_db(path);
}

#[test]
fn test_concurrent_reader_writer_no_blocking() {
    let path = "test_reader_writer_no_blocking.db";
    let db = Arc::new(create_mvcc_db(path));

    // Create initial nodes
    let node_id1 = {
        let mut tx = db.begin_transaction().unwrap();
        let node = Node::new(0);
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    let node_id2 = {
        let mut tx = db.begin_transaction().unwrap();
        let node = Node::new(0);
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // Spawn reader thread that holds a long transaction
    let db_clone = Arc::clone(&db);
    let reader_handle = thread::spawn(move || {
        let mut tx = db_clone.begin_transaction().unwrap();
        
        // Read node 1
        let node = tx.get_node(node_id1).unwrap().unwrap();
        assert_eq!(node.id, node_id1);

        // Sleep to simulate long-running read
        thread::sleep(std::time::Duration::from_millis(100));

        tx.commit().unwrap();
    });

    // Wait a bit for reader to start
    thread::sleep(std::time::Duration::from_millis(10));

    // Writer should not be blocked by the reader
    let mut tx_writer = db.begin_transaction().unwrap();
    let mut node2 = tx_writer.get_node(node_id2).unwrap().unwrap();
    node2.properties.insert("updated".to_string(), PropertyValue::Bool(true));
    tx_writer.add_node(node2).unwrap();
    tx_writer.commit().unwrap();

    // Wait for reader to complete
    reader_handle.join().unwrap();

    cleanup_test_db(path);
}

#[test]
fn test_concurrent_writers_no_blocking() {
    let path = "test_concurrent_writers.db";
    let db = Arc::new(create_mvcc_db(path));

    let db1 = Arc::clone(&db);
    let db2 = Arc::clone(&db);

    // Spawn two writer threads
    let writer1 = thread::spawn(move || {
        let mut tx = db1.begin_transaction().unwrap();
        let mut node = Node::new(0);
        node.labels.push("type1".to_string());
        node.properties.insert("writer".to_string(), PropertyValue::Int(1));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    });

    let writer2 = thread::spawn(move || {
        let mut tx = db2.begin_transaction().unwrap();
        let mut node = Node::new(0);
        node.labels.push("type2".to_string());
        node.properties.insert("writer".to_string(), PropertyValue::Int(2));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    });

    // Both writers should complete successfully
    writer1.join().unwrap();
    writer2.join().unwrap();

    // Verify both nodes were created
    {
        let mut tx = db.begin_transaction().unwrap();
        let nodes_label1 = tx.get_nodes_by_label("type1").unwrap();
        let nodes_label2 = tx.get_nodes_by_label("type2").unwrap();
        
        assert_eq!(nodes_label1.len(), 1, "Writer 1's node should exist");
        assert_eq!(nodes_label2.len(), 1, "Writer 2's node should exist");
        
        tx.commit().unwrap();
    }

    cleanup_test_db(path);
}

#[test]
fn test_snapshot_consistency_during_concurrent_updates() {
    let path = "test_snapshot_consistency.db";
    let db = Arc::new(create_mvcc_db(path));

    // Create initial nodes with counter values
    let node_ids: Vec<NodeId> = (0..5)
        .map(|_| {
            let mut tx = db.begin_transaction().unwrap();
            let mut node = Node::new(0);
            node.properties
                .insert("counter".to_string(), PropertyValue::Int(0));
            let id = tx.add_node(node).unwrap();
            tx.commit().unwrap();
            id
        })
        .collect();

    // Start a reader with snapshot S1
    let mut tx_reader = db.begin_transaction().unwrap();
    let snapshot_ts = tx_reader.snapshot_ts();

    // Perform concurrent updates to all nodes
    let db_clone = Arc::clone(&db);
    let node_ids_clone = node_ids.clone();
    let updater = thread::spawn(move || {
        for node_id in node_ids_clone {
            let mut tx = db_clone.begin_transaction().unwrap();
            let mut node = tx.get_node(node_id).unwrap().unwrap();
            node.properties
                .insert("counter".to_string(), PropertyValue::Int(1));
            tx.add_node(node).unwrap();
            tx.commit().unwrap();
        }
    });

    // Wait for updates to complete
    updater.join().unwrap();

    // Reader should see consistent snapshot (all counters = 0)
    for node_id in &node_ids {
        let node = tx_reader.get_node(*node_id).unwrap().unwrap();
        assert_eq!(
            node.properties.get("counter"),
            Some(&PropertyValue::Int(0)),
            "Reader snapshot should see consistent old values (counter=0)"
        );
    }

    tx_reader.commit().unwrap();

    // New reader should see all updates
    {
        let mut tx_new = db.begin_transaction().unwrap();
        let new_snapshot = tx_new.snapshot_ts();
        
        assert!(
            new_snapshot > snapshot_ts,
            "New reader should have newer snapshot"
        );

        for node_id in &node_ids {
            let node = tx_new.get_node(*node_id).unwrap().unwrap();
            assert_eq!(
                node.properties.get("counter"),
                Some(&PropertyValue::Int(1)),
                "New reader should see all updates (counter=1)"
            );
        }
        
        tx_new.commit().unwrap();
    }

    cleanup_test_db(path);
}

#[test]
fn test_many_concurrent_readers() {
    let path = "test_many_concurrent_readers.db";
    let db = Arc::new(create_mvcc_db(path));

    // Create test data
    let node_ids: Vec<NodeId> = (0..10)
        .map(|i| {
            let mut tx = db.begin_transaction().unwrap();
            let mut node = Node::new(0);
            node.properties
                .insert("value".to_string(), PropertyValue::Int(i));
            let id = tx.add_node(node).unwrap();
            tx.commit().unwrap();
            id
        })
        .collect();

    // Spawn 20 concurrent readers
    let mut handles = vec![];
    for reader_id in 0..20 {
        let db_clone = Arc::clone(&db);
        let node_ids_clone = node_ids.clone();
        
        let handle = thread::spawn(move || {
            let mut tx = db_clone.begin_transaction().unwrap();
            
            // Each reader reads all nodes
            let mut sum = 0i64;
            for node_id in node_ids_clone {
                let node = tx.get_node(node_id).unwrap().unwrap();
                if let Some(PropertyValue::Int(val)) = node.properties.get("value") {
                    sum += val;
                }
            }
            
            tx.commit().unwrap();
            
            // Sum should be 0+1+2+...+9 = 45
            assert_eq!(sum, 45, "Reader {} should see consistent data", reader_id);
        });
        
        handles.push(handle);
    }

    // All readers should complete successfully
    for handle in handles {
        handle.join().unwrap();
    }

    cleanup_test_db(path);
}
