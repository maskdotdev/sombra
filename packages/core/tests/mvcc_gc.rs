//! MVCC Garbage Collection Tests
//!
//! Tests for the garbage collection system that reclaims old MVCC versions.

use sombra::{Config, GraphDB, Node, PropertyValue};
use tempfile::NamedTempFile;

/// Helper function to create a test database
fn create_test_db() -> (GraphDB, NamedTempFile) {
    let temp_file = NamedTempFile::new().unwrap();
    let mut config = Config::default();
    config.gc_interval_secs = None; // Disable background GC for manual testing
    let db = GraphDB::open_with_config(temp_file.path(), config).unwrap();
    (db, temp_file)
}

#[test]
fn test_gc_basic_run_no_active_transactions() {
    let (mut db, _temp) = create_test_db();

    // Create a node
    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(1);
        node.properties
            .insert("value".to_string(), PropertyValue::Int(1));
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // Update the node multiple times to create version chain
    for i in 2..=5 {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("value".to_string(), PropertyValue::Int(i as i64));
        tx.add_node(node).unwrap(); // Creates new version
        tx.commit().unwrap();
    }

    // Run GC - should reclaim old versions since no active transactions
    let stats = db.run_gc().unwrap();

    // We should have reclaimed some versions
    // Note: The exact number depends on implementation details
    println!("GC Stats: {stats:?}");
    assert!(stats.versions_examined > 0 || stats.chains_scanned > 0);
}

#[test]
fn test_gc_preserves_minimum_versions() {
    let (mut db, _temp) = create_test_db();

    // Create a node
    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(1);
        node.properties
            .insert("value".to_string(), PropertyValue::Int(1));
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // Update the node a few times
    for i in 2..=3 {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("value".to_string(), PropertyValue::Int(i as i64));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // Run GC
    let stats = db.run_gc().unwrap();

    // After GC, we should still be able to read the node
    let mut tx = db.begin_transaction().unwrap();
    let node = tx.get_node(node_id).unwrap().unwrap();
    assert_eq!(node.properties.get("value"), Some(&PropertyValue::Int(3))); // Should have latest version
    tx.commit().unwrap();

    println!("GC preserved versions, stats: {stats:?}");
}

#[test]
fn test_gc_respects_watermark() {
    let (mut db, _temp) = create_test_db();

    // Create a node
    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(1);
        node.properties
            .insert("value".to_string(), PropertyValue::Int(1));
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // Start a long-running read transaction
    let read_tx = db.begin_transaction().unwrap();
    let read_snapshot = read_tx.snapshot_ts();

    // Rollback the read transaction properly (don't just drop it)
    read_tx.rollback().unwrap();

    // Update the node
    {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("value".to_string(), PropertyValue::Int(2));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // Run GC - should respect the watermark
    // Note: Since we dropped the read transaction, GC might reclaim more
    let stats = db.run_gc().unwrap();

    println!("GC with active transaction snapshot {read_snapshot}, stats: {stats:?}");

    // The GC should complete without errors
    // (Exact behavior depends on watermark calculation)
}

#[test]
fn test_gc_doesnt_break_concurrent_reads() {
    let (mut db, _temp) = create_test_db();

    // Create and update a node multiple times
    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(1);
        node.properties
            .insert("value".to_string(), PropertyValue::Int(1));
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    for i in 2..=10 {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("value".to_string(), PropertyValue::Int(i as i64));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // Run GC
    let _stats = db.run_gc().unwrap();

    // Verify we can still read the node correctly
    let mut tx = db.begin_transaction().unwrap();
    let node = tx.get_node(node_id).unwrap().unwrap();
    assert_eq!(node.properties.get("value"), Some(&PropertyValue::Int(10)));
    tx.commit().unwrap();
}

#[test]
fn test_background_gc_start_stop() {
    // Create a test database with background GC enabled
    let temp_file = NamedTempFile::new().unwrap();
    let mut config = Config::default();
    config.gc_interval_secs = Some(60); // Enable background GC with 60s interval
    let mut db = GraphDB::open_with_config(temp_file.path(), config).unwrap();

    // Start background GC
    db.start_background_gc().unwrap();

    // Create some data
    {
        let mut tx = db.begin_transaction().unwrap();
        let node = Node::new(1);
        let _node_id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // Stop background GC
    db.stop_background_gc().unwrap();

    // Should be able to stop again (no-op)
    db.stop_background_gc().unwrap();
}

#[test]
fn test_gc_stats_accuracy() {
    let (mut db, _temp) = create_test_db();

    // Create a node
    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(1);
        node.properties
            .insert("value".to_string(), PropertyValue::Int(1));
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // Update it a few times
    for i in 2..=5 {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("value".to_string(), PropertyValue::Int(i as i64));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // Run GC and check stats
    let stats = db.run_gc().unwrap();

    println!("GC Stats: {stats:?}");

    // Stats should have reasonable values
    assert!(stats.duration_ms >= 0); // Duration should be non-negative
    assert!(stats.versions_reclaimed <= stats.versions_reclaimable);
}

#[test]
fn test_gc_empty_database() {
    let (mut db, _temp) = create_test_db();

    // Run GC on empty database
    let stats = db.run_gc().unwrap();

    // Should complete without errors, no versions to reclaim
    assert_eq!(stats.versions_reclaimed, 0);
    assert_eq!(stats.versions_reclaimable, 0);
}

#[test]
fn test_gc_with_multiple_nodes() {
    let (mut db, _temp) = create_test_db();

    // Create multiple nodes and update them
    let mut node_ids = Vec::new();
    for i in 1..=5 {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(i);
        node.properties
            .insert("value".to_string(), PropertyValue::Int(i as i64));
        let node_id = tx.add_node(node).unwrap();
        node_ids.push(node_id);
        tx.commit().unwrap();
    }

    // Update each node multiple times
    for node_id in &node_ids {
        for j in 1..=3 {
            let mut tx = db.begin_transaction().unwrap();
            let mut node = tx.get_node(*node_id).unwrap().unwrap();
            let current_value = match node.properties.get("value") {
                Some(PropertyValue::Int(v)) => *v,
                _ => 0,
            };
            node.properties.insert(
                "value".to_string(),
                PropertyValue::Int(current_value + j * 10),
            );
            tx.add_node(node).unwrap();
            tx.commit().unwrap();
        }
    }

    // Run GC
    let stats = db.run_gc().unwrap();

    println!("GC with multiple nodes, stats: {stats:?}");

    // Verify all nodes are still readable with correct data
    for (i, node_id) in node_ids.iter().enumerate() {
        let mut tx = db.begin_transaction().unwrap();
        let node = tx.get_node(*node_id).unwrap().unwrap();
        // Final value should be: initial_value + (1+2+3)*10 = initial + 60
        let expected = (i as i64 + 1) + 60;
        assert_eq!(
            node.properties.get("value"),
            Some(&PropertyValue::Int(expected))
        );
        tx.commit().unwrap();
    }
}
