//! MVCC Version Chain tests
//!
//! These tests verify that version chains are correctly maintained:
//! - Multiple versions of the same record are stored
//! - Version chains are properly linked
//! - Correct version is returned based on snapshot timestamp
//! - Version metadata is accurate
//!
//! ## Status: Tests are IGNORED - Requires Phase 2 MVCC Implementation
//!
//! These tests are currently failing because Phase 2 (MVCC Integration) is incomplete.
//! Specifically:
//! - Write operations (add_node) do NOT create version chains
//! - `add_node()` always creates new nodes with new IDs instead of updating existing nodes
//! - `store_new_version()` exists but is not called by write path
//!
//! NOTE: Some tests have compilation errors (E0499 - cannot borrow `db` mutably more than once)
//! because they try to hold multiple transactions simultaneously. This will be addressed
//! when Phase 2 is fully implemented with proper concurrent transaction support.
//!
//! The tests will be re-enabled once Phase 2 Task 10 is complete.

#[allow(dead_code, unused_imports, unused_variables)]
use sombra::{Config, GraphDB, Node, PropertyValue};
use std::fs;

fn cleanup_test_db(path: &str) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));
}

fn create_mvcc_db(path: &str) -> GraphDB {
    cleanup_test_db(path);
    let mut config = Config::default();
    GraphDB::open_with_config(path, config).unwrap()
}

#[test]
#[ignore = "Requires Phase 2 MVCC: write operations don't create version chains yet"]
fn test_version_chain_creation() {
    let path = "test_version_chain_create.db";
    let mut db = create_mvcc_db(path);

    // Create initial version
    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(1);
        node.properties
            .insert("version".to_string(), PropertyValue::Int(1));
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // Create second version
    {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("version".to_string(), PropertyValue::Int(2));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // Create third version
    {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("version".to_string(), PropertyValue::Int(3));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // Latest read should return version 3
    {
        let mut tx = db.begin_transaction().unwrap();
        let node = tx.get_node(node_id).unwrap().unwrap();
        assert_eq!(node.properties.get("version"), Some(&PropertyValue::Int(3)));
        tx.commit().unwrap();
    }

    cleanup_test_db(path);
}

#[test]
#[ignore = "Requires Phase 2 MVCC: write operations don't create version chains yet"]
#[cfg(FALSE)] // Compilation error: needs concurrent transactions
fn test_version_visibility_at_different_timestamps() {
    let path = "test_version_visibility.db";
    let mut db = create_mvcc_db(path);

    // Version 1
    let (node_id, ts1) = {
        let mut tx = db.begin_transaction().unwrap();
        let ts = tx.snapshot_ts();
        let mut node = Node::new(1);
        node.properties
            .insert("value".to_string(), PropertyValue::String("v1".to_string()));
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        (id, ts)
    };

    // Snapshot at ts1
    let mut snapshot1 = db.begin_transaction().unwrap();
    let snapshot1_ts = snapshot1.snapshot_ts();
    assert!(snapshot1_ts > ts1);

    // Version 2
    let ts2 = {
        let mut tx = db.begin_transaction().unwrap();
        let ts = tx.snapshot_ts();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("value".to_string(), PropertyValue::String("v2".to_string()));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
        ts
    };

    // Snapshot at ts2
    let mut snapshot2 = db.begin_transaction().unwrap();
    let snapshot2_ts = snapshot2.snapshot_ts();
    assert!(snapshot2_ts > ts2);

    // Version 3
    {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("value".to_string(), PropertyValue::String("v3".to_string()));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // snapshot1 should see v1
    let node1 = snapshot1.get_node(node_id).unwrap().unwrap();
    assert_eq!(
        node1.properties.get("value"),
        Some(&PropertyValue::String("v1".to_string()))
    );
    snapshot1.commit().unwrap();

    // snapshot2 should see v2
    let node2 = snapshot2.get_node(node_id).unwrap().unwrap();
    assert_eq!(
        node2.properties.get("value"),
        Some(&PropertyValue::String("v2".to_string()))
    );
    snapshot2.commit().unwrap();

    // Fresh snapshot should see v3
    {
        let mut tx = db.begin_transaction().unwrap();
        let node = tx.get_node(node_id).unwrap().unwrap();
        assert_eq!(
            node.properties.get("value"),
            Some(&PropertyValue::String("v3".to_string()))
        );
        tx.commit().unwrap();
    }

    cleanup_test_db(path);
}

#[test]
#[ignore = "Requires Phase 2 MVCC: write operations don't create version chains yet"]
fn test_version_chain_with_multiple_properties() {
    let path = "test_version_multi_props.db";
    let mut db = create_mvcc_db(path);

    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(1);
        node.properties.insert(
            "name".to_string(),
            PropertyValue::String("Alice".to_string()),
        );
        node.properties
            .insert("age".to_string(), PropertyValue::Int(25));
        node.properties
            .insert("active".to_string(), PropertyValue::Bool(true));
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // Update only one property
    {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("age".to_string(), PropertyValue::Int(26));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // Verify all properties are in the new version
    {
        let mut tx = db.begin_transaction().unwrap();
        let node = tx.get_node(node_id).unwrap().unwrap();
        assert_eq!(
            node.properties.get("name"),
            Some(&PropertyValue::String("Alice".to_string()))
        );
        assert_eq!(node.properties.get("age"), Some(&PropertyValue::Int(26)));
        assert_eq!(
            node.properties.get("active"),
            Some(&PropertyValue::Bool(true))
        );
        tx.commit().unwrap();
    }

    cleanup_test_db(path);
}

#[test]
#[ignore = "Requires Phase 2 MVCC: write operations don't create version chains yet"]
fn test_version_chain_persistence() {
    let path = "test_version_chain_persist.db";

    // Create versions
    {
        let mut db = create_mvcc_db(path);
        let node_id = {
            let mut tx = db.begin_transaction().unwrap();
            let mut node = Node::new(1);
            node.properties
                .insert("gen".to_string(), PropertyValue::Int(1));
            let id = tx.add_node(node).unwrap();
            tx.commit().unwrap();
            id
        };

        // Add more versions
        for i in 2..=5 {
            let mut tx = db.begin_transaction().unwrap();
            let mut node = tx.get_node(node_id).unwrap().unwrap();
            node.properties
                .insert("gen".to_string(), PropertyValue::Int(i));
            tx.add_node(node).unwrap();
            tx.commit().unwrap();
        }
    }

    // Reopen and verify latest version
    {
        let mut config = Config::default();
        let mut db = GraphDB::open_with_config(path, config).unwrap();

        let mut tx = db.begin_transaction().unwrap();
        let node = tx.get_node(1).unwrap().unwrap();
        assert_eq!(node.properties.get("gen"), Some(&PropertyValue::Int(5)));
        tx.commit().unwrap();
    }

    cleanup_test_db(path);
}

#[test]
#[ignore = "Requires Phase 2 MVCC: write operations don't create version chains yet"]
fn test_long_version_chain() {
    let path = "test_long_version_chain.db";
    let mut db = create_mvcc_db(path);

    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(1);
        node.properties
            .insert("counter".to_string(), PropertyValue::Int(0));
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // Create 20 versions
    for i in 1..=20 {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = tx.get_node(node_id).unwrap().unwrap();
        node.properties
            .insert("counter".to_string(), PropertyValue::Int(i));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    // Verify latest version
    {
        let mut tx = db.begin_transaction().unwrap();
        let node = tx.get_node(node_id).unwrap().unwrap();
        assert_eq!(
            node.properties.get("counter"),
            Some(&PropertyValue::Int(20))
        );
        tx.commit().unwrap();
    }

    cleanup_test_db(path);
}

#[test]
#[ignore = "Requires Phase 2 MVCC: write operations don't create version chains yet"]
fn test_interleaved_version_chains() {
    let path = "test_interleaved_chains.db";
    let mut db = create_mvcc_db(path);

    // Create two nodes
    let (node1_id, node2_id) = {
        let mut tx = db.begin_transaction().unwrap();
        let mut n1 = Node::new(1);
        n1.properties
            .insert("value".to_string(), PropertyValue::Int(1));
        let id1 = tx.add_node(n1).unwrap();

        let mut n2 = Node::new(2);
        n2.properties
            .insert("value".to_string(), PropertyValue::Int(1));
        let id2 = tx.add_node(n2).unwrap();

        tx.commit().unwrap();
        (id1, id2)
    };

    // Interleave updates
    for i in 2..=10 {
        // Update node1
        {
            let mut tx = db.begin_transaction().unwrap();
            let mut node = tx.get_node(node1_id).unwrap().unwrap();
            node.properties
                .insert("value".to_string(), PropertyValue::Int(i));
            tx.add_node(node).unwrap();
            tx.commit().unwrap();
        }

        // Update node2
        {
            let mut tx = db.begin_transaction().unwrap();
            let mut node = tx.get_node(node2_id).unwrap().unwrap();
            node.properties
                .insert("value".to_string(), PropertyValue::Int(i));
            tx.add_node(node).unwrap();
            tx.commit().unwrap();
        }
    }

    // Verify both chains
    {
        let mut tx = db.begin_transaction().unwrap();
        let node1 = tx.get_node(node1_id).unwrap().unwrap();
        let node2 = tx.get_node(node2_id).unwrap().unwrap();

        assert_eq!(node1.properties.get("value"), Some(&PropertyValue::Int(10)));
        assert_eq!(node2.properties.get("value"), Some(&PropertyValue::Int(10)));

        tx.commit().unwrap();
    }

    cleanup_test_db(path);
}

#[test]
#[ignore = "Requires Phase 2 MVCC: write operations don't create version chains yet"]
#[cfg(FALSE)] // Compilation error: needs concurrent transactions
fn test_version_chain_with_deleted_version() {
    let path = "test_version_deleted.db";
    let mut db = create_mvcc_db(path);

    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let node = Node::new(1);
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // Take snapshot before delete
    let mut snapshot_before = db.begin_transaction().unwrap();

    // Delete node
    {
        let mut tx = db.begin_transaction().unwrap();
        tx.delete_node(node_id).unwrap();
        tx.commit().unwrap();
    }

    // Snapshot before should still see the node
    let node_before = snapshot_before.get_node(node_id).unwrap();
    assert!(
        node_before.is_some(),
        "Old snapshot should see non-deleted version"
    );
    snapshot_before.commit().unwrap();

    // New snapshot should not see it
    {
        let mut tx = db.begin_transaction().unwrap();
        let node = tx.get_node(node_id).unwrap();
        assert!(node.is_none(), "New snapshot should see deleted version");
        tx.commit().unwrap();
    }

    cleanup_test_db(path);
}
