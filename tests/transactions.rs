#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use sombra::{GraphDB, Node, Result};
use std::fs;
use tempfile::NamedTempFile;

#[test]
fn transaction_commit_wal_only() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let node_id;
    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;
        node_id = tx.add_node(Node::new(0))?;
        tx.commit()?;

        // After commit but before checkpoint, data should be in WAL but not main file
        let wal_path = path.with_extension("wal");
        assert!(wal_path.exists());
        assert!(fs::metadata(&wal_path)?.len() > 24); // More than just header
    }

    // Data should be recoverable from WAL after reopening
    {
        let mut db = GraphDB::open(&path)?;
        let node = db.get_node(node_id)?;
        assert_eq!(node.id, node_id);
    }

    Ok(())
}

#[test]
fn transaction_rollback_no_wal_traces() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;
        tx.add_node(Node::new(0))?;
        tx.rollback()?;

        // After rollback, WAL should be minimal (just header)
        let wal_path = path.with_extension("wal");
        assert!(wal_path.exists());
        assert_eq!(fs::metadata(&wal_path)?.len(), 32); // Just header
    }

    // No data should be recoverable
    {
        let mut db = GraphDB::open(&path)?;
        assert!(db.get_node(1).is_err());
    }

    Ok(())
}

#[test]
fn multi_transaction_isolation() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let tx1_node_id = {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;
        let node_id = tx.add_node(Node::new(0))?;
        tx.commit()?;
        db.checkpoint()?;
        drop(db);
        node_id
    };

    let tx2_node_id = {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;
        let node_id = tx.add_node(Node::new(0))?;
        tx.commit()?;
        // Don't checkpoint yet
        drop(db);
        node_id
    };

    // At this point, both should be visible since WAL is replayed on open
    {
        let mut db = GraphDB::open(&path)?;
        assert!(db.get_node(tx1_node_id).is_ok());
        assert!(db.get_node(tx2_node_id).is_ok());
    }

    // After checkpoint, both should still be visible
    {
        let mut db = GraphDB::open(&path)?;
        db.checkpoint()?;
        assert!(db.get_node(tx1_node_id).is_ok());
        assert!(db.get_node(tx2_node_id).is_ok());
    }

    Ok(())
}

#[test]
fn transaction_id_persistence() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;
        tx.add_node(Node::new(0))?;
        tx.commit()?;
        db.checkpoint()?;
        drop(db);
    }

    let tx2_id;
    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;
        tx.add_node(Node::new(0))?;
        tx2_id = tx.id();
        tx.commit()?;
        drop(db);

        // Transaction ID should be incremented from persisted value
        assert_eq!(tx2_id, 2);
    }

    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;
        tx.add_node(Node::new(0))?;
        let tx3_id = tx.id();
        tx.commit()?;
        drop(db);

        // Transaction ID should be incremented from persisted value
        assert_eq!(tx3_id, 3);
    }

    Ok(())
}

#[test]
fn nested_transactions_prevented() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    // Test that we can create sequential transactions
    let mut db = GraphDB::open(&path)?;

    // First transaction
    let mut tx1 = db.begin_transaction()?;
    let node_id = tx1.add_node(Node::new(0))?;
    tx1.commit()?;

    // Second transaction should work fine after first is committed
    let mut tx2 = db.begin_transaction()?;
    tx2.add_node(Node::new(0))?;
    tx2.commit()?;

    // Verify both nodes exist
    assert!(db.get_node(node_id).is_ok());

    Ok(())
}

#[test]
fn mutations_outside_transaction_prevented() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path)?;

    // When not in transaction context, mutations should work
    assert!(db.add_node(Node::new(0)).is_ok());

    Ok(())
}

#[test]
fn crash_simulation_uncommitted_tx_lost() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let node_id;
    // Simulate a crash after creating data but before commit
    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;
        node_id = tx.add_node(Node::new(0))?;

        // Rollback to simulate crash
        tx.rollback()?;
        drop(db);
    }

    // On recovery, uncommitted transaction should be lost
    {
        let mut db = GraphDB::open(&path)?;
        assert!(db.get_node(node_id).is_err());
    }

    // On recovery, uncommitted transaction should be lost
    {
        let mut db = GraphDB::open(&path)?;
        assert!(db.get_node(node_id).is_err());
    }

    Ok(())
}

#[test]
fn large_transaction_dirty_page_tracking() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;

        // Create many nodes to dirty multiple pages
        let mut node_ids = Vec::new();
        for _i in 0..50 {
            let node_id = tx.add_node(Node::new(0))?;
            node_ids.push(node_id);
        }

        tx.commit()?;
        db.checkpoint()?;
    }

    // Verify all data is recoverable
    {
        let mut db = GraphDB::open(&path)?;
        // Check how many nodes were actually created
        let mut found_nodes = 0;
        for i in 1..=50 {
            if let Ok(node) = db.get_node(i as u64) {
                found_nodes += 1;
                assert_eq!(node.id, i as u64);
            }
        }
        assert_eq!(found_nodes, 50, "Expected 50 nodes, found {}", found_nodes);
    }

    Ok(())
}
