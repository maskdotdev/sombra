//! Basic MVCC functionality tests
//!
//! These tests verify that the MVCC infrastructure components are properly
//! integrated into GraphDB, including:
//! - Timestamp oracle initialization and restoration
//! - Transaction snapshot timestamp allocation
//! - MVCC mode enabled/disabled behavior
//! - Backwards compatibility with non-MVCC databases

use sombra::{Config, Edge, GraphDB, Node};
use std::fs;

#[test]
fn test_mvcc_disabled_by_default() {
    let path = "test_mvcc_disabled.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));

    let config = Config::default();
    assert!(!config.mvcc_enabled, "MVCC should be disabled by default");

    let mut db = GraphDB::open_with_config(path, config).unwrap();
    let mut tx = db.begin_transaction().unwrap();

    // With MVCC disabled, snapshot_ts should be 0
    assert_eq!(
        tx.snapshot_ts(),
        0,
        "Snapshot timestamp should be 0 when MVCC is disabled"
    );

    let node = Node::new(1);
    tx.add_node(node).unwrap();
    tx.commit().unwrap();

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));
}

#[test]
fn test_mvcc_enabled_allocates_snapshot_timestamp() {
    let path = "test_mvcc_enabled.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));

    let mut config = Config::default();
    config.mvcc_enabled = true;

    let mut db = GraphDB::open_with_config(path, config).unwrap();
    let mut tx1 = db.begin_transaction().unwrap();

    // With MVCC enabled, snapshot_ts should be > 0
    let ts1 = tx1.snapshot_ts();
    assert!(
        ts1 > 0,
        "Snapshot timestamp should be > 0 when MVCC is enabled"
    );

    let node = Node::new(1);
    tx1.add_node(node).unwrap();
    tx1.commit().unwrap();

    // Second transaction should get a higher timestamp
    let tx2 = db.begin_transaction().unwrap();
    let ts2 = tx2.snapshot_ts();
    assert!(
        ts2 > ts1,
        "Second transaction should get higher timestamp (ts2={ts2}, ts1={ts1})"
    );
    tx2.commit().unwrap();

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));
}

#[test]
fn test_mvcc_timestamp_persists_across_reopen() {
    let path = "test_mvcc_persist.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));

    let mut config = Config::default();
    config.mvcc_enabled = true;

    let last_ts = {
        let mut db = GraphDB::open_with_config(path, config.clone()).unwrap();
        let mut tx1 = db.begin_transaction().unwrap();
        let _ts1 = tx1.snapshot_ts();

        let node = Node::new(1);
        tx1.add_node(node).unwrap();
        tx1.commit().unwrap();

        let tx2 = db.begin_transaction().unwrap();
        let ts2 = tx2.snapshot_ts();
        tx2.commit().unwrap();

        ts2
    };

    // Reopen the database
    {
        let mut db = GraphDB::open_with_config(path, config.clone()).unwrap();
        let tx = db.begin_transaction().unwrap();
        let ts = tx.snapshot_ts();

        // New timestamp should be greater than the last one from before close
        assert!(
            ts > last_ts,
            "Timestamp after reopen should be > last timestamp (ts={ts}, last_ts={last_ts})"
        );

        tx.commit().unwrap();
    }

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));
}

#[test]
fn test_mvcc_rollback_unregisters_snapshot() {
    let path = "test_mvcc_rollback.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));

    let mut config = Config::default();
    config.mvcc_enabled = true;

    let mut db = GraphDB::open_with_config(path, config).unwrap();

    // Start a transaction and roll it back
    let mut tx1 = db.begin_transaction().unwrap();
    let ts1 = tx1.snapshot_ts();
    assert!(ts1 > 0);

    let node = Node::new(1);
    tx1.add_node(node).unwrap();
    tx1.rollback().unwrap();

    // Start another transaction - it should work fine
    let tx2 = db.begin_transaction().unwrap();
    let ts2 = tx2.snapshot_ts();
    assert!(
        ts2 > ts1,
        "New transaction after rollback should get fresh timestamp"
    );
    tx2.commit().unwrap();

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));
}

#[test]
fn test_mvcc_multiple_concurrent_snapshots() {
    let path = "test_mvcc_concurrent.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));

    let mut config = Config::default();
    config.mvcc_enabled = true;

    let mut db = GraphDB::open_with_config(path, config).unwrap();

    // Create first transaction
    let mut tx1 = db.begin_transaction().unwrap();
    let ts1 = tx1.snapshot_ts();

    let node1 = Node::new(1);
    tx1.add_node(node1).unwrap();
    tx1.commit().unwrap();

    // Create second transaction
    let mut tx2 = db.begin_transaction().unwrap();
    let ts2 = tx2.snapshot_ts();

    let node2 = Node::new(2);
    tx2.add_node(node2).unwrap();

    // Verify timestamps are increasing
    assert!(ts2 > ts1, "Timestamps should be monotonically increasing");

    tx2.commit().unwrap();

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));
}

#[test]
fn test_backwards_compatibility_non_mvcc_database() {
    let path = "test_backwards_compat.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));

    // Create database with MVCC disabled
    {
        let config = Config::default(); // MVCC disabled by default
        let mut db = GraphDB::open_with_config(path, config).unwrap();

        let mut tx = db.begin_transaction().unwrap();
        assert_eq!(tx.snapshot_ts(), 0);

        let node = Node::new(1);
        tx.add_node(node).unwrap();

        let edge = Edge::new(1, 1, 1, "test");
        tx.add_edge(edge).unwrap();

        tx.commit().unwrap();
    }

    // Reopen with MVCC still disabled - should work
    {
        let config = Config::default();
        let mut db = GraphDB::open_with_config(path, config).unwrap();

        let mut tx = db.begin_transaction().unwrap();
        assert_eq!(tx.snapshot_ts(), 0);

        let node = tx.get_node(1).unwrap();
        assert!(node.is_some());

        tx.commit().unwrap();
    }

    // Reopen with MVCC enabled - should work (forward compatibility)
    {
        let mut config = Config::default();
        config.mvcc_enabled = true;

        let mut db = GraphDB::open_with_config(path, config).unwrap();

        let mut tx = db.begin_transaction().unwrap();
        assert!(tx.snapshot_ts() > 0, "MVCC should work on old database");

        let node = tx.get_node(1).unwrap();
        assert!(node.is_some(), "Should be able to read old data");

        tx.commit().unwrap();
    }

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));
}

#[test]
fn test_mvcc_config_options() {
    let path = "test_mvcc_config.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));

    let mut config = Config::default();
    config.mvcc_enabled = true;
    config.gc_interval_secs = Some(120);
    // TODO: Add max_version_chain_length and snapshot_retention_secs to Config
    // config.max_version_chain_length = 200;
    // config.snapshot_retention_secs = 600;

    let _db = GraphDB::open_with_config(path, config.clone()).unwrap();

    // Verify config options were accepted (implicit - if they weren't valid, open would fail)
    // The actual config values are internal to GraphDB

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));
}

#[test]
fn test_mvcc_timestamp_oracle_initialization() {
    let path = "test_mvcc_oracle_init.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));

    let mut config = Config::default();
    config.mvcc_enabled = true;

    // First open - should initialize timestamp oracle
    {
        let mut db = GraphDB::open_with_config(path, config.clone()).unwrap();
        let tx = db.begin_transaction().unwrap();
        let ts = tx.snapshot_ts();

        // First timestamp should be 1 (0 is reserved)
        assert!(ts >= 1, "First timestamp should be >= 1");

        tx.commit().unwrap();
    }

    // Reopen - should restore timestamp oracle state
    {
        let mut db = GraphDB::open_with_config(path, config.clone()).unwrap();
        let tx = db.begin_transaction().unwrap();
        let ts = tx.snapshot_ts();

        // Timestamp should continue from where it left off
        assert!(
            ts > 1,
            "Timestamp should be restored and continue incrementing"
        );

        tx.commit().unwrap();
    }

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{path}.wal"));
}
