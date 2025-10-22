#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use sombra::{GraphDB, Node, Result};
use tempfile::NamedTempFile;

#[test]
fn test_no_ghost_entries_after_delete_and_checkpoint() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path)?;
    let mut tx = db.begin_transaction()?;

    let mut node_ids = Vec::new();
    for _ in 0..100 {
        let node = Node::new(0);
        let node_id = tx.add_node(node)?;
        node_ids.push(node_id);
    }

    tx.commit()?;
    db.checkpoint()?;

    let all_before = db.get_all_node_ids_ordered();
    assert_eq!(all_before.len(), 100);

    let mut tx = db.begin_transaction()?;
    for (idx, node_id) in node_ids.iter().enumerate() {
        if idx % 2 == 0 {
            tx.delete_node(*node_id)?;
        }
    }
    tx.commit()?;
    db.checkpoint()?;

    let all_after = db.get_all_node_ids_ordered();
    assert_eq!(
        all_after.len(),
        50,
        "BTree should contain exactly 50 nodes after deletion"
    );

    let mut tx = db.begin_transaction()?;
    let mut ghost_count = 0;
    for node_id in &all_after {
        if tx.get_node(*node_id).is_err() {
            ghost_count += 1;
        }
    }
    tx.commit()?;

    assert_eq!(
        ghost_count, 0,
        "No ghost entries should exist in BTree index after checkpoint"
    );

    Ok(())
}

#[test]
fn test_no_ghost_entries_large_dataset() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path)?;
    let mut tx = db.begin_transaction()?;

    let mut node_ids = Vec::new();
    for _ in 0..1000 {
        let node = Node::new(0);
        let node_id = tx.add_node(node)?;
        node_ids.push(node_id);
    }

    tx.commit()?;
    db.checkpoint()?;

    let mut tx = db.begin_transaction()?;
    let mut deleted_count = 0;
    for (idx, node_id) in node_ids.iter().enumerate() {
        if idx % 3 == 0 {
            tx.delete_node(*node_id)?;
            deleted_count += 1;
        }
    }
    tx.commit()?;
    db.checkpoint()?;

    let all_after = db.get_all_node_ids_ordered();
    let expected = 1000 - deleted_count;
    assert_eq!(all_after.len(), expected);

    let mut tx = db.begin_transaction()?;
    for node_id in &all_after {
        assert!(
            tx.get_node(*node_id).is_ok(),
            "Node {} should exist (no ghost entries allowed)",
            node_id
        );
    }
    tx.commit()?;

    Ok(())
}

#[test]
fn test_no_ghost_entries_multiple_checkpoints() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path)?;

    for round in 0..5 {
        let mut tx = db.begin_transaction()?;
        let mut node_ids = Vec::new();

        for _ in 0..50 {
            let node = Node::new(0);
            let node_id = tx.add_node(node)?;
            node_ids.push(node_id);
        }
        tx.commit()?;
        db.checkpoint()?;

        let mut tx = db.begin_transaction()?;
        for (idx, node_id) in node_ids.iter().enumerate() {
            if idx % 2 == 0 {
                tx.delete_node(*node_id)?;
            }
        }
        tx.commit()?;
        db.checkpoint()?;

        let all_nodes = db.get_all_node_ids_ordered();
        let expected = (round + 1) * 25;
        assert_eq!(
            all_nodes.len(),
            expected,
            "Round {}: should have {} nodes",
            round,
            expected
        );

        let mut tx = db.begin_transaction()?;
        for node_id in &all_nodes {
            assert!(
                tx.get_node(*node_id).is_ok(),
                "Round {}: node {} should exist",
                round,
                node_id
            );
        }
        tx.commit()?;
    }

    Ok(())
}
