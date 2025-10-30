#![allow(clippy::uninlined_format_args)]

/// Regression test for BIDX error during transaction commit
/// 
/// Issue: When nodes are added via tx.add_node(), the BTree node ID index gets updated,
/// marking BTree pages (magic: "BIDX") as dirty. During tx.commit(), the slow path in
/// update_versions_commit_ts() would scan ALL dirty pages and blindly call
/// RecordPage::from_bytes() on each one, causing "not a record page (magic: BIDX)" errors.
///
/// Fix: Modified update_versions_commit_ts() to detect and skip index pages (BTree, Property)
/// by catching InvalidArgument errors from RecordPage::from_bytes() and returning empty Vec.

use sombra::{GraphDB, Node, Result, Config};
use tempfile::NamedTempFile;

#[test]
fn test_commit_succeeds_with_btree_index_pages() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    // Use fully durable config to ensure commits go through the problematic code path
    let config = Config::fully_durable();
    let mut db = GraphDB::open_with_config(&path, config)?;

    // Add nodes in multiple transactions to trigger BTree index growth
    // This creates BTree index pages which become dirty
    let mut node_ids = Vec::new();
    for i in 0..100 {
        let mut tx = db.begin_transaction()?;
        let mut node = Node::new(i);
        node.labels.push("TestLabel".to_string());
        
        let node_id = tx.add_node(node)?;
        node_ids.push(node_id);
        
        // This commit should NOT fail with "not a record page (magic: BIDX)" error
        // The fix allows update_versions_commit_ts() to skip BTree index pages
        tx.commit()?;
    }

    // Verify all nodes were committed successfully
    assert_eq!(node_ids.len(), 100, "Should have added 100 nodes");
    
    // Verify we can read all the nodes back
    for node_id in &node_ids {
        assert!(db.get_node(*node_id).is_ok(), "Should be able to read node {}", node_id);
    }

    Ok(())
}

#[test]
fn test_bulk_commit_with_labeled_nodes() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let config = Config::fully_durable();
    let mut db = GraphDB::open_with_config(&path, config)?;

    // Bulk insert with labels to stress the BTree index
    let mut tx = db.begin_transaction()?;
    let mut node_ids = Vec::new();
    for i in 0..500 {
        let mut node = Node::new(i);
        node.labels.push(format!("Label{}", i % 10));
        let node_id = tx.add_node(node)?;
        node_ids.push(node_id);
    }

    // This large commit should handle dirty BTree pages correctly
    tx.commit()?;

    // Verify
    assert_eq!(node_ids.len(), 500, "Should have added 500 nodes");
    for node_id in &node_ids {
        assert!(db.get_node(*node_id).is_ok(), "Should be able to read node {}", node_id);
    }

    Ok(())
}

#[test]
fn test_commit_with_checkpoint_and_btree_pages() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let config = Config::fully_durable();
    let mut db = GraphDB::open_with_config(&path, config)?;

    // First batch
    let mut tx = db.begin_transaction()?;
    let mut batch_a_ids = Vec::new();
    for i in 0..100 {
        let mut node = Node::new(i);
        node.labels.push("BatchA".to_string());
        let node_id = tx.add_node(node)?;
        batch_a_ids.push(node_id);
    }
    tx.commit()?;

    // Checkpoint to persist BTree structure
    db.checkpoint()?;

    // Second batch - this will have both RecordPages and BTree pages dirty
    let mut tx = db.begin_transaction()?;
    let mut batch_b_ids = Vec::new();
    for i in 100..200 {
        let mut node = Node::new(i);
        node.labels.push("BatchB".to_string());
        let node_id = tx.add_node(node)?;
        batch_b_ids.push(node_id);
    }
    
    // Commit should handle mix of dirty page types
    tx.commit()?;

    assert_eq!(batch_a_ids.len() + batch_b_ids.len(), 200, "Should have 200 nodes total");
    
    // Verify all nodes are readable
    for node_id in batch_a_ids.iter().chain(batch_b_ids.iter()) {
        assert!(db.get_node(*node_id).is_ok(), "Should be able to read node {}", node_id);
    }

    Ok(())
}
