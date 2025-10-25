#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use sombra::{GraphDB, Node, PropertyValue, Result};
use std::collections::BTreeMap;
use tempfile::NamedTempFile;

#[test]
fn investigate_btree_delete_behavior_small() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path)?;
    let mut tx = db.begin_transaction()?;

    let mut node_ids = Vec::new();
    for _ in 0..10 {
        let node = Node::new(0);
        let node_id = tx.add_node(node)?;
        node_ids.push(node_id);
    }

    tx.commit()?;
    db.checkpoint()?;

    let all_before = db.get_all_node_ids_ordered();
    eprintln!(
        "
=== BEFORE DELETION ==="
    );
    eprintln!("Nodes in BTree index: {}", all_before.len());
    eprintln!("Node IDs: {:?}", all_before);

    let mut tx = db.begin_transaction()?;
    for (idx, node_id) in node_ids.iter().enumerate() {
        if idx % 2 == 0 {
            eprintln!("Deleting node {}", node_id);
            tx.delete_node(*node_id)?;
        }
    }
    tx.commit()?;
    db.checkpoint()?;

    let all_after = db.get_all_node_ids_ordered();
    eprintln!(
        "
=== AFTER DELETION ==="
    );
    eprintln!("Nodes in BTree index: {}", all_after.len());
    eprintln!("Node IDs: {:?}", all_after);

    eprintln!(
        "
=== TRYING TO RETRIEVE NODES ==="
    );
    let mut tx = db.begin_transaction()?;
    let mut exists_count = 0;
    for node_id in &all_after {
        match tx.get_node(*node_id) {
            Ok(_) => {
                eprintln!("Node {} EXISTS", node_id);
                exists_count += 1;
            }
            Err(e) => {
                eprintln!("Node {} MISSING: {:?}", node_id, e);
            }
        }
    }
    tx.commit()?;

    eprintln!(
        "
=== SUMMARY ==="
    );
    eprintln!("BTree index contains: {} node IDs", all_after.len());
    eprintln!("Actually exist: {}", exists_count);
    eprintln!(
        "Missing (ghost entries): {}",
        all_after.len() - exists_count
    );

    assert_eq!(exists_count, 5, "Should have 5 remaining nodes");
    assert_eq!(
        all_after.len(),
        5,
        "BTree should only contain 5 node IDs after deletion"
    );

    Ok(())
}

#[test]
fn investigate_btree_delete_behavior_large() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    eprintln!(
        "
=== CREATING 5000 NODES ==="
    );
    let mut db = GraphDB::open(&path)?;
    let mut tx = db.begin_transaction()?;

    let mut node_ids = Vec::new();
    for i in 0..5000 {
        let mut node = Node::new(0);
        node.labels.push("Test".to_string());
        let mut props = BTreeMap::new();
        props.insert("seq".to_string(), PropertyValue::Int(i));
        node.properties = props;
        let node_id = tx.add_node(node)?;
        node_ids.push(node_id);
    }

    tx.commit()?;
    db.checkpoint()?;

    let all_before = db.get_all_node_ids_ordered();
    eprintln!("Created {} nodes", all_before.len());

    eprintln!(
        "
=== DELETING EVERY OTHER NODE (2500 deletions) ==="
    );
    let mut tx = db.begin_transaction()?;
    let mut deleted_count = 0;
    for (idx, node_id) in node_ids.iter().enumerate() {
        if idx % 2 == 0 {
            tx.delete_node(*node_id)?;
            deleted_count += 1;
        }
    }
    eprintln!("Deleted {} nodes in transaction", deleted_count);
    tx.commit()?;
    db.checkpoint()?;

    let all_after = db.get_all_node_ids_ordered();
    eprintln!(
        "
=== AFTER DELETION ==="
    );
    eprintln!("BTree index contains: {} node IDs", all_after.len());
    eprintln!("Expected: {} node IDs", 2500);

    eprintln!(
        "
=== SAMPLING NODE RETRIEVAL (first 50) ==="
    );
    let mut tx = db.begin_transaction()?;

    let sample_size = 50.min(all_after.len());
    for node_id in all_after.iter().take(sample_size) {
        match tx.get_node(*node_id) {
            Ok(Some(node)) => {
                let seq = match node.properties.get("seq") {
                    Some(PropertyValue::Int(s)) => *s,
                    _ => -1,
                };
                eprintln!("Node {} EXISTS (seq={})", node_id, seq);
            }
            Ok(None) => {
                eprintln!("Node {} MISSING: returned None", node_id);
            }
            Err(e) => {
                eprintln!("Node {} ERROR: {:?}", node_id, e);
            }
        }
    }

    // Count all
    let mut total_exists = 0;
    let mut total_missing = 0;
    for node_id in &all_after {
        match tx.get_node(*node_id) {
            Ok(_) => total_exists += 1,
            Err(_) => total_missing += 1,
        }
    }
    tx.commit()?;

    eprintln!(
        "
=== SUMMARY ==="
    );
    eprintln!("BTree index contains: {} node IDs", all_after.len());
    eprintln!("Actually exist: {}", total_exists);
    eprintln!("Missing (ghost entries): {}", total_missing);
    eprintln!("Expected to exist: 2500");

    if total_missing > 0 {
        eprintln!(
            "
!!! ISSUE FOUND !!!"
        );
        eprintln!(
            "BTree index contains {} ghost entries (deleted nodes still in index)",
            total_missing
        );
    }

    Ok(())
}
