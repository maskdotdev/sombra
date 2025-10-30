#![allow(clippy::uninlined_format_args)]

/// Test to reproduce checkpoint BIDX page error with MVCC enabled
/// 
/// Error from mvcc_performance.rs benchmark:
/// InvalidArgument("not a record page (magic: \"BIDX\")")
/// Occurs during `db.checkpoint()` with MVCC enabled

use sombra::{GraphDB, Node, Edge, PropertyValue, Result, Config};
use tempfile::NamedTempFile;

#[test]
fn test_mvcc_checkpoint_with_btree_pages() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    // Use MVCC config like the benchmark
    let mut config = Config::benchmark();
    config.mvcc_enabled = true;
    config.max_concurrent_transactions = Some(200);
    config.gc_interval_secs = None; // Disable GC for consistent benchmarks

    let mut db = GraphDB::open_with_config(&path, config)?;

    // Create test graph similar to mvcc_performance.rs
    let node_count = 100;
    let mut node_ids = Vec::new();
    for i in 0..node_count {
        let mut node = Node::new(0); // 0 = auto-assign ID
        node.labels.push("TestNode".to_string());
        node.properties
            .insert("index".to_string(), PropertyValue::Int(i as i64));
        let node_id = db.add_node(node)?;
        node_ids.push(node_id);
    }

    // Create edges for traversal tests
    for i in 0..node_count {
        let target = (i + 1) % node_count;
        db.add_edge(Edge::new(
            0, // auto-assign
            node_ids[i],
            node_ids[target],
            "next",
        ))?;
    }

    // This checkpoint should NOT fail with "not a record page (magic: BIDX)" error
    println!("Running checkpoint...");
    db.checkpoint()?;
    println!("Checkpoint completed successfully!");

    Ok(())
}

#[test]
fn test_mvcc_checkpoint_after_updates() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut config = Config::benchmark();
    config.mvcc_enabled = true;
    config.max_concurrent_transactions = Some(200);
    config.gc_interval_secs = None;

    let mut db = GraphDB::open_with_config(&path, config)?;

    // Create initial nodes
    let mut node_ids = Vec::new();
    for i in 0..100 {
        let mut node = Node::new(0);
        node.labels.push("TestNode".to_string());
        node.properties.insert("index".to_string(), PropertyValue::Int(i));
        let node_id = db.add_node(node)?;
        node_ids.push(node_id);
    }

    // Update nodes to create version chains (like bench_memory_usage)
    for update_num in 0..10 {
        let mut tx = db.begin_transaction()?;
        for &node_id in &node_ids {
            if let Ok(Some(mut node)) = tx.get_node(node_id) {
                node.properties
                    .insert("update".to_string(), PropertyValue::Int(update_num));
                tx.add_node(node)?;
            }
        }
        tx.commit()?;
    }

    // This is where the error occurs in bench_memory_usage
    println!("Running checkpoint after updates...");
    db.checkpoint()?;
    println!("Checkpoint completed successfully!");

    Ok(())
}
