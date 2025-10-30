#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use sombra::db::{Config, GraphDB};
use sombra::model::Node;
use tempfile::TempDir;

#[test]
fn test_btree_serialization_large() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("test.db");

    let config = Config {
        page_cache_size: 100,
        ..Config::balanced()
    };
    let mut db = GraphDB::open_with_config(path.to_str().unwrap(), config).unwrap();

    // Create 1000 nodes
    let node_ids: Vec<u64> = (0..1000)
        .map(|i| db.add_node(Node::new(i)).unwrap())
        .collect();

    println!("Created {} nodes", node_ids.len());
    println!("First 10 IDs: {:?}", &node_ids[..10]);

    // Verify all nodes exist before checkpoint
    for &node_id in &node_ids {
        assert!(
            db.get_node(node_id).is_ok(),
            "Node {node_id} should exist before checkpoint"
        );
    }

    // Do checkpoint (this persists and reloads btree)
    println!("Calling checkpoint...");
    db.checkpoint().unwrap();
    println!("Checkpoint complete");

    // Verify all nodes still exist after checkpoint
    let mut missing_nodes = Vec::new();
    for (idx, &node_id) in node_ids.iter().enumerate() {
        if db.get_node(node_id).is_err() {
            missing_nodes.push((idx, node_id));
            if missing_nodes.len() <= 10 {
                println!("Missing node at index {idx}: ID {node_id}");
            }
        }
    }

    if !missing_nodes.is_empty() {
        panic!(
            "After checkpoint, {} out of {} nodes are missing! First 10: {:?}",
            missing_nodes.len(),
            node_ids.len(),
            &missing_nodes[..missing_nodes.len().min(10)]
        );
    }

    println!("All {} nodes verified after checkpoint", node_ids.len());
}

#[test]
fn test_btree_serialization_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("test.db");
    let path_str = path.to_str().unwrap().to_string();

    let node_ids: Vec<u64>;

    // Create database and nodes
    {
        let config = Config {
            page_cache_size: 100,
            ..Config::balanced()
        };
        let mut db = GraphDB::open_with_config(&path_str, config).unwrap();

        node_ids = (0..1000)
            .map(|i| db.add_node(Node::new(i)).unwrap())
            .collect();

        println!("Created {} nodes", node_ids.len());

        // Checkpoint to persist
        db.checkpoint().unwrap();
        println!("Checkpoint complete");
    } // db closed

    // Reopen database
    {
        let config = Config {
            page_cache_size: 100,
            ..Config::balanced()
        };
        let db = GraphDB::open_with_config(&path_str, config).unwrap();

        println!("Database reopened");

        // Verify all nodes exist after reopening
        let mut missing_nodes = Vec::new();
        for (idx, &node_id) in node_ids.iter().enumerate() {
            if db.get_node(node_id).is_err() {
                missing_nodes.push((idx, node_id));
                if missing_nodes.len() <= 10 {
                    println!("Missing node at index {idx}: ID {node_id}");
                }
            }
        }

        if !missing_nodes.is_empty() {
            panic!(
                "After reopen, {} out of {} nodes are missing!",
                missing_nodes.len(),
                node_ids.len()
            );
        }

        println!("All {} nodes verified after reopen", node_ids.len());
    }
}
