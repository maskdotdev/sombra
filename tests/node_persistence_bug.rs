#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use sombra::db::{Config, GraphDB};
use sombra::model::{Edge, Node};
use tempfile::TempDir;

#[test]
fn test_nodes_persist_after_checkpoint_minimal() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("test.db");

    let mut db = GraphDB::open_with_config(path.to_str().unwrap(), Config::balanced()).unwrap();

    let node_ids: Vec<u64> = (0..10)
        .map(|i| db.add_node(Node::new(i)).unwrap())
        .collect();

    println!("Created nodes: {node_ids:?}");

    for &node_id in &node_ids {
        let node = db.get_node(node_id).unwrap();
        println!("Before checkpoint - Node {node_id}: {node:?}");
    }

    db.checkpoint().unwrap();
    println!(
        "
After checkpoint:"
    );

    for &node_id in &node_ids {
        match db.get_node(node_id) {
            Ok(node) => println!("Node {node_id}: {node:?}"),
            Err(e) => println!("ERROR: Node {node_id} not found: {e:?}"),
        }
    }

    for &node_id in &node_ids {
        assert!(
            db.get_node(node_id).is_ok(),
            "Node {node_id} should exist after checkpoint"
        );
    }
}

#[test]
fn test_nodes_persist_after_checkpoint_large() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("test.db");

    let config = Config {
        page_cache_size: 100,
        ..Config::balanced()
    };
    let mut db = GraphDB::open_with_config(path.to_str().unwrap(), config).unwrap();

    let node_ids: Vec<u64> = (0..1000)
        .map(|i| db.add_node(Node::new(i)).unwrap())
        .collect();

    println!("Created 1000 nodes, first 10: {:?}", &node_ids[..10]);

    db.checkpoint().unwrap();
    println!(
        "
Checking nodes after checkpoint..."
    );

    let mut missing_nodes = Vec::new();
    for (idx, &node_id) in node_ids.iter().enumerate() {
        if db.get_node(node_id).is_err() {
            missing_nodes.push((idx, node_id));
        }
    }

    if !missing_nodes.is_empty() {
        println!("Missing {} nodes:", missing_nodes.len());
        for (idx, node_id) in &missing_nodes {
            println!("  Index {idx}: Node ID {node_id}");
        }
    }

    assert!(
        missing_nodes.is_empty(),
        "Expected all nodes to persist, but {} nodes are missing",
        missing_nodes.len()
    );
}

#[test]
fn test_nodes_with_edges_persist_after_checkpoint() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("test.db");

    let config = Config {
        page_cache_size: 100,
        ..Config::balanced()
    };
    let mut db = GraphDB::open_with_config(path.to_str().unwrap(), config).unwrap();

    let node_ids: Vec<u64> = (0..1000)
        .map(|i| db.add_node(Node::new(i)).unwrap())
        .collect();

    for i in 0..999 {
        db.add_edge(Edge::new(0, node_ids[i], node_ids[i + 1], "next"))
            .ok();
    }

    println!("Created 1000 nodes with edges");

    db.checkpoint().unwrap();
    println!(
        "
Checking nodes after checkpoint..."
    );

    let mut missing_nodes = Vec::new();
    for (idx, &node_id) in node_ids.iter().enumerate() {
        if db.get_node(node_id).is_err() {
            missing_nodes.push((idx, node_id));
        }
    }

    if !missing_nodes.is_empty() {
        println!("Missing {} nodes:", missing_nodes.len());
        for (idx, node_id) in &missing_nodes {
            println!("  Index {idx}: Node ID {node_id}");
        }
    }

    assert!(
        missing_nodes.is_empty(),
        "Expected all nodes to persist, but {} nodes are missing",
        missing_nodes.len()
    );
}
