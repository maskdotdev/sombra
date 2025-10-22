#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use rand::Rng;
use sombra::db::{Config, GraphDB};
use sombra::model::{Edge, Node};
use tempfile::TempDir;

fn setup_code_graph_debug(path: &str, node_count: usize) -> (GraphDB, Vec<u64>) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));

    let config = Config {
        page_cache_size: 2000,
        ..Config::balanced()
    };
    let mut db = GraphDB::open_with_config(path, config).unwrap();

    let mut node_ids = Vec::new();
    for _ in 0..node_count {
        let node_id = db.add_node(Node::new(0)).unwrap();
        node_ids.push(node_id);
    }

    println!("Created {node_count} nodes");

    let mut rng = rand::thread_rng();
    for i in 0..node_count {
        let calls = rng.gen_range(2..8);
        for _ in 0..calls {
            let target = rng.gen_range(0..node_count);
            db.add_edge(Edge::new(0, node_ids[i], node_ids[target], "CALLS"))
                .ok();
        }

        let contains = rng.gen_range(1..5);
        for _ in 0..contains {
            let target = rng.gen_range(0..node_count);
            db.add_edge(Edge::new(0, node_ids[i], node_ids[target], "CONTAINS"))
                .ok();
        }

        let imports = rng.gen_range(0..3);
        for _ in 0..imports {
            let target = rng.gen_range(0..node_count);
            db.add_edge(Edge::new(0, node_ids[i], node_ids[target], "IMPORTS"))
                .ok();
        }
    }

    println!("Added edges");

    // Check nodes BEFORE checkpoint
    println!(
        "
Before checkpoint - checking first 10 nodes:"
    );
    for &node_id in node_ids.iter().take(10) {
        match db.get_node(node_id) {
            Ok(_) => print!("{node_id} "),
            Err(e) => print!("{node_id}[ERR:{e:?}] "),
        }
    }
    println!();

    println!(
        "
Calling checkpoint..."
    );
    db.checkpoint().unwrap();
    println!("Checkpoint returned OK");

    // Check nodes AFTER checkpoint
    println!(
        "
After checkpoint - checking first 10 nodes:"
    );
    for &node_id in node_ids.iter().take(10) {
        match db.get_node(node_id) {
            Ok(_) => print!("{node_id} "),
            Err(e) => print!("{node_id}[ERR:{e:?}] "),
        }
    }
    println!();

    // Check all nodes
    println!(
        "
Checking all {node_count} nodes..."
    );
    let mut missing = Vec::new();
    for (idx, &node_id) in node_ids.iter().enumerate() {
        if db.get_node(node_id).is_err() {
            missing.push((idx, node_id));
        }
    }

    if !missing.is_empty() {
        println!("Missing {} nodes:", missing.len());
        for (idx, node_id) in &missing[..missing.len().min(20)] {
            println!("  Index {idx}: Node ID {node_id}");
        }
    } else {
        println!("All nodes present!");
    }

    (db, node_ids)
}

#[test]
fn test_debug_checkpoint_issue() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("bench.db");
    let (mut db, node_ids) = setup_code_graph_debug(path.to_str().unwrap(), 1000);

    // Final verification
    let mut missing = 0;
    for &node_id in &node_ids {
        if db.get_node(node_id).is_err() {
            missing += 1;
        }
    }

    assert_eq!(missing, 0, "{missing} nodes are missing after checkpoint!");
}
