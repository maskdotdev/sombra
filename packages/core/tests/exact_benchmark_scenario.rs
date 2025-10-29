#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use rand::Rng;
use sombra::db::{Config, GraphDB};
use sombra::model::{Edge, Node};
use tempfile::TempDir;

fn setup_code_graph(path: &str, node_count: usize) -> (GraphDB, Vec<u64>) {
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

    db.checkpoint().unwrap();
    (db, node_ids)
}

#[test]
fn test_exact_benchmark_scenario() {
    for node_count in [100, 500, 1000] {
        println!(
            "
=== Testing with {node_count} nodes ==="
        );
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("bench.db");
        let (db, node_ids) = setup_code_graph(path.to_str().unwrap(), node_count);

        println!(
            "  Setup {} nodes, first 10 IDs: {:?}",
            node_count,
            &node_ids[..10]
        );

        let mut missing = 0;
        for &node_id in node_ids.iter().take(10) {
            match db.get_node(node_id) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("    Node {node_id} not found: {e:?}");
                    missing += 1;
                }
            }
        }

        if missing > 0 {
            panic!("{missing} nodes missing out of first 10!");
        }

        println!("  ✓ All first 10 nodes found");
    }
}
