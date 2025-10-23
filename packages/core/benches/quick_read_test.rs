use sombra::db::Config;
use sombra::sqlite_adapter::SqliteGraphDB;
use sombra::{Edge, GraphDB, Node};
use std::time::Instant;
use tempfile::TempDir;

fn main() {
    println!("=== Quick Read Benchmark Test ===\n");

    let temp_dir = TempDir::new().unwrap();
    let sombra_path = temp_dir.path().join("sombra_test.db");
    let sqlite_path = temp_dir.path().join("sqlite_test.db");

    let node_count = 1000;
    let edge_count = 5000;

    let mut nodes = Vec::new();
    for i in 1..=node_count {
        let mut node = Node::new(i);
        node.labels.push("User".to_string());
        nodes.push(node);
    }

    let mut edges = Vec::new();
    for i in 0..edge_count {
        let source = (i % node_count) + 1;
        let target = ((i + 1) % node_count) + 1;
        edges.push(Edge::new(i, source, target, "FOLLOWS"));
    }

    println!("Setting up Sombra database...");
    {
        let mut db = GraphDB::open_with_config(&sombra_path, Config::balanced()).unwrap();
        let mut tx = db.begin_transaction().unwrap();
        for node in &nodes {
            tx.add_node(node.clone()).unwrap();
        }
        for edge in &edges {
            tx.add_edge(edge.clone()).unwrap();
        }
        tx.commit().unwrap();
    }

    println!("Setting up SQLite database...");
    {
        let mut db = SqliteGraphDB::new(sqlite_path.to_str().unwrap()).unwrap();
        db.bulk_insert_nodes(&nodes).unwrap();
        db.bulk_insert_edges(&edges).unwrap();
    }

    let sample_ids: Vec<u64> = (1..=100).collect();

    println!("\n--- Sombra Read Test (DB opened once) ---");
    let mut sombra_db = GraphDB::open_with_config(&sombra_path, Config::balanced()).unwrap();

    let start = Instant::now();
    for &node_id in &sample_ids {
        let _node = sombra_db.get_node(node_id).unwrap();
    }
    let duration = start.elapsed();
    println!(
        "get_node: {} reads in {:.3}ms ({:.2}µs/op, {:.0} ops/sec)",
        sample_ids.len(),
        duration.as_secs_f64() * 1000.0,
        duration.as_micros() as f64 / sample_ids.len() as f64,
        sample_ids.len() as f64 / duration.as_secs_f64()
    );

    let start = Instant::now();
    for &node_id in &sample_ids {
        let _neighbors = sombra_db.get_neighbors(node_id).unwrap();
    }
    let duration = start.elapsed();
    println!(
        "get_neighbors: {} reads in {:.3}ms ({:.2}µs/op, {:.0} ops/sec)",
        sample_ids.len(),
        duration.as_secs_f64() * 1000.0,
        duration.as_micros() as f64 / sample_ids.len() as f64,
        sample_ids.len() as f64 / duration.as_secs_f64()
    );

    println!("\n--- SQLite Read Test (DB opened once) ---");
    let mut sqlite_db = SqliteGraphDB::new(sqlite_path.to_str().unwrap()).unwrap();

    let start = Instant::now();
    for &node_id in &sample_ids {
        let _node = sqlite_db.get_node(node_id).unwrap();
    }
    let duration = start.elapsed();
    println!(
        "get_node: {} reads in {:.3}ms ({:.2}µs/op, {:.0} ops/sec)",
        sample_ids.len(),
        duration.as_secs_f64() * 1000.0,
        duration.as_micros() as f64 / sample_ids.len() as f64,
        sample_ids.len() as f64 / duration.as_secs_f64()
    );

    let start = Instant::now();
    for &node_id in &sample_ids {
        let _neighbors = sqlite_db.get_neighbors(node_id).unwrap();
    }
    let duration = start.elapsed();
    println!(
        "get_neighbors: {} reads in {:.3}ms ({:.2}µs/op, {:.0} ops/sec)",
        sample_ids.len(),
        duration.as_secs_f64() * 1000.0,
        duration.as_micros() as f64 / sample_ids.len() as f64,
        sample_ids.len() as f64 / duration.as_secs_f64()
    );

    println!("\n✓ Quick read test completed!");
}
