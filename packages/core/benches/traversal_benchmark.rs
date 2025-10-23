#![allow(clippy::uninlined_format_args)]

use sombra::db::{Config, GraphDB};
use sombra::model::{Edge, Node};
use std::time::Instant;
use tempfile::TempDir;

fn setup_chain_graph(path: &str, chain_length: usize) -> GraphDB {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));

    let mut db = GraphDB::open_with_config(path, Config::balanced()).unwrap();

    let mut prev_id = db.add_node(Node::new(0)).unwrap();
    for i in 1..chain_length {
        let node_id = db.add_node(Node::new(i as u64)).unwrap();
        db.add_edge(Edge::new(0, prev_id, node_id, "next")).unwrap();
        prev_id = node_id;
    }

    db.checkpoint().unwrap();
    db
}

fn setup_star_graph(path: &str, neighbor_count: usize) -> (GraphDB, u64) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));

    let mut db = GraphDB::open_with_config(path, Config::balanced()).unwrap();

    let center = db.add_node(Node::new(9999)).unwrap();
    for i in 0..neighbor_count {
        let node_id = db.add_node(Node::new(i as u64)).unwrap();
        db.add_edge(Edge::new(0, center, node_id, "connected"))
            .unwrap();
    }

    db.checkpoint().unwrap();
    (db, center)
}

fn setup_social_graph(path: &str, user_count: usize, avg_friends: usize) -> (GraphDB, Vec<u64>) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));

    let mut db = GraphDB::open_with_config(path, Config::balanced()).unwrap();

    let mut user_ids = Vec::new();
    for i in 0..user_count {
        let user_id = db.add_node(Node::new(i as u64)).unwrap();
        user_ids.push(user_id);
    }

    use rand::Rng;
    let mut rng = rand::thread_rng();
    for i in 0..user_count {
        let friend_count = rng.gen_range(avg_friends / 2..avg_friends * 2);
        for _ in 0..friend_count {
            let friend_idx = rng.gen_range(0..user_count);
            if friend_idx != i {
                db.add_edge(Edge::new(0, user_ids[i], user_ids[friend_idx], "friend"))
                    .ok();
            }
        }
    }

    db.checkpoint().unwrap();
    (db, user_ids)
}

fn bench_get_neighbors() {
    println!("\n=== get_neighbors Benchmark ===");

    for neighbor_count in [10, 100, 1000] {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("bench.db");
        let (mut db, center) = setup_star_graph(path.to_str().unwrap(), neighbor_count);

        let iterations = 1000;
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = db.get_neighbors(center).unwrap();
        }
        let duration = start.elapsed();

        println!(
            "  {} neighbors: {:.3}ms per op ({:.0} ops/sec)",
            neighbor_count,
            duration.as_secs_f64() * 1000.0 / iterations as f64,
            iterations as f64 / duration.as_secs_f64()
        );
    }
}

fn bench_get_neighbors_cache() {
    println!("\n=== get_neighbors Cache Hit Benchmark ===");

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("bench.db");
    let (mut db, center) = setup_star_graph(path.to_str().unwrap(), 1000);

    db.get_neighbors(center).unwrap();

    let iterations = 10000;
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = db.get_neighbors(center).unwrap();
    }
    let duration = start.elapsed();

    println!(
        "  1000 neighbors (cached): {:.3}µs per op ({:.0} ops/sec)",
        duration.as_secs_f64() * 1_000_000.0 / iterations as f64,
        iterations as f64 / duration.as_secs_f64()
    );
}

fn bench_two_hop() {
    println!("\n=== Two-Hop Traversal Benchmark ===");

    for neighbor_count in [10, 50, 100] {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("bench.db");
        let (mut db, center) = setup_star_graph(path.to_str().unwrap(), neighbor_count);

        let iterations = 100;
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = db.get_neighbors_two_hops(center).unwrap();
        }
        let duration = start.elapsed();

        println!(
            "  {} neighbors: {:.3}ms per op ({:.0} ops/sec)",
            neighbor_count,
            duration.as_secs_f64() * 1000.0 / iterations as f64,
            iterations as f64 / duration.as_secs_f64()
        );
    }
}

fn bench_three_hop() {
    println!("\n=== Three-Hop Traversal Benchmark ===");

    for neighbor_count in [5, 10, 20] {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("bench.db");
        let (mut db, center) = setup_star_graph(path.to_str().unwrap(), neighbor_count);

        let iterations = 50;
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = db.get_neighbors_three_hops(center).unwrap();
        }
        let duration = start.elapsed();

        println!(
            "  {} neighbors: {:.3}ms per op ({:.0} ops/sec)",
            neighbor_count,
            duration.as_secs_f64() * 1000.0 / iterations as f64,
            iterations as f64 / duration.as_secs_f64()
        );
    }
}

fn bench_bfs() {
    println!("\n=== BFS Traversal Benchmark ===");

    for depth in [2, 4, 6] {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("bench.db");
        let (mut db, user_ids) = setup_social_graph(path.to_str().unwrap(), 100, 10);

        let iterations = 100;
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = db.bfs_traversal(user_ids[0], depth).unwrap();
        }
        let duration = start.elapsed();

        println!(
            "  depth {}: {:.3}ms per op ({:.0} ops/sec)",
            depth,
            duration.as_secs_f64() * 1000.0 / iterations as f64,
            iterations as f64 / duration.as_secs_f64()
        );
    }
}

fn bench_parallel_bfs() {
    println!("\n=== Parallel BFS Benchmark ===");

    for depth in [2, 4, 6] {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("bench.db");
        let (mut db, user_ids) = setup_social_graph(path.to_str().unwrap(), 100, 10);

        let iterations = 100;
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = db.parallel_bfs(user_ids[0], depth).unwrap();
        }
        let duration = start.elapsed();

        println!(
            "  depth {}: {:.3}ms per op ({:.0} ops/sec)",
            depth,
            duration.as_secs_f64() * 1000.0 / iterations as f64,
            iterations as f64 / duration.as_secs_f64()
        );
    }
}

fn bench_parallel_multi_hop() {
    println!("\n=== Parallel Multi-Hop Benchmark ===");

    for batch_size in [10, 50, 100] {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("bench.db");
        let (mut db, user_ids) = setup_social_graph(path.to_str().unwrap(), 200, 15);
        let batch = &user_ids[..batch_size];

        let iterations = 50;
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = db.parallel_multi_hop_neighbors(batch, 2).unwrap();
        }
        let duration = start.elapsed();

        println!(
            "  batch {}: {:.3}ms per op ({:.0} ops/sec)",
            batch_size,
            duration.as_secs_f64() * 1000.0 / iterations as f64,
            iterations as f64 / duration.as_secs_f64()
        );
    }
}

fn bench_chain_traversal() {
    println!("\n=== Chain Traversal Benchmark ===");

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("bench.db");
    let mut db = setup_chain_graph(path.to_str().unwrap(), 100);

    let iterations = 100;
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = db.bfs_traversal(1, 100).unwrap();
    }
    let duration = start.elapsed();

    println!(
        "  chain length 100: {:.3}ms per op ({:.0} ops/sec)",
        duration.as_secs_f64() * 1000.0 / iterations as f64,
        iterations as f64 / duration.as_secs_f64()
    );
}

fn main() {
    println!("Sombra Graph Traversal Benchmarks");
    println!("==================================");

    bench_get_neighbors();
    bench_get_neighbors_cache();
    bench_two_hop();
    bench_three_hop();
    bench_bfs();
    bench_parallel_bfs();
    bench_parallel_multi_hop();
    bench_chain_traversal();

    println!("\n✓ All traversal benchmarks completed");
}
