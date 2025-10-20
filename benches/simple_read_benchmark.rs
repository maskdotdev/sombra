use sombra::db::{Config, GraphDB};
use sombra::model::{Edge, Node};
use std::time::Instant;

fn setup_database(path: &str, count: usize) -> (GraphDB, Vec<u64>) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));

    let mut db = GraphDB::open_with_config(path, Config::balanced()).unwrap();

    println!("Inserting {count} nodes...");
    let insert_start = Instant::now();

    let mut node_ids = Vec::with_capacity(count);
    let mut tx = db.begin_transaction().unwrap();
    for i in 0..count {
        let mut node = Node::new(i as u64);
        node.labels.push("User".to_string());
        let node_id = tx.add_node(node).unwrap();
        node_ids.push(node_id);

        if i > 0 && i % 10 == 0 {
            let source = node_ids[i];
            let target = node_ids[i - 1];
            tx.add_edge(Edge::new(0, source, target, "FOLLOWS"))
                .unwrap();
        }

        if i % 10000 == 0 && i > 0 {
            print!(".");
            std::io::Write::flush(&mut std::io::stdout()).unwrap();
        }
    }
    tx.commit().unwrap();

    let insert_duration = insert_start.elapsed();
    println!(
        "\nInserted {} nodes in {:.2}s ({:.0} ops/sec)",
        count,
        insert_duration.as_secs_f64(),
        count as f64 / insert_duration.as_secs_f64()
    );

    (db, node_ids)
}

fn benchmark_sequential_reads(db: &mut GraphDB, node_ids: &[u64], iterations: usize) {
    println!("\n=== Sequential Read Benchmark ===");
    let mut total_duration = std::time::Duration::ZERO;

    for iter in 0..iterations {
        let start = Instant::now();
        for &node_id in node_ids {
            let _node = db.get_node(node_id).unwrap();
        }
        let duration = start.elapsed();
        total_duration += duration;

        println!(
            "Iteration {}: {} reads in {:.3}s ({:.0} ops/sec, {:.2}µs/op)",
            iter + 1,
            node_ids.len(),
            duration.as_secs_f64(),
            node_ids.len() as f64 / duration.as_secs_f64(),
            duration.as_micros() as f64 / node_ids.len() as f64
        );
    }

    let avg_duration = total_duration / iterations as u32;
    println!(
        "\nAverage: {} reads in {:.3}s ({:.0} ops/sec, {:.2}µs/op)",
        node_ids.len(),
        avg_duration.as_secs_f64(),
        node_ids.len() as f64 / avg_duration.as_secs_f64(),
        avg_duration.as_micros() as f64 / node_ids.len() as f64
    );
}

fn benchmark_random_reads(db: &mut GraphDB, node_ids: &[u64], reads: usize) {
    println!("\n=== Random Read Benchmark ===");

    use std::collections::hash_map::RandomState;
    use std::hash::BuildHasher;
    let hasher = RandomState::new();

    let mut ids = Vec::new();
    for i in 0..reads {
        let idx = (hasher.hash_one(i) % node_ids.len() as u64) as usize;
        ids.push(node_ids[idx]);
    }

    let start = Instant::now();
    for &id in &ids {
        let _node = db.get_node(id).unwrap();
    }
    let duration = start.elapsed();

    println!(
        "{} random reads in {:.3}s ({:.0} ops/sec, {:.2}µs/op)",
        reads,
        duration.as_secs_f64(),
        reads as f64 / duration.as_secs_f64(),
        duration.as_micros() as f64 / reads as f64
    );
}

fn benchmark_neighbor_queries(db: &mut GraphDB, node_ids: &[u64], queries: usize) {
    println!("\n=== Neighbor Query Benchmark ===");

    let query_count = queries.min(node_ids.len());
    let start = Instant::now();
    for &node_id in node_ids.iter().take(query_count) {
        let _neighbors = db.get_neighbors(node_id).unwrap();
    }
    let duration = start.elapsed();

    println!(
        "{} neighbor queries in {:.3}s ({:.0} ops/sec, {:.2}µs/op)",
        query_count,
        duration.as_secs_f64(),
        query_count as f64 / duration.as_secs_f64(),
        duration.as_micros() as f64 / query_count as f64
    );
}

fn benchmark_cache_effectiveness(db: &mut GraphDB, node_ids: &[u64]) {
    println!("\n=== Cache Effectiveness Benchmark ===");

    let hot_set_size = node_ids.len().min(1000);

    println!("Cold reads (first time):");
    let start = Instant::now();
    for &node_id in node_ids.iter().take(hot_set_size) {
        let _node = db.get_node(node_id).unwrap();
    }
    let cold_duration = start.elapsed();
    println!(
        "  {} reads in {:.3}s ({:.2}µs/op)",
        hot_set_size,
        cold_duration.as_secs_f64(),
        cold_duration.as_micros() as f64 / hot_set_size as f64
    );

    println!("Hot reads (cached):");
    let start = Instant::now();
    for &node_id in node_ids.iter().take(hot_set_size) {
        let _node = db.get_node(node_id).unwrap();
    }
    let hot_duration = start.elapsed();
    println!(
        "  {} reads in {:.3}s ({:.2}µs/op)",
        hot_set_size,
        hot_duration.as_secs_f64(),
        hot_duration.as_micros() as f64 / hot_set_size as f64
    );

    let speedup = cold_duration.as_micros() as f64 / hot_duration.as_micros() as f64;
    println!("  Cache speedup: {speedup:.2}x");
}

fn print_metrics(db: &GraphDB) {
    println!("\n=== Performance Metrics ===");
    println!("Node lookups: {}", db.metrics.node_lookups);
    println!("Cache hits: {}", db.metrics.cache_hits);
    println!("Cache misses: {}", db.metrics.cache_misses);
    if db.metrics.node_lookups > 0 {
        let hit_rate = (db.metrics.cache_hits as f64 / db.metrics.node_lookups as f64) * 100.0;
        println!("Cache hit rate: {hit_rate:.1}%");
    }
}

fn main() {
    let sizes = vec![(1_000, "1K"), (10_000, "10K"), (100_000, "100K")];

    for (count, name) in sizes {
        println!("\n{}", "=".repeat(60));
        println!("BENCHMARK: {name} nodes");
        println!("{}", "=".repeat(60));

        let (mut db, node_ids) = setup_database("bench_temp.db", count);

        benchmark_sequential_reads(&mut db, &node_ids, 3);
        benchmark_random_reads(&mut db, &node_ids, 1000);
        benchmark_neighbor_queries(&mut db, &node_ids, 1000);
        benchmark_cache_effectiveness(&mut db, &node_ids);

        print_metrics(&db);

        std::fs::remove_file("bench_temp.db").ok();
        std::fs::remove_file("bench_temp.db.wal").ok();
    }
}
