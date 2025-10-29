#![allow(clippy::uninlined_format_args)]

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sombra::model::{Edge, Node};
use sombra::{ConcurrentGraphDB, Config, GraphDB, PropertyValue};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

// ============================================================================
// Setup Utilities
// ============================================================================

fn setup_graphdb(path: &str, mvcc_enabled: bool) -> GraphDB {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));

    let mut config = Config::balanced();
    config.mvcc_enabled = mvcc_enabled;
    GraphDB::open_with_config(path, config).unwrap()
}

fn setup_concurrent_db(path: &str) -> ConcurrentGraphDB {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));

    let mut config = Config::balanced();
    config.mvcc_enabled = true;
    ConcurrentGraphDB::open_with_config(path, config).unwrap()
}

fn populate_db(db: &mut GraphDB, node_count: usize) -> Vec<u64> {
    let mut node_ids = Vec::new();
    for i in 0..node_count {
        let mut node = Node::new(i as u64);
        node.labels.push("User".to_string());
        node.properties
            .insert("id".to_string(), PropertyValue::Int(i as i64));
        let node_id = db.add_node(node).unwrap();
        node_ids.push(node_id);
    }

    // Add some edges
    for i in 0..(node_count / 2) {
        let _ = db.add_edge(Edge::new(
            i as u64,
            node_ids[i],
            node_ids[(i + 1) % node_count],
            "follows",
        ));
    }

    db.checkpoint().unwrap();
    node_ids
}

// ============================================================================
// Criterion Benchmarks
// ============================================================================

fn bench_node_creation_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_creation_overhead");
    group.throughput(Throughput::Elements(1));

    // Baseline: GraphDB without MVCC
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("baseline.db");
    let mut db_baseline = setup_graphdb(path.to_str().unwrap(), false);

    group.bench_function("graphdb_no_mvcc", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            let mut node = Node::new(counter);
            node.labels.push("Test".to_string());
            let _ = black_box(db_baseline.add_node(node).unwrap());
            counter += 1;
        });
    });

    // GraphDB with MVCC (single-threaded)
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("mvcc.db");
    let mut db_mvcc = setup_graphdb(path.to_str().unwrap(), true);

    group.bench_function("graphdb_with_mvcc", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            let mut node = Node::new(counter);
            node.labels.push("Test".to_string());
            let _ = black_box(db_mvcc.add_node(node).unwrap());
            counter += 1;
        });
    });

    // ConcurrentGraphDB (MVCC + Mutex)
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("concurrent.db");
    let db_concurrent = setup_concurrent_db(path.to_str().unwrap());

    group.bench_function("concurrent_graphdb", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            let mut tx = db_concurrent.begin_transaction().unwrap();
            let mut node = Node::new(counter);
            node.labels.push("Test".to_string());
            let _ = black_box(tx.add_node(node).unwrap());
            tx.commit().unwrap();
            counter += 1;
        });
    });

    group.finish();
}

fn bench_node_read_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_read_overhead");
    group.throughput(Throughput::Elements(1));

    // Setup databases with 1000 nodes
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("baseline.db");
    let mut db_baseline = setup_graphdb(path.to_str().unwrap(), false);
    let node_ids_baseline = populate_db(&mut db_baseline, 1000);

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("mvcc.db");
    let mut db_mvcc = setup_graphdb(path.to_str().unwrap(), true);
    let node_ids_mvcc = populate_db(&mut db_mvcc, 1000);

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("concurrent.db");
    let db_concurrent = setup_concurrent_db(path.to_str().unwrap());
    let mut tx_setup = db_concurrent.begin_transaction().unwrap();
    let mut node_ids_concurrent = Vec::new();
    for i in 0..1000 {
        let mut node = Node::new(i);
        node.labels.push("User".to_string());
        node_ids_concurrent.push(tx_setup.add_node(node).unwrap());
    }
    tx_setup.commit().unwrap();

    group.bench_function("graphdb_no_mvcc", |b| {
        let mut idx = 0;
        b.iter(|| {
            let node_id = node_ids_baseline[idx % node_ids_baseline.len()];
            let _ = black_box(db_baseline.get_node(node_id).unwrap());
            idx += 1;
        });
    });

    group.bench_function("graphdb_with_mvcc", |b| {
        let mut idx = 0;
        b.iter(|| {
            let node_id = node_ids_mvcc[idx % node_ids_mvcc.len()];
            let _ = black_box(db_mvcc.get_node(node_id).unwrap());
            idx += 1;
        });
    });

    group.bench_function("concurrent_graphdb", |b| {
        let mut idx = 0;
        b.iter(|| {
            let tx = db_concurrent.begin_transaction().unwrap();
            let node_id = node_ids_concurrent[idx % node_ids_concurrent.len()];
            let _ = black_box(tx.get_node(node_id).unwrap());
            tx.commit().unwrap();
            idx += 1;
        });
    });

    group.finish();
}

fn bench_transaction_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_overhead");
    group.throughput(Throughput::Elements(1));

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("concurrent.db");
    let db = setup_concurrent_db(path.to_str().unwrap());

    group.bench_function("begin_commit_empty", |b| {
        b.iter(|| {
            let tx = black_box(db.begin_transaction().unwrap());
            black_box(tx.commit().unwrap());
        });
    });

    group.bench_function("begin_commit_1_node", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            let mut tx = db.begin_transaction().unwrap();
            let mut node = Node::new(counter);
            node.labels.push("Test".to_string());
            let _ = black_box(tx.add_node(node).unwrap());
            tx.commit().unwrap();
            counter += 1;
        });
    });

    group.bench_function("begin_commit_5_nodes", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            let mut tx = db.begin_transaction().unwrap();
            for _ in 0..5 {
                let mut node = Node::new(counter);
                node.labels.push("Test".to_string());
                let _ = tx.add_node(node).unwrap();
                counter += 1;
            }
            tx.commit().unwrap();
        });
    });

    group.finish();
}

// ============================================================================
// Multi-threaded Throughput Benchmarks
// ============================================================================

fn bench_concurrent_throughput_reads() {
    println!("\n=== Concurrent Read Throughput ===");

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("concurrent_reads.db");
    let db = setup_concurrent_db(path.to_str().unwrap());

    // Setup: Create 1000 nodes
    let mut tx = db.begin_transaction().unwrap();
    let mut node_ids = Vec::new();
    for i in 0..1000 {
        let mut node = Node::new(i);
        node.labels.push("User".to_string());
        node_ids.push(tx.add_node(node).unwrap());
    }
    tx.commit().unwrap();
    let node_ids = Arc::new(node_ids);

    // Benchmark with varying thread counts
    for num_threads in [1, 2, 5, 10, 20, 50] {
        let operations_per_thread = 1000;
        let mut handles = vec![];
        let start = Instant::now();

        for _ in 0..num_threads {
            let db = db.clone();
            let node_ids: Arc<Vec<u64>> = Arc::clone(&node_ids);

            let handle = thread::spawn(move || {
                for i in 0..operations_per_thread {
                    let tx = db.begin_transaction().unwrap();
                    let node_id = node_ids[i % node_ids.len()];
                    let _ = tx.get_node(node_id).unwrap();
                    tx.commit().unwrap();
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_ops = num_threads * operations_per_thread;
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
        let avg_latency_us = duration.as_micros() as f64 / total_ops as f64;

        println!(
            "  {} threads: {:.2}ms total, {:.0} ops/sec, {:.2}μs avg latency",
            num_threads,
            duration.as_secs_f64() * 1000.0,
            ops_per_sec,
            avg_latency_us
        );
    }
}

fn bench_concurrent_throughput_writes() {
    println!("\n=== Concurrent Write Throughput ===");

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("concurrent_writes.db");
    let db = setup_concurrent_db(path.to_str().unwrap());

    // Benchmark with varying thread counts
    for num_threads in [1, 2, 5, 10, 20, 50] {
        let operations_per_thread = 100; // Fewer ops for writes
        let mut handles = vec![];
        let start = Instant::now();

        for thread_id in 0..num_threads {
            let db = db.clone();

            let handle = thread::spawn(move || {
                for i in 0..operations_per_thread {
                    let mut tx = db.begin_transaction().unwrap();
                    let node_id = (thread_id * 1000000 + i) as u64;
                    let mut node = Node::new(node_id);
                    node.labels.push(format!("Thread{}", thread_id));
                    let _ = tx.add_node(node).unwrap();
                    tx.commit().unwrap();
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_ops = num_threads * operations_per_thread;
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
        let avg_latency_us = duration.as_micros() as f64 / total_ops as f64;

        println!(
            "  {} threads: {:.2}ms total, {:.0} ops/sec, {:.2}μs avg latency",
            num_threads,
            duration.as_secs_f64() * 1000.0,
            ops_per_sec,
            avg_latency_us
        );
    }
}

fn bench_concurrent_throughput_mixed() {
    println!("\n=== Concurrent Mixed Workload (80% reads, 20% writes) ===");

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("concurrent_mixed.db");
    let db = setup_concurrent_db(path.to_str().unwrap());

    // Setup: Create 1000 nodes
    let mut tx = db.begin_transaction().unwrap();
    let mut node_ids = Vec::new();
    for i in 0..1000 {
        let mut node = Node::new(i);
        node.labels.push("User".to_string());
        node_ids.push(tx.add_node(node).unwrap());
    }
    tx.commit().unwrap();
    let node_ids = Arc::new(node_ids);

    // Benchmark with varying thread counts
    for num_threads in [1, 2, 5, 10, 20, 50] {
        let operations_per_thread = 500;
        let mut handles = vec![];
        let start = Instant::now();

        for thread_id in 0..num_threads {
            let db = db.clone();
            let node_ids: Arc<Vec<u64>> = Arc::clone(&node_ids);

            let handle = thread::spawn(move || {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                let mut read_count = 0;
                let mut write_count = 0;

                for i in 0..operations_per_thread {
                    let is_read = rng.gen_bool(0.8);

                    if is_read {
                        let tx = db.begin_transaction().unwrap();
                        let idx = rng.gen_range(0..node_ids.len());
                        let _ = tx.get_node(node_ids[idx]).unwrap();
                        tx.commit().unwrap();
                        read_count += 1;
                    } else {
                        let mut tx = db.begin_transaction().unwrap();
                        let node_id = (thread_id * 1000000 + i) as u64;
                        let mut node = Node::new(node_id);
                        node.labels.push(format!("Thread{}", thread_id));
                        let _ = tx.add_node(node).unwrap();
                        tx.commit().unwrap();
                        write_count += 1;
                    }
                }

                (read_count, write_count)
            });

            handles.push(handle);
        }

        let mut total_reads = 0;
        let mut total_writes = 0;
        for handle in handles {
            let (reads, writes) = handle.join().unwrap();
            total_reads += reads;
            total_writes += writes;
        }

        let duration = start.elapsed();
        let total_ops = total_reads + total_writes;
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();

        println!(
            "  {} threads: {:.2}ms total, {} reads, {} writes, {:.0} ops/sec",
            num_threads,
            duration.as_secs_f64() * 1000.0,
            total_reads,
            total_writes,
            ops_per_sec
        );
    }
}

// ============================================================================
// Scalability Analysis
// ============================================================================

fn bench_scalability_analysis() {
    println!("\n=== Scalability Analysis ===");
    println!("Measuring speedup vs single-threaded baseline\n");

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("scalability.db");
    let db = setup_concurrent_db(path.to_str().unwrap());

    // Setup: Create 1000 nodes
    let mut tx = db.begin_transaction().unwrap();
    let mut node_ids = Vec::new();
    for i in 0..1000 {
        let mut node = Node::new(i);
        node.labels.push("User".to_string());
        node_ids.push(tx.add_node(node).unwrap());
    }
    tx.commit().unwrap();
    let node_ids = Arc::new(node_ids);

    // Measure single-threaded baseline
    let operations = 10000;
    let start = Instant::now();
    for i in 0..operations {
        let tx = db.begin_transaction().unwrap();
        let node_id = node_ids[i % node_ids.len()];
        let _ = tx.get_node(node_id).unwrap();
        tx.commit().unwrap();
    }
    let baseline_time = start.elapsed();
    let baseline_ops_per_sec = operations as f64 / baseline_time.as_secs_f64();

    println!(
        "Baseline (1 thread): {:.0} ops/sec ({:.2}μs per op)",
        baseline_ops_per_sec,
        baseline_time.as_micros() as f64 / operations as f64
    );
    println!();

    // Test with multiple thread counts
    for num_threads in [2, 4, 8, 16, 32] {
        let operations_per_thread = operations / num_threads;
        let mut handles = vec![];
        let start = Instant::now();

        for _ in 0..num_threads {
            let db = db.clone();
            let node_ids: Arc<Vec<u64>> = Arc::clone(&node_ids);

            let handle = thread::spawn(move || {
                for i in 0..operations_per_thread {
                    let tx = db.begin_transaction().unwrap();
                    let node_id = node_ids[i % node_ids.len()];
                    let _ = tx.get_node(node_id).unwrap();
                    tx.commit().unwrap();
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_ops = num_threads * operations_per_thread;
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
        let speedup = ops_per_sec / baseline_ops_per_sec;
        let efficiency = speedup / num_threads as f64 * 100.0;

        println!(
            "{:2} threads: {:.0} ops/sec, {:.2}x speedup, {:.1}% efficiency",
            num_threads, ops_per_sec, speedup, efficiency
        );
    }
}

// ============================================================================
// Main Benchmark Harness
// ============================================================================

criterion_group!(
    benches,
    bench_node_creation_overhead,
    bench_node_read_overhead,
    bench_transaction_overhead
);

criterion_main!(benches);
