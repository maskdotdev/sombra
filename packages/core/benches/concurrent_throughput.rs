#![allow(clippy::uninlined_format_args)]

//! Manual throughput benchmarks for ConcurrentGraphDB
//! Run with: cargo run --release --bench concurrent_throughput

use sombra::{ConcurrentGraphDB, Config, PropertyValue};
use sombra::model::Node;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

fn setup_concurrent_db(path: &str) -> ConcurrentGraphDB {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));

    let mut config = Config::balanced();
    config.mvcc_enabled = true;
    ConcurrentGraphDB::open_with_config(path, config).unwrap()
}

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

fn main() {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║       ConcurrentGraphDB Performance Benchmarks            ║");
    println!("╚════════════════════════════════════════════════════════════╝");

    bench_concurrent_throughput_reads();
    bench_concurrent_throughput_writes();
    bench_concurrent_throughput_mixed();
    bench_scalability_analysis();

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                    Benchmark Complete                     ║");
    println!("╚════════════════════════════════════════════════════════════╝");
}
