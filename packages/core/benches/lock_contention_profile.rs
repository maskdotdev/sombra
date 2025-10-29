#![allow(clippy::uninlined_format_args)]

//! Lock contention profiling benchmark
//! Run with: cargo bench --bench lock_contention_profile

use sombra::model::Node;
use sombra::{ConcurrentGraphDB, Config};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

static LOCK_WAIT_TIME_NS: AtomicU64 = AtomicU64::new(0);
static OPERATION_TIME_NS: AtomicU64 = AtomicU64::new(0);
static TOTAL_OPS: AtomicU64 = AtomicU64::new(0);

fn setup_concurrent_db(path: &str) -> ConcurrentGraphDB {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));

    let mut config = Config::balanced();
    config.mvcc_enabled = true;
    ConcurrentGraphDB::open_with_config(path, config).unwrap()
}

fn bench_lock_contention() {
    println!("\n=== Lock Contention Analysis ===\n");

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("lock_profile.db");
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

    // Test with different thread counts
    for num_threads in [1, 2, 5, 10, 20, 50] {
        // Reset counters
        LOCK_WAIT_TIME_NS.store(0, Ordering::Relaxed);
        OPERATION_TIME_NS.store(0, Ordering::Relaxed);
        TOTAL_OPS.store(0, Ordering::Relaxed);

        let operations_per_thread = 1000;
        let mut handles = vec![];
        let start = Instant::now();

        for _ in 0..num_threads {
            let db = db.clone();
            let node_ids = Arc::clone(&node_ids);

            let handle = thread::spawn(move || {
                for i in 0..operations_per_thread {
                    let op_start = Instant::now();

                    // Measure time to begin transaction (lock acquisition)
                    let tx_start = Instant::now();
                    let tx = db.begin_transaction().unwrap();
                    let tx_duration = tx_start.elapsed();

                    // Measure time for actual operation
                    let node_id = node_ids[i % node_ids.len()];
                    let _ = tx.get_node(node_id).unwrap();

                    // Measure commit time (lock acquisition)
                    let commit_start = Instant::now();
                    tx.commit().unwrap();
                    let commit_duration = commit_start.elapsed();

                    let op_duration = op_start.elapsed();

                    // Track timings
                    let lock_time = tx_duration + commit_duration;
                    LOCK_WAIT_TIME_NS.fetch_add(lock_time.as_nanos() as u64, Ordering::Relaxed);
                    OPERATION_TIME_NS.fetch_add(op_duration.as_nanos() as u64, Ordering::Relaxed);
                    TOTAL_OPS.fetch_add(1, Ordering::Relaxed);
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_ops = TOTAL_OPS.load(Ordering::Relaxed);
        let lock_wait_ns = LOCK_WAIT_TIME_NS.load(Ordering::Relaxed);
        let operation_ns = OPERATION_TIME_NS.load(Ordering::Relaxed);

        let lock_wait_pct = (lock_wait_ns as f64 / operation_ns as f64) * 100.0;
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();

        println!("  {} threads:", num_threads);
        println!("    Total time: {:.2}ms", duration.as_secs_f64() * 1000.0);
        println!("    Throughput: {:.0} ops/sec", ops_per_sec);
        println!(
            "    Lock wait time: {:.2}% of total operation time",
            lock_wait_pct
        );
        println!(
            "    Avg lock wait per op: {:.2}μs",
            (lock_wait_ns as f64 / total_ops as f64) / 1000.0
        );
        println!(
            "    Avg total per op: {:.2}μs",
            (operation_ns as f64 / total_ops as f64) / 1000.0
        );
        println!();
    }
}

fn main() {
    bench_lock_contention();
}
