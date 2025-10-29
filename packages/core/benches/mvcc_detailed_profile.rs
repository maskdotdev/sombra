//! MVCC Detailed Profiling Benchmark
//!
//! Instruments MVCC operations to identify performance bottlenecks.

use sombra::db::{Config, GraphDB};
use sombra::model::{Node, PropertyValue};
use std::time::{Duration, Instant};

fn main() {
    println!("=== MVCC Detailed Performance Profiling ===\n");

    // Profile each component separately
    profile_transaction_overhead();
    profile_node_creation();
    profile_version_chain_reads();
    profile_wal_overhead();

    println!("\n=== Profiling Complete ===");
}

fn profile_transaction_overhead() {
    println!("1. Transaction Overhead Analysis");
    println!("   Testing begin + immediate commit (no work)");

    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("txn_overhead.db");

    // MVCC mode
    let mut config = Config::benchmark();
    config.mvcc_enabled = true;
    config.gc_interval_secs = None;

    let mut db = GraphDB::open_with_config(path.to_str().unwrap(), config).unwrap();

    let iterations = 10_000;
    let start = Instant::now();

    for _ in 0..iterations {
        let tx = db.begin_transaction().unwrap();
        tx.commit().unwrap();
    }

    let duration = start.elapsed();
    let avg_us = duration.as_micros() as f64 / iterations as f64;

    println!("   MVCC: {:.2}µs per empty transaction", avg_us);
    println!(
        "   Total: {:.2}ms for {} transactions",
        duration.as_secs_f64() * 1000.0,
        iterations
    );

    // Single-writer mode comparison
    drop(db);
    let temp_dir2 = tempfile::TempDir::new().unwrap();
    let path2 = temp_dir2.path().join("txn_overhead_sw.db");

    let mut config = Config::benchmark();
    config.mvcc_enabled = false;

    let mut db = GraphDB::open_with_config(path2.to_str().unwrap(), config).unwrap();

    let start = Instant::now();

    for _ in 0..iterations {
        let tx = db.begin_transaction().unwrap();
        tx.commit().unwrap();
    }

    let duration_sw = start.elapsed();
    let avg_us_sw = duration_sw.as_micros() as f64 / iterations as f64;

    println!("   Single-Writer: {:.2}µs per empty transaction", avg_us_sw);
    println!(
        "   Overhead: {:.2}µs ({:.1}x slower)\n",
        avg_us - avg_us_sw,
        avg_us / avg_us_sw
    );
}

fn profile_node_creation() {
    println!("2. Node Creation Analysis");
    println!("   Testing node creation with properties");

    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("node_creation.db");

    let mut config = Config::benchmark();
    config.mvcc_enabled = true;
    config.gc_interval_secs = None;

    let mut db = GraphDB::open_with_config(path.to_str().unwrap(), config).unwrap();

    let iterations = 5_000;
    let mut timings = Vec::new();

    for i in 0..iterations {
        let tx_start = Instant::now();
        let mut tx = db.begin_transaction().unwrap();
        let tx_begin = tx_start.elapsed();

        let node_start = Instant::now();
        let mut node = Node::new(0);
        node.labels.push("Benchmark".to_string());
        node.properties
            .insert("id".to_string(), PropertyValue::Int(i));
        node.properties.insert(
            "data".to_string(),
            PropertyValue::String("test".to_string()),
        );
        tx.add_node(node).unwrap();
        let node_add = node_start.elapsed();

        let commit_start = Instant::now();
        tx.commit().unwrap();
        let commit_time = commit_start.elapsed();

        timings.push((tx_begin, node_add, commit_time));
    }

    let avg_tx_begin = avg_duration(&timings.iter().map(|(t, _, _)| *t).collect::<Vec<_>>());
    let avg_node_add = avg_duration(&timings.iter().map(|(_, t, _)| *t).collect::<Vec<_>>());
    let avg_commit = avg_duration(&timings.iter().map(|(_, _, t)| *t).collect::<Vec<_>>());

    println!("   Average timings ({}  iterations):", iterations);
    println!(
        "   - begin_transaction(): {:.2}µs",
        avg_tx_begin.as_micros() as f64
    );
    println!(
        "   - add_node():          {:.2}µs",
        avg_node_add.as_micros() as f64
    );
    println!(
        "   - commit():            {:.2}µs",
        avg_commit.as_micros() as f64
    );
    println!(
        "   - Total:               {:.2}µs\n",
        (avg_tx_begin + avg_node_add + avg_commit).as_micros() as f64
    );
}

fn profile_version_chain_reads() {
    println!("3. Version Chain Read Analysis");
    println!("   Testing reads with different chain depths");

    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("version_reads.db");

    let mut config = Config::benchmark();
    config.mvcc_enabled = true;
    config.gc_interval_secs = None;

    let mut db = GraphDB::open_with_config(path.to_str().unwrap(), config).unwrap();

    // Create a single node
    let node_id = {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(0);
        node.labels.push("VersionTest".to_string());
        node.properties
            .insert("value".to_string(), PropertyValue::Int(0));
        let id = tx.add_node(node).unwrap();
        tx.commit().unwrap();
        id
    };

    // Test read performance at different chain depths
    for depth in [0, 5, 10, 20, 50] {
        // Add versions up to target depth
        let current_depth = if depth == 0 {
            0
        } else {
            depth - if depth == 5 { 0 } else { 5 }
        };
        for v in 0..current_depth {
            let mut tx = db.begin_transaction().unwrap();
            if let Some(mut node) = tx.get_node(node_id).unwrap() {
                node.properties
                    .insert("value".to_string(), PropertyValue::Int(v as i64));
                tx.add_node(node).unwrap();
            }
            tx.commit().unwrap();
        }

        // Measure read performance
        let iterations = 1_000;
        let start = Instant::now();

        for _ in 0..iterations {
            let mut tx = db.begin_transaction().unwrap();
            let _ = tx.get_node(node_id).unwrap();
            tx.commit().unwrap();
        }

        let duration = start.elapsed();
        let avg_us = duration.as_micros() as f64 / iterations as f64;

        println!("   Chain depth {}: {:.2}µs per read", depth, avg_us);
    }
    println!();
}

fn profile_wal_overhead() {
    println!("4. WAL Write Overhead Analysis");
    println!("   Comparing transaction with/without WAL writes");

    // This is harder to measure directly, but we can compare:
    // - Empty transaction (just timestamp allocation)
    // - Transaction with node creation (WAL write included)

    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("wal_overhead.db");

    let mut config = Config::benchmark();
    config.mvcc_enabled = true;
    config.gc_interval_secs = None;

    let mut db = GraphDB::open_with_config(path.to_str().unwrap(), config).unwrap();

    // Measure empty transaction
    let iterations = 5_000;
    let start = Instant::now();

    for _ in 0..iterations {
        let tx = db.begin_transaction().unwrap();
        tx.commit().unwrap();
    }

    let empty_duration = start.elapsed();
    let empty_avg = empty_duration.as_micros() as f64 / iterations as f64;

    // Measure transaction with minimal write
    let start = Instant::now();

    for i in 0..iterations {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(0);
        node.properties
            .insert("x".to_string(), PropertyValue::Int(i));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    let write_duration = start.elapsed();
    let write_avg = write_duration.as_micros() as f64 / iterations as f64;

    println!("   Empty transaction:     {:.2}µs", empty_avg);
    println!("   With node write:       {:.2}µs", write_avg);
    println!(
        "   WAL/Version overhead:  {:.2}µs ({:.1}%)\n",
        write_avg - empty_avg,
        ((write_avg - empty_avg) / write_avg) * 100.0
    );
}

fn avg_duration(durations: &[Duration]) -> Duration {
    let total: Duration = durations.iter().sum();
    total / durations.len() as u32
}
