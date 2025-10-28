//! MVCC Profiling Benchmark
//!
//! Focused benchmark for profiling MVCC hot paths.
//! Designed to run long enough for accurate profiling data.

use sombra::db::{Config, GraphDB};
use sombra::model::{Node, PropertyValue};
use std::time::Instant;

fn main() {
    println!("=== MVCC Profiling Benchmark ===\n");
    
    // Profile transaction throughput (write path)
    profile_write_path();
    
    // Profile read path with version chains
    profile_read_path();
    
    println!("\n=== Profiling Complete ===");
}

fn profile_write_path() {
    println!("Profiling write path (10,000 transactions)...");
    
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("profile_writes.db");
    
    let mut config = Config::benchmark();
    config.mvcc_enabled = true;
    config.max_concurrent_transactions = Some(200);
    config.gc_interval_secs = None;
    
    let mut db = GraphDB::open_with_config(path.to_str().unwrap(), config).unwrap();
    
    let start = Instant::now();
    for i in 0..10_000 {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(0);
        node.labels.push("TestNode".to_string());
        node.properties.insert("counter".to_string(), PropertyValue::Int(i));
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
        
        if i % 1000 == 0 && i > 0 {
            println!("  {} transactions...", i);
        }
    }
    let duration = start.elapsed();
    
    println!("Write path: {:.2}ms total, {:.2} txn/sec", 
             duration.as_secs_f64() * 1000.0,
             10_000.0 / duration.as_secs_f64());
}

fn profile_read_path() {
    println!("\nProfiling read path (10,000 reads with version chains)...");
    
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("profile_reads.db");
    
    let mut config = Config::benchmark();
    config.mvcc_enabled = true;
    config.max_concurrent_transactions = Some(200);
    config.gc_interval_secs = None;
    
    let mut db = GraphDB::open_with_config(path.to_str().unwrap(), config).unwrap();
    
    // Create 100 nodes and update each 10 times (version chains of depth 10)
    let mut node_ids = Vec::new();
    for _i in 0..100 {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(0);
        node.labels.push("TestNode".to_string());
        node.properties.insert("value".to_string(), PropertyValue::Int(0));
        let node_id = tx.add_node(node).unwrap();
        node_ids.push(node_id);
        tx.commit().unwrap();
    }
    
    // Create version chains
    for update_round in 1..=10 {
        for &node_id in &node_ids {
            let mut tx = db.begin_transaction().unwrap();
            if let Some(mut node) = tx.get_node(node_id).unwrap() {
                node.properties.insert("value".to_string(), PropertyValue::Int(update_round));
                tx.add_node(node).unwrap();
            }
            tx.commit().unwrap();
        }
    }
    
    println!("  Created 100 nodes with version chains (depth 10)");
    
    // Now profile reads
    let start = Instant::now();
    for i in 0..10_000 {
        let mut tx = db.begin_transaction().unwrap();
        let node_id = node_ids[i % node_ids.len()];
        let _ = tx.get_node(node_id).unwrap();
        tx.commit().unwrap();
        
        if i % 1000 == 0 && i > 0 {
            println!("  {} reads...", i);
        }
    }
    let duration = start.elapsed();
    
    println!("Read path: {:.2}ms total, {:.2}μs per read",
             duration.as_secs_f64() * 1000.0,
             duration.as_secs_f64() * 1_000_000.0 / 10_000.0);
}
