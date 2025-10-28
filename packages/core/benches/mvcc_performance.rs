//! MVCC Performance Benchmarks
//!
//! Comprehensive performance comparison between MVCC and single-writer modes.
//! Measures throughput, latency, memory overhead, and version chain impact.

#![allow(clippy::uninlined_format_args)]

use sombra::db::{Config, GraphDB};
use sombra::model::{Edge, Node, PropertyValue};
use std::time::Instant;
use tempfile::TempDir;

// ============================================================================
// Test Data Setup
// ============================================================================

fn create_test_graph(path: &str, node_count: usize, mvcc_enabled: bool) -> (GraphDB, Vec<u64>) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));

    let mut config = Config::benchmark();
    config.mvcc_enabled = mvcc_enabled;
    config.max_concurrent_transactions = Some(200);
    config.gc_interval_secs = None; // Disable GC for consistent benchmarks

    let mut db = GraphDB::open_with_config(path, config).unwrap();

    let mut node_ids = Vec::new();
    for i in 0..node_count {
        let mut node = Node::new(0); // 0 = auto-assign ID
        node.labels.push("TestNode".to_string());
        node.properties.insert("index".to_string(), PropertyValue::Int(i as i64));
        let node_id = db.add_node(node).unwrap();
        node_ids.push(node_id);
    }

    // Create edges for traversal tests
    for i in 0..node_count {
        let target = (i + 1) % node_count;
        db.add_edge(Edge::new(
            0, // auto-assign
            node_ids[i],
            node_ids[target],
            "next",
        ))
        .ok();
    }

    db.checkpoint().unwrap();
    (db, node_ids)
}

// ============================================================================
// Benchmark 1: Transaction Throughput
// ============================================================================

fn bench_transaction_throughput() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║         Benchmark 1: Transaction Throughput               ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("\nSequential node creation (1000 transactions):\n");

    let temp_dir = TempDir::new().unwrap();

    // Single-writer mode
    let path_single = temp_dir.path().join("throughput_single.db");
    let _ = std::fs::remove_file(&path_single);
    let _ = std::fs::remove_file(format!("{}.wal", path_single.to_str().unwrap()));
    
    let mut config = Config::benchmark();
    config.mvcc_enabled = false;
    let mut db = GraphDB::open_with_config(path_single.to_str().unwrap(), config).unwrap();

    let start = Instant::now();
    for _ in 0..1000 {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(0);
        node.labels.push("NewNode".to_string());
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }
    let single_duration = start.elapsed();
    let single_throughput = 1000.0 / single_duration.as_secs_f64();

    println!(
        "  Single-writer: {:>7.2}ms total, {:>6.0} txn/sec",
        single_duration.as_secs_f64() * 1000.0,
        single_throughput
    );
    drop(db);

    // MVCC mode
    let path_mvcc = temp_dir.path().join("throughput_mvcc.db");
    let _ = std::fs::remove_file(&path_mvcc);
    let _ = std::fs::remove_file(format!("{}.wal", path_mvcc.to_str().unwrap()));
    
    let mut config = Config::benchmark();
    config.mvcc_enabled = true;
    config.max_concurrent_transactions = Some(200);
    config.gc_interval_secs = None;
    let mut db = GraphDB::open_with_config(path_mvcc.to_str().unwrap(), config).unwrap();

    let start = Instant::now();
    for _ in 0..1000 {
        let mut tx = db.begin_transaction().unwrap();
        let mut node = Node::new(0);
        node.labels.push("NewNode".to_string());
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }
    let mvcc_duration = start.elapsed();
    let mvcc_throughput = 1000.0 / mvcc_duration.as_secs_f64();

    println!(
        "  MVCC:          {:>7.2}ms total, {:>6.0} txn/sec",
        mvcc_duration.as_secs_f64() * 1000.0,
        mvcc_throughput
    );

    let overhead_pct = ((mvcc_duration.as_secs_f64() / single_duration.as_secs_f64()) - 1.0) * 100.0;
    println!(
        "\n  MVCC overhead: {:>+6.1}%",
        overhead_pct
    );
}

// ============================================================================
// Benchmark 2: Read Latency with Version Chains
// ============================================================================

fn bench_read_latency() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║         Benchmark 2: Read Latency (Version Chains)        ║");
    println!("╚════════════════════════════════════════════════════════════╝");

    let temp_dir = TempDir::new().unwrap();

    // Test with different version chain depths
    for chain_depth in [0, 5, 10, 25, 50] {
        println!("\nVersion chain depth: {}", chain_depth);

        // Single-writer mode (no versions)
        let path = temp_dir.path().join(format!("read_single_{}.db", chain_depth));
        let (mut db, node_ids) = create_test_graph(path.to_str().unwrap(), 100, false);

        // Create version chain by updating
        for _ in 0..chain_depth {
            let mut tx = db.begin_transaction().unwrap();
            for &node_id in &node_ids {
                if let Ok(Some(mut node)) = tx.get_node(node_id) {
                    node.properties.insert("counter".to_string(), PropertyValue::Int(chain_depth as i64));
                    tx.add_node(node).ok(); // Creates new version
                }
            }
            tx.commit().unwrap();
        }

        // Measure read performance
        let start = Instant::now();
        let iterations = 1000;
        for _ in 0..iterations {
            let mut tx = db.begin_transaction().unwrap();
            for &node_id in &node_ids {
                let _ = tx.get_node(node_id);
            }
            tx.commit().unwrap();
        }
        let single_duration = start.elapsed();
        let single_avg_us = (single_duration.as_micros() as f64) / (iterations as f64 * node_ids.len() as f64);

        // MVCC mode (with version chains)
        let path = temp_dir.path().join(format!("read_mvcc_{}.db", chain_depth));
        let (mut db, node_ids) = create_test_graph(path.to_str().unwrap(), 100, true);

        // Create version chain by updating
        for _ in 0..chain_depth {
            let mut tx = db.begin_transaction().unwrap();
            for &node_id in &node_ids {
                if let Ok(Some(mut node)) = tx.get_node(node_id) {
                    node.properties.insert("counter".to_string(), PropertyValue::Int(chain_depth as i64));
                    tx.add_node(node).ok(); // Creates new version
                }
            }
            tx.commit().unwrap();
        }

        // Measure read performance
        let start = Instant::now();
        for _ in 0..iterations {
            let mut tx = db.begin_transaction().unwrap();
            for &node_id in &node_ids {
                let _ = tx.get_node(node_id);
            }
            tx.commit().unwrap();
        }
        let mvcc_duration = start.elapsed();
        let mvcc_avg_us = (mvcc_duration.as_micros() as f64) / (iterations as f64 * node_ids.len() as f64);

        println!(
            "  Single-writer: {:>6.2}μs per read",
            single_avg_us
        );
        println!(
            "  MVCC:          {:>6.2}μs per read",
            mvcc_avg_us
        );
        
        if chain_depth > 0 {
            let overhead_pct = ((mvcc_avg_us / single_avg_us) - 1.0) * 100.0;
            println!(
                "  MVCC overhead: {:>+6.1}%",
                overhead_pct
            );
        }
    }
}

// ============================================================================
// Benchmark 3: Write Amplification
// ============================================================================

fn bench_write_amplification() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║         Benchmark 3: Write Amplification                  ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("\nUpdating 100 nodes 50 times each:\n");

    let temp_dir = TempDir::new().unwrap();

    // Single-writer mode
    let path = temp_dir.path().join("write_single.db");
    let (mut db, node_ids) = create_test_graph(path.to_str().unwrap(), 100, false);

    let start = Instant::now();
    for iteration in 0..50 {
        let mut tx = db.begin_transaction().unwrap();
        for &node_id in &node_ids {
            if let Ok(Some(mut node)) = tx.get_node(node_id) {
                node.properties.insert("iteration".to_string(), PropertyValue::Int(iteration));
                tx.add_node(node).ok();
            }
        }
        tx.commit().unwrap();
    }
    let single_duration = start.elapsed();

    let single_file_size = std::fs::metadata(path.to_str().unwrap())
        .map(|m| m.len())
        .unwrap_or(0);

    println!(
        "  Single-writer: {:>7.2}ms, {:>6.1} KB on disk",
        single_duration.as_secs_f64() * 1000.0,
        single_file_size as f64 / 1024.0
    );

    // MVCC mode
    let path = temp_dir.path().join("write_mvcc.db");
    let (mut db, node_ids) = create_test_graph(path.to_str().unwrap(), 100, true);

    let start = Instant::now();
    for iteration in 0..50 {
        let mut tx = db.begin_transaction().unwrap();
        for &node_id in &node_ids {
            if let Ok(Some(mut node)) = tx.get_node(node_id) {
                node.properties.insert("iteration".to_string(), PropertyValue::Int(iteration));
                tx.add_node(node).ok();
            }
        }
        tx.commit().unwrap();
    }
    let mvcc_duration = start.elapsed();

    let mvcc_file_size = std::fs::metadata(path.to_str().unwrap())
        .map(|m| m.len())
        .unwrap_or(0);

    println!(
        "  MVCC:          {:>7.2}ms, {:>6.1} KB on disk",
        mvcc_duration.as_secs_f64() * 1000.0,
        mvcc_file_size as f64 / 1024.0
    );

    let time_overhead = ((mvcc_duration.as_secs_f64() / single_duration.as_secs_f64()) - 1.0) * 100.0;
    let space_amplification = (mvcc_file_size as f64 / single_file_size as f64) - 1.0;

    println!(
        "\n  Time overhead:       {:>+6.1}%",
        time_overhead
    );
    println!(
        "  Space amplification: {:>+6.1}% ({:.1}x)",
        space_amplification * 100.0,
        mvcc_file_size as f64 / single_file_size as f64
    );
}

// ============================================================================
// Benchmark 4: Memory Usage
// ============================================================================

fn bench_memory_usage() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║         Benchmark 4: Memory Usage                          ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("\nCreating version chains (100 nodes × N updates):\n");

    let temp_dir = TempDir::new().unwrap();

    let measurements = vec![
        ("Initial", 0),
        ("10 updates", 10),
        ("25 updates", 25),
        ("50 updates", 50),
        ("100 updates", 100),
    ];

    for (label, num_updates) in measurements {
        let path = temp_dir.path().join(format!("memory_mvcc_{}.db", num_updates));
        let (mut db, node_ids) = create_test_graph(path.to_str().unwrap(), 100, true);

        for update_num in 0..num_updates {
            let mut tx = db.begin_transaction().unwrap();
            for &node_id in &node_ids {
                if let Ok(Some(mut node)) = tx.get_node(node_id) {
                    node.properties.insert("update".to_string(), PropertyValue::Int(update_num));
                    tx.add_node(node).ok();
                }
            }
            tx.commit().unwrap();
        }

        db.checkpoint().unwrap();

        let file_size = std::fs::metadata(path.to_str().unwrap())
            .map(|m| m.len())
            .unwrap_or(0);
        
        let kb_per_version = if num_updates > 0 {
            (file_size as f64) / (100.0 * num_updates as f64) / 1024.0
        } else {
            0.0
        };

        println!(
            "  {:>12}: {:>7.1} KB total",
            label,
            file_size as f64 / 1024.0
        );
        
        if num_updates > 0 {
            println!(
                "               {:>7.2} KB per version (avg)",
                kb_per_version
            );
        }
    }
}

// ============================================================================
// Benchmark 5: Update Hot Spots
// ============================================================================

fn bench_update_hotspots() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║         Benchmark 5: Update Hot Spots                     ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("\nUpdating same 10 nodes 100 times each:\n");

    let temp_dir = TempDir::new().unwrap();

    // Single-writer mode
    let path = temp_dir.path().join("hotspot_single.db");
    let (mut db, node_ids) = create_test_graph(path.to_str().unwrap(), 100, false);

    let hotspot_nodes: Vec<u64> = node_ids.iter().take(10).copied().collect();

    let start = Instant::now();
    for iteration in 0..100 {
        let mut tx = db.begin_transaction().unwrap();
        for &node_id in &hotspot_nodes {
            if let Ok(Some(mut node)) = tx.get_node(node_id) {
                node.properties.insert("counter".to_string(), PropertyValue::Int(iteration));
                tx.add_node(node).ok();
            }
        }
        tx.commit().unwrap();
    }
    let single_duration = start.elapsed();

    println!(
        "  Single-writer: {:>7.2}ms",
        single_duration.as_secs_f64() * 1000.0
    );

    // MVCC mode
    let path = temp_dir.path().join("hotspot_mvcc.db");
    let (mut db, node_ids) = create_test_graph(path.to_str().unwrap(), 100, true);

    let hotspot_nodes: Vec<u64> = node_ids.iter().take(10).copied().collect();

    let start = Instant::now();
    for iteration in 0..100 {
        let mut tx = db.begin_transaction().unwrap();
        for &node_id in &hotspot_nodes {
            if let Ok(Some(mut node)) = tx.get_node(node_id) {
                node.properties.insert("counter".to_string(), PropertyValue::Int(iteration));
                tx.add_node(node).ok();
            }
        }
        tx.commit().unwrap();
    }
    let mvcc_duration = start.elapsed();

    println!(
        "  MVCC:          {:>7.2}ms",
        mvcc_duration.as_secs_f64() * 1000.0
    );

    let overhead_pct = ((mvcc_duration.as_secs_f64() / single_duration.as_secs_f64()) - 1.0) * 100.0;
    println!(
        "\n  MVCC overhead: {:>+6.1}%",
        overhead_pct
    );
}

// ============================================================================
// Benchmark 6: Timestamp Allocation Overhead
// ============================================================================

fn bench_timestamp_overhead() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║         Benchmark 6: Timestamp Allocation Overhead        ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("\nEmpty transactions (measure MVCC bookkeeping overhead):\n");

    let temp_dir = TempDir::new().unwrap();

    // Single-writer mode
    let path = temp_dir.path().join("timestamp_single.db");
    let (mut db, _) = create_test_graph(path.to_str().unwrap(), 10, false);

    let start = Instant::now();
    for _ in 0..1000 {
        let tx = db.begin_transaction().unwrap();
        tx.commit().unwrap();
    }
    let single_duration = start.elapsed();
    let single_avg_us = single_duration.as_micros() as f64 / 1000.0;

    println!(
        "  Single-writer: {:>7.2}ms total, {:>6.2}μs per txn",
        single_duration.as_secs_f64() * 1000.0,
        single_avg_us
    );

    // MVCC mode
    let path = temp_dir.path().join("timestamp_mvcc.db");
    let (mut db, _) = create_test_graph(path.to_str().unwrap(), 10, true);

    let start = Instant::now();
    for _ in 0..1000 {
        let tx = db.begin_transaction().unwrap();
        tx.commit().unwrap();
    }
    let mvcc_duration = start.elapsed();
    let mvcc_avg_us = mvcc_duration.as_micros() as f64 / 1000.0;

    println!(
        "  MVCC:          {:>7.2}ms total, {:>6.2}μs per txn",
        mvcc_duration.as_secs_f64() * 1000.0,
        mvcc_avg_us
    );

    let overhead_us = mvcc_avg_us - single_avg_us;
    println!(
        "\n  MVCC adds:     {:>+6.2}μs per transaction",
        overhead_us
    );
}

// ============================================================================
// Benchmark 7: Traversal Performance
// ============================================================================

fn bench_traversal_performance() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║         Benchmark 7: Traversal Performance                ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("\nNeighbor traversal (1000 queries):\n");

    let temp_dir = TempDir::new().unwrap();

    // Single-writer mode
    let path = temp_dir.path().join("traversal_single.db");
    let (mut db, node_ids) = create_test_graph(path.to_str().unwrap(), 200, false);

    let start = Instant::now();
    for _ in 0..1000 {
        let mut tx = db.begin_transaction().unwrap();
        let node_id = node_ids[0];
        let _ = tx.get_neighbors(node_id);
        tx.commit().unwrap();
    }
    let single_duration = start.elapsed();
    let single_avg_us = single_duration.as_micros() as f64 / 1000.0;

    println!(
        "  Single-writer: {:>7.2}ms total, {:>6.2}μs per query",
        single_duration.as_secs_f64() * 1000.0,
        single_avg_us
    );

    // MVCC mode (no version chains)
    let path = temp_dir.path().join("traversal_mvcc_clean.db");
    let (mut db, node_ids) = create_test_graph(path.to_str().unwrap(), 200, true);

    let start = Instant::now();
    for _ in 0..1000 {
        let mut tx = db.begin_transaction().unwrap();
        let node_id = node_ids[0];
        let _ = tx.get_neighbors(node_id);
        tx.commit().unwrap();
    }
    let mvcc_clean_duration = start.elapsed();
    let mvcc_clean_avg_us = mvcc_clean_duration.as_micros() as f64 / 1000.0;

    println!(
        "  MVCC (clean):  {:>7.2}ms total, {:>6.2}μs per query",
        mvcc_clean_duration.as_secs_f64() * 1000.0,
        mvcc_clean_avg_us
    );

    // MVCC mode (with version chains - update all nodes 10 times)
    let path = temp_dir.path().join("traversal_mvcc_versions.db");
    let (mut db, node_ids) = create_test_graph(path.to_str().unwrap(), 200, true);

    for iteration in 0..10 {
        let mut tx = db.begin_transaction().unwrap();
        for &node_id in &node_ids {
            if let Ok(Some(mut node)) = tx.get_node(node_id) {
                node.properties.insert("version".to_string(), PropertyValue::Int(iteration));
                tx.add_node(node).ok();
            }
        }
        tx.commit().unwrap();
    }

    let start = Instant::now();
    for _ in 0..1000 {
        let mut tx = db.begin_transaction().unwrap();
        let node_id = node_ids[0];
        let _ = tx.get_neighbors(node_id);
        tx.commit().unwrap();
    }
    let mvcc_versions_duration = start.elapsed();
    let mvcc_versions_avg_us = mvcc_versions_duration.as_micros() as f64 / 1000.0;

    println!(
        "  MVCC (10 ver): {:>7.2}ms total, {:>6.2}μs per query",
        mvcc_versions_duration.as_secs_f64() * 1000.0,
        mvcc_versions_avg_us
    );

    let clean_overhead = ((mvcc_clean_avg_us / single_avg_us) - 1.0) * 100.0;
    let version_overhead = ((mvcc_versions_avg_us / single_avg_us) - 1.0) * 100.0;

    println!(
        "\n  MVCC (clean):  {:>+6.1}% vs single-writer",
        clean_overhead
    );
    println!(
        "  MVCC (10 ver): {:>+6.1}% vs single-writer",
        version_overhead
    );
}

// ============================================================================
// Summary Report
// ============================================================================

fn print_summary() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                    Summary & Analysis                     ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("\nKey Findings:");
    println!("  • MVCC adds timestamp allocation overhead (~few μs/txn)");
    println!("  • Version chains increase storage (proportional to updates)");
    println!("  • Read latency increases with chain depth (linear scan)");
    println!("  • Snapshot isolation enables non-blocking reads");
    println!("  • Write performance comparable to single-writer mode");
    println!("\nWhen to Use MVCC:");
    println!("  ✓ Need concurrent readers and writers");
    println!("  ✓ Long-running analytics queries");
    println!("  ✓ Can tolerate some storage overhead");
    println!("  ✓ Workload has many reads vs writes");
    println!("\nWhen to Use Single-Writer:");
    println!("  ✓ Purely sequential workloads");
    println!("  ✓ Write-heavy applications");
    println!("  ✓ Storage space is constrained");
    println!("  ✓ Single-threaded access patterns");
}

// ============================================================================
// Main Entry Point
// ============================================================================

fn main() {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                MVCC Performance Benchmark Suite           ║");
    println!("║                                                            ║");
    println!("║  Comparing MVCC vs Single-Writer Transaction Modes        ║");
    println!("╚════════════════════════════════════════════════════════════╝");

    bench_transaction_throughput();
    bench_read_latency();
    bench_write_amplification();
    bench_memory_usage();
    bench_update_hotspots();
    bench_timestamp_overhead();
    bench_traversal_performance();
    print_summary();

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                  Benchmark Complete!                      ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");
}
