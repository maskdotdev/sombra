use sombra::{data_generator::DataGenerator, Edge, GraphDB, Node};
use std::time::Instant;
use tempfile::TempDir;

fn main() {
    println!("=== Comprehensive Checksum Performance Benchmark ===\n");
    println!("Running throughput tests with checksums ENABLED and DISABLED");
    println!("Target: < 3% performance impact with checksums enabled\n");

    let mut data_gen = DataGenerator::new();

    run_benchmark_suite("Small", data_gen.generate_small_dataset(), 5);
    run_benchmark_suite("Medium", data_gen.generate_medium_dataset(), 5);
    run_benchmark_suite("Large", data_gen.generate_large_dataset(), 3);
}

fn run_benchmark_suite(name: &str, dataset: (Vec<Node>, Vec<Edge>), num_runs: usize) {
    let (nodes, edges) = dataset;
    let total_ops = nodes.len() + edges.len();

    println!("\n============================================================");
    println!("=== {} Dataset Benchmark ===", name);
    println!("Total operations: {} nodes + {} edges = {}", nodes.len(), edges.len(), total_ops);
    println!("============================================================");

    println!("\n--- Checksums DISABLED (baseline) ---");
    let mut baseline_times = Vec::new();
    for run in 1..=num_runs {
        let elapsed = run_throughput_test(&nodes, &edges, false);
        baseline_times.push(elapsed);
        println!(
            "  Run {}: {:.2}ms ({:.0} ops/sec)",
            run,
            elapsed.as_secs_f64() * 1000.0,
            total_ops as f64 / elapsed.as_secs_f64()
        );
    }

    println!("\n--- Checksums ENABLED ---");
    let mut checksum_times = Vec::new();
    for run in 1..=num_runs {
        let elapsed = run_throughput_test(&nodes, &edges, true);
        checksum_times.push(elapsed);
        println!(
            "  Run {}: {:.2}ms ({:.0} ops/sec)",
            run,
            elapsed.as_secs_f64() * 1000.0,
            total_ops as f64 / elapsed.as_secs_f64()
        );
    }

    baseline_times.sort();
    checksum_times.sort();

    let baseline_median = baseline_times[num_runs / 2];
    let checksum_median = checksum_times[num_runs / 2];

    let baseline_ops_per_sec = total_ops as f64 / baseline_median.as_secs_f64();
    let checksum_ops_per_sec = total_ops as f64 / checksum_median.as_secs_f64();

    let overhead_pct = ((checksum_median.as_secs_f64() - baseline_median.as_secs_f64())
        / baseline_median.as_secs_f64())
        * 100.0;

    println!("\n📊 Results for {} Dataset:", name);
    println!("  Baseline (OFF): {:.2}ms ({:.0} ops/sec)",
        baseline_median.as_secs_f64() * 1000.0, baseline_ops_per_sec);
    println!("  Checksums (ON): {:.2}ms ({:.0} ops/sec)",
        checksum_median.as_secs_f64() * 1000.0, checksum_ops_per_sec);
    println!("  Overhead: {:.2}%", overhead_pct);

    if overhead_pct < 3.0 {
        println!("  ✅ PASS: < 3% overhead");
    } else {
        println!("  ❌ FAIL: Exceeds 3% target");
    }
}

fn run_throughput_test(nodes: &[Node], edges: &[Edge], checksum_enabled: bool) -> std::time::Duration {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("throughput_test.db");

    let mut config = sombra::db::Config::benchmark();
    config.checksum_enabled = checksum_enabled;

    let mut db = GraphDB::open_with_config(&db_path, config).expect("Failed to open DB");

    let start = Instant::now();

    let mut tx = db.begin_transaction().expect("Failed to begin transaction");

    for node in nodes {
        tx.add_node(node.clone()).expect("Failed to add node");
    }

    for edge in edges {
        tx.add_edge(edge.clone()).expect("Failed to add edge");
    }

    tx.commit().expect("Failed to commit");

    start.elapsed()
}
