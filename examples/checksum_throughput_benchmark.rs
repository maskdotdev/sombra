use sombra::{data_generator::DataGenerator, Edge, GraphDB, Node};
use std::time::Instant;
use tempfile::TempDir;

fn main() {
    println!("=== Checksum Performance Benchmark ===\n");
    println!("Running throughput tests with checksums ENABLED and DISABLED");
    println!("Target: < 3% performance impact with checksums enabled\n");

    let mut data_gen = DataGenerator::new();

    println!("Generating test data...");
    let (nodes, edges) = data_gen.generate_medium_dataset();
    println!(
        "Generated {} nodes and {} edges\n",
        nodes.len(),
        edges.len()
    );

    const NUM_RUNS: usize = 3;

    println!("--- Run 1: Checksums DISABLED (baseline) ---");
    let mut baseline_times = Vec::new();
    for run in 1..=NUM_RUNS {
        let elapsed = run_throughput_test(&nodes, &edges, false);
        baseline_times.push(elapsed);
        println!(
            "  Run {}: {:.2}ms ({:.0} ops/sec)",
            run,
            elapsed.as_secs_f64() * 1000.0,
            (nodes.len() + edges.len()) as f64 / elapsed.as_secs_f64()
        );
    }

    println!("\n--- Run 2: Checksums ENABLED ---");
    let mut checksum_times = Vec::new();
    for run in 1..=NUM_RUNS {
        let elapsed = run_throughput_test(&nodes, &edges, true);
        checksum_times.push(elapsed);
        println!(
            "  Run {}: {:.2}ms ({:.0} ops/sec)",
            run,
            elapsed.as_secs_f64() * 1000.0,
            (nodes.len() + edges.len()) as f64 / elapsed.as_secs_f64()
        );
    }

    println!("\n=== Results Summary ===");

    baseline_times.sort();
    checksum_times.sort();

    let baseline_median = baseline_times[NUM_RUNS / 2];
    let checksum_median = checksum_times[NUM_RUNS / 2];

    let baseline_ops_per_sec = (nodes.len() + edges.len()) as f64 / baseline_median.as_secs_f64();
    let checksum_ops_per_sec = (nodes.len() + edges.len()) as f64 / checksum_median.as_secs_f64();

    println!(
        "Baseline (checksums OFF) - Median: {:.2}ms ({:.0} ops/sec)",
        baseline_median.as_secs_f64() * 1000.0,
        baseline_ops_per_sec
    );
    println!(
        "With checksums (ON)      - Median: {:.2}ms ({:.0} ops/sec)",
        checksum_median.as_secs_f64() * 1000.0,
        checksum_ops_per_sec
    );

    let overhead_pct = ((checksum_median.as_secs_f64() - baseline_median.as_secs_f64())
        / baseline_median.as_secs_f64())
        * 100.0;

    println!("\nPerformance overhead: {:.2}%", overhead_pct);

    if overhead_pct < 3.0 {
        println!("✅ PASS: Checksum overhead is < 3% (target met)");
    } else {
        println!("❌ FAIL: Checksum overhead exceeds 3% target");
    }

    println!("\n--- Detailed Breakdown ---");
    println!(
        "Insert throughput reduction: {:.2}%",
        (1.0 - checksum_ops_per_sec / baseline_ops_per_sec) * 100.0
    );
    println!(
        "Time per operation: baseline={:.2}µs, checksum={:.2}µs",
        baseline_median.as_micros() as f64 / (nodes.len() + edges.len()) as f64,
        checksum_median.as_micros() as f64 / (nodes.len() + edges.len()) as f64
    );
}

fn run_throughput_test(
    nodes: &[Node],
    edges: &[Edge],
    checksum_enabled: bool,
) -> std::time::Duration {
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
