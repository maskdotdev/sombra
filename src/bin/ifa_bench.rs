//! Benchmark comparing IFA vs B-tree adjacency creation and lookups.
//!
//! Usage: cargo run --release --bin ifa_bench
#![allow(clippy::arc_with_non_send_sync)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sombra::primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions};
use sombra::storage::AdjacencyBackend;
use sombra::storage::{Dir, EdgeSpec, ExpandOpts, Graph, GraphOptions, NodeSpec};
use sombra::types::{LabelId, NodeId, TypeId};

const WARMUP_ITERS: usize = 100;
const BENCH_ITERS: usize = 1000;
const CREATION_RUNS: usize = 3; // Number of runs to average for creation benchmarks

fn main() {
    println!("=== IFA vs B-tree Benchmark ===\n");

    // Test different graph sizes
    let configs = [
        ("Small", 1_000, 5_000),      // ~5 edges per node avg
        ("Medium", 10_000, 50_000),   // ~5 edges per node avg
        ("Large", 10_000, 200_000),   // ~20 edges per node avg (high degree)
    ];

    // Run creation benchmarks first
    println!("========== CREATION BENCHMARKS ==========\n");
    for (name, nodes, edges) in configs {
        println!("--- {name} Graph: {nodes} nodes, {edges} edges ---\n");
        run_creation_benchmark(nodes, edges);
        println!();
    }

    // Run lookup benchmarks
    println!("========== LOOKUP BENCHMARKS ==========\n");
    for (name, nodes, edges) in configs {
        println!("--- {name} Graph: {nodes} nodes, {edges} edges ---\n");
        run_lookup_comparison(nodes, edges);
        println!();
    }

    println!("=== Benchmark Complete ===");
}

/// Benchmark creation performance (nodes and edges) for both backends.
fn run_creation_benchmark(node_count: usize, edge_count: usize) {
    print_header();

    // Benchmark node creation
    let btree_node_times: Vec<_> = (0..CREATION_RUNS)
        .map(|_| bench_node_creation(node_count, AdjacencyBackend::BTree))
        .collect();
    let ifa_node_times: Vec<_> = (0..CREATION_RUNS)
        .map(|_| bench_node_creation(node_count, AdjacencyBackend::IfaOnly))
        .collect();

    let btree_node_avg = average_duration(&btree_node_times);
    let ifa_node_avg = average_duration(&ifa_node_times);
    let node_speedup = btree_node_avg.as_nanos() as f64 / ifa_node_avg.as_nanos() as f64;
    print_row(
        &format!("create_node x{node_count}"),
        btree_node_avg,
        ifa_node_avg,
        node_speedup,
    );

    // Benchmark edge creation (on a pre-populated graph with nodes)
    let btree_edge_times: Vec<_> = (0..CREATION_RUNS)
        .map(|_| bench_edge_creation(node_count, edge_count, AdjacencyBackend::BTree))
        .collect();
    let ifa_edge_times: Vec<_> = (0..CREATION_RUNS)
        .map(|_| bench_edge_creation(node_count, edge_count, AdjacencyBackend::IfaOnly))
        .collect();

    let btree_edge_avg = average_duration(&btree_edge_times);
    let ifa_edge_avg = average_duration(&ifa_edge_times);
    let edge_speedup = btree_edge_avg.as_nanos() as f64 / ifa_edge_avg.as_nanos() as f64;
    print_row(
        &format!("create_edge x{edge_count}"),
        btree_edge_avg,
        ifa_edge_avg,
        edge_speedup,
    );

    // Benchmark full graph creation (nodes + edges together)
    let btree_full_times: Vec<_> = (0..CREATION_RUNS)
        .map(|_| bench_full_graph_creation(node_count, edge_count, AdjacencyBackend::BTree))
        .collect();
    let ifa_full_times: Vec<_> = (0..CREATION_RUNS)
        .map(|_| bench_full_graph_creation(node_count, edge_count, AdjacencyBackend::IfaOnly))
        .collect();

    let btree_full_avg = average_duration(&btree_full_times);
    let ifa_full_avg = average_duration(&ifa_full_times);
    let full_speedup = btree_full_avg.as_nanos() as f64 / ifa_full_avg.as_nanos() as f64;
    print_row("full_graph_creation", btree_full_avg, ifa_full_avg, full_speedup);
}

/// Benchmark lookup performance for both backends.
fn run_lookup_comparison(node_count: usize, edge_count: usize) {
    // Setup B-tree graph
    let btree_harness = GraphHarness::new(node_count, edge_count, AdjacencyBackend::BTree);

    // Setup IFA-only graph
    let ifa_harness = GraphHarness::new(node_count, edge_count, AdjacencyBackend::IfaOnly);

    // Pre-warm BOTH graphs by doing a few iterations across all directions
    // This ensures page caches are populated for both backends
    println!("  Pre-warming caches...");
    for _ in 0..3 {
        for dir in [Dir::Out, Dir::In, Dir::Both] {
            bench_neighbors(&btree_harness, dir, None);
            bench_neighbors(&ifa_harness, dir, None);
        }
    }

    print_header();

    for dir in [Dir::Out, Dir::In, Dir::Both] {
        // Benchmark B-tree
        let btree_time = bench_neighbors(&btree_harness, dir, None);

        // Benchmark IFA
        let ifa_time = bench_neighbors(&ifa_harness, dir, None);

        let speedup = btree_time.as_nanos() as f64 / ifa_time.as_nanos() as f64;
        print_row(&format!("neighbors({dir:?})"), btree_time, ifa_time, speedup);
    }

    // Benchmark with type filter
    let ty = Some(TypeId(1));
    for dir in [Dir::Out, Dir::In] {
        let btree_time = bench_neighbors(&btree_harness, dir, ty);
        let ifa_time = bench_neighbors(&ifa_harness, dir, ty);
        let speedup = btree_time.as_nanos() as f64 / ifa_time.as_nanos() as f64;
        print_row(
            &format!("neighbors({dir:?}, ty=1)"),
            btree_time,
            ifa_time,
            speedup,
        );
    }

    // Benchmark distinct neighbors
    for dir in [Dir::Out, Dir::Both] {
        let btree_time = bench_neighbors_distinct(&btree_harness, dir);
        let ifa_time = bench_neighbors_distinct(&ifa_harness, dir);
        let speedup = btree_time.as_nanos() as f64 / ifa_time.as_nanos() as f64;
        print_row(&format!("distinct({dir:?})"), btree_time, ifa_time, speedup);
    }
}

fn average_duration(durations: &[Duration]) -> Duration {
    let total: Duration = durations.iter().sum();
    total / durations.len() as u32
}

/// Benchmark node creation only.
fn bench_node_creation(node_count: usize, backend: AdjacencyBackend) -> Duration {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = tmpdir.path().join(format!("bench-create-{backend:?}.db"));
    let pager = Arc::new(Pager::create(&path, PagerOptions::default()).expect("pager"));
    let store: Arc<dyn PageStore> = pager.clone();
    let graph = Graph::open(GraphOptions::new(store).adjacency_backend(backend)).expect("graph");

    let start = Instant::now();
    let mut write = pager.begin_write().expect("write");
    for _ in 0..node_count {
        graph
            .create_node(
                &mut write,
                NodeSpec {
                    labels: &[LabelId(1)],
                    props: &[],
                },
            )
            .expect("node");
    }
    pager.commit(write).expect("commit");
    start.elapsed()
}

/// Benchmark edge creation on a graph that already has nodes.
fn bench_edge_creation(node_count: usize, edge_count: usize, backend: AdjacencyBackend) -> Duration {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = tmpdir.path().join(format!("bench-create-{backend:?}.db"));
    let pager = Arc::new(Pager::create(&path, PagerOptions::default()).expect("pager"));
    let store: Arc<dyn PageStore> = pager.clone();
    let graph = Graph::open(
        GraphOptions::new(store)
            .adjacency_backend(backend)
            .defer_adjacency_flush(true)  // Enable batching for fair comparison
    ).expect("graph");

    // First create nodes (not timed)
    let mut nodes = Vec::with_capacity(node_count);
    let mut write = pager.begin_write().expect("write");
    for _ in 0..node_count {
        let node = graph
            .create_node(
                &mut write,
                NodeSpec {
                    labels: &[LabelId(1)],
                    props: &[],
                },
            )
            .expect("node");
        nodes.push(node);
    }
    pager.commit(write).expect("commit");

    // Now benchmark edge creation
    let mut rng = ChaCha8Rng::seed_from_u64(0xDEADBEEF);
    let types = [TypeId(1), TypeId(2), TypeId(3)];

    let start = Instant::now();
    let batch_size = 1000;
    let mut count = 0;
    while count < edge_count {
        let mut write = pager.begin_write().expect("write");
        let batch_end = (count + batch_size).min(edge_count);
        for _ in count..batch_end {
            let src = nodes[rng.gen_range(0..nodes.len())];
            let dst = nodes[rng.gen_range(0..nodes.len())];
            let ty = types[rng.gen_range(0..types.len())];
            graph
                .create_edge(
                    &mut write,
                    EdgeSpec {
                        src,
                        dst,
                        ty,
                        props: &[],
                    },
                )
                .expect("edge");
        }
        graph.flush_deferred_writes(&mut write).expect("flush");
        pager.commit(write).expect("commit");
        count = batch_end;
    }
    start.elapsed()
}

/// Benchmark full graph creation (nodes + edges).
fn bench_full_graph_creation(
    node_count: usize,
    edge_count: usize,
    backend: AdjacencyBackend,
) -> Duration {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let path = tmpdir.path().join(format!("bench-create-{backend:?}.db"));
    let pager = Arc::new(Pager::create(&path, PagerOptions::default()).expect("pager"));
    let store: Arc<dyn PageStore> = pager.clone();
    let graph = Graph::open(
        GraphOptions::new(store)
            .adjacency_backend(backend)
            .defer_adjacency_flush(true)  // Enable batching for fair comparison
    ).expect("graph");

    let start = Instant::now();

    // Create nodes
    let mut nodes = Vec::with_capacity(node_count);
    let mut write = pager.begin_write().expect("write");
    for _ in 0..node_count {
        let node = graph
            .create_node(
                &mut write,
                NodeSpec {
                    labels: &[LabelId(1)],
                    props: &[],
                },
            )
            .expect("node");
        nodes.push(node);
    }
    pager.commit(write).expect("commit");

    // Create edges
    let mut rng = ChaCha8Rng::seed_from_u64(0xDEADBEEF);
    let types = [TypeId(1), TypeId(2), TypeId(3)];
    let batch_size = 1000;
    let mut count = 0;
    while count < edge_count {
        let mut write = pager.begin_write().expect("write");
        let batch_end = (count + batch_size).min(edge_count);
        for _ in count..batch_end {
            let src = nodes[rng.gen_range(0..nodes.len())];
            let dst = nodes[rng.gen_range(0..nodes.len())];
            let ty = types[rng.gen_range(0..types.len())];
            graph
                .create_edge(
                    &mut write,
                    EdgeSpec {
                        src,
                        dst,
                        ty,
                        props: &[],
                    },
                )
                .expect("edge");
        }
        graph.flush_deferred_writes(&mut write).expect("flush");
        pager.commit(write).expect("commit");
        count = batch_end;
    }

    pager.checkpoint(CheckpointMode::Force).expect("checkpoint");
    start.elapsed()
}

fn bench_neighbors(harness: &GraphHarness, dir: Dir, ty: Option<TypeId>) -> Duration {
    let nodes = &harness.nodes;
    let mut rng = ChaCha8Rng::seed_from_u64(0xBEEF);

    // Warmup
    for _ in 0..WARMUP_ITERS {
        let node = nodes[rng.gen_range(0..nodes.len())];
        let read = harness.pager.begin_read().unwrap();
        let cursor = harness.graph.neighbors(
            &read,
            node,
            dir,
            ty,
            ExpandOpts { distinct_nodes: false },
        ).unwrap();
        let _ = cursor.count();
    }

    // Benchmark
    let mut rng = ChaCha8Rng::seed_from_u64(0xCAFE);
    let start = Instant::now();
    for _ in 0..BENCH_ITERS {
        let node = nodes[rng.gen_range(0..nodes.len())];
        let read = harness.pager.begin_read().unwrap();
        let cursor = harness.graph.neighbors(
            &read,
            node,
            dir,
            ty,
            ExpandOpts { distinct_nodes: false },
        ).unwrap();
        let _ = cursor.count();
    }
    start.elapsed() / BENCH_ITERS as u32
}

fn bench_neighbors_distinct(harness: &GraphHarness, dir: Dir) -> Duration {
    let nodes = &harness.nodes;
    let mut rng = ChaCha8Rng::seed_from_u64(0xBEEF);

    // Warmup
    for _ in 0..WARMUP_ITERS {
        let node = nodes[rng.gen_range(0..nodes.len())];
        let read = harness.pager.begin_read().unwrap();
        let cursor = harness.graph.neighbors(
            &read,
            node,
            dir,
            None,
            ExpandOpts { distinct_nodes: true },
        ).unwrap();
        let _ = cursor.count();
    }

    // Benchmark
    let mut rng = ChaCha8Rng::seed_from_u64(0xCAFE);
    let start = Instant::now();
    for _ in 0..BENCH_ITERS {
        let node = nodes[rng.gen_range(0..nodes.len())];
        let read = harness.pager.begin_read().unwrap();
        let cursor = harness.graph.neighbors(
            &read,
            node,
            dir,
            None,
            ExpandOpts { distinct_nodes: true },
        ).unwrap();
        let _ = cursor.count();
    }
    start.elapsed() / BENCH_ITERS as u32
}

fn print_header() {
    println!(
        "{:<25} {:>12} {:>12} {:>10}",
        "Operation", "B-tree", "IFA", "Speedup"
    );
    println!("{}", "-".repeat(62));
}

fn print_row(name: &str, btree: Duration, ifa: Duration, speedup: f64) {
    let speedup_str = if speedup >= 1.0 {
        format!("{:.2}x faster", speedup)
    } else {
        format!("{:.2}x slower", 1.0 / speedup)
    };
    println!(
        "{:<25} {:>12} {:>12} {:>10}",
        name,
        format_duration(btree),
        format_duration(ifa),
        speedup_str
    );
}

fn format_duration(d: Duration) -> String {
    let nanos = d.as_nanos();
    if nanos < 1_000 {
        format!("{nanos} ns")
    } else if nanos < 1_000_000 {
        format!("{:.2} µs", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.2} ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.2} s", nanos as f64 / 1_000_000_000.0)
    }
}

struct GraphHarness {
    _tmpdir: tempfile::TempDir,
    pager: Arc<Pager>,
    graph: Arc<Graph>,
    nodes: Vec<NodeId>,
}

impl GraphHarness {
    fn new(node_count: usize, edge_count: usize, backend: AdjacencyBackend) -> Self {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join(format!("bench-{backend:?}.db"));
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).expect("pager"));
        let store: Arc<dyn PageStore> = pager.clone();
        let graph = Graph::open(
            GraphOptions::new(store)
                .adjacency_backend(backend)
                .defer_adjacency_flush(true)  // Enable batching
        ).expect("graph");

        let mut harness = Self {
            _tmpdir: tmpdir,
            pager,
            graph,
            nodes: Vec::with_capacity(node_count),
        };
        harness.seed(node_count, edge_count);
        harness
    }

    fn seed(&mut self, node_count: usize, edge_count: usize) {
        // Create nodes
        let mut write = self.pager.begin_write().expect("write");
        for _ in 0..node_count {
            let node = self.graph.create_node(
                &mut write,
                NodeSpec {
                    labels: &[LabelId(1)],
                    props: &[],
                },
            ).expect("node");
            self.nodes.push(node);
        }
        self.pager.commit(write).expect("commit");

        // Create edges with some variety in types
        let mut rng = ChaCha8Rng::seed_from_u64(0xDEADBEEF);
        let types = [TypeId(1), TypeId(2), TypeId(3)];
        
        // Batch edges for efficiency
        let batch_size = 1000;
        let mut count = 0;
        while count < edge_count {
            let mut write = self.pager.begin_write().expect("write");
            let batch_end = (count + batch_size).min(edge_count);
            for _ in count..batch_end {
                let src = self.nodes[rng.gen_range(0..self.nodes.len())];
                let dst = self.nodes[rng.gen_range(0..self.nodes.len())];
                let ty = types[rng.gen_range(0..types.len())];
                self.graph.create_edge(
                    &mut write,
                    EdgeSpec {
                        src,
                        dst,
                        ty,
                        props: &[],
                    },
                ).expect("edge");
            }
            self.graph.flush_deferred_writes(&mut write).expect("flush");
            self.pager.commit(write).expect("commit");
            count = batch_end;
        }

        // Checkpoint to ensure data is on disk
        self.pager.checkpoint(CheckpointMode::Force).expect("checkpoint");
    }
}
