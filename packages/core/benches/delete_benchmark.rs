//! Delete Operations Performance Benchmarks using Criterion
//!
//! Benchmarks for node and edge deletion performance across various scenarios.
//!
//! Run with: cargo bench --bench delete_benchmark --features benchmarks
//! View results: open target/criterion/report/index.html

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use sombra::db::{Config, GraphDB};
use sombra::model::{Edge, Node, PropertyValue};
use std::cell::RefCell;
use tempfile::TempDir;

// ============================================================================
// Setup Helpers
// ============================================================================

fn create_test_db(path: &str) -> GraphDB {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));

    let mut config = Config::benchmark();
    config.mvcc_enabled = true;

    GraphDB::open_with_config(path, config).unwrap()
}

fn create_db_with_nodes(path: &str, node_count: usize) -> (GraphDB, Vec<u64>) {
    let mut db = create_test_db(path);

    let mut node_ids = Vec::new();
    let mut tx = db.begin_transaction().unwrap();
    for i in 0..node_count {
        let mut node = Node::new(0);
        node.labels.push("TestNode".to_string());
        node.properties
            .insert("index".to_string(), PropertyValue::Int(i as i64));
        let node_id = tx.add_node(node).unwrap();
        node_ids.push(node_id);
    }
    tx.commit().unwrap();

    (db, node_ids)
}

fn create_db_with_star_graph(path: &str, neighbor_count: usize) -> (GraphDB, u64, Vec<u64>) {
    let mut db = create_test_db(path);

    let mut tx = db.begin_transaction().unwrap();
    
    // Create center node
    let mut center_node = Node::new(0);
    center_node.labels.push("Center".to_string());
    let center_id = tx.add_node(center_node).unwrap();
    
    // Create neighbors and edges
    let mut edge_ids = Vec::new();
    for i in 0..neighbor_count {
        let mut node = Node::new(0);
        node.labels.push("Neighbor".to_string());
        node.properties
            .insert("index".to_string(), PropertyValue::Int(i as i64));
        let node_id = tx.add_node(node).unwrap();
        
        let edge_id = tx
            .add_edge(Edge::new(0, center_id, node_id, "connected"))
            .unwrap();
        edge_ids.push(edge_id);
    }
    
    tx.commit().unwrap();

    (db, center_id, edge_ids)
}

fn create_db_with_chain(path: &str, chain_length: usize) -> (GraphDB, Vec<u64>, Vec<u64>) {
    let mut db = create_test_db(path);

    let mut tx = db.begin_transaction().unwrap();
    
    let mut node_ids = Vec::new();
    let mut edge_ids = Vec::new();
    
    // Create chain
    let mut prev_id = None;
    for i in 0..chain_length {
        let mut node = Node::new(0);
        node.labels.push("ChainNode".to_string());
        node.properties
            .insert("index".to_string(), PropertyValue::Int(i as i64));
        let node_id = tx.add_node(node).unwrap();
        node_ids.push(node_id);
        
        if let Some(prev) = prev_id {
            let edge_id = tx
                .add_edge(Edge::new(0, prev, node_id, "next"))
                .unwrap();
            edge_ids.push(edge_id);
        }
        prev_id = Some(node_id);
    }
    
    tx.commit().unwrap();

    (db, node_ids, edge_ids)
}

// ============================================================================
// Benchmark 1: Single Node Deletion (Isolated Nodes)
// ============================================================================

fn bench_single_node_deletion(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_node_deletion");

    for size in [100, 1000, 10000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let path = temp_dir.path().join("delete.db");
                    let (db, node_ids) = create_db_with_nodes(path.to_str().unwrap(), size);
                    (db, node_ids, temp_dir)
                },
                |(mut db, node_ids, _temp_dir)| {
                    let mut tx = db.begin_transaction().unwrap();
                    // Delete first node
                    tx.delete_node(node_ids[0]).unwrap();
                    tx.commit().unwrap();
                    black_box(db);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark 2: Bulk Node Deletion
// ============================================================================

fn bench_bulk_node_deletion(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_node_deletion");

    for delete_count in [10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(delete_count),
            &delete_count,
            |b, &delete_count| {
                b.iter_batched(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let path = temp_dir.path().join("delete.db");
                        let (db, node_ids) =
                            create_db_with_nodes(path.to_str().unwrap(), delete_count * 2);
                        (db, node_ids, temp_dir)
                    },
                    |(mut db, node_ids, _temp_dir)| {
                        let mut tx = db.begin_transaction().unwrap();
                        // Delete first half of nodes
                        for i in 0..delete_count {
                            tx.delete_node(node_ids[i]).unwrap();
                        }
                        tx.commit().unwrap();
                        black_box(db);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 3: Node Deletion with Cascade (Star Graph)
// ============================================================================

fn bench_node_deletion_cascade(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_deletion_cascade");

    for neighbor_count in [10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(neighbor_count),
            &neighbor_count,
            |b, &neighbor_count| {
                b.iter_batched(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let path = temp_dir.path().join("delete.db");
                        let (db, center_id, _edge_ids) =
                            create_db_with_star_graph(path.to_str().unwrap(), neighbor_count);
                        (db, center_id, temp_dir)
                    },
                    |(mut db, center_id, _temp_dir)| {
                        let mut tx = db.begin_transaction().unwrap();
                        // Delete center node (should cascade delete all edges)
                        tx.delete_node(center_id).unwrap();
                        tx.commit().unwrap();
                        black_box(db);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 4: Edge Deletion
// ============================================================================

fn bench_single_edge_deletion(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_edge_deletion");

    for neighbor_count in [10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(neighbor_count),
            &neighbor_count,
            |b, &neighbor_count| {
                b.iter_batched(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let path = temp_dir.path().join("delete.db");
                        let (db, _center_id, edge_ids) =
                            create_db_with_star_graph(path.to_str().unwrap(), neighbor_count);
                        (db, edge_ids, temp_dir)
                    },
                    |(mut db, edge_ids, _temp_dir)| {
                        let mut tx = db.begin_transaction().unwrap();
                        // Delete first edge
                        tx.delete_edge(edge_ids[0]).unwrap();
                        tx.commit().unwrap();
                        black_box(db);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 5: Bulk Edge Deletion
// ============================================================================

fn bench_bulk_edge_deletion(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_edge_deletion");

    for neighbor_count in [10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(neighbor_count),
            &neighbor_count,
            |b, &neighbor_count| {
                b.iter_batched(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let path = temp_dir.path().join("delete.db");
                        let (db, _center_id, edge_ids) =
                            create_db_with_star_graph(path.to_str().unwrap(), neighbor_count);
                        (db, edge_ids, temp_dir)
                    },
                    |(mut db, edge_ids, _temp_dir)| {
                        let mut tx = db.begin_transaction().unwrap();
                        // Delete all edges
                        for edge_id in edge_ids {
                            tx.delete_edge(edge_id).unwrap();
                        }
                        tx.commit().unwrap();
                        black_box(db);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 6: Chain Deletion (Sequential Dependencies)
// ============================================================================

fn bench_chain_deletion(c: &mut Criterion) {
    let mut group = c.benchmark_group("chain_deletion");

    for chain_length in [10, 100, 500] {
        group.bench_with_input(
            BenchmarkId::from_parameter(chain_length),
            &chain_length,
            |b, &chain_length| {
                b.iter_batched(
                    || {
                        let temp_dir = TempDir::new().unwrap();
                        let path = temp_dir.path().join("delete.db");
                        let (db, node_ids, _edge_ids) =
                            create_db_with_chain(path.to_str().unwrap(), chain_length);
                        (db, node_ids, temp_dir)
                    },
                    |(mut db, node_ids, _temp_dir)| {
                        let mut tx = db.begin_transaction().unwrap();
                        // Delete middle node (breaks chain)
                        let middle = node_ids.len() / 2;
                        tx.delete_node(node_ids[middle]).unwrap();
                        tx.commit().unwrap();
                        black_box(db);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark 7: Delete and Recreate Pattern
// ============================================================================

fn bench_delete_recreate_pattern(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete_recreate_pattern");

    group.bench_function("delete_recreate_100_nodes", |b| {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("delete.db");
        let mut db = create_test_db(path.to_str().unwrap());
        let db = RefCell::new(db);

        let mut counter = 0i64;
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let mut tx = db_mut.begin_transaction().unwrap();
            
            // Create 100 nodes
            let mut node_ids = Vec::new();
            for i in 0..100 {
                let mut node = Node::new(0);
                node.labels.push("TempNode".to_string());
                node.properties
                    .insert("counter".to_string(), PropertyValue::Int(counter + i));
                let node_id = tx.add_node(node).unwrap();
                node_ids.push(node_id);
            }
            counter += 100;
            
            tx.commit().unwrap();
            
            // Delete all nodes
            let mut tx = db_mut.begin_transaction().unwrap();
            for node_id in node_ids {
                tx.delete_node(node_id).unwrap();
            }
            tx.commit().unwrap();
            
            black_box(counter);
        });
    });

    group.finish();
}

// ============================================================================
// Criterion Configuration
// ============================================================================

criterion_group!(
    benches,
    bench_single_node_deletion,
    bench_bulk_node_deletion,
    bench_node_deletion_cascade,
    bench_single_edge_deletion,
    bench_bulk_edge_deletion,
    bench_chain_deletion,
    bench_delete_recreate_pattern,
);

criterion_main!(benches);
