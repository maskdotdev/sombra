//! MVCC Performance Benchmarks using Criterion
//!
//! Statistical benchmarking with warmup, multiple iterations, and HTML reports.
//! 
//! Run with: cargo bench --bench mvcc_criterion --features benchmarks
//! View results: open target/criterion/report/index.html

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, BatchSize};
use sombra::db::{Config, GraphDB};
use sombra::model::{Node, Edge, PropertyValue};
use tempfile::TempDir;
use std::cell::RefCell;

// ============================================================================
// Setup Helpers
// ============================================================================

fn create_mvcc_db(path: &str) -> GraphDB {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));
    
    let mut config = Config::benchmark();
    config.mvcc_enabled = true;
    config.max_concurrent_transactions = Some(200);
    config.gc_interval_secs = None;
    
    GraphDB::open_with_config(path, config).unwrap()
}

fn create_single_writer_db(path: &str) -> GraphDB {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));
    
    let mut config = Config::benchmark();
    config.mvcc_enabled = false;
    
    GraphDB::open_with_config(path, config).unwrap()
}

fn create_db_with_nodes(path: &str, node_count: usize, mvcc: bool) -> (GraphDB, Vec<u64>) {
    let mut db = if mvcc {
        create_mvcc_db(path)
    } else {
        create_single_writer_db(path)
    };
    
    let mut node_ids = Vec::new();
    let mut tx = db.begin_transaction().unwrap();
    for i in 0..node_count {
        let mut node = Node::new(0);
        node.labels.push("TestNode".to_string());
        node.properties.insert("index".to_string(), PropertyValue::Int(i as i64));
        let node_id = tx.add_node(node).unwrap();
        node_ids.push(node_id);
    }
    tx.commit().unwrap();
    
    (db, node_ids)
}

// ============================================================================
// Benchmark 1: Transaction Begin/Commit Overhead
// ============================================================================

fn bench_transaction_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_overhead");
    
    group.bench_function("single_writer", |b| {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("single.db");
        let db = create_single_writer_db(path.to_str().unwrap());
        let db = RefCell::new(db);
        
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let tx = db_mut.begin_transaction().unwrap();
            tx.commit().unwrap();
        });
    });
    
    group.bench_function("mvcc", |b| {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("mvcc.db");
        let db = create_mvcc_db(path.to_str().unwrap());
        let db = RefCell::new(db);
        
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let tx = db_mut.begin_transaction().unwrap();
            tx.commit().unwrap();
        });
    });
    
    group.finish();
}

// ============================================================================
// Benchmark 2: Node Creation Throughput
// ============================================================================

fn bench_node_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_creation");
    
    group.bench_function("single_writer", |b| {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("single.db");
        let db = create_single_writer_db(path.to_str().unwrap());
        let db = RefCell::new(db);
        
        let mut counter = 0i64;
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let mut tx = db_mut.begin_transaction().unwrap();
            let mut node = Node::new(0);
            node.labels.push("TestNode".to_string());
            node.properties.insert("counter".to_string(), PropertyValue::Int(counter));
            counter += 1;
            let node_id = tx.add_node(node).unwrap();
            tx.commit().unwrap();
            black_box(node_id);
        });
    });
    
    group.bench_function("mvcc", |b| {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("mvcc.db");
        let db = create_mvcc_db(path.to_str().unwrap());
        let db = RefCell::new(db);
        
        let mut counter = 0i64;
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let mut tx = db_mut.begin_transaction().unwrap();
            let mut node = Node::new(0);
            node.labels.push("TestNode".to_string());
            node.properties.insert("counter".to_string(), PropertyValue::Int(counter));
            counter += 1;
            let node_id = tx.add_node(node).unwrap();
            tx.commit().unwrap();
            black_box(node_id);
        });
    });
    
    group.finish();
}

// ============================================================================
// Benchmark 3: Node Read Performance
// ============================================================================

fn bench_node_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_read");
    
    // Single-writer read
    group.bench_function("single_writer", |b| {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("single.db");
        let (db, node_ids) = create_db_with_nodes(path.to_str().unwrap(), 100, false);
        let db = RefCell::new(db);
        
        let mut idx = 0;
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let mut tx = db_mut.begin_transaction().unwrap();
            let node = tx.get_node(node_ids[idx % node_ids.len()]).unwrap();
            tx.commit().unwrap();
            idx += 1;
            black_box(node);
        });
    });
    
    // MVCC read (no versions)
    group.bench_function("mvcc_no_versions", |b| {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("mvcc.db");
        let (db, node_ids) = create_db_with_nodes(path.to_str().unwrap(), 100, true);
        let db = RefCell::new(db);
        
        let mut idx = 0;
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let mut tx = db_mut.begin_transaction().unwrap();
            let node = tx.get_node(node_ids[idx % node_ids.len()]).unwrap();
            tx.commit().unwrap();
            idx += 1;
            black_box(node);
        });
    });
    
    group.finish();
}

// ============================================================================
// Benchmark 4: Node Update Performance
// ============================================================================

fn bench_node_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_update");
    
    group.bench_function("single_writer", |b| {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("single.db");
        let (db, node_ids) = create_db_with_nodes(path.to_str().unwrap(), 100, false);
        let db = RefCell::new(db);
        
        let mut counter = 0i64;
        let mut idx = 0;
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let mut tx = db_mut.begin_transaction().unwrap();
            let node_id = node_ids[idx % node_ids.len()];
            if let Some(mut node) = tx.get_node(node_id).unwrap() {
                node.properties.insert("updated".to_string(), PropertyValue::Int(counter));
                counter += 1;
                tx.add_node(node).unwrap();
            }
            tx.commit().unwrap();
            idx += 1;
        });
    });
    
    group.bench_function("mvcc", |b| {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("mvcc.db");
        let (db, node_ids) = create_db_with_nodes(path.to_str().unwrap(), 100, true);
        let db = RefCell::new(db);
        
        let mut counter = 0i64;
        let mut idx = 0;
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let mut tx = db_mut.begin_transaction().unwrap();
            let node_id = node_ids[idx % node_ids.len()];
            if let Some(mut node) = tx.get_node(node_id).unwrap() {
                node.properties.insert("updated".to_string(), PropertyValue::Int(counter));
                counter += 1;
                tx.add_node(node).unwrap();
            }
            tx.commit().unwrap();
            idx += 1;
        });
    });
    
    group.finish();
}

// ============================================================================
// Benchmark 5: Version Chain Read (Parameterized)
// ============================================================================

fn bench_version_chain_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("version_chain_read");
    
    // Test different version chain depths
    for chain_depth in [0, 5, 10, 25, 50].iter() {
        group.bench_with_input(
            BenchmarkId::new("mvcc", chain_depth),
            chain_depth,
            |b, &depth| {
                let temp_dir = TempDir::new().unwrap();
                let path = temp_dir.path().join("mvcc.db");
                let (db, node_ids) = create_db_with_nodes(path.to_str().unwrap(), 100, true);
                let db = RefCell::new(db);
                
                // Create version chains
                for update_round in 0..depth {
                    for &node_id in &node_ids {
                        let mut db_mut = db.borrow_mut();
                        let mut tx = db_mut.begin_transaction().unwrap();
                        if let Some(mut node) = tx.get_node(node_id).unwrap() {
                            node.properties.insert("version".to_string(), PropertyValue::Int(update_round));
                            tx.add_node(node).unwrap();
                        }
                        tx.commit().unwrap();
                    }
                }
                
                let mut idx = 0;
                b.iter(|| {
                    let mut db_mut = db.borrow_mut();
                    let mut tx = db_mut.begin_transaction().unwrap();
                    let node = tx.get_node(node_ids[idx % node_ids.len()]).unwrap();
                    tx.commit().unwrap();
                    idx += 1;
                    black_box(node);
                });
            },
        );
    }
    
    group.finish();
}

// ============================================================================
// Benchmark 6: Edge Traversal
// ============================================================================

fn bench_edge_traversal(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge_traversal");
    
    group.bench_function("single_writer", |b| {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("single.db");
        let (db, node_ids) = create_db_with_nodes(path.to_str().unwrap(), 100, false);
        let db = RefCell::new(db);
        
        // Create edges
        {
            let mut db_mut = db.borrow_mut();
            let mut tx = db_mut.begin_transaction().unwrap();
            for i in 0..node_ids.len() {
                let target = (i + 1) % node_ids.len();
                tx.add_edge(Edge::new(0, node_ids[i], node_ids[target], "next")).ok();
            }
            tx.commit().unwrap();
        }
        
        let mut idx = 0;
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let mut tx = db_mut.begin_transaction().unwrap();
            let neighbors = tx.get_neighbors(node_ids[idx % node_ids.len()]).unwrap();
            tx.commit().unwrap();
            idx += 1;
            black_box(neighbors);
        });
    });
    
    group.bench_function("mvcc", |b| {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("mvcc.db");
        let (db, node_ids) = create_db_with_nodes(path.to_str().unwrap(), 100, true);
        let db = RefCell::new(db);
        
        // Create edges
        {
            let mut db_mut = db.borrow_mut();
            let mut tx = db_mut.begin_transaction().unwrap();
            for i in 0..node_ids.len() {
                let target = (i + 1) % node_ids.len();
                tx.add_edge(Edge::new(0, node_ids[i], node_ids[target], "next")).ok();
            }
            tx.commit().unwrap();
        }
        
        let mut idx = 0;
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let mut tx = db_mut.begin_transaction().unwrap();
            let neighbors = tx.get_neighbors(node_ids[idx % node_ids.len()]).unwrap();
            tx.commit().unwrap();
            idx += 1;
            black_box(neighbors);
        });
    });
    
    group.finish();
}

// ============================================================================
// Benchmark 7: Hot Spot Updates
// ============================================================================

fn bench_hot_spot_updates(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_spot_updates");
    
    group.bench_function("single_writer_same_node", |b| {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("single.db");
        let (db, node_ids) = create_db_with_nodes(path.to_str().unwrap(), 1, false);
        let db = RefCell::new(db);
        let node_id = node_ids[0];
        
        let mut counter = 0i64;
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let mut tx = db_mut.begin_transaction().unwrap();
            if let Some(mut node) = tx.get_node(node_id).unwrap() {
                node.properties.insert("counter".to_string(), PropertyValue::Int(counter));
                counter += 1;
                tx.add_node(node).unwrap();
            }
            tx.commit().unwrap();
        });
    });
    
    group.bench_function("mvcc_same_node", |b| {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("mvcc.db");
        let (db, node_ids) = create_db_with_nodes(path.to_str().unwrap(), 1, true);
        let db = RefCell::new(db);
        let node_id = node_ids[0];
        
        let mut counter = 0i64;
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let mut tx = db_mut.begin_transaction().unwrap();
            if let Some(mut node) = tx.get_node(node_id).unwrap() {
                node.properties.insert("counter".to_string(), PropertyValue::Int(counter));
                counter += 1;
                tx.add_node(node).unwrap();
            }
            tx.commit().unwrap();
        });
    });
    
    group.finish();
}

// ============================================================================
// Benchmark Configuration
// ============================================================================

criterion_group!(
    benches,
    bench_transaction_overhead,
    bench_node_creation,
    bench_node_read,
    bench_node_update,
    bench_version_chain_read,
    bench_edge_traversal,
    bench_hot_spot_updates,
);

criterion_main!(benches);
