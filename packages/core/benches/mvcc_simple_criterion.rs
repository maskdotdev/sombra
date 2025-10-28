//! Simplified MVCC Performance Benchmarks using Criterion
//!
//! Focused benchmarks that avoid resource contention issues.
//! 
//! Run with: cargo bench --bench mvcc_simple_criterion --features benchmarks

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sombra::db::{Config, GraphDB};
use sombra::model::{Node, PropertyValue};
use std::cell::RefCell;
use std::sync::atomic::{AtomicU32, Ordering};

static BENCH_COUNTER: AtomicU32 = AtomicU32::new(0);

fn get_unique_path(prefix: &str) -> String {
    let id = BENCH_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("/tmp/sombra_bench_{}_{}.db", prefix, id)
}

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

// Benchmark 1: Empty Transaction Overhead
fn bench_transaction_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("txn_overhead");
    group.sample_size(50); // Reduce sample size for stability
    
    group.bench_function("single_writer", |b| {
        let path = get_unique_path("txn_sw");
        let db = create_single_writer_db(&path);
        let db = RefCell::new(db);
        
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let tx = db_mut.begin_transaction().unwrap();
            tx.commit().unwrap();
        });
    });
    
    group.bench_function("mvcc", |b| {
        let path = get_unique_path("txn_mvcc");
        let db = create_mvcc_db(&path);
        let db = RefCell::new(db);
        
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let tx = db_mut.begin_transaction().unwrap();
            tx.commit().unwrap();
        });
    });
    
    group.finish();
}

// Benchmark 2: Node Creation
fn bench_node_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_create");
    group.sample_size(50);
    
    group.bench_function("single_writer", |b| {
        let path = get_unique_path("create_sw");
        let db = create_single_writer_db(&path);
        let db = RefCell::new(db);
        
        let mut counter = 0i64;
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let mut tx = db_mut.begin_transaction().unwrap();
            let mut node = Node::new(0);
            node.properties.insert("counter".to_string(), PropertyValue::Int(counter));
            counter += 1;
            let node_id = tx.add_node(node).unwrap();
            tx.commit().unwrap();
            black_box(node_id);
        });
    });
    
    group.bench_function("mvcc", |b| {
        let path = get_unique_path("create_mvcc");
        let db = create_mvcc_db(&path);
        let db = RefCell::new(db);
        
        let mut counter = 0i64;
        b.iter(|| {
            let mut db_mut = db.borrow_mut();
            let mut tx = db_mut.begin_transaction().unwrap();
            let mut node = Node::new(0);
            node.properties.insert("counter".to_string(), PropertyValue::Int(counter));
            counter += 1;
            let node_id = tx.add_node(node).unwrap();
            tx.commit().unwrap();
            black_box(node_id);
        });
    });
    
    group.finish();
}

// Benchmark 3: Node Read
fn bench_node_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_read");
    group.sample_size(50);
    
    group.bench_function("single_writer", |b| {
        let path = get_unique_path("read_sw");
        let (db, node_ids) = create_db_with_nodes(&path, 50, false);
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
    
    group.bench_function("mvcc", |b| {
        let path = get_unique_path("read_mvcc");
        let (db, node_ids) = create_db_with_nodes(&path, 50, true);
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

criterion_group!(
    benches,
    bench_transaction_overhead,
    bench_node_creation,
    bench_node_read,
);

criterion_main!(benches);
