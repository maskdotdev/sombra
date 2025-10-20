use sombra::{GraphDB, PropertyValue, Node, Edge};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

const REGRESSION_THRESHOLD: f64 = 0.10;

struct BenchmarkResult {
    name: String,
    duration: Duration,
    throughput: f64,
}

fn benchmark_insert_throughput() -> BenchmarkResult {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let operations = 1000;
    let start = Instant::now();

    for i in 0..operations {
        let mut tx = db.begin_transaction().unwrap();
        let mut props = BTreeMap::new();
        props.insert("id".to_string(), PropertyValue::Int(i));
        
        let mut node = Node::new(0);
        node.labels.push("Benchmark".to_string());
        node.properties = props;
        
        tx.add_node(node).unwrap();
        tx.commit().unwrap();
    }

    let duration = start.elapsed();
    let throughput = operations as f64 / duration.as_secs_f64();

    BenchmarkResult {
        name: "insert_throughput".to_string(),
        duration,
        throughput,
    }
}

fn benchmark_read_latency() -> BenchmarkResult {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    {
        let mut tx = db.begin_transaction().unwrap();
        for i in 0..1000 {
            let mut props = BTreeMap::new();
            props.insert("id".to_string(), PropertyValue::Int(i));
            
            let mut node = Node::new(0);
            node.labels.push("Benchmark".to_string());
            node.properties = props;
            
            tx.add_node(node).unwrap();
        }
        tx.commit().unwrap();
    }

    let operations = 10000;
    let start = Instant::now();

    for i in 0..operations {
        let mut tx = db.begin_transaction().unwrap();
        let node_id = (i % 1000) + 1;
        tx.get_node(node_id).unwrap();
        tx.commit().unwrap();
    }

    let duration = start.elapsed();
    let throughput = operations as f64 / duration.as_secs_f64();

    BenchmarkResult {
        name: "read_latency".to_string(),
        duration,
        throughput,
    }
}

fn benchmark_edge_creation() -> BenchmarkResult {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    {
        let mut tx = db.begin_transaction().unwrap();
        for i in 0..100 {
            let mut props = BTreeMap::new();
            props.insert("id".to_string(), PropertyValue::Int(i));
            
            let mut node = Node::new(0);
            node.labels.push("Node".to_string());
            node.properties = props;
            
            tx.add_node(node).unwrap();
        }
        tx.commit().unwrap();
    }

    let operations = 500;
    let start = Instant::now();

    for i in 0..operations {
        let mut tx = db.begin_transaction().unwrap();
        let from = (i % 100) + 1;
        let to = ((i + 1) % 100) + 1;
        
        let edge = Edge::new(0, from, to, "CONNECTS");
        tx.add_edge(edge).unwrap();
        tx.commit().unwrap();
    }

    let duration = start.elapsed();
    let throughput = operations as f64 / duration.as_secs_f64();

    BenchmarkResult {
        name: "edge_creation".to_string(),
        duration,
        throughput,
    }
}

fn benchmark_traversal() -> BenchmarkResult {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    {
        let mut tx = db.begin_transaction().unwrap();
        for i in 0..100 {
            let mut props = BTreeMap::new();
            props.insert("id".to_string(), PropertyValue::Int(i));
            
            let mut node = Node::new(0);
            node.labels.push("Node".to_string());
            node.properties = props;
            
            tx.add_node(node).unwrap();
        }
        
        for i in 0..200 {
            let from = (i % 100) + 1;
            let to = ((i * 7) % 100) + 1;
            
            let edge = Edge::new(0, from, to, "LINK");
            tx.add_edge(edge).unwrap();
        }
        
        tx.commit().unwrap();
    }

    let operations = 1000;
    let start = Instant::now();

    for i in 0..operations {
        let mut tx = db.begin_transaction().unwrap();
        let node_id = (i % 100) + 1;
        let _ = tx.get_neighbors(node_id).unwrap();
        tx.commit().unwrap();
    }

    let duration = start.elapsed();
    let throughput = operations as f64 / duration.as_secs_f64();

    BenchmarkResult {
        name: "traversal".to_string(),
        duration,
        throughput,
    }
}

fn benchmark_mixed_workload() -> BenchmarkResult {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    {
        let mut tx = db.begin_transaction().unwrap();
        for i in 0..50 {
            let mut props = BTreeMap::new();
            props.insert("id".to_string(), PropertyValue::Int(i));
            
            let mut node = Node::new(0);
            node.labels.push("Initial".to_string());
            node.properties = props;
            
            tx.add_node(node).unwrap();
        }
        tx.commit().unwrap();
    }

    let operations = 1000;
    let start = Instant::now();

    for i in 0..operations {
        let mut tx = db.begin_transaction().unwrap();
        
        match i % 4 {
            0 => {
                let mut props = BTreeMap::new();
                props.insert("value".to_string(), PropertyValue::Int(i as i64));
                
                let mut node = Node::new(0);
                node.labels.push("New".to_string());
                node.properties = props;
                
                tx.add_node(node).unwrap();
            }
            1 => {
                let node_id = (i % 50) + 1;
                tx.get_node(node_id as u64).unwrap();
            }
            2 => {
                let from = (i % 50) + 1;
                let to = ((i + 1) % 50) + 1;
                let edge = Edge::new(0, from as u64, to as u64, "LINK");
                let _ = tx.add_edge(edge);
            }
            3 => {
                let node_id = (i % 50) + 1;
                let _ = tx.get_neighbors(node_id as u64);
            }
            _ => unreachable!(),
        }
        
        tx.commit().unwrap();
    }

    let duration = start.elapsed();
    let throughput = operations as f64 / duration.as_secs_f64();

    BenchmarkResult {
        name: "mixed_workload".to_string(),
        duration,
        throughput,
    }
}

#[test]
#[ignore]
fn test_insert_throughput_regression() {
    let result = benchmark_insert_throughput();
    println!("{}: {:.2} ops/sec ({:?})", result.name, result.throughput, result.duration);
    
    let baseline_throughput = 500.0;
    let min_acceptable = baseline_throughput * (1.0 - REGRESSION_THRESHOLD);
    
    assert!(
        result.throughput >= min_acceptable,
        "Insert throughput regression detected: {:.2} ops/sec < {:.2} ops/sec (baseline: {:.2}, threshold: {}%)",
        result.throughput, min_acceptable, baseline_throughput, REGRESSION_THRESHOLD * 100.0
    );
}

#[test]
#[ignore]
fn test_read_latency_regression() {
    let result = benchmark_read_latency();
    println!("{}: {:.2} ops/sec ({:?})", result.name, result.throughput, result.duration);
    
    let baseline_throughput = 5000.0;
    let min_acceptable = baseline_throughput * (1.0 - REGRESSION_THRESHOLD);
    
    assert!(
        result.throughput >= min_acceptable,
        "Read latency regression detected: {:.2} ops/sec < {:.2} ops/sec (baseline: {:.2}, threshold: {}%)",
        result.throughput, min_acceptable, baseline_throughput, REGRESSION_THRESHOLD * 100.0
    );
}

#[test]
#[ignore]
fn test_edge_creation_regression() {
    let result = benchmark_edge_creation();
    println!("{}: {:.2} ops/sec ({:?})", result.name, result.throughput, result.duration);
    
    let baseline_throughput = 400.0;
    let min_acceptable = baseline_throughput * (1.0 - REGRESSION_THRESHOLD);
    
    assert!(
        result.throughput >= min_acceptable,
        "Edge creation regression detected: {:.2} ops/sec < {:.2} ops/sec (baseline: {:.2}, threshold: {}%)",
        result.throughput, min_acceptable, baseline_throughput, REGRESSION_THRESHOLD * 100.0
    );
}

#[test]
#[ignore]
fn test_traversal_regression() {
    let result = benchmark_traversal();
    println!("{}: {:.2} ops/sec ({:?})", result.name, result.throughput, result.duration);
    
    let baseline_throughput = 2000.0;
    let min_acceptable = baseline_throughput * (1.0 - REGRESSION_THRESHOLD);
    
    assert!(
        result.throughput >= min_acceptable,
        "Traversal regression detected: {:.2} ops/sec < {:.2} ops/sec (baseline: {:.2}, threshold: {}%)",
        result.throughput, min_acceptable, baseline_throughput, REGRESSION_THRESHOLD * 100.0
    );
}

#[test]
#[ignore]
fn test_mixed_workload_regression() {
    let result = benchmark_mixed_workload();
    println!("{}: {:.2} ops/sec ({:?})", result.name, result.throughput, result.duration);
    
    let baseline_throughput = 1000.0;
    let min_acceptable = baseline_throughput * (1.0 - REGRESSION_THRESHOLD);
    
    assert!(
        result.throughput >= min_acceptable,
        "Mixed workload regression detected: {:.2} ops/sec < {:.2} ops/sec (baseline: {:.2}, threshold: {}%)",
        result.throughput, min_acceptable, baseline_throughput, REGRESSION_THRESHOLD * 100.0
    );
}

#[test]
#[ignore]
fn test_all_benchmarks() {
    let benchmarks = vec![
        benchmark_insert_throughput(),
        benchmark_read_latency(),
        benchmark_edge_creation(),
        benchmark_traversal(),
        benchmark_mixed_workload(),
    ];

    println!("\n=== Benchmark Results ===");
    for result in &benchmarks {
        println!("{:20} {:10.2} ops/sec ({:?})", result.name, result.throughput, result.duration);
    }
    println!("========================\n");
}
