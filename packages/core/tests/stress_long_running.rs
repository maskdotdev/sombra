#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use sombra::{Edge, GraphDB, Node, PropertyValue};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[test]
#[ignore]
fn stress_test_large_insertion() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let start = Instant::now();
    let target_nodes = 1_000_000;
    let target_edges = 10_000_000;

    println!(
        "Starting insertion of {} nodes and {} edges",
        target_nodes, target_edges
    );

    for i in 0..target_nodes {
        let mut props = BTreeMap::new();
        props.insert("id".to_string(), PropertyValue::Int(i as i64));
        props.insert(
            "name".to_string(),
            PropertyValue::String(format!("Node_{}", i)),
        );

        let mut node = Node::new(0);
        node.labels.push("TestNode".to_string());
        node.properties = props;

        let mut tx = db.begin_transaction().unwrap();
        tx.add_node(node).unwrap();
        tx.commit().unwrap();

        if i % 10_000 == 0 && i > 0 {
            let elapsed = start.elapsed();
            let rate = i as f64 / elapsed.as_secs_f64();
            println!("Inserted {} nodes ({:.0} nodes/sec)", i, rate);
        }
    }

    println!("Node insertion completed in {:?}", start.elapsed());
    let edge_start = Instant::now();

    for i in 0..target_edges {
        let from = (i % target_nodes) as u64 + 1;
        let to = ((i + 1) % target_nodes) as u64 + 1;

        let mut props = BTreeMap::new();
        props.insert("weight".to_string(), PropertyValue::Float(1.0));

        let mut edge = Edge::new(0, from, to, "CONNECTS");
        edge.properties = props;

        let mut tx = db.begin_transaction().unwrap();
        tx.add_edge(edge).unwrap();
        tx.commit().unwrap();

        if i % 100_000 == 0 && i > 0 {
            let elapsed = edge_start.elapsed();
            let rate = i as f64 / elapsed.as_secs_f64();
            println!("Inserted {} edges ({:.0} edges/sec)", i, rate);
        }
    }

    println!("Edge insertion completed in {:?}", edge_start.elapsed());
    println!("Total time: {:?}", start.elapsed());

    let mut tx = db.begin_transaction().unwrap();
    let node = tx.get_node(1).unwrap().expect("node should exist");
    assert!(node.id > 0);
    tx.commit().unwrap();
}

#[test]
#[ignore]
fn stress_test_sustained_throughput() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let duration = Duration::from_secs(60);
    let target_rate = 1000;
    let start = Instant::now();
    let mut count = 0u64;
    let mut last_report = Instant::now();

    println!(
        "Running sustained {} tx/sec for {:?}",
        target_rate, duration
    );

    while start.elapsed() < duration {
        let mut props = BTreeMap::new();
        props.insert("index".to_string(), PropertyValue::Int(count as i64));
        props.insert(
            "timestamp".to_string(),
            PropertyValue::Int(start.elapsed().as_millis() as i64),
        );

        let mut node = Node::new(0);
        node.labels.push("Stress".to_string());
        node.properties = props;

        let mut tx = db.begin_transaction().unwrap();
        tx.add_node(node).unwrap();
        tx.commit().unwrap();

        count += 1;

        if last_report.elapsed() >= Duration::from_secs(5) {
            let elapsed = start.elapsed();
            let rate = count as f64 / elapsed.as_secs_f64();
            println!(
                "{:?} elapsed: {} transactions ({:.0} tx/sec)",
                elapsed, count, rate
            );
            last_report = Instant::now();
        }

        let target_count = (start.elapsed().as_secs_f64() * target_rate as f64) as u64;
        if count > target_count {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    let final_rate = count as f64 / start.elapsed().as_secs_f64();
    println!(
        "Completed {} transactions in {:?} ({:.0} tx/sec)",
        count,
        start.elapsed(),
        final_rate
    );

    assert!(
        final_rate >= target_rate as f64 * 0.9,
        "Failed to maintain target throughput"
    );
}

#[test]
#[ignore]
fn stress_test_memory_stability() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let iterations = 100_000;
    println!("Testing memory stability over {} operations", iterations);

    for i in 0..iterations {
        let mut props = BTreeMap::new();
        props.insert(
            "data".to_string(),
            PropertyValue::String(format!("Data_{}", i)),
        );

        let mut node = Node::new(0);
        node.labels.push("Memory".to_string());
        node.properties = props;

        let mut tx = db.begin_transaction().unwrap();
        let node_id = tx.add_node(node).unwrap();

        let retrieved_node = tx.get_node(node_id).unwrap().expect("node should exist");
        assert!(retrieved_node.id > 0);

        tx.commit().unwrap();

        if i % 10_000 == 0 && i > 0 {
            println!("Completed {} iterations", i);
        }
    }

    println!("Memory stability test completed");
}

#[test]
#[ignore]
fn stress_test_mixed_workload() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let operations = 10_000;
    println!("Running mixed workload with {} operations", operations);

    let mut node_ids = Vec::new();

    for i in 0..operations {
        match i % 4 {
            0 => {
                let mut props = BTreeMap::new();
                props.insert(
                    "type".to_string(),
                    PropertyValue::String("mixed".to_string()),
                );

                let mut node = Node::new(0);
                node.labels.push("Mixed".to_string());
                node.properties = props;

                let mut tx = db.begin_transaction().unwrap();
                let node_id = tx.add_node(node).unwrap();
                node_ids.push(node_id);
                tx.commit().unwrap();
            }
            1 => {
                if !node_ids.is_empty() {
                    let node_id = node_ids[i % node_ids.len()];
                    let mut tx = db.begin_transaction().unwrap();
                    let node = tx.get_node(node_id).unwrap().expect("node should exist");
                    assert!(node.id > 0);
                    tx.commit().unwrap();
                }
            }
            2 => {
                if node_ids.len() >= 2 {
                    let from = node_ids[i % node_ids.len()];
                    let to = node_ids[(i + 1) % node_ids.len()];

                    let edge = Edge::new(0, from, to, "LINKS");

                    let mut tx = db.begin_transaction().unwrap();
                    tx.add_edge(edge).unwrap();
                    tx.commit().unwrap();
                }
            }
            3 => {
                if !node_ids.is_empty() {
                    let node_id = node_ids[i % node_ids.len()];
                    let mut tx = db.begin_transaction().unwrap();
                    let _ = tx.get_neighbors(node_id);
                    tx.commit().unwrap();
                }
            }
            _ => unreachable!(),
        }

        if i % 1_000 == 0 && i > 0 {
            println!("Completed {} mixed operations", i);
        }
    }

    println!("Mixed workload test completed");
}
