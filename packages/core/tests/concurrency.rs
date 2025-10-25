#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use sombra::{Edge, GraphDB, Node, PropertyValue};
use std::collections::BTreeMap;
use std::thread;
use std::time::Duration;

#[test]
fn sequential_transactions() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    {
        let mut tx = db.begin_transaction().unwrap();
        for i in 1..=100 {
            let mut props = BTreeMap::new();
            props.insert("id".to_string(), PropertyValue::Int(i));

            let mut node = Node::new(0);
            node.labels.push("Test".to_string());
            node.properties = props;

            tx.add_node(node).unwrap();
        }
        tx.commit().unwrap();
    }

    for _ in 0..10 {
        let mut tx = db.begin_transaction().unwrap();

        for node_id in 1..=100 {
            let node = tx.get_node(node_id).unwrap().expect("node should exist");
            assert!(node.id > 0);
        }

        tx.commit().unwrap();
        thread::sleep(Duration::from_millis(1));
    }

    let mut tx = db.begin_transaction().unwrap();
    for node_id in 1..=100 {
        let node = tx.get_node(node_id).unwrap().expect("node should exist");
        assert!(node.id > 0);
    }
    tx.commit().unwrap();
}

#[test]
fn sequential_write_transactions() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let ops_per_batch = 100;
    let batches = 8;

    for batch_id in 0..batches {
        let mut tx = db.begin_transaction().unwrap();

        for i in 0..ops_per_batch {
            let mut props = BTreeMap::new();
            props.insert("batch".to_string(), PropertyValue::Int(batch_id));
            props.insert("iteration".to_string(), PropertyValue::Int(i));

            let mut node = Node::new(0);
            node.labels.push(format!("Batch{}", batch_id));
            node.properties = props;

            tx.add_node(node).unwrap();
        }

        tx.commit().unwrap();
    }

    let mut tx = db.begin_transaction().unwrap();

    let mut total_nodes = 0;
    for node_id in 1..=(batches * ops_per_batch + 100) {
        if let Ok(Some(node)) = tx.get_node(node_id as u64) {
            if node.id > 0 {
                total_nodes += 1;
            }
        }
    }

    assert_eq!(total_nodes, batches * ops_per_batch);
    tx.commit().unwrap();
}

#[test]
fn sequential_edge_creation() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    {
        let mut tx = db.begin_transaction().unwrap();
        for i in 1..=100 {
            let mut props = BTreeMap::new();
            props.insert("id".to_string(), PropertyValue::Int(i));

            let mut node = Node::new(0);
            node.labels.push("Node".to_string());
            node.properties = props;

            tx.add_node(node).unwrap();
        }
        tx.commit().unwrap();
    }

    for i in 0..50 {
        let from = (i % 100 + 1) as u64;
        let to = ((i + 1) % 100 + 1) as u64;

        let mut tx = db.begin_transaction().unwrap();

        let edge = Edge::new(0, from, to, "CONNECTS");
        tx.add_edge(edge).unwrap();

        tx.commit().unwrap();
    }

    let mut tx = db.begin_transaction().unwrap();

    let mut total_edges = 0;
    for node_id in 1..=100 {
        if let Ok(neighbors) = tx.get_neighbors(node_id) {
            total_edges += neighbors.len();
        }
    }

    assert_eq!(total_edges, 50);
    tx.commit().unwrap();
}

#[test]
fn rollback_safety() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    for i in 0..25 {
        let mut tx = db.begin_transaction().unwrap();

        let mut props = BTreeMap::new();
        props.insert("value".to_string(), PropertyValue::Int(i));

        let mut node = Node::new(0);
        node.labels.push("Temp".to_string());
        node.properties = props;

        tx.add_node(node).unwrap();

        if i % 2 == 0 {
            tx.commit().unwrap();
        } else {
            tx.rollback().unwrap();
        }
    }

    let mut tx = db.begin_transaction().unwrap();

    let mut committed_nodes = 0;
    for node_id in 1..=1000 {
        if let Ok(Some(node)) = tx.get_node(node_id) {
            if node.id > 0 {
                committed_nodes += 1;
            }
        }
    }

    assert_eq!(committed_nodes, 13);
    tx.commit().unwrap();
}

#[test]
fn mixed_operations() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    {
        let mut tx = db.begin_transaction().unwrap();
        for i in 1..=50 {
            let mut props = BTreeMap::new();
            props.insert("initial".to_string(), PropertyValue::Int(i));

            let mut node = Node::new(0);
            node.labels.push("Initial".to_string());
            node.properties = props;

            tx.add_node(node).unwrap();
        }
        tx.commit().unwrap();
    }

    for i in 0..25 {
        let mut tx = db.begin_transaction().unwrap();

        match i % 3 {
            0 => {
                let mut props = BTreeMap::new();
                props.insert("iteration".to_string(), PropertyValue::Int(i));

                let mut node = Node::new(0);
                node.labels.push("New".to_string());
                node.properties = props;

                tx.add_node(node).unwrap();
            }
            1 => {
                let node_id = ((i % 50) + 1) as u64;
                let _ = tx.get_node(node_id);
            }
            2 => {
                let from = ((i % 50) + 1) as u64;
                let to = (((i + 1) % 50) + 1) as u64;
                let edge = Edge::new(0, from, to, "LINK");
                let _ = tx.add_edge(edge);
            }
            _ => unreachable!(),
        }

        tx.commit().unwrap();
    }

    println!("All sequential operations completed successfully");
}
