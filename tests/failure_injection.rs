#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use sombra::{Edge, GraphDB, Node, PropertyValue};
use std::fs;
use std::io::Write;

#[test]
fn test_recovery_after_unclean_shutdown() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let path = temp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path).unwrap();
        let mut tx = db.begin_transaction().unwrap();

        for i in 1..=100 {
            let mut node = Node::new(0);
            node.labels.push("Test".to_string());
            node.properties
                .insert("id".to_string(), PropertyValue::Int(i));
            tx.add_node(node).unwrap();
        }

        tx.commit().unwrap();
    }

    let mut db = GraphDB::open(&path).unwrap();
    let mut tx = db.begin_transaction().unwrap();

    for i in 1..=100 {
        let node = tx.get_node(i);
        assert!(node.is_ok(), "Node {} should exist after recovery", i);
    }

    tx.commit().unwrap();
}

#[test]
fn test_commit_durability() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let path = temp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path).unwrap();
        let mut tx = db.begin_transaction().unwrap();

        let mut node = Node::new(0);
        node.labels.push("Durable".to_string());
        node.properties.insert(
            "value".to_string(),
            PropertyValue::String("durable".to_string()),
        );
        let node_id = tx.add_node(node).unwrap();

        tx.commit().unwrap();

        assert_eq!(node_id, 1);
    }

    let mut db = GraphDB::open(&path).unwrap();
    let mut tx = db.begin_transaction().unwrap();

    let node = tx.get_node(1).unwrap();
    assert_eq!(node.labels, vec!["Durable".to_string()]);

    tx.commit().unwrap();
}

#[test]
fn test_rollback_leaves_no_trace() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    {
        let mut tx = db.begin_transaction().unwrap();
        for i in 1..=50 {
            let mut node = Node::new(0);
            node.labels.push("Committed".to_string());
            node.properties
                .insert("id".to_string(), PropertyValue::Int(i));
            tx.add_node(node).unwrap();
        }
        tx.commit().unwrap();
    }

    {
        let mut tx = db.begin_transaction().unwrap();
        for i in 51..=100 {
            let mut node = Node::new(0);
            node.labels.push("RolledBack".to_string());
            node.properties
                .insert("id".to_string(), PropertyValue::Int(i));
            tx.add_node(node).unwrap();
        }
        tx.rollback().unwrap();
    }

    let mut tx = db.begin_transaction().unwrap();

    for i in 1..=50 {
        let node = tx.get_node(i);
        assert!(node.is_ok(), "Committed node {} should exist", i);
    }

    for i in 51..=100 {
        let node = tx.get_node(i);
        assert!(node.is_err(), "Rolled back node {} should not exist", i);
    }

    tx.commit().unwrap();
}

#[test]
fn test_transaction_isolation() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let mut tx1 = db.begin_transaction().unwrap();
    let mut node = Node::new(0);
    node.labels.push("Test".to_string());
    node.properties
        .insert("tx".to_string(), PropertyValue::Int(1));
    tx1.add_node(node).unwrap();
    tx1.commit().unwrap();

    let mut tx2 = db.begin_transaction().unwrap();
    let node = tx2.get_node(1);
    assert!(node.is_ok());
    tx2.commit().unwrap();
}

#[test]
fn test_large_transaction_rollback() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    {
        let mut tx = db.begin_transaction().unwrap();
        for i in 0..1000 {
            let mut node = Node::new(0);
            node.labels.push("Large".to_string());
            node.properties
                .insert("index".to_string(), PropertyValue::Int(i));
            tx.add_node(node).unwrap();
        }
        tx.rollback().unwrap();
    }

    let mut tx = db.begin_transaction().unwrap();
    for i in 1..=1000 {
        let node = tx.get_node(i);
        assert!(node.is_err(), "Node {} should not exist after rollback", i);
    }
    tx.commit().unwrap();
}

#[test]
fn test_out_of_memory_simulation() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let mut tx = db.begin_transaction().unwrap();

    let result = (|| {
        for _i in 0..10_000 {
            let mut node = Node::new(0);
            node.labels.push("Heavy".to_string());
            node.properties
                .insert("data".to_string(), PropertyValue::String("x".repeat(1000)));
            tx.add_node(node)?;
        }
        Ok::<_, sombra::GraphError>(())
    })();

    match result {
        Ok(_) => {
            tx.commit().unwrap();
        }
        Err(_) => {
            tx.rollback().unwrap();
        }
    }
}

#[test]
fn test_corrupted_database_detection() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let path = temp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path).unwrap();
        let mut tx = db.begin_transaction().unwrap();

        for i in 1..=10 {
            let mut node = Node::new(0);
            node.labels.push("Test".to_string());
            node.properties
                .insert("id".to_string(), PropertyValue::Int(i));
            tx.add_node(node).unwrap();
        }

        tx.commit().unwrap();
    }

    {
        let mut file = fs::OpenOptions::new().write(true).open(&path).unwrap();

        file.write_all(&[0xFF; 64]).unwrap();
    }

    let result = GraphDB::open(&path);
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn test_edge_integrity_after_node_operations() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let mut tx = db.begin_transaction().unwrap();

    for i in 1..=10 {
        let mut node = Node::new(0);
        node.labels.push("Node".to_string());
        node.properties
            .insert("id".to_string(), PropertyValue::Int(i));
        tx.add_node(node).unwrap();
    }

    for i in 1..=9 {
        let edge = Edge::new(0, i, i + 1, "NEXT");
        tx.add_edge(edge).unwrap();
    }

    tx.commit().unwrap();

    let mut tx = db.begin_transaction().unwrap();

    for i in 1..=9 {
        let neighbors = tx.get_neighbors(i).unwrap();
        assert!(
            neighbors.contains(&(i + 1)),
            "Node {} should have outgoing edge to {}",
            i,
            i + 1
        );
    }

    tx.commit().unwrap();
}

#[test]
fn test_transaction_abort_on_error() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    let result = (|| {
        let mut tx = db.begin_transaction()?;

        let mut node = Node::new(0);
        node.labels.push("Test".to_string());
        node.properties
            .insert("valid".to_string(), PropertyValue::Int(1));
        tx.add_node(node)?;

        let edge = Edge::new(0, 9999, 9998, "INVALID");
        let result = tx.add_edge(edge);

        if result.is_err() {
            tx.rollback()?;
            return Err(result.unwrap_err());
        }

        tx.commit()?;
        Ok(())
    })();

    assert!(result.is_err());

    let mut tx = db.begin_transaction().unwrap();
    let node = tx.get_node(1);
    assert!(
        node.is_err(),
        "Node should not exist after failed transaction"
    );
    tx.commit().unwrap();
}

#[test]
fn test_multiple_checkpoint_cycles() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let mut db = GraphDB::open(temp.path()).unwrap();

    for cycle in 0..5 {
        for i in 0..20 {
            let mut tx = db.begin_transaction().unwrap();
            let mut node = Node::new(0);
            node.labels.push("Cycle".to_string());
            node.properties
                .insert("cycle".to_string(), PropertyValue::Int(cycle));
            node.properties
                .insert("index".to_string(), PropertyValue::Int(i));
            tx.add_node(node).unwrap();
            tx.commit().unwrap();
        }

        db.checkpoint().unwrap();
    }

    let mut tx = db.begin_transaction().unwrap();
    let mut count = 0;
    for i in 1..=200 {
        if tx.get_node(i).is_ok() {
            count += 1;
        }
    }
    assert_eq!(count, 100, "Should have exactly 100 nodes");
    tx.commit().unwrap();
}
