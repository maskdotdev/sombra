#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]
#![allow(clippy::manual_range_contains)]

use sombra::{Edge, GraphDB, Node, PropertyValue, Result};
use std::collections::BTreeMap;
use tempfile::NamedTempFile;

#[test]
fn test_range_queries_with_social_graph() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path)?;
    let mut tx = db.begin_transaction()?;

    let mut user_ids = Vec::new();
    for i in 0..1000 {
        let mut node = Node::new(0);
        node.labels.push("User".to_string());
        let mut props = BTreeMap::new();
        props.insert("user_id".to_string(), PropertyValue::Int(i));
        props.insert(
            "name".to_string(),
            PropertyValue::String(format!("User{}", i)),
        );
        props.insert("age".to_string(), PropertyValue::Int(20 + (i % 50)));
        node.properties = props;
        let node_id = tx.add_node(node)?;
        user_ids.push(node_id);
    }

    for i in 0..999 {
        let edge = Edge::new(0, user_ids[i], user_ids[i + 1], "follows");
        tx.add_edge(edge)?;
    }

    tx.commit()?;
    db.checkpoint()?;

    let range_start = user_ids[100];
    let range_end = user_ids[199];
    let node_ids_in_range = db.get_nodes_in_range(range_start, range_end);

    assert_eq!(node_ids_in_range.len(), 100);

    for (i, node_id) in node_ids_in_range.iter().enumerate() {
        assert!(*node_id >= range_start && *node_id <= range_end);
        let mut tx = db.begin_transaction()?;
        let node = tx.get_node(*node_id)?.expect("node should exist");
        assert!(node.labels.contains(&"User".to_string()));
        let user_id = match node.properties.get("user_id") {
            Some(PropertyValue::Int(id)) => *id,
            _ => panic!("Expected user_id property"),
        };
        assert_eq!(user_id, (100 + i) as i64);
        tx.commit()?;
    }

    Ok(())
}

#[test]
fn test_ordered_iteration_maintains_consistency() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path)?;
    let mut tx = db.begin_transaction()?;

    let mut expected_ids = Vec::new();
    for i in 0..500 {
        let mut node = Node::new(0);
        node.labels.push("Product".to_string());
        let mut props = BTreeMap::new();
        props.insert(
            "sku".to_string(),
            PropertyValue::String(format!("SKU{:05}", i)),
        );
        props.insert(
            "price".to_string(),
            PropertyValue::Float((10.0 + i as f64) * 1.5),
        );
        node.properties = props;
        let node_id = tx.add_node(node)?;
        expected_ids.push(node_id);
    }

    tx.commit()?;
    db.checkpoint()?;

    let ordered_node_ids = db.get_all_node_ids_ordered();
    assert_eq!(ordered_node_ids.len(), 500);

    for i in 0..ordered_node_ids.len() - 1 {
        assert!(ordered_node_ids[i] <= ordered_node_ids[i + 1]);
    }

    let first_10 = db.get_first_n_nodes(10);
    assert_eq!(first_10.len(), 10);
    for i in 0..10 {
        assert_eq!(first_10[i], ordered_node_ids[i]);
    }

    Ok(())
}

#[test]
fn test_range_queries_after_deletes() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path)?;
    let mut tx = db.begin_transaction()?;

    let mut node_ids = Vec::new();
    for i in 0..200 {
        let mut node = Node::new(0);
        node.labels.push("Item".to_string());
        let mut props = BTreeMap::new();
        props.insert("index".to_string(), PropertyValue::Int(i));
        node.properties = props;
        let node_id = tx.add_node(node)?;
        node_ids.push(node_id);
    }

    tx.commit()?;
    db.checkpoint()?;

    let mut tx = db.begin_transaction()?;
    for i in (50..150).step_by(2) {
        tx.delete_node(node_ids[i])?;
    }
    tx.commit()?;
    db.checkpoint()?;

    let range_start = node_ids[0];
    let range_end = node_ids[199];
    let node_ids_in_range = db.get_nodes_in_range(range_start, range_end);

    assert_eq!(node_ids_in_range.len(), 150);

    for node_id in &node_ids_in_range {
        let mut tx = db.begin_transaction()?;
        let node = tx.get_node(*node_id)?.expect("node should exist");
        let index = match node.properties.get("index") {
            Some(PropertyValue::Int(i)) => *i,
            _ => panic!("Expected index property"),
        };
        if index >= 50 && index < 150 {
            assert!(index % 2 != 0);
        }
        tx.commit()?;
    }

    Ok(())
}

#[test]
fn test_range_queries_across_restart() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let expected_range_count;
    let range_start;
    let range_end;

    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;

        let mut node_ids = Vec::new();
        for i in 0..300 {
            let mut node = Node::new(0);
            node.labels.push("Record".to_string());
            let mut props = BTreeMap::new();
            props.insert("timestamp".to_string(), PropertyValue::Int(1000000 + i));
            node.properties = props;
            let node_id = tx.add_node(node)?;
            node_ids.push(node_id);
        }

        tx.commit()?;
        db.checkpoint()?;

        range_start = node_ids[50];
        range_end = node_ids[149];
        let node_ids_in_range = db.get_nodes_in_range(range_start, range_end);
        expected_range_count = node_ids_in_range.len();
    }

    {
        let db = GraphDB::open(&path)?;
        let node_ids_in_range = db.get_nodes_in_range(range_start, range_end);
        assert_eq!(node_ids_in_range.len(), expected_range_count);
        assert_eq!(node_ids_in_range.len(), 100);
    }

    Ok(())
}

#[test]
fn test_empty_range_queries() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path)?;
    let mut tx = db.begin_transaction()?;

    let mut node_ids = Vec::new();
    for i in 0..100 {
        let mut node = Node::new(0);
        node.labels.push("Data".to_string());
        let mut props = BTreeMap::new();
        props.insert("value".to_string(), PropertyValue::Int(i));
        node.properties = props;
        let node_id = tx.add_node(node)?;
        node_ids.push(node_id);
    }

    tx.commit()?;
    db.checkpoint()?;

    let non_existent_start = node_ids[99] + 100;
    let non_existent_end = node_ids[99] + 200;
    let empty_range = db.get_nodes_in_range(non_existent_start, non_existent_end);
    assert_eq!(empty_range.len(), 0);

    let reverse_range = db.get_nodes_in_range(node_ids[50], node_ids[10]);
    assert_eq!(reverse_range.len(), 0);

    Ok(())
}

#[test]
fn test_range_queries_with_property_index() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path)?;

    db.create_property_index("Player", "score")?;

    let mut tx = db.begin_transaction()?;

    let mut node_ids = Vec::new();
    for i in 0..400 {
        let mut node = Node::new(0);
        node.labels.push("Player".to_string());
        let mut props = BTreeMap::new();
        props.insert("score".to_string(), PropertyValue::Int(i * 10));
        props.insert("level".to_string(), PropertyValue::Int(i / 20));
        node.properties = props;
        let node_id = tx.add_node(node)?;
        node_ids.push(node_id);
    }

    tx.commit()?;
    db.checkpoint()?;

    let range_start = node_ids[100];
    let range_end = node_ids[199];
    let node_ids_in_range = db.get_nodes_in_range(range_start, range_end);

    assert_eq!(node_ids_in_range.len(), 100);

    let score_query = db.find_nodes_by_property("Player", "score", &PropertyValue::Int(1500))?;
    assert_eq!(score_query.len(), 1);
    assert!(score_query[0] >= node_ids[0] && score_query[0] <= node_ids[399]);

    let mut level_5_count = 0;
    for node_id in &node_ids_in_range {
        let mut tx = db.begin_transaction()?;
        if let Some(node) = tx.get_node(*node_id)? {
            if let Some(PropertyValue::Int(5)) = node.properties.get("level") {
                level_5_count += 1;
            }
        }
        tx.commit()?;
    }
    assert!(level_5_count > 0);

    Ok(())
}

#[test]
fn test_concurrent_range_queries() -> Result<()> {
    use std::sync::Arc;
    use std::thread;

    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;

        for i in 0..500 {
            let mut node = Node::new(0);
            node.labels.push("Concurrent".to_string());
            let mut props = BTreeMap::new();
            props.insert("id".to_string(), PropertyValue::Int(i));
            node.properties = props;
            tx.add_node(node)?;
        }

        tx.commit()?;
        db.checkpoint()?;
    }

    let db = Arc::new(GraphDB::open(&path)?);
    let mut handles = vec![];

    for thread_id in 0..4 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || -> Result<()> {
            let start_offset = thread_id * 100;
            let ordered = db_clone.get_all_node_ids_ordered();

            if ordered.len() > start_offset + 50 {
                let range_start = ordered[start_offset];
                let range_end = ordered[start_offset + 50];
                let range_result = db_clone.get_nodes_in_range(range_start, range_end);
                assert!(range_result.len() <= 51);
            }

            Ok(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap()?;
    }

    Ok(())
}
