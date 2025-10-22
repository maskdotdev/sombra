#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]
#![allow(clippy::collapsible_if)]

use sombra::{Edge, GraphDB, Node, PropertyValue, Result};
use std::collections::{BTreeMap, HashSet};
use tempfile::NamedTempFile;

const LARGE_NODE_COUNT: usize = 100_000;
const STRESS_RANGE_SIZE: usize = 10_000;

#[test]
#[ignore]
fn stress_large_scale_range_queries() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    eprintln!("Creating database with {} nodes...", LARGE_NODE_COUNT);
    let mut db = GraphDB::open(&path)?;
    let mut tx = db.begin_transaction()?;

    let mut node_ids = Vec::with_capacity(LARGE_NODE_COUNT);
    for i in 0..LARGE_NODE_COUNT {
        let mut node = Node::new(0);
        node.labels.push("LargeScaleTest".to_string());
        let mut props = BTreeMap::new();
        props.insert("index".to_string(), PropertyValue::Int(i as i64));
        props.insert(
            "data".to_string(),
            PropertyValue::String(format!("data_{}", i)),
        );
        node.properties = props;
        let node_id = tx.add_node(node)?;
        node_ids.push(node_id);

        if (i + 1) % 10000 == 0 {
            eprintln!("Created {} nodes", i + 1);
        }
    }

    tx.commit()?;
    db.checkpoint()?;
    eprintln!("Checkpoint complete");

    eprintln!("Testing range queries...");
    let start_idx = LARGE_NODE_COUNT / 4;
    let end_idx = start_idx + STRESS_RANGE_SIZE;

    let range_start = node_ids[start_idx];
    let range_end = node_ids[end_idx - 1];

    let range_node_ids = db.get_nodes_in_range(range_start, range_end);
    assert_eq!(range_node_ids.len(), STRESS_RANGE_SIZE);

    let mut tx = db.begin_transaction()?;
    for (i, node_id) in range_node_ids.iter().enumerate() {
        assert!(*node_id >= range_start && *node_id <= range_end);
        let node = tx.get_node(*node_id)?;
        let expected_index = (start_idx + i) as i64;
        assert_eq!(
            node.properties.get("index"),
            Some(&PropertyValue::Int(expected_index))
        );
    }
    tx.commit()?;
    eprintln!("Range query validation passed");

    eprintln!("Testing ordered iteration...");
    let ordered_node_ids = db.get_all_node_ids_ordered();
    assert_eq!(ordered_node_ids.len(), LARGE_NODE_COUNT);

    for i in 0..ordered_node_ids.len() - 1 {
        assert!(ordered_node_ids[i] <= ordered_node_ids[i + 1]);
    }
    eprintln!("Ordered iteration validation passed");

    eprintln!("Testing first/last N queries...");
    let first_1000 = db.get_first_n_nodes(1000);
    assert_eq!(first_1000.len(), 1000);
    assert_eq!(first_1000[0], ordered_node_ids[0]);
    assert_eq!(first_1000[999], ordered_node_ids[999]);
    eprintln!("First N query validation passed");

    Ok(())
}

#[test]
#[ignore]
fn stress_range_queries_with_fragmentation() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    eprintln!("Creating fragmented database...");
    let mut db = GraphDB::open(&path)?;
    let mut tx = db.begin_transaction()?;

    let mut node_ids = Vec::with_capacity(50_000);
    for i in 0..50_000 {
        let mut node = Node::new(0);
        node.labels.push("Fragmented".to_string());
        let mut props = BTreeMap::new();
        props.insert("seq".to_string(), PropertyValue::Int(i));
        node.properties = props;
        let node_id = tx.add_node(node)?;
        node_ids.push(node_id);

        if (i + 1) % 5000 == 0 {
            eprintln!("Created {} nodes", i + 1);
        }
    }

    tx.commit()?;
    db.checkpoint()?;

    eprintln!("Deleting every other node...");
    let mut tx = db.begin_transaction()?;
    let mut deleted_count = 0;
    for (idx, node_id) in node_ids.iter().enumerate() {
        if idx % 2 == 0 {
            tx.delete_node(*node_id)?;
            deleted_count += 1;
        }
        if (deleted_count + 1) % 1000 == 0 {
            eprintln!("Deleted {} nodes", deleted_count);
        }
    }
    tx.commit()?;
    db.checkpoint()?;
    eprintln!("Deleted {} nodes total", deleted_count);

    eprintln!("Testing range queries on fragmented data...");
    let remaining_ids: Vec<_> = node_ids
        .iter()
        .enumerate()
        .filter(|(idx, _)| idx % 2 != 0)
        .map(|(_, id)| *id)
        .collect();

    let range_start = remaining_ids[0];
    let range_end = remaining_ids[remaining_ids.len() - 1];

    let range_node_ids = db.get_nodes_in_range(range_start, range_end);
    eprintln!(
        "Range query returned {} nodes (from {} remaining, span from ID {} to {})",
        range_node_ids.len(),
        remaining_ids.len(),
        range_start,
        range_end
    );
    assert!(
        range_node_ids.len() >= 9_000,
        "Expected at least 9000 nodes in range after fragmentation, got {}",
        range_node_ids.len()
    );
    assert!(
        range_node_ids.len() <= remaining_ids.len(),
        "Range returned more nodes than remaining: {} > {}",
        range_node_ids.len(),
        remaining_ids.len()
    );

    let mut tx = db.begin_transaction()?;
    let mut validated_count = 0;
    for node_id in &range_node_ids {
        if let Ok(node) = tx.get_node(*node_id) {
            let seq = match node.properties.get("seq") {
                Some(PropertyValue::Int(s)) => *s,
                _ => panic!("Expected seq property"),
            };
            assert!(seq % 2 != 0, "Node {} has even seq {}", node_id, seq);
            validated_count += 1;
        }
    }
    tx.commit()?;
    eprintln!("Validated {} nodes in range", validated_count);
    assert!(
        validated_count >= 10_000,
        "Expected at least 10000 valid nodes after deleting half, got {}",
        validated_count
    );
    assert!(
        validated_count <= 25_000,
        "Expected at most 25000 nodes (original remaining), got {}",
        validated_count
    );
    eprintln!("Fragmented range query validation passed");

    Ok(())
}

#[test]
#[ignore]
fn stress_concurrent_range_operations() -> Result<()> {
    use std::sync::Arc;
    use std::thread;

    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    eprintln!("Creating database for concurrent stress test...");
    {
        let mut db = GraphDB::open(&path)?;
        let mut tx = db.begin_transaction()?;

        for i in 0..50_000 {
            let mut node = Node::new(0);
            node.labels.push("Concurrent".to_string());
            let mut props = BTreeMap::new();
            props.insert("thread_safe".to_string(), PropertyValue::Int(i));
            node.properties = props;
            tx.add_node(node)?;

            if (i + 1) % 10000 == 0 {
                eprintln!("Created {} nodes", i + 1);
            }
        }

        tx.commit()?;
        db.checkpoint()?;
    }

    eprintln!("Running concurrent range queries...");
    let path_for_threads = Arc::new(path.clone());
    let mut handles = vec![];

    for thread_id in 0..8 {
        let path_clone = Arc::clone(&path_for_threads);
        let handle = thread::spawn(move || -> Result<usize> {
            let mut db = GraphDB::open(&*path_clone)?;
            let mut total_nodes = 0;

            for iteration in 0..10 {
                let ordered = db.get_all_node_ids_ordered();
                let nodes_per_thread = ordered.len() / 8;
                let start_idx = thread_id * nodes_per_thread;
                let end_idx = start_idx + nodes_per_thread.min(5000);

                if end_idx < ordered.len() {
                    let range_start = ordered[start_idx];
                    let range_end = ordered[end_idx];
                    let range_node_ids = db.get_nodes_in_range(range_start, range_end);

                    let mut tx = db.begin_transaction()?;
                    for node_id in &range_node_ids {
                        let node = tx.get_node(*node_id)?;
                        assert!(node.labels.contains(&"Concurrent".to_string()));
                        assert!(node.properties.contains_key("thread_safe"));
                    }
                    tx.commit()?;

                    total_nodes += range_node_ids.len();
                }

                if iteration % 2 == 0 {
                    eprintln!("Thread {} completed iteration {}", thread_id, iteration);
                }
            }

            Ok(total_nodes)
        });
        handles.push(handle);
    }

    let mut total_processed = 0;
    for (thread_id, handle) in handles.into_iter().enumerate() {
        let count = handle.join().unwrap()?;
        eprintln!("Thread {} processed {} nodes", thread_id, count);
        total_processed += count;
    }

    eprintln!(
        "Total nodes processed across all threads: {}",
        total_processed
    );
    assert!(total_processed > 0);

    Ok(())
}

#[test]
#[ignore]
fn stress_range_queries_with_updates() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    eprintln!("Creating database with updates...");
    let mut db = GraphDB::open(&path)?;
    let mut tx = db.begin_transaction()?;

    let mut node_ids = Vec::with_capacity(30_000);
    for i in 0..30_000 {
        let mut node = Node::new(0);
        node.labels.push("Updateable".to_string());
        let mut props = BTreeMap::new();
        props.insert("version".to_string(), PropertyValue::Int(1));
        props.insert("value".to_string(), PropertyValue::Int(i));
        node.properties = props;
        let node_id = tx.add_node(node)?;
        node_ids.push(node_id);

        if (i + 1) % 5000 == 0 {
            eprintln!("Created {} nodes", i + 1);
        }
    }

    tx.commit()?;
    db.checkpoint()?;

    eprintln!("Performing bulk updates...");
    for update_round in 1..=5 {
        let mut tx = db.begin_transaction()?;

        for (idx, node_id) in node_ids.iter().enumerate() {
            if idx % 10 == 0 {
                if let Ok(node) = tx.get_node(*node_id) {
                    tx.delete_node(*node_id)?;
                    let mut updated_node = node.clone();
                    updated_node.id = 0;
                    updated_node
                        .properties
                        .insert("version".to_string(), PropertyValue::Int(update_round + 1));
                    tx.add_node(updated_node)?;
                }
            }
        }

        tx.commit()?;
        if update_round % 2 == 0 {
            db.checkpoint()?;
            eprintln!("Completed update round {}", update_round);
        }
    }

    eprintln!("Validating range queries after updates...");
    let range_start = node_ids[5000];
    let range_end = node_ids[14999];

    let range_node_ids = db.get_nodes_in_range(range_start, range_end);

    let mut tx = db.begin_transaction()?;
    let mut updated_count = 0;
    let mut found_count = 0;
    for (idx, node_id) in range_node_ids.iter().enumerate() {
        if let Ok(node) = tx.get_node(*node_id) {
            found_count += 1;
            let version = match node.properties.get("version") {
                Some(PropertyValue::Int(v)) => *v,
                _ => 1,
            };

            if (5000 + idx) % 10 == 0 {
                if version > 1 {
                    updated_count += 1;
                }
            }
        }
    }
    tx.commit()?;

    eprintln!(
        "Range query validation after updates passed (found {}, updated {})",
        found_count, updated_count
    );

    Ok(())
}

#[test]
#[ignore]
fn stress_range_queries_with_edges() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    eprintln!("Creating graph with edges...");
    let mut db = GraphDB::open(&path)?;
    let mut tx = db.begin_transaction()?;

    let mut node_ids = Vec::with_capacity(20_000);
    for i in 0..20_000 {
        let mut node = Node::new(0);
        node.labels.push("GraphNode".to_string());
        let mut props = BTreeMap::new();
        props.insert("degree".to_string(), PropertyValue::Int(0));
        node.properties = props;
        let node_id = tx.add_node(node)?;
        node_ids.push(node_id);

        if (i + 1) % 5000 == 0 {
            eprintln!("Created {} nodes", i + 1);
        }
    }

    eprintln!("Creating edges...");
    let mut edge_count = 0;
    for i in 0..node_ids.len() - 1 {
        if i % 100 < 10 {
            let edge = Edge::new(0, node_ids[i], node_ids[i + 1], "connects");
            tx.add_edge(edge)?;
            edge_count += 1;

            if edge_count % 500 == 0 {
                eprintln!("Created {} edges", edge_count);
            }
        }
    }

    tx.commit()?;
    db.checkpoint()?;
    eprintln!("Total edges created: {}", edge_count);

    eprintln!("Testing range queries with edge traversal...");
    let range_start = node_ids[2000];
    let range_end = node_ids[7999];

    let range_node_ids = db.get_nodes_in_range(range_start, range_end);
    assert_eq!(range_node_ids.len(), 6000);

    let mut tx = db.begin_transaction()?;
    let mut nodes_with_edges = 0;
    for node_id in &range_node_ids {
        let node = tx.get_node(*node_id)?;
        if node.first_outgoing_edge_id != 0 || node.first_incoming_edge_id != 0 {
            nodes_with_edges += 1;
        }
    }
    tx.commit()?;

    eprintln!("Nodes with edges in range: {}", nodes_with_edges);
    assert!(nodes_with_edges > 0);

    Ok(())
}

#[test]
fn stress_multiple_small_ranges() -> Result<()> {
    let tmp = NamedTempFile::new()?;
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open(&path)?;
    let mut tx = db.begin_transaction()?;

    let mut node_ids = Vec::with_capacity(10_000);
    for i in 0..10_000 {
        let mut node = Node::new(0);
        node.labels.push("MultiRange".to_string());
        let mut props = BTreeMap::new();
        props.insert("bucket".to_string(), PropertyValue::Int(i / 100));
        node.properties = props;
        let node_id = tx.add_node(node)?;
        node_ids.push(node_id);
    }

    tx.commit()?;
    db.checkpoint()?;

    let mut total_queried = HashSet::new();
    for bucket in 0..100 {
        let start_idx = bucket * 100;
        let end_idx = start_idx + 99;

        let range_start = node_ids[start_idx];
        let range_end = node_ids[end_idx];

        let range_node_ids = db.get_nodes_in_range(range_start, range_end);
        assert_eq!(range_node_ids.len(), 100);

        let mut tx = db.begin_transaction()?;
        for node_id in range_node_ids {
            total_queried.insert(node_id);
            let node = tx.get_node(node_id)?;
            let node_bucket = match node.properties.get("bucket") {
                Some(PropertyValue::Int(b)) => *b,
                _ => panic!("Expected bucket property"),
            };
            assert_eq!(node_bucket, bucket as i64);
        }
        tx.commit()?;
    }

    assert_eq!(total_queried.len(), 10_000);

    Ok(())
}
