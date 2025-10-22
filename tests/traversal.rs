#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::unnecessary_cast)]

use sombra::{Config, Edge, GraphDB, Node, Result};
use std::collections::HashSet;
use tempfile::NamedTempFile;

fn setup_star_graph(neighbor_count: usize) -> (GraphDB, u64, Vec<u64>) {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open_with_config(&path, Config::balanced()).unwrap();

    let center = db.add_node(Node::new(9999)).unwrap();
    let mut neighbors = Vec::new();
    for i in 0..neighbor_count {
        let node_id = db.add_node(Node::new(i as u64)).unwrap();
        db.add_edge(Edge::new(0, center, node_id, "connected"))
            .unwrap();
        neighbors.push(node_id);
    }

    (db, center, neighbors)
}

fn setup_chain_graph(length: usize) -> (GraphDB, Vec<u64>) {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open_with_config(&path, Config::balanced()).unwrap();

    let mut node_ids = Vec::new();
    let first = db.add_node(Node::new(0)).unwrap();
    node_ids.push(first);

    let mut prev_id = first;
    for i in 1..length {
        let node_id = db.add_node(Node::new(i as u64)).unwrap();
        db.add_edge(Edge::new(0, prev_id, node_id, "next")).unwrap();
        node_ids.push(node_id);
        prev_id = node_id;
    }

    (db, node_ids)
}

fn setup_diamond_graph() -> (GraphDB, Vec<u64>) {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open_with_config(&path, Config::balanced()).unwrap();

    let a = db.add_node(Node::new(1)).unwrap();
    let b = db.add_node(Node::new(2)).unwrap();
    let c = db.add_node(Node::new(3)).unwrap();
    let d = db.add_node(Node::new(4)).unwrap();

    db.add_edge(Edge::new(0, a, b, "edge")).unwrap();
    db.add_edge(Edge::new(0, a, c, "edge")).unwrap();
    db.add_edge(Edge::new(0, b, d, "edge")).unwrap();
    db.add_edge(Edge::new(0, c, d, "edge")).unwrap();

    (db, vec![a, b, c, d])
}

#[test]
fn test_get_neighbors_basic() -> Result<()> {
    let (mut db, center, expected_neighbors) = setup_star_graph(10);

    let neighbors = db.get_neighbors(center)?;
    assert_eq!(neighbors.len(), 10);

    let neighbors_set: HashSet<_> = neighbors.iter().copied().collect();
    let expected_set: HashSet<_> = expected_neighbors.iter().copied().collect();
    assert_eq!(neighbors_set, expected_set);

    Ok(())
}

#[test]
fn test_get_neighbors_empty() -> Result<()> {
    let (mut db, _, _) = setup_star_graph(0);

    let leaf = db.add_node(Node::new(999))?;
    let neighbors = db.get_neighbors(leaf)?;
    assert_eq!(neighbors.len(), 0);

    Ok(())
}

#[test]
fn test_get_neighbors_cache() -> Result<()> {
    let (mut db, center, expected_neighbors) = setup_star_graph(100);

    let neighbors1 = db.get_neighbors(center)?;
    let neighbors2 = db.get_neighbors(center)?;

    assert_eq!(neighbors1, neighbors2);
    assert_eq!(neighbors1.len(), 100);

    let neighbors_set: HashSet<_> = neighbors1.iter().copied().collect();
    let expected_set: HashSet<_> = expected_neighbors.iter().copied().collect();
    assert_eq!(neighbors_set, expected_set);

    Ok(())
}

#[test]
fn test_get_incoming_neighbors() -> Result<()> {
    let (mut db, center, expected_neighbors) = setup_star_graph(10);

    for &neighbor in &expected_neighbors {
        let incoming = db.get_incoming_neighbors(neighbor)?;
        assert_eq!(incoming.len(), 1);
        assert_eq!(incoming[0], center);
    }

    Ok(())
}

#[test]
fn test_two_hop_traversal() -> Result<()> {
    let (mut db, nodes) = setup_diamond_graph();

    let two_hop = db.get_neighbors_two_hops(nodes[0])?;
    assert_eq!(two_hop.len(), 1);
    assert_eq!(two_hop[0], nodes[3]);

    Ok(())
}

#[test]
fn test_two_hop_star_graph() -> Result<()> {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open_with_config(&path, Config::balanced()).unwrap();

    let center = db.add_node(Node::new(0))?;

    let mut layer1_nodes = Vec::new();
    for i in 0..5 {
        let node = db.add_node(Node::new(i))?;
        db.add_edge(Edge::new(0, center, node, "edge"))?;
        layer1_nodes.push(node);
    }

    let mut layer2_nodes = Vec::new();
    for (i, &l1_node) in layer1_nodes.iter().enumerate() {
        for j in 0..3 {
            let node = db.add_node(Node::new((i * 10 + j) as u64))?;
            db.add_edge(Edge::new(0, l1_node, node, "edge"))?;
            layer2_nodes.push(node);
        }
    }

    let two_hop = db.get_neighbors_two_hops(center)?;
    assert_eq!(two_hop.len(), 15);

    let two_hop_set: HashSet<_> = two_hop.iter().copied().collect();
    let expected_set: HashSet<_> = layer2_nodes.iter().copied().collect();
    assert_eq!(two_hop_set, expected_set);

    Ok(())
}

#[test]
fn test_three_hop_traversal() -> Result<()> {
    let (mut db, nodes) = setup_chain_graph(4);

    let three_hop = db.get_neighbors_three_hops(nodes[0])?;
    assert_eq!(three_hop.len(), 1);
    assert_eq!(three_hop[0], nodes[3]);

    Ok(())
}

#[test]
fn test_three_hop_no_duplicates() -> Result<()> {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open_with_config(&path, Config::balanced()).unwrap();

    let a = db.add_node(Node::new(1))?;
    let b = db.add_node(Node::new(2))?;
    let c = db.add_node(Node::new(3))?;
    let d = db.add_node(Node::new(4))?;
    let e = db.add_node(Node::new(5))?;

    db.add_edge(Edge::new(0, a, b, "edge"))?;
    db.add_edge(Edge::new(0, a, c, "edge"))?;
    db.add_edge(Edge::new(0, b, d, "edge"))?;
    db.add_edge(Edge::new(0, c, d, "edge"))?;
    db.add_edge(Edge::new(0, d, e, "edge"))?;

    let three_hop = db.get_neighbors_three_hops(a)?;

    assert_eq!(three_hop.len(), 1);
    assert_eq!(three_hop[0], e);

    Ok(())
}

#[test]
fn test_bfs_traversal_chain() -> Result<()> {
    let (mut db, nodes) = setup_chain_graph(10);

    let result = db.bfs_traversal(nodes[0], 10)?;

    assert_eq!(result.len(), 10);
    for i in 0..10 {
        assert_eq!(result[i], (nodes[i], i));
    }

    Ok(())
}

#[test]
fn test_bfs_traversal_depth_limit() -> Result<()> {
    let (mut db, nodes) = setup_chain_graph(10);

    let result = db.bfs_traversal(nodes[0], 5)?;

    assert_eq!(result.len(), 5);
    for i in 0..5 {
        assert_eq!(result[i], (nodes[i], i));
    }

    Ok(())
}

#[test]
fn test_bfs_traversal_star() -> Result<()> {
    let (mut db, center, neighbors) = setup_star_graph(10);

    let result = db.bfs_traversal(center, 2)?;

    assert_eq!(result.len(), 11);
    assert_eq!(result[0], (center, 0));

    let depth1_nodes: Vec<_> = result[1..11].iter().map(|&(id, _)| id).collect();
    let depth1_set: HashSet<_> = depth1_nodes.iter().copied().collect();
    let expected_set: HashSet<_> = neighbors.iter().copied().collect();
    assert_eq!(depth1_set, expected_set);

    for i in 1..11 {
        assert_eq!(result[i].1, 1);
    }

    Ok(())
}

#[test]
fn test_parallel_bfs() -> Result<()> {
    let (mut db, nodes) = setup_chain_graph(10);

    let result = db.parallel_bfs(nodes[0], 10)?;

    assert_eq!(result.len(), 10);

    let result_sorted: Vec<_> = {
        let mut r = result;
        r.sort_by_key(|&(id, _)| id);
        r
    };

    for i in 0..10 {
        assert_eq!(result_sorted[i].0, nodes[i]);
        assert_eq!(result_sorted[i].1, i);
    }

    Ok(())
}

#[test]
fn test_parallel_bfs_star() -> Result<()> {
    let (mut db, center, neighbors) = setup_star_graph(100);

    let result = db.parallel_bfs(center, 2)?;

    assert_eq!(result.len(), 101);
    assert_eq!(result[0], (center, 0));

    let depth1_nodes: Vec<_> = result[1..101].iter().map(|&(id, _)| id).collect();
    let depth1_set: HashSet<_> = depth1_nodes.iter().copied().collect();
    let expected_set: HashSet<_> = neighbors.iter().copied().collect();
    assert_eq!(depth1_set, expected_set);

    Ok(())
}

#[test]
fn test_parallel_multi_hop_basic() -> Result<()> {
    let (mut db, nodes) = setup_diamond_graph();

    let batch = vec![nodes[0]];
    let result = db.parallel_multi_hop_neighbors(&batch, 2)?;

    assert_eq!(result.len(), 1);
    assert!(result.contains_key(&nodes[0]));

    let neighbors = &result[&nodes[0]];
    assert_eq!(neighbors.len(), 3);

    let neighbors_set: HashSet<_> = neighbors.iter().copied().collect();
    assert!(neighbors_set.contains(&nodes[1]));
    assert!(neighbors_set.contains(&nodes[2]));
    assert!(neighbors_set.contains(&nodes[3]));

    Ok(())
}

#[test]
fn test_parallel_multi_hop_batch() -> Result<()> {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    let mut db = GraphDB::open_with_config(&path, Config::balanced()).unwrap();

    let center = db.add_node(Node::new(0))?;
    let mut spokes = Vec::new();
    for i in 0..10 {
        let spoke = db.add_node(Node::new(i))?;
        db.add_edge(Edge::new(0, center, spoke, "edge"))?;

        for j in 0..5 {
            let leaf = db.add_node(Node::new((i * 100 + j) as u64))?;
            db.add_edge(Edge::new(0, spoke, leaf, "edge"))?;
        }
        spokes.push(spoke);
    }

    let result = db.parallel_multi_hop_neighbors(&spokes, 2)?;

    assert_eq!(result.len(), 10);
    for &node_id in &spokes {
        assert!(result.contains_key(&node_id));
        let neighbors = &result[&node_id];
        assert!(
            !neighbors.is_empty(),
            "Expected neighbors for node {}",
            node_id
        );
    }

    Ok(())
}

#[test]
fn test_parallel_multi_hop_empty_batch() -> Result<()> {
    let (mut db, _, _) = setup_star_graph(10);

    let result = db.parallel_multi_hop_neighbors(&[], 2)?;

    assert_eq!(result.len(), 0);

    Ok(())
}

#[test]
fn test_parallel_multi_hop_zero_hops() -> Result<()> {
    let (mut db, _, neighbors) = setup_star_graph(10);

    let batch: Vec<_> = neighbors[..5].to_vec();
    let result = db.parallel_multi_hop_neighbors(&batch, 0)?;

    assert_eq!(result.len(), 5);
    for &node_id in &batch {
        assert!(result.contains_key(&node_id));
        assert_eq!(result[&node_id].len(), 0);
    }

    Ok(())
}

#[test]
fn test_traversal_edge_count_metrics() -> Result<()> {
    let (mut db, center, _) = setup_star_graph(10);

    let edge_traversals_before = db.metrics.edge_traversals;

    db.get_neighbors(center)?;

    let edge_traversals_after = db.metrics.edge_traversals;

    assert!(edge_traversals_after >= edge_traversals_before + 10);

    Ok(())
}

#[test]
fn test_traversal_cycle_handling() -> Result<()> {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    let mut db = GraphDB::open_with_config(&path, Config::balanced()).unwrap();

    let a = db.add_node(Node::new(1))?;
    let b = db.add_node(Node::new(2))?;
    let c = db.add_node(Node::new(3))?;

    db.add_edge(Edge::new(0, a, b, "edge"))?;
    db.add_edge(Edge::new(0, b, c, "edge"))?;
    db.add_edge(Edge::new(0, c, a, "edge"))?;

    let result = db.bfs_traversal(a, 10)?;
    assert_eq!(result.len(), 3);

    let ids: HashSet<_> = result.iter().map(|&(id, _)| id).collect();
    assert_eq!(ids.len(), 3);

    Ok(())
}

#[test]
fn test_large_fanout_traversal() -> Result<()> {
    let (mut db, center, _) = setup_star_graph(1000);

    let neighbors = db.get_neighbors(center)?;
    assert_eq!(neighbors.len(), 1000);

    let cached_neighbors = db.get_neighbors(center)?;
    assert_eq!(cached_neighbors.len(), 1000);
    assert_eq!(neighbors, cached_neighbors);

    Ok(())
}
