#![allow(missing_docs)]

use std::sync::Arc;

use sombra::primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions};
#[cfg(feature = "degree-cache")]
use sombra::storage::DegreeDir;
use sombra::storage::{
    DeleteNodeOpts, Dir, EdgeSpec, ExpandOpts, Graph, GraphOptions, NodeSpec, PropEntry, PropPatch,
    PropPatchOp, PropValue, PropValueOwned,
};
use sombra::types::{EdgeId, LabelId, NodeId, PropId, Result, SombraError, TypeId};
use tempfile::tempdir;

fn setup_graph(path: &std::path::Path) -> Result<(Arc<Pager>, Arc<Graph>)> {
    let pager = Arc::new(Pager::create(path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let graph = Graph::open(GraphOptions::new(store))?;
    Ok((pager, graph))
}

#[test]
fn edge_creation_populates_adjacency() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("adjacency.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;
    let dst = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(2)],
            props: &[],
        },
    )?;
    let ty = TypeId(5);
    let edge = graph.create_edge(
        &mut write,
        EdgeSpec {
            src,
            dst,
            ty,
            props: &[],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let fwd = graph.debug_collect_adj_fwd(&read)?;
    assert_eq!(fwd, vec![(src, ty, dst, edge)]);
    let rev = graph.debug_collect_adj_rev(&read)?;
    assert_eq!(rev, vec![(dst, ty, src, edge)]);
    Ok(())
}

#[test]
fn create_edge_requires_existing_endpoints() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("missing_nodes.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let err = graph.create_edge(
        &mut write,
        EdgeSpec {
            src: NodeId(42),
            dst: NodeId(43),
            ty: TypeId(7),
            props: &[],
        },
    );
    match err {
        Err(SombraError::Invalid(msg)) => {
            assert_eq!(msg, "edge source node missing");
        }
        other => panic!("unexpected result: {:?}", other),
    }
    Ok(())
}

#[test]
fn delete_edge_clears_adjacency_entries() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("delete_edge.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;
    let dst = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(2)],
            props: &[],
        },
    )?;
    let ty = TypeId(3);
    let edge = graph.create_edge(
        &mut write,
        EdgeSpec {
            src,
            dst,
            ty,
            props: &[],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let mut write = pager.begin_write()?;
    graph.delete_edge(&mut write, edge)?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    assert!(graph.debug_collect_adj_fwd(&read)?.is_empty());
    assert!(graph.debug_collect_adj_rev(&read)?.is_empty());
    assert!(graph.get_edge(&read, edge)?.is_none());
    Ok(())
}

#[test]
fn edge_update_properties_replaces_payload() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("edge_update_props.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;
    let dst = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(2)],
            props: &[],
        },
    )?;
    let edge = graph.create_edge(
        &mut write,
        EdgeSpec {
            src,
            dst,
            ty: TypeId(9),
            props: &[PropEntry::new(PropId(1), PropValue::Int(10))],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let mut write = pager.begin_write()?;
    graph.update_edge(
        &mut write,
        edge,
        PropPatch::new(vec![
            PropPatchOp::Set(PropId(1), PropValue::Int(20)),
            PropPatchOp::Set(PropId(2), PropValue::Str("updated")),
        ]),
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let data = graph.get_edge(&read, edge)?.expect("edge present");
    assert_eq!(data.props.len(), 2);
    assert_eq!(data.props[0].0, PropId(1));
    assert_eq!(data.props[1].0, PropId(2));
    if let PropValueOwned::Int(value) = data.props[0].1 {
        assert_eq!(value, 20);
    } else {
        panic!("expected int prop");
    }
    if let PropValueOwned::Str(value) = &data.props[1].1 {
        assert_eq!(value, "updated");
    } else {
        panic!("expected string prop");
    }
    let adj = graph.debug_collect_adj_fwd(&read)?;
    assert_eq!(adj, vec![(src, TypeId(9), dst, edge)]);
    Ok(())
}

#[test]
fn edge_update_missing_returns_not_found() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("edge_update_missing.db");
    let (pager, graph) = setup_graph(&path)?;
    let mut write = pager.begin_write()?;
    let result = graph.update_edge(
        &mut write,
        EdgeId(999),
        PropPatch::new(vec![PropPatchOp::Set(PropId(1), PropValue::Int(1))]),
    );
    assert!(matches!(result, Err(SombraError::NotFound)));
    Ok(())
}

#[test]
fn neighbors_out_collects_edges() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("neighbors_out.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let node_a = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;
    let node_b = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(2)],
            props: &[],
        },
    )?;
    let node_c = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(3)],
            props: &[],
        },
    )?;
    let edge1 = graph.create_edge(
        &mut write,
        EdgeSpec {
            src: node_a,
            dst: node_b,
            ty: TypeId(10),
            props: &[],
        },
    )?;
    let edge2 = graph.create_edge(
        &mut write,
        EdgeSpec {
            src: node_a,
            dst: node_c,
            ty: TypeId(11),
            props: &[],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let cursor = graph.neighbors(&read, node_a, Dir::Out, None, ExpandOpts::default())?;
    let neighbors: Vec<_> = cursor.collect();
    assert_eq!(neighbors.len(), 2);
    assert_eq!(neighbors[0].neighbor, node_b);
    assert_eq!(neighbors[0].edge, edge1);
    assert_eq!(neighbors[0].ty, TypeId(10));
    assert_eq!(neighbors[1].neighbor, node_c);
    assert_eq!(neighbors[1].edge, edge2);
    assert_eq!(neighbors[1].ty, TypeId(11));

    let filtered = graph.neighbors(
        &read,
        node_a,
        Dir::Out,
        Some(TypeId(10)),
        ExpandOpts::default(),
    )?;
    let filtered: Vec<_> = filtered.collect();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].neighbor, node_b);
    Ok(())
}

#[test]
fn neighbors_distinct_deduplicates_nodes() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("neighbors_distinct.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let node_a = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;
    let node_b = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(2)],
            props: &[],
        },
    )?;
    graph.create_edge(
        &mut write,
        EdgeSpec {
            src: node_a,
            dst: node_b,
            ty: TypeId(5),
            props: &[],
        },
    )?;
    graph.create_edge(
        &mut write,
        EdgeSpec {
            src: node_a,
            dst: node_b,
            ty: TypeId(6),
            props: &[],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let cursor = graph.neighbors(
        &read,
        node_a,
        Dir::Out,
        None,
        ExpandOpts {
            distinct_nodes: true,
        },
    )?;
    let neighbors: Vec<_> = cursor.collect();
    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[0].neighbor, node_b);
    Ok(())
}

#[test]
fn degree_counts_without_cache() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("degree_counts.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let node_a = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;
    let node_b = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(2)],
            props: &[],
        },
    )?;
    let node_c = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(3)],
            props: &[],
        },
    )?;
    graph.create_edge(
        &mut write,
        EdgeSpec {
            src: node_a,
            dst: node_b,
            ty: TypeId(1),
            props: &[],
        },
    )?;
    graph.create_edge(
        &mut write,
        EdgeSpec {
            src: node_a,
            dst: node_c,
            ty: TypeId(2),
            props: &[],
        },
    )?;
    graph.create_edge(
        &mut write,
        EdgeSpec {
            src: node_b,
            dst: node_a,
            ty: TypeId(1),
            props: &[],
        },
    )?;
    graph.create_edge(
        &mut write,
        EdgeSpec {
            src: node_a,
            dst: node_a,
            ty: TypeId(3),
            props: &[],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    assert_eq!(graph.degree(&read, node_a, Dir::Out, None)?, 3);
    assert_eq!(graph.degree(&read, node_a, Dir::Out, Some(TypeId(1)))?, 1);
    assert_eq!(graph.degree(&read, node_a, Dir::In, None)?, 2);
    assert_eq!(graph.degree(&read, node_a, Dir::Both, None)?, 4);
    Ok(())
}

#[test]
fn node_update_applies_patch() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("node_update.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let node = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[
                PropEntry::new(PropId(1), PropValue::Int(5)),
                PropEntry::new(PropId(2), PropValue::Str("old")),
            ],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let mut write = pager.begin_write()?;
    graph.update_node(
        &mut write,
        node,
        PropPatch::new(vec![
            PropPatchOp::Set(PropId(1), PropValue::Int(9)),
            PropPatchOp::Delete(PropId(2)),
            PropPatchOp::Set(PropId(3), PropValue::Str("new")),
        ]),
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let data = graph.get_node(&read, node)?.expect("node present");
    assert_eq!(data.props.len(), 2);
    assert_eq!(data.props[0].0, PropId(1));
    assert_eq!(data.props[1].0, PropId(3));
    assert!(matches!(data.props[0].1, PropValueOwned::Int(9)));
    if let PropValueOwned::Str(value) = &data.props[1].1 {
        assert_eq!(value, "new");
    } else {
        panic!("expected string value");
    }
    Ok(())
}

#[test]
fn node_update_missing_returns_not_found() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("node_update_missing.db");
    let (pager, graph) = setup_graph(&path)?;
    let mut write = pager.begin_write()?;
    let result = graph.update_node(
        &mut write,
        NodeId(77),
        PropPatch::new(vec![PropPatchOp::Set(PropId(1), PropValue::Int(1))]),
    );
    assert!(matches!(result, Err(SombraError::NotFound)));
    Ok(())
}

#[test]
fn delete_node_restrict_blocks_incident_edges() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("node_restrict.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(10)],
            props: &[],
        },
    )?;
    let dst = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(11)],
            props: &[],
        },
    )?;
    graph.create_edge(
        &mut write,
        EdgeSpec {
            src,
            dst,
            ty: TypeId(4),
            props: &[],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let mut write = pager.begin_write()?;
    let err = graph.delete_node(&mut write, src, DeleteNodeOpts::restrict());
    match err {
        Err(SombraError::Invalid(msg)) => {
            assert_eq!(msg, "node has incident edges");
        }
        other => panic!("expected invalid error, got {:?}", other),
    }
    Ok(())
}

#[test]
fn delete_node_cascade_removes_incident_edges() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("node_cascade.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(20)],
            props: &[],
        },
    )?;
    let dst = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(21)],
            props: &[],
        },
    )?;
    let edge = graph.create_edge(
        &mut write,
        EdgeSpec {
            src,
            dst,
            ty: TypeId(12),
            props: &[],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let mut write = pager.begin_write()?;
    graph.delete_node(&mut write, src, DeleteNodeOpts::cascade())?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    assert!(graph.get_node(&read, src)?.is_none());
    assert!(graph.get_edge(&read, edge)?.is_none());
    assert!(graph.debug_collect_adj_fwd(&read)?.is_empty());
    assert!(graph.debug_collect_adj_rev(&read)?.is_empty());
    // Destination node remains.
    assert!(graph.get_node(&read, dst)?.is_some());
    Ok(())
}

#[cfg(feature = "degree-cache")]
#[test]
fn degree_cache_tracks_counts() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("degree_cache.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(11)],
            props: &[],
        },
    )?;
    let dst = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(12)],
            props: &[],
        },
    )?;
    let ty = TypeId(99);
    let edge = graph.create_edge(
        &mut write,
        EdgeSpec {
            src,
            dst,
            ty,
            props: &[],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let mut entries = graph.debug_collect_degree(&read)?;
    entries.sort_by_key(|entry| (entry.0, entry.1.into_u8(), entry.2));
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0], (src, DegreeDir::Out, ty, 1));
    assert_eq!(entries[1], (dst, DegreeDir::In, ty, 1));
    drop(read);

    let mut write = pager.begin_write()?;
    graph.delete_edge(&mut write, edge)?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    assert!(graph.debug_collect_degree(&read)?.is_empty());
    Ok(())
}

#[cfg(feature = "degree-cache")]
#[test]
fn degree_cache_validation_detects_mismatch() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("degree_validation.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(30)],
            props: &[],
        },
    )?;
    let dst = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(31)],
            props: &[],
        },
    )?;
    let ty = TypeId(77);
    graph.create_edge(
        &mut write,
        EdgeSpec {
            src,
            dst,
            ty,
            props: &[],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    graph.validate_degree_cache(&read)?;
    drop(read);

    let mut write = pager.begin_write()?;
    graph.debug_set_degree_entry(&mut write, src, DegreeDir::Out, ty, 5)?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let result = graph.validate_degree_cache(&read);
    assert!(result.is_err());
    Ok(())
}
