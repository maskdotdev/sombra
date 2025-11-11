#![allow(missing_docs)]

use std::sync::Arc;

use sombra::primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions};
use sombra::storage::{
    EdgeSpec, Graph, GraphOptions, NodeSpec, PropEntry, PropValue, PropValueOwned,
};
use sombra::types::{LabelId, PropId, Result, TypeId};
use tempfile::tempdir;

fn setup_graph(path: &std::path::Path) -> Result<(Arc<Pager>, Graph)> {
    let pager = Arc::new(Pager::create(path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let graph = Graph::open(GraphOptions::new(store))?;
    Ok((pager, graph))
}

#[test]
fn node_roundtrip_inline_props() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("node_inline.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let node_id = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(5), LabelId(1), LabelId(5)],
            props: &[
                PropEntry::new(PropId(10), PropValue::Int(42)),
                PropEntry::new(PropId(5), PropValue::Bool(true)),
                PropEntry::new(PropId(7), PropValue::Str("hello")),
            ],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let node = graph.get_node(&read, node_id)?.expect("node present");
    assert_eq!(node.labels, vec![LabelId(1), LabelId(5)]);
    assert_eq!(node.props.len(), 3);
    assert_eq!(node.props[0].0, PropId(5));
    assert_eq!(node.props[1].0, PropId(7));
    assert_eq!(node.props[2].0, PropId(10));
    Ok(())
}

#[test]
fn node_property_spills_large_string() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("node_spill.db");
    let (pager, graph) = setup_graph(&path)?;
    let large = "x".repeat(256);

    let mut write = pager.begin_write()?;
    let node_id = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(3)],
            props: &[PropEntry::new(PropId(1), PropValue::Str(&large))],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let node = graph.get_node(&read, node_id)?.expect("node present");
    assert_eq!(node.props.len(), 1);
    assert_eq!(node.props[0].0, PropId(1));
    if let PropValueOwned::Str(value) = &node.props[0].1 {
        assert_eq!(value.len(), large.len());
    } else {
        panic!("expected string value");
    }
    Ok(())
}

#[test]
fn edge_roundtrip() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("edge_roundtrip.db");
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
    let edge_id = graph.create_edge(
        &mut write,
        EdgeSpec {
            src,
            dst,
            ty: TypeId(9),
            props: &[
                PropEntry::new(PropId(2), PropValue::Float(3.14)),
                PropEntry::new(PropId(8), PropValue::Bytes(&[1, 2, 3])),
            ],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let edge = graph.get_edge(&read, edge_id)?.expect("edge present");
    assert_eq!(edge.src, src);
    assert_eq!(edge.dst, dst);
    assert_eq!(edge.ty, TypeId(9));
    assert_eq!(edge.props.len(), 2);
    Ok(())
}
