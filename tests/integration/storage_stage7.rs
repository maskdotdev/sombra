#![allow(missing_docs)]

use std::ops::Bound;
use std::sync::Arc;

use sombra::primitives::pager::{PageStore, Pager, PagerOptions};
use sombra::storage::index::{collect_all, intersect_k, intersect_sorted, PostingStream};
use sombra::storage::{
    BulkEdgeValidator, CreateEdgeOptions, DeleteNodeOpts, EdgeSpec, Graph, GraphOptions,
    GraphWriter, IndexDef, IndexKind, LabelScan, NodeSpec, PropEntry, PropPatch, PropPatchOp,
    PropValue, PropValueOwned, TypeTag,
};
use sombra::types::{LabelId, PropId, Result, SombraError, TypeId};
use tempfile::tempdir;

fn setup_graph(path: &std::path::Path) -> Result<(Arc<Pager>, Arc<Graph>)> {
    let pager = Arc::new(Pager::create(path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let graph = Graph::open(GraphOptions::new(store))?;
    Ok((pager, graph))
}

fn collect_scan(mut scan: LabelScan<'_>) -> Result<Vec<sombra::types::NodeId>> {
    let mut out = Vec::new();
    while let Some(node) = scan.next()? {
        out.push(node);
    }
    Ok(out)
}

fn collect_stream(stream: &mut dyn PostingStream) -> Result<Vec<sombra::types::NodeId>> {
    let mut out = Vec::new();
    collect_all(stream, &mut out)?;
    out.sort_by_key(|node| node.0);
    out.dedup_by_key(|node| node.0);
    Ok(out)
}

fn collect_property_eq_stream(
    pager: &Arc<Pager>,
    graph: &Graph,
    label: LabelId,
    prop: PropId,
    value: PropValueOwned,
) -> Result<Vec<sombra::types::NodeId>> {
    let read = pager.begin_latest_committed_read()?;
    let mut stream = graph.property_scan_eq_stream(&read, label, prop, &value)?;
    let result = collect_stream(&mut *stream)?;
    drop(stream);
    drop(read);
    Ok(result)
}

fn collect_property_range_stream(
    pager: &Arc<Pager>,
    graph: &Graph,
    label: LabelId,
    prop: PropId,
    start: Bound<PropValueOwned>,
    end: Bound<PropValueOwned>,
) -> Result<Vec<sombra::types::NodeId>> {
    let read = pager.begin_latest_committed_read()?;
    let start_owned = start;
    let end_owned = end;
    let start_ref = match &start_owned {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(v) => Bound::Included(v),
        Bound::Excluded(v) => Bound::Excluded(v),
    };
    let end_ref = match &end_owned {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(v) => Bound::Included(v),
        Bound::Excluded(v) => Bound::Excluded(v),
    };
    let mut stream = graph.property_scan_range_stream(&read, label, prop, start_ref, end_ref)?;
    let result = collect_stream(&mut *stream)?;
    drop(stream);
    drop(read);
    Ok(result)
}

#[test]
fn label_index_create_scan_drop() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("label_index.db");
    let (pager, graph) = setup_graph(&path)?;

    // Populate a few nodes across two labels.
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
            labels: &[LabelId(1), LabelId(2)],
            props: &[],
        },
    )?;
    let _node_c = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(2)],
            props: &[],
        },
    )?;
    pager.commit(write)?;

    // Create and validate the label index.
    let mut write = pager.begin_write()?;
    graph.create_label_index(&mut write, LabelId(1))?;
    pager.commit(write)?;
    assert!(graph.has_label_index(LabelId(1))?);

    let read = pager.begin_latest_committed_read()?;
    let scan = graph
        .label_scan(&read, LabelId(1))?
        .expect("label index stream");
    let nodes = collect_scan(scan)?;
    assert_eq!(nodes, vec![node_a, node_b]);
    drop(read);

    // Inserts after index creation should appear in scans.
    let mut write = pager.begin_write()?;
    let node_d = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;
    pager.commit(write)?;

    let read = pager.begin_latest_committed_read()?;
    let scan = graph
        .label_scan(&read, LabelId(1))?
        .expect("label index stream after insert");
    let nodes = collect_scan(scan)?;
    assert_eq!(nodes, vec![node_a, node_b, node_d]);
    drop(read);

    // Deletes should be reflected immediately.
    let mut write = pager.begin_write()?;
    graph.delete_node(&mut write, node_a, DeleteNodeOpts::restrict())?;
    pager.commit(write)?;

    let read = pager.begin_latest_committed_read()?;
    let scan = graph
        .label_scan(&read, LabelId(1))?
        .expect("label index stream after delete");
    let nodes = collect_scan(scan)?;
    assert_eq!(nodes, vec![node_b, node_d]);
    drop(read);

    // Dropping the index removes it from future scans.
    let mut write = pager.begin_write()?;
    graph.drop_label_index(&mut write, LabelId(1))?;
    pager.commit(write)?;
    assert!(!graph.has_label_index(LabelId(1))?);

    let read = pager.begin_latest_committed_read()?;
    assert!(
        graph.label_scan(&read, LabelId(1))?.is_none(),
        "label scan absent after drop"
    );
    Ok(())
}

#[test]
fn property_index_chunked_eq_updates() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("prop_chunked.db");
    let (pager, graph) = setup_graph(&path)?;

    // Seed nodes with integer property.
    let mut write = pager.begin_write()?;
    let node_a = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(10))],
        },
    )?;
    let node_b = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(20))],
        },
    )?;
    pager.commit(write)?;

    // Create the chunked property index.
    let mut write = pager.begin_write()?;
    graph.create_property_index(
        &mut write,
        IndexDef {
            label: LabelId(1),
            prop: PropId(1),
            kind: IndexKind::Chunked,
            ty: TypeTag::Int,
        },
    )?;
    pager.commit(write)?;
    assert!(graph.has_property_index(LabelId(1), PropId(1))?);

    // Equality scan matches existing nodes.
    let read = pager.begin_latest_committed_read()?;
    let mut matches =
        graph.property_scan_eq(&read, LabelId(1), PropId(1), &PropValueOwned::Int(10))?;
    drop(read);
    matches.sort_by_key(|node| node.0);
    assert_eq!(matches, vec![node_a]);
    let streamed = collect_property_eq_stream(
        &pager,
        &graph,
        LabelId(1),
        PropId(1),
        PropValueOwned::Int(10),
    )?;
    assert_eq!(streamed, vec![node_a]);

    // Inserting a new node updates the index.
    let mut write = pager.begin_write()?;
    let node_c = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(10))],
        },
    )?;
    pager.commit(write)?;

    let read = pager.begin_latest_committed_read()?;
    let mut matches =
        graph.property_scan_eq(&read, LabelId(1), PropId(1), &PropValueOwned::Int(10))?;
    drop(read);
    matches.sort_by_key(|node| node.0);
    assert_eq!(matches, vec![node_a, node_c]);
    let streamed = collect_property_eq_stream(
        &pager,
        &graph,
        LabelId(1),
        PropId(1),
        PropValueOwned::Int(10),
    )?;
    assert_eq!(streamed, vec![node_a, node_c]);

    // Updating a node's property reflects immediately.
    let mut write = pager.begin_write()?;
    graph.update_node(
        &mut write,
        node_b,
        PropPatch::new(vec![PropPatchOp::Set(PropId(1), PropValue::Int(10))]),
    )?;
    pager.commit(write)?;

    let read = pager.begin_latest_committed_read()?;
    let mut matches =
        graph.property_scan_eq(&read, LabelId(1), PropId(1), &PropValueOwned::Int(10))?;
    drop(read);
    matches.sort_by_key(|node| node.0);
    assert_eq!(matches, vec![node_a, node_b, node_c]);
    let streamed = collect_property_eq_stream(
        &pager,
        &graph,
        LabelId(1),
        PropId(1),
        PropValueOwned::Int(10),
    )?;
    assert_eq!(streamed, vec![node_a, node_b, node_c]);

    // Deleting a node removes it from the index.
    let mut write = pager.begin_write()?;
    graph.delete_node(&mut write, node_a, DeleteNodeOpts::restrict())?;
    pager.commit(write)?;

    let read = pager.begin_latest_committed_read()?;
    let mut matches =
        graph.property_scan_eq(&read, LabelId(1), PropId(1), &PropValueOwned::Int(10))?;
    drop(read);
    matches.sort_by_key(|node| node.0);
    assert_eq!(matches, vec![node_b, node_c]);
    let streamed = collect_property_eq_stream(
        &pager,
        &graph,
        LabelId(1),
        PropId(1),
        PropValueOwned::Int(10),
    )?;
    assert_eq!(streamed, vec![node_b, node_c]);

    // Dropping the index clears it from the catalog.
    let mut write = pager.begin_write()?;
    graph.drop_property_index(&mut write, LabelId(1), PropId(1))?;
    pager.commit(write)?;
    assert!(!graph.has_property_index(LabelId(1), PropId(1))?);

    Ok(())
}

#[test]
fn property_index_btree_eq() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("prop_btree.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let node_a = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(2)],
            props: &[PropEntry::new(PropId(2), PropValue::Str("alpha"))],
        },
    )?;
    let _node_b = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(2)],
            props: &[PropEntry::new(PropId(2), PropValue::Str("beta"))],
        },
    )?;
    pager.commit(write)?;

    let mut write = pager.begin_write()?;
    graph.create_property_index(
        &mut write,
        IndexDef {
            label: LabelId(2),
            prop: PropId(2),
            kind: IndexKind::BTree,
            ty: TypeTag::String,
        },
    )?;
    pager.commit(write)?;

    let read = pager.begin_latest_committed_read()?;
    let matches = graph.property_scan_eq(
        &read,
        LabelId(2),
        PropId(2),
        &PropValueOwned::Str("alpha".into()),
    )?;
    drop(read);
    assert_eq!(matches, vec![node_a]);

    Ok(())
}

#[test]
fn property_index_chunked_range() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("prop_chunked_range.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let node_a = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(3)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(5))],
        },
    )?;
    let node_b = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(3)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(10))],
        },
    )?;
    let node_c = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(3)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(20))],
        },
    )?;
    let _node_d = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(3)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(30))],
        },
    )?;
    pager.commit(write)?;

    let mut write = pager.begin_write()?;
    graph.create_property_index(
        &mut write,
        IndexDef {
            label: LabelId(3),
            prop: PropId(1),
            kind: IndexKind::Chunked,
            ty: TypeTag::Int,
        },
    )?;
    pager.commit(write)?;

    let read = pager.begin_latest_committed_read()?;
    let matches = graph.property_scan_range(
        &read,
        LabelId(3),
        PropId(1),
        &PropValueOwned::Int(10),
        &PropValueOwned::Int(25),
    )?;
    drop(read);
    assert_eq!(matches, vec![node_b, node_c]);

    let streamed = collect_property_range_stream(
        &pager,
        &graph,
        LabelId(3),
        PropId(1),
        Bound::Included(PropValueOwned::Int(10)),
        Bound::Included(PropValueOwned::Int(25)),
    )?;
    assert_eq!(streamed, vec![node_b, node_c]);

    let start_excl = PropValueOwned::Int(10);
    let end_excl = PropValueOwned::Int(25);
    let read = pager.begin_latest_committed_read()?;
    let matches = graph.property_scan_range_bounds(
        &read,
        LabelId(3),
        PropId(1),
        Bound::Excluded(&start_excl),
        Bound::Excluded(&end_excl),
    )?;
    drop(read);
    assert_eq!(matches, vec![node_c]);

    let read = pager.begin_latest_committed_read()?;
    let low = graph.property_scan_range(
        &read,
        LabelId(3),
        PropId(1),
        &PropValueOwned::Int(0),
        &PropValueOwned::Int(9),
    )?;
    drop(read);
    assert_eq!(low, vec![node_a]);
    let streamed = collect_property_range_stream(
        &pager,
        &graph,
        LabelId(3),
        PropId(1),
        Bound::Unbounded,
        Bound::Included(PropValueOwned::Int(9)),
    )?;
    assert_eq!(streamed, vec![node_a]);

    let upper_excl = PropValueOwned::Int(20);
    let read = pager.begin_latest_committed_read()?;
    let matches = graph.property_scan_range_bounds(
        &read,
        LabelId(3),
        PropId(1),
        Bound::Unbounded,
        Bound::Excluded(&upper_excl),
    )?;
    drop(read);
    assert_eq!(matches, vec![node_a, node_b]);
    let streamed = collect_property_range_stream(
        &pager,
        &graph,
        LabelId(3),
        PropId(1),
        Bound::Unbounded,
        Bound::Excluded(upper_excl.clone()),
    )?;
    assert_eq!(streamed, vec![node_a, node_b]);

    let read = pager.begin_latest_committed_read()?;
    let empty = graph.property_scan_range(
        &read,
        LabelId(3),
        PropId(1),
        &PropValueOwned::Int(35),
        &PropValueOwned::Int(40),
    )?;
    drop(read);
    assert!(empty.is_empty());
    let streamed = collect_property_range_stream(
        &pager,
        &graph,
        LabelId(3),
        PropId(1),
        Bound::Included(PropValueOwned::Int(35)),
        Bound::Included(PropValueOwned::Int(40)),
    )?;
    assert!(streamed.is_empty());

    let read = pager.begin_latest_committed_read()?;
    let inverted = graph.property_scan_range(
        &read,
        LabelId(3),
        PropId(1),
        &PropValueOwned::Int(25),
        &PropValueOwned::Int(10),
    )?;
    drop(read);
    assert!(inverted.is_empty());
    let streamed = collect_property_range_stream(
        &pager,
        &graph,
        LabelId(3),
        PropId(1),
        Bound::Included(PropValueOwned::Int(25)),
        Bound::Included(PropValueOwned::Int(10)),
    )?;
    assert!(streamed.is_empty());

    Ok(())
}

#[test]
fn property_index_btree_range() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("prop_btree_range.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let node_a = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(4)],
            props: &[PropEntry::new(PropId(2), PropValue::Str("aaa"))],
        },
    )?;
    let node_b = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(4)],
            props: &[PropEntry::new(PropId(2), PropValue::Str("bbb"))],
        },
    )?;
    let node_c = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(4)],
            props: &[PropEntry::new(PropId(2), PropValue::Str("ccc"))],
        },
    )?;
    pager.commit(write)?;

    let mut write = pager.begin_write()?;
    graph.create_property_index(
        &mut write,
        IndexDef {
            label: LabelId(4),
            prop: PropId(2),
            kind: IndexKind::BTree,
            ty: TypeTag::String,
        },
    )?;
    pager.commit(write)?;

    let read = pager.begin_latest_committed_read()?;
    let matches = graph.property_scan_range(
        &read,
        LabelId(4),
        PropId(2),
        &PropValueOwned::Str("aaa".into()),
        &PropValueOwned::Str("bbb".into()),
    )?;
    drop(read);
    assert_eq!(matches, vec![node_a, node_b]);

    let streamed = collect_property_range_stream(
        &pager,
        &graph,
        LabelId(4),
        PropId(2),
        Bound::Included(PropValueOwned::Str("aaa".into())),
        Bound::Included(PropValueOwned::Str("bbb".into())),
    )?;
    assert_eq!(streamed, vec![node_a, node_b]);

    let start_excl = PropValueOwned::Str("aaa".into());
    let end_incl = PropValueOwned::Str("ccc".into());
    let read = pager.begin_latest_committed_read()?;
    let matches = graph.property_scan_range_bounds(
        &read,
        LabelId(4),
        PropId(2),
        Bound::Excluded(&start_excl),
        Bound::Included(&end_incl),
    )?;
    drop(read);
    assert_eq!(matches, vec![node_b, node_c]);

    let streamed = collect_property_range_stream(
        &pager,
        &graph,
        LabelId(4),
        PropId(2),
        Bound::Excluded(start_excl),
        Bound::Included(end_incl),
    )?;
    assert_eq!(streamed, vec![node_b, node_c]);

    let read = pager.begin_latest_committed_read()?;
    let full = graph.property_scan_range(
        &read,
        LabelId(4),
        PropId(2),
        &PropValueOwned::Str("aaa".into()),
        &PropValueOwned::Str("ccc".into()),
    )?;
    drop(read);
    assert_eq!(full, vec![node_a, node_b, node_c]);

    Ok(())
}

#[test]
fn posting_stream_intersection() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("stream_intersection.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let node_a = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(5)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(1))],
        },
    )?;
    let _node_b = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(5)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(2))],
        },
    )?;
    let node_c = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(5)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(1))],
        },
    )?;
    let _node_d = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(6)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(1))],
        },
    )?;
    pager.commit(write)?;

    let mut write = pager.begin_write()?;
    graph.create_label_index(&mut write, LabelId(5))?;
    graph.create_property_index(
        &mut write,
        IndexDef {
            label: LabelId(5),
            prop: PropId(1),
            kind: IndexKind::Chunked,
            ty: TypeTag::Int,
        },
    )?;
    pager.commit(write)?;

    let mut intersection_sorted = {
        let read = pager.begin_latest_committed_read()?;
        let mut label_stream = graph
            .label_scan(&read, LabelId(5))?
            .expect("label stream available");
        let mut prop_stream =
            graph.property_scan_eq_stream(&read, LabelId(5), PropId(1), &PropValueOwned::Int(1))?;
        let mut result = Vec::new();
        intersect_sorted(
            &mut label_stream as &mut dyn PostingStream,
            &mut *prop_stream,
            &mut result,
        )?;
        result
    };
    intersection_sorted.sort_by_key(|node| node.0);
    assert_eq!(intersection_sorted, vec![node_a, node_c]);

    let mut intersection_k = {
        let read = pager.begin_latest_committed_read()?;
        let mut label_stream = graph
            .label_scan(&read, LabelId(5))?
            .expect("label stream available");
        let mut prop_stream =
            graph.property_scan_eq_stream(&read, LabelId(5), PropId(1), &PropValueOwned::Int(1))?;
        let mut result = Vec::new();
        {
            let mut streams: [&mut dyn PostingStream; 2] = [
                &mut label_stream as &mut dyn PostingStream,
                &mut *prop_stream,
            ];
            intersect_k(&mut streams, &mut result)?;
        }
        result
    };
    intersection_k.sort_by_key(|node| node.0);
    assert_eq!(intersection_k, vec![node_a, node_c]);

    Ok(())
}

#[test]
fn posting_stream_intersection_btree() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("stream_intersection_btree.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let node_a = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(7)],
            props: &[PropEntry::new(PropId(2), PropValue::Str("alpha"))],
        },
    )?;
    let _node_b = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(7)],
            props: &[PropEntry::new(PropId(2), PropValue::Str("beta"))],
        },
    )?;
    let node_c = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(7)],
            props: &[PropEntry::new(PropId(2), PropValue::Str("alpha"))],
        },
    )?;
    let _node_d = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(8)],
            props: &[PropEntry::new(PropId(2), PropValue::Str("alpha"))],
        },
    )?;
    pager.commit(write)?;

    let mut write = pager.begin_write()?;
    graph.create_label_index(&mut write, LabelId(7))?;
    graph.create_property_index(
        &mut write,
        IndexDef {
            label: LabelId(7),
            prop: PropId(2),
            kind: IndexKind::BTree,
            ty: TypeTag::String,
        },
    )?;
    pager.commit(write)?;

    let mut intersection = {
        let read = pager.begin_latest_committed_read()?;
        let mut label_stream = graph
            .label_scan(&read, LabelId(7))?
            .expect("label stream available");
        let mut prop_stream = graph.property_scan_eq_stream(
            &read,
            LabelId(7),
            PropId(2),
            &PropValueOwned::Str("alpha".into()),
        )?;
        let mut result = Vec::new();
        {
            let mut streams: [&mut dyn PostingStream; 2] = [
                &mut label_stream as &mut dyn PostingStream,
                &mut *prop_stream,
            ];
            intersect_k(&mut streams, &mut result)?;
        }
        result
    };
    intersection.sort_by_key(|node| node.0);
    assert_eq!(intersection, vec![node_a, node_c]);

    Ok(())
}

#[test]
fn graph_writer_requires_validation_before_trust() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("trusted_writer.db");
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
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;
    pager.commit(write)?;

    let opts = CreateEdgeOptions {
        trusted_endpoints: true,
        exists_cache_capacity: 0,
    };
    let mut writer = GraphWriter::try_new(&graph, opts, Some(Box::new(NoopValidator)))?;
    let edge_spec = EdgeSpec {
        src,
        dst,
        ty: TypeId(1),
        props: &[],
    };

    let mut write = pager.begin_write()?;
    let err = writer
        .create_edge(&mut write, edge_spec.clone())
        .expect_err("missing validation must error");
    match err {
        SombraError::Invalid(msg) => {
            assert_eq!(msg, "trusted endpoints batch must be validated");
        }
        other => panic!("unexpected error: {other:?}"),
    }
    writer.validate_trusted_batch(&[(src, dst)])?;
    writer.create_edge(&mut write, edge_spec)?;
    pager.commit(write)?;
    assert_eq!(writer.stats().trusted_edges, 1);
    Ok(())
}

#[test]
fn graph_writer_node_cache_tracks_hits() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("writer_cache.db");
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
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;
    pager.commit(write)?;

    let opts = CreateEdgeOptions {
        trusted_endpoints: false,
        exists_cache_capacity: 8,
    };
    let mut writer = GraphWriter::try_new(&graph, opts, None)?;
    let spec = EdgeSpec {
        src,
        dst,
        ty: TypeId(1),
        props: &[],
    };

    let mut write = pager.begin_write()?;
    writer.create_edge(&mut write, spec.clone())?;
    writer.create_edge(&mut write, spec)?;
    pager.commit(write)?;
    let stats = writer.stats();
    assert_eq!(stats.exists_cache_misses, 2);
    assert_eq!(stats.exists_cache_hits, 2);
    Ok(())
}

struct NoopValidator;

impl BulkEdgeValidator for NoopValidator {
    fn validate_batch(&self, _: &[(sombra::types::NodeId, sombra::types::NodeId)]) -> Result<()> {
        Ok(())
    }
}
