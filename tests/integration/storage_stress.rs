#![allow(missing_docs)]
#![allow(clippy::arc_with_non_send_sync, clippy::field_reassign_with_default)]

use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use sombra::primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions};
use sombra::storage::{
    Dir, EdgeSpec, ExpandOpts, Graph, GraphOptions, NodeSpec, PropEntry, PropPatch, PropPatchOp,
    PropValue,
};
use sombra::types::{EdgeId, LabelId, NodeId, PropId, Result, TypeId};
use tempfile::tempdir;

const NODE_COUNT: usize = 256;
const EDGE_COUNT: usize = 1_024;
const PATCHES: usize = 128;
const SEED: u64 = 0x5eed_cafe;

#[test]
fn randomized_graph_stress_and_recovery() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("stress.db");

    let pager = Arc::new(Pager::create(&path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let graph = Graph::open(GraphOptions::new(store))?;

    let mut rng = ChaCha8Rng::seed_from_u64(SEED);
    let mut nodes = Vec::with_capacity(NODE_COUNT);
    let mut edges = HashSet::new();
    let mut edge_specs: HashMap<EdgeId, (NodeId, NodeId, TypeId)> = HashMap::new();

    {
        let mut write = pager.begin_write()?;
        for _ in 0..NODE_COUNT {
            let node = graph.create_node(
                &mut write,
                NodeSpec {
                    labels: &[LabelId(rng.gen_range(0..16))],
                    props: &[PropEntry::new(
                        PropId(1),
                        PropValue::Int(rng.gen_range(0..1_000)),
                    )],
                },
            )?;
            nodes.push(node);
        }
        for _ in 0..EDGE_COUNT {
            let src = nodes[rng.gen_range(0..nodes.len())];
            let dst = nodes[rng.gen_range(0..nodes.len())];
            let ty = TypeId(rng.gen_range(0..32));
            let edge = graph.create_edge(
                &mut write,
                EdgeSpec {
                    src,
                    dst,
                    ty,
                    props: &[PropEntry::new(
                        PropId(2),
                        PropValue::Int(rng.gen_range(0..1_000)),
                    )],
                },
            )?;
            edges.insert(edge);
            edge_specs.insert(edge, (src, dst, ty));
        }
        pager.commit(write)?;
    }

    for _ in 0..PATCHES {
        if rng.gen_bool(0.5) {
            let id = nodes[rng.gen_range(0..nodes.len())];
            let mut write = pager.begin_write()?;
            graph.update_node(
                &mut write,
                id,
                PropPatch::new(vec![
                    PropPatchOp::Set(PropId(3), PropValue::Int(rng.gen_range(0..10_000))),
                    PropPatchOp::Delete(PropId(1)),
                ]),
            )?;
            pager.commit(write)?;
        } else if let Some(edge) = edges.iter().copied().choose(&mut rng) {
            let mut write = pager.begin_write()?;
            graph.update_edge(
                &mut write,
                edge,
                PropPatch::new(vec![PropPatchOp::Set(
                    PropId(2),
                    PropValue::Int(rng.gen_range(0..5_000)),
                )]),
            )?;
            pager.commit(write)?;
        }
    }

    pager.checkpoint(CheckpointMode::Force)?;

    {
        let read = pager.begin_read()?;
        for &node in &nodes {
            let out_neighbors: Vec<_> = graph
                .neighbors(&read, node, Dir::Out, None, ExpandOpts::default())?
                .collect();
            let in_neighbors: Vec<_> = graph
                .neighbors(&read, node, Dir::In, None, ExpandOpts::default())?
                .collect();

            let out_degree = graph.degree(&read, node, Dir::Out, None)?;
            let in_degree = graph.degree(&read, node, Dir::In, None)?;
            assert_eq!(out_neighbors.len() as u64, out_degree);
            assert_eq!(in_neighbors.len() as u64, in_degree);
        }

        let mut specs: Vec<_> = edge_specs.iter().collect();
        specs.sort_unstable_by_key(|(edge_id, _)| edge_id.0);
        for (edge_id, (src, dst, ty)) in specs {
            let out_neighbors: Vec<_> = graph
                .neighbors(&read, *src, Dir::Out, Some(*ty), ExpandOpts::default())?
                .collect();
            assert!(
                out_neighbors.iter().any(|n| n.neighbor == *dst),
                "Edge {:?} missing from out-adjacency of {:?} (expected dst {:?}, ty {:?}); entries: {:?}",
                edge_id,
                src,
                dst,
                ty,
                graph
                    .debug_collect_adj_fwd(&read)?
                    .into_iter()
                    .filter(|(_, _, _, edge)| edge == edge_id)
                    .collect::<Vec<_>>()
            );
            let in_neighbors: Vec<_> = graph
                .neighbors(&read, *dst, Dir::In, Some(*ty), ExpandOpts::default())?
                .collect();
            assert!(
                in_neighbors.iter().any(|n| n.neighbor == *src),
                "Edge {:?} missing from in-adjacency of {:?} (expected src {:?}, ty {:?}); entries: {:?}",
                edge_id,
                dst,
                src,
                ty,
                graph
                    .debug_collect_adj_rev(&read)?
                    .into_iter()
                    .filter(|(_, _, _, edge)| edge == edge_id)
                    .collect::<Vec<_>>()
            );
        }
    }

    Ok(())
}

/// Regression test for cumulative write buffer exhaustion bug.
///
/// With a small page cache and disabled autocheckpoint, repeated writes would
/// eventually fail with "no eviction candidate available" when the accumulated
/// WAL data exceeded a threshold (~35KB in the original report).
///
/// The root cause was that without checkpointing, overlays and version chains
/// accumulated indefinitely, and certain B-tree split patterns could temporarily
/// pin multiple frames during a single operation.
#[test]
fn small_cache_cumulative_writes_no_checkpoint() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("small_cache.db");

    // Create pager with small cache and disabled autocheckpoint.
    // 8 pages @ 8KB = 64KB cache, which should exercise the eviction logic
    // while still being large enough for basic B-tree operations.
    let options = PagerOptions {
        page_size: 8192,
        cache_pages: 8,
        autocheckpoint_pages: usize::MAX,
        ..PagerOptions::default()
    };
    let pager = Arc::new(Pager::create(&path, options)?);
    let store: Arc<dyn PageStore> = pager.clone();
    let graph = Graph::open(GraphOptions::new(store))?;

    // Write all nodes in a SINGLE transaction
    let target_count = 512u64;
    let mut write = pager.begin_write()?;
    for i in 0..target_count {
        let _node = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(1)],
                props: &[
                    PropEntry::new(PropId(1), PropValue::Int(i as i64)),
                    PropEntry::new(PropId(2), PropValue::Str(&format!("node_{i}"))),
                ],
            },
        )?;
    }
    pager.commit(write)?;

    // Verify we can still read back - just do a quick sanity check.
    let read = pager.begin_read()?;
    
    // First check: directly get each node by ID
    let mut found_by_id = 0u64;
    for i in 1..=target_count {
        if graph.get_node(&read, NodeId(i))?.is_some() {
            found_by_id += 1;
        }
    }
    assert_eq!(found_by_id, target_count, "get_node should find all nodes");
    
    // Second check: scan_all_nodes should return all nodes
    let all_scanned = graph.scan_all_nodes(&read)?;
    assert_eq!(
        all_scanned.len() as u64, target_count,
        "scan_all_nodes should return all nodes"
    );
    
    // Third check: nodes_with_label should return all nodes (they all have LabelId(1))
    let label_nodes = graph.nodes_with_label(&read, LabelId(1))?;
    assert_eq!(
        label_nodes.len() as u64, target_count,
        "nodes_with_label should return all nodes with label 1"
    );
    
    // Fourth check: count_nodes_with_label should match
    let label_count = graph.count_nodes_with_label(&read, LabelId(1))?;
    assert_eq!(
        label_count, target_count,
        "count_nodes_with_label should return the correct count"
    );

    // Drop the read guard before checkpoint (checkpoint waits for readers)
    drop(read);
    
    // Checkpoint should still work after all these writes.
    pager.checkpoint(CheckpointMode::Force)?;

    Ok(())
}
