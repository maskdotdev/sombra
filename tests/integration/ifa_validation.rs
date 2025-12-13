//! IFA (Index-Free Adjacency) Validation Tests
//!
//! These tests validate the IFA implementation against the requirements from
//! docs/build/mvcc/index-free-adjacency.md, covering:
//!
//! - Overflow chain handling (>5 edge types per node)
//! - High-degree node behavior (extent chaining boundary)
//! - Dual-mode validation (IFA vs B-tree consistency)
//! - MVCC visibility with concurrent snapshots
//! - Segment version chain correctness

#![allow(missing_docs)]
#![allow(clippy::arc_with_non_send_sync, clippy::field_reassign_with_default)]

use std::collections::HashSet;
use std::sync::Arc;

use sombra::primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions};
use sombra::storage::{
    AdjacencyBackend, Dir, EdgeSpec, ExpandOpts, Graph, GraphOptions, NodeSpec, PropEntry, PropValue,
};
use sombra::types::{EdgeId, LabelId, NodeId, PropId, Result, TypeId};
use tempfile::tempdir;

fn setup_graph(path: &std::path::Path) -> Result<(Arc<Pager>, Arc<Graph>)> {
    let pager = Arc::new(Pager::create(path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let graph = Graph::open(GraphOptions::new(store))?;
    Ok((pager, graph))
}

fn setup_graph_with_backend(path: &std::path::Path, backend: AdjacencyBackend) -> Result<(Arc<Pager>, Arc<Graph>)> {
    let pager = Arc::new(Pager::create(path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let options = GraphOptions::new(store).adjacency_backend(backend);
    let graph = Graph::open(options)?;
    Ok((pager, graph))
}

// =============================================================================
// Phase 1: Overflow Chain Tests (>5 edge types triggers overflow)
// =============================================================================

/// Tests that a node with exactly K-1 (5) edge types stays inline.
#[test]
fn overflow_not_triggered_at_k_minus_1_types() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("inline_types.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;

    // Create 5 different edge types (K-1 = 5 should fit inline)
    for i in 1..=5 {
        let dst = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[],
            },
        )?;
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty: TypeId(i),
                props: &[],
            },
        )?;
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    // Verify all edges are queryable
    let read = pager.begin_read()?;
    let total_degree = graph.degree(&read, src, Dir::Out, None)?;
    assert_eq!(total_degree, 5, "Should have 5 outgoing edges");

    // Verify each type is accessible
    for i in 1..=5 {
        let degree = graph.degree(&read, src, Dir::Out, Some(TypeId(i)))?;
        assert_eq!(degree, 1, "Type {} should have 1 edge", i);
    }

    Ok(())
}

/// Tests that a node with >K-1 (>5) edge types triggers overflow handling.
#[test]
fn overflow_triggered_with_many_types() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("overflow_types.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;

    // Create 10 different edge types (triggers overflow at type 6)
    let type_count = 10;
    for i in 1..=type_count {
        let dst = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[],
            },
        )?;
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty: TypeId(i),
                props: &[],
            },
        )?;
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    // Verify all edges are queryable even with overflow
    let read = pager.begin_read()?;
    let total_degree = graph.degree(&read, src, Dir::Out, None)?;
    assert_eq!(total_degree, type_count as u64, "Should have {} outgoing edges", type_count);

    // Verify each type is accessible (including overflow types)
    for i in 1..=type_count {
        let degree = graph.degree(&read, src, Dir::Out, Some(TypeId(i)))?;
        assert_eq!(degree, 1, "Type {} should have 1 edge", i);
    }

    // Verify neighbors() returns all edges
    let neighbors: Vec<_> = graph
        .neighbors(&read, src, Dir::Out, None, ExpandOpts::default())?
        .collect();
    assert_eq!(neighbors.len(), type_count as usize);

    Ok(())
}

/// Tests overflow with many edges per type (combined inline + overflow stress).
#[test]
fn overflow_with_multiple_edges_per_type() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("overflow_multi.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;

    // Create 8 types with 5 edges each = 40 total edges
    let type_count = 8;
    let edges_per_type = 5;
    let mut expected_neighbors: Vec<(TypeId, NodeId)> = Vec::new();

    for type_idx in 1..=type_count {
        for _edge_idx in 0..edges_per_type {
            let dst = graph.create_node(
                &mut write,
                NodeSpec {
                    labels: &[LabelId(2)],
                    props: &[],
                },
            )?;
            graph.create_edge(
                &mut write,
                EdgeSpec {
                    src,
                    dst,
                    ty: TypeId(type_idx),
                    props: &[],
                },
            )?;
            expected_neighbors.push((TypeId(type_idx), dst));
        }
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    
    // Verify total degree
    let total = graph.degree(&read, src, Dir::Out, None)?;
    assert_eq!(total, (type_count * edges_per_type) as u64);

    // Verify per-type degree
    for type_idx in 1..=type_count {
        let degree = graph.degree(&read, src, Dir::Out, Some(TypeId(type_idx)))?;
        assert_eq!(degree, edges_per_type as u64, "Type {} should have {} edges", type_idx, edges_per_type);
    }

    // Verify neighbors
    let neighbors: Vec<_> = graph
        .neighbors(&read, src, Dir::Out, None, ExpandOpts::default())?
        .collect();
    assert_eq!(neighbors.len(), expected_neighbors.len());

    Ok(())
}

// =============================================================================
// Phase 2: High-Degree Node Tests (Extent Chaining Boundary)
// =============================================================================

/// Tests behavior near the extent chaining boundary.
/// With 8KB pages and 32-byte entries, max ~250 entries per segment.
#[test]
fn high_degree_node_within_single_segment() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("high_degree.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;

    // Create many edges of the same type (should fit in one segment)
    // Using 100 edges which is well under the ~250 limit
    let edge_count = 100;
    let ty = TypeId(1);
    let mut destinations = Vec::new();

    for _ in 0..edge_count {
        let dst = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[],
            },
        )?;
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty,
                props: &[],
            },
        )?;
        destinations.push(dst);
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    
    // Verify degree
    let degree = graph.degree(&read, src, Dir::Out, Some(ty))?;
    assert_eq!(degree, edge_count as u64);

    // Verify all neighbors are returned
    let neighbors: Vec<_> = graph
        .neighbors(&read, src, Dir::Out, Some(ty), ExpandOpts::default())?
        .collect();
    assert_eq!(neighbors.len(), edge_count);

    // Verify each destination is reachable
    let neighbor_set: HashSet<NodeId> = neighbors.iter().map(|n| n.neighbor).collect();
    for dst in destinations {
        assert!(neighbor_set.contains(&dst), "Missing neighbor {:?}", dst);
    }

    Ok(())
}

/// Tests that approaching extent chaining limit doesn't corrupt data.
#[test]
fn high_degree_node_near_segment_limit() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("near_limit.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;

    // Create edges up to 200 (approaching but not exceeding typical limit)
    let edge_count = 200;
    let ty = TypeId(1);

    for _ in 0..edge_count {
        let dst = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[],
            },
        )?;
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty,
                props: &[],
            },
        )?;
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let degree = graph.degree(&read, src, Dir::Out, Some(ty))?;
    assert_eq!(degree, edge_count as u64);

    Ok(())
}

/// Tests extent chaining with exactly 254 edges (max for single primary page).
/// With 8KB pages, 50-byte header, 32-byte entries: (8192-50)/32 = 254 max.
#[test]
fn high_degree_node_at_exact_segment_limit() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("exact_limit.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;

    // Create exactly 254 edges (should fit in one page without extent chaining)
    let edge_count = 254;
    let ty = TypeId(1);
    let mut destinations = Vec::new();

    for _ in 0..edge_count {
        let dst = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[],
            },
        )?;
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty,
                props: &[],
            },
        )?;
        destinations.push(dst);
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    
    // Verify degree
    let degree = graph.degree(&read, src, Dir::Out, Some(ty))?;
    assert_eq!(degree, edge_count as u64, "Should have exactly 254 edges");

    // Verify all neighbors are returned
    let neighbors: Vec<_> = graph
        .neighbors(&read, src, Dir::Out, Some(ty), ExpandOpts::default())?
        .collect();
    assert_eq!(neighbors.len(), edge_count, "Should return 254 neighbors");

    // Verify each destination is reachable
    let neighbor_set: HashSet<NodeId> = neighbors.iter().map(|n| n.neighbor).collect();
    for dst in destinations {
        assert!(neighbor_set.contains(&dst), "Missing neighbor {:?}", dst);
    }

    Ok(())
}

/// Tests extent chaining with 300 edges (exceeds 254, triggers extent chaining).
#[test]
fn high_degree_node_triggers_extent_chaining() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("extent_chaining.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;

    // Create 300 edges (254 on primary page + 46 on first extent page)
    let edge_count = 300;
    let ty = TypeId(1);
    let mut destinations = Vec::new();

    for _ in 0..edge_count {
        let dst = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[],
            },
        )?;
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty,
                props: &[],
            },
        )?;
        destinations.push(dst);
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    
    // Verify degree includes all entries across primary + extent pages
    let degree = graph.degree(&read, src, Dir::Out, Some(ty))?;
    assert_eq!(degree, edge_count as u64, "Should have 300 edges spanning extent pages");

    // Verify all neighbors are returned
    let neighbors: Vec<_> = graph
        .neighbors(&read, src, Dir::Out, Some(ty), ExpandOpts::default())?
        .collect();
    assert_eq!(neighbors.len(), edge_count, "Should return all 300 neighbors");

    // Verify each destination is reachable
    let neighbor_set: HashSet<NodeId> = neighbors.iter().map(|n| n.neighbor).collect();
    for dst in &destinations {
        assert!(neighbor_set.contains(dst), "Missing neighbor {:?}", dst);
    }

    // Verify reverse adjacency is also correct (each dest should have IN edge to src)
    for dst in &destinations {
        let in_neighbors: Vec<_> = graph
            .neighbors(&read, *dst, Dir::In, Some(ty), ExpandOpts::default())?
            .collect();
        let has_reverse = in_neighbors.iter().any(|n| n.neighbor == src);
        assert!(has_reverse, "Missing reverse adjacency for dst {:?}", dst);
    }

    Ok(())
}

/// Tests extent chaining with 600 edges (requires multiple extent pages).
/// 254 on primary + 255 on extent 1 + 91 on extent 2 = 600 total.
#[test]
fn high_degree_node_multiple_extent_pages() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("multi_extent.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;

    // Create 600 edges (spans primary + 2 extent pages)
    let edge_count = 600;
    let ty = TypeId(1);
    let mut destinations = Vec::new();

    for _ in 0..edge_count {
        let dst = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[],
            },
        )?;
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty,
                props: &[],
            },
        )?;
        destinations.push(dst);
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    
    // Verify degree
    let degree = graph.degree(&read, src, Dir::Out, Some(ty))?;
    assert_eq!(degree, edge_count as u64, "Should have 600 edges");

    // Verify all neighbors
    let neighbors: Vec<_> = graph
        .neighbors(&read, src, Dir::Out, Some(ty), ExpandOpts::default())?
        .collect();
    assert_eq!(neighbors.len(), edge_count, "Should return all 600 neighbors");

    // Verify no duplicates
    let neighbor_set: HashSet<NodeId> = neighbors.iter().map(|n| n.neighbor).collect();
    assert_eq!(neighbor_set.len(), edge_count, "No duplicate neighbors");

    Ok(())
}

/// Tests combined scenario: many edge types (triggers overflow) + high degree per type (triggers extent chaining).
#[test]
fn overflow_combined_with_extent_chaining() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("overflow_extent.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;

    // Create 10 different edge types (triggers overflow at type 6)
    // Each type has 300 edges (triggers extent chaining)
    let type_count = 10;
    let edges_per_type = 300;
    let mut expected: Vec<(TypeId, Vec<NodeId>)> = Vec::new();

    for type_idx in 1..=type_count {
        let ty = TypeId(type_idx);
        let mut type_dests = Vec::new();
        
        for _ in 0..edges_per_type {
            let dst = graph.create_node(
                &mut write,
                NodeSpec {
                    labels: &[LabelId(2)],
                    props: &[],
                },
            )?;
            graph.create_edge(
                &mut write,
                EdgeSpec {
                    src,
                    dst,
                    ty,
                    props: &[],
                },
            )?;
            type_dests.push(dst);
        }
        expected.push((ty, type_dests));
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    
    // Verify total degree
    let total_degree = graph.degree(&read, src, Dir::Out, None)?;
    assert_eq!(
        total_degree,
        (type_count * edges_per_type) as u64,
        "Should have {} total edges",
        type_count * edges_per_type
    );

    // Verify per-type degree
    for (ty, type_dests) in &expected {
        let degree = graph.degree(&read, src, Dir::Out, Some(*ty))?;
        assert_eq!(
            degree, edges_per_type as u64,
            "Type {:?} should have {} edges",
            ty, edges_per_type
        );

        // Verify neighbors for this type
        let neighbors: Vec<_> = graph
            .neighbors(&read, src, Dir::Out, Some(*ty), ExpandOpts::default())?
            .collect();
        assert_eq!(
            neighbors.len(), edges_per_type as usize,
            "Type {:?} should return {} neighbors",
            ty, edges_per_type
        );

        // Verify all expected destinations are present
        let neighbor_set: HashSet<NodeId> = neighbors.iter().map(|n| n.neighbor).collect();
        for dst in type_dests {
            assert!(neighbor_set.contains(dst), "Missing neighbor {:?} for type {:?}", dst, ty);
        }
    }

    Ok(())
}

// =============================================================================
// Phase 3: Dual-Mode Validation (IFA vs B-tree Consistency)
// =============================================================================

/// Validates that adjacency queries return consistent results.
/// This test creates a complex graph and verifies forward/reverse consistency.
#[test]
fn dual_mode_fwd_rev_consistency() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("dual_mode.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    
    // Create a star topology: central node with many connections
    let center = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;

    let mut edges_out: Vec<(NodeId, TypeId, EdgeId)> = Vec::new();
    let mut edges_in: Vec<(NodeId, TypeId, EdgeId)> = Vec::new();

    // Create outgoing edges from center
    for i in 0..20 {
        let dst = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[],
            },
        )?;
        let ty = TypeId((i % 5) + 1); // 5 different types
        let edge = graph.create_edge(
            &mut write,
            EdgeSpec {
                src: center,
                dst,
                ty,
                props: &[],
            },
        )?;
        edges_out.push((dst, ty, edge));
    }

    // Create incoming edges to center
    for i in 0..15 {
        let src = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(3)],
                props: &[],
            },
        )?;
        let ty = TypeId((i % 3) + 10); // 3 different types
        let edge = graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst: center,
                ty,
                props: &[],
            },
        )?;
        edges_in.push((src, ty, edge));
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;

    // Validate outgoing edges
    let out_neighbors: Vec<_> = graph
        .neighbors(&read, center, Dir::Out, None, ExpandOpts::default())?
        .collect();
    assert_eq!(out_neighbors.len(), edges_out.len());

    for (expected_dst, expected_ty, expected_edge) in &edges_out {
        let found = out_neighbors.iter().any(|n| {
            n.neighbor == *expected_dst && n.ty == *expected_ty && n.edge == *expected_edge
        });
        assert!(found, "Missing outgoing edge {:?}", expected_edge);
    }

    // Validate incoming edges
    let in_neighbors: Vec<_> = graph
        .neighbors(&read, center, Dir::In, None, ExpandOpts::default())?
        .collect();
    assert_eq!(in_neighbors.len(), edges_in.len());

    for (expected_src, expected_ty, expected_edge) in &edges_in {
        let found = in_neighbors.iter().any(|n| {
            n.neighbor == *expected_src && n.ty == *expected_ty && n.edge == *expected_edge
        });
        assert!(found, "Missing incoming edge {:?}", expected_edge);
    }

    // Cross-validate: for each outgoing edge, verify the reverse entry exists
    for (dst, ty, edge) in &edges_out {
        let reverse: Vec<_> = graph
            .neighbors(&read, *dst, Dir::In, Some(*ty), ExpandOpts::default())?
            .collect();
        let found = reverse.iter().any(|n| n.neighbor == center && n.edge == *edge);
        assert!(found, "Reverse entry missing for edge {:?}", edge);
    }

    // Cross-validate: for each incoming edge, verify the forward entry exists
    for (src, ty, edge) in &edges_in {
        let forward: Vec<_> = graph
            .neighbors(&read, *src, Dir::Out, Some(*ty), ExpandOpts::default())?
            .collect();
        let found = forward.iter().any(|n| n.neighbor == center && n.edge == *edge);
        assert!(found, "Forward entry missing for edge {:?}", edge);
    }

    Ok(())
}

/// Validates that degree counts match actual neighbor counts.
#[test]
fn dual_mode_degree_vs_neighbors_count() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("degree_match.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;

    // Create edges with various types
    for i in 0..30 {
        let dst = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[],
            },
        )?;
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty: TypeId((i % 7) + 1),
                props: &[],
            },
        )?;
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;

    // Total degree should match neighbors count
    let degree_total = graph.degree(&read, src, Dir::Out, None)?;
    let neighbors: Vec<_> = graph
        .neighbors(&read, src, Dir::Out, None, ExpandOpts::default())?
        .collect();
    assert_eq!(degree_total, neighbors.len() as u64, "Total degree mismatch");

    // Per-type degree should match filtered neighbors count
    for ty_idx in 1..=7 {
        let ty = TypeId(ty_idx);
        let degree_typed = graph.degree(&read, src, Dir::Out, Some(ty))?;
        let neighbors_typed: Vec<_> = graph
            .neighbors(&read, src, Dir::Out, Some(ty), ExpandOpts::default())?
            .collect();
        assert_eq!(
            degree_typed,
            neighbors_typed.len() as u64,
            "Type {} degree mismatch",
            ty_idx
        );
    }

    Ok(())
}

// =============================================================================
// Phase 3: MVCC Visibility Tests
// =============================================================================

/// Tests that old snapshots don't see newly inserted edges.
#[test]
fn mvcc_snapshot_isolation_insert() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("mvcc_insert.db");
    let (pager, graph) = setup_graph(&path)?;

    // Create initial graph
    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;
    let dst1 = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(2)],
            props: &[],
        },
    )?;
    graph.create_edge(
        &mut write,
        EdgeSpec {
            src,
            dst: dst1,
            ty: TypeId(1),
            props: &[],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    // Take snapshot before insert
    let snapshot_before = pager.begin_latest_committed_read()?;
    let degree_before = graph.degree(&snapshot_before, src, Dir::Out, None)?;
    assert_eq!(degree_before, 1);

    // Insert new edge
    let mut write = pager.begin_write()?;
    let dst2 = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(2)],
            props: &[],
        },
    )?;
    graph.create_edge(
        &mut write,
        EdgeSpec {
            src,
            dst: dst2,
            ty: TypeId(1),
            props: &[],
        },
    )?;
    pager.commit(write)?;

    // Old snapshot should still see 1 edge
    let degree_old = graph.degree(&snapshot_before, src, Dir::Out, None)?;
    assert_eq!(degree_old, 1, "Old snapshot should not see new edge");
    drop(snapshot_before);

    // New snapshot should see 2 edges
    let snapshot_after = pager.begin_latest_committed_read()?;
    let degree_after = graph.degree(&snapshot_after, src, Dir::Out, None)?;
    assert_eq!(degree_after, 2, "New snapshot should see both edges");

    Ok(())
}

/// Tests that old snapshots still see deleted edges.
#[test]
fn mvcc_snapshot_isolation_delete() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("mvcc_delete.db");
    let (pager, graph) = setup_graph(&path)?;

    // Create initial graph with 2 edges
    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;
    let dst1 = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(2)],
            props: &[],
        },
    )?;
    let dst2 = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(2)],
            props: &[],
        },
    )?;
    let edge1 = graph.create_edge(
        &mut write,
        EdgeSpec {
            src,
            dst: dst1,
            ty: TypeId(1),
            props: &[],
        },
    )?;
    graph.create_edge(
        &mut write,
        EdgeSpec {
            src,
            dst: dst2,
            ty: TypeId(1),
            props: &[],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    // Take snapshot before delete
    let snapshot_before = pager.begin_latest_committed_read()?;
    let degree_before = graph.degree(&snapshot_before, src, Dir::Out, None)?;
    assert_eq!(degree_before, 2);

    // Delete one edge
    let mut write = pager.begin_write()?;
    graph.delete_edge(&mut write, edge1)?;
    pager.commit(write)?;

    // Old snapshot should still see 2 edges
    let degree_old = graph.degree(&snapshot_before, src, Dir::Out, None)?;
    assert_eq!(degree_old, 2, "Old snapshot should still see deleted edge");
    drop(snapshot_before);

    // New snapshot should see 1 edge
    let snapshot_after = pager.begin_latest_committed_read()?;
    let degree_after = graph.degree(&snapshot_after, src, Dir::Out, None)?;
    assert_eq!(degree_after, 1, "New snapshot should not see deleted edge");

    Ok(())
}

/// Tests MVCC with multiple sequential modifications.
/// Verifies that the final state reflects all committed changes.
#[test]
fn mvcc_version_chain_traversal() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("mvcc_chain.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;
    pager.commit(write)?;

    // Add edges one by one, creating version chain
    let edge_count = 5;
    for _ in 0..edge_count {
        let mut write = pager.begin_write()?;
        let dst = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[],
            },
        )?;
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty: TypeId(1),
                props: &[],
            },
        )?;
        pager.commit(write)?;
    }
    pager.checkpoint(CheckpointMode::Force)?;

    // Final snapshot should see all edges
    let read = pager.begin_latest_committed_read()?;
    let degree = graph.degree(&read, src, Dir::Out, None)?;
    assert_eq!(
        degree, edge_count as u64,
        "Should see all {} edges after version chain",
        edge_count
    );

    Ok(())
}

// =============================================================================
// Phase 4: Segment GC Readiness Tests
// =============================================================================

/// Tests that multiple CoW versions are created correctly.
/// This is a prerequisite for GC - we need to verify version chains exist.
#[test]
fn cow_creates_version_chain() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("cow_chain.db");
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
    
    // Create first edge
    let edge1 = graph.create_edge(
        &mut write,
        EdgeSpec {
            src,
            dst,
            ty: TypeId(1),
            props: &[PropEntry::new(PropId(1), PropValue::Int(1))],
        },
    )?;
    pager.commit(write)?;

    // Modify the same adjacency multiple times (creates CoW versions)
    for i in 2..=5 {
        let mut write = pager.begin_write()?;
        let new_dst = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[],
            },
        )?;
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst: new_dst,
                ty: TypeId(1), // Same type = same segment gets CoW'd
                props: &[PropEntry::new(PropId(1), PropValue::Int(i))],
            },
        )?;
        pager.commit(write)?;
    }
    pager.checkpoint(CheckpointMode::Force)?;

    // Verify final state
    let read = pager.begin_read()?;
    let degree = graph.degree(&read, src, Dir::Out, Some(TypeId(1)))?;
    assert_eq!(degree, 5, "Should have 5 edges after 5 inserts");

    // Verify the first edge still exists
    assert!(graph.get_edge(&read, edge1)?.is_some());

    Ok(())
}

/// Tests that deleting edges creates proper version history.
#[test]
fn delete_creates_tombstone_versions() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("tombstone.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;

    // Create 5 edges
    let mut edges = Vec::new();
    for _ in 0..5 {
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
                ty: TypeId(1),
                props: &[],
            },
        )?;
        edges.push(edge);
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    // Delete edges one by one
    for edge in &edges {
        let snapshot_before = pager.begin_latest_committed_read()?;
        let degree_before = graph.degree(&snapshot_before, src, Dir::Out, None)?;

        let mut write = pager.begin_write()?;
        graph.delete_edge(&mut write, *edge)?;
        pager.commit(write)?;

        // Old snapshot should see one more edge than new snapshot
        let snapshot_after = pager.begin_latest_committed_read()?;
        let degree_after = graph.degree(&snapshot_after, src, Dir::Out, None)?;
        
        assert_eq!(
            degree_before,
            degree_after + 1,
            "Delete should reduce degree by 1"
        );
        
        drop(snapshot_before);
    }

    // Final state should have 0 edges
    let read = pager.begin_read()?;
    let final_degree = graph.degree(&read, src, Dir::Out, None)?;
    assert_eq!(final_degree, 0, "All edges should be deleted");

    Ok(())
}

// =============================================================================
// Stress Tests
// =============================================================================

/// Randomized stress test for IFA with many nodes and edges.
#[test]
fn ifa_randomized_stress() -> Result<()> {
    use rand::prelude::*;
    use rand_chacha::ChaCha8Rng;

    let dir = tempdir()?;
    let path = dir.path().join("ifa_stress.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut rng = ChaCha8Rng::seed_from_u64(0x1FA_5EED);
    
    let node_count = 100;
    let edge_count = 500;
    let type_count = 12; // More than K-1 to trigger overflow

    let mut write = pager.begin_write()?;
    let mut nodes = Vec::with_capacity(node_count);
    
    for _ in 0..node_count {
        let node = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(rng.gen_range(1..5))],
                props: &[],
            },
        )?;
        nodes.push(node);
    }

    let mut edge_specs: Vec<(NodeId, NodeId, TypeId, EdgeId)> = Vec::new();
    for _ in 0..edge_count {
        let src = nodes[rng.gen_range(0..nodes.len())];
        let dst = nodes[rng.gen_range(0..nodes.len())];
        let ty = TypeId(rng.gen_range(1..=type_count));
        let edge = graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty,
                props: &[],
            },
        )?;
        edge_specs.push((src, dst, ty, edge));
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    // Validate all edges
    let read = pager.begin_read()?;
    for (src, dst, ty, edge) in &edge_specs {
        // Check forward adjacency
        let out_neighbors: Vec<_> = graph
            .neighbors(&read, *src, Dir::Out, Some(*ty), ExpandOpts::default())?
            .collect();
        let found_fwd = out_neighbors.iter().any(|n| n.neighbor == *dst && n.edge == *edge);
        assert!(found_fwd, "Missing forward entry for edge {:?}", edge);

        // Check reverse adjacency
        let in_neighbors: Vec<_> = graph
            .neighbors(&read, *dst, Dir::In, Some(*ty), ExpandOpts::default())?
            .collect();
        let found_rev = in_neighbors.iter().any(|n| n.neighbor == *src && n.edge == *edge);
        assert!(found_rev, "Missing reverse entry for edge {:?}", edge);
    }

    Ok(())
}

/// Tests concurrent read operations (SWMR - Single Writer Multiple Reader).
#[test]
fn swmr_concurrent_reads() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("swmr.db");
    let (pager, graph) = setup_graph(&path)?;

    // Create test data
    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;
    for i in 0..50 {
        let dst = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[],
            },
        )?;
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty: TypeId((i % 8) + 1),
                props: &[],
            },
        )?;
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    // Create multiple concurrent readers
    let readers: Vec<_> = (0..5)
        .map(|_| pager.begin_read())
        .collect::<Result<Vec<_>>>()?;

    // All readers should see the same data
    for reader in &readers {
        let degree = graph.degree(reader, src, Dir::Out, None)?;
        assert_eq!(degree, 50, "All readers should see 50 edges");
    }

    // Verify neighbors are consistent across readers
    let expected_neighbors: Vec<_> = graph
        .neighbors(&readers[0], src, Dir::Out, None, ExpandOpts::default())?
        .map(|n| n.neighbor)
        .collect();

    for reader in &readers[1..] {
        let neighbors: Vec<_> = graph
            .neighbors(reader, src, Dir::Out, None, ExpandOpts::default())?
            .map(|n| n.neighbor)
            .collect();
        assert_eq!(neighbors.len(), expected_neighbors.len());
    }

    Ok(())
}

// =============================================================================
// Phase 8: True IFA Mode Tests (IfaOnly backend with hybrid overflow)
// =============================================================================

/// Tests that IfaOnly mode correctly handles nodes with exactly 5 edge types (inline only).
#[test]
fn true_ifa_inline_types_only() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("true_ifa_inline.db");
    let (pager, graph) = setup_graph_with_backend(&path, AdjacencyBackend::IfaOnly)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;

    // Create exactly 5 edge types (fits in inline buckets)
    let inline_types = 5;
    let mut expected: Vec<(TypeId, NodeId)> = Vec::new();
    
    for i in 1..=inline_types {
        let dst = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[],
            },
        )?;
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty: TypeId(i),
                props: &[],
            },
        )?;
        expected.push((TypeId(i), dst));
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    // Verify all edges are queryable in IfaOnly mode
    let read = pager.begin_read()?;
    let total_degree = graph.degree(&read, src, Dir::Out, None)?;
    assert_eq!(total_degree, inline_types as u64, "Should have {} outgoing edges", inline_types);

    // Verify each type is accessible
    for i in 1..=inline_types {
        let degree = graph.degree(&read, src, Dir::Out, Some(TypeId(i)))?;
        assert_eq!(degree, 1, "Type {} should have 1 edge", i);
    }

    // Verify neighbors returns all edges
    let neighbors: Vec<_> = graph
        .neighbors(&read, src, Dir::Out, None, ExpandOpts::default())?
        .collect();
    assert_eq!(neighbors.len(), inline_types as usize);

    Ok(())
}

/// Tests that IfaOnly mode correctly handles nodes with >5 edge types (overflow required).
/// This validates the hybrid overflow approach where overflow types use B-tree lookup.
#[test]
fn true_ifa_overflow_types() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("true_ifa_overflow.db");
    let (pager, graph) = setup_graph_with_backend(&path, AdjacencyBackend::IfaOnly)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;

    // Create 10 edge types (5 inline + 5 overflow)
    let total_types = 10;
    let mut expected: Vec<(TypeId, NodeId)> = Vec::new();
    
    for i in 1..=total_types {
        let dst = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[],
            },
        )?;
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty: TypeId(i),
                props: &[],
            },
        )?;
        expected.push((TypeId(i), dst));
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    // Verify all edges are queryable in IfaOnly mode (including overflow types)
    let read = pager.begin_read()?;
    let total_degree = graph.degree(&read, src, Dir::Out, None)?;
    assert_eq!(total_degree, total_types as u64, "Should have {} outgoing edges", total_types);

    // Verify inline types (1-5) are accessible
    for i in 1..=5 {
        let degree = graph.degree(&read, src, Dir::Out, Some(TypeId(i)))?;
        assert_eq!(degree, 1, "Inline type {} should have 1 edge", i);
    }

    // Verify overflow types (6-10) are accessible via hybrid B-tree lookup
    for i in 6..=total_types {
        let degree = graph.degree(&read, src, Dir::Out, Some(TypeId(i)))?;
        assert_eq!(degree, 1, "Overflow type {} should have 1 edge", i);
    }

    // Verify neighbors returns all edges (inline + overflow)
    let neighbors: Vec<_> = graph
        .neighbors(&read, src, Dir::Out, None, ExpandOpts::default())?
        .collect();
    assert_eq!(neighbors.len(), total_types as usize);

    // Verify we got edges of all expected types
    let returned_types: HashSet<u32> = neighbors.iter().map(|n| n.ty.0).collect();
    for i in 1..=total_types {
        assert!(returned_types.contains(&(i as u32)), "Should have edge of type {}", i);
    }

    Ok(())
}

/// Tests IfaOnly mode with many edges per overflow type.
#[test]
fn true_ifa_overflow_with_multiple_edges_per_type() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("true_ifa_overflow_multi.db");
    let (pager, graph) = setup_graph_with_backend(&path, AdjacencyBackend::IfaOnly)?;

    let mut write = pager.begin_write()?;
    let src = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[],
        },
    )?;

    // Create 8 types with 10 edges each = 80 total edges
    // Types 1-5 inline, types 6-8 overflow
    let total_types = 8;
    let edges_per_type = 10;
    
    for type_idx in 1..=total_types {
        for _ in 0..edges_per_type {
            let dst = graph.create_node(
                &mut write,
                NodeSpec {
                    labels: &[LabelId(2)],
                    props: &[],
                },
            )?;
            graph.create_edge(
                &mut write,
                EdgeSpec {
                    src,
                    dst,
                    ty: TypeId(type_idx),
                    props: &[],
                },
            )?;
        }
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    
    // Verify total degree
    let total = graph.degree(&read, src, Dir::Out, None)?;
    assert_eq!(total, (total_types * edges_per_type) as u64);

    // Verify per-type degree for inline types
    for type_idx in 1..=5 {
        let degree = graph.degree(&read, src, Dir::Out, Some(TypeId(type_idx)))?;
        assert_eq!(degree, edges_per_type as u64, "Inline type {} should have {} edges", type_idx, edges_per_type);
    }

    // Verify per-type degree for overflow types
    for type_idx in 6..=total_types {
        let degree = graph.degree(&read, src, Dir::Out, Some(TypeId(type_idx)))?;
        assert_eq!(degree, edges_per_type as u64, "Overflow type {} should have {} edges", type_idx, edges_per_type);
    }

    // Verify neighbors
    let neighbors: Vec<_> = graph
        .neighbors(&read, src, Dir::Out, None, ExpandOpts::default())?
        .collect();
    assert_eq!(neighbors.len(), (total_types * edges_per_type) as usize);

    Ok(())
}

/// Tests IfaOnly mode with bidirectional edges and overflow.
#[test]
fn true_ifa_bidirectional_overflow() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("true_ifa_bidir.db");
    let (pager, graph) = setup_graph_with_backend(&path, AdjacencyBackend::IfaOnly)?;

    let mut write = pager.begin_write()?;
    
    // Create two nodes that will be connected in both directions
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

    // Create 8 edge types from A -> B (5 inline + 3 overflow)
    for i in 1..=8 {
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src: node_a,
                dst: node_b,
                ty: TypeId(i),
                props: &[],
            },
        )?;
    }

    // Create 8 edge types from B -> A (5 inline + 3 overflow)
    for i in 1..=8 {
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src: node_b,
                dst: node_a,
                ty: TypeId(i),
                props: &[],
            },
        )?;
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;

    // Verify outgoing edges from A
    let out_a = graph.degree(&read, node_a, Dir::Out, None)?;
    assert_eq!(out_a, 8, "Node A should have 8 outgoing edges");

    // Verify incoming edges to A (from B)
    let in_a = graph.degree(&read, node_a, Dir::In, None)?;
    assert_eq!(in_a, 8, "Node A should have 8 incoming edges");

    // Verify outgoing edges from B
    let out_b = graph.degree(&read, node_b, Dir::Out, None)?;
    assert_eq!(out_b, 8, "Node B should have 8 outgoing edges");

    // Verify incoming edges to B (from A)
    let in_b = graph.degree(&read, node_b, Dir::In, None)?;
    assert_eq!(in_b, 8, "Node B should have 8 incoming edges");

    // Verify both directions work for overflow types
    for i in 6..=8 {
        let deg = graph.degree(&read, node_a, Dir::Out, Some(TypeId(i)))?;
        assert_eq!(deg, 1, "Overflow type {} outgoing from A should have 1 edge", i);
        
        let deg = graph.degree(&read, node_a, Dir::In, Some(TypeId(i)))?;
        assert_eq!(deg, 1, "Overflow type {} incoming to A should have 1 edge", i);
    }

    Ok(())
}
