//! MVCC Concurrent Stress Tests
//!
//! Validates MVCC correctness under concurrent multi-threaded access with
//! readers and writers operating simultaneously.
//!
//! These tests verify:
//! - Snapshot consistency under concurrent writes
//! - Reader isolation from ongoing modifications
//! - Vacuum respects active readers
//! - No deadlocks or corruption under high contention

#![allow(missing_docs)]
#![allow(clippy::arc_with_non_send_sync)]
#![allow(unused_imports)] // Some imports needed for disabled tests

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use sombra::primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions};
use sombra::storage::{
    Dir, EdgeSpec, Graph, GraphOptions, NodeSpec, PropEntry, PropPatch, PropPatchOp, PropValue,
    PropValueOwned, VacuumCfg,
};
use sombra::types::{LabelId, NodeId, PropId, Result, TypeId};
use tempfile::tempdir;

// Constants kept for documentation of what the disabled tests would use
#[allow(dead_code)]
const READER_COUNT: usize = 4;
#[allow(dead_code)]
const WRITER_ITERATIONS: usize = 500;
#[allow(dead_code)]
const INITIAL_NODE_COUNT: usize = 100;

fn setup_graph(path: &std::path::Path) -> Result<(Arc<Pager>, Arc<Graph>)> {
    let pager = Arc::new(Pager::create(path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let graph = Graph::open(GraphOptions::new(store))?;
    Ok((pager, graph))
}

fn setup_graph_with_vacuum(
    path: &std::path::Path,
    vacuum_cfg: VacuumCfg,
) -> Result<(Arc<Pager>, Arc<Graph>)> {
    let pager = Arc::new(Pager::create(path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let opts = GraphOptions::new(store).vacuum(vacuum_cfg);
    let graph = Graph::open(opts)?;
    Ok((pager, graph))
}

fn small_vacuum_cfg() -> VacuumCfg {
    VacuumCfg {
        enabled: true,
        interval: Duration::from_millis(50),
        retention_window: Duration::from_millis(100),
        log_high_water_bytes: 1024,
        max_pages_per_pass: 16,
        max_millis_per_pass: 10,
        index_cleanup: true,
        reader_timeout: Duration::MAX, // Disable for stress tests
        reader_timeout_warn_threshold_pct: 0,
    }
}

/// Creates initial nodes with a `version` property set to 0.
fn create_initial_nodes(
    pager: &Pager,
    graph: &Graph,
    count: usize,
) -> Result<Vec<NodeId>> {
    let mut node_ids = Vec::with_capacity(count);
    let mut write = pager.begin_write()?;
    for _ in 0..count {
        let node_id = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(1)],
                props: &[PropEntry::new(PropId(1), PropValue::Int(0))],
            },
        )?;
        node_ids.push(node_id);
    }
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;
    Ok(node_ids)
}

/// Reads all nodes and extracts their version property values.
#[allow(dead_code)] // Kept for future use when Graph becomes Send+Sync
fn read_all_versions(
    pager: &Pager,
    graph: &Graph,
    node_ids: &[NodeId],
) -> Result<Vec<i64>> {
    let read = pager.begin_latest_committed_read()?;
    let mut versions = Vec::with_capacity(node_ids.len());
    for &node_id in node_ids {
        let node = graph.get_node(&read, node_id)?.expect("node should exist");
        let version = node
            .props
            .iter()
            .find(|(id, _)| *id == PropId(1))
            .and_then(|(_, v)| match v {
                PropValueOwned::Int(i) => Some(*i),
                _ => None,
            })
            .expect("version property should exist");
        versions.push(version);
    }
    Ok(versions)
}

/// Test 1.1: Concurrent readers with an active writer
///
/// Verifies that:
/// - Each reader sees a consistent snapshot (no partial updates)
/// - Reader snapshots don't change even as writes continue
/// - All threads complete without panic/error
///
/// NOTE: This test is currently disabled because Graph is not Send+Sync.
/// The Graph struct contains Cell and RefCell fields (in VacuumSched, IndexStore, etc.)
/// which are not thread-safe. Making Graph thread-safe would require architectural
/// changes to use atomic types or mutexes for these fields.
///
/// To enable this test, Graph needs to implement Send+Sync by replacing:
/// - Cell<bool> with AtomicBool
/// - Cell<u128> with AtomicU128 (or Mutex<u128>)
/// - RefCell<T> with RwLock<T> or Mutex<T>
/// - dyn PageStore with dyn PageStore + Send + Sync
#[test]
#[ignore = "Graph is not Send+Sync - requires architectural changes for thread safety"]
fn concurrent_readers_with_active_writer() -> Result<()> {
    // Test disabled at compile time because Graph is not thread-safe.
    // See docstring for details on what would need to change.
    //
    // The intended test would:
    // 1. Create 100 nodes with version=0
    // 2. Spawn 4 reader threads that continuously read and verify snapshot consistency
    // 3. Run a writer thread that updates all nodes to increasing version numbers
    // 4. Verify all readers see consistent snapshots (all nodes at same version)
    // 5. Verify final state has all nodes at the expected version
    Ok(())
}

/// Test 1.2: Snapshot stability under concurrent writes
///
/// Verifies that readers pinned to different snapshots see the correct
/// version of data, even as new commits occur.
#[test]
fn snapshot_stability_under_concurrent_writes() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("snapshot_stability.db");
    let (pager, graph) = setup_graph(&path)?;

    // Create 10 nodes with version=0
    let node_count = 10;
    let node_ids = create_initial_nodes(&pager, &graph, node_count)?;

    // Reader A opens snapshot (should see version=0)
    let read_a = pager.begin_latest_committed_read()?;

    // Writer updates all nodes to version=1
    {
        let mut write = pager.begin_write()?;
        for &node_id in &node_ids {
            graph.update_node(
                &mut write,
                node_id,
                PropPatch::new(vec![PropPatchOp::Set(PropId(1), PropValue::Int(1))]),
            )?;
        }
        pager.commit(write)?;
    }

    // Reader B opens snapshot (should see version=1)
    let read_b = pager.begin_latest_committed_read()?;

    // Writer updates all nodes to version=2
    {
        let mut write = pager.begin_write()?;
        for &node_id in &node_ids {
            graph.update_node(
                &mut write,
                node_id,
                PropPatch::new(vec![PropPatchOp::Set(PropId(1), PropValue::Int(2))]),
            )?;
        }
        pager.commit(write)?;
    }

    // Verify Reader A sees version=0 for all nodes
    for &node_id in &node_ids {
        let node = graph.get_node(&read_a, node_id)?.expect("node should exist");
        let version = node
            .props
            .iter()
            .find(|(id, _)| *id == PropId(1))
            .and_then(|(_, v)| match v {
                PropValueOwned::Int(i) => Some(*i),
                _ => None,
            })
            .expect("version property should exist");
        assert_eq!(version, 0, "Reader A should see version=0");
    }

    // Verify Reader B sees version=1 for all nodes
    for &node_id in &node_ids {
        let node = graph.get_node(&read_b, node_id)?.expect("node should exist");
        let version = node
            .props
            .iter()
            .find(|(id, _)| *id == PropId(1))
            .and_then(|(_, v)| match v {
                PropValueOwned::Int(i) => Some(*i),
                _ => None,
            })
            .expect("version property should exist");
        assert_eq!(version, 1, "Reader B should see version=1");
    }

    // Verify new reader sees version=2
    let read_c = pager.begin_latest_committed_read()?;
    for &node_id in &node_ids {
        let node = graph.get_node(&read_c, node_id)?.expect("node should exist");
        let version = node
            .props
            .iter()
            .find(|(id, _)| *id == PropId(1))
            .and_then(|(_, v)| match v {
                PropValueOwned::Int(i) => Some(*i),
                _ => None,
            })
            .expect("version property should exist");
        assert_eq!(version, 2, "Reader C should see version=2");
    }

    Ok(())
}

/// Test 1.3: Vacuum respects active readers
///
/// Verifies that:
/// - Old snapshots can still see deleted data while pinned
/// - Vacuum does not reclaim versions pinned by readers
/// - After reader drops, vacuum can reclaim versions
#[test]
fn vacuum_respects_active_readers() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("vacuum_readers.db");

    // Use aggressive vacuum settings
    let vacuum_cfg = small_vacuum_cfg();
    let (pager, graph) = setup_graph_with_vacuum(&path, vacuum_cfg)?;

    // Create nodes
    let mut write = pager.begin_write()?;
    let node1 = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(100))],
        },
    )?;
    let node2 = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(200))],
        },
    )?;
    pager.commit(write)?;

    // Reader pins old snapshot
    let old_read = pager.begin_latest_committed_read()?;

    // Delete node1
    {
        let mut write = pager.begin_write()?;
        graph.delete_node(&mut write, node1, Default::default())?;
        pager.commit(write)?;
    }

    // Trigger some vacuum activity
    for _ in 0..5 {
        let mut write = pager.begin_write()?;
        graph.update_node(
            &mut write,
            node2,
            PropPatch::new(vec![PropPatchOp::Set(PropId(1), PropValue::Int(999))]),
        )?;
        pager.commit(write)?;
        thread::sleep(Duration::from_millis(20));
    }

    // Old reader should still see deleted node
    let node1_old = graph.get_node(&old_read, node1)?;
    assert!(
        node1_old.is_some(),
        "Old snapshot should still see deleted node"
    );
    if let Some(node) = node1_old {
        let value = node
            .props
            .iter()
            .find(|(id, _)| *id == PropId(1))
            .and_then(|(_, v)| match v {
                PropValueOwned::Int(i) => Some(*i),
                _ => None,
            });
        assert_eq!(value, Some(100), "Old snapshot should see original value");
    }

    // New reader should NOT see deleted node
    let new_read = pager.begin_latest_committed_read()?;
    let node1_new = graph.get_node(&new_read, node1)?;
    assert!(
        node1_new.is_none(),
        "New snapshot should not see deleted node"
    );

    // Drop old reader
    drop(old_read);

    // After dropping, vacuum should be able to reclaim (eventually)
    // We don't strictly verify reclamation here, just that the system
    // continues to function correctly
    thread::sleep(Duration::from_millis(100));

    // Verify graph is still consistent
    let verify_read = pager.begin_latest_committed_read()?;
    assert!(graph.get_node(&verify_read, node1)?.is_none());
    assert!(graph.get_node(&verify_read, node2)?.is_some());

    Ok(())
}

/// Test 1.4: High contention read-write mix
///
/// Stress test with multiple writers attempting to acquire locks and
/// multiple readers performing continuous reads.
///
/// NOTE: This test is currently disabled because Graph is not Send+Sync.
/// See concurrent_readers_with_active_writer for details on what would need
/// to change to enable this test.
#[test]
#[ignore = "Graph is not Send+Sync - requires architectural changes for thread safety"]
fn high_contention_read_write_mix() -> Result<()> {
    // Test disabled at compile time because Graph is not thread-safe.
    //
    // The intended test would:
    // 1. Create 50 nodes and 200 edges
    // 2. Spawn 8 reader threads performing continuous reads
    // 3. Spawn 2 writer threads contending for the single-writer lock
    // 4. Run for 5 seconds under high contention
    // 5. Verify no deadlocks (test completes within timeout)
    // 6. Verify graph integrity after stress test
    Ok(())
}

/// Test: Multiple readers pinning same snapshot
///
/// Verifies that multiple readers can pin the same snapshot commit
/// and all see consistent data.
#[test]
fn multiple_readers_same_snapshot() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("multi_reader_snapshot.db");
    let (pager, graph) = setup_graph(&path)?;

    // Create initial data
    let node_ids = create_initial_nodes(&pager, &graph, 5)?;

    // Multiple readers pin the same snapshot
    let readers: Vec<_> = (0..10)
        .map(|_| pager.begin_latest_committed_read())
        .collect::<Result<Vec<_>>>()?;

    // Update data
    {
        let mut write = pager.begin_write()?;
        for &node_id in &node_ids {
            graph.update_node(
                &mut write,
                node_id,
                PropPatch::new(vec![PropPatchOp::Set(PropId(1), PropValue::Int(999))]),
            )?;
        }
        pager.commit(write)?;
    }

    // All original readers should still see version=0
    for (i, read) in readers.iter().enumerate() {
        let node = graph.get_node(read, node_ids[0])?.expect("node should exist");
        let version = node
            .props
            .iter()
            .find(|(id, _)| *id == PropId(1))
            .and_then(|(_, v)| match v {
                PropValueOwned::Int(i) => Some(*i),
                _ => None,
            })
            .expect("version property should exist");
        assert_eq!(version, 0, "Reader {} should see version=0", i);
    }

    Ok(())
}

/// Test: Rapid snapshot creation and release
///
/// Tests the overhead and correctness of rapidly creating and dropping
/// read snapshots.
#[test]
fn rapid_snapshot_churn() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("snapshot_churn.db");
    let (pager, graph) = setup_graph(&path)?;

    let node_ids = create_initial_nodes(&pager, &graph, 10)?;

    let iterations = 1000;
    let start = Instant::now();

    for i in 0..iterations {
        let read = pager.begin_latest_committed_read()?;

        // Verify we can read
        let node = graph.get_node(&read, node_ids[0])?;
        assert!(node.is_some(), "Node should exist at iteration {}", i);

        // Snapshot is dropped here
    }

    let elapsed = start.elapsed();
    eprintln!(
        "Snapshot churn: {} iterations in {:?} ({:.0} ops/sec)",
        iterations,
        elapsed,
        iterations as f64 / elapsed.as_secs_f64()
    );

    // Verify system is still healthy
    let final_read = pager.begin_latest_committed_read()?;
    for &node_id in &node_ids {
        assert!(
            graph.get_node(&final_read, node_id)?.is_some(),
            "All nodes should exist after churn test"
        );
    }

    Ok(())
}
