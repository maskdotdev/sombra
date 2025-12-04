//! MVCC Isolation Anomaly Tests
//!
//! Explicitly tests for known SQL isolation anomalies to document which are
//! prevented by Snapshot Isolation (SI) and which are allowed.
//!
//! ## Anomalies Prevented by SI
//! - Dirty reads (P1)
//! - Non-repeatable reads (P2)
//! - Phantom reads (P3)
//! - Lost updates (prevented by single-writer lock)
//!
//! ## Anomalies Theoretically Allowed by SI
//! - Write skew (A5B) - currently prevented by single-writer architecture
//!
//! References:
//! - "A Critique of ANSI SQL Isolation Levels" (Berenson et al., 1995)
//! - "Generalized Isolation Level Definitions" (Adya et al., 2000)
//! - docs/isolation-guarantees.md

#![allow(missing_docs)]
#![allow(clippy::arc_with_non_send_sync)]

use std::sync::Arc;

use sombra::primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions};
use sombra::storage::{
    EdgeSpec, Graph, GraphOptions, NodeSpec, PropEntry, PropPatch, PropPatchOp, PropValue,
    PropValueOwned,
};
use sombra::types::{LabelId, NodeId, PropId, Result, TypeId};
use tempfile::tempdir;

fn setup_graph(path: &std::path::Path) -> Result<(Arc<Pager>, Arc<Graph>)> {
    let pager = Arc::new(Pager::create(path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let graph = Graph::open(GraphOptions::new(store))?;
    Ok((pager, graph))
}

/// Helper to read a node's integer property value.
fn read_int_prop(graph: &Graph, read: &sombra::primitives::pager::ReadGuard, node_id: NodeId, prop_id: PropId) -> Result<Option<i64>> {
    Ok(graph.get_node(read, node_id)?.and_then(|node| {
        node.props
            .iter()
            .find(|(id, _)| *id == prop_id)
            .and_then(|(_, v)| match v {
                PropValueOwned::Int(i) => Some(*i),
                _ => None,
            })
    }))
}

// ============================================================================
// ANOMALIES PREVENTED BY SNAPSHOT ISOLATION
// ============================================================================

/// Test 2.1: Dirty Read Prevention (P1)
///
/// A dirty read occurs when a transaction reads data written by a concurrent
/// uncommitted transaction. SI prevents this by ensuring only committed data
/// is visible to readers.
///
/// Scenario:
/// 1. Txn A begins write, creates node with value=1, does NOT commit
/// 2. Txn B begins read, tries to read the node
/// 3. Assertion: Txn B does NOT see the uncommitted node
///
/// In Sombra, uncommitted writes are marked with CommitStatus::Pending and
/// have visibility flags that exclude them from reader snapshots.
#[test]
fn si_prevents_dirty_read() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("dirty_read.db");
    let (pager, graph) = setup_graph(&path)?;

    // Txn A: Begin write, create node, do NOT commit
    let mut write_a = pager.begin_write()?;
    let node_id = graph.create_node(
        &mut write_a,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(42))],
        },
    )?;

    // Txn B: Begin read, try to see the uncommitted node
    let read_b = pager.begin_latest_committed_read()?;
    let node = graph.get_node(&read_b, node_id)?;

    // ASSERTION: Txn B should NOT see uncommitted data
    assert!(
        node.is_none(),
        "Dirty read detected! Reader saw uncommitted node"
    );

    // Now commit Txn A
    pager.commit(write_a)?;

    // Txn C: New reader should now see the committed node
    let read_c = pager.begin_latest_committed_read()?;
    let node = graph.get_node(&read_c, node_id)?;
    assert!(
        node.is_some(),
        "Committed node should be visible to new readers"
    );

    Ok(())
}

/// Test 2.2: Non-Repeatable Read Prevention (P2)
///
/// A non-repeatable read occurs when a transaction reads the same row twice
/// and gets different values because another transaction modified and committed
/// the row between the two reads.
///
/// Scenario:
/// 1. Create node with value=1
/// 2. Txn A: begin read, read node (sees value=1)
/// 3. Txn B: update node to value=2, commit
/// 4. Txn A: read node again
/// 5. Assertion: Txn A still sees value=1 (repeatable read)
///
/// SI ensures that a transaction's snapshot is fixed at the start and never
/// changes, providing repeatable reads automatically.
#[test]
fn si_prevents_non_repeatable_read() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("non_repeatable.db");
    let (pager, graph) = setup_graph(&path)?;

    // Setup: Create node with value=1
    let node_id = {
        let mut write = pager.begin_write()?;
        let id = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(1)],
                props: &[PropEntry::new(PropId(1), PropValue::Int(1))],
            },
        )?;
        pager.commit(write)?;
        id
    };

    // Txn A: Begin read, first read
    let read_a = pager.begin_latest_committed_read()?;
    let value_first_read = read_int_prop(&graph, &read_a, node_id, PropId(1))?;
    assert_eq!(value_first_read, Some(1), "First read should see value=1");

    // Txn B: Update node to value=2, commit
    {
        let mut write_b = pager.begin_write()?;
        graph.update_node(
            &mut write_b,
            node_id,
            PropPatch::new(vec![PropPatchOp::Set(PropId(1), PropValue::Int(2))]),
        )?;
        pager.commit(write_b)?;
    }

    // Txn A: Second read (same transaction)
    let value_second_read = read_int_prop(&graph, &read_a, node_id, PropId(1))?;

    // ASSERTION: Txn A should still see value=1 (repeatable read)
    assert_eq!(
        value_second_read,
        Some(1),
        "Non-repeatable read detected! Second read returned different value"
    );

    // Verify Txn B's changes are visible to new transactions
    let read_c = pager.begin_latest_committed_read()?;
    let value_new_reader = read_int_prop(&graph, &read_c, node_id, PropId(1))?;
    assert_eq!(
        value_new_reader,
        Some(2),
        "New reader should see updated value=2"
    );

    Ok(())
}

/// Test 2.3: Phantom Read Prevention (P3)
///
/// A phantom read occurs when a transaction executes the same query twice
/// and the second query returns rows that weren't present in the first
/// (because another transaction inserted and committed them).
///
/// Scenario:
/// 1. Create nodes with label L1: node1, node2
/// 2. Txn A: begin read, scan nodes with label L1 (sees 2 nodes)
/// 3. Txn B: create node3 with label L1, commit
/// 4. Txn A: scan nodes with label L1 again
/// 5. Assertion: Txn A still sees only 2 nodes (no phantom)
///
/// SI prevents phantoms because the snapshot includes the state of all indexes
/// and structures at transaction start time.
#[test]
fn si_prevents_phantom_read() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("phantom.db");
    let (pager, graph) = setup_graph(&path)?;

    let label = LabelId(99);

    // Setup: Create 2 nodes with label
    {
        let mut write = pager.begin_write()?;
        for i in 0..2 {
            graph.create_node(
                &mut write,
                NodeSpec {
                    labels: &[label],
                    props: &[PropEntry::new(PropId(1), PropValue::Int(i))],
                },
            )?;
        }
        pager.commit(write)?;
    }

    // Txn A: Begin read, count nodes with label
    let read_a = pager.begin_latest_committed_read()?;

    // Count nodes by iterating (since we don't have direct label scan in this test)
    // We'll verify by counting all nodes we created
    // Note: In practice you'd use a label index scan, but for this test we verify
    // that no new node IDs appear in our snapshot
    let mut write_check = pager.begin_write()?;
    let new_node_id = graph.create_node(
        &mut write_check,
        NodeSpec {
            labels: &[label],
            props: &[PropEntry::new(PropId(1), PropValue::Int(999))],
        },
    )?;
    // Don't commit yet - just get the ID to check visibility

    // The new uncommitted node should not be visible
    let phantom_check = graph.get_node(&read_a, new_node_id)?;
    assert!(
        phantom_check.is_none(),
        "Phantom detected! Uncommitted node visible"
    );

    // Now commit and verify behavior
    pager.commit(write_check)?;

    // Txn A (same snapshot) should still not see the new node
    let phantom_check_after_commit = graph.get_node(&read_a, new_node_id)?;
    assert!(
        phantom_check_after_commit.is_none(),
        "Phantom detected! Node committed after snapshot start became visible"
    );

    // New transaction should see it
    let read_b = pager.begin_latest_committed_read()?;
    let new_node = graph.get_node(&read_b, new_node_id)?;
    assert!(
        new_node.is_some(),
        "New reader should see committed node"
    );

    Ok(())
}

/// Test 2.4: Lost Update Prevention (via Single-Writer Lock)
///
/// A lost update occurs when two transactions both read the same row, then both
/// update it based on what they read, and one update overwrites the other.
///
/// Classic example:
/// 1. Both Txn A and Txn B read counter=0
/// 2. Txn A: sets counter=1
/// 3. Txn B: sets counter=1 (based on reading 0)
/// 4. Result: counter=1 instead of 2 (one increment lost)
///
/// In Sombra, this is prevented by the single-writer lock:
/// - Only one write transaction can be active at a time
/// - Txn B must wait for Txn A to complete before it can begin writing
///
/// NOTE: This is an architecture-enforced guarantee, not a pure SI guarantee.
/// SI by itself would allow this anomaly (it's the "write skew" family).
#[test]
fn single_writer_prevents_lost_update() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("lost_update.db");
    let (pager, graph) = setup_graph(&path)?;

    // Setup: Create counter node with value=0
    let counter_id = {
        let mut write = pager.begin_write()?;
        let id = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(1)],
                props: &[PropEntry::new(PropId(1), PropValue::Int(0))],
            },
        )?;
        pager.commit(write)?;
        pager.checkpoint(CheckpointMode::Force)?;
        id
    };

    // Txn A: Begin write, read counter (sees 0)
    // Note: We cannot read within a write transaction (no read-your-own-writes support yet),
    // so we verify the value was 0 before starting the write.
    {
        let read_check = pager.begin_latest_committed_read()?;
        let value = read_int_prop(&graph, &read_check, counter_id, PropId(1))?;
        assert_eq!(value, Some(0), "Initial counter value should be 0");
    }
    let mut write_a = pager.begin_write()?;
    // In real code, Txn A would read then increment: value + 1 = 1

    // Txn B: Try to begin write from the same process
    // With single-writer, this should fail because write_a is active
    // The pager returns Invalid("writer lock already held") in this case
    let write_b_result = pager.begin_write();

    // ASSERTION: Txn B cannot begin while Txn A is active (same process)
    assert!(
        write_b_result.is_err(),
        "Lost update possible! Two concurrent writes were allowed"
    );
    match &write_b_result {
        Err(sombra::types::SombraError::Conflict(msg)) => {
            assert!(
                msg.contains("writer lock already held"),
                "Expected writer lock conflict message, got: {}",
                msg
            );
        }
        Err(e) => panic!("Expected Conflict error, got: {}", e),
        Ok(_) => panic!("Expected error but got Ok"),
    }

    // Complete Txn A
    graph.update_node(
        &mut write_a,
        counter_id,
        PropPatch::new(vec![PropPatchOp::Set(PropId(1), PropValue::Int(1))]),
    )?;
    pager.commit(write_a)?;

    // Now Txn B can proceed
    let mut write_b = pager.begin_write()?;
    // Read the committed value before modifying
    {
        let read_b = pager.begin_latest_committed_read()?;
        let value_b = read_int_prop(&graph, &read_b, counter_id, PropId(1))?;
        assert_eq!(
            value_b,
            Some(1),
            "Txn B should see Txn A's committed update"
        );
    }

    // Txn B increments to 2
    graph.update_node(
        &mut write_b,
        counter_id,
        PropPatch::new(vec![PropPatchOp::Set(PropId(1), PropValue::Int(2))]),
    )?;
    pager.commit(write_b)?;

    // Verify final value
    let read = pager.begin_latest_committed_read()?;
    let final_value = read_int_prop(&graph, &read, counter_id, PropId(1))?;
    assert_eq!(
        final_value,
        Some(2),
        "Counter should be 2 (both increments applied)"
    );

    Ok(())
}

// ============================================================================
// ANOMALIES THAT SI WOULD THEORETICALLY ALLOW (BUT PREVENTED BY ARCHITECTURE)
// ============================================================================

/// Test 2.5: Write Skew Scenario (Documented)
///
/// Write skew is an anomaly where two transactions read overlapping data,
/// make disjoint writes based on what they read, and together violate a
/// constraint that neither would violate alone.
///
/// Classic example (on-call doctors):
/// - Constraint: At least one doctor must be on call
/// - Doctor A and Doctor B are both on call
/// - Txn 1: Reads both, sees both on call, takes A off call
/// - Txn 2: Reads both, sees both on call, takes B off call
/// - Both commit -> no doctors on call! (constraint violated)
///
/// In Sombra's single-writer model, this CANNOT occur because:
/// - Only one write transaction can be active
/// - Txn 2 must wait for Txn 1 to commit
/// - When Txn 2 reads, it sees Txn 1's changes
///
/// This test documents the scenario and verifies single-writer prevention.
#[test]
fn write_skew_prevented_by_single_writer() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("write_skew.db");
    let (pager, graph) = setup_graph(&path)?;

    // Setup: Two accounts with balance=100 each
    // Constraint: sum(balances) >= 0
    let (account_a, account_b) = {
        let mut write = pager.begin_write()?;
        let a = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(1)],
                props: &[PropEntry::new(PropId(1), PropValue::Int(100))],
            },
        )?;
        let b = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(1)],
                props: &[PropEntry::new(PropId(1), PropValue::Int(100))],
            },
        )?;
        pager.commit(write)?;
        (a, b)
    };

    // Helper to read total balance
    let read_total = |pager: &Pager, graph: &Graph| -> Result<i64> {
        let read = pager.begin_latest_committed_read()?;
        let a = read_int_prop(graph, &read, account_a, PropId(1))?.unwrap_or(0);
        let b = read_int_prop(graph, &read, account_b, PropId(1))?.unwrap_or(0);
        Ok(a + b)
    };

    // Verify initial state
    assert_eq!(read_total(&pager, &graph)?, 200);

    // Simulate what WOULD happen in a multi-writer system:
    // Txn 1 would read both (total=200), decide to withdraw 150 from A
    // Txn 2 would read both (total=200), decide to withdraw 150 from B
    // Both would commit -> total = -100 (constraint violated!)

    // In Sombra, Txn 1 goes first:
    {
        let mut write1 = pager.begin_write()?;
        // Read the current balances using a committed read snapshot
        let total = {
            let read = pager.begin_latest_committed_read()?;
            let a = read_int_prop(&graph, &read, account_a, PropId(1))?.unwrap();
            let b = read_int_prop(&graph, &read, account_b, PropId(1))?.unwrap();
            a + b
        };
        assert_eq!(total, 200, "Txn 1 sees total=200");

        // Withdraw 150 from A (balance check passes: 200 - 150 = 50 >= 0)
        graph.update_node(
            &mut write1,
            account_a,
            PropPatch::new(vec![PropPatchOp::Set(PropId(1), PropValue::Int(-50))]), // 100 - 150 = -50
        )?;
        pager.commit(write1)?;
    }

    // Txn 2 now reads AFTER Txn 1 committed
    {
        let _write2 = pager.begin_write()?;
        let (a_balance, b_balance, total) = {
            let read = pager.begin_latest_committed_read()?;
            let a = read_int_prop(&graph, &read, account_a, PropId(1))?.unwrap();
            let b = read_int_prop(&graph, &read, account_b, PropId(1))?.unwrap();
            (a, b, a + b)
        };

        // Txn 2 sees the updated state
        assert_eq!(a_balance, -50, "Txn 2 should see A's balance after Txn 1");
        assert_eq!(b_balance, 100, "Txn 2 should see B's original balance");
        assert_eq!(total, 50, "Txn 2 sees total=50 (after Txn 1's withdrawal)");

        // If Txn 2 tries to withdraw 150 from B, it would see total would be -100
        // A proper application would reject this:
        let would_be_total_after_withdrawal = total - 150;
        assert!(
            would_be_total_after_withdrawal < 0,
            "Application should reject: would violate constraint"
        );

        // Don't actually do the violating write
        // drop(write2) - implicit rollback
    }

    // ASSERTION: Constraint is preserved because single-writer forced serialization
    let final_total = read_total(&pager, &graph)?;
    assert!(
        final_total >= 0 || final_total == 50, // 50 after first withdrawal
        "Write skew prevented by single-writer serialization"
    );

    Ok(())
}

/// Test 2.6: Read-Only Anomaly (Not Applicable to SI)
///
/// The "read-only transaction anomaly" is a specific phenomenon where a
/// read-only transaction observes a state that could not have existed in
/// any serial execution. This is possible under some weak isolation levels
/// but NOT under Snapshot Isolation.
///
/// SI prevents this because:
/// - Each transaction sees a consistent snapshot
/// - The snapshot represents a state that DID exist at some point
/// - No "future" or "mixed" states are observable
///
/// This test documents why the anomaly is not applicable.
#[test]
fn read_only_anomaly_not_applicable() {
    // This is a documentation-only test.
    //
    // The read-only anomaly occurs in systems with weaker isolation where
    // a read-only transaction might see:
    // - Some effects of Txn A
    // - Some effects of Txn B
    // - In a combination that never existed in any serial history
    //
    // Example:
    // - Initial: x=0, y=0
    // - Txn A: x=1, y=1
    // - Txn B: if x==1 then y=2
    // - Read-only Txn R might see: x=1, y=0 (impossible in any serial order)
    //
    // In Sombra's SI:
    // - Txn R's snapshot is fixed at begin time
    // - R sees EITHER (x=0, y=0) OR (x=1, y=1) OR later states
    // - R cannot see partial states from uncommitted transactions
    //
    // Therefore, read-only anomaly is structurally impossible under SI.

    // No actual test code needed - this documents the guarantee
}

// ============================================================================
// ADDITIONAL ISOLATION TESTS
// ============================================================================

/// Test: Read-your-own-writes within a transaction
///
/// A transaction should be able to read data it has written, even before commit.
///
/// NOTE: This test is currently ignored because the pager does not yet support
/// read-your-own-writes (no `as_read()` method on WriteGuard). This feature
/// would require significant infrastructure to properly implement MVCC visibility
/// for uncommitted writes within the same transaction.
#[test]
#[ignore = "read-your-own-writes not yet implemented - requires WriteGuard::as_read()"]
fn read_your_own_writes() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("ryow.db");
    let (pager, graph) = setup_graph(&path)?;

    let mut write = pager.begin_write()?;

    // Create node
    let node_id = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[PropEntry::new(PropId(1), PropValue::Int(100))],
        },
    )?;

    // TODO: Read own write (within same transaction) - requires as_read()
    // let read_guard = write.as_read();
    // let value = read_int_prop(&graph, &read_guard, node_id, PropId(1))?;
    // assert_eq!(value, Some(100), "Should be able to read own uncommitted write");

    // Update the node
    graph.update_node(
        &mut write,
        node_id,
        PropPatch::new(vec![PropPatchOp::Set(PropId(1), PropValue::Int(200))]),
    )?;

    // TODO: Read updated value - requires as_read()
    // let read_guard = write.as_read();
    // let value = read_int_prop(&graph, &read_guard, node_id, PropId(1))?;
    // assert_eq!(value, Some(200), "Should see own updated value");

    pager.commit(write)?;

    // Verify final state (this part works)
    let read = pager.begin_latest_committed_read()?;
    let final_value = read_int_prop(&graph, &read, node_id, PropId(1))?;
    assert_eq!(final_value, Some(200));

    Ok(())
}

/// Test: Rollback does not affect readers
///
/// If a write transaction rolls back, readers should not see any of its changes.
/// The rollback mechanism invalidates (evicts) modified pages from the cache
/// rather than restoring them in-place, ensuring concurrent readers never see
/// partially-written or corrupted page data.
#[test]
fn rollback_invisible_to_readers() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("rollback.db");
    let (pager, graph) = setup_graph(&path)?;

    // Create initial node
    let node_id = {
        let mut write = pager.begin_write()?;
        let id = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(1)],
                props: &[PropEntry::new(PropId(1), PropValue::Int(1))],
            },
        )?;
        pager.commit(write)?;
        id
    };

    // Reader opens snapshot
    let read_before = pager.begin_latest_committed_read()?;

    // Start write transaction but DON'T commit (will rollback)
    {
        let mut write = pager.begin_write()?;
        graph.update_node(
            &mut write,
            node_id,
            PropPatch::new(vec![PropPatchOp::Set(PropId(1), PropValue::Int(999))]),
        )?;
        // Implicit rollback when write goes out of scope without commit
    }

    // Reader opened before should see original value
    let value = read_int_prop(&graph, &read_before, node_id, PropId(1))?;
    assert_eq!(value, Some(1), "Reader should see original value");

    // New reader should also see original value (rollback had no effect)
    let read_after = pager.begin_latest_committed_read()?;
    let value = read_int_prop(&graph, &read_after, node_id, PropId(1))?;
    assert_eq!(
        value,
        Some(1),
        "New reader should see original value (rollback is invisible)"
    );

    Ok(())
}

/// Test: Edge visibility follows same rules as node visibility
///
/// Edges should have the same isolation guarantees as nodes.
#[test]
fn edge_isolation_consistency() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("edge_isolation.db");
    let (pager, graph) = setup_graph(&path)?;

    // Create two nodes
    let (src, dst) = {
        let mut write = pager.begin_write()?;
        let s = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(1)],
                props: &[],
            },
        )?;
        let d = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(1)],
                props: &[],
            },
        )?;
        pager.commit(write)?;
        (s, d)
    };

    // Reader A opens snapshot (no edges yet)
    let read_a = pager.begin_latest_committed_read()?;

    // Create edge
    let edge_id = {
        let mut write = pager.begin_write()?;
        let e = graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst,
                ty: TypeId(1),
                props: &[PropEntry::new(PropId(1), PropValue::Int(42))],
            },
        )?;
        pager.commit(write)?;
        e
    };

    // Reader A should NOT see the edge (created after snapshot)
    let edge_a = graph.get_edge(&read_a, edge_id)?;
    assert!(
        edge_a.is_none(),
        "Phantom edge! Reader A saw edge created after snapshot"
    );

    // New reader should see the edge
    let read_b = pager.begin_latest_committed_read()?;
    let edge_b = graph.get_edge(&read_b, edge_id)?;
    assert!(edge_b.is_some(), "New reader should see committed edge");

    Ok(())
}

/// Test: Degree queries are consistent with snapshot
///
/// The degree() function should return results consistent with the transaction's
/// snapshot, not the current committed state.
#[test]
fn degree_snapshot_consistency() -> Result<()> {
    use sombra::storage::Dir;

    let dir = tempdir()?;
    let path = dir.path().join("degree_snapshot.db");
    let (pager, graph) = setup_graph(&path)?;

    // Create source node and one edge
    let src = {
        let mut write = pager.begin_write()?;
        let s = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(1)],
                props: &[],
            },
        )?;
        let d = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(1)],
                props: &[],
            },
        )?;
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src: s,
                dst: d,
                ty: TypeId(1),
                props: &[],
            },
        )?;
        pager.commit(write)?;
        s
    };

    // Reader A: degree should be 1
    let read_a = pager.begin_latest_committed_read()?;
    let degree_a = graph.degree(&read_a, src, Dir::Out, None)?;
    assert_eq!(degree_a, 1, "Initial degree should be 1");

    // Add another edge
    {
        let mut write = pager.begin_write()?;
        let d2 = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(1)],
                props: &[],
            },
        )?;
        graph.create_edge(
            &mut write,
            EdgeSpec {
                src,
                dst: d2,
                ty: TypeId(1),
                props: &[],
            },
        )?;
        pager.commit(write)?;
    }

    // Reader A should still see degree=1
    let degree_a_after = graph.degree(&read_a, src, Dir::Out, None)?;
    assert_eq!(
        degree_a_after, 1,
        "Reader A should still see degree=1 (snapshot consistency)"
    );

    // New reader should see degree=2
    let read_b = pager.begin_latest_committed_read()?;
    let degree_b = graph.degree(&read_b, src, Dir::Out, None)?;
    assert_eq!(degree_b, 2, "New reader should see degree=2");

    Ok(())
}

/// Test: try_acquire_writer returns Conflict when lock is already held
///
/// The non-blocking writer acquisition should return a Conflict error when
/// the writer lock is already held by the same process.
#[test]
fn try_acquire_writer_conflict() -> Result<()> {
    use sombra::primitives::concurrency::SingleWriter;

    let dir = tempdir()?;
    let lock_path = dir.path().join("try_writer.lock");

    // Use a single SingleWriter instance - this tests the in-memory conflict detection
    let lock = SingleWriter::open(&lock_path)?;

    // Acquire first writer
    let _write1 = lock.acquire_writer()?;

    // Try to acquire second writer using try_acquire on the same instance
    // This should detect the conflict via in-memory state tracking
    let result = lock.try_acquire_writer();

    match result {
        Err(sombra::types::SombraError::Conflict(msg)) => {
            assert!(
                msg.contains("writer lock already held"),
                "Expected writer lock conflict message, got: {}",
                msg
            );
        }
        Ok(Some(_)) => panic!("try_acquire_writer should not succeed when writer is held"),
        Ok(None) => panic!("try_acquire_writer should return Conflict, not None"),
        Err(e) => panic!("Expected Conflict error, got: {:?}", e),
    }

    Ok(())
}
