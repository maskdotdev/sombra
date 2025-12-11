#![cfg(test)]

use super::*;
use std::ops::Bound;

mod vacuum_background_tests {
    use super::*;
    use crate::primitives::pager::{PageStore, Pager, PagerOptions};
    use crate::storage::{DeleteNodeOpts, GraphOptions, NodeSpec, PropEntry, PropValue};
    use crate::types::{LabelId, PropId, Result};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;

    fn setup_graph(cfg: VacuumCfg) -> (tempfile::TempDir, Arc<Pager>, Arc<Graph>) {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("vacuum-bg.db");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
        let store: Arc<dyn PageStore> = pager.clone();
        let graph = Graph::open(GraphOptions::new(store).vacuum(cfg)).unwrap();
        (dir, pager, graph)
    }

    fn create_and_delete_node(pager: &Pager, graph: &Graph) -> Result<()> {
        let mut write = pager.begin_write()?;
        let node = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(1)],
                props: &[PropEntry::new(PropId(1), PropValue::Int(1))],
            },
        )?;
        pager.commit(write)?;
        eprintln!("node created");
        let mut write = pager.begin_write()?;
        graph.delete_node(&mut write, node, DeleteNodeOpts::default())?;
        pager.commit(write)?;
        Ok(())
    }

    #[test]
    fn reentrancy_guard_skips_when_running() -> Result<()> {
        let cfg = VacuumCfg {
            enabled: true,
            interval: Duration::from_millis(50),
            retention_window: Duration::from_millis(0),
            log_high_water_bytes: u64::MAX,
            max_pages_per_pass: 32,
            max_millis_per_pass: 50,
            index_cleanup: true,
            reader_timeout: Duration::MAX,
            reader_timeout_warn_threshold_pct: 0,
        };
        let (_tmpdir, pager, graph) = setup_graph(cfg.clone());
        create_and_delete_node(&pager, &graph)?;
        graph.vacuum_sched.last_stats.borrow_mut().take();
        graph.vacuum_sched.running.set(true);
        graph.trigger_vacuum();
        assert!(graph.vacuum_sched.last_stats.borrow().is_none());
        graph.vacuum_sched.running.set(false);
        graph.trigger_vacuum();
        assert!(graph.last_vacuum_stats().is_some());
        drop(graph);
        drop(pager);
        Ok(())
    }

    #[test]
    fn high_water_sets_pending_trigger() -> Result<()> {
        let cfg = VacuumCfg {
            enabled: false,
            interval: Duration::from_secs(60),
            retention_window: Duration::from_millis(0),
            log_high_water_bytes: 1,
            max_pages_per_pass: 16,
            max_millis_per_pass: 10,
            index_cleanup: true,
            reader_timeout: Duration::MAX,
            reader_timeout_warn_threshold_pct: 0,
        };
        let (_tmpdir, pager, graph) = setup_graph(cfg.clone());
        let mut write = pager.begin_write()?;
        let node = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[LabelId(2)],
                props: &[PropEntry::new(PropId(2), PropValue::Int(2))],
            },
        )?;
        pager.commit(write)?;
        let mut write = pager.begin_write()?;
        graph.delete_node(&mut write, node, DeleteNodeOpts::default())?;
        pager.commit(write)?;
        let bytes = graph.version_log_bytes();
        assert!(
            bytes >= cfg.log_high_water_bytes,
            "version_log_bytes below threshold: {} < {}",
            bytes,
            cfg.log_high_water_bytes
        );
        assert!(matches!(
            graph.vacuum_sched.pending_trigger.get(),
            Some(VacuumTrigger::HighWater)
        ));
        assert_eq!(graph.vacuum_sched.next_deadline_ms.get(), 0);
        drop(graph);
        drop(pager);
        Ok(())
    }

    #[test]
    fn reports_configured_retention_window() -> Result<()> {
        let retention = Duration::from_secs(42);
        let cfg = VacuumCfg {
            enabled: true,
            interval: Duration::from_millis(50),
            retention_window: retention,
            log_high_water_bytes: u64::MAX,
            max_pages_per_pass: 16,
            max_millis_per_pass: 10,
            index_cleanup: true,
            reader_timeout: Duration::MAX,
            reader_timeout_warn_threshold_pct: 0,
        };
        let (_tmpdir, _pager, graph) = setup_graph(cfg);
        assert_eq!(graph.vacuum_retention_window(), retention);
        Ok(())
    }
}

mod adjacency_commit_tests {
    use super::*;
    use crate::primitives::pager::{PageStore, Pager, PagerOptions, ReadGuard};
    use crate::storage::{adjacency, EdgeSpec, GraphOptions, NodeSpec};
    use crate::types::{Result, TypeId};
    use std::ops::Bound;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn setup_graph(name: &str) -> (tempfile::TempDir, Arc<Pager>, Arc<Graph>) {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join(name);
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
        let store: Arc<dyn PageStore> = pager.clone();
        let graph = Graph::open(GraphOptions::new(store)).unwrap();
        (dir, pager, graph)
    }

    fn collect_edges(
        graph: &Graph,
        read: &ReadGuard,
        fwd: bool,
    ) -> Result<Vec<(NodeId, TypeId, NodeId, EdgeId)>> {
        let tree = if fwd { &graph.adj_fwd } else { &graph.adj_rev };
        let mut cursor = tree.range(read, Bound::Unbounded, Bound::Unbounded)?;
        let mut entries = Vec::new();
        let snapshot = Graph::reader_snapshot_commit(read);
        while let Some((key, value)) = cursor.next()? {
            if !Graph::version_visible(&value.header, snapshot) {
                continue;
            }
            let decoded = if fwd {
                adjacency::decode_fwd_key(&key).ok_or(SombraError::Corruption("adj key decode"))?
            } else {
                adjacency::decode_rev_key(&key).ok_or(SombraError::Corruption("adj key decode"))?
            };
            entries.push(decoded);
        }
        entries.sort();
        Ok(entries)
    }

    #[test]
    fn adjacency_batch_flushes_on_commit() -> Result<()> {
        let (dir, pager, graph) = setup_graph("adjacency_batch_flush.db");

        let mut write = pager.begin_write()?;
        let a = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[],
                props: &[],
            },
        )?;
        let b = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[],
                props: &[],
            },
        )?;
        let ty = TypeId(1);
        let _edge1 = graph.create_edge(
            &mut write,
            EdgeSpec {
                src: a,
                dst: b,
                ty,
                props: &[],
            },
        )?;
        let _edge2 = graph.create_edge(
            &mut write,
            EdgeSpec {
                src: b,
                dst: a,
                ty,
                props: &[],
            },
        )?;
        pager.commit(write)?;

        let read = pager.begin_latest_committed_read()?;
        let fwd = collect_edges(&graph, &read, true)?;
        let rev = collect_edges(&graph, &read, false)?;
        assert_eq!(fwd.len(), 2);
        assert_eq!(rev.len(), 2);
        drop(read);
        drop(graph);
        drop(pager);
        drop(dir);
        Ok(())
    }
}

mod wal_recovery_tests {
    use super::*;
    use crate::primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions};
    use crate::storage::mvcc::VersionLogEntry;
    use crate::storage::node;
    use crate::storage::patch::{PropPatch, PropPatchOp};
    use crate::storage::props;
    use crate::storage::props::RawPropValue;
    use crate::storage::{NodeSpec, PropEntry, PropValue};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn wal_recovery_restores_version_log_entries() -> Result<()> {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("mvcc_wal_recovery.db");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
        let store: Arc<dyn PageStore> = pager.clone();
        let graph = Graph::open(GraphOptions::new(store)).unwrap();

        let mut write = pager.begin_write()?;
        let node = graph.create_node(
            &mut write,
            NodeSpec {
                labels: &[],
                props: &[PropEntry::new(PropId(1), PropValue::Int(1))],
            },
        )?;
        pager.commit(write)?;

        let mut write = pager.begin_write()?;
        graph.update_node(
            &mut write,
            node,
            PropPatch::new(vec![PropPatchOp::Set(PropId(1), PropValue::Int(2))]),
        )?;
        pager.commit(write)?;
        eprintln!("node updated");
        pager.checkpoint(CheckpointMode::Force)?;
        eprintln!("checkpoint after update");

        drop(graph);
        drop(pager);

        let reopened_store: Arc<dyn PageStore> =
            Arc::new(Pager::open(&path, PagerOptions::default()).unwrap());
        let reopened_graph = Graph::open(GraphOptions::new(Arc::clone(&reopened_store))).unwrap();
        let read = reopened_store.begin_latest_committed_read()?;

        let mut cursor = reopened_graph
            .nodes
            .range(&read, Bound::Unbounded, Bound::Unbounded)?;
        let (_node_key, head_bytes) = cursor.next()?.expect("node present after crash recovery");
        let head = node::decode(&head_bytes)?;
        let head_props = reopened_graph.read_node_prop_bytes(&head.row.props)?;
        let head_raw = props::decode_raw(&head_props)?;
        assert_eq!(head_raw.len(), 1);
        match head_raw[0].value {
            RawPropValue::Int(value) => assert_eq!(value, 2),
            _ => panic!("unexpected property value {:?}", head_raw[0].value),
        }

        let prev_ptr = head.prev_ptr;
        assert!(
            !prev_ptr.is_null(),
            "node update should log previous version"
        );
        let log_bytes = reopened_graph
            .version_log
            .get(&read, &prev_ptr.raw())?
            .expect("version log entry missing after recovery");
        let entry = VersionLogEntry::decode(&log_bytes)?;
        let old_version = node::decode(&entry.bytes)?;
        let old_props_bytes = reopened_graph.read_node_prop_bytes(&old_version.row.props)?;
        let old_raw = props::decode_raw(&old_props_bytes)?;
        assert_eq!(old_raw.len(), 1);
        match old_raw[0].value {
            RawPropValue::Int(value) => assert_eq!(value, 1),
            _ => panic!("unexpected historical property {:?}", old_raw[0].value),
        }

        drop(read);
        drop(reopened_graph);
        drop(reopened_store);
        Ok(())
    }
}

mod wal_alert_tests {
    use crate::primitives::pager::AsyncFsyncBacklog;
    use crate::primitives::wal::{WalAllocatorStats, WalCommitBacklog};
    use crate::storage::graph::mvcc_ops::{
        wal_health, ASYNC_FSYNC_LAG_ALERT, WAL_HORIZON_LAG_ALERT,
    };
    use crate::types::Lsn;

    #[test]
    fn recommends_reuse_when_backlog_exceeds_ready_queue() {
        let backlog = WalCommitBacklog {
            pending_commits: 12,
            pending_frames: 20_000,
            worker_running: true,
            direct_commit_active: false,
            pending_syncs: 0,
        };
        let allocator = WalAllocatorStats {
            segment_size_bytes: 64 * 1024 * 1024,
            preallocate_segments: 1,
            ready_segments: 0,
            recycle_segments: 0,
            reused_segments_total: 0,
            created_segments_total: 0,
            allocation_error: None,
        };
        let (alerts, recommended) =
            wal_health(4096, Some(&backlog), Some(&allocator), None, None, None);
        assert!(alerts.iter().any(|a| a.contains("reuse queue short")));
        assert_eq!(recommended, Some(2));
    }

    #[test]
    fn alerts_on_async_fsync_lag_and_horizon_gap() {
        let async_fsync = AsyncFsyncBacklog {
            pending_lsn: Lsn(9000),
            durable_lsn: Lsn(100),
            pending_lag: ASYNC_FSYNC_LAG_ALERT + 10,
            last_error: None,
        };
        let (alerts, recommended) = wal_health(
            4096,
            None,
            None,
            Some(&async_fsync),
            Some(5),
            Some(Lsn(WAL_HORIZON_LAG_ALERT + 50)),
        );
        assert!(alerts.iter().any(|a| a.contains("async fsync lag")));
        assert!(alerts.iter().any(|a| a.contains("vacuum horizon lags")));
        assert!(recommended.is_none());
    }
}
