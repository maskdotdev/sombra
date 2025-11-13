#![allow(missing_docs)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use sombra::{
    admin::{
        checkpoint, stats, vacuum_into, verify, AdminOpenOptions, CheckpointMode, VacuumOptions,
        VerifyLevel,
    },
    primitives::pager::{PageStore, Pager, PagerOptions},
    storage::{EdgeSpec, Graph, GraphOptions, NodeSpec, PropEntry, PropValue},
    types::{LabelId, PropId, TypeId},
};
use tempfile::TempDir;

fn setup_db(name: &str) -> (TempDir, PathBuf) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join(format!("{name}.sombra"));
    seed_demo(&path).expect("seed demo data");
    (dir, path)
}

fn seed_demo(path: &Path) -> sombra::types::Result<()> {
    let pager = Arc::new(Pager::create(path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let graph = Graph::open(
        GraphOptions::new(store.clone())
            .inline_prop_blob(256)
            .inline_prop_value(64),
    )?;

    let mut write = pager.begin_write()?;
    let alice = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[PropEntry::new(PropId(1), PropValue::Str("Alice"))],
        },
    )?;
    let bob = graph.create_node(
        &mut write,
        NodeSpec {
            labels: &[LabelId(1)],
            props: &[PropEntry::new(PropId(1), PropValue::Str("Bob"))],
        },
    )?;
    graph.create_edge(
        &mut write,
        EdgeSpec {
            src: alice,
            dst: bob,
            ty: TypeId(1),
            props: &[PropEntry::new(PropId(2), PropValue::Int(42))],
        },
    )?;
    pager.commit(write)?;
    pager.checkpoint(CheckpointMode::Force)?;
    Ok(())
}

fn admin_opts() -> AdminOpenOptions {
    AdminOpenOptions::default()
}

#[test]
fn stats_reports_sections() {
    let (_dir, db_path) = setup_db("admin-stats");
    let opts = admin_opts();
    let report = stats(&db_path, &opts).expect("stats report");
    assert!(report.pager.page_size >= 4096);
    assert_eq!(
        report.storage.next_node_id,
        report.storage.estimated_node_count + 1
    );
    assert!(report.filesystem.db_size_bytes > 0);
}

#[test]
fn checkpoint_returns_lsn() {
    let (_dir, db_path) = setup_db("admin-checkpoint");
    let opts = admin_opts();
    let report = checkpoint(&db_path, &opts, CheckpointMode::Force).expect("checkpoint");
    assert_eq!(report.mode, "force");
    assert!(report.last_checkpoint_lsn > 0);
}

#[test]
fn vacuum_into_creates_copy_with_analysis() {
    let (dir, db_path) = setup_db("admin-vacuum");
    let dst = dir.path().join("vacuumed.sombra");
    let opts = admin_opts();
    let mut vac_opts = VacuumOptions::default();
    vac_opts.analyze = true;
    let report = vacuum_into(&db_path, &dst, &opts, &vac_opts).expect("vacuum");
    assert!(dst.exists());
    assert!(report.analyze_performed);
    assert!(report.analyze_summary.is_some());
    assert!(report.copied_bytes > 0);
}

#[test]
fn verify_full_reports_counts() {
    let (_dir, db_path) = setup_db("admin-verify");
    let opts = admin_opts();
    let report = verify(&db_path, &opts, VerifyLevel::Full).expect("verify");
    assert!(
        report.success,
        "verification findings: {:?}",
        report.findings
    );
    assert_eq!(report.counts.nodes_found, 2);
    assert_eq!(report.counts.edges_found, 1);
    assert!(report.counts.adjacency_entries >= 2);
}
