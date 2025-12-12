#![allow(missing_docs)]
#![allow(clippy::arc_with_non_send_sync, clippy::field_reassign_with_default)]

use std::path::PathBuf;

use assert_cmd::cargo::cargo_bin_cmd;
use sombra::primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions};
use sombra::storage::{EdgeSpec, Graph, GraphOptions, NodeSpec, PropEntry, PropValue};
use sombra::types::{LabelId, PropId, TypeId};
use std::sync::Arc;
use tempfile::TempDir;

fn setup_db(name: &str) -> (TempDir, PathBuf) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join(format!("{name}.sombra"));
    seed_demo(&path).expect("seed demo data");
    (dir, path)
}

fn seed_demo(path: &std::path::Path) -> sombra::types::Result<()> {
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

#[test]
fn stats_detailed_text_includes_storage_space_section() {
    let (_dir, db_path) = setup_db("stats-detailed");
    let output = cargo_bin_cmd!("cli")
        .args(["stats", "--detailed"])
        .arg(&db_path)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let stdout = String::from_utf8_lossy(&output);
    assert!(
        stdout.contains("StorageSpace"),
        "expected StorageSpace section in detailed stats output"
    );
}
