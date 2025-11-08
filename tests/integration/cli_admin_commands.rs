use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::cargo::cargo_bin_cmd;
use serde_json::Value;
use sombra::{
    primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions},
    storage::{EdgeSpec, Graph, GraphOptions, NodeSpec, PropEntry, PropValue},
    types::{LabelId, PropId, TypeId},
};
use std::sync::Arc;
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

#[test]
fn stats_emits_json() {
    let (_dir, db_path) = setup_db("stats");
    let output = cargo_bin_cmd!("sombra")
        .args(["--format", "json", "stats"])
        .arg(&db_path)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: Value = serde_json::from_slice(&output).expect("valid json");
    assert!(json["pager"]["page_size"].is_number());
}

#[test]
fn checkpoint_completes() {
    let (_dir, db_path) = setup_db("checkpoint");
    cargo_bin_cmd!("sombra")
        .args(["checkpoint"])
        .arg(&db_path)
        .assert()
        .success();
}

#[test]
fn verify_full_succeeds() {
    let (_dir, db_path) = setup_db("verify");
    let output = cargo_bin_cmd!("sombra")
        .args(["--format", "json", "verify", "--level", "full"])
        .arg(&db_path)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: Value = serde_json::from_slice(&output).expect("valid json");
    assert!(json["success"].as_bool().unwrap_or(false));
}

#[test]
fn vacuum_writes_destination_file() {
    let (dir, db_path) = setup_db("vacuum");
    let dst = dir.path().join("vacuumed.sombra");
    cargo_bin_cmd!("sombra")
        .arg("vacuum")
        .arg(&db_path)
        .arg("--into")
        .arg(&dst)
        .assert()
        .success();
    assert!(dst.exists(), "vacuum destination file should be created");
    let src_size = fs::metadata(&db_path).unwrap().len();
    let dst_size = fs::metadata(&dst).unwrap().len();
    assert_eq!(src_size, dst_size);
}

#[test]
fn vacuum_analyze_reports_summary() {
    let (dir, db_path) = setup_db("vacuum-analyze");
    let dst = dir.path().join("vacuumed_analyze.sombra");
    let output = cargo_bin_cmd!("sombra")
        .args(["--format", "json", "vacuum"])
        .arg(&db_path)
        .arg("--into")
        .arg(&dst)
        .arg("--analyze")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: Value = serde_json::from_slice(&output).expect("valid json");
    assert!(
        json["analyze_summary"].is_object(),
        "analyze summary should be present"
    );
    assert!(dst.exists());
}

#[test]
fn import_and_export_round_trip() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("graph.sombra");
    let nodes_csv = dir.path().join("nodes.csv");
    let edges_csv = dir.path().join("edges.csv");
    let nodes_out = dir.path().join("nodes_out.csv");
    let edges_out = dir.path().join("edges_out.csv");

    fs::write(&nodes_csv, "id,name\n1,Ada\n2,Bob\n").unwrap();
    fs::write(&edges_csv, "src,dst\n1,2\n").unwrap();

    cargo_bin_cmd!("sombra")
        .args([
            "import",
            db_path.to_str().unwrap(),
            "--nodes",
            nodes_csv.to_str().unwrap(),
            "--node-id-column",
            "id",
            "--node-labels",
            "User",
            "--node-props",
            "name",
            "--edges",
            edges_csv.to_str().unwrap(),
            "--edge-src-column",
            "src",
            "--edge-dst-column",
            "dst",
            "--edge-type",
            "FOLLOWS",
            "--create",
        ])
        .assert()
        .success();

    cargo_bin_cmd!("sombra")
        .args([
            "export",
            db_path.to_str().unwrap(),
            "--nodes",
            nodes_out.to_str().unwrap(),
            "--edges",
            edges_out.to_str().unwrap(),
            "--node-props",
            "name",
        ])
        .assert()
        .success();

    let nodes_contents = fs::read_to_string(&nodes_out).unwrap();
    assert!(nodes_contents.contains("Ada"));
    assert!(nodes_contents.contains("Bob"));

    let edges_contents = fs::read_to_string(&edges_out).unwrap();
    assert!(edges_contents.contains("FOLLOWS"));
}
