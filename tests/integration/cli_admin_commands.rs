#![allow(missing_docs)]
#![allow(clippy::arc_with_non_send_sync, clippy::field_reassign_with_default)]

use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::cargo::cargo_bin_cmd;
use csv::ReaderBuilder;
use serde_json::Value;
use sombra::{
    primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions},
    storage::{
        catalog::{Dict, DictOptions},
        EdgeSpec, Graph, GraphOptions, IndexDef, IndexKind, NodeSpec, PropEntry, PropValue,
        PropValueOwned, TypeTag,
    },
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

fn create_age_index(path: &Path) {
    let pager = Arc::new(Pager::open(path, PagerOptions::default()).expect("open pager"));
    let store: Arc<dyn PageStore> = pager.clone();
    let graph = Graph::open(GraphOptions::new(store.clone())).expect("graph open");
    let dict = Dict::open(store, DictOptions::default()).expect("dict open");
    let label_id = dict
        .lookup("User")
        .expect("lookup label")
        .expect("label missing");
    let prop_age = dict
        .lookup("age")
        .expect("lookup prop")
        .expect("age prop missing");
    let mut write = pager.begin_write().expect("begin write");
    graph
        .create_label_index(&mut write, LabelId(label_id.0))
        .expect("create label index");
    let def = IndexDef {
        label: LabelId(label_id.0),
        prop: PropId(prop_age.0),
        kind: IndexKind::BTree,
        ty: TypeTag::Int,
    };
    graph
        .create_property_index(&mut write, def)
        .expect("create property index");
    pager.commit(write).expect("commit index build");
    pager
        .checkpoint(CheckpointMode::Force)
        .expect("checkpoint index build");

    let read = pager.begin_read().expect("begin read");
    let defs = graph
        .all_property_indexes()
        .expect("collect property indexes");
    assert_eq!(
        defs.len(),
        1,
        "expected property index definitions to be persisted"
    );
    drop(read);
}

fn assert_property_index_has_values(path: &Path, ages: &[i64]) {
    let pager = Arc::new(Pager::open(path, PagerOptions::default()).expect("open pager"));
    let store: Arc<dyn PageStore> = pager.clone();
    let graph = Graph::open(GraphOptions::new(store.clone())).expect("graph open");
    let dict = Dict::open(store, DictOptions::default()).expect("dict open");

    let label_id = dict
        .lookup("User")
        .expect("lookup label")
        .expect("label missing");
    let prop_age = dict
        .lookup("age")
        .expect("lookup prop")
        .expect("age prop missing");
    assert!(
        graph
            .property_index(LabelId(label_id.0), PropId(prop_age.0))
            .expect("property index lookup")
            .is_some(),
        "property index should exist; defs={:?}",
        graph.all_property_indexes().expect("list property indexes")
    );
    let read = pager.begin_read().expect("begin read");
    for age in ages {
        let matches = graph
            .property_scan_eq(
                &read,
                LabelId(label_id.0),
                PropId(prop_age.0),
                &PropValueOwned::Int(*age),
            )
            .expect("scan property index");
        assert!(
            !matches.is_empty(),
            "property index should return rows for age={age}, nodes={:?}",
            graph
                .scan_all_nodes(&read)
                .expect("scan nodes")
                .into_iter()
                .map(|(id, data)| {
                    let mut props = Vec::new();
                    for (prop, val) in data.props {
                        props.push((prop.0, format!("{val:?}")));
                    }
                    (
                        id.0,
                        data.labels.into_iter().map(|l| l.0).collect::<Vec<_>>(),
                        props,
                    )
                })
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn stats_emits_json() {
    let (_dir, db_path) = setup_db("stats");
    let output = cargo_bin_cmd!("cli")
        .args(["--format", "json", "stats"])
        .arg(&db_path)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: Value = serde_json::from_slice(&output).expect("valid json");
    assert!(json["pager"]["page_size"].is_number());
    assert!(json["storage_space"]["version_log_bytes"].is_number());
    assert!(json["storage_space"]["nodes_tree_pages"].is_number());
}

#[test]
fn checkpoint_completes() {
    let (_dir, db_path) = setup_db("checkpoint");
    cargo_bin_cmd!("cli")
        .args(["checkpoint"])
        .arg(&db_path)
        .assert()
        .success();
}

#[test]
fn verify_full_succeeds() {
    let (_dir, db_path) = setup_db("verify");
    let output = cargo_bin_cmd!("cli")
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
    cargo_bin_cmd!("cli")
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
    let output = cargo_bin_cmd!("cli")
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
    let nodes_out = dir.path().join("nodes_out.csv");
    let edges_out = dir.path().join("edges_out.csv");

    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/import");
    let base_nodes_csv = fixtures.join("people_nodes.csv");
    let base_edges_csv = fixtures.join("follows_edges.csv");
    let delta_nodes_csv = fixtures.join("people_nodes_delta.csv");
    let delta_edges_csv = fixtures.join("follows_edges_delta.csv");

    cargo_bin_cmd!("cli")
        .args([
            "import",
            db_path.to_str().unwrap(),
            "--nodes",
            base_nodes_csv.to_str().unwrap(),
            "--node-id-column",
            "id",
            "--node-labels",
            "User",
            "--node-props",
            "name,age,birth_date,last_seen,avatar",
            "--node-prop-types",
            "age:int,birth_date:date,last_seen:datetime,avatar:bytes",
            "--edges",
            base_edges_csv.to_str().unwrap(),
            "--edge-src-column",
            "src",
            "--edge-dst-column",
            "dst",
            "--edge-type",
            "FOLLOWS",
            "--edge-props",
            "weight,created_at",
            "--edge-prop-types",
            "weight:float,created_at:datetime",
            "--create",
        ])
        .assert()
        .success();

    create_age_index(&db_path);
    assert_property_index_has_values(&db_path, &[36]);

    cargo_bin_cmd!("cli")
        .args([
            "import",
            db_path.to_str().unwrap(),
            "--nodes",
            delta_nodes_csv.to_str().unwrap(),
            "--node-id-column",
            "id",
            "--node-labels",
            "User",
            "--node-props",
            "name,age,birth_date,last_seen,avatar",
            "--node-prop-types",
            "age:int,birth_date:date,last_seen:datetime,avatar:bytes",
            "--edges",
            delta_edges_csv.to_str().unwrap(),
            "--edge-src-column",
            "src",
            "--edge-dst-column",
            "dst",
            "--edge-type",
            "FOLLOWS",
            "--edge-props",
            "weight,created_at",
            "--edge-prop-types",
            "weight:float,created_at:datetime",
            "--disable-indexes",
            "--build-indexes",
        ])
        .assert()
        .success();

    cargo_bin_cmd!("cli")
        .args([
            "export",
            db_path.to_str().unwrap(),
            "--nodes",
            nodes_out.to_str().unwrap(),
            "--edges",
            edges_out.to_str().unwrap(),
            "--node-props",
            "name,age,birth_date,last_seen,avatar",
            "--edge-props",
            "weight,created_at",
        ])
        .assert()
        .success();

    let mut reader = ReaderBuilder::new().from_path(&nodes_out).unwrap();
    let rows: Vec<_> = reader
        .records()
        .map(|rec| rec.expect("valid node row"))
        .collect();
    assert_eq!(rows.len(), 4);
    let row_by_id = |id: &str| {
        rows.iter()
            .find(|record| record.get(0) == Some(id))
            .unwrap_or_else(|| panic!("missing node id {id}"))
    };
    let row1 = row_by_id("1");
    assert_eq!(row1.get(2), Some("Ada Lovelace"));
    assert_eq!(row1.get(3), Some("36"));
    assert_eq!(row1.get(4), Some("6918"));
    assert_eq!(row1.get(5), Some("1588336200000"));
    assert_eq!(row1.get(6), Some("0x416461"));
    let row4 = row_by_id("4");
    assert_eq!(row4.get(2), Some("Diana Prince"));
    assert_eq!(row4.get(3), Some("33"));
    assert_eq!(row4.get(4), Some("6258"));
    assert_eq!(row4.get(5), Some("1588704300000"));
    assert_eq!(row4.get(6), Some("0x446961"));

    let mut edges_reader = ReaderBuilder::new().from_path(&edges_out).unwrap();
    let edge_rows: Vec<_> = edges_reader
        .records()
        .map(|rec| rec.expect("valid edge row"))
        .collect();
    assert_eq!(edge_rows.len(), 3);
    assert!(
        edge_rows.iter().any(|row| row.get(0) == Some("3")
            && row.get(1) == Some("4")
            && row.get(4) == Some("1588705200000")),
        "expected edge 3->4 to be exported"
    );

    assert_property_index_has_values(&db_path, &[41, 36]);
}
