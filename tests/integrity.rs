#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]

use sombra::pager::DEFAULT_PAGE_SIZE;
use sombra::{Config, GraphDB, IntegrityOptions, Node};
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use tempfile::TempDir;

#[test]
fn verify_integrity_reports_clean_state() {
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path().join("graph.sombra");

    {
        let mut db = GraphDB::open_with_config(&db_path, Config::production()).expect("open db");
        let mut tx = db.begin_transaction().expect("begin transaction");
        tx.add_node(Node::new(0)).expect("add node");
        tx.add_node(Node::new(0)).expect("add node");
        tx.commit().expect("commit");
        db.flush().expect("flush");
    }

    let mut db = GraphDB::open(&db_path).expect("reopen db");
    let report = db
        .verify_integrity(IntegrityOptions::default())
        .expect("integrity report");
    assert!(report.is_clean(), "expected clean report: {report:?}");
}

#[test]
fn verify_integrity_detects_checksum_mismatch() {
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path().join("graph.sombra");

    let mut db = GraphDB::open_with_config(&db_path, Config::production()).expect("open db");
    {
        let mut tx = db.begin_transaction().expect("begin tx");
        tx.add_node(Node::new(0)).expect("add node");
        tx.commit().expect("commit");
    }
    db.flush().expect("flush");

    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .expect("open db file");
        // Corrupt first byte of the first data page (page 1).
        file.seek(SeekFrom::Start(DEFAULT_PAGE_SIZE as u64))
            .expect("seek to page start");
        file.write_all(&[0xAA]).expect("corrupt page data");
    }

    let report = db
        .verify_integrity(IntegrityOptions::default())
        .expect("integrity report");
    assert!(
        !report.is_clean(),
        "expected checksum mismatch to be reported"
    );
    assert!(
        report.checksum_failures > 0,
        "checksum failures should be recorded"
    );
}
