#![allow(clippy::uninlined_format_args)]
#![allow(clippy::useless_vec)]
#![allow(clippy::len_zero)]
#![allow(clippy::ptr_arg)]

use sombra::error::GraphError;
use sombra::model::{Edge, Node, PropertyValue};
use sombra::GraphDB;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

const ITERATIONS: usize = 10_000;

#[test]
fn corruption_fuzzing_handles_errors_gracefully() {
    let temp_dir = tempdir().expect("temp dir");
    let db_path = temp_dir.path().join("graph.sombra");
    let db_path_str = db_path.to_str().expect("utf8 path").to_owned();

    seed_database(&db_path);

    let base_db = fs::read(&db_path).expect("read base db file");
    let wal_path = wal_path_for(&db_path);
    let base_wal = fs::read(&wal_path).unwrap_or_default();

    let mut rng_state = 0xDEADBEEFCAFEBABE_u64;

    for _ in 0..ITERATIONS {
        fs::write(&db_path, &base_db).expect("reset database file");
        if !base_wal.is_empty() {
            fs::write(&wal_path, &base_wal).expect("reset wal file");
        } else if wal_path.exists() {
            fs::write(&wal_path, []).expect("truncate wal file");
        }

        if base_db.len() > 0 && (next_random(&mut rng_state) & 1) == 0 {
            let mut corrupted = base_db.clone();
            let idx = (next_random(&mut rng_state) as usize) % corrupted.len();
            let bit = 1 << (next_random(&mut rng_state) as u8 & 7);
            corrupted[idx] ^= bit;
            fs::write(&db_path, &corrupted).expect("write corrupted db");
        } else if !base_wal.is_empty() {
            let max_truncate = base_wal.len();
            let new_len = (next_random(&mut rng_state) as usize) % (max_truncate + 1);
            fs::write(&wal_path, &base_wal[..new_len]).expect("write truncated wal");
        }

        match GraphDB::open(&db_path_str) {
            Ok(mut db) => {
                match db.get_node(1) {
                    Ok(_) => {}
                    Err(GraphError::Corruption(_)) | Err(GraphError::NotFound(_)) => {}
                    Err(other) => panic!("unexpected error while reading node: {other:?}"),
                }
                if let Err(err) = db.flush() {
                    match err {
                        GraphError::Corruption(_) | GraphError::Io(_) => {}
                        other => panic!("unexpected flush error: {other:?}"),
                    }
                }
            }
            Err(GraphError::Corruption(_)) => {}
            Err(GraphError::Io(_)) => {}
            Err(other) => panic!("unexpected error opening corrupted database: {other:?}"),
        }
    }
}

fn seed_database(db_path: &PathBuf) {
    let mut db = GraphDB::open(
        db_path
            .to_str()
            .expect("database path convertible to string"),
    )
    .expect("open clean database");

    let mut node_ids = Vec::new();
    for i in 0..5 {
        let mut node = Node::new(0);
        node.labels.push("Seed".to_string());
        let mut props = BTreeMap::new();
        props.insert("value".into(), PropertyValue::Int(i as i64));
        node.properties = props;
        let node_id = db.add_node(node).expect("seed node");
        node_ids.push(node_id);
    }

    for window in node_ids.windows(2) {
        if let &[source, target] = window {
            let mut edge = Edge::new(0, source, target, "seed");
            edge.properties
                .insert("weight".into(), PropertyValue::Float(1.5));
            db.add_edge(edge).expect("seed edge");
        }
    }

    db.flush().expect("flush base database");
}

fn wal_path_for(db_path: &PathBuf) -> PathBuf {
    let mut os_string = db_path.as_os_str().to_owned();
    os_string.push(".wal");
    PathBuf::from(os_string)
}

fn next_random(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 7;
    x ^= x >> 9;
    x ^= x << 8;
    *state = x;
    x
}
