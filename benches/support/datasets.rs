#![forbid(unsafe_code)]

use std::fs;
use std::path::{Path, PathBuf};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Synthetic dataset backed by generated CSV files for benchmarking.
pub struct SyntheticDataset {
    base: PathBuf,
    pub nodes_csv: PathBuf,
    pub edges_csv: PathBuf,
    pub node_count: usize,
    pub edge_count: usize,
}

impl SyntheticDataset {
    pub fn ensure(node_count: usize, edge_count: usize) -> Self {
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target/bench_datasets/synthetic");
        fs::create_dir_all(&base).expect("dataset dir");
        let nodes_csv = base.join(format!("nodes_{node_count}.csv"));
        if !nodes_csv.exists() {
            write_nodes(&nodes_csv, node_count);
        }
        let edges_csv = base.join(format!("edges_{edge_count}.csv"));
        if !edges_csv.exists() {
            write_edges(&edges_csv, edge_count, node_count);
        }
        Self {
            base,
            nodes_csv,
            edges_csv,
            node_count,
            edge_count,
        }
    }

    pub fn scratch_dir(&self) -> PathBuf {
        self.base.join("scratch")
    }
}

fn write_nodes(path: &Path, count: usize) {
    let mut writer = csv::Writer::from_path(path).expect("nodes csv");
    writer
        .write_record(["id", "label", "name"])
        .expect("header");
    for id in 0..count {
        writer
            .write_record([id.to_string(), "User".to_string(), format!("user-{id}")])
            .expect("row");
    }
    writer.flush().expect("flush");
}

fn write_edges(path: &Path, count: usize, nodes: usize) {
    let mut writer = csv::Writer::from_path(path).expect("edges csv");
    writer
        .write_record(["src", "dst", "weight"])
        .expect("header");
    let mut rng = ChaCha8Rng::seed_from_u64(0x5151_5151);
    for _ in 0..count {
        let src = rng.gen_range(0..nodes);
        let mut dst = rng.gen_range(0..nodes);
        if dst == src {
            dst = (dst + 1) % nodes;
        }
        let weight = rng.gen_range(1..1000);
        writer
            .write_record([src.to_string(), dst.to_string(), weight.to_string()])
            .expect("row");
    }
    writer.flush().expect("flush");
}
