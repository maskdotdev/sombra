//! Macro benchmark that exercises the CSV import pipeline.
#![forbid(unsafe_code)]
#![allow(missing_docs)]

mod support;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::collections::HashMap;
use sombra::admin::AdminOpenOptions;
use sombra::cli::import_export::{run_import, EdgeImportConfig, ImportConfig, NodeImportConfig};
use support::datasets::SyntheticDataset;

const NODE_COUNT: usize = 50_000;
const EDGE_COUNT: usize = 200_000;

fn macro_import(c: &mut Criterion) {
    let mut group = c.benchmark_group("macro/import");
    group.sample_size(10);
    group.throughput(Throughput::Elements((NODE_COUNT + EDGE_COUNT) as u64));
    let harness = ImportHarness::new(NODE_COUNT, EDGE_COUNT);
    group.bench_function("synthetic_csv", |b| b.iter(|| black_box(harness.run())));
    group.finish();
}

struct ImportHarness {
    dataset: SyntheticDataset,
    opts: AdminOpenOptions,
}

impl ImportHarness {
    fn new(nodes: usize, edges: usize) -> Self {
        let dataset = SyntheticDataset::ensure(nodes, edges);
        Self {
            dataset,
            opts: AdminOpenOptions::default(),
        }
    }

    fn run(&self) -> (u64, u64) {
        let tmpdir = tempfile::tempdir().expect("tempdir");
        let db_path = tmpdir.path().join("macro-import.sombra");
        let config = ImportConfig {
            db_path: db_path.clone(),
            create_if_missing: true,
            disable_indexes: false,
            build_indexes: false,
            nodes: Some(NodeImportConfig {
                path: self.dataset.nodes_csv.clone(),
                id_column: "id".into(),
                label_column: Some("label".into()),
                static_labels: Vec::new(),
                prop_columns: Some(vec!["name".into()]),
                prop_types: HashMap::new(),
            }),
            edges: Some(EdgeImportConfig {
                path: self.dataset.edges_csv.clone(),
                src_column: "src".into(),
                dst_column: "dst".into(),
                type_column: None,
                static_type: Some("FOLLOWS".into()),
                prop_columns: None,
                trusted_endpoints: false,
                exists_cache_capacity: 1024,
                prop_types: HashMap::new(),
            }),
        };
        let summary = run_import(&config, &self.opts).expect("import");
        (summary.nodes_imported, summary.edges_imported)
    }
}

criterion_group!(benches, macro_import);
criterion_main!(benches);
