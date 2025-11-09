#![forbid(unsafe_code)]

#[cfg(feature = "ffi-benches")]
mod support;

#[cfg(not(feature = "ffi-benches"))]
fn main() {
    eprintln!("Enable the `ffi-benches` feature to build the macro_queries benchmark.");
}

#[cfg(feature = "ffi-benches")]
mod bench {
    use super::*;

    use std::fs;
    use std::path::PathBuf;

    use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
    use sombra::admin::AdminOpenOptions;
    use sombra::cli::import_export::{
        run_import, EdgeImportConfig, ImportConfig, NodeImportConfig,
    };
    use sombra::ffi::{
        Database, DatabaseOptions, DirectionSpec, EdgeSpec, LiteralSpec, MatchSpec, PredicateSpec,
        ProjectionSpec, QuerySpec,
    };
    use support::datasets::SyntheticDataset;

    const NODE_COUNT: usize = 50_000;
    const EDGE_COUNT: usize = 200_000;

    fn macro_queries(c: &mut Criterion) {
        let mut group = c.benchmark_group("macro/query_mix");
        group.sample_size(20);
        let harness = QueryHarness::new(NODE_COUNT, EDGE_COUNT);
        group.throughput(Throughput::Elements(3));
        group.bench_function("synthetic_mix", |b| b.iter(|| black_box(harness.run_mix())));
        group.finish();
    }

    struct QueryHarness {
        db: Database,
    }

    impl QueryHarness {
        fn new(nodes: usize, edges: usize) -> Self {
            let dataset = SyntheticDataset::ensure(nodes, edges);
            let scratch = dataset.scratch_dir();
            fs::create_dir_all(&scratch).expect("scratch dir");
            let db_path = scratch.join(format!("macro_queries_{nodes}_{edges}.sombra"));
            if !db_path.exists() {
                let opts = AdminOpenOptions::default();
                let cfg = build_import_config(&dataset, db_path.clone());
                run_import(&cfg, &opts).expect("import macro query db");
            }
            let db = Database::open(&db_path, DatabaseOptions::default()).expect("db open");
            Self { db }
        }

        fn run_mix(&self) -> usize {
            let specs = [mutual_follows(), two_hop(), name_filter_expand()];
            let mut total = 0usize;
            for spec in specs {
                let rows = self.db.execute(spec).expect("query");
                total += rows.len();
            }
            total
        }
    }

    fn build_import_config(dataset: &SyntheticDataset, db_path: PathBuf) -> ImportConfig {
        ImportConfig {
            db_path,
            create_if_missing: true,
            nodes: Some(NodeImportConfig {
                path: dataset.nodes_csv.clone(),
                id_column: "id".into(),
                label_column: Some("label".into()),
                static_labels: Vec::new(),
                prop_columns: Some(vec!["name".into()]),
            }),
            edges: Some(EdgeImportConfig {
                path: dataset.edges_csv.clone(),
                src_column: "src".into(),
                dst_column: "dst".into(),
                type_column: None,
                static_type: Some("FOLLOWS".into()),
                prop_columns: None,
                trusted_endpoints: false,
                exists_cache_capacity: 1024,
            }),
        }
    }

    fn mutual_follows() -> QuerySpec {
        QuerySpec {
            matches: vec![
                MatchSpec {
                    var: "a".into(),
                    label: Some("User".into()),
                },
                MatchSpec {
                    var: "b".into(),
                    label: Some("User".into()),
                },
            ],
            edges: vec![
                EdgeSpec {
                    from: "a".into(),
                    to: "b".into(),
                    edge_type: Some("FOLLOWS".into()),
                    direction: DirectionSpec::Out,
                },
                EdgeSpec {
                    from: "b".into(),
                    to: "a".into(),
                    edge_type: Some("FOLLOWS".into()),
                    direction: DirectionSpec::Out,
                },
            ],
            predicates: Vec::new(),
            distinct: true,
            projections: vec![
                ProjectionSpec::Var {
                    var: "a".into(),
                    alias: None,
                },
                ProjectionSpec::Var {
                    var: "b".into(),
                    alias: None,
                },
            ],
        }
    }

    fn two_hop() -> QuerySpec {
        QuerySpec {
            matches: vec![
                MatchSpec {
                    var: "src".into(),
                    label: Some("User".into()),
                },
                MatchSpec {
                    var: "mid".into(),
                    label: Some("User".into()),
                },
                MatchSpec {
                    var: "dst".into(),
                    label: Some("User".into()),
                },
            ],
            edges: vec![
                EdgeSpec {
                    from: "src".into(),
                    to: "mid".into(),
                    edge_type: Some("FOLLOWS".into()),
                    direction: DirectionSpec::Out,
                },
                EdgeSpec {
                    from: "mid".into(),
                    to: "dst".into(),
                    edge_type: Some("FOLLOWS".into()),
                    direction: DirectionSpec::Out,
                },
            ],
            predicates: Vec::new(),
            distinct: false,
            projections: vec![ProjectionSpec::Var {
                var: "dst".into(),
                alias: None,
            }],
        }
    }

    fn name_filter_expand() -> QuerySpec {
        QuerySpec {
            matches: vec![MatchSpec {
                var: "u".into(),
                label: Some("User".into()),
            }],
            edges: vec![EdgeSpec {
                from: "u".into(),
                to: "f".into(),
                edge_type: Some("FOLLOWS".into()),
                direction: DirectionSpec::Out,
            }],
            predicates: vec![PredicateSpec::Eq {
                var: "u".into(),
                prop: "name".into(),
                value: LiteralSpec::String("user-123".into()),
            }],
            distinct: false,
            projections: vec![
                ProjectionSpec::Var {
                    var: "u".into(),
                    alias: Some("origin".into()),
                },
                ProjectionSpec::Var {
                    var: "f".into(),
                    alias: Some("neighbor".into()),
                },
            ],
        }
    }
}

#[cfg(feature = "ffi-benches")]
use bench::macro_queries;

#[cfg(feature = "ffi-benches")]
criterion_group!(benches, macro_queries);
#[cfg(feature = "ffi-benches")]
criterion_main!(benches);
