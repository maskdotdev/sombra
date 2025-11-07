#![forbid(unsafe_code)]

use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use serde::Serialize;
use serde_json;
use sombra_admin::{verify, AdminOpenOptions, VerifyLevel};
use sombra_bench::env::EnvMetadata;
use sombra_cli::import_export::{run_import, EdgeImportConfig, ImportConfig, NodeImportConfig};
use sombra_ffi::{
    Database, DatabaseOptions, DirectionSpec, EdgeSpec, LiteralSpec, MatchSpec, PredicateSpec,
    ProjectionSpec, QuerySpec,
};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Parser, Debug)]
#[command(author, version, about = "Runs LDBC SNB baseline import + query mix")]
struct Args {
    #[arg(long, value_name = "CSV", help = "Nodes CSV in Sombra import format")]
    nodes: PathBuf,

    #[arg(long, value_name = "CSV", help = "Edges CSV in Sombra import format")]
    edges: PathBuf,

    #[arg(long, default_value = "ldbc-baseline.sombra")]
    db: PathBuf,

    #[arg(long)]
    out_dir: Option<PathBuf>,

    #[arg(long, default_value = "bench-results")]
    results_root: PathBuf,

    #[arg(long, help = "Skip the import step and reuse existing database")]
    skip_import: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let out_dir = args
        .out_dir
        .unwrap_or_else(|| args.results_root.join(format!("ldbc-{timestamp}")));
    fs::create_dir_all(&out_dir)?;

    let opts = AdminOpenOptions::default();
    let mut import_report = None;
    if !args.skip_import {
        let cfg = build_import_config(&args.nodes, &args.edges, &args.db);
        let start = Instant::now();
        let summary = run_import(&cfg, &opts)?;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        import_report = Some(ImportReport {
            nodes: summary.nodes_imported,
            edges: summary.edges_imported,
            elapsed_ms,
        });
    }

    let verify_start = Instant::now();
    let verify_report = verify(&args.db, &opts, VerifyLevel::Full)?;
    let verify_elapsed = verify_start.elapsed().as_secs_f64() * 1000.0;

    let db = Database::open(&args.db, DatabaseOptions::default())?;
    let queries = run_queries(&db)?;

    let report = BaselineReport {
        dataset: DatasetInfo {
            nodes_csv: args.nodes.clone(),
            edges_csv: args.edges.clone(),
        },
        import: import_report,
        verify: VerifySummary {
            success: verify_report.success,
            findings: verify_report.findings.len() as u64,
            elapsed_ms: verify_elapsed,
        },
        queries,
    };

    let env = EnvMetadata::collect(&out_dir);
    fs::write(
        out_dir.join("ldbc_env.json"),
        serde_json::to_vec_pretty(&env)?,
    )?;
    fs::write(
        out_dir.join("ldbc_baseline.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!(
        "ldbc baseline complete → {}",
        out_dir.join("ldbc_baseline.json").display()
    );
    Ok(())
}

fn build_import_config(nodes: &PathBuf, edges: &PathBuf, db: &PathBuf) -> ImportConfig {
    ImportConfig {
        db_path: db.clone(),
        create_if_missing: true,
        nodes: Some(NodeImportConfig {
            path: nodes.clone(),
            id_column: "id".into(),
            label_column: Some("label".into()),
            static_labels: Vec::new(),
            prop_columns: None,
        }),
        edges: Some(EdgeImportConfig {
            path: edges.clone(),
            src_column: "src".into(),
            dst_column: "dst".into(),
            type_column: Some("type".into()),
            static_type: None,
            prop_columns: None,
        }),
    }
}

fn run_queries(db: &Database) -> Result<Vec<QueryRun>> {
    let workloads = [
        ("mutual_follows", mutual_follows()),
        ("two_hop", two_hop()),
        ("name_filter", name_filter_expand()),
    ];
    let mut out = Vec::new();
    for (name, spec) in workloads {
        let start = Instant::now();
        let rows = db.execute(spec)?;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        out.push(QueryRun {
            name: name.to_string(),
            rows: rows.len() as u64,
            elapsed_ms,
        });
    }
    Ok(out)
}

#[derive(Serialize)]
struct BaselineReport {
    dataset: DatasetInfo,
    import: Option<ImportReport>,
    verify: VerifySummary,
    queries: Vec<QueryRun>,
}

#[derive(Serialize)]
struct DatasetInfo {
    nodes_csv: PathBuf,
    edges_csv: PathBuf,
}

#[derive(Serialize)]
struct ImportReport {
    nodes: u64,
    edges: u64,
    elapsed_ms: f64,
}

#[derive(Serialize)]
struct VerifySummary {
    success: bool,
    findings: u64,
    elapsed_ms: f64,
}

#[derive(Serialize)]
struct QueryRun {
    name: String,
    rows: u64,
    elapsed_ms: f64,
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
